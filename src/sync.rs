//! Two-way live sync of one folder with every other member of a mesh.
//!
//! The mesh `state` document holds 1 signed slice for each author. Each peer writes its full view
//! of the folder into its own slice: `{"files": {"<rel/path>": {"sha256", "ticket", "clock"}}}`.
//! The merge rule is in [`crate::view`]. File bytes do not fit in a gossip frame, so each entry
//! holds a blob ticket that a peer redeems against the blob server of the author. A peer that
//! applies a winner copies it into its own slice with its own ticket, so the folder survives
//! when the share leaves.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{self, BlobServer, BlobTicket, ContentId};
use habilis_network::protocol::{Lookup, LookupOpts};
use habilis_network::runtime::{SendError, SliceError};
use serde_json::{Value, json};
use tokio::io::AsyncWrite;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::scan::{self, resolve, sha256_bytes};
use crate::view::View;
use crate::watch::watch;

const MESH_NAME: &str = "agent-knowledge";
/// A safety net for a `state_changed` event that never fires, for example after a backfill. It
/// also retries a write that failed.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);
/// A fetch fails when no byte arrives for this long. A big file is not cut off while it flows.
const FETCH_IDLE_LIMIT: Duration = Duration::from_secs(10);
const MAX_FETCHES: usize = 8;
/// A joiner waits this long for a slice that holds files before its first publish.
const FIRST_PUBLISH_WAIT: Duration = Duration::from_secs(5);
/// One slice holds at most 1 MiB. An entry is about 520 bytes: a 407 byte ticket (measured), a
/// 64 byte sha256, the keys and the clock. That is about 2000 entries. The estimate is a first
/// guard. The refusal of the mesh is the last one, because a ticket changes in length by host.
const SLICE_BYTES: usize = 1_000_000;
const ENTRY_BYTES: usize = 520;

/// Sync `root` until `shutdown` resolves. With `mesh` `None`, create a new mesh; else join it.
/// `known_dir` holds the last synced state of each folder and mesh, across restarts.
/// `on_ready` gets the mesh id, which is the hash that `mount` takes.
pub async fn run(
    root: PathBuf,
    mesh: Option<String>,
    known_dir: PathBuf,
    on_ready: impl FnOnce(&str),
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let root = std::fs::canonicalize(&root).with_context(|| format!("no folder at {}", root.display()))?;
    let (sink, mut events) = membership::json_sink();
    let creator = mesh.is_none();
    let options = JoinOptions::default()
        .mesh(mesh)
        .name(Some(MESH_NAME.to_owned()))
        .lookup([Lookup::Mdns, Lookup::Dht, Lookup::Relay]);
    let member = membership::join(&options, sink).await.map_err(|error| anyhow!("{error}"))?;
    let mesh_id = wait_ready(&mut events).await?;
    on_ready(&mesh_id);

    let blobs = BlobServer::start(LookupOpts::public_preset(), spool_dir(), None, PayloadPath::RelayAllowed)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let (_watcher, mut ticks) = watch(&root)?;
    let known_file = known_file(&known_dir, &mesh_id, &root);
    let known = load_known(&known_file);
    let mut folder = Folder {
        root,
        member,
        blobs,
        unadopted: known.keys().cloned().collect(),
        known,
        disk: BTreeMap::new(),
        at_join: None,
        can_publish: creator,
        started: Instant::now(),
        capped: false,
        overflowed: HashSet::new(),
        fetches: JoinSet::new(),
        in_flight: HashMap::new(),
        fetch_slots: Arc::new(Semaphore::new(MAX_FETCHES)),
        known_file,
    };

    folder.pass(true).await;

    let mut every = tokio::time::interval(RECONCILE_EVERY);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(()) = ticks.recv() => folder.pass(true).await,
            Some(joined) = folder.fetches.join_next(), if !folder.fetches.is_empty() => {
                folder.landed(joined).await;
            }
            Some(event) = events.recv() => {
                // Not the event's own document: events queue up while a pass runs, and an
                // older snapshot would undo a newer one. A pass reads the document again.
                if is_remote_state_change(&event) {
                    folder.pass(false).await;
                }
            }
            _ = every.tick() => {
                folder.can_publish |= folder.started.elapsed() >= FIRST_PUBLISH_WAIT;
                folder.pass(false).await;
            }
        }
    }
    folder.fetches.shutdown().await;
    folder.blobs.shutdown().await;
    membership::depart(folder.member.node).await.map_err(|error| anyhow!("{error}"))
}

struct Folder {
    root: PathBuf,
    member: Membership,
    blobs: BlobServer,
    /// The `(sha256, clock)` of each path as the disk and this peer last agreed on it. A watcher
    /// tick or a winner that matches it is our own echo, and is skipped. A file that a download
    /// wrote is here at once, so it never looks like a local edit.
    known: BTreeMap<String, Known>,
    /// The paths of `known` that our slice does not hold yet, or holds with a ticket that may be
    /// dead (every path loaded at start). Each pass stages them until a write succeeds.
    unadopted: HashSet<String>,
    /// The sha256 of each file as of the last scan, and of our own writes since.
    disk: BTreeMap<String, String>,
    /// The files that were on disk at join, and had no `known` entry. If the mesh already holds
    /// such a path, the mesh wins, as long as the file is unchanged.
    at_join: Option<BTreeMap<String, String>>,
    /// Off for a joiner until a slice that holds files shows up, so it does not publish before it
    /// has seen what the others hold.
    can_publish: bool,
    started: Instant,
    capped: bool,
    /// The paths whose clock overflowed, so each is logged once.
    overflowed: HashSet<String>,
    fetches: JoinSet<Fetched>,
    /// The `(clock, sha256)` that each running fetch is for.
    in_flight: HashMap<String, (u64, String)>,
    fetch_slots: Arc<Semaphore>,
    known_file: PathBuf,
}

/// The `(sha256, clock)` of a path in our slice. A `None` sha256 is a tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Known {
    sha256: Option<String>,
    clock: u64,
}

/// A change for our slice in the next write, and what `known` becomes when the write succeeds.
type Pending = BTreeMap<String, (Value, Known)>;

struct Download {
    path: String,
    sha256: String,
    clock: u64,
    tickets: Vec<String>,
    /// The sha256 of the file on disk when we planned the download. A change since then is a local
    /// edit, and it wins.
    local_before: Option<String>,
}

struct Fetched {
    download: Download,
    result: Result<Vec<u8>>,
}

impl Folder {
    /// Bring the disk, our slice and the other slices to 1 state. Each pass makes at most 1 write.
    /// A write that fails leaves `known` as it was, so the next pass plans the same change again.
    /// A download runs in the background and lands through [`Folder::landed`].
    async fn pass(&mut self, rescan: bool) {
        if rescan {
            match scan::scan(&self.root) {
                Ok(disk) => self.disk = disk.into_iter().collect(),
                Err(error) => return eprintln!("scan failed: {error}"),
            }
        }
        if self.at_join.is_none() {
            let unsynced = self.disk.iter().filter(|(path, _)| !self.known.contains_key(*path));
            self.at_join = Some(unsynced.map(|(path, sha)| (path.clone(), sha.clone())).collect());
        }
        let Some(document) = self.document().await else { return };
        let view = View::parse(&document);
        self.can_publish |= !view.is_empty();
        let mut pending = Pending::new();
        let paths: BTreeSet<String> =
            view.paths().chain(self.disk.keys()).chain(self.known.keys()).cloned().collect();
        for path in paths {
            self.plan(&path, &view, &mut pending).await;
        }
        self.write(pending).await;
    }

    async fn plan(&mut self, path: &str, view: &View, pending: &mut Pending) {
        let flying = self.in_flight.remove(path);
        let disk = self.disk.get(path).cloned();
        let known = self.known.get(path).cloned();
        let known_sha = known.as_ref().and_then(|known| known.sha256.clone());
        let winner = view.winner(path);
        let disk_is_winner = winner.as_ref().is_some_and(|winner| winner.sha256 == disk);
        // A file that was here at join, and that no earlier run synced, yields to a live file of
        // the mesh. A tombstone does not delete it: the file is published as a new version.
        let mesh_wins = known.is_none()
            && winner.as_ref().is_some_and(|winner| winner.sha256.is_some())
            && self.at_join.as_ref().and_then(|at_join| at_join.get(path)) == disk.as_ref();

        // A change on disk that no slice holds yet wins over everything: publish it above the
        // highest clock.
        if disk != known_sha && !disk_is_winner && !mesh_wins {
            if !self.can_publish {
                return;
            }
            let own = known.as_ref().map_or(0, |known| known.clock);
            let Some(clock) = view.next_clock(path, own) else {
                if self.overflowed.insert(path.to_owned()) {
                    eprintln!("clock of {path} is at its maximum");
                }
                return;
            };
            let change = Known { sha256: disk, clock };
            return self.stage(path, change, pending).await;
        }
        let Some(winner) = winner else {
            // No slice holds the path. Our saved state goes back into our slice.
            if let (Some(known), true) = (known, self.unadopted.contains(path)) {
                self.stage(path, known, pending).await;
            }
            return;
        };
        let target = Known { sha256: winner.sha256.clone(), clock: winner.clock };
        // Our slice is level with the winner, or the view is behind a write of ours.
        if let Some(known) = known.as_ref().filter(|known| known.clock > winner.clock || **known == target) {
            if self.unadopted.contains(path) {
                self.stage(path, known.clone(), pending).await;
            }
            return;
        }
        match winner.sha256 {
            Some(sha) if disk.as_deref() == Some(&sha) => self.adopt(path, target, pending).await,
            Some(sha) => {
                if flying == Some((winner.clock, sha.clone())) {
                    self.in_flight.insert(path.to_owned(), (winner.clock, sha));
                } else {
                    self.spawn_fetch(Download {
                        path: path.to_owned(),
                        sha256: sha,
                        clock: winner.clock,
                        tickets: winner.tickets,
                        local_before: disk,
                    });
                }
            }
            None => {
                if let Some(local) = resolve(&self.root, path).filter(|local| file_sha(local) == disk)
                    && std::fs::remove_file(local).is_ok()
                {
                    eprintln!("↓ deleted {path}");
                }
                self.disk.remove(path);
                self.adopt(path, target, pending).await;
            }
        }
    }

    /// Take a winner that the disk already matches as our own state, and stage it.
    async fn adopt(&mut self, path: &str, target: Known, pending: &mut Pending) {
        self.known.insert(path.to_owned(), target.clone());
        self.unadopted.insert(path.to_owned());
        self.stage(path, target, pending).await;
    }

    /// Stage an entry for our slice: a file with a ticket from our own blob server, or a tombstone.
    async fn stage(&self, path: &str, known: Known, pending: &mut Pending) {
        // A merge sets keys only, so each entry nulls the keys of the other kind.
        let entry = match &known.sha256 {
            Some(sha) => match self.blobs.register(&self.root.join(path), ContentId::new(sha)).await {
                Ok(ticket) => json!({ "sha256": sha, "ticket": ticket.encode(), "clock": known.clock, "deleted": null }),
                Err(error) => return eprintln!("publish {path} failed: {error}"),
            },
            None => json!({ "deleted": true, "clock": known.clock, "sha256": null, "ticket": null }),
        };
        pending.insert(path.to_owned(), (entry, known));
    }

    fn spawn_fetch(&mut self, download: Download) {
        self.in_flight.insert(download.path.clone(), (download.clock, download.sha256.clone()));
        let slots = Arc::clone(&self.fetch_slots);
        self.fetches.spawn(async move {
            let _slot = slots.acquire_owned().await;
            let result = fetch_any(download.tickets.clone(), download.sha256.clone()).await;
            Fetched { download, result }
        });
    }

    /// A fetch ended. Write the file, and stage it for our slice because we now hold its bytes.
    async fn landed(&mut self, joined: std::result::Result<Fetched, tokio::task::JoinError>) {
        let Ok(Fetched { download, result }) = joined else { return };
        let current = (download.clock, download.sha256.clone());
        if self.in_flight.get(&download.path) != Some(&current) {
            return; // The winner moved on while we fetched.
        }
        self.in_flight.remove(&download.path);
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => return eprintln!("download {} failed: {error:#}", download.path),
        };
        let Some(local) = resolve(&self.root, &download.path) else {
            return eprintln!("unsafe path {:?}", download.path);
        };
        if file_sha(&local) != download.local_before {
            return; // A local edit came in during the fetch. It wins at the next pass.
        }
        if let Some(parent) = local.parent()
            && let Err(error) = tokio::fs::create_dir_all(parent).await
        {
            return eprintln!("download {} failed: {error}", download.path);
        }
        if let Err(error) = write_atomic(&local, &bytes).await {
            return eprintln!("download {} failed: {error:#}", download.path);
        }
        eprintln!("↓ {}", download.path);
        self.disk.insert(download.path.clone(), download.sha256.clone());
        let mut pending = Pending::new();
        let target = Known { sha256: Some(download.sha256), clock: download.clock };
        self.adopt(&download.path, target, &mut pending).await;
        self.write(pending).await;
    }

    /// Write every staged change into our slice with 1 merge.
    async fn write(&mut self, pending: Pending) {
        if pending.is_empty() {
            return;
        }
        let entries: BTreeSet<&String> = self.known.keys().chain(pending.keys()).collect();
        let bytes: usize = entries.iter().map(|path| path.len() + ENTRY_BYTES).sum();
        if bytes > SLICE_BYTES {
            return self.cap();
        }
        let files: serde_json::Map<String, Value> =
            pending.iter().map(|(path, (entry, _))| (path.clone(), entry.clone())).collect();
        match self.merge(json!({ "files": files })).await {
            Ok(()) => {}
            Err(Merge::Full) => return self.cap(),
            Err(Merge::Failed(error)) => return eprintln!("publish failed: {error:#}"),
        }
        self.capped = false;
        for (path, (_, known)) in pending {
            eprintln!("↑ {}{path}", if known.sha256.is_none() { "deleted " } else { "" });
            self.unadopted.remove(&path);
            self.known.insert(path, known);
        }
        self.save_known();
    }

    /// Our slice is full: publishing stops, downloads go on.
    fn cap(&mut self) {
        if !self.capped {
            eprintln!("the folder has too many files for 1 slice: publishing stops, downloads go on");
        }
        self.capped = true;
    }

    async fn merge(&self, merge: Value) -> std::result::Result<(), Merge> {
        let sent = self
            .member
            .request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply }))
            .await
            .map_err(|error| Merge::Failed(anyhow!("{error}")))?;
        match sent {
            Ok(()) => Ok(()),
            Err(SendError::State(SliceError::ValueTooLarge { .. } | SliceError::ValueTooComplex { .. })) => {
                Err(Merge::Full)
            }
            Err(error) => Err(Merge::Failed(anyhow!("{error}"))),
        }
    }

    async fn document(&self) -> Option<Value> {
        let text = self.member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await.ok()?;
        serde_json::from_str(&text).ok()
    }

    fn save_known(&self) {
        let known: serde_json::Map<String, Value> = self
            .known
            .iter()
            .map(|(path, known)| (path.clone(), json!({ "sha256": known.sha256, "clock": known.clock })))
            .collect();
        let saved = self
            .known_file
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| std::fs::write(&self.known_file, Value::Object(known).to_string()));
        if let Err(error) = saved {
            eprintln!("could not save {}: {error}", self.known_file.display());
        }
    }
}

enum Merge {
    /// The slice is past its size cap.
    Full,
    Failed(anyhow::Error),
}

/// Try every ticket for the same bytes at once. The first that verifies wins.
async fn fetch_any(tickets: Vec<String>, sha256: String) -> Result<Vec<u8>> {
    let mut fetches = JoinSet::new();
    for ticket in tickets {
        let sha256 = sha256.clone();
        fetches.spawn(async move { fetch_one(&ticket, &sha256).await });
    }
    let mut last = anyhow!("no peer holds these bytes");
    while let Some(joined) = fetches.join_next().await {
        match joined {
            Ok(Ok(bytes)) => return Ok(bytes),
            Ok(Err(error)) => last = error,
            Err(error) => last = anyhow!("{error}"),
        }
    }
    Err(last)
}

async fn fetch_one(ticket: &str, sha256: &str) -> Result<Vec<u8>> {
    let ticket = BlobTicket::decode(ticket).map_err(|error| anyhow!("{error}"))?;
    let clock = Instant::now();
    let last_byte = Arc::new(AtomicU64::new(0));
    let mut sink = Sink { bytes: Vec::new(), clock, last_byte: Arc::clone(&last_byte) };
    let stalled = async {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let quiet = clock.elapsed().saturating_sub(Duration::from_millis(last_byte.load(Ordering::Relaxed)));
            if quiet >= FETCH_IDLE_LIMIT {
                return;
            }
        }
    };
    tokio::select! {
        fetched = blob::fetch(&ticket, &mut sink, None) => fetched.map_err(|error| anyhow!("{error}"))?,
        () = stalled => bail!("no bytes for {} s", FETCH_IDLE_LIMIT.as_secs()),
    }
    if sha256_bytes(&sink.bytes) != sha256 {
        bail!("content does not match its sha256");
    }
    Ok(sink.bytes)
}

/// Collects the bytes of a fetch, and notes when the last byte came.
struct Sink {
    bytes: Vec<u8>,
    clock: Instant,
    last_byte: Arc<AtomicU64>,
}

impl AsyncWrite for Sink {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        self.bytes.extend_from_slice(buf);
        let millis = u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last_byte.store(millis, Ordering::Relaxed);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Where the saved state of `root` in `mesh` lives. Another mesh on the same folder has its own.
fn known_file(known_dir: &Path, mesh: &str, root: &Path) -> PathBuf {
    let key = format!("{mesh}\n{}", root.display());
    known_dir.join(format!("{}.json", sha256_bytes(key.as_bytes())))
}

fn load_known(file: &Path) -> BTreeMap<String, Known> {
    let Ok(text) = std::fs::read_to_string(file) else { return BTreeMap::new() };
    let Ok(Value::Object(saved)) = serde_json::from_str(&text) else { return BTreeMap::new() };
    saved
        .into_iter()
        .filter_map(|(path, raw)| {
            let clock = raw.get("clock")?.as_u64()?;
            Some((path, Known { sha256: raw.get("sha256").and_then(Value::as_str).map(str::to_owned), clock }))
        })
        .collect()
}

/// Write through a hidden sibling and rename it, so an editor never sees half a file.
/// The scanner skips hidden names, so the temp file never gets published.
async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path.file_name().context("no file name")?.to_string_lossy();
    let temp = path.with_file_name(format!(".{name}.agent-knowledge"));
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, path).await?;
    Ok(())
}

fn file_sha(path: &Path) -> Option<String> {
    std::fs::read(path).ok().map(|bytes| sha256_bytes(&bytes))
}

async fn wait_ready(events: &mut mpsc::UnboundedReceiver<String>) -> Result<String> {
    while let Some(event) = events.recv().await {
        let event: Value = serde_json::from_str(&event)?;
        if event["kind"] == "ready" {
            return event["mesh"].as_str().map(str::to_owned).context("ready event without a mesh id");
        }
    }
    bail!("the mesh stopped before it was ready")
}

fn is_remote_state_change(event: &str) -> bool {
    serde_json::from_str::<Value>(event).is_ok_and(|event| {
        event["kind"] == "state_changed" && event["channel"] == "state" && event["is_self"] == false
    })
}

fn spool_dir() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let instance = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("agent-knowledge-{}-{instance}", std::process::id()))
}
