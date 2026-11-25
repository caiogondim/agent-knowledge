//! Two-way live sync of one folder with every other member of a mesh.
//!
//! The mesh `state` document holds 1 signed slice for each author. Each peer writes its full view
//! of the folder into its own slice: `{"files": {"<rel/path>": {"sha256", "ticket", "clock"}}}`.
//! The merge rule is in [`crate::view`]. File bytes do not fit in a gossip frame, so each entry
//! holds a blob ticket that a peer redeems against the blob server of the author. A peer that
//! applies a winner copies it into its own slice with its own ticket, so the folder survives
//! when the share leaves.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{self, BlobServer, BlobTicket, ContentId};
use habilis_network::protocol::{Lookup, LookupOpts};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::scan::{self, resolve, sha256_bytes};
use crate::view::View;
use crate::watch::watch;

const MESH_NAME: &str = "agent-knowledge";
/// A safety net for a `state_changed` event that never fires, for example after a backfill. It
/// also retries a write that failed.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FETCHES: usize = 8;
/// A joiner waits this long for a remote slice before its first publish.
const FIRST_PUBLISH_WAIT: Duration = Duration::from_secs(5);
/// One slice holds at most 1 MiB. An entry is about 520 bytes: a 407 byte ticket (measured), a
/// 64 byte sha256, the keys and the clock. That is about 2000 entries.
const SLICE_BYTES: usize = 1_000_000;
const ENTRY_BYTES: usize = 520;

/// Sync `root` until `shutdown` resolves. With `mesh` `None`, create a new mesh; else join it.
/// `on_ready` gets the mesh id, which is the hash that `mount` takes.
pub async fn run(
    root: PathBuf,
    mesh: Option<String>,
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
    on_ready(&wait_ready(&mut events).await?);

    let blobs = BlobServer::start(LookupOpts::public_preset(), spool_dir(), None, PayloadPath::RelayAllowed)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let (_watcher, mut ticks) = watch(&root)?;
    let known_file = known_file(&root);
    let known = load_known(known_file.as_deref());
    let mut folder = Folder {
        root,
        member,
        blobs,
        unadopted: known.keys().cloned().collect(),
        known,
        disk: BTreeMap::new(),
        can_publish: creator,
        started: Instant::now(),
        capped: false,
        known_file,
    };

    folder.pass(true).await;

    let mut every = tokio::time::interval(RECONCILE_EVERY);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(()) = ticks.recv() => folder.pass(true).await,
            Some(event) = events.recv() => {
                // Not the event's own document: events queue up while a download runs, and an
                // older snapshot would undo a newer one. A pass reads the document again.
                if is_remote_state_change(&event) {
                    folder.can_publish = true;
                    folder.pass(false).await;
                }
            }
            _ = every.tick() => {
                folder.can_publish |= folder.started.elapsed() >= FIRST_PUBLISH_WAIT;
                folder.pass(false).await;
            }
        }
    }
    folder.blobs.shutdown().await;
    membership::depart(folder.member.node).await.map_err(|error| anyhow!("{error}"))
}

struct Folder {
    root: PathBuf,
    member: Membership,
    blobs: BlobServer,
    /// What we last wrote into our own slice for each path, and agreed with the disk.
    /// A watcher tick or a winner that matches it is our own echo, and is skipped.
    known: BTreeMap<String, Known>,
    /// The sha256 of each file as of the last scan, and of our own writes since.
    disk: BTreeMap<String, String>,
    /// Paths from the persisted `known`. Our old tickets may be dead, so we publish a new one.
    unadopted: HashSet<String>,
    /// Off for a joiner until it sees a remote slice, so it does not publish before it has seen
    /// what the others hold.
    can_publish: bool,
    started: Instant,
    capped: bool,
    known_file: Option<PathBuf>,
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

impl Folder {
    /// Bring the disk, our slice and the other slices to 1 state. Each pass makes 1 write.
    /// A write that fails leaves `known` as it was, so the next pass plans the same change again.
    async fn pass(&mut self, rescan: bool) {
        if rescan {
            match scan::scan(&self.root) {
                Ok(disk) => self.disk = disk.into_iter().collect(),
                Err(error) => return eprintln!("scan failed: {error}"),
            }
        }
        let Some(document) = self.document().await else { return };
        let view = View::parse(&document);
        let mut pending = Pending::new();
        let mut downloads = Vec::new();
        let paths: BTreeSet<String> =
            view.paths().chain(self.disk.keys()).chain(self.known.keys()).cloned().collect();
        for path in paths {
            self.plan(&path, &view, &mut pending, &mut downloads).await;
        }
        self.download(downloads, &mut pending).await;
        self.write(pending).await;
    }

    async fn plan(&mut self, path: &str, view: &View, pending: &mut Pending, downloads: &mut Vec<Download>) {
        let disk = self.disk.get(path).cloned();
        let known = self.known.get(path).cloned();
        let known_sha = known.as_ref().and_then(|known| known.sha256.clone());
        let winner = view.winner(path);
        let disk_is_winner = winner.as_ref().is_some_and(|winner| winner.sha256 == disk);

        // A change on disk that no slice holds yet wins over everything: publish it above the
        // highest clock.
        if disk != known_sha && !disk_is_winner {
            if !self.can_publish {
                return;
            }
            let Some(clock) = view.next_clock(path) else {
                return eprintln!("clock of {path} is at its maximum");
            };
            match disk {
                Some(sha) => self.stage_file(path, &sha, clock, pending).await,
                None => stage_tombstone(path, clock, pending),
            }
            return;
        }
        let Some(winner) = winner else {
            if let (Some(known), true) = (&known, self.unadopted.contains(path))
                && let Some(sha) = &known.sha256
            {
                self.stage_file(path, sha, known.clock, pending).await;
            }
            return;
        };
        let target = Known { sha256: winner.sha256.clone(), clock: winner.clock };
        if known.as_ref().is_some_and(|known| known.clock > winner.clock) {
            // We hold a newer write than the view shows. The view has not caught up.
            if self.unadopted.contains(path)
                && let Some(Known { sha256: Some(sha), clock }) = known
            {
                self.stage_file(path, &sha, clock, pending).await;
            }
            return;
        }
        if known.as_ref() == Some(&target) && !self.unadopted.contains(path) {
            return;
        }
        match winner.sha256 {
            Some(sha) if disk.as_deref() == Some(&sha) => self.stage_file(path, &sha, winner.clock, pending).await,
            Some(sha) => downloads.push(Download {
                path: path.to_owned(),
                sha256: sha,
                clock: winner.clock,
                tickets: winner.tickets,
                local_before: disk,
            }),
            None => {
                if let Some(local) = resolve(&self.root, path).filter(|local| file_sha(local) == known_sha)
                    && std::fs::remove_file(local).is_ok()
                {
                    eprintln!("↓ deleted {path}");
                }
                self.disk.remove(path);
                stage_tombstone(path, winner.clock, pending);
            }
        }
    }

    /// Stage a file for our slice, with a ticket from our own blob server.
    async fn stage_file(&self, path: &str, sha: &str, clock: u64, pending: &mut Pending) {
        let ticket = match self.blobs.register(&self.root.join(path), ContentId::new(sha)).await {
            Ok(ticket) => ticket.encode(),
            Err(error) => return eprintln!("publish {path} failed: {error}"),
        };
        // A merge sets keys only, so each entry nulls the keys of the other kind.
        let entry = json!({ "sha256": sha, "ticket": ticket, "clock": clock, "deleted": null });
        pending.insert(path.to_owned(), (entry, Known { sha256: Some(sha.to_owned()), clock }));
    }

    /// Fetch every file at once, at most `MAX_FETCHES` at a time. A file that lands is staged
    /// for our slice, because we now hold its bytes.
    async fn download(&mut self, downloads: Vec<Download>, pending: &mut Pending) {
        let limit = Arc::new(Semaphore::new(MAX_FETCHES));
        let mut fetches = JoinSet::new();
        for download in downloads {
            let limit = Arc::clone(&limit);
            fetches.spawn(async move {
                let _permit = limit.acquire_owned().await;
                let result = fetch_any(download.tickets.clone(), download.sha256.clone()).await;
                (download, result)
            });
        }
        while let Some(joined) = fetches.join_next().await {
            let Ok((download, result)) = joined else { continue };
            match result {
                Ok(bytes) => self.land(&download, &bytes, pending).await,
                Err(error) => eprintln!("download {} failed: {error:#}", download.path),
            }
        }
    }

    async fn land(&mut self, download: &Download, bytes: &[u8], pending: &mut Pending) {
        let Some(local) = resolve(&self.root, &download.path) else {
            return eprintln!("unsafe path {:?}", download.path);
        };
        if file_sha(&local) != download.local_before {
            return;
        }
        if let Some(parent) = local.parent()
            && let Err(error) = tokio::fs::create_dir_all(parent).await
        {
            return eprintln!("download {} failed: {error}", download.path);
        }
        if let Err(error) = write_atomic(&local, bytes).await {
            return eprintln!("download {} failed: {error:#}", download.path);
        }
        eprintln!("↓ {}", download.path);
        self.disk.insert(download.path.clone(), download.sha256.clone());
        self.stage_file(&download.path, &download.sha256, download.clock, pending).await;
    }

    /// Write every staged change into our slice with 1 merge.
    async fn write(&mut self, pending: Pending) {
        if pending.is_empty() {
            return;
        }
        let entries: BTreeSet<&String> = self.known.keys().chain(pending.keys()).collect();
        let bytes: usize = entries.iter().map(|path| path.len() + ENTRY_BYTES).sum();
        if bytes > SLICE_BYTES {
            if !self.capped {
                eprintln!("the folder has too many files for 1 slice: publishing stops, downloads go on");
            }
            self.capped = true;
            return;
        }
        self.capped = false;
        let files: serde_json::Map<String, Value> =
            pending.iter().map(|(path, (entry, _))| (path.clone(), entry.clone())).collect();
        if let Err(error) = self.merge(json!({ "files": files })).await {
            return eprintln!("publish failed: {error:#}");
        }
        for (path, (_, known)) in pending {
            eprintln!("↑ {}{path}", if known.sha256.is_none() { "deleted " } else { "" });
            self.unadopted.remove(&path);
            self.known.insert(path, known);
        }
        self.save_known();
    }

    async fn merge(&self, merge: Value) -> Result<()> {
        self.member
            .request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply }))
            .await
            .map_err(|error| anyhow!("{error}"))?
            .map_err(|error| anyhow!("{error}"))
    }

    async fn document(&self) -> Option<Value> {
        let text = self.member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await.ok()?;
        serde_json::from_str(&text).ok()
    }

    fn save_known(&self) {
        let Some(file) = &self.known_file else { return };
        let known: serde_json::Map<String, Value> = self
            .known
            .iter()
            .map(|(path, known)| (path.clone(), json!({ "sha256": known.sha256, "clock": known.clock })))
            .collect();
        let saved = file
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| std::fs::write(file, Value::Object(known).to_string()));
        if let Err(error) = saved {
            eprintln!("could not save {}: {error}", file.display());
        }
    }
}

fn stage_tombstone(path: &str, clock: u64, pending: &mut Pending) {
    let entry = json!({ "deleted": true, "clock": clock, "sha256": null, "ticket": null });
    pending.insert(path.to_owned(), (entry, Known { sha256: None, clock }));
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
    let mut bytes = Vec::new();
    tokio::time::timeout(FETCH_TIMEOUT, blob::fetch(&ticket, &mut bytes, None))
        .await
        .context("fetch timed out")?
        .map_err(|error| anyhow!("{error}"))?;
    if sha256_bytes(&bytes) != sha256 {
        bail!("content does not match its sha256");
    }
    Ok(bytes)
}

/// Where `known` of `root` lives. `None` if there is no home folder.
fn known_file(root: &Path) -> Option<PathBuf> {
    let home = match std::env::var("AGENT_KNOWLEDGE_HOME").ok().filter(|dir| !dir.is_empty()) {
        Some(home) => PathBuf::from(home),
        None => PathBuf::from(std::env::var("HOME").ok().filter(|dir| !dir.is_empty())?).join(".agent-knowledge"),
    };
    Some(home.join("known").join(format!("{}.json", sha256_bytes(root.to_string_lossy().as_bytes()))))
}

fn load_known(file: Option<&Path>) -> BTreeMap<String, Known> {
    let Some(Ok(text)) = file.map(std::fs::read_to_string) else { return BTreeMap::new() };
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
