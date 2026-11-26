// Each test binary compiles this module and uses only part of it.
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use agent_knowledge::sync::{self, Target};
use agent_knowledge::writers::Writers;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

pub const WAIT_LIMIT: Duration = Duration::from_secs(30);
pub const WAIT_STEP: Duration = Duration::from_millis(200);

pub async fn wait_for(path: &Path, want: Option<&str>) {
    let deadline = tokio::time::Instant::now() + WAIT_LIMIT;
    loop {
        let got = std::fs::read_to_string(path).ok();
        if got.as_deref() == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{} is {got:?}, want {want:?}", path.display());
        tokio::time::sleep(WAIT_STEP).await;
    }
}

/// A synced folder and its own `known_dir`. Both live as long as the test, so a peer that
/// restarts on the folder finds the state that it saved.
pub struct Folder {
    root: tempfile::TempDir,
    known: tempfile::TempDir,
}

impl Folder {
    pub fn new() -> Self {
        Self { root: tempfile::tempdir().unwrap(), known: tempfile::tempdir().unwrap() }
    }

    pub fn path(&self) -> &Path {
        self.root.path()
    }
}

/// One `sync::run` loop on one folder, with its own stop signal.
pub struct Peer {
    stop: watch::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Peer {
    /// Shares `dir` and returns the peer with the hash that other peers mount.
    pub async fn share(folder: &Folder) -> (Self, String) {
        Self::share_as(folder, Writers::Everybody).await
    }

    /// Like `share`, with the writer rule of the share.
    pub async fn share_as(folder: &Folder, writers: Writers) -> (Self, String) {
        let (hash_tx, hash_rx) = oneshot::channel();
        let peer = Self::spawn(folder, Target::Create(writers), move |hash: &str| hash_tx.send(hash.to_owned()).unwrap());
        (peer, hash_rx.await.unwrap())
    }

    pub fn mount(folder: &Folder, hash: &str) -> Self {
        Self::spawn(folder, Target::Join(hash.to_owned()), |_: &str| {})
    }

    fn spawn(folder: &Folder, target: Target, on_ready: impl FnOnce(&str) + Send + 'static) -> Self {
        let (stop, mut stop_rx) = watch::channel(());
        let stopped = async move {
            let _ = stop_rx.changed().await;
        };
        let known_dir = folder.known.path().to_path_buf();
        let task = tokio::spawn(sync::run(folder.path().to_path_buf(), target, known_dir, on_ready, stopped));
        Self { stop, task }
    }

    /// Graceful leave: the loop sees its stop signal and must end without an error.
    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}

/// `agent-knowledge share` as a child process, so a kill takes its blob server down with it.
/// An in-process abort leaves the blob server of the peer alive in the test process.
pub struct ShareProcess {
    child: Child,
    _home: tempfile::TempDir,
}

impl ShareProcess {
    /// Starts the share on `dir` and returns it with the hash that it prints.
    pub async fn start(dir: &Path) -> (Self, String) {
        let home = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_agent-knowledge"))
            .arg("share")
            .arg(dir)
            .env("AGENT_KNOWLEDGE_HOME", home.path())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let share = Self { child, _home: home };
        let read_hash = tokio::task::spawn_blocking(move || {
            let mut hash = String::new();
            BufReader::new(stdout).read_line(&mut hash).map(|_| hash.trim().to_owned())
        });
        let hash = tokio::time::timeout(WAIT_LIMIT, read_hash).await.expect("share printed no hash").unwrap().unwrap();
        assert!(!hash.is_empty(), "share exited before it printed a hash");
        (share, hash)
    }

    /// SIGKILL: the process sends no leave and its blob server is gone.
    pub fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for ShareProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts one peer per folder on one mesh. The first folder shares, the rest mount its hash.
pub async fn start_mesh<const N: usize>(folders: [&Folder; N]) -> (String, [Peer; N]) {
    let (first, rest) = folders.split_first().expect("a mesh needs at least 1 folder");
    let (share, hash) = Peer::share(first).await;
    let peers: Vec<Peer> =
        std::iter::once(share).chain(rest.iter().map(|folder| Peer::mount(folder, &hash))).collect();
    let Ok(peers) = peers.try_into() else { unreachable!("one peer per folder") };
    (hash, peers)
}

/// A `share` loop on `share_dir` and a `mount` loop on `mount_dir`, joined by the share's hash.
pub struct Pair {
    peers: [Peer; 2],
}

impl Pair {
    pub async fn start(share_dir: &Folder, mount_dir: &Folder) -> Self {
        let (_, peers) = start_mesh([share_dir, mount_dir]).await;
        Self { peers }
    }

    pub async fn stop(self) {
        let [share, mount] = self.peers;
        tokio::join!(share.stop(), mount.stop());
    }
}
