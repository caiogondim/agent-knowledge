// Each test binary compiles this module and uses only part of it.
#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use agent_knowledge::sync;
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

/// One `sync::run` loop on one folder, with its own stop signal.
pub struct Peer {
    stop: watch::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Peer {
    /// Shares `dir` and returns the peer with the hash that other peers mount.
    pub async fn share(dir: &Path) -> (Self, String) {
        let (hash_tx, hash_rx) = oneshot::channel();
        let peer = Self::spawn(dir, None, move |hash: &str| hash_tx.send(hash.to_owned()).unwrap());
        (peer, hash_rx.await.unwrap())
    }

    pub fn mount(dir: &Path, hash: &str) -> Self {
        Self::spawn(dir, Some(hash.to_owned()), |_: &str| {})
    }

    fn spawn(dir: &Path, mesh: Option<String>, on_ready: impl FnOnce(&str) + Send + 'static) -> Self {
        let (stop, mut stop_rx) = watch::channel(());
        let stopped = async move {
            let _ = stop_rx.changed().await;
        };
        let task = tokio::spawn(sync::run(dir.to_path_buf(), mesh, on_ready, stopped));
        Self { stop, task }
    }

    /// Graceful leave: the loop sees its stop signal and must end without an error.
    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }

    /// Crash: the loop is dropped at its next await point and sends no leave.
    pub async fn abort(self) {
        self.task.abort();
        assert!(self.task.await.unwrap_err().is_cancelled());
    }
}

/// Starts one peer per folder on one mesh. The first folder shares, the rest mount its hash.
pub async fn start_mesh<const N: usize>(dirs: [&Path; N]) -> (String, [Peer; N]) {
    let (first, rest) = dirs.split_first().expect("a mesh needs at least 1 folder");
    let (share, hash) = Peer::share(first).await;
    let peers: Vec<Peer> = std::iter::once(share).chain(rest.iter().map(|dir| Peer::mount(dir, &hash))).collect();
    let Ok(peers) = peers.try_into() else { unreachable!("one peer per folder") };
    (hash, peers)
}

/// A `share` loop on `share_dir` and a `mount` loop on `mount_dir`, joined by the share's hash.
pub struct Pair {
    peers: [Peer; 2],
}

impl Pair {
    pub async fn start(share_dir: &Path, mount_dir: &Path) -> Self {
        let (_, peers) = start_mesh([share_dir, mount_dir]).await;
        Self { peers }
    }

    pub async fn stop(self) {
        let [share, mount] = self.peers;
        tokio::join!(share.stop(), mount.stop());
    }
}
