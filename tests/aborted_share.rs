mod common;

use common::{Folder, Peer, ShareProcess, wait_for};

#[tokio::test(flavor = "multi_thread")]
async fn file_survives_after_share_is_killed() {
    let a_dir = Folder::new();
    let b_dir = Folder::new();
    let c_dir = Folder::new();
    std::fs::write(a_dir.path().join("kept.md"), "from a").unwrap();

    let (a, hash) = ShareProcess::start(a_dir.path()).await;
    let b = Peer::mount(&b_dir, &hash);
    wait_for(&b_dir.path().join("kept.md"), Some("from a")).await;

    a.kill();
    let c = Peer::mount(&c_dir, &hash);
    wait_for(&c_dir.path().join("kept.md"), Some("from a")).await;

    tokio::join!(b.stop(), c.stop());
}
