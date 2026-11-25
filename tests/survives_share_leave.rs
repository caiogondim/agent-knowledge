mod common;

use common::{Peer, start_mesh, wait_for};

#[tokio::test(flavor = "multi_thread")]
async fn file_survives_after_share_leaves() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let c_dir = tempfile::tempdir().unwrap();
    std::fs::write(a_dir.path().join("kept.md"), "from a").unwrap();

    let (hash, [a, b]) = start_mesh([a_dir.path(), b_dir.path()]).await;
    wait_for(&b_dir.path().join("kept.md"), Some("from a")).await;

    a.stop().await;
    let c = Peer::mount(c_dir.path(), &hash);
    wait_for(&c_dir.path().join("kept.md"), Some("from a")).await;

    tokio::join!(b.stop(), c.stop());
}
