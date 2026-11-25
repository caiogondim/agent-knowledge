mod common;

use common::{Folder, Peer, start_mesh, wait_for};

// The mesh holds only a tombstone for f.md. C joins with its own f.md, which it never synced.
// The tombstone is not about C's file, so C keeps it, and A and B get it.
#[tokio::test(flavor = "multi_thread")]
async fn join_keeps_a_file_that_the_mesh_only_deleted() {
    let a_dir = Folder::new();
    let b_dir = Folder::new();
    let c_dir = Folder::new();
    let (a_file, b_file, c_file) = (a_dir.path().join("f.md"), b_dir.path().join("f.md"), c_dir.path().join("f.md"));
    std::fs::write(&a_file, "v1").unwrap();

    let (hash, [a, b]) = start_mesh([&a_dir, &b_dir]).await;
    wait_for(&b_file, Some("v1")).await;
    std::fs::remove_file(&a_file).unwrap();
    wait_for(&b_file, None).await;

    std::fs::write(&c_file, "c local").unwrap();
    let c = Peer::mount(&c_dir, &hash);
    wait_for(&a_file, Some("c local")).await;
    wait_for(&b_file, Some("c local")).await;
    assert_eq!(std::fs::read_to_string(&c_file).unwrap(), "c local");

    tokio::join!(a.stop(), b.stop(), c.stop());
}
