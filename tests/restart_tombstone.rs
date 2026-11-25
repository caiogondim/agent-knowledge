mod common;

use std::time::Duration;

use common::{Folder, Peer, start_mesh, wait_for};

const STAYS_DELETED_FOR: Duration = Duration::from_secs(4);

// A shares, B and C mount, and all 3 get f.md. C stops while it still holds f.md. B deletes
// f.md and A sees the delete. A stops, then B stops. B and C mount the same hash again: B with
// its saved tombstone, C with its saved copy. B must publish its saved tombstone again. It is
// above the clock of C's copy, so f.md must end deleted on C and stay deleted on B and C.
#[tokio::test(flavor = "multi_thread")]
async fn saved_tombstone_is_published_again_after_a_mount_restarts() {
    let a_dir = Folder::new();
    let b_dir = Folder::new();
    let c_dir = Folder::new();
    let (a_file, b_file, c_file) = (a_dir.path().join("f.md"), b_dir.path().join("f.md"), c_dir.path().join("f.md"));
    std::fs::write(&a_file, "v1").unwrap();

    let (hash, [a, b, c]) = start_mesh([&a_dir, &b_dir, &c_dir]).await;
    wait_for(&b_file, Some("v1")).await;
    wait_for(&c_file, Some("v1")).await;
    c.stop().await;

    std::fs::remove_file(&b_file).unwrap();
    wait_for(&a_file, None).await;
    a.stop().await;
    b.stop().await;

    let b = Peer::mount(&b_dir, &hash);
    let c = Peer::mount(&c_dir, &hash);
    wait_for(&c_file, None).await;
    tokio::time::sleep(STAYS_DELETED_FOR).await;
    assert!(!b_file.exists(), "b got the deleted file back");
    assert!(!c_file.exists(), "c got the deleted file back");

    tokio::join!(b.stop(), c.stop());
}
