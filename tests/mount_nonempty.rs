mod common;

use common::{Folder, start_mesh, wait_for};

// A mount into a folder that already holds another version of a shared path.
// The mesh wins: its version overwrites the local copy at join.
#[tokio::test(flavor = "multi_thread")]
async fn mesh_wins_on_mount_into_nonempty_folder() {
    let a_dir = Folder::new();
    let b_dir = Folder::new();
    let (a_file, b_file) = (a_dir.path().join("f.md"), b_dir.path().join("f.md"));
    std::fs::write(&a_file, "from the share").unwrap();
    std::fs::write(&b_file, "stale copy in the mount").unwrap();

    let (_, [a, b]) = start_mesh([&a_dir, &b_dir]).await;
    wait_for(&b_file, Some("from the share")).await;
    wait_for(&a_file, Some("from the share")).await;

    tokio::join!(a.stop(), b.stop());
}
