mod common;

use common::{Folder, Peer, start_mesh, wait_for};

// Both peers reach clock >= 3 on one path, both stop, the share edits the file offline,
// then both start again. The offline edit must win.
#[tokio::test(flavor = "multi_thread")]
async fn offline_edit_survives_restart_of_both() {
    let a_dir = Folder::new();
    let b_dir = Folder::new();
    let (a_file, b_file) = (a_dir.path().join("f.md"), b_dir.path().join("f.md"));
    std::fs::write(&a_file, "v1").unwrap();
    let (_, [a, b]) = start_mesh([&a_dir, &b_dir]).await;
    wait_for(&b_file, Some("v1")).await;
    std::fs::write(&a_file, "v2").unwrap();
    wait_for(&b_file, Some("v2")).await;
    std::fs::write(&b_file, "v3").unwrap();
    wait_for(&a_file, Some("v3")).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    tokio::join!(a.stop(), b.stop());

    std::fs::write(&a_file, "offline edit on a").unwrap();
    let (a, hash) = Peer::share(&a_dir).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let b = Peer::mount(&b_dir, &hash);
    wait_for(&b_file, Some("offline edit on a")).await;
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert_eq!(std::fs::read_to_string(&a_file).unwrap(), "offline edit on a");
    tokio::join!(a.stop(), b.stop());
}
