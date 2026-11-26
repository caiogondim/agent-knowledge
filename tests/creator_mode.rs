mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use agent_knowledge::writers::{CreatorKey, Writers};
use common::{Folder, Peer, wait_for};

// Long enough for an entry to cross the mesh several times, so a missing file is not a slow file.
const SETTLE: Duration = Duration::from_secs(6);

const CREATOR_V1: &str = "creator v1";

fn write(dir: &Folder, name: &str, text: &str) {
    std::fs::write(dir.path().join(name), text).unwrap();
}

fn read(dir: &Folder, name: &str) -> Option<String> {
    std::fs::read_to_string(dir.path().join(name)).ok()
}

fn mesh_of(hash: &str) -> &str {
    hash.split_once('.').expect("a creator hash has a suffix").0
}

/// A creator share on `creator_dir` with one file, and one mounter for each of `mounter_dirs`.
async fn creator_mesh(creator_dir: &Folder, mounter_dirs: &[&Folder]) -> (String, Peer, Vec<Peer>) {
    write(creator_dir, "base.md", CREATOR_V1);
    let (creator, hash) = Peer::share_as(creator_dir, Writers::Creator).await;
    let mounters: Vec<Peer> = mounter_dirs.iter().map(|dir| Peer::mount(dir, &hash)).collect();
    for dir in mounter_dirs {
        wait_for(&dir.path().join("base.md"), Some(CREATOR_V1)).await;
    }
    (hash, creator, mounters)
}

async fn stop_all(peers: Vec<Peer>) {
    for peer in peers {
        peer.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_creator_write_is_not_applied_on_other_peers() {
    let (c_dir, a_dir, b_dir) = (Folder::new(), Folder::new(), Folder::new());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir, &b_dir]).await;

    write(&a_dir, "new.md", "from a");
    write(&a_dir, "base.md", "edited by a");
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&b_dir, "new.md"), None);
    assert_eq!(read(&b_dir, "base.md").as_deref(), Some(CREATOR_V1));
    assert_eq!(read(&c_dir, "new.md"), None);
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));

    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_folder_syncs_to_a_new_mounter_after_the_creator_leaves() {
    let (c_dir, a_dir, b_dir) = (Folder::new(), Folder::new(), Folder::new());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;

    creator.stop().await;
    mounters.push(Peer::mount(&b_dir, &hash));
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rehoster_that_restarts_after_the_creator_left_still_serves() {
    let (c_dir, a_dir, b_dir) = (Folder::new(), Folder::new(), Folder::new());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;

    creator.stop().await;
    mounters.remove(0).stop().await;
    let a = Peer::mount(&a_dir, &hash);
    let b = Peer::mount(&b_dir, &hash);
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    tokio::join!(a.stop(), b.stop());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_creator_edit_is_reverted_and_the_next_creator_change_applies() {
    let (c_dir, a_dir, b_dir) = (Folder::new(), Folder::new(), Folder::new());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir, &b_dir]).await;

    write(&a_dir, "base.md", "edited by a");
    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    write(&c_dir, "base.md", "creator v2");
    wait_for(&a_dir.path().join("base.md"), Some("creator v2")).await;
    wait_for(&b_dir.path().join("base.md"), Some("creator v2")).await;

    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_creator_delete_is_reverted() {
    let (c_dir, a_dir) = (Folder::new(), Folder::new());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir]).await;

    std::fs::remove_file(a_dir.path().join("base.md")).unwrap();
    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));

    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mount_with_a_malformed_creator_suffix_fails() {
    let (c_dir, target) = (Folder::new(), tempfile::tempdir().unwrap());
    let home = tempfile::tempdir().unwrap();
    write(&c_dir, "base.md", CREATOR_V1);
    let (creator, hash) = Peer::share_as(&c_dir, Writers::Creator).await;

    let output = mount_output(&format!("{}.zz", mesh_of(&hash)), target.path(), home.path());

    assert!(!output.status.success(), "mount joined with a malformed suffix");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("the creator key in the hash is not 64 lowercase hex digits of an Ed25519 key"),
        "unexpected error: {stderr}"
    );
    creator.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mount_with_another_creator_key_applies_nothing() {
    let (c_dir, a_dir, b_dir) = (Folder::new(), Folder::new(), Folder::new());
    write(&c_dir, "base.md", CREATOR_V1);
    let (creator, hash) = Peer::share_as(&c_dir, Writers::Creator).await;
    let other_key = CreatorKey::generate().unwrap().public_hex();

    // A mounts the true hash at the same time as B. When A has the file, the mesh has delivered it.
    let a = Peer::mount(&a_dir, &hash);
    let b = Peer::mount(&b_dir, &format!("{}.{other_key}", mesh_of(&hash)));
    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&b_dir, "base.md"), None);
    creator.stop().await;
    tokio::join!(a.stop(), b.stop());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_peer_reverts_an_offline_edit_found_at_restart() {
    let (c_dir, a_dir) = (Folder::new(), Folder::new());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;
    mounters.remove(0).stop().await;

    write(&a_dir, "base.md", "offline edit on a");
    let a = Peer::mount(&a_dir, &hash);

    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));
    creator.stop().await;
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mounter_keeps_its_own_file_at_a_tombstoned_path() {
    let (c_dir, a_dir, b_dir) = (Folder::new(), Folder::new(), Folder::new());
    let (hash, creator, mounters) = creator_mesh(&c_dir, &[&a_dir]).await;
    std::fs::remove_file(c_dir.path().join("base.md")).unwrap();
    wait_for(&a_dir.path().join("base.md"), None).await;

    write(&b_dir, "base.md", "mine");
    let b = Peer::mount(&b_dir, &hash);
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&b_dir, "base.md").as_deref(), Some("mine"));
    creator.stop().await;
    b.stop().await;
    stop_all(mounters).await;
}

fn mount_output(hash: &str, target: &Path, home: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_agent-knowledge"))
        .args(["mount", hash])
        .arg(target)
        .env("AGENT_KNOWLEDGE_HOME", home)
        .output()
        .unwrap()
}
