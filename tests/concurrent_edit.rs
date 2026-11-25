mod common;

use std::path::Path;
use std::time::Duration;

use common::{Folder, WAIT_LIMIT, WAIT_STEP, start_mesh, wait_for};

const STABLE_FOR: Duration = Duration::from_secs(3);

fn read(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_converge_on_one_write() {
    let a_dir = Folder::new();
    let b_dir = Folder::new();
    std::fs::write(a_dir.path().join("ready.md"), "linked").unwrap();

    let (_, [a, b]) = start_mesh([&a_dir, &b_dir]).await;
    wait_for(&b_dir.path().join("ready.md"), Some("linked")).await;

    let a_path = a_dir.path().join("same.md");
    let b_path = b_dir.path().join("same.md");
    std::fs::write(&a_path, "written by a").unwrap();
    std::fs::write(&b_path, "written by b").unwrap();
    let writes = [Some("written by a".to_owned()), Some("written by b".to_owned())];

    let deadline = tokio::time::Instant::now() + WAIT_LIMIT;
    let winner = loop {
        let (in_a, in_b) = (read(&a_path), read(&b_path));
        if in_a == in_b && writes.contains(&in_a) {
            break in_a;
        }
        assert!(tokio::time::Instant::now() < deadline, "a has {in_a:?}, b has {in_b:?}, want one of {writes:?} in both");
        tokio::time::sleep(WAIT_STEP).await;
    };

    let stable_until = tokio::time::Instant::now() + STABLE_FOR;
    while tokio::time::Instant::now() < stable_until {
        tokio::time::sleep(WAIT_STEP).await;
        assert_eq!(read(&a_path), winner, "a changed after the folders converged");
        assert_eq!(read(&b_path), winner, "b changed after the folders converged");
    }

    tokio::join!(a.stop(), b.stop());
}
