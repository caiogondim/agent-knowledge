mod common;

use common::{start_mesh, wait_for};

const FILE_COUNT: usize = 25;
const EDIT_COUNT: usize = 10;

fn name(i: usize) -> String {
    format!("note-{i:02}.md")
}

#[tokio::test(flavor = "multi_thread")]
async fn many_files_flow_both_ways() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    for i in 0..FILE_COUNT {
        std::fs::write(a_dir.path().join(name(i)), format!("a wrote {i}")).unwrap();
    }

    let (_, [a, b]) = start_mesh([a_dir.path(), b_dir.path()]).await;
    for i in 0..FILE_COUNT {
        wait_for(&b_dir.path().join(name(i)), Some(&format!("a wrote {i}"))).await;
    }

    for i in 0..EDIT_COUNT {
        std::fs::write(b_dir.path().join(name(i)), format!("b edited {i}")).unwrap();
    }
    for i in 0..EDIT_COUNT {
        wait_for(&a_dir.path().join(name(i)), Some(&format!("b edited {i}"))).await;
    }
    for i in EDIT_COUNT..FILE_COUNT {
        wait_for(&a_dir.path().join(name(i)), Some(&format!("a wrote {i}"))).await;
    }

    tokio::join!(a.stop(), b.stop());
}
