use super::*;
#[cfg(unix)]
use std::process::Command;
#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use tempfile::{TempDir, tempdir};

#[test]
fn destination_url_encodes_each_component_and_rejects_escapes() {
    let root = "file:///data/dst/inner/";
    assert_eq!(
        destination_url(root, Path::new("a/b c/d.txt")).expect("ok"),
        "file:///data/dst/inner/a/b%20c/d.txt"
    );
    assert_eq!(
        destination_url(root, Path::new("%2e%2e/x")).expect("ok"),
        "file:///data/dst/inner/%252e%252e/x"
    );
    for bad in ["../x", "/abs", "a/../../x", ""] {
        assert!(
            matches!(
                destination_url(root, Path::new(bad)),
                Err(CliError::UnsafePath { .. })
            ),
            "{bad:?}"
        );
    }
}

#[cfg(unix)]
fn assert_refused(res: &Result<tokio::fs::File, CliError>) {
    assert!(matches!(res, Err(CliError::UnsafePath { .. })), "{res:?}");
}

/// A tree with `src/top.txt`, `src/a/b/deep.txt` and `src/sib/s.txt`, and an
/// outside directory with a secret.
#[cfg(unix)]
fn walk_fixture() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let w = tempdir().expect("work");
    let src = w.path().join("src");
    std::fs::create_dir_all(src.join("a/b")).expect("dirs");
    std::fs::create_dir_all(src.join("sib")).expect("sib");
    std::fs::write(src.join("top.txt"), b"top").expect("top");
    std::fs::write(src.join("a/b/deep.txt"), b"deep").expect("deep");
    std::fs::write(src.join("sib/s.txt"), b"s").expect("s");
    let outside = w.path().join("outside");
    std::fs::create_dir_all(outside.join("b")).expect("outside");
    std::fs::write(outside.join("b/secret.txt"), b"secret").expect("secret");
    (w, src, outside)
}

/// Replaces the directory `dir` with a symlink to `target`.
#[cfg(unix)]
fn swap_for_symlink(dir: &Path, target: &Path) {
    let moved = dir.with_extension("moved");
    std::fs::rename(dir, moved).expect("move");
    std::os::unix::fs::symlink(target, dir).expect("symlink");
}

#[cfg(unix)]
#[tokio::test]
async fn scan_lists_a_nested_tree_and_opens_its_files() {
    use tokio::io::AsyncReadExt;
    let (_w, src, _outside) = walk_fixture();
    let (tree, files) = SourceTree::scan(&src).expect("scan");
    let mut rels: Vec<_> = files.iter().map(|f| f.rel.clone()).collect();
    rels.sort();
    assert_eq!(
        rels,
        [
            Path::new("a/b/deep.txt"),
            Path::new("sib/s.txt"),
            Path::new("top.txt")
        ]
    );
    for f in &files {
        let mut got = Vec::new();
        let mut file = tree.open(f).expect("open");
        file.read_to_end(&mut got).await.expect("read");
        assert_eq!(got, std::fs::read(src.join(&f.rel)).expect("expected"));
    }
}

#[cfg(unix)]
#[test]
fn walk_stops_when_the_consumer_stops() {
    let (_w, src, _outside) = walk_fixture();
    let tree = SourceTree::open_root(&src).expect("open");
    let (mut emitted, mut opened) = (Vec::new(), 0);
    tree.walk(&mut |_| opened += 1, &mut |file| {
        emitted.push(file.rel);
        ControlFlow::Break(())
    })
    .expect("a stopped walk is not an error");
    // `top.txt` is the only file at the root and is listed before any
    // subdirectory is opened.
    assert_eq!(emitted, [Path::new("top.txt")]);
    assert_eq!(opened, 0, "no subdirectory was opened");

    // Without the stop the same walk reaches the subdirectories and all files.
    let mut all = 0;
    tree.walk(&mut |_| opened += 1, &mut |_| {
        all += 1;
        ControlFlow::Continue(())
    })
    .expect("walk");
    assert_eq!((all, opened), (3, 3));
}

/// The scanned entry for `top.txt` of [`walk_fixture`].
#[cfg(unix)]
fn scan_top(src: &Path) -> (SourceTree, TreeFile) {
    let (tree, files) = SourceTree::scan(src).expect("scan");
    let top = files
        .into_iter()
        .find(|f| f.rel == Path::new("top.txt"))
        .expect("top.txt");
    (tree, top)
}

#[cfg(unix)]
#[test]
fn open_refuses_a_file_replaced_by_a_symlink() {
    // Replaced by a symlink to a file outside the tree.
    let (_w, src, outside) = walk_fixture();
    let (tree, top) = scan_top(&src);
    std::fs::remove_file(src.join("top.txt")).expect("rm");
    std::os::unix::fs::symlink(outside.join("b/secret.txt"), src.join("top.txt")).expect("link");
    assert_refused(&tree.open(&top));

    // Renamed away and replaced by a symlink to its new place: the inode is the
    // same, so only O_NOFOLLOW refuses it.
    let (_w, src, _outside) = walk_fixture();
    let (tree, top) = scan_top(&src);
    std::fs::rename(src.join("top.txt"), src.join("moved.txt")).expect("move");
    std::os::unix::fs::symlink(src.join("moved.txt"), src.join("top.txt")).expect("link");
    assert_refused(&tree.open(&top));
}

#[cfg(unix)]
#[test]
fn open_refuses_a_file_replaced_by_another_file() {
    let (_w, src, _outside) = walk_fixture();
    let (tree, top) = scan_top(&src);
    // Written while top.txt exists, so it has a different inode.
    std::fs::write(src.join("other.txt"), b"other").expect("other");
    std::fs::rename(src.join("other.txt"), src.join("top.txt")).expect("replace");
    assert_refused(&tree.open(&top));
}

#[cfg(unix)]
#[test]
fn open_refuses_a_fifo_without_hanging() {
    let (_w, src, _outside) = walk_fixture();
    let (tree, top) = scan_top(&src);
    std::fs::remove_file(src.join("top.txt")).expect("rm");
    let status = Command::new("mkfifo")
        .arg(src.join("top.txt"))
        .status()
        .expect("mkfifo");
    assert!(status.success());
    // A blocking open of a FIFO without a writer never returns, so the open runs
    // on a thread and the test waits for it.
    let (sent, done) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sent.send(tree.open(&top).map(drop));
    });
    let res = done
        .recv_timeout(Duration::from_secs(5))
        .expect("open returned within 5 s");
    assert!(matches!(res, Err(CliError::UnsafePath { .. })), "{res:?}");
}

#[cfg(unix)]
#[test]
fn scan_never_reads_a_directory_swapped_for_a_symlink_after_listing() {
    let (_w, src, outside) = walk_fixture();
    let a = src.join("a");
    let res = SourceTree::scan_with_hook(&src, &mut |dir| {
        if dir == a {
            swap_for_symlink(&a, &outside);
        }
    });
    assert!(
        matches!(res, Err(CliError::UnsafePath { .. })),
        "{:?}",
        res.err()
    );
}

#[cfg(unix)]
#[test]
fn scan_never_reads_a_nested_directory_swapped_after_listing() {
    let (_w, src, outside) = walk_fixture();
    let b = src.join("a/b");
    let res = SourceTree::scan_with_hook(&src, &mut |dir| {
        if dir == b {
            swap_for_symlink(&b, &outside.join("b"));
        }
    });
    assert!(
        matches!(res, Err(CliError::UnsafePath { .. })),
        "{:?}",
        res.err()
    );
}

#[cfg(unix)]
#[test]
fn open_refuses_an_intermediate_directory_swapped_after_the_scan() {
    let (_w, src, outside) = walk_fixture();
    let (tree, files) = SourceTree::scan(&src).expect("scan");
    let deep = files
        .iter()
        .find(|f| f.rel == Path::new("a/b/deep.txt"))
        .expect("deep");
    swap_for_symlink(&src.join("a"), &outside);
    assert_refused(&tree.open(deep));
}

#[cfg(unix)]
#[test]
fn open_refuses_a_directory_replaced_by_another_directory() {
    let (_w, src, _outside) = walk_fixture();
    let (tree, files) = SourceTree::scan(&src).expect("scan");
    let deep = files
        .iter()
        .find(|f| f.rel == Path::new("a/b/deep.txt"))
        .expect("deep");
    // A look-alike directory, not a symlink: the identity no longer matches.
    std::fs::rename(src.join("a"), src.join("old")).expect("move");
    std::fs::create_dir_all(src.join("a/b")).expect("lookalike");
    std::fs::write(src.join("a/b/deep.txt"), b"fake").expect("fake");
    assert_refused(&tree.open(deep));
}
