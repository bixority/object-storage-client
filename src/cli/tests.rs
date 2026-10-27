use super::*;
use std::process::Command;
use tempfile::{TempDir, tempdir};

#[cfg(unix)]
#[tokio::test]
async fn temp_file_for_an_existing_0600_file_is_never_more_permissive() {
    use std::os::unix::fs::PermissionsExt;
    let w = tempdir().expect("work");
    let tmp = w.path().join("dst.bin.osc-tmp-test");

    let file = create_temp_file(&tmp, Some(0o600)).await.expect("create");
    let mode = file.metadata().await.expect("meta").permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "mode right after creation");
    assert_eq!(
        std::fs::metadata(&tmp).expect("meta").len(),
        0,
        "no data yet"
    );

    // Exclusive: an existing temp path is refused.
    create_temp_file(&tmp, Some(0o600))
        .await
        .expect_err("create_new");
}

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

/// Device and inode of `path`, as the directory walk records them.
#[cfg(unix)]
fn id_of(path: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::symlink_metadata(path).expect("meta");
    (m.dev(), m.ino())
}

#[cfg(unix)]
fn assert_refused(res: &Result<tokio::fs::File, CliError>) {
    assert!(matches!(res, Err(CliError::UnsafePath { .. })), "{res:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn open_verified_accepts_the_recorded_file() {
    use tokio::io::AsyncReadExt;
    let w = tempdir().expect("work");
    let f = w.path().join("f");
    std::fs::write(&f, b"hello").expect("f");
    let mut file = open_verified(&f, Some(id_of(&f))).expect("open");
    let mut got = String::new();
    file.read_to_string(&mut got).await.expect("read");
    assert_eq!(got, "hello");
}

#[cfg(unix)]
#[tokio::test]
async fn open_verified_refuses_a_file_replaced_by_a_symlink() {
    let w = tempdir().expect("work");
    let f = w.path().join("src/zz");
    std::fs::create_dir_all(f.parent().expect("parent")).expect("dir");
    std::fs::write(&f, b"inside").expect("f");
    let secret = w.path().join("secret");
    std::fs::write(&secret, b"secret").expect("secret");
    let recorded = id_of(&f);

    std::fs::remove_file(&f).expect("rm");
    std::os::unix::fs::symlink(&secret, &f).expect("symlink");
    assert_refused(&open_verified(&f, Some(recorded)));
}

#[cfg(unix)]
#[tokio::test]
async fn open_verified_refuses_a_file_replaced_by_another_file() {
    let w = tempdir().expect("work");
    let f = w.path().join("zz");
    std::fs::write(&f, b"one").expect("f");
    let recorded = id_of(&f);
    let other = w.path().join("other");
    std::fs::write(&other, b"two").expect("other");
    std::fs::rename(&other, &f).expect("swap");
    assert_refused(&open_verified(&f, Some(recorded)));
}

#[cfg(unix)]
#[tokio::test]
async fn open_verified_refuses_a_directory_replaced_by_a_symlink() {
    let w = tempdir().expect("work");
    let sub = w.path().join("src/sub");
    std::fs::create_dir_all(&sub).expect("dir");
    std::fs::write(sub.join("zz"), b"inside").expect("f");
    let recorded = id_of(&sub.join("zz"));

    let outside = w.path().join("outside");
    std::fs::create_dir(&outside).expect("outside");
    std::fs::write(outside.join("zz"), b"secret").expect("secret");
    std::fs::rename(&sub, w.path().join("moved")).expect("move");
    std::os::unix::fs::symlink(&outside, &sub).expect("symlink");
    assert_refused(&open_verified(&sub.join("zz"), Some(recorded)));
}

#[cfg(unix)]
#[tokio::test]
async fn open_verified_refuses_a_fifo_without_hanging() {
    let w = tempdir().expect("work");
    let f = w.path().join("zz");
    std::fs::write(&f, b"x").expect("f");
    let recorded = id_of(&f);
    std::fs::remove_file(&f).expect("rm");
    let status = Command::new("mkfifo").arg(&f).status().expect("mkfifo");
    assert!(status.success());
    assert_refused(&open_verified(&f, Some(recorded)));
    // Even with a matching identity a FIFO is no regular file.
    assert_refused(&open_verified(&f, Some(id_of(&f))));
}

#[test]
fn download_modes_adopts_the_mode_only_of_a_file_we_own() {
    // Owned by us: the mode is kept, special bits dropped.
    assert_eq!(
        download_modes(Some((0o100_666, 1000)), 1000, Some(0o022)),
        (Some(0o666), Some(0o666))
    );
    assert_eq!(
        download_modes(Some((0o106_777, 1000)), 1000, Some(0o022)),
        (Some(0o777), Some(0o777))
    );
    assert_eq!(
        download_modes(Some((0o104_600, 1000)), 1000, None),
        (Some(0o600), Some(0o600))
    );
    // Owned by someone else: like a new file, private first and the umask
    // default at the end, whatever the foreign file allowed.
    assert_eq!(
        download_modes(Some((0o106_666, 1001)), 1000, Some(0o027)),
        (Some(0o600), Some(0o640))
    );
    assert_eq!(
        download_modes(Some((0o101_777, 0)), 1000, Some(0o022)),
        (Some(0o600), Some(0o644))
    );
    assert_eq!(
        download_modes(Some((0o100_666, 1001)), 1000, None),
        (None, None)
    );
    // New file.
    assert_eq!(
        download_modes(None, 1000, Some(0o077)),
        (Some(0o600), Some(0o600))
    );
    assert_eq!(download_modes(None, 1000, None), (None, None));
}

#[test]
fn redact_url_drops_secrets() {
    assert_eq!(
        redact_url("https://user:secret@host.example/a/b?X-Amz-Signature=abc#frag"),
        "https://user@host.example/a/b?…"
    );
    assert_eq!(redact_url("s3://bucket/key"), "s3://bucket/key");
    assert_eq!(redact_url("https://h/x?sig=1"), "https://h/x?…");
    assert_eq!(redact_url("https://:pw@h/x"), "https://h/x");
    // Not parseable as a URL: still cut.
    let odd = redact_url("http://u:pw@[bad/x?token=1");
    assert!(!odd.contains("pw") && !odd.contains("token"), "{odd}");
}

#[test]
fn temp_file_name_stays_within_name_max() {
    use std::ffi::OsStr;
    let suffix = "0123456789abcdef";
    assert_eq!(
        temp_file_name(OsStr::new("dst.bin"), suffix),
        "dst.bin.osc-tmp-0123456789abcdef"
    );
    for name in ["a".repeat(400), "é".repeat(200), "€".repeat(100)] {
        let tmp = temp_file_name(OsStr::new(&name), suffix);
        let tmp = tmp.to_str().expect("utf-8");
        assert!(tmp.len() <= 255, "{}", tmp.len());
        assert!(tmp.ends_with(".osc-tmp-0123456789abcdef"));
        assert!(tmp.starts_with('a') || tmp.starts_with('é') || tmp.starts_with('€'));
    }
    let exact = "a".repeat(255 - 25);
    assert_eq!(
        temp_file_name(OsStr::new(&exact), suffix).len(),
        255,
        "a name that fits is kept whole"
    );
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

#[cfg(all(unix, feature = "encryption"))]
mod keyring_command {
    use super::super::keyring::run_command;
    use super::*;
    use std::ffi::OsStr;
    use std::time::Instant;

    const LONG: Duration = Duration::from_secs(30);

    fn run(command: &str, timeout: Duration) -> Result<crate::encryption::Keyring, CliError> {
        run_command(OsStr::new(command), timeout)
    }

    #[test]
    fn a_command_that_prints_a_keyring_is_parsed() {
        let text =
            crate::encryption::new_keyring("kek", chrono::NaiveDate::default()).expect("keyring");
        let dir = tempdir().expect("dir");
        let file = dir.path().join("ring");
        std::fs::write(&file, text.as_bytes()).expect("write");
        let ring = run(&format!("cat '{}'", file.display()), LONG).expect("run");
        assert_eq!(ring.status().keys.len(), 1);
    }

    #[test]
    fn a_command_that_outlives_the_timeout_is_killed() {
        let start = Instant::now();
        let err = run("sleep 20", Duration::from_millis(200)).expect_err("timeout");
        assert!(matches!(err, CliError::KeyringCommandTimeout(_)), "{err:?}");
        assert!(start.elapsed() < Duration::from_secs(10));

        // One that prints and then keeps running is also cut off.
        let err =
            run("echo osc-keyring-v1; sleep 20", Duration::from_millis(200)).expect_err("timeout");
        assert!(matches!(err, CliError::KeyringCommandTimeout(_)), "{err:?}");
    }

    #[test]
    fn a_failing_command_reports_its_status_not_its_output() {
        let err = run("echo hush; exit 7", LONG).expect_err("failed");
        let CliError::KeyringCommandStatus(status) = &err else {
            panic!("{err:?}");
        };
        assert!(status.contains('7'), "{status}");
        assert!(!err.to_string().contains("hush"));
    }

    #[test]
    fn too_much_output_and_bad_text_are_errors() {
        let err = run("head -c 65537 /dev/zero", LONG).expect_err("big");
        assert!(matches!(err, CliError::KeyringCommandTooLarge), "{err:?}");
        // Exactly the cap is read in full (and then fails to parse).
        let err = run("head -c 65536 /dev/zero | tr '\\0' ' '", LONG).expect_err("blank");
        assert!(matches!(err, CliError::Key(_)), "{err:?}");
        let err = run("printf '\\377'", LONG).expect_err("bytes");
        assert!(matches!(err, CliError::KeyringCommandNotUtf8), "{err:?}");
    }

    #[test]
    fn a_missing_command_is_a_status_error() {
        let err = run("exit 127", LONG).expect_err("missing");
        assert!(matches!(err, CliError::KeyringCommandStatus(_)), "{err:?}");
    }
}
