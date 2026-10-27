//! Functional tests of the `osc` CLI over `file://` stores.

use chrono::NaiveDate;
use clap::{CommandFactory, FromArgMatches};
use object_storage_client::Error;
use object_storage_client::cli::{Cli, CliError};
use object_storage_client::encryption::{Keyring, new_keyring, rotate_keyring};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::{TempDir, tempdir};

const OSC: &str = env!("CARGO_BIN_EXE_osc");

/// Runs the CLI in-process.
async fn osc(args: &[&str]) -> Result<(), CliError> {
    let full = std::iter::once("osc").chain(args.iter().copied());
    // Without the env binding, so an exported OSC_KEYRING_FILE cannot leak in.
    let m = Cli::command()
        .mut_arg("keyring", |a| a.env(None))
        .try_get_matches_from(full)
        .expect("arguments parse");
    Cli::from_arg_matches(&m).expect("arguments").run().await
}

/// Runs the binary, with the key source variables cleared unless `env` sets
/// them.
fn osc_bin(args: &[&str], env: &[(&str, &Path)]) -> Output {
    let mut cmd = Command::new(OSC);
    cmd.args(args)
        .env_remove("OSC_KEYRING_FILE")
        .env_remove("OSC_KEYRING_COMMAND");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("run osc")
}

/// Runs the binary with `stdin` (the null device when `None`), the key source
/// variables cleared unless `env` sets them.
fn osc_run(args: &[&str], stdin: Option<&[u8]>, env: &[(&str, &str)]) -> Output {
    use std::io::Write;
    use std::process::Stdio;
    let mut cmd = Command::new(OSC);
    cmd.args(args)
        .env_remove("OSC_KEYRING_FILE")
        .env_remove("OSC_KEYRING_COMMAND")
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn osc");
    // Written from a thread: the input may be larger than the pipe.
    let writer = child.stdin.take().map(|mut pipe| {
        let data = stdin.unwrap_or_default().to_vec();
        std::thread::spawn(move || {
            let _ = pipe.write_all(&data);
        })
    });
    let out = child.wait_with_output().expect("wait");
    if let Some(writer) = writer {
        writer.join().expect("writer");
    }
    out
}

/// Runs the CLI in-process with the leading `flags`.
async fn osc_with(flags: &[&str], args: Vec<&str>) -> Result<(), CliError> {
    osc(&[flags, &args[..]].concat()).await
}

/// Writes a keyring file readable by its owner only, as `--keyring` requires.
fn write_ring(path: &Path, text: &str) {
    std::fs::write(path, text).expect("write keyring");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
}

const fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("date")
}

/// A new keyring text and its active key ID.
fn new_ring(prefix: &str) -> (String, String) {
    let text = new_keyring(prefix, day(2026, 10, 26)).expect("keyring");
    let id = Keyring::parse(&text).expect("parse").status().active;
    (text.to_string(), id)
}

fn s(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}

fn sample(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

/// Files in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

/// A keyring file in a directory of its own, removed on drop.
struct Ring {
    path: PathBuf,
    _dir: TempDir,
}

impl std::ops::Deref for Ring {
    type Target = PathBuf;
    fn deref(&self) -> &PathBuf {
        &self.path
    }
}

struct Setup {
    work: TempDir,
    ring: Ring,
    id: String,
}

fn setup() -> Setup {
    let keys = tempdir().expect("keys");
    let (text, id) = new_ring("kek");
    let path = keys.path().join("keyring");
    write_ring(&path, &text);
    Setup {
        work: tempdir().expect("work"),
        ring: Ring { path, _dir: keys },
        id,
    }
}

#[tokio::test]
async fn plain_put_get_roundtrip_streams_files() {
    let w = setup().work;
    // Several parts' worth, so the multipart path is taken.
    let body = sample(9 * 1024 * 1024 + 3);
    let src = w.path().join("src.bin");
    std::fs::write(&src, &body).expect("write");
    let store = tempdir().expect("store");
    let url = format!("file://{}/obj", store.path().display());
    let dst = w.path().join("dst.bin");

    osc(&["put", s(&src), &url]).await.expect("put");
    assert_eq!(
        std::fs::read(store.path().join("obj")).expect("stored"),
        body
    );
    osc(&["get", &url, s(&dst)]).await.expect("get");
    assert_eq!(std::fs::read(&dst).expect("dst"), body);
    assert_eq!(names(w.path()), ["dst.bin", "src.bin"]);
}

#[tokio::test]
async fn encrypted_put_get_roundtrip() {
    let Setup { work, ring, .. } = setup();
    let body = sample(200_000);
    let src = work.path().join("src.bin");
    std::fs::write(&src, &body).expect("write");
    let store = tempdir().expect("store");
    let url = format!("file://{}/obj", store.path().display());
    let dst = work.path().join("dst.bin");
    let k = ["--keyring", s(&ring)];

    osc(&[&k[..], &["put", s(&src), &url]].concat())
        .await
        .expect("put");
    let stored = std::fs::read(store.path().join("obj")).expect("stored");
    assert_eq!(stored[..3], [0x02, 0x04, 0x78]);
    assert_ne!(stored, body);

    osc(&[&k[..], &["get", &url, s(&dst)]].concat())
        .await
        .expect("get");
    assert_eq!(std::fs::read(&dst).expect("dst"), body);

    // Global flags are accepted after the subcommand too.
    osc(&["get", &url, s(&dst), k[0], k[1]]).await.expect("get");
}

#[tokio::test]
async fn encrypted_object_commands_work_on_ciphertext() {
    let Setup { work, ring, .. } = setup();
    let body = sample(150_000);
    let src = work.path().join("src.bin");
    std::fs::write(&src, &body).expect("write");
    let store = tempdir().expect("store");
    let url = |name: &str| format!("file://{}/{name}", store.path().display());
    let k = ["--keyring", s(&ring)];

    osc_with(&k, vec!["put", s(&src), &url("a")])
        .await
        .expect("put");
    osc_with(&k, vec!["cp", &url("a"), &url("b")])
        .await
        .expect("cp");
    osc_with(&k, vec!["mv", &url("b"), &url("c")])
        .await
        .expect("mv");
    assert_eq!(names(store.path()), ["a", "c"]);
    for name in ["a", "c"] {
        let stored = std::fs::read(store.path().join(name)).expect("stored");
        assert_eq!(stored[..3], [0x02, 0x04, 0x78], "{name}");
        assert_ne!(stored, body);
    }

    // Streaming reads decrypt, to a file and to stdout.
    let dst = work.path().join("dst.bin");
    osc_with(&k, vec!["get-stream", &url("c"), s(&dst)])
        .await
        .expect("get-stream");
    assert_eq!(std::fs::read(&dst).expect("dst"), body);
    let out = osc_bin(&[k[0], k[1], "get-stream", &url("c")], &[]);
    assert!(out.status.success());
    assert!(out.stdout.ends_with(&body), "stdout carries the plaintext");

    osc_with(&k, vec!["exists", &url("c")])
        .await
        .expect("exists");
    osc_with(&k, vec!["ls", &url("")]).await.expect("ls");
    osc_with(&k, vec!["rm", &url("c")]).await.expect("rm");
    assert_eq!(names(store.path()), ["a"]);
}

/// The bound `osc:key` is the lexical absolute path, so a new file written
/// through a symlinked directory reads back through the same path, and again
/// after an overwrite.
#[cfg(unix)]
#[tokio::test]
async fn encrypted_roundtrip_through_a_symlinked_directory() {
    let Setup { work, ring, .. } = setup();
    let body = sample(1_000);
    let src = work.path().join("src.bin");
    std::fs::write(&src, &body).expect("write");
    let store = tempdir().expect("store");
    std::fs::create_dir(store.path().join("real")).expect("real");
    std::os::unix::fs::symlink(store.path().join("real"), store.path().join("link"))
        .expect("symlink");
    let obj = store.path().join("link/obj");
    let k = ["--keyring", s(&ring)];
    let dst = work.path().join("dst.bin");

    for round in 0..2 {
        osc(&[&k[..], &["put", s(&src), s(&obj)]].concat())
            .await
            .expect("put");
        osc(&[&k[..], &["get", s(&obj), s(&dst)]].concat())
            .await
            .unwrap_or_else(|e| panic!("get, round {round}: {e:?}"));
        assert_eq!(std::fs::read(&dst).expect("dst"), body);
    }
}

#[tokio::test]
async fn get_stream_of_a_tampered_object_fails() {
    let Setup { work, ring, .. } = setup();
    let src = work.path().join("src.bin");
    std::fs::write(&src, sample(100_000)).expect("write");
    let store = tempdir().expect("store");
    let url = format!("file://{}/obj", store.path().display());
    let k = ["--keyring", s(&ring)];
    osc(&[&k[..], &["put", s(&src), &url]].concat())
        .await
        .expect("put");
    let path = store.path().join("obj");
    let mut stored = std::fs::read(&path).expect("stored");
    let last = stored.len() - 1;
    stored[last] ^= 1;
    std::fs::write(&path, stored).expect("tamper");

    let dst = work.path().join("dst.bin");
    std::fs::write(&dst, b"precious").expect("existing dst");
    let err = osc(&[&k[..], &["get-stream", &url, s(&dst)]].concat()).await;
    assert!(
        matches!(err, Err(CliError::Storage(Error::Decryption(_)))),
        "{err:?}"
    );
    assert_eq!(std::fs::read(&dst).expect("dst"), b"precious");
    assert_eq!(names(work.path()), ["dst.bin", "src.bin"], "no temp file");
}

#[tokio::test]
async fn recursive_put_encrypts_every_file() {
    let Setup { work, ring, .. } = setup();
    let tree = work.path().join("tree");
    std::fs::create_dir_all(tree.join("sub")).expect("dirs");
    std::fs::write(tree.join("a.txt"), b"alpha").expect("a");
    std::fs::write(tree.join("sub/b b.txt"), b"beta").expect("b");
    let store = tempdir().expect("store");
    let url = format!("file://{}/", store.path().display());

    osc(&["--keyring", s(&ring), "put", "-r", s(&tree), &url])
        .await
        .expect("put");
    for (name, plain) in [("a.txt", &b"alpha"[..]), ("sub/b b.txt", b"beta")] {
        let stored = std::fs::read(store.path().join(name)).expect("stored");
        assert_eq!(stored[..3], [0x02, 0x04, 0x78]);
        assert!(!stored.windows(plain.len()).any(|w| w == plain));
    }
}

#[tokio::test]
async fn recursive_put_without_a_trailing_slash_uploads_under_the_prefix() {
    let Setup { work, ring, .. } = setup();
    let tree = work.path().join("tree");
    std::fs::create_dir_all(&tree).expect("dirs");
    std::fs::write(tree.join("a.txt"), b"alpha").expect("a");
    let store = tempdir().expect("store");
    let url = format!("file://{}/pfx", store.path().display());
    let k = ["--keyring", s(&ring)];

    osc_with(&k, vec!["put", "-r", s(&tree), &url])
        .await
        .expect("put");
    let stored = std::fs::read(store.path().join("pfx/a.txt")).expect("stored");
    assert_eq!(stored[..3], [0x02, 0x04, 0x78]);
    assert_eq!(names(store.path()), ["pfx"]);
}

#[tokio::test]
async fn encrypted_put_refuses_bad_sources_before_storing() {
    let Setup { work, ring, .. } = setup();
    let dir = work.path().join("dir");
    std::fs::create_dir_all(&dir).expect("dir");
    let store = tempdir().expect("store");
    let url = format!("file://{}/obj", store.path().display());
    let k = ["--keyring", s(&ring)];

    let err = osc_with(&k, vec!["put", s(&dir), &url]).await;
    assert!(
        matches!(err, Err(CliError::DirectoryRecursiveRequired { .. })),
        "{err:?}"
    );
    let missing = work.path().join("missing");
    let err = osc_with(&k, vec!["put", s(&missing), &url]).await;
    assert!(matches!(err, Err(CliError::ReadFile { .. })), "{err:?}");
    assert_eq!(names(store.path()), Vec::<String>::new());
}

#[tokio::test]
async fn sign_with_a_keyring_errors() {
    let Setup { ring, .. } = setup();
    let err = osc(&["--keyring", s(&ring), "sign", "s3://b/k"])
        .await
        .expect_err("sign");
    assert!(matches!(err, CliError::SignWithKeys));
    assert_eq!(err.to_string(), "sign is not available with a keyring");
}

#[tokio::test]
async fn an_unreadable_keyring_fails_before_any_work() {
    let Setup { work, ring, .. } = setup();
    let dir = ring.parent().expect("dir").to_path_buf();
    let url = format!("file://{}/x", work.path().display());
    let err = osc(&["--keyring", s(&dir.join("missing")), "exists", &url])
        .await
        .expect_err("missing keyring");
    assert!(matches!(err, CliError::Key(_)), "{err:?}");

    let bad = dir.join("bad");
    write_ring(&bad, "not a keyring\n");
    let err = osc(&["--keyring", s(&bad), "exists", &url])
        .await
        .expect_err("bad keyring");
    assert!(err.to_string().contains("line 1"), "{err}");
}

#[test]
fn env_keyring_file_enables_encryption() {
    let Setup { work, ring, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"plain").expect("write");
    let url = format!("file://{}/obj", work.path().display());

    let out = osc_run(
        &["put", s(&src), &url],
        None,
        &[("OSC_KEYRING_FILE", s(&ring))],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(work.path().join("obj")).expect("stored")[..3],
        [2, 4, 0x78]
    );
}

#[test]
fn both_key_sources_are_a_usage_error() {
    let Setup { work, ring, .. } = setup();
    let url = format!("file://{}/x", work.path().display());
    let out = osc_run(
        &["--keyring", s(&ring), "exists", &url],
        None,
        &[("OSC_KEYRING_COMMAND", "true")],
    );
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("both set"), "{err}");

    let out = osc_run(
        &["exists", &url],
        None,
        &[
            ("OSC_KEYRING_FILE", s(&ring)),
            ("OSC_KEYRING_COMMAND", "true"),
        ],
    );
    assert_eq!(out.status.code(), Some(1));
    // Every command that needs the source refuses, including `status`.
    let out = osc_run(
        &["keyring", "status", "--keyring", s(&ring)],
        None,
        &[("OSC_KEYRING_COMMAND", "true")],
    );
    assert_eq!(out.status.code(), Some(1));
}

#[cfg(unix)]
#[test]
fn keyring_command_supplies_the_keys() {
    let Setup { work, ring, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"through a command").expect("write");
    let url = format!("file://{}/obj", work.path().display());
    let dst = work.path().join("dst");
    let command = format!("cat '{}'", ring.display());
    let env = [("OSC_KEYRING_COMMAND", command.as_str())];

    let out = osc_run(&["put", s(&src), &url], None, &env);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(work.path().join("obj")).expect("stored")[..3],
        [2, 4, 0x78]
    );
    let out = osc_run(&["get", &url, s(&dst)], None, &env);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read(&dst).expect("dst"), b"through a command");
}

#[cfg(unix)]
#[test]
fn keyring_command_failures_are_errors_that_do_not_echo_the_output() {
    let url = "file:///nonexistent/x";
    let run = |command: &str| {
        let out = osc_run(&["exists", url], None, &[("OSC_KEYRING_COMMAND", command)]);
        assert_eq!(out.status.code(), Some(1), "{command}");
        String::from_utf8_lossy(&out.stderr).into_owned()
    };

    let err = run("echo osc-keyring-v1-ish-secret; exit 3");
    assert!(err.contains("OSC_KEYRING_COMMAND failed"), "{err}");
    assert!(err.contains("exit status: 3"), "{err}");
    assert!(!err.contains("ish-secret"), "{err}");

    let err = run("head -c 70000 /dev/zero | tr '\\0' a");
    assert!(err.contains("more than 64 KiB"), "{err}");

    // Standard input is the null device: `cat` sees end of file at once.
    let err = run("cat");
    assert!(err.contains("empty keyring"), "{err}");

    let err = run("printf 'osc-keyring-v1\\n'");
    assert!(err.contains("missing active line"), "{err}");

    let err = run("printf '\\377'");
    assert!(err.contains("not valid UTF-8"), "{err}");
}

#[tokio::test]
async fn enc_inspect_ignores_the_keyring() {
    let Setup { work, .. } = setup();
    let obj = work.path().join("obj");
    std::fs::write(&obj, b"plain").expect("obj");
    let url = format!("file://{}", obj.display());
    // The unreadable keyring is not even opened.
    let out = osc_bin(
        &["--keyring", "/nonexistent/keyring", "enc", "inspect", &url],
        &[],
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("Not an encrypted object"), "{err}");
}

#[tokio::test]
async fn failed_get_leaves_no_destination_and_no_temp_file() {
    let Setup { work, ring, .. } = setup();
    let src = work.path().join("src.bin");
    std::fs::write(&src, sample(300_000)).expect("write");
    let store = tempdir().expect("store");
    let url = format!("file://{}/obj", store.path().display());
    let k = ["--keyring", s(&ring)];
    osc(&[&k[..], &["put", s(&src), &url]].concat())
        .await
        .expect("put");

    let mut stored = std::fs::read(store.path().join("obj")).expect("stored");
    let last = stored.len() - 1;
    stored[last] ^= 1;
    std::fs::write(store.path().join("obj"), stored).expect("tamper");

    let out = tempdir().expect("out");
    let dst = out.path().join("dst.bin");
    let err = osc(&[&k[..], &["get", &url, s(&dst)]].concat())
        .await
        .expect_err("tampered");
    assert!(matches!(err, CliError::Storage(_)), "{err}");
    assert!(names(out.path()).is_empty(), "left {:?}", names(out.path()));

    // A missing object fails the same way.
    let missing = format!("file://{}/nope", store.path().display());
    osc(&["get", &missing, s(&dst)]).await.expect_err("missing");
    assert_eq!(names(out.path()).len(), 0);
}

#[tokio::test]
async fn failed_get_keeps_an_existing_destination() {
    let Setup { work, ring, .. } = setup();
    let store = tempdir().expect("store");
    let url = format!("file://{}/obj", store.path().display());
    std::fs::write(store.path().join("obj"), b"not encrypted at all").expect("obj");
    let dst = work.path().join("dst.bin");
    std::fs::write(&dst, b"precious").expect("dst");

    osc(&["--keyring", s(&ring), "get", &url, s(&dst)])
        .await
        .expect_err("plain object");
    assert_eq!(std::fs::read(&dst).expect("dst"), b"precious");
    assert_eq!(names(work.path()), ["dst.bin"]);
}

fn stdout(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).expect("utf-8")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn keyring_of(out: &Output) -> Keyring {
    assert!(out.status.success(), "{}", stderr(out));
    Keyring::parse(&stdout(out)).expect("output is a keyring")
}

#[test]
fn keyring_new_prints_a_keyring() {
    let out = osc_run(&["keyring", "new", "--prefix", "main"], None, &[]);
    let status = keyring_of(&out).status();
    assert!(status.active.starts_with("main-"), "{}", status.active);
    assert_eq!(status.keys.len(), 1);
    assert_eq!(stderr(&out), "");

    // The guard only applies to terminals; the flag is accepted anyway.
    let out = osc_run(&["keyring", "new", "--force"], None, &[]);
    assert!(keyring_of(&out).status().active.starts_with("kek-"));

    let out = osc_run(&["keyring", "new", "--prefix", "a b"], None, &[]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(stdout(&out), "");
}

#[test]
fn keyring_rotate_adds_a_key_and_keeps_the_others() {
    let first = osc_run(&["keyring", "new"], None, &[]);
    let before = keyring_of(&first).status();
    let out = osc_run(
        &["keyring", "rotate", "--prefix", "next"],
        Some(&first.stdout),
        &[],
    );
    let after = keyring_of(&out).status();
    assert_eq!(after.keys.len(), 2);
    assert!(after.active.starts_with("next-"), "{}", after.active);
    assert!(after.keys.iter().any(|k| k.id == before.active));
    // The old key line is unchanged and stays first.
    let lines: Vec<String> = stdout(&out).lines().map(str::to_owned).collect();
    assert_eq!(lines[1], format!("active={}", after.active));
    assert_eq!(lines[2], stdout(&first).lines().nth(2).expect("key line"));

    // A third round keeps both.
    let out = osc_run(&["keyring", "rotate", "--force"], Some(&out.stdout), &[]);
    assert_eq!(keyring_of(&out).status().keys.len(), 3);

    let out = osc_run(&["keyring", "rotate"], Some(b"junk\n"), &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("line 1"), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
}

#[test]
fn keyring_retire_removes_a_key_and_refuses_the_active_and_unknown_ones() {
    let first = osc_run(&["keyring", "new"], None, &[]);
    let old = keyring_of(&first).status().active;
    let rotated = osc_run(&["keyring", "rotate"], Some(&first.stdout), &[]);
    let active = keyring_of(&rotated).status().active;

    let out = osc_run(&["keyring", "retire", &old], Some(&rotated.stdout), &[]);
    let left = keyring_of(&out).status();
    assert_eq!(left.active, active);
    assert_eq!(left.keys.len(), 1);

    let out = osc_run(&["keyring", "retire", &active], Some(&rotated.stdout), &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("active key"), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");

    let out = osc_run(
        &["keyring", "retire", "nope", "--force"],
        Some(&rotated.stdout),
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("no key"), "{}", stderr(&out));
}

#[test]
fn keyring_helpers_refuse_oversized_input() {
    let big = vec![b'\n'; 64 * 1024 + 1];
    let out = osc_run(&["keyring", "rotate"], Some(&big), &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("larger than 64 KiB"),
        "{}",
        stderr(&out)
    );
    let out = osc_run(&["keyring", "rotate"], Some(b"\xff\xfe"), &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("UTF-8"), "{}", stderr(&out));
}

#[test]
fn keyring_status_lists_keys_and_flags_an_old_active_key() {
    let (fresh, fresh_id) = new_ring("kek");
    let out = osc_run(&["keyring", "status"], Some(fresh.as_bytes()), &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    let rows: Vec<Vec<&str>> = text
        .lines()
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(rows[0], ["ID", "CREATED", "AGE_DAYS", "ACTIVE"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][..2], [fresh_id.as_str(), "2026-10-26"]);
    assert_eq!(rows[1].last(), Some(&"*"));
    // Never material.
    let material = fresh
        .split_whitespace()
        .find_map(|f| f.strip_prefix("material="))
        .expect("material");
    assert!(!text.contains(material));

    // Rotated: the older key is listed without the marker, newest first.
    let rotated = osc_run(&["keyring", "rotate"], Some(fresh.as_bytes()), &[]);
    let out = osc_run(&["keyring", "status"], Some(&rotated.stdout), &[]);
    let text = stdout(&out);
    let rows: Vec<Vec<&str>> = text
        .lines()
        .skip(1)
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].last(), Some(&"*"));
    assert_eq!(rows[1].len(), 3);
    assert_eq!(rows[1][0], fresh_id);

    // 3 for an overdue active key (written in 2020), 0 within the limit.
    let old = new_keyring("kek", day(2020, 1, 1)).expect("keyring");
    let out = osc_run(&["keyring", "status"], Some(old.as_bytes()), &[]);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert!(stderr(&out).contains("days old"), "{}", stderr(&out));
    assert!(stdout(&out).contains("2020-01-01"));
    let out = osc_run(
        &["keyring", "status", "--max-age-days", "100000"],
        Some(old.as_bytes()),
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    // 1 for an invalid keyring.
    let out = osc_run(&["keyring", "status"], Some(b"osc-keyring-v1\n"), &[]);
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn keyring_status_reads_the_configured_source() {
    let Setup { ring, id, .. } = setup();
    // The null device as stdin: the file is used, not standard input.
    let out = osc_run(&["keyring", "status", "--keyring", s(&ring)], None, &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).contains(&id));

    let out = osc_run(
        &["keyring", "status"],
        None,
        &[("OSC_KEYRING_FILE", s(&ring))],
    );
    assert!(stdout(&out).contains(&id));

    #[cfg(unix)]
    {
        let command = format!("cat '{}'", ring.display());
        let out = osc_run(
            &["keyring", "status"],
            None,
            &[("OSC_KEYRING_COMMAND", command.as_str())],
        );
        assert!(stdout(&out).contains(&id), "{}", stderr(&out));
    }
}

#[cfg(unix)]
#[test]
fn keyring_files_with_open_permissions_are_refused() {
    use std::os::unix::fs::PermissionsExt;
    let Setup { ring, id, .. } = setup();
    std::fs::set_permissions(&*ring, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    let out = osc_run(&["keyring", "status", "--keyring", s(&ring)], None, &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("has mode 644"), "{}", stderr(&out));
    assert!(!stdout(&out).contains(&id));
    // Group read is fine.
    std::fs::set_permissions(&*ring, std::fs::Permissions::from_mode(0o640)).expect("chmod");
    let out = osc_run(&["keyring", "status", "--keyring", s(&ring)], None, &[]);
    assert!(out.status.success(), "{}", stderr(&out));
}

#[tokio::test]
async fn enc_inspect_prints_the_header() {
    let Setup { work, ring, id, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"hello").expect("write");
    let url = format!("file://{}/obj", work.path().display());
    osc(&["--keyring", s(&ring), "put", s(&src), &url])
        .await
        .expect("put");

    // Without keys.
    let out = osc_bin(&["enc", "inspect", &url], &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).expect("utf-8");
    assert!(text.contains("format: osc-esdk2-0478-v1\n"), "{text}");
    assert!(text.contains(&format!("key_names: {id}\n")), "{text}");
    assert!(text.contains("frame_length: 65536\n"), "{text}");
    assert!(text.contains("context.osc:key="), "{text}");
    assert!(text.contains("message_id: "), "{text}");

    // With keys, the same.
    let out = osc_bin(&["--keyring", s(&ring), "enc", "inspect", &url], &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8(out.stdout).expect("utf-8"), text);
}

#[tokio::test]
async fn enc_inspect_of_a_plain_object_errors() {
    let w = setup().work;
    std::fs::write(w.path().join("plain"), b"just text").expect("write");
    let url = format!("file://{}/plain", w.path().display());
    let out = osc_bin(&["enc", "inspect", &url], &[]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("Not an encrypted object"));
}

#[cfg(unix)]
#[tokio::test]
async fn get_follows_a_symlink_destination() {
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());
    let target = w.path().join("target.txt");
    std::fs::write(&target, b"x").expect("target");
    let link = w.path().join("link.bin");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");

    osc(&["get", &url, s(&link)]).await.expect("get");
    assert!(std::fs::symlink_metadata(&link).expect("meta").is_symlink());
    assert_eq!(std::fs::read(&target).expect("target"), b"payload");
    assert_eq!(names(w.path()), ["link.bin", "target.txt"]);
}

#[cfg(unix)]
#[tokio::test]
async fn get_writes_through_a_fifo() {
    use std::os::unix::fs::FileTypeExt;
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());
    let fifo = w.path().join("pipe");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo")
            .success()
    );
    let reader = {
        let fifo = fifo.clone();
        std::thread::spawn(move || std::fs::read(fifo).expect("read fifo"))
    };

    osc(&["get", &url, s(&fifo)]).await.expect("get");
    assert_eq!(reader.join().expect("reader"), b"payload");
    let ft = std::fs::symlink_metadata(&fifo).expect("meta").file_type();
    assert!(ft.is_fifo(), "still a fifo");
    assert_eq!(names(w.path()), ["pipe"]);
}

#[cfg(unix)]
#[tokio::test]
async fn get_keeps_the_mode_of_an_existing_destination() {
    use std::os::unix::fs::PermissionsExt;
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());
    let dst = w.path().join("dst.bin");
    std::fs::write(&dst, b"old").expect("dst");
    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o600)).expect("chmod");

    osc(&["get", &url, s(&dst)]).await.expect("get");
    let mode = std::fs::metadata(&dst).expect("meta").permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    assert_eq!(std::fs::read(&dst).expect("dst"), b"payload");
}

#[cfg(unix)]
#[tokio::test]
async fn put_of_a_private_file_to_a_local_store_stays_private() {
    use std::os::unix::fs::PermissionsExt;
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    let src = w.path().join("secret.env");
    std::fs::write(&src, b"token=1").expect("src");
    std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    let dst = store.path().join("secret.env");

    osc(&["put", s(&src), &format!("file://{}", dst.display())])
        .await
        .expect("put");
    let mode = std::fs::metadata(&dst).expect("meta").permissions().mode();
    assert_eq!(mode & 0o077, 0, "no group/other bits: {mode:o}");
    assert_eq!(std::fs::read(&dst).expect("dst"), b"token=1");
}

#[tokio::test]
async fn get_stream_to_stdout_prints_only_the_object_bytes() {
    let store = tempdir().expect("store");
    let body = sample(5000);
    std::fs::write(store.path().join("obj"), &body).expect("obj");
    let url = format!("file://{}/obj", store.path().display());

    let out = osc_bin(&["get-stream", &url], &[]);
    assert!(out.status.success());
    assert_eq!(out.stdout, body);
}

#[cfg(unix)]
#[tokio::test]
async fn recursive_put_keeps_hostile_file_names_inside_the_destination() {
    let w = tempdir().expect("work");
    let src = w.path().join("src");
    std::fs::create_dir_all(src.join("sub")).expect("dirs");
    std::fs::write(src.join("..\\..\\pwned"), b"evil").expect("evil");
    std::fs::write(src.join("%2e%2e"), b"pct").expect("pct");
    std::fs::write(src.join("sub/ok.txt"), b"ok").expect("ok");
    let dst = w.path().join("t/dst/inner");
    let url = format!("file://{}", dst.display());

    let res = osc(&["put", "-r", s(&src), &url]).await;
    if let Err(e) = &res {
        assert!(matches!(e, CliError::UnsafePath { .. }), "{e}");
    }

    // Nothing outside the destination directory, whatever the outcome.
    assert!(!w.path().join("pwned").exists());
    assert!(!w.path().join("t/pwned").exists());
    assert!(!w.path().join("t/dst/pwned").exists());
    assert!(!w.path().join("t/dst/inner/pwned").exists());
    for entry in std::fs::read_dir(w.path().join("t")).expect("t") {
        assert_eq!(entry.expect("e").file_name(), "dst");
    }
    if res.is_ok() {
        assert_eq!(std::fs::read(dst.join("sub/ok.txt")).expect("ok"), b"ok");
        assert_eq!(
            std::fs::read(dst.join("..\\..\\pwned")).expect("evil"),
            b"evil"
        );
        assert_eq!(std::fs::read(dst.join("%2e%2e")).expect("pct"), b"pct");
    }
}

/// Starts `osc get` of a large sparse object, sends `signal` once a temp file
/// holds data and checks the clean-up. Returns the exit code, or `None` when
/// the download ended before the signal could be sent.
#[cfg(unix)]
fn interrupt_get(signal: &str, size: u64) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    use std::time::{Duration, Instant};

    let store = tempdir().expect("store");
    let out = tempdir().expect("out");
    let obj = std::fs::File::create(store.path().join("big")).expect("obj");
    obj.set_len(size).expect("sparse");
    let url = format!("file://{}/big", store.path().display());
    let dst = out.path().join("plain.bin");

    let mut child = Command::new(OSC)
        .args(["get", &url, s(&dst)])
        .env_remove("OSC_KEYRING_FILE")
        .env_remove("OSC_KEYRING_COMMAND")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn osc");

    let tmp_with_data = || {
        std::fs::read_dir(out.path())
            .expect("read dir")
            .flatten()
            .any(|e| {
                e.file_name().to_string_lossy().contains(".osc-tmp-")
                    && e.metadata().is_ok_and(|m| m.len() > 0)
            })
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = false;
    while Instant::now() < deadline && child.try_wait().expect("wait").is_none() {
        if tmp_with_data() {
            seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if !seen || child.try_wait().expect("wait").is_some() {
        let _ = child.kill();
        let _ = child.wait();
        return None;
    }
    let status = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let exit = child.wait().expect("wait");
    assert!(exit.signal().is_none(), "died on the signal: {exit:?}");
    assert!(!dst.exists(), "destination must not exist");
    assert_eq!(names(out.path()), Vec::<String>::new(), "temp file left");
    Some(exit.code().expect("exit code"))
}

#[cfg(unix)]
fn assert_signal_cleans_up(signal: &str, code: i32) {
    for size in [512u64 << 20, 4 << 30] {
        if let Some(exit) = interrupt_get(signal, size) {
            assert_eq!(exit, code);
            return;
        }
        eprintln!("download finished before the signal at {size} bytes; retrying bigger");
    }
    eprintln!("skipped: could not interrupt the download in flight");
}

#[cfg(unix)]
#[test]
fn interrupted_get_removes_the_temp_file_on_sigint() {
    assert_signal_cleans_up("-INT", 130);
}

#[cfg(unix)]
#[test]
fn interrupted_get_removes_the_temp_file_on_sigterm() {
    assert_signal_cleans_up("-TERM", 143);
}

#[cfg(unix)]
#[tokio::test]
async fn new_download_keeps_the_default_file_mode() {
    use std::os::unix::fs::PermissionsExt;
    let mode_of = |p: &Path| std::fs::metadata(p).expect("meta").permissions().mode() & 0o777;
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"data").expect("obj");
    let out = tempdir().expect("out");
    let probe = out.path().join("probe");
    std::fs::File::create(&probe).expect("probe");
    let dst = out.path().join("o");
    let url = format!("file://{}/obj", store.path().display());
    osc(&["get", &url, s(&dst)]).await.expect("get");
    assert_eq!(mode_of(&dst), mode_of(&probe));
}

#[cfg(unix)]
#[tokio::test]
async fn recursive_put_still_uploads_a_nested_tree() {
    let w = tempdir().expect("work");
    let src = w.path().join("src");
    std::fs::create_dir_all(src.join("a/b")).expect("dirs");
    std::fs::write(src.join("top.txt"), b"top").expect("top");
    std::fs::write(src.join("a/b/deep.txt"), b"deep").expect("deep");
    let dst = w.path().join("dst");
    let url = format!("file://{}", dst.display());

    osc(&["put", "-r", s(&src), &url]).await.expect("put");
    assert_eq!(std::fs::read(dst.join("top.txt")).expect("top"), b"top");
    assert_eq!(
        std::fs::read(dst.join("a/b/deep.txt")).expect("deep"),
        b"deep"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn get_keeps_a_0666_destination_but_drops_special_bits() {
    use std::os::unix::fs::PermissionsExt;
    let mode_of = |p: &Path| std::fs::metadata(p).expect("meta").permissions().mode() & 0o7777;
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());

    let dst = w.path().join("a");
    std::fs::write(&dst, b"old").expect("dst");
    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o666)).expect("chmod");
    osc(&["get", &url, s(&dst)]).await.expect("get");
    assert_eq!(mode_of(&dst), 0o666);

    let dst = w.path().join("b");
    std::fs::write(&dst, b"old").expect("dst");
    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o6777)).expect("chmod");
    osc(&["get", &url, s(&dst)]).await.expect("get");
    assert_eq!(mode_of(&dst), 0o777, "no setuid/setgid");
    assert_eq!(std::fs::read(&dst).expect("dst"), b"payload");
    assert_eq!(names(w.path()), ["a", "b"]);
}

/// `ls` prints every name, sanitized, one per line, and exits 0.
#[test]
fn ls_prints_every_name_sanitized() {
    let dir = tempdir().expect("dir");
    for i in 0..50 {
        std::fs::write(dir.path().join(format!("f{i:03}.txt")), b"x").expect("write");
    }
    std::fs::create_dir(dir.path().join("sub")).expect("mkdir");
    std::fs::write(dir.path().join("sub/inner"), b"x").expect("write");
    std::fs::write(dir.path().join("bidi\u{202E}name"), b"x").expect("write");

    let out = osc_bin(&["ls", s(dir.path())], &[]);
    assert!(out.status.success(), "{out:?}");
    let text = String::from_utf8(out.stdout).expect("utf-8");
    assert!(!text.contains('\u{202E}'), "bidi characters are escaped");
    let mut lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 52);
    lines.sort_unstable();
    assert!(lines.contains(&"f000.txt") && lines.contains(&"f049.txt"));
    assert!(lines.contains(&"sub/inner"));
}

/// A name the store cannot parse fails the listing mid-stream: `ls` exits
/// non-zero and prints nothing unsanitized. Walk order is unspecified, so how
/// many names come out before the failure is not asserted, only that the
/// failure is reported and nothing but good names was printed.
#[test]
fn ls_fails_on_an_unlistable_name() {
    let dir = tempdir().expect("dir");
    std::fs::write(dir.path().join("ok.txt"), b"x").expect("write");
    std::fs::write(dir.path().join("bad\x1bname"), b"x").expect("write");

    let out = osc_bin(&["ls", s(dir.path())], &[]);
    assert!(!out.status.success(), "{out:?}");
    let text = String::from_utf8(out.stdout).expect("utf-8");
    assert!(text.lines().all(|l| l == "ok.txt"), "{text:?}");
}

#[test]
fn sanitize_escapes_control_and_bidi_characters() {
    use object_storage_client::cli::sanitize;
    assert_eq!(sanitize("plain ünï/ok.txt"), "plain ünï/ok.txt");
    assert_eq!(
        sanitize("a\x1b]0;pwn\x07\x1b[2Jb"),
        "a\\u{1b}]0;pwn\\u{7}\\u{1b}[2Jb"
    );
    assert_eq!(
        sanitize("x\x7fy\u{85}z\n\r\t"),
        "x\\u{7f}y\\u{85}z\\u{a}\\u{d}\\u{9}"
    );
    assert_eq!(
        sanitize("a\u{202E}b\u{2066}c\u{2069}d\u{202A}"),
        "a\\u{202e}b\\u{2066}c\\u{2069}d\\u{202a}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn get_works_for_a_destination_name_near_the_limit() {
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());
    let name = "n".repeat(250);
    let dst = w.path().join(&name);

    osc(&["get", &url, s(&dst)]).await.expect("get");
    assert_eq!(std::fs::read(&dst).expect("dst"), b"payload");
    assert_eq!(names(w.path()), [name]);
}

#[cfg(unix)]
#[test]
fn status_lines_and_errors_do_not_pass_terminal_escapes() {
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());
    let evil = "a\x1b]0;pwn\x07\x1b[2Jb";

    // Success path: the Download line.
    let dst = w.path().join(evil);
    let out = osc_bin(&["get", &url, s(&dst)], &[]);
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf-8");
    assert!(
        !stdout.contains('\x1b') && !stdout.contains('\x07'),
        "{stdout:?}"
    );
    assert!(
        stdout.contains("a\\u{1b}]0;pwn\\u{7}\\u{1b}[2Jb"),
        "{stdout:?}"
    );

    // Upload line and the error chain.
    let missing = w.path().join(format!("{evil}-missing"));
    let out = osc_bin(&["put", s(&missing), &url], &[]);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        !stderr.contains('\x1b') && !stderr.contains('\x07'),
        "{stderr:?}"
    );
    assert!(stderr.contains("\\u{1b}"), "{stderr:?}");
}

#[test]
fn status_lines_redact_url_credentials_and_tokens() {
    let out = osc_bin(
        &[
            "get",
            "file://user:s3cret@localhost/nonexistent?token=TOK#frag",
            "/nonexistent-dir/x",
        ],
        &[],
    );
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(all.contains("Download "), "{all}");
    for secret in ["s3cret", "TOK", "frag"] {
        assert!(!all.contains(secret), "{secret} leaked: {all}");
    }
}

/// Starts `osc put` of a large sparse file to a `file://` store, sends SIGINT
/// once a staging file (`path#N`) holds data and checks the clean-up. Returns
/// `None` when the upload ended before the signal could be sent.
#[cfg(unix)]
fn interrupt_put(size: u64) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    use std::time::{Duration, Instant};

    let src_dir = tempdir().expect("src");
    let store = tempdir().expect("store");
    let src = src_dir.path().join("big");
    std::fs::File::create(&src)
        .expect("src")
        .set_len(size)
        .expect("sparse");
    let url = format!("file://{}/big", store.path().display());

    let mut child = Command::new(OSC)
        .args(["put", s(&src), &url])
        .env_remove("OSC_KEYRING_FILE")
        .env_remove("OSC_KEYRING_COMMAND")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn osc");

    let staging_with_data = || {
        std::fs::read_dir(store.path())
            .expect("read dir")
            .flatten()
            .any(|e| {
                e.file_name().to_string_lossy().contains('#')
                    && e.metadata().is_ok_and(|m| m.len() > 0)
            })
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = false;
    while Instant::now() < deadline && child.try_wait().expect("wait").is_none() {
        if staging_with_data() {
            seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if !seen || child.try_wait().expect("wait").is_some() {
        let _ = child.kill();
        let _ = child.wait();
        return None;
    }
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let exit = child.wait().expect("wait");
    assert!(exit.signal().is_none(), "died on the signal: {exit:?}");
    assert_eq!(names(store.path()), Vec::<String>::new(), "staging left");
    Some(exit.code().expect("exit code"))
}

#[cfg(unix)]
#[test]
fn interrupted_put_removes_the_staging_file_on_sigint() {
    for size in [512u64 << 20, 4 << 30] {
        if let Some(exit) = interrupt_put(size) {
            assert_eq!(exit, 130);
            return;
        }
        eprintln!("upload finished before the signal at {size} bytes; retrying bigger");
    }
    eprintln!("skipped: could not interrupt the upload in flight");
}

#[cfg(unix)]
#[test]
fn sigint_interrupts_a_get_blocked_opening_a_fifo() {
    use std::os::unix::process::ExitStatusExt;
    use std::time::{Duration, Instant};

    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"data").expect("obj");
    let out = tempdir().expect("out");
    let fifo = out.path().join("fifo");
    let status = Command::new("mkfifo").arg(&fifo).status().expect("mkfifo");
    assert!(status.success());
    let url = format!("file://{}/obj", store.path().display());

    let mut child = Command::new(OSC)
        .args(["get", &url, s(&fifo)])
        .env_remove("OSC_KEYRING_FILE")
        .env_remove("OSC_KEYRING_COMMAND")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn osc");
    // Let it reach the blocking open (no reader on the FIFO).
    std::thread::sleep(Duration::from_millis(1000));
    assert!(child.try_wait().expect("wait").is_none(), "must be blocked");
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());

    let deadline = Instant::now() + Duration::from_secs(10);
    let exit = loop {
        if let Some(exit) = child.try_wait().expect("wait") {
            break exit;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("osc did not exit after SIGINT while blocked on a FIFO");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(exit.signal().is_none(), "died on the signal: {exit:?}");
    assert_eq!(exit.code(), Some(130));
}

#[cfg(unix)]
#[test]
fn recursive_put_fails_fast_and_leaves_no_staging_files() {
    use std::os::unix::fs::PermissionsExt;
    let w = tempdir().expect("work");
    let src = w.path().join("src");
    std::fs::create_dir_all(&src).expect("src");
    let data = sample(9 * 1024 * 1024);
    for i in 0..16 {
        std::fs::write(src.join(format!("f{i:02}.bin")), &data).expect("file");
    }
    let bad = src.join("f07.bin");
    std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    if std::fs::File::open(&bad).is_ok() {
        // Running as a user that ignores permissions (root): nothing to test.
        return;
    }
    let dst = w.path().join("dst");
    let url = format!("file://{}", dst.display());

    let out = osc_bin(&["put", "-r", s(&src), &url], &[]);
    assert!(!out.status.success(), "put -r must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("f07.bin"),
        "stderr names the unreadable file"
    );
    // Whatever was stored is complete; nothing partial or staged remains.
    let stored = if dst.exists() { names(&dst) } else { vec![] };
    assert!(!stored.iter().any(|n| n == "f07.bin"));
    for name in &stored {
        assert!(
            name.starts_with('f') && name.len() == "f00.bin".len(),
            "unexpected leftover {name}"
        );
        assert_eq!(std::fs::read(dst.join(name)).expect("stored"), data);
    }
}

/// A store with objects under the old key (`a`, `dir/b`), a plain one
/// (`plain`), a corrupt one (`bad`) and, under a rotated keyring's new key,
/// `new`.
struct Scanned {
    store: TempDir,
    url: String,
    _keys: TempDir,
    old_ring: PathBuf,
    new_ring: PathBuf,
    old_id: String,
    new_id: String,
}

fn put_with(ring: &Path, store: &TempDir, name: &str, body: &[u8]) {
    let src = tempdir().expect("src");
    let file = src.path().join("src");
    std::fs::write(&file, body).expect("write");
    let url = format!("file://{}/{name}", store.path().display());
    let out = osc_bin(&["--keyring", s(ring), "put", s(&file), &url], &[]);
    assert!(out.status.success(), "{}", stderr(&out));
}

fn scanned() -> Scanned {
    let keys = tempdir().expect("keys");
    let (text, old_id) = new_ring("old");
    let old_ring = keys.path().join("old");
    write_ring(&old_ring, &text);
    let rotated = rotate_keyring(&text, "new", day(2026, 10, 27)).expect("rotate");
    let new_id = Keyring::parse(&rotated).expect("parse").status().active;
    let new_ring = keys.path().join("new");
    write_ring(&new_ring, &rotated);

    let store = tempdir().expect("store");
    put_with(&old_ring, &store, "a", b"alpha");
    put_with(&old_ring, &store, "dir/b", &sample(100_000));
    put_with(&new_ring, &store, "new", b"fresh");
    std::fs::write(store.path().join("plain"), b"just text").expect("plain");
    let mut bad = std::fs::read(store.path().join("a")).expect("a");
    bad.truncate(20);
    std::fs::write(store.path().join("bad"), bad).expect("bad");
    let url = format!("file://{}", store.path().display());
    Scanned {
        store,
        url,
        _keys: keys,
        old_ring,
        new_ring,
        old_id,
        new_id,
    }
}

#[test]
fn enc_scan_counts_objects_per_key() {
    let t = scanned();
    // No keyring is read.
    let out = osc_bin(
        &[
            "--keyring",
            "/nonexistent/keyring",
            "enc",
            "scan",
            &t.url,
            "--jobs",
            "2",
        ],
        &[],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    let rows: Vec<Vec<&str>> = text
        .lines()
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(rows[0], ["KEY_ID", "OBJECTS"]);
    // Sorted by key ID: "new-..." before "old-...".
    assert_eq!(rows[1], [t.new_id.as_str(), "1"]);
    assert_eq!(rows[2], [t.old_id.as_str(), "2"]);
    assert_eq!(rows[3], ["(not", "encrypted)", "1"]);
    assert_eq!(rows[4], ["(unreadable)", "1"]);
    let err = stderr(&out);
    assert!(
        err.contains("unreadable: ") && err.contains("/bad"),
        "{err}"
    );
}

#[test]
fn enc_scan_key_prints_the_urls_of_that_key() {
    let t = scanned();
    let out = osc_bin(&["enc", "scan", &t.url, "--key", &t.old_id], &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    let mut urls: Vec<String> = stdout(&out).lines().map(str::to_owned).collect();
    urls.sort();
    assert_eq!(urls, [format!("{}/a", t.url), format!("{}/dir/b", t.url)]);

    let out = osc_bin(&["enc", "scan", &t.url, "--key", "nope"], &[]);
    assert!(out.status.success());
    assert_eq!(stdout(&out), "");
    let out = osc_bin(&["enc", "scan", &t.url, "--key", "x", "--json"], &[]);
    assert!(!out.status.success(), "--key and --json conflict");
}

#[test]
fn enc_scan_json_has_a_line_per_object_and_a_summary() {
    let t = scanned();
    let out = osc_bin(&["enc", "scan", &t.url, "--json"], &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    let mut lines: Vec<String> = stdout(&out).lines().map(str::to_owned).collect();
    let summary = lines.pop().expect("summary");
    assert_eq!(
        summary,
        format!(
            "{{\"summary\":{{\"{}\":1,\"{}\":2,\"no-key\":0,\"not-encrypted\":1,\"unreadable\":1}}}}",
            t.new_id, t.old_id
        )
    );
    lines.sort();
    let u = &t.url;
    let old = &t.old_id;
    let want = [
        format!("{{\"url\":\"{u}/a\",\"key_ids\":[\"{old}\"],\"status\":\"encrypted\"}}"),
        format!("{{\"url\":\"{u}/bad\",\"key_ids\":[],\"status\":\"unreadable\"}}"),
        format!("{{\"url\":\"{u}/dir/b\",\"key_ids\":[\"{old}\"],\"status\":\"encrypted\"}}"),
        format!(
            "{{\"url\":\"{u}/new\",\"key_ids\":[\"{}\"],\"status\":\"encrypted\"}}",
            t.new_id
        ),
        format!("{{\"url\":\"{u}/plain\",\"key_ids\":[],\"status\":\"not-encrypted\"}}"),
    ];
    assert_eq!(lines, want);
}

/// The parsed `outcome` of every JSON line, by object name, and the summary.
fn outcomes(out: &Output, root: &str) -> (std::collections::BTreeMap<String, String>, String) {
    let text = stdout(out);
    let mut lines: Vec<&str> = text.lines().collect();
    let summary = lines.pop().expect("summary").to_owned();
    let map = lines
        .iter()
        .map(|l| {
            let rest = l.strip_prefix("{\"url\":\"").expect("url");
            let (url, rest) = rest.split_once("\",\"outcome\":\"").expect("outcome");
            let outcome = rest.split('"').next().expect("name");
            (
                url.strip_prefix(root).expect("under root").to_owned(),
                outcome.to_owned(),
            )
        })
        .collect();
    (map, summary)
}

#[test]
fn enc_reencrypt_rewrites_old_key_objects_and_is_idempotent() {
    let t = scanned();
    let a_before = std::fs::read(t.store.path().join("a")).expect("a");
    let args = |extra: &[&'static str]| {
        let mut v = vec![
            "--keyring",
            s(&t.new_ring),
            "enc",
            "reencrypt",
            &t.url,
            "--from",
            &t.old_id,
        ];
        v.extend_from_slice(extra);
        osc_bin(&v, &[])
    };

    // A dry run decides and writes nothing; the corrupt object is an error.
    let out = args(&["--dry-run"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let (map, summary) = outcomes(&out, &t.url);
    assert_eq!(map["/a"], "would-rewrite");
    assert_eq!(map["/dir/b"], "would-rewrite");
    assert_eq!(map["/new"], "skipped");
    assert_eq!(map["/plain"], "not-encrypted");
    assert_eq!(map["/bad"], "error");
    assert!(
        summary.contains("\"would-rewrite\":2") && summary.contains("\"error\":1"),
        "{summary}"
    );
    assert_eq!(
        std::fs::read(t.store.path().join("a")).expect("a"),
        a_before
    );

    // The corrupt object is removed; now everything succeeds.
    std::fs::remove_file(t.store.path().join("bad")).expect("rm");
    let out = args(&["--jobs", "1"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let (map, summary) = outcomes(&out, &t.url);
    assert_eq!(map["/a"], "rewritten");
    assert_eq!(map["/dir/b"], "rewritten");
    assert!(summary.contains("\"rewritten\":2"), "{summary}");
    assert_ne!(
        std::fs::read(t.store.path().join("a")).expect("a"),
        a_before
    );

    // Only the new key remains, and the data reads back.
    let out = osc_bin(&["enc", "scan", &t.url, "--key", &t.old_id], &[]);
    assert_eq!(stdout(&out), "");
    let dst = tempdir().expect("dst");
    let file = dst.path().join("a");
    let url = format!("{}/a", t.url);
    let out = osc_bin(&["--keyring", s(&t.new_ring), "get", &url, s(&file)], &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(std::fs::read(&file).expect("a"), b"alpha");

    // A second run has nothing left to do.
    let out = args(&[]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let (map, _) = outcomes(&out, &t.url);
    assert_eq!(map["/a"], "skipped");
    assert_eq!(map["/dir/b"], "skipped");
}

#[test]
fn enc_reencrypt_before_leaves_recent_objects_and_exits_1() {
    let t = scanned();
    std::fs::remove_file(t.store.path().join("bad")).expect("rm");
    let run = |before: &str| {
        osc_bin(
            &[
                "--keyring",
                s(&t.new_ring),
                "enc",
                "reencrypt",
                &t.url,
                "--from",
                &t.old_id,
                "--before",
                before,
            ],
            &[],
        )
    };
    let out = run("2000-01-01T00:00:00Z");
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("2 after the cutoff"),
        "{}",
        stderr(&out)
    );
    let (map, _) = outcomes(&out, &t.url);
    assert_eq!(map["/a"], "after-cutoff");
    assert_eq!(map["/new"], "skipped");

    let out = run("2999-01-01T00:00:00+02:00");
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(outcomes(&out, &t.url).0["/a"], "rewritten");

    let out = run("yesterday");
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("RFC 3339"), "{}", stderr(&out));
}

#[test]
fn enc_reencrypt_needs_a_keyring_and_refuses_the_active_key() {
    let t = scanned();
    let out = osc_bin(&["enc", "reencrypt", &t.url, "--from", &t.old_id], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(stdout(&out), "");
    assert!(stderr(&out).contains("needs a keyring"), "{}", stderr(&out));

    // The old keyring's active key is the one being moved away from.
    let out = osc_bin(
        &[
            "--keyring",
            s(&t.old_ring),
            "enc",
            "reencrypt",
            &t.url,
            "--from",
            &t.old_id,
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(stdout(&out), "");
    assert!(
        stderr(&out).contains("rotate the keyring first"),
        "{}",
        stderr(&out)
    );
}
