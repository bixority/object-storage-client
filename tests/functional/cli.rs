//! Functional tests of the `osc` CLI over `file://` stores.

use crate::support::Osc;
use chrono::NaiveDate;
use clap::{CommandFactory, FromArgMatches};
use object_storage_client::Error;
use object_storage_client::cli::{Cli, CliError};
use object_storage_client::encryption::{Keyring, new_keyring, rotate_keyring};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::{TempDir, tempdir};

/// Runs the CLI in-process.
async fn osc(args: &[&str]) -> Result<(), CliError> {
    let full = std::iter::once("osc").chain(args.iter().copied());
    // Without the env bindings, so an exported OSC_KEYRING_FILE or
    // OSC_REQUIRE_ENCRYPTION cannot leak in.
    let m = Cli::command()
        .mut_arg("keyring", |a| a.env(None))
        .mut_arg("require_encryption", |a| a.env(None))
        .try_get_matches_from(full)
        .expect("arguments parse");
    Cli::from_arg_matches(&m).expect("arguments").run().await
}

/// Runs the binary in a cleared environment plus `env`.
fn osc_bin(args: &[&str], env: &[(&str, &Path)]) -> Output {
    let mut cmd = Osc::new();
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("run osc")
}

/// Runs the binary with `stdin` (the null device when `None`), in a cleared
/// environment plus `env`.
fn osc_run(args: &[&str], stdin: Option<&[u8]>, env: &[(&str, &str)]) -> Output {
    use std::io::Write;
    use std::process::Stdio;
    let mut cmd = Osc::new();
    cmd.args(args)
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

#[test]
fn sign_refuses_an_expiry_outside_the_range() {
    for expires in ["0", "604801"] {
        let out = osc_bin(&["sign", "--expires-in", expires, "s3://b/k"], &[]);
        assert!(!out.status.success(), "{expires}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("7 days"), "{expires}: {stderr}");
    }
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
    let dst = work.path().join("dst");
    // `get` needs the keys; `exists` would not read the keyring at all.
    let err = osc(&["--keyring", s(&dir.join("missing")), "get", &url, s(&dst)])
        .await
        .expect_err("missing keyring");
    assert!(matches!(err, CliError::Key(_)), "{err:?}");

    let bad = dir.join("bad");
    write_ring(&bad, "not a keyring\n");
    let err = osc(&["--keyring", s(&bad), "get", &url, s(&dst)])
        .await
        .expect_err("bad keyring");
    assert!(err.to_string().contains("line 1"), "{err}");
    assert!(!dst.exists());
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
fn the_profile_flag_is_global_and_listed_in_help() {
    let cmd = Cli::command();
    let parse = |args: &[&str]| {
        cmd.clone()
            .try_get_matches_from(std::iter::once("osc").chain(args.iter().copied()))
    };
    for args in [
        &["--profile", "dev", "exists", "s3://b/k"][..],
        &["exists", "--profile", "dev", "s3://b/k"][..],
    ] {
        let m = parse(args).expect("parses");
        assert_eq!(
            m.get_one::<String>("profile").map(String::as_str),
            Some("dev")
        );
    }
    assert!(parse(&["--profile"]).is_err());

    // Rendered here, not by a child: the child's help is coloured when
    // CLICOLOR_FORCE is set.
    let help = Cli::command().render_long_help().to_string();
    assert!(help.contains("--profile <NAME>"), "{help}");
}

#[test]
fn the_version_flag_prints_the_package_version() {
    let out = osc_run(&["--version"], None, &[]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        concat!("osc ", env!("CARGO_PKG_VERSION"), "\n")
    );
    assert_eq!(
        Cli::command().render_version().trim_end(),
        concat!("osc ", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn every_subcommand_has_a_description() {
    fn check(cmd: &clap::Command, path: &str, seen: &mut usize) {
        for sub in cmd.get_subcommands() {
            let name = format!("{path} {}", sub.get_name());
            assert!(sub.get_about().is_some(), "{name} has no description");
            *seen += 1;
            check(sub, &name, seen);
        }
    }
    let mut seen = 0;
    check(&Cli::command(), "osc", &mut seen);
    // The object commands are among those checked, not an empty loop.
    assert!(seen >= 10, "{seen}");
}

#[test]
fn get_stream_help_states_the_exit_status_rule() {
    let cmd = Cli::command();
    let sub = cmd.find_subcommand("get-stream").expect("get-stream");
    let help = sub.clone().render_long_help().to_string();
    assert!(help.contains("only if osc exits 0"), "{help}");
    assert!(help.contains("pipefail"), "{help}");
}

#[test]
fn sign_method_is_validated_by_the_parser() {
    let parse = |method: &str| {
        Cli::command().try_get_matches_from(["osc", "sign", "--method", method, "s3://b/k"])
    };
    let err = parse("FOO").expect_err("bad method");
    assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    assert!(err.to_string().contains("DELETE"), "{err}");
    let m = parse("put").expect("a lower-case method parses");
    let (_, sign) = m.subcommand().expect("sign");
    assert_eq!(
        sign.get_one::<String>("method").map(String::as_str),
        Some("put")
    );

    // The binary reports it as a usage error.
    let out = osc_run(&["sign", "--method", "FOO", "s3://b/k"], None, &[]);
    assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));
}

#[test]
fn an_empty_profile_name_exits_1_before_any_backend() {
    for profile in ["", "  "] {
        let out = osc_run(&["--profile", profile, "exists", "s3://b/k"], None, &[]);
        assert_eq!(out.status.code(), Some(1), "{profile:?}");
        assert!(
            stderr(&out).contains("profile name is empty"),
            "{}",
            stderr(&out)
        );
    }
}

#[test]
fn require_encryption_without_a_key_source_fails() {
    let Setup { work, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"plain").expect("write");
    let dst = work.path().join("dst");
    let url = format!("file://{}/obj", work.path().display());
    std::fs::write(work.path().join("obj"), b"x").expect("obj");
    let env = [("OSC_REQUIRE_ENCRYPTION", "1")];

    let put_url = format!("file://{}/new", work.path().display());
    for args in [
        vec!["put", s(&src), &put_url],
        vec!["get", &url, s(&dst)],
        vec!["get-stream", &url],
        vec!["cp", &url, &put_url],
        vec!["mv", &url, &put_url],
        vec!["sign", "s3://b/k"],
    ] {
        let out = osc_run(&args, None, &env);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        assert!(
            stderr(&out).contains("--require-encryption is set"),
            "{args:?}: {}",
            stderr(&out)
        );
    }
    // Nothing was created or removed; the flag does the same as the variable.
    assert_eq!(names(work.path()), ["obj", "src"]);
    let out = osc_run(
        &["--require-encryption", "put", s(&src), &put_url],
        None,
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(!work.path().join("new").exists());
}

#[test]
fn require_encryption_with_a_keyring_stores_ciphertext() {
    let Setup { work, ring, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"plain").expect("write");
    let url = format!("file://{}/obj", work.path().display());
    let out = osc_run(
        &["put", s(&src), &url],
        None,
        &[
            ("OSC_REQUIRE_ENCRYPTION", "1"),
            ("OSC_KEYRING_FILE", s(&ring)),
        ],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        std::fs::read(work.path().join("obj")).expect("stored")[..3],
        [2, 4, 0x78]
    );
}

#[test]
fn require_encryption_zero_means_unset() {
    let Setup { work, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"plain").expect("write");
    let url = format!("file://{}/obj", work.path().display());
    let out = osc_run(
        &["put", s(&src), &url],
        None,
        &[("OSC_REQUIRE_ENCRYPTION", "0")],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        std::fs::read(work.path().join("obj")).expect("stored"),
        b"plain"
    );
}

#[test]
fn listing_ignores_require_encryption() {
    let Setup { work, .. } = setup();
    std::fs::write(work.path().join("obj"), b"x").expect("obj");
    let dir = format!("file://{}/", work.path().display());
    let obj = format!("file://{}/obj", work.path().display());
    let env = [("OSC_REQUIRE_ENCRYPTION", "1")];
    for args in [
        vec!["ls", &dir],
        vec!["exists", &obj],
        vec!["bucket-exists", &dir],
    ] {
        let out = osc_run(&args, None, &env);
        assert!(out.status.success(), "{args:?}: {}", stderr(&out));
    }
    let out = osc_run(&["ls", &dir], None, &env);
    assert_eq!(stdout(&out), "obj\n");
}

#[test]
fn the_require_encryption_flag_is_global_and_listed_in_help() {
    let help = Cli::command().render_long_help().to_string();
    assert!(help.contains("--require-encryption"), "{help}");
    let parse = |args: &[&str]| {
        Cli::command().try_get_matches_from(std::iter::once("osc").chain(args.iter().copied()))
    };
    for args in [
        &["--require-encryption", "ls", "s3://b/"][..],
        &["ls", "--require-encryption", "s3://b/"][..],
    ] {
        let m = parse(args).expect("parses");
        assert!(m.get_flag("require_encryption"));
    }
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
        &["keyring", "status", "--configured", "--keyring", s(&ring)],
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
fn the_core_limit_is_zero_before_keys_are_loaded() {
    let Setup { work, ring, .. } = setup();
    let src = work.path().join("src");
    std::fs::write(&src, b"payload").expect("write");
    let url = format!("file://{}/obj", work.path().display());
    // The command runs while the keys load and inherits osc's limits.
    let command = "echo core=$(ulimit -H -c) >&2; cat \"$KR\"";
    let ring = ring.display().to_string();
    let env = [("OSC_KEYRING_COMMAND", command), ("KR", ring.as_str())];

    let out = osc_run(&["put", s(&src), &url], None, &env);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stderr(&out).contains("core=0"), "{}", stderr(&out));
    assert_eq!(
        std::fs::read(work.path().join("obj")).expect("stored")[..3],
        [2, 4, 0x78]
    );
}

#[cfg(unix)]
#[test]
fn a_binary_download_still_gets_the_umask_mode() {
    use std::os::unix::fs::PermissionsExt;
    let w = tempdir().expect("work");
    let store = tempdir().expect("store");
    std::fs::write(store.path().join("obj"), b"payload").expect("obj");
    let url = format!("file://{}/obj", store.path().display());
    let dst = w.path().join("dst");
    let witness = w.path().join("witness");
    std::fs::write(&witness, b"w").expect("witness");

    let out = osc_run(&["get", &url, s(&dst)], None, &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    let mode = |p: &Path| std::fs::metadata(p).expect("meta").permissions().mode();
    assert_eq!(mode(&dst), mode(&witness));
    assert_eq!(std::fs::read(&dst).expect("dst"), b"payload");
}

#[cfg(unix)]
#[test]
fn keyring_command_failures_are_errors_that_do_not_echo_the_output() {
    let url = "file:///nonexistent/x";
    // `get` needs the keys; `exists` would not run the command at all.
    let run = |command: &str| {
        let out = osc_run(
            &["get", url, "/nonexistent/dst"],
            None,
            &[("OSC_KEYRING_COMMAND", command)],
        );
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

#[cfg(unix)]
#[test]
fn keyless_commands_never_run_the_key_command() {
    let Setup { work, ring, .. } = setup();
    let mark = work.path().join("mark");
    let (mark_s, ring_s) = (mark.display().to_string(), ring.display().to_string());
    let command = "touch \"$MARK\"; cat \"$KR\"";
    let env = [
        ("OSC_KEYRING_COMMAND", command),
        ("MARK", mark_s.as_str()),
        ("KR", ring_s.as_str()),
    ];
    let run = |args: &[&str]| {
        let out = osc_run(args, None, &env);
        assert!(out.status.success(), "{args:?}: {}", stderr(&out));
    };

    let bucket = work.path().join("bucket");
    let url = format!("file://{}/obj", work.path().display());
    let bucket_url = format!("file://{}/", bucket.display());
    std::fs::write(work.path().join("obj"), b"x").expect("obj");
    run(&["ls", &format!("file://{}/", work.path().display())]);
    run(&["exists", &url]);
    run(&["rm", &url]);
    run(&["mb", &bucket_url]);
    run(&["bucket-exists", &bucket_url]);
    assert!(!mark.exists(), "a keyless command ran the key command");

    // Positive control: the commands that need keys do run it.
    let src = work.path().join("src");
    std::fs::write(&src, b"payload").expect("src");
    run(&["put", s(&src), &url]);
    assert!(mark.exists());
    std::fs::remove_file(&mark).expect("reset");
    let dst = work.path().join("dst");
    run(&["get", &url, s(&dst)]);
    assert!(mark.exists());
    assert_eq!(std::fs::read(&dst).expect("dst"), b"payload");
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
fn keyring_add_and_activate_round_trip() {
    let first = osc_run(&["keyring", "new"], None, &[]);
    let old = keyring_of(&first).status().active;

    let added = osc_run(
        &["keyring", "add", "--prefix", "next"],
        Some(&first.stdout),
        &[],
    );
    let status = keyring_of(&added).status();
    assert_eq!(status.active, old, "add keeps the old key active");
    assert_eq!(status.keys.len(), 2);
    let new = status.keys.last().expect("new key").id.clone();
    assert!(new.starts_with("next-"), "{new}");
    let note = stderr(&added);
    assert_eq!(note, format!("added inactive key {new}\n"));
    assert!(!note.contains("material="), "{note}");

    let activated = osc_run(&["keyring", "activate", &new], Some(&added.stdout), &[]);
    let status = keyring_of(&activated).status();
    assert_eq!(status.active, new);
    assert_eq!(status.keys.len(), 2);

    let out = osc_run(
        &["keyring", "activate", "nope", "--force"],
        Some(&added.stdout),
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("nope"), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
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
    let out = osc_run(
        &["keyring", "status", "--max-age-days", "100000"],
        Some(fresh.as_bytes()),
        &[],
    );
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
    let rotated = rotate_keyring(&fresh, "kek", day(2026, 10, 27)).expect("rotate");
    let out = osc_run(
        &["keyring", "status", "--max-age-days", "100000"],
        Some(rotated.as_bytes()),
        &[],
    );
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

/// The limit is relative to today. The one-day margin absorbs a UTC midnight
/// between the test and the child.
#[test]
fn keyring_status_age_limit_is_relative_to_today() {
    let created = chrono::Utc::now().date_naive() - chrono::Days::new(10);
    let ring = new_keyring("kek", created).expect("keyring");
    let status = |limit: &str| {
        osc_run(
            &["keyring", "status", "--max-age-days", limit],
            Some(ring.as_bytes()),
            &[],
        )
    };
    let out = status("11");
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let out = status("9");
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert!(stderr(&out).contains("days old"), "{}", stderr(&out));
}

#[test]
fn status_reads_the_piped_keyring() {
    let Setup { ring, id, .. } = setup();
    let (piped, piped_id) = new_ring("piped");
    assert_ne!(id, piped_id);
    // A source is configured, but standard input wins.
    let out = osc_run(
        &["keyring", "status", "--max-age-days", "100000"],
        Some(piped.as_bytes()),
        &[("OSC_KEYRING_FILE", s(&ring))],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).contains(&piped_id), "{}", stdout(&out));
    assert!(!stdout(&out).contains(&id), "{}", stdout(&out));

    // Nothing piped in (the null device) is an empty keyring, not the
    // configured one.
    let out = osc_run(
        &["keyring", "status"],
        None,
        &[("OSC_KEYRING_FILE", s(&ring))],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("empty keyring"), "{}", stderr(&out));
}

#[test]
fn status_configured_checks_the_configured_source() {
    let Setup { ring, id, .. } = setup();
    let (piped, piped_id) = new_ring("piped");
    let args = [
        "keyring",
        "status",
        "--configured",
        "--max-age-days",
        "100000",
    ];
    let listed = |out: &Output| {
        assert!(out.status.success(), "{}", stderr(out));
        let text = stdout(out);
        assert!(text.contains(&id), "{text}");
        assert!(!text.contains(&piped_id), "{text}");
    };

    // The piped keyring is ignored.
    listed(&osc_run(
        &args,
        Some(piped.as_bytes()),
        &[("OSC_KEYRING_FILE", s(&ring))],
    ));
    let mut with_flag = args.to_vec();
    with_flag.extend(["--keyring", s(&ring)]);
    listed(&osc_run(&with_flag, Some(piped.as_bytes()), &[]));

    #[cfg(unix)]
    {
        let command = format!("cat '{}'", ring.display());
        listed(&osc_run(
            &args,
            Some(piped.as_bytes()),
            &[("OSC_KEYRING_COMMAND", command.as_str())],
        ));
    }

    // Without a source it is an error, not a fall back to standard input.
    let out = osc_run(&args, Some(piped.as_bytes()), &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("needs a key source"),
        "{}",
        stderr(&out)
    );
    assert!(!stdout(&out).contains(&piped_id));
}

#[cfg(unix)]
#[test]
fn keyring_files_with_open_permissions_are_refused() {
    use std::os::unix::fs::PermissionsExt;
    let Setup { ring, id, .. } = setup();
    std::fs::set_permissions(&*ring, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    let out = osc_run(
        &[
            "keyring",
            "status",
            "--configured",
            "--keyring",
            s(&ring),
            "--max-age-days",
            "100000",
        ],
        None,
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("has mode 644"), "{}", stderr(&out));
    assert!(!stdout(&out).contains(&id));
    // Group read is fine.
    std::fs::set_permissions(&*ring, std::fs::Permissions::from_mode(0o640)).expect("chmod");
    let out = osc_run(
        &[
            "keyring",
            "status",
            "--configured",
            "--keyring",
            s(&ring),
            "--max-age-days",
            "100000",
        ],
        None,
        &[],
    );
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

    osc(&["put", "-r", s(&src), &url])
        .await
        .expect("put -r stores the names inside the destination");

    // Nothing outside the destination directory.
    assert!(!w.path().join("pwned").exists());
    assert!(!w.path().join("t/pwned").exists());
    assert!(!w.path().join("t/dst/pwned").exists());
    assert!(!w.path().join("t/dst/inner/pwned").exists());
    for entry in std::fs::read_dir(w.path().join("t")).expect("t") {
        assert_eq!(entry.expect("e").file_name(), "dst");
    }
    assert_eq!(std::fs::read(dst.join("sub/ok.txt")).expect("ok"), b"ok");
    assert_eq!(
        std::fs::read(dst.join("..\\..\\pwned")).expect("evil"),
        b"evil"
    );
    assert_eq!(std::fs::read(dst.join("%2e%2e")).expect("pct"), b"pct");
}

/// How long a signal test waits for the child to reach the state it signals.
#[cfg(unix)]
const IN_FLIGHT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Polls until `reached()` holds. Panics, rather than skipping, when the child
/// exits first or the deadline passes: the in-flight state is what the test is
/// about.
#[cfg(unix)]
fn wait_in_flight(child: &mut std::process::Child, what: &str, reached: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + IN_FLIGHT_DEADLINE;
    loop {
        if reached() {
            return;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("osc exited ({status:?}) before {what}");
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{what} not reached within {IN_FLIGHT_DEADLINE:?}");
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

/// Sends `signal` (`-INT`, `-TERM`) to the child and returns its exit code,
/// after checking that it handled the signal instead of dying on it.
#[cfg(unix)]
fn signal_and_wait(child: &mut std::process::Child, signal: &str) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    let status = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let exit = child.wait().expect("wait");
    assert!(exit.signal().is_none(), "died on the signal: {exit:?}");
    exit.code().expect("exit code")
}

/// A loopback server for one request: it reads the request head, answers 200
/// with a `Content-Length` of 4 MiB and 1 MiB of body, then keeps the
/// connection open until the client closes it.
#[cfg(unix)]
fn serve_stalled_object() -> (u16, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};
    const SENT: usize = 1 << 20;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => head.extend_from_slice(&buf[..n]),
            }
        }
        let header = "HTTP/1.1 200 OK\r\nContent-Length: 4194304\r\nConnection: close\r\n\r\n";
        if stream.write_all(header.as_bytes()).is_err()
            || stream.write_all(&vec![7u8; SENT]).is_err()
        {
            return;
        }
        // Until the client closes the connection (or dies).
        while matches!(stream.read(&mut buf), Ok(n) if n > 0) {}
    });
    (port, thread)
}

/// Starts `osc get` of an object whose server stalls after 1 MiB, sends
/// `signal` once the temp file holds data, and checks the clean-up. Returns
/// the exit code.
#[cfg(unix)]
fn interrupt_get(signal: &str) -> i32 {
    let out = tempdir().expect("out");
    let (port, server) = serve_stalled_object();
    let url = format!("http://127.0.0.1:{port}/obj");
    let dst = out.path().join("plain.bin");

    let mut osc = Osc::new();
    let mut child = osc
        .args(["get", &url, s(&dst)])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn osc");

    wait_in_flight(&mut child, "a temp file holding data", || {
        std::fs::read_dir(out.path())
            .expect("read dir")
            .flatten()
            .any(|e| {
                e.file_name().to_string_lossy().contains(".osc-tmp-")
                    && e.metadata().is_ok_and(|m| m.len() > 0)
            })
    });
    let code = signal_and_wait(&mut child, signal);
    assert!(!dst.exists(), "destination must not exist");
    assert_eq!(names(out.path()), Vec::<String>::new(), "temp file left");
    // The child closed the connection, so the server thread ends.
    server.join().expect("server");
    code
}

#[cfg(unix)]
#[test]
fn interrupted_get_removes_the_temp_file_on_sigint() {
    assert_eq!(interrupt_get("-INT"), 130);
}

#[cfg(unix)]
#[test]
fn interrupted_get_removes_the_temp_file_on_sigterm() {
    assert_eq!(interrupt_get("-TERM"), 143);
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
    // A closed loopback port: the request fails at once and nothing leaves the
    // machine. (`file://` URLs cannot carry credentials, so this uses `s3://`.)
    let out = osc_run(
        &[
            "get",
            "s3://u5erTOKEN:s3cret@bucket/nonexistent?token=TOK#FRAGX9",
            "/nonexistent-dir/x",
        ],
        None,
        &[
            ("S3_ENDPOINT", "http://127.0.0.1:1"),
            ("S3_ACCESS_KEY_ID", "AKIDTEST"),
            ("S3_SECRET_ACCESS_KEY", "SECRETTEST"),
            ("S3_REGION", "us-east-1"),
        ],
    );
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(all.contains("Download "), "{all}");
    for secret in ["u5erTOKEN", "s3cret", "TOK", "FRAGX9"] {
        assert!(!all.contains(secret), "{secret} leaked: {all}");
    }
}

/// Starts `osc put` of a FIFO to a `file://` store, feeds it 1 MiB and keeps
/// the writer open, sends SIGINT once a staging file (`path#N`) holds data and
/// checks the clean-up.
#[cfg(unix)]
#[test]
fn interrupted_put_removes_the_staging_file_on_sigint() {
    use std::io::Write;
    let src_dir = tempdir().expect("src");
    let store = tempdir().expect("store");
    let fifo = src_dir.path().join("big");
    let status = Command::new("mkfifo").arg(&fifo).status().expect("mkfifo");
    assert!(status.success());
    let url = format!("file://{}/big", store.path().display());

    let mut osc = Osc::new();
    let mut child = osc
        .args(["put", s(&fifo), &url])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn osc");

    // Opened for writing on a thread, so that an osc that never opens the FIFO
    // cannot hang the test. The writer stays open until `release` is dropped.
    let (release, held) = std::sync::mpsc::channel::<()>();
    let writer = std::thread::spawn(move || {
        let mut pipe = std::fs::OpenOptions::new()
            .write(true)
            .open(fifo)
            .expect("open fifo");
        // An error means osc is gone; the test reports that.
        let _ = pipe.write_all(&vec![7u8; 1 << 20]);
        let _ = held.recv();
    });

    wait_in_flight(&mut child, "a staging file holding data", || {
        std::fs::read_dir(store.path())
            .expect("read dir")
            .flatten()
            .any(|e| {
                e.file_name().to_string_lossy().contains('#')
                    && e.metadata().is_ok_and(|m| m.len() > 0)
            })
    });
    assert_eq!(signal_and_wait(&mut child, "-INT"), 130);
    assert_eq!(names(store.path()), Vec::<String>::new(), "staging left");
    drop(release);
    writer.join().expect("writer");
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

    let mut osc = Osc::new();
    let mut child = osc
        .args(["get", &url, s(&fifo)])
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
    let w = tempdir().expect("work");
    let src = w.path().join("src");
    std::fs::create_dir_all(&src).expect("src");
    let data = sample(9 * 1024 * 1024);
    for i in 0..16 {
        std::fs::write(src.join(format!("f{i:02}.bin")), &data).expect("file");
    }
    let dst = w.path().join("dst");
    let url = format!("file://{}", dst.display());
    // A directory where f07.bin must go: the local write stages f07.bin#1 and
    // the final rename onto a directory fails with EISDIR for every uid
    // (unlike an unreadable source, which root can read).
    std::fs::create_dir_all(dst.join("f07.bin")).expect("blocking directory");

    let out = osc_bin(&["put", "-r", s(&src), &url], &[]);
    assert!(!out.status.success(), "put -r must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("f07.bin"),
        "stderr names the unreadable file"
    );
    // Whatever was stored is complete; nothing partial or staged remains.
    let stored = names(&dst);
    assert!(
        !stored.iter().any(|n| n.contains('#')),
        "staging file left: {stored:?}"
    );
    assert!(
        dst.join("f07.bin").is_dir(),
        "the blocking directory is intact"
    );
    for name in stored.iter().filter(|n| *n != "f07.bin") {
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
    // The unreadable object makes the scan incomplete.
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
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
    assert_eq!(rows.len(), 5, "no listing-errors row without one: {text}");
    let err = stderr(&out);
    assert!(
        err.contains("unreadable: ") && err.contains("/bad: "),
        "{err}"
    );
    assert!(
        err.contains("scan incomplete: 1 objects could not be read"),
        "{err}"
    );
}

#[test]
fn enc_scan_key_prints_the_urls_of_that_key() {
    let t = scanned();
    // With the unreadable object present the scan is incomplete.
    let out = osc_bin(&["enc", "scan", &t.url, "--key", &t.old_id], &[]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(stderr(&out).contains("unreadable: "), "{}", stderr(&out));

    std::fs::remove_file(t.store.path().join("bad")).expect("rm");
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
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let mut lines: Vec<String> = stdout(&out).lines().map(str::to_owned).collect();
    let summary = lines.pop().expect("summary");
    assert_eq!(
        summary,
        format!(
            "{{\"summary\":{{\"{}\":1,\"{}\":2,\"no-key\":0,\"not-encrypted\":1,\"unreadable\":1,\"listing-errors\":0}}}}",
            t.new_id, t.old_id
        )
    );
    lines.sort();
    let u = &t.url;
    let old = &t.old_id;
    // The unreadable line carries the reason.
    let bad_at = lines.iter().position(|l| l.contains("/bad")).expect("bad");
    let bad = lines.remove(bad_at);
    let bad_head =
        format!("{{\"url\":\"{u}/bad\",\"key_ids\":[],\"status\":\"unreadable\",\"error\":\"");
    assert!(bad.starts_with(&bad_head) && bad.ends_with("\"}"), "{bad}");
    let want = [
        format!("{{\"url\":\"{u}/a\",\"key_ids\":[\"{old}\"],\"status\":\"encrypted\"}}"),
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

/// Every file under `dir` with its bytes, by path relative to `dir`.
fn tree_bytes(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut std::collections::BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path.strip_prefix(root).expect("under root");
                out.insert(
                    rel.to_string_lossy().into_owned(),
                    std::fs::read(&path).expect("read"),
                );
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

#[test]
fn enc_reencrypt_refuses_an_unknown_from_key() {
    let t = scanned();
    let before = tree_bytes(t.store.path());

    // A mistyped ID.
    let out = osc_bin(
        &[
            "--keyring",
            s(&t.new_ring),
            "enc",
            "reencrypt",
            &t.url,
            "--from",
            "nope",
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
    assert!(
        stderr(&out).contains("no key \"nope\" in the keyring"),
        "{}",
        stderr(&out)
    );

    // A keyring that lacks the old key.
    let other = tempdir().expect("other");
    let (text, _) = new_ring("other");
    let other_ring = other.path().join("ring");
    write_ring(&other_ring, &text);
    let out = osc_bin(
        &[
            "--keyring",
            s(&other_ring),
            "enc",
            "reencrypt",
            &t.url,
            "--from",
            &t.old_id,
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert_eq!(stdout(&out), "");
    assert!(
        stderr(&out).contains(&format!("no key \"{}\" in the keyring", t.old_id)),
        "{}",
        stderr(&out)
    );

    assert_eq!(tree_bytes(t.store.path()), before, "nothing was rewritten");
}

/// The local store cannot list a name with a control character: the listing
/// fails, and the walk must say so and exit 1 instead of ending quietly.
#[cfg(unix)]
#[test]
fn enc_walks_report_a_listing_error() {
    let t = scanned();
    std::fs::remove_file(t.store.path().join("bad")).expect("rm");
    std::fs::write(t.store.path().join("odd\x1bname"), b"x").expect("write");

    let out = osc_bin(&["enc", "scan", &t.url, "--json"], &[]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let summary = stdout(&out).lines().last().expect("summary").to_owned();
    assert!(summary.contains("\"listing-errors\":1"), "{summary}");
    assert!(stderr(&out).contains("\\u{1b}"), "{}", stderr(&out));
    assert!(!stderr(&out).contains('\x1b'), "{:?}", stderr(&out));

    let table = osc_bin(&["enc", "scan", &t.url], &[]);
    assert_eq!(table.status.code(), Some(1), "{}", stderr(&table));
    assert!(
        stdout(&table).contains("(listing errors)"),
        "{}",
        stdout(&table)
    );

    let out = osc_bin(
        &[
            "--keyring",
            s(&t.new_ring),
            "enc",
            "reencrypt",
            &t.url,
            "--from",
            &t.old_id,
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let summary = stdout(&out).lines().last().expect("summary").to_owned();
    assert!(summary.contains("\"listing-errors\":1"), "{summary}");
    assert!(stderr(&out).contains("\\u{1b}"), "{}", stderr(&out));
}

/// Starts `osc` with a minimal environment of its own and both output streams
/// piped, so the test can close the reader of standard output.
#[cfg(unix)]
fn spawn_piped(args: &[&str], home: &Path) -> std::process::Child {
    use std::process::Stdio;
    Osc::new()
        .args(args)
        .env("HOME", home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn osc")
}

/// Reads `lines` lines from the child's standard output, closes it, and
/// returns the exit status with what the child wrote to standard error.
#[cfg(unix)]
fn close_stdout_after(
    mut child: std::process::Child,
    lines: usize,
) -> (std::process::ExitStatus, String) {
    use std::io::{BufRead, BufReader, Read};
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));
    for _ in 0..lines {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).expect("read line"), 0, "output");
    }
    drop(reader);
    let status = child.wait().expect("wait");
    let mut err = String::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut err)
        .expect("stderr text");
    (status, err)
}

#[cfg(unix)]
#[test]
fn ls_into_a_closed_pipe_exits_141() {
    use std::os::unix::process::ExitStatusExt;
    let dir = tempdir().expect("dir");
    let home = tempdir().expect("home");
    // Far more output than a 64 KiB pipe holds.
    for i in 0..2000 {
        let name = format!("{i:04}-{}", "n".repeat(145));
        std::fs::write(dir.path().join(name), b"").expect("write");
    }

    let (status, err) = close_stdout_after(spawn_piped(&["ls", s(dir.path())], home.path()), 1);
    assert_eq!(status.signal(), None, "{status:?}");
    assert_eq!(status.code(), Some(141), "{err}");
    assert_eq!(err, "", "no panic message or error line");
}

#[cfg(unix)]
#[test]
fn recursive_put_into_a_closed_pipe_cleans_up() {
    let src = tempdir().expect("src");
    let dst = tempdir().expect("dst");
    let home = tempdir().expect("home");
    for i in 0..16u8 {
        let file = std::fs::File::create(src.path().join(format!("f{i:02}"))).expect("create");
        file.set_len(9 * 1024 * 1024).expect("size");
        std::os::unix::fs::FileExt::write_at(&file, &[i + 1; 16], 0).expect("head");
    }
    let url = format!("file://{}", dst.path().display());

    let child = spawn_piped(&["put", "-r", s(src.path()), &url], home.path());
    let (status, err) = close_stdout_after(child, 3);
    assert_eq!(status.code(), Some(141), "{err}");

    let stored = tree_bytes(dst.path());
    for (name, bytes) in &stored {
        assert!(!name.contains('#'), "staging file left behind: {name}");
        let source = std::fs::read(src.path().join(name)).expect("source");
        assert!(*bytes == source, "{name} differs from its source");
    }
}

/// A URL or path that names a prefix (it ends in `/`) is refused by the
/// commands that address one object, instead of being rewritten to `dir`.
#[tokio::test]
async fn object_commands_refuse_a_url_naming_a_prefix() {
    fn refused(result: Result<(), CliError>, what: &str) {
        let err = result.expect_err(what);
        assert!(
            matches!(err, CliError::Storage(Error::InvalidInput(_))),
            "{what}: {err:?}"
        );
    }
    let work = tempdir().expect("work");
    let store = work.path().join("store");
    std::fs::create_dir_all(store.join("obj")).expect("store");
    std::fs::write(store.join("a"), b"alpha").expect("a");
    std::fs::write(store.join("obj/inner"), b"inner").expect("inner");
    let src = work.path().join("src.txt");
    std::fs::write(&src, b"payload").expect("src");
    let dst = work.path().join("dst");
    let before = tree_bytes(&store);
    let root = format!("file://{}", store.display());

    for target in [format!("{root}/dir/"), format!("{}/dir/", s(&store))] {
        refused(osc(&["put", s(&src), &target]).await, &target);
    }
    assert!(!store.join("dir").exists(), "nothing was created");
    refused(osc(&["rm", &format!("{root}/")]).await, "rm");
    refused(osc(&["get", &format!("{root}/obj/"), s(&dst)]).await, "get");
    refused(
        osc(&["cp", &format!("{root}/a"), &format!("{root}/b/")]).await,
        "cp",
    );
    assert!(!dst.exists());
    assert_eq!(tree_bytes(&store), before, "the store is intact");

    // A recursive upload still treats its destination as a prefix.
    let tree = work.path().join("tree");
    std::fs::create_dir_all(&tree).expect("tree");
    std::fs::write(tree.join("x"), b"x").expect("x");
    osc(&["put", "-r", s(&tree), &format!("{root}/pfx/")])
        .await
        .expect("recursive put");
    assert_eq!(std::fs::read(store.join("pfx/x")).expect("x"), b"x");
}

/// A loopback server that answers every request with an empty 403 and counts
/// the connections it accepted, so an unsigned request fails fast and visibly.
struct DenyingServer {
    port: u16,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    seen: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DenyingServer {
    fn start() -> Self {
        use std::io::{Read, Write};
        use std::sync::atomic::Ordering;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (stop_flag, count) = (stop.clone(), seen.clone());
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
                count.fetch_add(1, Ordering::SeqCst);
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let _ = stream.write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self {
            port,
            stop,
            seen,
            thread: Some(thread),
        }
    }

    /// Stops the server and returns how many connections it accepted.
    fn finish(mut self) -> usize {
        use std::sync::atomic::Ordering;
        self.stop.store(true, Ordering::SeqCst);
        // Wakes the blocked accept so the thread sees the flag.
        let _ = std::net::TcpStream::connect(("127.0.0.1", self.port));
        self.thread.take().expect("thread").join().expect("join");
        self.seen.load(Ordering::SeqCst)
    }
}

#[test]
fn every_s3_command_uses_the_profile() {
    let home = tempdir().expect("home");
    std::fs::write(home.path().join("config"), "").expect("config");
    std::fs::write(home.path().join("credentials"), "").expect("credentials");

    let keys = tempdir().expect("keys");
    let (text, inactive) = new_ring("old");
    let rotated = rotate_keyring(&text, "new", day(2026, 10, 27)).expect("rotate");
    let ring = keys.path().join("ring");
    write_ring(&ring, &rotated);
    let ring = s(&ring);
    let out_file = keys.path().join("out");
    let out_file = s(&out_file);

    let server = DenyingServer::start();
    let endpoint = format!("http://127.0.0.1:{}", server.port);
    let runs: [Vec<&str>; 6] = [
        vec!["exists", "s3://bkt/k"],
        vec!["--keyring", ring, "get", "s3://bkt/k", out_file],
        vec!["sign", "s3://bkt/k"],
        vec!["enc", "inspect", "s3://bkt/k"],
        vec!["enc", "scan", "s3://bkt/p/"],
        vec![
            "--keyring",
            ring,
            "enc",
            "reencrypt",
            "s3://bkt/p/",
            "--from",
            &inactive,
        ],
    ];
    for run in &runs {
        let out = Osc::new()
            .env("HOME", home.path())
            .env("AWS_CONFIG_FILE", home.path().join("config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                home.path().join("credentials"),
            )
            .env("S3_ENDPOINT", &endpoint)
            .args(["--profile", "nope"])
            .args(run)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("run osc");
        let err = stderr(&out);
        assert_eq!(out.status.code(), Some(1), "{run:?}: {err}");
        assert!(err.contains("AWS profile `nope`"), "{run:?}: {err}");
        assert!(!err.contains("169.254.169.254"), "{run:?}: {err}");
    }
    assert_eq!(server.finish(), 0, "a request was sent unsigned");
}

/// Runs `osc <flags> sign s3://bkt/k` against a home with two profiles, the
/// way a user would: `env` is everything the child sees besides `HOME` and the
/// AWS file locations. Fails, instead of hanging, if the child does not finish
/// (an IMDS lookup) within 30 s.
fn sign_in(home: &Path, flags: &[&str], env: &[(&str, &str)]) -> Output {
    use std::io::Read;
    use std::process::Stdio;
    use std::time::{Duration, Instant};
    let mut osc = Osc::new();
    osc.args(flags)
        .args(["sign", "s3://bkt/k"])
        .env("HOME", home)
        .env("AWS_CONFIG_FILE", home.join("config"))
        .env("AWS_SHARED_CREDENTIALS_FILE", home.join("credentials"))
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = osc.spawn().expect("spawn osc");
    // Drained on threads so a full pipe cannot stall the child.
    let mut out_pipe = child.stdout.take().expect("stdout");
    let mut err_pipe = child.stderr.take().expect("stderr");
    let out_reader = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = out_pipe.read_to_end(&mut v);
        v
    });
    let err_reader = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = err_pipe.read_to_end(&mut v);
        v
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("osc sign did not finish within 30 s");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    Output {
        status,
        stdout: out_reader.join().expect("stdout reader"),
        stderr: err_reader.join().expect("stderr reader"),
    }
}

#[test]
fn profile_flag_signs_with_the_profile_keys_and_region() {
    let home = tempdir().expect("home");
    std::fs::write(
        home.path().join("config"),
        "[profile dev]\naws_access_key_id = AKIDPROFILEDEV\n\
         aws_secret_access_key = SECRETPROFILEDEV\nregion = eu-west-3\n\
         [profile other]\naws_access_key_id = AKIDOTHER\n\
         aws_secret_access_key = SECRETOTHER\n",
    )
    .expect("config");
    std::fs::write(home.path().join("credentials"), "").expect("credentials");

    let signs_with = |env: &[(&str, &str)], key: &str, region: &str| {
        let out = sign_in(home.path(), &["--profile", "dev"], env);
        let (so, se) = (stdout(&out), stderr(&out));
        assert_eq!(out.status.code(), Some(0), "{env:?}: {se}");
        assert!(
            so.contains(&format!("X-Amz-Credential={key}%2F")),
            "{env:?}: {so}"
        );
        assert!(
            so.contains(&format!("%2F{region}%2Fs3%2Faws4_request")),
            "{env:?}: {so}"
        );
        for secret in ["SECRETPROFILEDEV", "SECRETOTHER", "SECRETSTATIC"] {
            assert!(!so.contains(secret) && !se.contains(secret), "{env:?}");
        }
    };

    signs_with(&[], "AKIDPROFILEDEV", "eu-west-3");
    // AWS_PROFILE never overrides the flag.
    signs_with(&[("AWS_PROFILE", "other")], "AKIDPROFILEDEV", "eu-west-3");
    // The flag's profile is the only credential source (D2): static keys do not
    // take over.
    signs_with(
        &[
            ("AWS_ACCESS_KEY_ID", "AKIDSTATIC"),
            ("AWS_SECRET_ACCESS_KEY", "SECRETSTATIC"),
        ],
        "AKIDPROFILEDEV",
        "eu-west-3",
    );
    signs_with(
        &[
            ("S3_ACCESS_KEY_ID", "AKIDSTATIC"),
            ("S3_SECRET_ACCESS_KEY", "SECRETSTATIC"),
        ],
        "AKIDPROFILEDEV",
        "eu-west-3",
    );
    // A region variable wins over the profile's region, which is the fallback.
    signs_with(
        &[("AWS_REGION", "us-west-2")],
        "AKIDPROFILEDEV",
        "us-west-2",
    );
}

#[test]
fn without_the_profile_flag_aws_profile_is_never_read() {
    let home = tempdir().expect("home");
    std::fs::write(
        home.path().join("config"),
        "[profile dev]\naws_access_key_id = AKIDPROFILEDEV\n\
         aws_secret_access_key = SECRETPROFILEDEV\nregion = eu-west-3\n",
    )
    .expect("config");
    std::fs::write(home.path().join("credentials"), "").expect("credentials");

    let out = sign_in(
        home.path(),
        &[],
        &[
            ("AWS_PROFILE", "dev"),
            ("AWS_ACCESS_KEY_ID", "AKIDSTATIC"),
            ("AWS_SECRET_ACCESS_KEY", "SECRETSTATIC"),
            ("AWS_REGION", "us-west-2"),
        ],
    );
    let (so, se) = (stdout(&out), stderr(&out));
    assert_eq!(out.status.code(), Some(0), "{se}");
    assert!(so.contains("X-Amz-Credential=AKIDSTATIC%2F"), "{so}");
    assert!(so.contains("%2Fus-west-2%2Fs3%2Faws4_request"), "{so}");
    assert!(!so.contains("AKIDPROFILEDEV"), "{so}");
}

#[test]
fn errors_print_each_cause_once() {
    let dir = tempdir().expect("dir");
    let base = dir.path().display();

    // A library error embeds its source: one line, no repeated cause.
    let out = osc_run(
        &[
            "get",
            &format!("file://{base}/nope"),
            &format!("{base}/out"),
        ],
        None,
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    let text = stderr(&out);
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(text.starts_with("Error: "), "{text}");

    // A cause that adds information is kept.
    let out = osc_run(
        &[
            "put",
            &format!("{base}/missing"),
            &format!("file://{base}/x"),
        ],
        None,
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    let text = stderr(&out);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "{text}");
    assert!(
        lines[0].starts_with("Error: failed to read ") && lines[0].ends_with("/missing"),
        "{text}"
    );
    assert!(
        lines[1].starts_with("  caused by: ") && lines[1].contains("(os error 2)"),
        "{text}"
    );
}

/// Runs `osc put src <dst>` in `cwd` with the child's `HOME` as given.
fn put_from(cwd: &Path, home: &Path, src: &Path, dst: &str) -> Output {
    let mut cmd = Osc::new();
    cmd.env("HOME", home)
        .current_dir(cwd)
        .args(["put", s(src), dst]);
    cmd.output().expect("run osc")
}

#[test]
fn operands_resolve_as_in_the_library() {
    let cwd = tempdir().expect("cwd");
    let home = tempdir().expect("home");
    let src = cwd.path().join("src");
    std::fs::write(&src, b"data").expect("write");

    std::fs::create_dir(cwd.path().join("rel")).expect("dir");
    let out = put_from(cwd.path(), home.path(), &src, "rel/obj");
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        std::fs::read(cwd.path().join("rel/obj")).expect("obj"),
        b"data"
    );

    // A scheme the library does not know is refused, not taken as a file name.
    let out = put_from(cwd.path(), home.path(), &src, "ab:c");
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("ab"), "{}", stderr(&out));
    assert!(!cwd.path().join("ab:c").exists());
    assert_eq!(names(cwd.path()), ["rel", "src"]);

    let out = put_from(cwd.path(), home.path(), &src, "./ab:c");
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        std::fs::read(cwd.path().join("ab:c")).expect("obj"),
        b"data"
    );

    // The literal `~/x` (no shell) is under the home directory.
    let out = put_from(cwd.path(), home.path(), &src, "~/x");
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(std::fs::read(home.path().join("x")).expect("obj"), b"data");
    assert!(!cwd.path().join("~").exists());
}
