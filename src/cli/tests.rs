use super::*;

#[cfg(unix)]
mod keyring_command {
    use super::super::keyring::run_command;
    use super::*;
    use std::ffi::OsStr;
    use std::time::Instant;
    use tempfile::tempdir;

    const LONG: Duration = Duration::from_secs(30);

    async fn run(command: &str, timeout: Duration) -> Result<crate::encryption::Keyring, CliError> {
        run_command(OsStr::new(command), timeout).await
    }

    #[tokio::test]
    async fn a_command_that_prints_a_keyring_is_parsed() {
        let text =
            crate::encryption::new_keyring("kek", chrono::NaiveDate::default()).expect("keyring");
        let dir = tempdir().expect("dir");
        let file = dir.path().join("ring");
        std::fs::write(&file, text.as_bytes()).expect("write");
        let ring = run(&format!("cat '{}'", file.display()), LONG)
            .await
            .expect("run");
        assert_eq!(ring.status().keys.len(), 1);
    }

    #[tokio::test]
    async fn a_command_that_outlives_the_timeout_is_killed() {
        let start = Instant::now();
        let err = run("sleep 20", Duration::from_millis(200))
            .await
            .expect_err("timeout");
        assert!(matches!(err, CliError::KeyringCommandTimeout(_)), "{err:?}");
        assert!(start.elapsed() < Duration::from_secs(10));

        // One that prints and then keeps running is also cut off.
        let err = run("echo osc-keyring-v1; sleep 20", Duration::from_millis(200))
            .await
            .expect_err("timeout");
        assert!(matches!(err, CliError::KeyringCommandTimeout(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_failing_command_reports_its_status_not_its_output() {
        let err = run("echo hush; exit 7", LONG).await.expect_err("failed");
        let CliError::KeyringCommandStatus(status) = &err else {
            panic!("{err:?}");
        };
        assert!(status.contains('7'), "{status}");
        assert!(!err.to_string().contains("hush"));
    }

    #[tokio::test]
    async fn too_much_output_and_bad_text_are_errors() {
        let err = run("head -c 65537 /dev/zero", LONG).await.expect_err("big");
        assert!(matches!(err, CliError::KeyringCommandTooLarge), "{err:?}");
        // Exactly the cap is read in full (and then fails to parse).
        let err = run("head -c 65536 /dev/zero | tr '\\0' ' '", LONG)
            .await
            .expect_err("blank");
        assert!(matches!(err, CliError::Key(_)), "{err:?}");
        let err = run("printf '\\377'", LONG).await.expect_err("bytes");
        assert!(matches!(err, CliError::KeyringCommandNotUtf8), "{err:?}");
    }

    #[tokio::test]
    async fn a_missing_command_is_a_status_error() {
        let err = run("exit 127", LONG).await.expect_err("missing");
        assert!(matches!(err, CliError::KeyringCommandStatus(_)), "{err:?}");
    }

    #[tokio::test]
    async fn dropping_the_future_kills_the_command() {
        let dir = tempdir().expect("dir");
        let pid_file = dir.path().join("pid");
        // `exec`, so that the shell's pid is the sleeper's.
        let command = format!("echo $$ > '{}'; exec sleep 20", pid_file.display());
        let pending = run(&command, LONG);
        let pid = tokio::time::timeout(Duration::from_secs(10), async {
            let mut pending = std::pin::pin!(pending);
            loop {
                tokio::select! {
                    _ = &mut pending => panic!("the command ended"),
                    () = tokio::time::sleep(Duration::from_millis(20)) => {}
                }
                if let Ok(text) = std::fs::read_to_string(&pid_file)
                    && let Ok(pid) = text.trim().parse::<u32>()
                {
                    break pid;
                }
            }
        })
        .await
        .expect("the command started");
        // The future is dropped when the block above ends.
        let stat = format!("/proc/{pid}/stat");
        let gone = || {
            std::fs::read_to_string(&stat).map_or(true, |s| {
                s.rsplit_once(')')
                    .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z'))
            })
        };
        let waited = Instant::now();
        while !gone() {
            assert!(waited.elapsed() < Duration::from_secs(10), "still running");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[test]
fn a_closed_output_pipe_exits_141_and_other_failures_1() {
    use std::io::{Error, ErrorKind};
    let broken = CliError::Output(Error::from(ErrorKind::BrokenPipe));
    assert!(broken.is_broken_pipe());
    assert_eq!(broken.exit_code(), 141);

    let other = CliError::Output(Error::from(ErrorKind::PermissionDenied));
    assert!(!other.is_broken_pipe());
    assert_eq!(other.exit_code(), 1);
    assert_eq!(
        CliError::Storage(StorageError::Generic("x".into())).exit_code(),
        1
    );
}

#[tokio::test]
async fn drain_reports_items_in_flight_when_the_listing_fails() {
    use futures::FutureExt;
    let in_flight: Vec<futures::future::BoxFuture<'static, Result<i32, StorageError>>> = vec![
        async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(1)
        }
        .boxed(),
        async { Err(StorageError::Generic("listing failed".into())) }.boxed(),
    ];
    let results = futures::stream::iter(in_flight).buffer_unordered(2);

    let mut seen = Vec::new();
    let listing = enc::drain(results, |n| {
        seen.push(n);
        std::future::ready(Ok(()))
    })
    .await
    .expect("drain");

    assert_eq!(seen, [1], "the item in flight is still reported");
    assert_eq!(listing.errors, 1);
    assert!(matches!(listing.first, Some(StorageError::Generic(_))));
}

#[tokio::test]
async fn drain_counts_every_listing_error_and_keeps_the_first() {
    let results = futures::stream::iter([
        Err(StorageError::Generic("first".into())),
        Ok(7),
        Err(StorageError::Generic("second".into())),
    ]);
    let mut seen = Vec::new();
    let listing = enc::drain(results, |n| {
        seen.push(n);
        std::future::ready(Ok(()))
    })
    .await
    .expect("drain");
    assert_eq!(seen, [7]);
    assert_eq!(listing.errors, 2);
    assert_eq!(
        listing.first.expect("first").to_string(),
        "Generic error: first"
    );
}
