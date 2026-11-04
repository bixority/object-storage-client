use super::*;
use std::time::Duration;
#[cfg(unix)]
use tempfile::tempdir;

#[cfg(unix)]
#[tokio::test]
async fn temp_file_for_an_existing_0600_file_is_never_more_permissive() {
    use std::os::unix::fs::PermissionsExt;
    let w = tempdir().expect("work");
    let tmp = w.path().join("dst.bin.osc-tmp-test");

    let (file, _guard) = create_temp_file(&tmp, Some(0o600)).await.expect("create");
    let mode = file.metadata().await.expect("meta").permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "mode right after creation");
    assert_eq!(
        std::fs::metadata(&tmp).expect("meta").len(),
        0,
        "no data yet"
    );

    // Exclusive: an existing temp path is refused.
    assert!(
        create_temp_file(&tmp, Some(0o600)).await.is_err(),
        "create_new"
    );
}

#[tokio::test]
async fn a_dropped_temp_file_creation_leaves_no_file() {
    let w = tempfile::tempdir().expect("work");
    let tmp = w.path().join("dst.bin.osc-tmp-test");
    {
        let mut create = std::pin::pin!(create_temp_file(&tmp, None));
        // Start the open, then drop the future before it is awaited.
        let _ = futures::poll!(create.as_mut());
    }
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!tmp.exists(), "a cancelled create leaves nothing behind");
}

#[test]
fn weight_counts_large_files_against_the_budget() {
    assert_eq!(weight(0, 8), 1);
    assert_eq!(weight(LARGE_FILE - 1, 8), 1);
    assert_eq!(weight(LARGE_FILE, 8), LARGE_FILE_WEIGHT);
    // Never more than the whole budget, or the job could not start.
    assert_eq!(weight(LARGE_FILE, 1), 1);
    assert_eq!(weight(LARGE_FILE, 3), 3);
    assert_eq!(weight(u64::MAX, 8), LARGE_FILE_WEIGHT);
}

#[tokio::test(start_paused = true)]
async fn large_jobs_share_the_budget() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let budget = Semaphore::new(8);
    let (running, most) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let (budget, running, most) = (&budget, &running, &most);
    let done = futures::stream::iter(0..8)
        .map(|_| async move {
            run_weighted(budget, weight(LARGE_FILE, 8), async {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(1)).await;
                running.fetch_sub(1, Ordering::SeqCst);
            })
            .await;
        })
        .buffer_unordered(8)
        .count()
        .await;
    assert_eq!(done, 8);
    assert_eq!(most.load(Ordering::SeqCst), 2);
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

/// The download temp file is private while it is written, and the finished
/// file gets the default mode, even where `/proc` cannot give the umask.
#[cfg(unix)]
#[tokio::test]
async fn download_temp_file_is_private_without_proc() {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let w = tempdir().expect("work");
    let mode_of = |p: &Path| std::fs::metadata(p).expect("meta").permissions().mode() & 0o777;

    let probe = w.path().join("independent-probe");
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o666)
        .open(&probe)
        .expect("probe");
    let want = mode_of(&probe);
    std::fs::remove_file(&probe).expect("remove");

    let src = w.path().join("src.bin");
    std::fs::write(&src, b"secret").expect("write");
    let dst = w.path().join("dst.bin");

    crate::fsmode::PROBE_ONLY
        .lock()
        .expect("lock")
        .push(dst.clone());
    // What `get` decides for a new destination, and what it creates.
    let (create, last) = download_modes(
        None,
        platform::effective_uid(),
        crate::fsmode::umask_near(&dst),
    );
    assert_eq!((create, last), (Some(0o600), Some(want)));
    let tmp = w.path().join("dst.bin.osc-tmp-test");
    let (_file, guard) = create_temp_file(&tmp, create).await.expect("create");
    assert_eq!(mode_of(&tmp), 0o600);
    drop(guard);
    assert!(!tmp.exists(), "the guard removes the temp file");

    let done = get(
        &crate::Client::<crate::Plain>::default(),
        &format!("file://{}", src.display()),
        &dst,
    )
    .await;
    crate::fsmode::PROBE_ONLY
        .lock()
        .expect("lock")
        .retain(|p| *p != dst);

    done.expect("get");
    assert_eq!(mode_of(&dst), want);
    let mut left: Vec<_> = std::fs::read_dir(w.path())
        .expect("dir")
        .map(|e| e.expect("entry").file_name())
        .collect();
    left.sort();
    assert_eq!(
        left,
        ["dst.bin", "src.bin"],
        "no temp or probe file is left"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn put_refuses_a_destination_inside_the_source() {
    let w = tempdir().expect("work");
    let src = w.path().join("d");
    std::fs::create_dir(&src).expect("dir");
    std::fs::write(src.join("a.txt"), b"data").expect("write");
    let client = crate::Client::<crate::Plain>::default();

    for dst in [
        src.display().to_string(),
        format!("{}/", src.display()),
        format!("{}/sub/", src.display()),
        format!("{}/sub/deeper", src.display()),
        // Through a link to the source.
        {
            let link = w.path().join("link");
            std::os::unix::fs::symlink(&src, &link).expect("link");
            format!("{}/sub", link.display())
        },
    ] {
        let got = put(&client, &src, &dst, true).await;
        assert!(
            matches!(got, Err(CliError::UnsafePath { .. })),
            "{dst}: {got:?}"
        );
    }
    let mut left: Vec<_> = std::fs::read_dir(&src)
        .expect("dir")
        .map(|e| e.expect("entry").file_name())
        .collect();
    left.sort();
    assert_eq!(left, ["a.txt"], "nothing was written");

    // A sibling with a similar name is outside the source.
    let out = w.path().join("d2");
    put(&client, &src, &out.display().to_string(), true)
        .await
        .expect("put");
    assert_eq!(std::fs::read(out.join("a.txt")).expect("copy"), b"data");
}

/// A file of four read chunks, for the shrink and growth tests.
#[cfg(unix)]
fn four_chunk_file(dir: &Path) -> (PathBuf, std::fs::File) {
    let path = dir.join("log");
    std::fs::write(&path, vec![7u8; 4 * FILE_BUF]).expect("write");
    let handle = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open");
    (path, handle)
}

#[cfg(unix)]
#[tokio::test]
async fn a_file_that_shrinks_during_the_upload_is_an_error_and_commits_nothing() {
    let w = tempdir().expect("work");
    let (path, handle) = four_chunk_file(w.path());
    let size = 4 * FILE_BUF as u64;

    // The stream itself: the first chunk reads, then the file is cut short.
    let file = tokio::fs::File::open(&path).await.expect("open");
    let mut chunks = file_chunks(file, Some(size), "log".into()).await;
    assert!(chunks.next().await.expect("first").is_ok());
    handle.set_len(FILE_BUF as u64).expect("truncate");
    let mut failure = None;
    while let Some(chunk) = chunks.next().await {
        if let Err(e) = chunk {
            failure = Some(e.to_string());
        }
    }
    let failure = failure.expect("a shrunk file ends in an error");
    assert!(
        failure.contains("log shrank during the upload"),
        "{failure}"
    );

    // The upload: nothing is committed.
    let (path, handle) = {
        std::fs::remove_file(&path).expect("remove");
        four_chunk_file(w.path())
    };
    let dst = w.path().join("out");
    let file = tokio::fs::File::open(&path).await.expect("open");
    // The size is known from before the cut, as in `put_file`.
    let chunks = file_chunks(file, Some(size), "log".into()).await;
    handle.set_len(0).expect("truncate");
    let client = crate::Client::<crate::Plain>::default();
    let url = format!("file://{}", dst.display());
    let got = client.put_local_file(&url, chunks, None, Some(size)).await;
    assert!(got.is_err(), "{got:?}");
    assert!(!dst.exists(), "a short object must not be committed");
}

#[cfg(unix)]
#[tokio::test]
async fn a_file_that_grows_after_open_uploads_in_full() {
    use std::io::Write;
    let w = tempdir().expect("work");
    let (path, _handle) = four_chunk_file(w.path());
    let file = tokio::fs::File::open(&path).await.expect("open");
    let mut appender = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append");
    appender.write_all(&[9u8; 1000]).expect("append");

    let dst = w.path().join("out");
    let client = crate::Client::<crate::Plain>::default();
    // The size was taken before the append in a real run; here it is taken
    // after, so the stream is also checked directly with the smaller size.
    put_file(&client, file, &format!("file://{}", dst.display()), &path)
        .await
        .expect("put");
    assert_eq!(
        std::fs::metadata(&dst).expect("meta").len(),
        4 * FILE_BUF as u64 + 1000
    );

    let file = tokio::fs::File::open(&path).await.expect("open");
    let chunks = file_chunks(file, Some(4 * FILE_BUF as u64 - 1), "log".into()).await;
    let total: usize = chunks
        .map(|c| c.expect("chunk").len())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum();
    assert_eq!(total, 4 * FILE_BUF + 1000);
}
