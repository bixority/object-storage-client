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
            .await
            .expect("permits");
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
    create_temp_file(&tmp, create).await.expect("create");
    assert_eq!(mode_of(&tmp), 0o600);
    std::fs::remove_file(&tmp).expect("remove");

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
