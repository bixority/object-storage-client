//! The worker over a real runtime: file:// folders, and a local TCP listener
//! that accepts and never answers, for work that must stay in flight.

use super::*;
use crate::ObjectStorageClient;
use crate::gui::model::{Item, OnExisting, OpKind, Plan};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(5);

fn start() -> (WorkerHandle, Arc<AtomicUsize>) {
    let repaints = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&repaints);
    let repaint: Repaint = Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    let handle = spawn(
        &Handle::current(),
        ObjectStorageClient::new(),
        repaint,
        false,
    );
    (handle, repaints)
}

fn dir_url(dir: &std::path::Path) -> String {
    let url = url::Url::from_directory_path(dir).expect("directory URL");
    url.to_string()
}

/// An `http://` base whose server accepts connections and never replies.
async fn stalled() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            // Held open and never answered.
            tokio::spawn(async move {
                let _socket = socket;
                std::future::pending::<()>().await;
            });
        }
    });
    format!("http://{addr}/")
}

async fn next(handle: &mut WorkerHandle) -> Event {
    tokio::time::timeout(WAIT, handle.events.recv())
        .await
        .expect("an event in time")
        .expect("the worker is running")
}

/// The next event that is not a progress report.
async fn next_settled(handle: &mut WorkerHandle) -> Event {
    loop {
        match next(handle).await {
            Event::Progress { .. } => {}
            event => return event,
        }
    }
}

async fn send(handle: &WorkerHandle, command: Command) {
    handle.commands.send(command).await.expect("worker accepts");
}

fn transfer(src: String, dst: String) -> Item {
    Item::Transfer {
        src,
        dst,
        rel: "x".to_owned(),
        size: 1,
        exists: Some(false),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_sends_page_and_repaints() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.txt"), b"a").expect("write");
    let (mut handle, repaints) = start();
    let url = dir_url(dir.path());
    send(
        &handle,
        Command::List {
            side: Side::Left,
            seq: 1,
            url,
            token: None,
        },
    )
    .await;
    let Event::Page {
        side,
        seq,
        first,
        result,
    } = next(&mut handle).await
    else {
        panic!("expected a page");
    };
    assert_eq!((side, seq, first), (Side::Left, 1, true));
    let page = result.expect("listed");
    assert_eq!(page.entries.len(), 1);
    // The repaint is requested right after the event is sent, on the
    // worker's thread: wait for it rather than race it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while repaints.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(repaints.load(Ordering::SeqCst) >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn new_listing_aborts_previous() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut handle, _) = start();
    send(
        &handle,
        Command::List {
            side: Side::Left,
            seq: 1,
            url: stalled().await,
            token: None,
        },
    )
    .await;
    send(
        &handle,
        Command::List {
            side: Side::Left,
            seq: 2,
            url: dir_url(dir.path()),
            token: None,
        },
    )
    .await;
    let Event::Page { seq, .. } = next(&mut handle).await else {
        panic!("expected a page");
    };
    assert_eq!(seq, 2);
    // The stalled listing was aborted: it never reports.
    let late = tokio::time::timeout(Duration::from_millis(500), handle.events.recv()).await;
    assert!(late.is_err(), "unexpected event: {late:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn execute_reports_progress_and_finishes() {
    let src = tempfile::tempdir().expect("tempdir");
    let dst = tempfile::tempdir().expect("tempdir");
    std::fs::write(src.path().join("a"), b"a").expect("write");
    std::fs::write(src.path().join("b"), b"b").expect("write");
    let (s, d) = (dir_url(src.path()), dir_url(dst.path()));
    let plan = Plan {
        kind: OpKind::Copy,
        items: vec![
            transfer(format!("{s}a"), format!("{d}a")),
            transfer(format!("{s}b"), format!("{d}b")),
        ],
        checked: true,
        failures: Vec::new(),
    };
    let (mut handle, _) = start();
    send(
        &handle,
        Command::Execute {
            job: JobId(3),
            plan,
            on_existing: OnExisting::Overwrite,
        },
    )
    .await;
    let mut progress = Vec::new();
    let outcome = loop {
        match next(&mut handle).await {
            Event::Progress {
                job, done, total, ..
            } => {
                assert_eq!(job, JobId(3));
                progress.push((done, total));
            }
            Event::Finished { job, outcome } => {
                assert_eq!(job, JobId(3));
                break outcome;
            }
            other => panic!("unexpected {other:?}"),
        }
    };
    progress.sort_unstable();
    assert_eq!(progress, [(1, 2), (2, 2)]);
    assert_eq!(outcome.done, 2);
    assert!(!outcome.cancelled);
    assert!(dst.path().join("a").exists() && dst.path().join("b").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_reports_cancelled() {
    let dst = tempfile::tempdir().expect("tempdir");
    let plan = Plan {
        kind: OpKind::Copy,
        items: vec![transfer(
            format!("{}x", stalled().await),
            format!("{}x", dir_url(dst.path())),
        )],
        checked: true,
        failures: vec![Failure {
            name: "planned".to_owned(),
            error: "listing failed".to_owned(),
        }],
    };
    let (mut handle, _) = start();
    send(
        &handle,
        Command::Execute {
            job: JobId(7),
            plan,
            on_existing: OnExisting::Overwrite,
        },
    )
    .await;
    send(&handle, Command::Abort(JobId(7))).await;
    let Event::Finished { job, outcome } = next_settled(&mut handle).await else {
        panic!("expected the job to finish");
    };
    assert_eq!(job, JobId(7));
    assert!(outcome.cancelled);
    assert_eq!(outcome.done, 0);
    // The plan's failures are reported with a cancelled job too.
    assert_eq!(outcome.failures.len(), 1);
    assert_eq!(outcome.failures[0].name, "planned");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_aborts_jobs_then_reports_done() {
    let dst = tempfile::tempdir().expect("tempdir");
    let plan = Plan {
        kind: OpKind::Copy,
        items: vec![transfer(
            format!("{}x", stalled().await),
            format!("{}x", dir_url(dst.path())),
        )],
        checked: true,
        failures: Vec::new(),
    };
    let (mut handle, _) = start();
    send(
        &handle,
        Command::Execute {
            job: JobId(1),
            plan,
            on_existing: OnExisting::Overwrite,
        },
    )
    .await;
    send(&handle, Command::Shutdown).await;
    // The aborted job reports as cancelled, then the drain ends.
    let mut seen_done = false;
    while let Ok(Some(event)) = tokio::time::timeout(WAIT, handle.events.recv()).await {
        match event {
            Event::Finished { outcome, .. } => {
                assert!(outcome.cancelled);
                assert!(!seen_done, "a job reported after ShutdownDone");
            }
            Event::ShutdownDone => seen_done = true,
            Event::Progress { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(seen_done);
    // The worker has stopped: the channel is closed.
    assert!(handle.commands.send(Command::Shutdown).await.is_err());
}
