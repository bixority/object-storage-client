//! The worker over a real runtime: file:// folders, and a local TCP listener
//! that accepts and never answers, for work that must stay in flight.

use super::*;
use crate::ObjectStorageClient;
use crate::backend::ObjectState;
use crate::gui::connection::{Connection, Provider, Source};
use crate::gui::model::{Batch, Item, OnExisting, OpKind, Plan};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(5);

fn local() -> Arc<Connection> {
    Arc::new(Connection::local())
}

/// A worker whose clients all come from `ObjectStorageClient::new`, and whose
/// discovery sees an empty home directory.
fn start() -> (WorkerHandle, Arc<AtomicUsize>) {
    start_with(Box::new(Connection::client))
}

/// A host with only `HOME` set, so discovery never reads the real environment.
fn empty_host(home: &std::path::Path) -> Host {
    Host::from_vars(HashMap::from([(
        "HOME".to_owned(),
        home.display().to_string(),
    )]))
}

fn start_with(factory: Factory<crate::codec::Plain>) -> (WorkerHandle, Arc<AtomicUsize>) {
    // The directory is gone at once; discovery finds nothing in it.
    let host = empty_host(tempfile::tempdir().expect("tempdir").path());
    let repaints = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&repaints);
    let repaint: Repaint = Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    let handle = spawn(&Handle::current(), factory, host, repaint, false);
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
            conn: local(),
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

fn s3_conn(id: &str) -> Arc<Connection> {
    Arc::new(Connection {
        id: id.to_owned(),
        label: id.to_owned(),
        provider: Provider::S3,
        source: Source::Env,
        login: None,
    })
}

async fn list_names(handle: &mut WorkerHandle, conn: &Arc<Connection>, seq: u64) -> Vec<String> {
    let command = Command::List {
        side: Side::Left,
        seq,
        conn: Arc::clone(conn),
        url: "s3://b/".to_owned(),
        token: None,
    };
    send(handle, command).await;
    let Event::Page { result, .. } = next(handle).await else {
        panic!("expected a page");
    };
    let page = result.expect("listed");
    page.entries.iter().map(|e| e.name().to_owned()).collect()
}

/// A client whose `s3://b/` holds only the object `name`.
async fn client_with(name: &str) -> ObjectStorageClient {
    let client = ObjectStorageClient::new();
    client.preload_store("s3://b/", Arc::new(object_store::memory::InMemory::new()));
    client
        .put(&format!("s3://b/{name}"), b"x".to_vec())
        .await
        .expect("put");
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn each_connection_lists_through_its_own_client_built_once() {
    let clients = HashMap::from([
        ("one".to_owned(), client_with("one.txt").await),
        ("two".to_owned(), client_with("two.txt").await),
    ]);
    let builds = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&builds);
    let factory: Factory<crate::codec::Plain> = Box::new(move |conn| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(clients[&conn.id].clone())
    });
    let (mut handle, _) = start_with(factory);
    let (one, two) = (s3_conn("one"), s3_conn("two"));
    assert_eq!(list_names(&mut handle, &one, 1).await, ["one.txt"]);
    assert_eq!(list_names(&mut handle, &two, 2).await, ["two.txt"]);
    assert_eq!(list_names(&mut handle, &one, 3).await, ["one.txt"]);
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    // A scan forgets the clients: they are built again from the new definitions.
    send(&handle, Command::Discover).await;
    while !matches!(
        next(&mut handle).await,
        Event::Connections { done: true, .. }
    ) {}
    assert_eq!(list_names(&mut handle, &two, 4).await, ["two.txt"]);
    assert_eq!(builds.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_definition_builds_a_new_client() {
    let builds = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&builds);
    let client = client_with("x").await;
    let factory: Factory<crate::codec::Plain> = Box::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(client.clone())
    });
    let (mut handle, _) = start_with(factory);
    let one = s3_conn("one");
    list_names(&mut handle, &one, 1).await;
    list_names(&mut handle, &one, 2).await;
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    let renamed = Arc::new(Connection {
        label: "renamed".to_owned(),
        ..(*one).clone()
    });
    list_names(&mut handle, &renamed, 3).await;
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_reads_through_the_source_and_writes_through_the_destination() {
    let (one, two) = (client_with("x").await, ObjectStorageClient::new());
    two.preload_store("s3://b/", Arc::new(object_store::memory::InMemory::new()));
    let clients = HashMap::from([
        ("one".to_owned(), one.clone()),
        ("two".to_owned(), two.clone()),
    ]);
    let factory: Factory<crate::codec::Plain> = Box::new(move |conn| Ok(clients[&conn.id].clone()));
    let (mut handle, _) = start_with(factory);
    let (src_conn, dst_conn) = (s3_conn("one"), s3_conn("two"));
    let batch = Batch {
        kind: OpKind::Copy,
        src_dir: Location::parse("s3://b/").expect("parses"),
        entries: vec![crate::gui::model::Entry::Object {
            name: "x".to_owned(),
            size: 1,
            modified: chrono::Utc::now(),
            state: ObjectState::Online,
            class: None,
        }],
        dst_dir: Location::parse("s3://b/d/").expect("parses"),
        rename_to: None,
    };
    send(
        &handle,
        Command::Plan {
            job: JobId(1),
            batch,
            src_conn: Arc::clone(&src_conn),
            dst_conn: Arc::clone(&dst_conn),
        },
    )
    .await;
    let Event::Planned { plan, .. } = next(&mut handle).await else {
        panic!("expected a plan");
    };
    assert!(matches!(
        plan.items.as_slice(),
        [Item::Transfer {
            exists: Some(false),
            ..
        }]
    ));
    let execute = Command::Execute {
        job: JobId(2),
        src_conn,
        dst_conn,
        plan,
        on_existing: OnExisting::Overwrite,
    };
    send(&handle, execute).await;
    let Event::Finished { outcome, .. } = next_settled(&mut handle).await else {
        panic!("expected the job to finish");
    };
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(two.get("s3://b/d/x").await.expect("copied").as_ref(), b"x");
    assert!(one.get("s3://b/d/x").await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_login_without_a_device_login_fails() {
    let (mut handle, _) = start();
    send(
        &handle,
        Command::Login {
            conn: s3_conn("one"),
        },
    )
    .await;
    let Event::LoginDone { conn_id, result } = next(&mut handle).await else {
        panic!("expected the login to end");
    };
    assert_eq!(conn_id, "one");
    assert!(result.expect_err("fails").contains("no device login"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_build_error_is_the_error_of_the_command() {
    let factory: Factory<crate::codec::Plain> =
        Box::new(|_| Err(crate::Error::InvalidInput("no such profile".to_owned())));
    let (mut handle, _) = start_with(factory);
    let conn = s3_conn("one");
    let list = Command::List {
        side: Side::Right,
        seq: 5,
        conn: Arc::clone(&conn),
        url: "s3://b/".to_owned(),
        token: None,
    };
    send(&handle, list).await;
    let Event::Page {
        side, seq, result, ..
    } = next(&mut handle).await
    else {
        panic!("expected a page");
    };
    assert_eq!((side, seq), (Side::Right, 5));
    assert!(result.expect_err("fails").contains("no such profile"));

    let batch = Batch {
        kind: OpKind::Copy,
        src_dir: Location::parse("s3://b/").expect("parses"),
        entries: Vec::new(),
        dst_dir: Location::parse("file:///tmp/").expect("parses"),
        rename_to: None,
    };
    send(
        &handle,
        Command::Plan {
            job: JobId(1),
            batch,
            src_conn: Arc::clone(&conn),
            dst_conn: Arc::new(Connection::local()),
        },
    )
    .await;
    let Event::Planned { plan, .. } = next(&mut handle).await else {
        panic!("expected a plan");
    };
    assert!(plan.items.is_empty() && plan.failures[0].error.contains("no such profile"));

    let run = Plan {
        kind: OpKind::Copy,
        items: Vec::new(),
        checked: true,
        failures: Vec::new(),
    };
    let execute = Command::Execute {
        job: JobId(2),
        src_conn: conn,
        dst_conn: Arc::new(Connection::local()),
        plan: run,
        on_existing: OnExisting::Overwrite,
    };
    send(&handle, execute).await;
    let Event::Finished { job, outcome } = next(&mut handle).await else {
        panic!("expected the job to finish");
    };
    assert_eq!(job, JobId(2));
    assert!(outcome.failures[0].error.contains("no such profile"));
}

#[tokio::test(flavor = "multi_thread")]
async fn discovery_reports_the_local_result_then_the_final_one() {
    let home = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(home.path().join(".aws")).expect("mkdir");
    std::fs::write(
        home.path().join(".aws/config"),
        "[profile dev]\nrole_arn = r\n",
    )
    .expect("write");
    let host = empty_host(home.path());
    let factory: Factory<crate::codec::Plain> = Box::new(Connection::client);
    let mut handle = spawn(&Handle::current(), factory, host, Arc::new(|| {}), false);
    send(&handle, Command::Discover).await;
    for done in [false, true] {
        let Event::Connections {
            discovery,
            done: got,
        } = next(&mut handle).await
        else {
            panic!("expected connections");
        };
        assert_eq!(got, done);
        let ids: Vec<_> = discovery
            .connections
            .iter()
            .map(|c| c.id.as_str())
            .collect();
        assert_eq!(ids, ["local", "aws-profile:dev"]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn new_listing_aborts_previous() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut handle, _) = start();
    send(
        &handle,
        Command::List {
            conn: local(),
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
            conn: local(),
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
            src_conn: local(),
            dst_conn: local(),
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
            src_conn: local(),
            dst_conn: local(),
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
            src_conn: local(),
            dst_conn: local(),
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
