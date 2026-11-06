//! The background worker: runs the reducer's commands on the tokio runtime
//! and reports back as events.
//!
//! Every listing and job is a task with an abort handle. Futures are only
//! ever cancelled inside the runtime (never dropped on the UI thread), so the
//! client's abort-on-drop clean-up of interrupted uploads keeps working, and
//! `Shutdown` waits for that clean-up before it reports `ShutdownDone`.

use super::action::{Command, Event};
use super::display;
use super::location::Location;
use super::model::{Batch, DRAIN_TIMEOUT, Failure, JobId, Outcome, Plan, Side};
use super::ops::{self, Progress};
use crate::gui::connection::{self, Connection, Discovery, Host, Login, azure, sso};
use crate::upload::drain_pending_aborts;
use crate::{Client, Codec};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};

#[cfg(test)]
mod tests;

/// Commands queued for the worker before the UI must hold them back.
const COMMAND_CAPACITY: usize = 64;

/// The UI's ends of the worker's channels.
pub(super) struct WorkerHandle {
    pub(super) commands: mpsc::Sender<Command>,
    pub(super) events: mpsc::UnboundedReceiver<Event>,
}

/// Wakes the UI after each event.
pub(super) type Repaint = Arc<dyn Fn() + Send + Sync>;

/// Builds the client of a connection, with encryption when it is on.
pub(super) type Factory<C> = Box<dyn Fn(&Connection) -> crate::Result<Client<C>> + Send>;

/// Starts the worker on `rt`. Clients are built by `factory` on first use of
/// a connection; `host` is where discovery looks for connections. With
/// `signals`, SIGINT/SIGTERM (Ctrl-C) are reported as [`Event::Interrupted`];
/// tests leave them to the test runner.
pub(super) fn spawn<C: Codec>(
    rt: &Handle,
    factory: Factory<C>,
    host: Host,
    repaint: Repaint,
    signals: bool,
) -> WorkerHandle {
    let (commands, inbox) = mpsc::channel(COMMAND_CAPACITY);
    let (outbox, events) = mpsc::unbounded_channel();
    let worker = Worker {
        factory,
        host: Arc::new(host),
        clients: HashMap::new(),
        sink: Sink {
            events: outbox,
            repaint,
        },
        listings: [None, None],
        jobs: HashMap::new(),
        discovery: None,
        login: None,
    };
    rt.spawn(worker.run(inbox, signals));
    WorkerHandle { commands, events }
}

/// The source and destination clients of a job.
type Clients<C> = (Arc<Client<C>>, Arc<Client<C>>);

/// Sends events to the UI and wakes it.
#[derive(Clone)]
struct Sink {
    events: mpsc::UnboundedSender<Event>,
    repaint: Repaint,
}

impl Sink {
    fn send(&self, event: Event) {
        if self.events.send(event).is_ok() {
            (self.repaint)();
        }
    }
}

/// A running job: its task, and the supervisor that reports its end.
struct Running {
    task: AbortHandle,
    supervisor: JoinHandle<()>,
}

struct Worker<C: Codec> {
    factory: Factory<C>,
    host: Arc<Host>,
    /// The client of each connection used so far, by connection id, with the
    /// definition it was built from: a changed definition builds a new one.
    clients: HashMap<String, (Connection, Arc<Client<C>>)>,
    sink: Sink,
    /// The listing in flight per panel; a new one replaces it.
    listings: [Option<AbortHandle>; 2],
    jobs: HashMap<JobId, Running>,
    /// The scan in flight; a new one replaces it.
    discovery: Option<AbortHandle>,
    /// The login in flight; only one runs at a time.
    login: Option<AbortHandle>,
}

impl<C: Codec> Worker<C> {
    async fn run(mut self, mut inbox: mpsc::Receiver<Command>, signals: bool) {
        let interrupted = interrupted(signals);
        tokio::pin!(interrupted);
        let mut signalled = false;
        loop {
            tokio::select! {
                command = inbox.recv() => match command {
                    Some(Command::Shutdown) => {
                        self.shutdown().await;
                        self.sink.send(Event::ShutdownDone);
                        return;
                    }
                    Some(command) => self.handle(command),
                    // The UI is gone without asking: still clean up.
                    None => {
                        self.shutdown().await;
                        return;
                    }
                },
                () = &mut interrupted, if !signalled => {
                    signalled = true;
                    self.sink.send(Event::Interrupted);
                }
            }
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::List {
                side,
                seq,
                conn,
                url,
                token,
            } => self.list(side, seq, &conn, url, token),
            Command::Plan {
                job,
                batch,
                src_conn,
                dst_conn,
            } => match self.pair(&src_conn, &dst_conn) {
                Ok((src, dst)) => self.start(job, None, async move {
                    let plan = ops::plan(&src, &dst, batch).await;
                    Event::Planned { job, plan }
                }),
                Err(error) => {
                    let plan = failed_plan(&batch, error);
                    self.sink.send(Event::Planned { job, plan });
                }
            },
            Command::Execute {
                job,
                src_conn,
                dst_conn,
                mut plan,
                on_existing,
            } => {
                let (src, dst) = match self.pair(&src_conn, &dst_conn) {
                    Ok(pair) => pair,
                    Err(error) => {
                        let outcome = Outcome {
                            failures: vec![Failure {
                                name: "(job)".to_owned(),
                                error,
                            }],
                            ..Outcome::default()
                        };
                        self.sink.send(Event::Finished { job, outcome });
                        return;
                    }
                };
                let progress = Arc::new(Progress::new(std::mem::take(&mut plan.failures)));
                let counters = Arc::clone(&progress);
                let sink = self.sink.clone();
                self.start(job, Some(progress), async move {
                    let notify = move |done: usize, total: usize, current: &str| {
                        sink.send(Event::Progress {
                            job,
                            done,
                            total,
                            current: current.to_owned(),
                        });
                    };
                    let outcome =
                        ops::execute(&src, &dst, plan, on_existing, &counters, &notify).await;
                    Event::Finished { job, outcome }
                });
            }
            Command::Abort(job) => {
                if let Some(running) = self.jobs.get(&job) {
                    running.task.abort();
                }
            }
            Command::Discover => self.discover(),
            Command::Login { conn } => self.log_in(&conn),
            Command::CancelLogin => {
                if let Some(login) = self.login.take() {
                    login.abort();
                }
            }
            Command::Shutdown => unreachable!("handled by the run loop"),
        }
    }

    /// The client of `conn`, built on first use.
    fn client(&mut self, conn: &Connection) -> Result<Arc<Client<C>>, String> {
        if let Some((built, client)) = self.clients.get(&conn.id)
            && built == conn
        {
            return Ok(Arc::clone(client));
        }
        let client = Arc::new((self.factory)(conn).map_err(|err| display::text(&err.to_string()))?);
        self.clients
            .insert(conn.id.clone(), (conn.clone(), Arc::clone(&client)));
        Ok(client)
    }

    /// The source and destination clients; one client twice for one connection.
    fn pair(&mut self, src: &Connection, dst: &Connection) -> Result<Clients<C>, String> {
        Ok((self.client(src)?, self.client(dst)?))
    }

    /// Lists one page for `side`, replacing that panel's listing in flight.
    fn list(
        &mut self,
        side: Side,
        seq: u64,
        conn: &Connection,
        url: String,
        token: Option<String>,
    ) {
        if let Some(previous) = self.listings[side.index()].take() {
            previous.abort();
        }
        let first = token.is_none();
        let client = self.client(conn);
        let sink = self.sink.clone();
        let task = tokio::spawn(async move {
            let result = match (client, Location::parse(&url)) {
                (Ok(client), Ok(dir)) => ops::list_page(&client, &dir, token).await,
                (Err(err), _) | (_, Err(err)) => Err(err),
            };
            sink.send(Event::Page {
                side,
                seq,
                first,
                result,
            });
        });
        self.listings[side.index()] = Some(task.abort_handle());
    }

    /// Scans for connections, replacing a scan in flight and forgetting the
    /// clients built from the old definitions. Reports what needs no network
    /// first, then adds the Azure accounts.
    fn discover(&mut self) {
        if let Some(previous) = self.discovery.take() {
            previous.abort();
        }
        self.clients.clear();
        let host = Arc::clone(&self.host);
        let sink = self.sink.clone();
        let task = tokio::spawn(async move {
            let scan = tokio::task::spawn_blocking(move || connection::discover(&host)).await;
            let Ok((mut discovery, subscriptions)) = scan else {
                let discovery = Discovery {
                    connections: vec![Connection::local()],
                    notes: vec!["Searching for connections failed".to_owned()],
                };
                sink.send(Event::Connections {
                    discovery,
                    done: true,
                });
                return;
            };
            sink.send(Event::Connections {
                discovery: discovery.clone(),
                done: false,
            });
            let azure = azure::discover_azure(&subscriptions).await;
            discovery.connections.extend(azure.connections);
            discovery.notes.extend(azure.notes);
            sink.send(Event::Connections {
                discovery,
                done: true,
            });
        });
        self.discovery = Some(task.abort_handle());
    }

    /// Runs the device login of `conn`: reports the code to approve, then the
    /// end. The token lands in the cache `aws_profile.rs` re-reads on its next
    /// call, so no client is dropped.
    fn log_in(&mut self, conn: &Connection) {
        let conn_id = conn.id.clone();
        let sink = self.sink.clone();
        let host = Arc::clone(&self.host);
        let busy = self
            .login
            .as_ref()
            .is_some_and(|login| !login.is_finished());
        let Some(Login::AwsSso(login)) = conn.login.clone().filter(|_| !busy) else {
            let error = if busy {
                "another login is in progress"
            } else {
                "this connection has no device login"
            };
            sink.send(Event::LoginDone {
                conn_id,
                result: Err(error.to_owned()),
            });
            return;
        };
        let task = tokio::spawn(async move {
            let result = async {
                let pending = sso::start(&login).await?;
                sink.send(Event::LoginCode {
                    conn_id: conn_id.clone(),
                    user_code: pending.user_code.clone(),
                    url: pending.url.clone(),
                });
                let dir = sso::default_cache_dir(&host).ok_or_else(|| {
                    crate::Error::InvalidInput("the home directory is unknown".to_owned())
                })?;
                sso::finish(pending, &dir).await
            }
            .await
            .map_err(|err| display::text(&err.to_string()));
            sink.send(Event::LoginDone { conn_id, result });
        });
        self.login = Some(task.abort_handle());
    }

    /// Runs `work` as job `id`. A supervisor reports its event, or a
    /// `Finished` built from `progress` when it was aborted or panicked.
    fn start<F>(&mut self, id: JobId, progress: Option<Arc<Progress>>, work: F)
    where
        F: Future<Output = Event> + Send + 'static,
    {
        self.jobs
            .retain(|_, running| !running.supervisor.is_finished());
        let task = tokio::spawn(work);
        let abort = task.abort_handle();
        let sink = self.sink.clone();
        let supervisor = tokio::spawn(async move {
            let event = match task.await {
                Ok(event) => event,
                Err(err) => {
                    let mut outcome =
                        progress.map_or_else(Outcome::default, |progress| progress.outcome());
                    if err.is_cancelled() {
                        outcome.cancelled = true;
                    } else {
                        outcome.failures.push(Failure {
                            name: "(job)".to_owned(),
                            error: "internal error".to_owned(),
                        });
                    }
                    Event::Finished { job: id, outcome }
                }
            };
            sink.send(event);
        });
        self.jobs.insert(
            id,
            Running {
                task: abort,
                supervisor,
            },
        );
    }

    /// Aborts everything, waits until the aborted jobs are gone, then for the
    /// clean-up of interrupted uploads; bounded by [`DRAIN_TIMEOUT`].
    async fn shutdown(&mut self) {
        for task in self
            .listings
            .iter_mut()
            .chain([&mut self.discovery, &mut self.login])
            .filter_map(Option::take)
        {
            task.abort();
        }
        let jobs: Vec<Running> = self.jobs.drain().map(|(_, running)| running).collect();
        for running in &jobs {
            running.task.abort();
        }
        let drained = async {
            // A supervisor ends once its job's future has been dropped, which
            // is when an interrupted upload registers its clean-up.
            for running in jobs {
                let _ = running.supervisor.await;
            }
            drain_pending_aborts(DRAIN_TIMEOUT).await;
        };
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, drained).await;
    }
}

/// The plan of a batch whose clients could not be built.
fn failed_plan(batch: &Batch, error: String) -> Plan {
    Plan {
        kind: batch.kind,
        items: Vec::new(),
        checked: true,
        failures: vec![Failure {
            name: batch.src_dir.display(),
            error,
        }],
    }
}

/// Resolves on SIGINT or SIGTERM (Ctrl-C elsewhere); never without `signals`
/// or when the handlers cannot be installed.
async fn interrupted(signals: bool) {
    if signals {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            if let (Ok(mut int), Ok(mut term)) = (
                signal(SignalKind::interrupt()),
                signal(SignalKind::terminate()),
            ) {
                tokio::select! {
                    _ = int.recv() => {}
                    _ = term.recv() => {}
                }
                return;
            }
        }
        #[cfg(not(unix))]
        if tokio::signal::ctrl_c().await.is_ok() {
            return;
        }
    }
    std::future::pending::<()>().await;
}
