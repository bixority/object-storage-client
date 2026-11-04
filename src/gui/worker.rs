//! The background worker: runs the reducer's commands on the tokio runtime
//! and reports back as events.
//!
//! Every listing and job is a task with an abort handle. Futures are only
//! ever cancelled inside the runtime (never dropped on the UI thread), so the
//! client's abort-on-drop clean-up of interrupted uploads keeps working, and
//! `Shutdown` waits for that clean-up before it reports `ShutdownDone`.

use super::action::{Command, Event};
use super::location::Location;
use super::model::{DRAIN_TIMEOUT, Failure, JobId, Outcome, Side};
use super::ops::{self, Progress};
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

/// Starts the worker on `rt`. With `signals`, SIGINT/SIGTERM (Ctrl-C) are
/// reported as [`Event::Interrupted`]; tests leave them to the test runner.
pub(super) fn spawn<C: Codec>(
    rt: &Handle,
    client: Client<C>,
    repaint: Repaint,
    signals: bool,
) -> WorkerHandle {
    let (commands, inbox) = mpsc::channel(COMMAND_CAPACITY);
    let (outbox, events) = mpsc::unbounded_channel();
    let worker = Worker {
        client: Arc::new(client),
        sink: Sink {
            events: outbox,
            repaint,
        },
        listings: [None, None],
        jobs: HashMap::new(),
    };
    rt.spawn(worker.run(inbox, signals));
    WorkerHandle { commands, events }
}

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
    client: Arc<Client<C>>,
    sink: Sink,
    /// The listing in flight per panel; a new one replaces it.
    listings: [Option<AbortHandle>; 2],
    jobs: HashMap<JobId, Running>,
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
                url,
                token,
            } => self.list(side, seq, url, token),
            Command::Plan { job, batch } => {
                let client = Arc::clone(&self.client);
                self.start(job, None, async move {
                    let plan = ops::plan(&client, batch).await;
                    Event::Planned { job, plan }
                });
            }
            Command::Execute {
                job,
                mut plan,
                on_existing,
            } => {
                let client = Arc::clone(&self.client);
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
                        ops::execute(&client, plan, on_existing, &counters, &notify).await;
                    Event::Finished { job, outcome }
                });
            }
            Command::Abort(job) => {
                if let Some(running) = self.jobs.get(&job) {
                    running.task.abort();
                }
            }
            Command::Shutdown => unreachable!("handled by the run loop"),
        }
    }

    /// Lists one page for `side`, replacing that panel's listing in flight.
    fn list(&mut self, side: Side, seq: u64, url: String, token: Option<String>) {
        if let Some(previous) = self.listings[side.index()].take() {
            previous.abort();
        }
        let client = Arc::clone(&self.client);
        let sink = self.sink.clone();
        let first = token.is_none();
        let task = tokio::spawn(async move {
            let result = match Location::parse(&url) {
                Ok(dir) => ops::list_page(&client, &dir, token).await,
                Err(err) => Err(err),
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
        for listing in self.listings.iter_mut().filter_map(Option::take) {
            listing.abort();
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
