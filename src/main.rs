use clap::Parser;
use object_storage_client::cli::{Cli, drain_pending_aborts, sanitize};
use std::env;
use std::error::Error;
use std::iter;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

/// Writes a line to standard error, ignoring a failure: a status or error line
/// must never abort the process (`eprintln!` panics on a closed pipe).
macro_rules! errln {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

/// Resolves with the conventional exit code (128 + signal number) and a name
/// when the process is asked to stop.
///
/// Without a handler the default action kills the process at once and no drop
/// guard runs, which would leave partial downloads behind. SIGKILL cannot be
/// caught, so a `kill -9` can still leave a `.osc-tmp-*` file behind.
#[cfg(unix)]
async fn shutdown_signal() -> (u8, &'static str) {
    use tokio::signal::unix::{SignalKind, signal};

    let (Ok(mut term), Ok(mut hup)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::hangup()),
    ) else {
        // Could not register: still honour Ctrl-C.
        let _ = tokio::signal::ctrl_c().await;
        return (130, "SIGINT");
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => (130, "SIGINT"),
        _ = term.recv() => (143, "SIGTERM"),
        _ = hup.recv() => (129, "SIGHUP"),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> (u8, &'static str) {
    let _ = tokio::signal::ctrl_c().await;
    (130, "SIGINT")
}

/// How long to wait, after a signal or an error, for cancelled uploads to clean up.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait, after a signal, for blocking threads (a file open stuck
/// on a FIFO, a write to a full pipe) before the process exits regardless.
const BLOCKING_SHUTDOWN: Duration = Duration::from_millis(200);

/// Keeps key material out of core dumps and away from same-user debuggers.
///
/// A release build aborts on panic and the abort signal dumps core by
/// default, which would write every loaded key and credential to disk. Both
/// calls are best effort: a limit that cannot be set is not worth refusing to
/// run for. The process keeps access to its own `/proc/self`, and children
/// (the keyring command) inherit the zero core limit.
#[cfg(unix)]
fn harden_process() {
    use rustix::process::{Resource, Rlimit, setrlimit};

    #[cfg(any(target_os = "linux", target_os = "android"))]
    let _ = rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable);
    let _ = setrlimit(
        Resource::Core,
        Rlimit {
            current: Some(0),
            maximum: Some(0),
        },
    );
}

fn main() -> ExitCode {
    #[cfg(unix)]
    harden_process();
    // Built by hand so that after a signal the runtime can be shut down
    // without waiting for blocking threads, which cannot be cancelled.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // One worker per core is more than a transfer can use on a big machine and
    // costs start-up time; an explicit TOKIO_WORKER_THREADS still wins.
    if env::var_os("TOKIO_WORKER_THREADS").is_none() {
        builder.worker_threads(
            thread::available_parallelism()
                .map_or(1, usize::from)
                .min(4),
        );
    }
    let runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) => {
            errln!("Error: failed to start the async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(run());
    // Normal exits wait for blocking work as before (the default drop);
    // after a signal it may never finish.
    match code {
        Exit::Done(code) => {
            drop(runtime);
            code
        }
        Exit::Interrupted(code) => {
            runtime.shutdown_timeout(BLOCKING_SHUTDOWN);
            code
        }
    }
}

enum Exit {
    Done(ExitCode),
    Interrupted(ExitCode),
}

async fn run() -> Exit {
    // Whichever branch loses is dropped before `select!` returns, so the
    // drop guards of a cancelled command (temp files, in-flight uploads) run
    // before the process exits.
    let outcome = tokio::select! {
        result = Cli::parse()
            .with_keyring_command(env::var_os("OSC_KEYRING_COMMAND"))
            .run() => Ok(result),
        signal = shutdown_signal() => Err(signal),
    };
    let result = match outcome {
        Ok(result) => result,
        Err((code, name)) => {
            errln!("Interrupted by {name}");
            // An aborted upload cleans up in a background task; give it time to
            // finish. A second signal skips the wait.
            tokio::select! {
                () = drain_pending_aborts(DRAIN_TIMEOUT) => {}
                _ = shutdown_signal() => errln!("Interrupted again; exiting at once"),
            }
            return Exit::Interrupted(ExitCode::from(code));
        }
    };

    let Err(err) = result else {
        return Exit::Done(ExitCode::SUCCESS);
    };

    // A failed command may have dropped uploads that clean up in a background
    // task (a multipart abort, a `file://` staging file); wait for them so the
    // process does not exit first. Ctrl-C skips the wait.
    tokio::select! {
        () = drain_pending_aborts(DRAIN_TIMEOUT) => {}
        _ = shutdown_signal() => errln!("Interrupted; exiting at once"),
    }

    // A closed output pipe is the reader's choice, not a failure to explain.
    if !err.is_broken_pipe() {
        // Messages can carry names taken from files and stores.
        let mut previous = err.to_string();
        errln!("Error: {}", sanitize(&previous));

        // Print the source chain so wrapped I/O / storage causes are visible.
        // Library errors embed their source in their own text; a cause that
        // text already contains adds nothing and is skipped.
        for cause in iter::successors(err.source(), |&e| e.source()) {
            let text = cause.to_string();
            if !previous.contains(&text) {
                errln!("  caused by: {}", sanitize(&text));
            }
            previous = text;
        }
    }

    Exit::Done(ExitCode::from(err.exit_code()))
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests {
    use super::harden_process;
    use rustix::process::{DumpableBehavior, Resource, Rlimit, dumpable_behavior, getrlimit};

    /// Irreversible for this test process, which no other test shares.
    #[test]
    fn hardening_disables_core_dumps() {
        assert_eq!(dumpable_behavior(), Ok(DumpableBehavior::Dumpable));
        harden_process();
        assert_eq!(dumpable_behavior(), Ok(DumpableBehavior::NotDumpable));
        assert_eq!(
            getrlimit(Resource::Core),
            Rlimit {
                current: Some(0),
                maximum: Some(0)
            }
        );
    }
}
