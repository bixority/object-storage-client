use clap::Parser;
use object_storage_client::cli::{Cli, drain_pending_aborts, sanitize};
use std::env;
use std::error::Error;
use std::iter;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

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

fn main() -> ExitCode {
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
            eprintln!("Error: failed to start the async runtime: {err}");
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
        result = Cli::parse().run() => Ok(result),
        signal = shutdown_signal() => Err(signal),
    };
    let result = match outcome {
        Ok(result) => result,
        Err((code, name)) => {
            eprintln!("Interrupted by {name}");
            // An aborted upload cleans up in a background task; give it time to
            // finish. A second signal skips the wait.
            tokio::select! {
                () = drain_pending_aborts(DRAIN_TIMEOUT) => {}
                _ = shutdown_signal() => eprintln!("Interrupted again; exiting at once"),
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
        _ = shutdown_signal() => eprintln!("Interrupted; exiting at once"),
    }

    // Messages can carry names taken from files and stores.
    eprintln!("Error: {}", sanitize(&err.to_string()));

    // Print the full source chain so wrapped I/O / storage causes are visible.
    for cause in iter::successors(err.source(), |&e| e.source()) {
        eprintln!("  caused by: {}", sanitize(&cause.to_string()));
    }

    Exit::Done(ExitCode::from(err.exit_code()))
}
