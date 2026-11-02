//! The `osc-gui` desktop browser. Not part of the public API: it is exported
//! only so the binary can reach it.

mod action;
mod app;
pub mod args;
mod display;
mod keys;
pub mod location;
pub mod model;
pub mod ops;
mod startup;
mod view;
mod worker;

use app::{App, StartupErrorApp};
use args::Args;
use location::Location;
use model::{AppState, Encryption};
use startup::Opened;
use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use worker::Repaint;

/// How long the runtime may take to stop once the window has closed.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Starts the GUI: opens the client, then the window. Returns 1 when the
/// client or the window could not be opened.
#[must_use]
pub fn run(args: Args) -> ExitCode {
    let runtime = match build_runtime() {
        Ok(runtime) => runtime,
        Err(err) => {
            return startup_error(&format!("failed to start the async runtime: {err}"));
        }
    };
    let opened = match runtime.block_on(startup::open_client(&args)) {
        Ok(opened) => opened,
        Err(err) => {
            drop(runtime);
            return startup_error(&err.to_string());
        }
    };
    let Args { left, right, .. } = args;
    let (left, right) = match (initial(left.as_deref()), initial(right.as_deref())) {
        (Ok(left), Ok(right)) => (left, right),
        (Err(err), _) | (_, Err(err)) => {
            drop(runtime);
            return startup_error(&err);
        }
    };
    let encryption = match &opened {
        Opened::Plain(_) => Encryption::Off,
        Opened::Encrypted(_, encryption) => *encryption,
    };
    let (mut state, commands) = AppState::new(left.0, right.0, encryption);
    state.panels[0].address_error = left.1;
    state.panels[1].address_error = right.1;
    let handle = runtime.handle().clone();
    let result = eframe::run_native(
        "osc-gui",
        options([1280.0, 760.0]),
        Box::new(move |cc| {
            let ctx = cc.egui_ctx.clone();
            let repaint: Repaint = Arc::new(move || ctx.request_repaint());
            let worker = match opened {
                Opened::Plain(client) => worker::spawn(&handle, client, repaint, true),
                Opened::Encrypted(client, _) => worker::spawn(&handle, client, repaint, true),
            };
            Ok(Box::new(App::new(state, worker, commands)))
        }),
    );
    runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            report(&crate::cli::sanitize(&err.to_string()));
            ExitCode::FAILURE
        }
    }
}

/// The multi-thread runtime, with `osc`'s worker count rule.
fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // As `osc`: an explicit TOKIO_WORKER_THREADS still wins.
    if env::var_os("TOKIO_WORKER_THREADS").is_none() {
        builder.worker_threads(
            thread::available_parallelism()
                .map_or(1, usize::from)
                .min(4),
        );
    }
    builder.build()
}

/// A panel's first folder: `arg`, or the current directory with the parse
/// error of `arg` to show under its address bar.
fn initial(arg: Option<&str>) -> Result<(Location, Option<String>), String> {
    let Some(arg) = arg else {
        return Location::parse(".").map(|location| (location, None));
    };
    Location::parse(arg).map_or_else(
        |err| Location::parse(".").map(|location| (location, Some(err))),
        |location| Ok((location, None)),
    )
}

/// Reports a startup failure on stderr and in a small window; returns 1.
fn startup_error(message: &str) -> ExitCode {
    let message = crate::cli::sanitize(message);
    report(&message);
    let result = eframe::run_native(
        "osc-gui",
        options([560.0, 240.0]),
        Box::new(|_| Ok(Box::new(StartupErrorApp { message }))),
    );
    if let Err(err) = result {
        report(&crate::cli::sanitize(&err.to_string()));
    }
    ExitCode::FAILURE
}

/// Window options; eframe's persistence stays off.
fn options(size: [f32; 2]) -> eframe::NativeOptions {
    eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default().with_inner_size(size),
        ..Default::default()
    }
}

/// Writes an error line to standard error, ignoring a failure.
fn report(message: &str) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "Error: {message}");
}
