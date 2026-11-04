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

use crate::connection::Connection;
use app::{App, StartupErrorApp};
use args::Args;
use model::{AppState, Encryption};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use worker::{Factory, Repaint};

/// How long the runtime may take to stop once the window has closed.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Starts the GUI: opens the key source, then the window. Returns 1 when the
/// keys or the window could not be opened.
#[must_use]
pub fn run(args: &Args) -> ExitCode {
    let runtime = match crate::cli::build_runtime() {
        Ok(runtime) => runtime,
        Err(err) => {
            return startup_error(&format!("failed to start the async runtime: {err}"));
        }
    };
    let keys = match runtime.block_on(startup::open_keys(args)) {
        Ok(keys) => keys,
        Err(err) => {
            drop(runtime);
            return startup_error(&err.to_string());
        }
    };
    let encryption = keys.as_ref().map_or(Encryption::Off, |(_, kind)| *kind);
    let (state, commands) = AppState::new(encryption);
    let handle = runtime.handle().clone();
    let result = eframe::run_native(
        "osc-gui",
        options([1280.0, 760.0]),
        Box::new(move |cc| {
            let ctx = cc.egui_ctx.clone();
            let repaint: Repaint = Arc::new(move || ctx.request_repaint());
            let host = crate::connection::Host::real();
            let worker = match keys {
                None => {
                    let factory: Factory<crate::codec::Plain> = Box::new(Connection::client);
                    worker::spawn(&handle, factory, host, repaint, true)
                }
                Some((config, _)) => {
                    let factory: Factory<crate::encryption::Envelope> = Box::new(move |conn| {
                        conn.client()
                            .map(|client| client.with_encryption(config.clone()))
                    });
                    worker::spawn(&handle, factory, host, repaint, true)
                }
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
