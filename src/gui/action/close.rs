//! Close and shutdown rules.
//!
//! Closing never drops a future on the UI thread: the worker aborts every task
//! inside the runtime and waits for interrupted uploads to clean up
//! (`Command::Shutdown`), then reports `Event::ShutdownDone`, and only then is
//! the window closed (`Closing::Done`).

use super::{Answer, Command, job};
use crate::gui::model::{AppState, Closing, Dialog, JobPhase};

/// The status shown while the worker drains.
pub(super) const DRAINING: &str = "Cleaning up unfinished uploads…";

/// Starts the drain: no new work, the worker shuts down.
fn drain(state: &mut AppState) -> Vec<Command> {
    if !state.worker_alive {
        // Nothing is left to drain, and no `ShutdownDone` would come.
        state.closing = Closing::Done;
        return Vec::new();
    }
    state.closing = Closing::Draining;
    state.dialog = None;
    DRAINING.clone_into(&mut state.status);
    vec![Command::Shutdown]
}

/// The window's close button or shortcut: asks first while work runs.
pub(super) fn close_requested(state: &mut AppState) -> Vec<Command> {
    if state.closing != Closing::Open {
        return Vec::new();
    }
    match &state.job {
        Some(job) if matches!(job.phase, JobPhase::Planning | JobPhase::Running) => {
            state.dialog = Some(Dialog::Quit);
            Vec::new()
        }
        // A job waiting for an answer has not started: nothing to lose.
        _ => {
            state.job = None;
            drain(state)
        }
    }
}

/// An interrupt signal: close without asking.
pub(super) fn interrupt(state: &mut AppState) -> Vec<Command> {
    if state.closing != Closing::Open {
        return Vec::new();
    }
    drain(state)
}

/// The worker finished draining: the window may close.
pub(super) fn shutdown_done(state: &mut AppState) -> Vec<Command> {
    if state.closing == Closing::Draining {
        state.closing = Closing::Done;
    }
    Vec::new()
}

/// An answer to `Dialog::Quit`.
pub(super) fn quit_answer(state: &mut AppState, answer: Answer) -> Vec<Command> {
    match answer {
        Answer::Proceed => drain(state),
        Answer::Cancel => {
            state.dialog = None;
            // A report that arrived while the question was open shows now;
            // else a plan that arrived meanwhile asks its own question.
            if let Some(report) = state.deferred.take() {
                state.dialog = Some(report);
                return Vec::new();
            }
            job::resume(state)
        }
        Answer::Overwrite | Answer::Skip => Vec::new(),
    }
}

/// The worker is gone: nothing it was doing will report back.
pub(super) fn worker_gone(state: &mut AppState) -> Vec<Command> {
    state.worker_alive = false;
    state.job = None;
    if matches!(
        state.dialog,
        Some(
            Dialog::Quit
                | Dialog::Overwrite { .. }
                | Dialog::Unchecked { .. }
                | Dialog::ConfirmMove { .. }
                | Dialog::ConfirmDelete { .. }
                | Dialog::Login { .. }
        )
    ) {
        state.dialog = None;
    }
    if state.closing == Closing::Draining {
        state.closing = Closing::Done;
    }
    job::WORKER_GONE.clone_into(&mut state.status);
    Vec::new()
}
