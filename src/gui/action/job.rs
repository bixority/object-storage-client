//! Job and dialog rules: starting operations, planning, confirming, progress.

use super::{Answer, Command, nav};
use crate::gui::display;
use crate::gui::location::Location;
use crate::gui::model::{
    AppState, Batch, Closing, Dialog, Entry, Item, Job, JobId, JobPhase, OnExisting, OpKind,
    Outcome, Plan, Side,
};
use std::mem;

/// The status shown once the worker has stopped.
pub(super) const WORKER_GONE: &str = "The background worker stopped; restart to continue";

/// The selected entries of `side`, in display order.
fn selected(state: &AppState, side: Side) -> Vec<Entry> {
    let panel = &state.panels[side.index()];
    panel
        .entries
        .iter()
        .filter(|entry| panel.selection.contains(&entry.key()))
        .cloned()
        .collect()
}

/// Whether a new operation may start. Says so in the status when the worker is gone.
fn can_start(state: &mut AppState) -> bool {
    if !state.worker_alive {
        WORKER_GONE.clone_into(&mut state.status);
        return false;
    }
    state.closing == Closing::Open && state.job.is_none() && state.dialog.is_none()
}

/// Allocates a job in `Planning` and asks the worker for its plan.
fn start(state: &mut AppState, batch: Batch) -> Vec<Command> {
    let id = JobId(state.next_job);
    state.next_job += 1;
    state.job = Some(Job {
        id,
        kind: batch.kind,
        phase: JobPhase::Planning,
        done: 0,
        total: 0,
        current: String::new(),
        src_dir: batch.src_dir.clone(),
        dst_dir: batch.dst_dir.clone(),
        cancelled: false,
    });
    vec![Command::Plan { job: id, batch }]
}

/// `Copy` or `Move` of the active panel's selection into the other panel.
pub(super) fn transfer(state: &mut AppState, kind: OpKind) -> Vec<Command> {
    if !can_start(state) {
        return Vec::new();
    }
    let side = state.active;
    let entries = selected(state, side);
    if entries.is_empty() {
        return Vec::new();
    }
    let src_dir = state.panels[side.index()].location.clone();
    let dst_dir = state.panels[side.other().index()].location.clone();
    if dst_dir.same_folder(&src_dir) {
        "Source and destination are the same folder".clone_into(&mut state.status);
        return Vec::new();
    }
    let into_itself = entries
        .iter()
        .filter(|entry| entry.is_folder())
        .filter_map(|entry| src_dir.child_dir(entry.name()))
        .any(|child| child.contains(&dst_dir));
    if into_itself {
        let verb = if kind == OpKind::Move { "move" } else { "copy" };
        state.status = format!("Cannot {verb} a folder into itself");
        return Vec::new();
    }
    start(
        state,
        Batch {
            kind,
            src_dir,
            entries,
            dst_dir,
            rename_to: None,
        },
    )
}

/// `Delete` of the active panel's selection.
pub(super) fn delete(state: &mut AppState) -> Vec<Command> {
    if !can_start(state) {
        return Vec::new();
    }
    let side = state.active;
    let entries = selected(state, side);
    if entries.is_empty() {
        return Vec::new();
    }
    let dir = state.panels[side.index()].location.clone();
    start(
        state,
        Batch {
            kind: OpKind::Delete,
            src_dir: dir.clone(),
            entries,
            dst_dir: dir,
            rename_to: None,
        },
    )
}

/// Opens the rename dialog for the single selected entry.
pub(super) fn start_rename(state: &mut AppState) -> Vec<Command> {
    if !can_start(state) {
        return Vec::new();
    }
    let side = state.active;
    let entries = selected(state, side);
    if let [entry] = entries.as_slice() {
        let from = entry.name().to_owned();
        state.dialog = Some(Dialog::Rename {
            side,
            folder: entry.is_folder(),
            to: from.clone(),
            from,
            error: None,
        });
    }
    Vec::new()
}

/// Text typed into the open dialog.
pub(super) fn dialog_text(state: &mut AppState, text: String) -> Vec<Command> {
    match &mut state.dialog {
        Some(Dialog::Rename { to, error, .. }) => {
            *to = text;
            *error = None;
        }
        Some(Dialog::ConfirmBucketDelete { typed, .. }) => *typed = text,
        _ => {}
    }
    Vec::new()
}

/// Why `to` cannot replace `from`, if it cannot.
fn invalid_name(from: &str, to: &str) -> Option<&'static str> {
    if to.is_empty() {
        Some("Enter a name")
    } else if to == "." || to == ".." {
        Some("A name cannot be . or ..")
    } else if to.contains('/') || (cfg!(windows) && to.contains('\\')) {
        Some("A name cannot contain a path separator")
    } else if to == from {
        Some("Enter a different name")
    } else {
        None
    }
}

/// `Proceed` on the rename dialog: validates the name, then plans the rename.
fn rename(state: &mut AppState) -> Vec<Command> {
    let Some(Dialog::Rename {
        side,
        folder,
        from,
        to,
        ..
    }) = &state.dialog
    else {
        return Vec::new();
    };
    let (side, to) = (*side, to.clone());
    let key = (*folder, from.clone());
    let problem = invalid_name(&key.1, &to).or_else(|| {
        let listed = state.panels[side.index()]
            .entries
            .iter()
            .any(|entry| entry.is(&key));
        (!listed).then_some("The entry is no longer listed")
    });
    if let Some(problem) = problem {
        if let Some(Dialog::Rename { error, .. }) = &mut state.dialog {
            *error = Some(problem.to_owned());
        }
        return Vec::new();
    }
    if !state.worker_alive {
        state.dialog = None;
        WORKER_GONE.clone_into(&mut state.status);
        return Vec::new();
    }
    let panel = &state.panels[side.index()];
    let entries: Vec<Entry> = panel
        .entries
        .iter()
        .filter(|entry| entry.is(&key))
        .cloned()
        .collect();
    let dir = panel.location.clone();
    state.dialog = None;
    start(
        state,
        Batch {
            kind: OpKind::Rename,
            src_dir: dir.clone(),
            entries,
            dst_dir: dir,
            rename_to: Some(to),
        },
    )
}

/// Moves a `Confirming` job to `Running` and hands its plan to the worker.
fn execute(state: &mut AppState, on_existing: OnExisting) -> Vec<Command> {
    let Some(job) = state.job.as_mut() else {
        return Vec::new();
    };
    let plan = match mem::replace(&mut job.phase, JobPhase::Running) {
        JobPhase::Confirming(plan) => plan,
        other => {
            job.phase = other;
            return Vec::new();
        }
    };
    job.done = 0;
    job.total = plan.items.len();
    job.current.clear();
    vec![Command::Execute {
        job: job.id,
        plan,
        on_existing,
    }]
}

/// An answer to any dialog but `Quit`.
pub(super) fn answer(state: &mut AppState, answer: Answer) -> Vec<Command> {
    let Some(dialog) = &state.dialog else {
        return Vec::new();
    };
    let on_existing = match (dialog, answer) {
        (_, Answer::Cancel) => {
            state.dialog = None;
            if matches!(
                state.job,
                Some(Job {
                    phase: JobPhase::Confirming(_),
                    ..
                })
            ) {
                state.job = None;
            }
            return Vec::new();
        }
        (Dialog::Rename { .. }, Answer::Proceed) => return rename(state),
        (Dialog::Failures { .. }, Answer::Proceed) => {
            state.dialog = None;
            return Vec::new();
        }
        (Dialog::ConfirmBucketDelete { bucket, typed, .. }, Answer::Proceed) => {
            if typed != bucket {
                return Vec::new();
            }
            OnExisting::Overwrite
        }
        (Dialog::Overwrite { .. }, Answer::Overwrite)
        | (
            Dialog::Unchecked { .. } | Dialog::ConfirmMove { .. } | Dialog::ConfirmDelete { .. },
            Answer::Proceed,
        ) => OnExisting::Overwrite,
        (Dialog::Overwrite { .. }, Answer::Skip) => OnExisting::Skip,
        _ => return Vec::new(),
    };
    state.dialog = None;
    execute(state, on_existing)
}

/// The job whose id is `id`, if it is the current one.
fn current(state: &mut AppState, id: JobId) -> Option<&mut Job> {
    state.job.as_mut().filter(|job| job.id == id)
}

/// The worker's plan: confirm it, or run it at once.
pub(super) fn planned(state: &mut AppState, id: JobId, plan: Plan) -> Vec<Command> {
    let closing = state.closing != Closing::Open;
    let Some(job) = current(state, id) else {
        return Vec::new();
    };
    if job.phase != JobPhase::Planning {
        return Vec::new();
    }
    // Closing or cancelled while planning: the plan is not wanted.
    if job.cancelled || closing {
        let cancelled = job.cancelled;
        state.job = None;
        // Closing: the drain owns the status.
        if cancelled && !closing {
            "Cancelled".clone_into(&mut state.status);
        }
        return Vec::new();
    }
    let kind = job.kind;
    if plan.items.is_empty() && !plan.failures.is_empty() {
        state.job = None;
        let report = Dialog::Failures {
            kind,
            outcome: Outcome {
                done: 0,
                cancelled: false,
                failures: plan.failures,
            },
        };
        // An open `Quit` question stays: the report waits until it is answered.
        match state.dialog {
            Some(Dialog::Quit) => state.deferred = Some(report),
            _ => state.dialog = Some(report),
        }
        return Vec::new();
    }
    job.phase = JobPhase::Confirming(plan);
    // An open close question stays; the plan's own question follows if the
    // user keeps the window open (`close::quit_answer`).
    if matches!(state.dialog, Some(Dialog::Quit)) {
        return Vec::new();
    }
    resume(state)
}

/// Asks the confirming job's question, or runs it when it needs none.
pub(super) fn resume(state: &mut AppState) -> Vec<Command> {
    let Some(Job {
        kind,
        phase: JobPhase::Confirming(plan),
        dst_dir,
        ..
    }) = &state.job
    else {
        return Vec::new();
    };
    match confirmation(*kind, plan, dst_dir) {
        Some(dialog) => {
            state.dialog = Some(dialog);
            Vec::new()
        }
        None => execute(state, OnExisting::Overwrite),
    }
}

/// The question `plan` needs answered first, or `None` to run it at once.
fn confirmation(kind: OpKind, plan: &Plan, dst_dir: &Location) -> Option<Dialog> {
    let total = plan.items.len();
    let existing = plan.existing();
    if existing > 0 {
        return Some(Dialog::Overwrite {
            kind,
            existing,
            total,
            dst: dst_dir.display(),
        });
    }
    match kind {
        OpKind::Copy | OpKind::Move | OpKind::Rename if !plan.checked => Some(Dialog::Unchecked {
            kind,
            total,
            dst: dst_dir.display(),
        }),
        OpKind::Move => Some(Dialog::ConfirmMove {
            total,
            dst: dst_dir.display(),
        }),
        OpKind::Delete => {
            let folders = plan
                .items
                .iter()
                .filter(|item| matches!(item, Item::DeletePrefix { .. }))
                .count();
            let bucket = dst_dir.bucket().filter(|_| dst_dir.is_root());
            let confirm = Dialog::ConfirmDelete {
                count: total,
                folders,
            };
            Some(
                bucket.map_or(confirm, |bucket| Dialog::ConfirmBucketDelete {
                    count: total,
                    folders,
                    bucket: display::text(&bucket),
                    typed: String::new(),
                }),
            )
        }
        OpKind::Copy | OpKind::Rename => None,
    }
}

/// The worker's progress on the running job.
pub(super) fn progress(
    state: &mut AppState,
    id: JobId,
    done: usize,
    total: usize,
    current_item: String,
) -> Vec<Command> {
    if let Some(job) = current(state, id) {
        job.done = done;
        job.total = total;
        job.current = current_item;
    }
    Vec::new()
}

/// The job ended: report it and refresh the panels it can have changed.
pub(super) fn finished(state: &mut AppState, id: JobId, outcome: Outcome) -> Vec<Command> {
    let Some(job) = current(state, id).map(|job| {
        (
            job.kind,
            job.total,
            (job.src_dir.clone(), job.dst_dir.clone()),
        )
    }) else {
        return Vec::new();
    };
    let (kind, total, (src_dir, dst_dir)) = job;
    state.job = None;
    // Closing: the drain owns the status, and nothing is left to report to.
    if state.closing != Closing::Open {
        return Vec::new();
    }
    let mut status = if outcome.cancelled {
        format!("Cancelled after {} of {total}", outcome.done)
    } else {
        let verb = match kind {
            OpKind::Copy => "copied",
            OpKind::Move => "moved",
            OpKind::Rename => "renamed",
            OpKind::Delete => "deleted",
        };
        format!("{} {verb}", outcome.done)
    };
    if !outcome.failures.is_empty() {
        status = format!("{status}; {} failed", outcome.failures.len());
        let report = Dialog::Failures { kind, outcome };
        // An open `Quit` question stays: the report waits until it is answered.
        match state.dialog {
            None => state.dialog = Some(report),
            Some(Dialog::Quit) => state.deferred = Some(report),
            Some(_) => {}
        }
    }
    state.status = status;
    // A copy changes only its destination; a move, rename or delete changes
    // its source, and a move its destination too. A panel above or below such
    // a folder shows the change, one elsewhere keeps its pages and selection.
    let touched: &[&Location] = match kind {
        OpKind::Copy => &[&dst_dir],
        OpKind::Move => &[&src_dir, &dst_dir],
        OpKind::Rename | OpKind::Delete => &[&src_dir],
    };
    let mut commands = Vec::new();
    for side in [Side::Left, Side::Right] {
        let shown = &state.panels[side.index()].location;
        if touched
            .iter()
            .any(|dir| dir.contains(shown) || shown.contains(dir))
        {
            commands.extend(nav::reload(state, side));
        }
    }
    commands
}

/// Asks the worker to stop the planning or running job.
pub(super) fn cancel(state: &mut AppState) -> Vec<Command> {
    match &mut state.job {
        Some(job) if matches!(job.phase, JobPhase::Planning | JobPhase::Running) => {
            // A plan already on its way is dropped when it arrives.
            job.cancelled = true;
            vec![Command::Abort(job.id)]
        }
        _ => Vec::new(),
    }
}
