//! The modal dialogs. Enter takes the answer that cannot lose data; Esc cancels.

use super::{count, doing, verb};
use crate::gui::action::{Action, Answer};
use crate::gui::display;
use crate::gui::model::{AppState, Dialog, OpKind, Outcome};
use eframe::egui;

/// Height of one line of the failure list.
const LINE_HEIGHT: f32 = 18.0;

pub(super) fn draw(ui: &egui::Ui, state: &AppState, dialog: &Dialog, actions: &mut Vec<Action>) {
    let (enter, escape) = ui.input(|i| {
        (
            i.key_pressed(egui::Key::Enter),
            i.key_pressed(egui::Key::Escape),
        )
    });
    let modal = egui::Modal::new(egui::Id::unique("osc-gui dialog"));
    let answer = modal
        .show(ui.ctx(), |ui| {
            ui.set_max_width(520.0);
            body(ui, state, dialog, actions)
        })
        .inner;
    let answer = answer.or_else(|| {
        if escape {
            Some(Answer::Cancel)
        } else if enter {
            Some(default_answer(dialog))
        } else {
            None
        }
    });
    if let Some(answer) = answer {
        actions.push(Action::DialogAnswer(answer));
    }
}

/// What Enter answers: the option that cannot lose data.
pub(super) const fn default_answer(dialog: &Dialog) -> Answer {
    match dialog {
        Dialog::Overwrite { kind, .. } if !matches!(kind, OpKind::Move) => Answer::Skip,
        Dialog::Rename { .. } | Dialog::Failures { .. } => Answer::Proceed,
        // Skipping still deletes the moved sources: a move is not run on Enter.
        Dialog::Overwrite { .. }
        | Dialog::Unchecked { .. }
        | Dialog::ConfirmMove { .. }
        | Dialog::ConfirmDelete { .. }
        | Dialog::ConfirmBucketDelete { .. }
        | Dialog::Quit { .. } => Answer::Cancel,
    }
}

/// The dialog's text and buttons; returns the button clicked.
fn body(
    ui: &mut egui::Ui,
    state: &AppState,
    dialog: &Dialog,
    actions: &mut Vec<Action>,
) -> Option<Answer> {
    match dialog {
        Dialog::Rename {
            from, to, error, ..
        } => rename(ui, from, to, error.as_deref(), actions),
        Dialog::Overwrite {
            kind,
            existing,
            total,
            dst,
        } => {
            ui.heading("Some items already exist");
            ui.label(overwrite_text(*kind, *existing, *total, dst));
            buttons(
                ui,
                &[
                    ("Overwrite all", Answer::Overwrite),
                    ("Skip existing", Answer::Skip),
                    ("Cancel", Answer::Cancel),
                ],
            )
        }
        Dialog::Unchecked { kind, total, dst } => {
            ui.heading(format!("{} {}?", verb(*kind), count(*total, "item")));
            ui.label(format!(
                "There are too many items to check for existing ones. Anything with the same name in {dst} will be overwritten."
            ));
            buttons(
                ui,
                &[(verb(*kind), Answer::Proceed), ("Cancel", Answer::Cancel)],
            )
        }
        Dialog::ConfirmMove { total, dst } => {
            ui.heading(format!("Move {}?", count(*total, "item")));
            ui.label(format!("They are copied to {dst}, then deleted here."));
            buttons(ui, &[("Move", Answer::Proceed), ("Cancel", Answer::Cancel)])
        }
        Dialog::ConfirmDelete { count: n, folders } => {
            delete_text(ui, *n, *folders);
            buttons(
                ui,
                &[("Delete", Answer::Proceed), ("Cancel", Answer::Cancel)],
            )
        }
        Dialog::ConfirmBucketDelete {
            count: n,
            folders,
            bucket,
            typed,
        } => bucket_delete(ui, *n, *folders, bucket, typed, actions),
        Dialog::Failures { kind, outcome } => failures(ui, *kind, outcome),
        Dialog::Quit { done, total } => {
            let doing = state.job.as_ref().map_or("working", |job| doing(job.kind));
            // The job keeps running behind the question: show its live count.
            let (done, total) = state
                .job
                .as_ref()
                .map_or((*done, *total), |job| (job.done, job.total));
            ui.heading(format!("Quit while {doing}?"));
            ui.label(format!(
                "{done} of {} done. Quitting stops the job; finished items stay done.",
                count(total, "item")
            ));
            buttons(
                ui,
                &[("Quit", Answer::Proceed), ("Keep running", Answer::Cancel)],
            )
        }
    }
}

/// "Move 5 items to …? 2 already exist there. The sources are deleted after copying."
pub(super) fn overwrite_text(kind: OpKind, existing: usize, total: usize, dst: &str) -> String {
    let already = if existing == 1 {
        "1 already exists there.".to_owned()
    } else {
        format!("{existing} already exist there.")
    };
    let mut text = format!(
        "{} {} to {dst}? {already}",
        verb(kind),
        count(total, "item")
    );
    if kind == OpKind::Move {
        text.push_str(" The sources are deleted after copying.");
    }
    text
}

/// A row of buttons, right-aligned in the given order.
fn buttons(ui: &mut egui::Ui, choices: &[(&str, Answer)]) -> Option<Answer> {
    ui.add_space(8.0);
    let mut picked = None;
    ui.horizontal(|ui| {
        for (label, answer) in choices {
            if ui.button(*label).clicked() {
                picked = Some(*answer);
            }
        }
    });
    picked
}

fn rename(
    ui: &mut egui::Ui,
    from: &str,
    to: &str,
    error: Option<&str>,
    actions: &mut Vec<Action>,
) -> Option<Answer> {
    ui.heading("Rename");
    ui.label(format!("New name for {}:", display::text(from)));
    let mut text = to.to_owned();
    let field = ui.add(egui::TextEdit::singleline(&mut text).desired_width(f32::INFINITY));
    if ui.memory(|m| m.focused().is_none()) {
        field.request_focus();
    }
    if field.changed() {
        actions.push(Action::DialogText(text));
    }
    if let Some(error) = error {
        ui.colored_label(ui.visuals().error_fg_color, error);
    }
    buttons(
        ui,
        &[("Rename", Answer::Proceed), ("Cancel", Answer::Cancel)],
    )
}

fn delete_text(ui: &mut egui::Ui, n: usize, folders: usize) {
    ui.heading(format!("Delete {}?", count(n, "item")));
    if folders > 0 {
        ui.label(if folders == n {
            "Everything inside is deleted too.".to_owned()
        } else {
            format!(
                "{} of them {} a folder; everything inside is deleted too.",
                folders,
                if folders == 1 { "is" } else { "are" }
            )
        });
    }
    ui.label("This cannot be undone.");
}

fn bucket_delete(
    ui: &mut egui::Ui,
    n: usize,
    folders: usize,
    bucket: &str,
    typed: &str,
    actions: &mut Vec<Action>,
) -> Option<Answer> {
    delete_text(ui, n, folders);
    ui.label(format!(
        "This is the root of the bucket {bucket}. Type its name to confirm:"
    ));
    let mut text = typed.to_owned();
    let field = ui.add(egui::TextEdit::singleline(&mut text).desired_width(f32::INFINITY));
    if ui.memory(|m| m.focused().is_none()) {
        field.request_focus();
    }
    if field.changed() {
        actions.push(Action::DialogText(text));
    }
    ui.add_space(8.0);
    let mut picked = None;
    ui.horizontal(|ui| {
        let delete = ui.add_enabled(typed == bucket, egui::Button::new("Delete"));
        if delete.clicked() {
            picked = Some(Answer::Proceed);
        }
        if ui.button("Cancel").clicked() {
            picked = Some(Answer::Cancel);
        }
    });
    picked
}

fn failures(ui: &mut egui::Ui, kind: OpKind, outcome: &Outcome) -> Option<Answer> {
    let failed = count(outcome.failures.len(), "item");
    if outcome.cancelled {
        ui.heading(format!("{} cancelled; {failed} failed", verb(kind)));
    } else {
        ui.heading(format!("{} finished; {failed} failed", verb(kind)));
    }
    ui.label(format!("{} done.", count(outcome.done, "item")));
    // Every line is display text already (redacted and escaped by `ops`).
    let line = |index: usize| {
        let failure = &outcome.failures[index];
        format!("{}: {}", failure.name, failure.error)
    };
    egui::ScrollArea::vertical()
        .id_salt("failures")
        .max_height(240.0)
        .auto_shrink([false, true])
        .show_rows(ui, LINE_HEIGHT, outcome.failures.len(), |ui, rows| {
            for index in rows {
                ui.add(egui::Label::new(line(index)).truncate());
            }
        });
    ui.add_space(8.0);
    let mut picked = None;
    ui.horizontal(|ui| {
        if ui.button("Copy list").clicked() {
            let list: Vec<String> = (0..outcome.failures.len()).map(line).collect();
            ui.ctx().copy_text(list.join("\n"));
        }
        if ui.button("Close").clicked() {
            picked = Some(Answer::Proceed);
        }
    });
    picked
}
