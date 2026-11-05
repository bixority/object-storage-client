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
        | Dialog::Login { .. }
        | Dialog::Quit => Answer::Cancel,
    }
}

/// The SSO login: the code to approve, or why the login failed.
fn login(
    ui: &mut egui::Ui,
    label: &str,
    code: Option<&(String, String)>,
    error: Option<&str>,
) -> Option<Answer> {
    ui.heading(format!("Log in to {label}"));
    if let Some(error) = error {
        ui.colored_label(ui.visuals().error_fg_color, display::text(error));
    } else if let Some((user_code, url)) = code {
        ui.label("Approve this code in the browser:");
        ui.heading(display::text(user_code));
        if ui.button("Open browser").clicked() {
            ui.ctx().open_url(egui::OpenUrl::new_tab(url));
        }
    } else {
        ui.label("Waiting…");
    }
    let cancel = if error.is_some() { "Close" } else { "Cancel" };
    ui.button(cancel).clicked().then_some(Answer::Cancel)
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
        Dialog::Failures { kind, outcome } => failures(ui, *kind, outcome),
        Dialog::Login {
            label, code, error, ..
        } => login(ui, label, code.as_ref(), error.as_deref()),
        Dialog::Quit => {
            if let Some(job) = &state.job {
                // The job keeps running behind the question: show its live count.
                ui.heading(format!("Quit while {}?", doing(job.kind)));
                ui.label(format!(
                    "{} of {} done. Quitting stops the job; finished items stay done.",
                    job.done,
                    count(job.total, "item")
                ));
                buttons(
                    ui,
                    &[("Quit", Answer::Proceed), ("Keep running", Answer::Cancel)],
                )
            } else {
                ui.heading("The job has ended.");
                ui.label("Quit now?");
                buttons(ui, &[("Quit", Answer::Proceed), ("Stay", Answer::Cancel)])
            }
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

/// The dialog's single-line text field, focused unless something else is.
fn dialog_field(ui: &mut egui::Ui, value: &str, actions: &mut Vec<Action>) {
    let mut text = value.to_owned();
    let field = ui.add(egui::TextEdit::singleline(&mut text).desired_width(f32::INFINITY));
    if ui.memory(|m| m.focused().is_none()) {
        field.request_focus();
    }
    if field.changed() {
        actions.push(Action::DialogText(text));
    }
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
    dialog_field(ui, to, actions);
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
