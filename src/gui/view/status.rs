//! The status bar: the running job with Cancel, or the last status line.

use super::{doing, verb};
use crate::gui::action::Action;
use crate::gui::model::{AppState, Closing, JobPhase};
use eframe::egui;

pub(super) fn draw(ui: &mut egui::Ui, state: &AppState, actions: &mut Vec<Action>) {
    ui.horizontal(|ui| {
        match &state.job {
            // While closing drains, its status line says so.
            Some(job)
                if state.closing == Closing::Open
                    && matches!(job.phase, JobPhase::Planning | JobPhase::Running) =>
            {
                super::busy(ui);
                if job.phase == JobPhase::Planning {
                    ui.label(format!("{}: preparing…", verb(job.kind)));
                } else {
                    let text = format!(
                        "{} {} of {}",
                        capitalised(doing(job.kind)),
                        job.done,
                        job.total
                    );
                    ui.label(text);
                    if !job.current.is_empty() {
                        // `current` is display text already.
                        ui.add(egui::Label::new(&job.current).truncate());
                    }
                }
                if ui.button("Cancel").clicked() {
                    actions.push(Action::CancelJob);
                }
            }
            _ => {
                if state.closing != Closing::Open {
                    super::busy(ui);
                }
                ui.label(&state.status);
            }
        }
    });
}

/// "copying" as "Copying".
fn capitalised(word: &str) -> String {
    let mut chars = word.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().chain(chars).collect()
    })
}
