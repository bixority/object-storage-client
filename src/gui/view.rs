//! Draws [`AppState`] and returns what the user did. Views never change state.
//!
//! Every string drawn is display text from the state, or passes through
//! `display` here (raw entry names).

mod address_bar;
mod dialogs;
mod panel;
mod startup_error;
mod status;
#[cfg(test)]
mod tests;
mod toolbar;

pub(super) use address_bar::address_id;
pub(super) use startup_error::draw as draw_startup_error;

use super::action::Action;
use super::model::{AppState, OpKind, Side};
use eframe::egui;

/// Draws the main window: toolbar, the two panels, the status bar and the
/// open dialog.
pub(super) fn draw(ui: &mut egui::Ui, state: &AppState) -> Vec<Action> {
    let mut actions = Vec::new();
    egui::Panel::top("toolbar").show(ui, |ui| toolbar::draw(ui, state, &mut actions));
    egui::Panel::bottom("status").show(ui, |ui| status::draw(ui, state, &mut actions));
    egui::CentralPanel::default().show(ui, |ui| {
        ui.columns_const(|[left, right]| {
            panel::draw(left, state, Side::Left, &mut actions);
            panel::draw(right, state, Side::Right, &mut actions);
        });
    });
    if let Some(dialog) = &state.dialog {
        dialogs::draw(ui, state, dialog, &mut actions);
    }
    actions
}

/// "1 item", "3 items".
fn count(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// The verb for `kind`, capitalised: "Copy".
const fn verb(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Copy => "Copy",
        OpKind::Move => "Move",
        OpKind::Rename => "Rename",
        OpKind::Delete => "Delete",
    }
}

/// The present participle for `kind`: "copying".
const fn doing(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Copy => "copying",
        OpKind::Move => "moving",
        OpKind::Rename => "renaming",
        OpKind::Delete => "deleting",
    }
}
