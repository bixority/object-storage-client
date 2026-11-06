//! Operation buttons and the encryption indicator.

use super::security;
use crate::gui::action::Action;
use crate::gui::model::{AppState, Closing};
use eframe::egui;

pub(super) fn draw(ui: &mut egui::Ui, state: &AppState, actions: &mut Vec<Action>) {
    let panel = &state.panels[state.active.index()];
    let selected = panel.selection.len();
    let idle = state.job.is_none()
        && state.dialog.is_none()
        && state.worker_alive
        && state.closing == Closing::Open;
    ui.horizontal(|ui| {
        let buttons = [
            ("Copy  F5", idle && selected > 0, Action::Copy),
            ("Move  F6", idle && selected > 0, Action::Move),
            ("Rename  F2", idle && selected == 1, Action::StartRename),
            ("Delete  Del", idle && selected > 0, Action::Delete),
            (
                "Refresh  Ctrl+R",
                state.closing == Closing::Open,
                Action::Refresh(state.active),
            ),
        ];
        for (label, enabled, action) in buttons {
            if ui.add_enabled(enabled, egui::Button::new(label)).clicked() {
                actions.push(action);
            }
        }
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.label(egui::RichText::new(security::encryption_icon(state.encryption)).monospace())
                .on_hover_text(security::encryption_hover(state.encryption));
        });
    });
}
