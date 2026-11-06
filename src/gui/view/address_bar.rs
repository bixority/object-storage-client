//! The connection list, the address field and Go.

use super::security;
use crate::gui::action::Action;
use crate::gui::display;
use crate::gui::model::{AppState, Side};
use eframe::egui;

/// The id of `side`'s address field, so Ctrl+L can focus it.
pub(in crate::gui) fn address_id(side: Side) -> egui::Id {
    egui::Id::unique(("osc-gui address", side.index()))
}

pub(super) fn draw(ui: &mut egui::Ui, state: &AppState, side: Side, actions: &mut Vec<Action>) {
    let panel = &state.panels[side.index()];
    ui.horizontal(|ui| {
        let tls = security::tls_state(&panel.connection);
        ui.label(egui::RichText::new(security::tls_icon(tls)).monospace())
            .on_hover_text(security::tls_hover(tls));
        egui::ComboBox::from_id_salt(("osc-gui connection", side.index()))
            .selected_text(display::text(&panel.connection.label))
            .show_ui(ui, |ui| {
                for conn in &state.connections {
                    let current = conn.id == panel.connection.id;
                    if ui
                        .selectable_label(current, display::text(&conn.label))
                        .clicked()
                    {
                        actions.push(Action::SelectConnection {
                            side,
                            id: conn.id.clone(),
                        });
                    }
                }
                ui.separator();
                if state.discovering {
                    ui.add_enabled(
                        false,
                        egui::Label::new(egui::RichText::new("Searching…").small()),
                    );
                } else if ui.selectable_label(false, "Rescan").clicked() {
                    actions.push(Action::Rescan);
                }
                for note in &state.notes {
                    ui.add_enabled(
                        false,
                        egui::Label::new(egui::RichText::new(display::text(note)).small()),
                    );
                }
            });
        // Right to left: Go sits at the right edge, the field fills the rest.
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            let go = ui.button("Go");
            let mut text = panel.address_text.clone();
            let field = ui.add(
                egui::TextEdit::singleline(&mut text)
                    .id(address_id(side))
                    .desired_width(ui.available_width()),
            );
            if field.changed() {
                actions.push(Action::AddressEdited(side, text.clone()));
            }
            let entered = field.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            if entered || go.clicked() {
                actions.push(Action::Navigate(side, text));
            }
        });
    });
    if let Some(error) = &panel.address_error {
        ui.colored_label(ui.visuals().error_fg_color, error);
    }
}
