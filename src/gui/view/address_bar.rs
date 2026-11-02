//! Up, the address field and Go.

use crate::gui::action::Action;
use crate::gui::model::{PanelState, Side};
use eframe::egui;

/// The id of `side`'s address field, so Ctrl+L can focus it.
pub(in crate::gui) fn address_id(side: Side) -> egui::Id {
    egui::Id::unique(("osc-gui address", side.index()))
}

pub(super) fn draw(ui: &mut egui::Ui, panel: &PanelState, side: Side, actions: &mut Vec<Action>) {
    ui.horizontal(|ui| {
        let up = ui
            .add_enabled(!panel.location.is_root(), egui::Button::new("⬆ Up"))
            .on_hover_text("Up one level (Backspace)");
        if up.clicked() {
            actions.push(Action::Up(side));
        }
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
