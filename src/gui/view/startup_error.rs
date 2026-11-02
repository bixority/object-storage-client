//! The "could not start" window.

use eframe::egui;

/// Draws the startup error; returns whether Quit was clicked. `message` is
/// display text (sanitized by the caller).
pub(in crate::gui) fn draw(ui: &mut egui::Ui, message: &str) -> bool {
    let mut quit = false;
    egui::CentralPanel::default().show(ui, |ui| {
        ui.heading("osc-gui could not start");
        ui.add_space(8.0);
        egui::ScrollArea::vertical()
            .id_salt("startup error")
            .max_height(ui.available_height() - 48.0)
            .show(ui, |ui| {
                ui.label(message);
            });
        ui.add_space(8.0);
        quit = ui.button("Quit").clicked();
    });
    quit
}
