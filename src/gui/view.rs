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
pub(super) use panel::page_rows;
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

/// How often the busy indicator moves.
const BUSY_STEP: std::time::Duration = std::time::Duration::from_millis(125);

/// A spinner that redraws about 8 times a second. `egui`'s own requests a
/// repaint every frame, which keeps the whole window drawing at display rate
/// for as long as a job runs.
fn busy(ui: &mut egui::Ui) {
    let size = ui.spacing().interact_size.y;
    let (rect, response) = ui.allocate_exact_size(egui::vec2(size, size), egui::Sense::hover());
    response.widget_info(|| egui::WidgetInfo::new(egui::Role::ProgressIndicator));
    if !ui.is_rect_visible(rect) {
        return;
    }
    ui.request_repaint_after(BUSY_STEP);
    // The time in whole steps, so the picture is the same until the next one.
    let step = BUSY_STEP.as_secs_f64();
    let time = (ui.input(|i| i.time) / step).floor() * step;
    let radius = rect.height().min(rect.width()).mul_add(0.5, -2.0);
    let start = time * std::f64::consts::TAU;
    let end = 240_f64.to_radians().mul_add(time.sin(), start);
    let points = (0..=16)
        .map(|i| {
            let angle = start + (end - start) * f64::from(i) / 16.0;
            let (sin, cos) = angle.sin_cos();
            #[allow(clippy::cast_possible_truncation)]
            let offset = egui::vec2(cos as f32, sin as f32) * radius;
            rect.center() + offset
        })
        .collect();
    let color = ui.visuals().strong_text_color();
    ui.painter()
        .add(egui::Shape::line(points, egui::Stroke::new(3.0, color)));
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
