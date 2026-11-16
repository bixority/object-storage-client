//! Highlights of the panels: the band of the cursor row and the fill of
//! selected rows.

use eframe::egui;

/// Alpha of the band behind the cursor row, over the full width of the table.
const BAND_ALPHA: u8 = 72;

/// The distance of `row`'s top from the top of the table's content.
#[expect(clippy::cast_precision_loss)]
pub(super) fn row_top(row: usize, row_height: f32, spacing: f32) -> f32 {
    row as f32 * (row_height + spacing)
}

/// Draws the band over the full width of the cursor row `row` of a table whose
/// scrolled body is `body` and whose content is scrolled by `scroll`. Paints
/// only; takes no input.
pub(super) fn draw_cursor_band(
    ui: &egui::Ui,
    row: Option<usize>,
    geometry: (egui::Rect, f32),
    row_height: f32,
    color: egui::Color32,
) {
    let Some(row) = row else { return };
    let (body, scroll) = geometry;
    let spacing = ui.spacing().item_spacing.y;
    let y = body.top() + row_top(row, row_height, spacing) - scroll;
    let band = egui::Rect::from_min_size(
        egui::pos2(body.left(), y),
        egui::vec2(body.width(), row_height),
    );
    ui.painter().with_clip_rect(body).rect_filled(
        band,
        0.0,
        crate::gui::theme::with_alpha(color, BAND_ALPHA),
    );
}

/// Paints the selection fill behind the contents of the table cell `ui` when
/// `selected`. Paints only; takes no input.
pub(super) fn draw_row_fill(ui: &egui::Ui, selected: bool) {
    if !selected {
        return;
    }
    let fill = ui.visuals().selection.bg_fill;
    // As wide as the gap-free background the table draws.
    let rect = ui.max_rect().expand2(0.5 * ui.spacing().item_spacing);
    ui.painter()
        .with_clip_rect(rect)
        .rect_filled(rect, 0.0, fill);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_tops_step_by_row_and_spacing() {
        assert!(row_top(0, 20.0, 4.0).abs() < f32::EPSILON);
        assert!((row_top(1, 20.0, 4.0) - 24.0).abs() < f32::EPSILON);
        assert!((row_top(999, 20.0, 4.0) - 23_976.0).abs() < 1e-2);
    }
}
