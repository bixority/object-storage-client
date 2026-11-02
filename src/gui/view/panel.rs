//! One panel: address bar, banners, the listing table and its footer.

use super::address_bar;
use crate::gui::action::{Action, SelectOp};
use crate::gui::display;
use crate::gui::model::{AppState, Entry, MAX_ENTRIES, PanelState, Side};
use eframe::egui;
use egui_extras::{Column, TableBuilder};

/// Height of one table row.
const ROW_HEIGHT: f32 = 20.0;
/// Width of a panel's border, active or not: switching panels moves nothing.
const BORDER_WIDTH: f32 = 2.0;
/// Rows a page moves before the table was first drawn.
pub(super) const DEFAULT_PAGE_ROWS: usize = 20;

/// Where the rows the table of `side` showed fully last frame are kept.
fn page_rows_id(side: Side) -> egui::Id {
    egui::Id::unique(("osc-gui page rows", side.index()))
}

/// Where the `cursor_moves` the view of `side` last scrolled for is kept.
pub(super) fn seen_moves_id(side: Side) -> egui::Id {
    egui::Id::unique(("osc-gui cursor moves", side.index()))
}

/// How many rows of `side`'s table were fully visible last frame: the
/// distance of Page Up and Page Down.
pub(in crate::gui) fn page_rows(ctx: &egui::Context, side: Side) -> usize {
    ctx.data(|d| d.get_temp(page_rows_id(side)))
        .unwrap_or(DEFAULT_PAGE_ROWS)
}

/// The `cursor_moves` the view of `side` last scrolled for, if any.
pub(super) fn seen_moves(ctx: &egui::Context, side: Side) -> Option<u64> {
    ctx.data(|d| d.get_temp(seen_moves_id(side)))
}

/// `color` in greyscale, keeping its alpha: its luminance as a grey.
pub(super) fn desaturate(color: egui::Color32) -> egui::Color32 {
    let luma =
        (299 * u32::from(color.r()) + 587 * u32::from(color.g()) + 114 * u32::from(color.b()))
            / 1000;
    let luma = u8::try_from(luma).unwrap_or(u8::MAX);
    egui::Color32::from_rgba_unmultiplied(luma, luma, luma, color.a())
}

/// The border of a panel. Only the colour shows which one is active.
pub(super) fn panel_stroke(visuals: &egui::Visuals, active: bool) -> egui::Stroke {
    let color = if active {
        visuals.selection.bg_fill
    } else {
        visuals.widgets.noninteractive.bg_stroke.color
    };
    egui::Stroke::new(BORDER_WIDTH, color)
}

pub(super) fn draw(ui: &mut egui::Ui, state: &AppState, side: Side, actions: &mut Vec<Action>) {
    let panel = &state.panels[side.index()];
    let active = state.active == side;
    let frame = egui::Frame::group(ui.style())
        .stroke(panel_stroke(ui.visuals(), active))
        .show(ui, |ui| {
            ui.set_min_size(ui.available_size());
            address_bar::draw(ui, panel, side, actions);
            banners(ui, panel, side, actions);
            // Leave room for the footer below the table.
            let footer = ui
                .spacing()
                .item_spacing
                .y
                .mul_add(2.0, ui.spacing().interact_size.y);
            let height = (ui.available_height() - footer).max(ROW_HEIGHT);
            ui.allocate_ui(egui::vec2(ui.available_width(), height), |ui| {
                // Rows of the previous folder are dimmed and inert while the new
                // one loads: a name would resolve against the new location.
                ui.add_enabled_ui(!panel.stale(), |ui| {
                    if active {
                        table(ui, panel, side, height, actions);
                    } else {
                        // The selection of an inactive panel is greyed out.
                        ui.scope(|ui| {
                            let visuals = ui.visuals_mut();
                            visuals.selection.bg_fill = desaturate(visuals.selection.bg_fill);
                            visuals.selection.stroke.color =
                                desaturate(visuals.selection.stroke.color);
                            table(ui, panel, side, height, actions);
                        });
                    }
                });
            });
            footer_row(ui, panel, side, actions);
        });
    let pressed = ui.input(|i| i.pointer.primary_pressed());
    if !active && pressed && ui.rect_contains_pointer(frame.response.rect) {
        actions.push(Action::Activate(side));
    }
}

/// Loading strip, listing error with Retry, truncation warning.
fn banners(ui: &mut egui::Ui, panel: &PanelState, side: Side, actions: &mut Vec<Action>) {
    if panel.loading {
        ui.horizontal(|ui| {
            ui.spinner();
            ui.weak("Loading…");
        });
    }
    if let Some(error) = &panel.error {
        ui.horizontal(|ui| {
            ui.colored_label(ui.visuals().error_fg_color, error);
            if ui.button("Retry").clicked() {
                actions.push(Action::Retry(side));
            }
        });
    }
    if panel.truncated {
        ui.colored_label(
            ui.visuals().warn_fg_color,
            format!(
                "The listing is incomplete: only the first {} entries are shown.",
                thousands(MAX_ENTRIES)
            ),
        );
    }
}

/// The virtualized listing.
fn table(
    ui: &mut egui::Ui,
    panel: &PanelState,
    side: Side,
    height: f32,
    actions: &mut Vec<Action>,
) {
    ui.style_mut().interaction.selectable_labels = false;
    let modifiers = ui.input(|i| i.modifiers);
    let spacing = ui.spacing().item_spacing.y;
    let ctx = ui.ctx().clone();
    // Keyboard motion since the last frame: bring the cursor into view.
    // Clicks never scroll, since they do not count as moves.
    let mut builder = TableBuilder::new(ui).id_salt(("panel", side.index()));
    if seen_moves(&ctx, side) != Some(panel.cursor_moves) {
        ctx.data_mut(|d| d.insert_temp(seen_moves_id(side), panel.cursor_moves));
        let at = panel
            .cursor
            .as_ref()
            .and_then(|cursor| panel.entries.iter().position(|e| e.is(cursor)));
        if let (Some(at), true) = (at, panel.cursor_moves > 0) {
            builder = builder.scroll_to_row(at, None);
        }
    }
    let output = builder
        .striped(true)
        // Cell content is centred on the row's height.
        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
        .sense(egui::Sense::click())
        .auto_shrink([false, false])
        // The header row sits above the scrolled body.
        .max_scroll_height((height - ROW_HEIGHT - spacing).max(ROW_HEIGHT))
        .column(Column::remainder().clip(true))
        .column(Column::exact(90.0))
        .column(Column::exact(130.0))
        .header(ROW_HEIGHT, |mut header| {
            header.col(|ui| {
                ui.strong("Name");
            });
            header.col(|ui| {
                ui.strong("Size");
            });
            header.col(|ui| {
                ui.strong("Modified");
            });
        })
        .body(|body| {
            body.rows(ROW_HEIGHT, panel.entries.len(), |mut row| {
                let entry = &panel.entries[row.index()];
                let name = entry.name();
                let key = entry.key();
                row.set_selected(panel.selection.contains(&key));
                let icon = if entry.is_folder() { "🗀" } else { "🗋" };
                row.col(|ui| {
                    ui.add(egui::Label::new(format!("{icon} {}", display::text(name))).truncate());
                });
                let (size_text, modified_text) = match entry {
                    Entry::Folder { .. } => (String::new(), String::new()),
                    Entry::Object { size, modified, .. } => (
                        human_size(*size),
                        modified.format("%Y-%m-%d %H:%M").to_string(),
                    ),
                };
                row.col(|ui| {
                    ui.label(size_text);
                });
                row.col(|ui| {
                    ui.label(modified_text);
                });
                let response = row.response();
                if response.double_clicked() {
                    // Only folders open; an object may share a folder's name.
                    if entry.is_folder() {
                        actions.push(Action::Open(side, name.to_owned()));
                    }
                } else if response.clicked() {
                    let op = if modifiers.command || modifiers.ctrl {
                        SelectOp::Toggle(key)
                    } else if modifiers.shift {
                        SelectOp::Range(key)
                    } else {
                        SelectOp::Only(key)
                    };
                    actions.push(Action::Select(side, op));
                }
            });
        });
    let body = output.inner_rect;
    let rows = (body.height() / (ROW_HEIGHT + spacing)).floor();
    // Truncation is the point: whole rows only, at least one.
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let rows = rows.max(1.0) as usize;
    ctx.data_mut(|d| d.insert_temp(page_rows_id(side), rows));
}

/// Selection count, shown count and Load more.
fn footer_row(ui: &mut egui::Ui, panel: &PanelState, side: Side, actions: &mut Vec<Action>) {
    ui.horizontal(|ui| {
        ui.label(format!(
            "{} selected · {} shown",
            thousands(panel.selection.len()),
            thousands(panel.entries.len())
        ));
        if panel.next.is_some() && !panel.truncated {
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let more = ui.add_enabled(!panel.loading, egui::Button::new("Load more"));
                if more.clicked() {
                    actions.push(Action::LoadMore(side));
                }
            });
        }
    });
}

/// `50000` as `50,000`.
pub(super) fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A byte count with a binary unit and one decimal: `1.5 KiB`.
pub(super) fn human_size(size: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if size < 1024 {
        return format!("{size} B");
    }
    // Tenths of the unit, in integers: no float rounding surprises.
    let mut tenths = u128::from(size) * 10;
    let mut unit = 0;
    while tenths >= 1024 * 10 && unit < UNITS.len() - 1 {
        tenths /= 1024;
        unit += 1;
    }
    format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
}
