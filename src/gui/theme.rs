//! The bixority.com palette, mapped onto egui's visuals for both themes.

use eframe::egui::{self, Color32};

/// The colour tokens of one theme.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Palette {
    /// Window and panel background.
    pub(super) ground: Color32,
    /// Text.
    pub(super) ink: Color32,
    /// Secondary text.
    pub(super) quiet: Color32,
    /// Selection, active border, links.
    pub(super) accent: Color32,
    /// Widget outlines.
    pub(super) line: Color32,
    /// Deepest background: text fields.
    pub(super) shade: Color32,
}

/// The dark palette.
pub(super) const DARK: Palette = Palette {
    ground: Color32::from_rgb(0x00, 0x1b, 0x35),
    ink: Color32::from_rgb(0xee, 0xf2, 0xf6),
    quiet: Color32::from_rgb(0x8a, 0xa0, 0xb6),
    accent: Color32::from_rgb(0x00, 0xcc, 0xff),
    line: Color32::from_rgb(0x00, 0x97, 0xc2),
    shade: Color32::from_rgb(0x00, 0x09, 0x13),
};

/// The light palette.
pub(super) const LIGHT: Palette = Palette {
    ground: Color32::from_rgb(0xe3, 0xe6, 0xe9),
    ink: Color32::from_rgb(0x00, 0x1b, 0x35),
    quiet: Color32::from_rgb(0x4a, 0x56, 0x64),
    accent: Color32::from_rgb(0x00, 0x6b, 0x85),
    line: Color32::from_rgb(0x00, 0x6b, 0x85),
    shade: Color32::from_rgb(0x0b, 0x2a, 0x48),
};

/// `color` with the given alpha, the colour channels kept.
pub(super) fn with_alpha(color: Color32, alpha: u8) -> Color32 {
    Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha)
}

impl Palette {
    /// The egui visuals of this palette. `dark` picks the base they start from.
    pub(super) fn visuals(&self, dark: bool) -> egui::Visuals {
        let mut v = if dark {
            egui::Visuals::dark()
        } else {
            egui::Visuals::light()
        };
        v.window_fill = self.ground;
        v.panel_fill = self.ground;
        v.window_stroke = egui::Stroke::new(1.0, with_alpha(self.line, 140));
        v.weak_text_color = Some(self.quiet);
        v.faint_bg_color = with_alpha(self.ink, 10);
        if dark {
            v.extreme_bg_color = self.shade;
        }
        v.selection.bg_fill = with_alpha(self.accent, 56);
        v.selection.stroke = egui::Stroke::new(1.0, self.accent);
        v.hyperlink_color = self.accent;
        let w = &mut v.widgets;
        w.noninteractive.bg_fill = self.ground;
        w.noninteractive.weak_bg_fill = self.ground;
        w.noninteractive.bg_stroke = egui::Stroke::new(1.0, with_alpha(self.line, 110));
        w.noninteractive.fg_stroke = egui::Stroke::new(1.0, self.ink);
        for (state, fill) in [
            (&mut w.inactive, with_alpha(self.ink, 14)),
            (&mut w.hovered, with_alpha(self.accent, 44)),
            (&mut w.active, with_alpha(self.accent, 80)),
            (&mut w.open, with_alpha(self.accent, 28)),
        ] {
            state.bg_fill = fill;
            state.weak_bg_fill = fill;
            state.bg_stroke = egui::Stroke::new(1.0, self.line);
            state.fg_stroke = egui::Stroke::new(1.5, self.ink);
        }
        w.hovered.bg_stroke = egui::Stroke::new(1.0, self.accent);
        w.hovered.fg_stroke = egui::Stroke::new(1.5, self.accent);
        v
    }
}

/// Installs the palettes for both themes; egui picks the one of the system.
pub(super) fn install(ctx: &egui::Context) {
    ctx.set_visuals_of(egui::Theme::Dark, DARK.visuals(true));
    ctx.set_visuals_of(egui::Theme::Light, LIGHT.visuals(false));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_the_site_colours() {
        assert_eq!(DARK.accent, Color32::from_rgb(0, 204, 255));
        assert_eq!(DARK.ground, Color32::from_rgb(0, 27, 53));
        assert_eq!(DARK.ink, Color32::from_rgb(238, 242, 246));
        assert_eq!(DARK.quiet, Color32::from_rgb(138, 160, 182));
        assert_eq!(DARK.line, Color32::from_rgb(0, 151, 194));
        assert_eq!(DARK.shade, Color32::from_rgb(0, 9, 19));
        assert_eq!(LIGHT.ground, Color32::from_rgb(227, 230, 233));
        assert_eq!(LIGHT.ink, Color32::from_rgb(0, 27, 53));
        assert_eq!(LIGHT.quiet, Color32::from_rgb(74, 86, 100));
        assert_eq!(LIGHT.accent, Color32::from_rgb(0, 107, 133));
        assert_eq!(LIGHT.line, Color32::from_rgb(0, 107, 133));
        assert_eq!(LIGHT.shade, Color32::from_rgb(11, 42, 72));
    }

    #[test]
    fn palettes_map_onto_visuals() {
        for (palette, dark) in [(DARK, true), (LIGHT, false)] {
            let v = palette.visuals(dark);
            assert_eq!(v.dark_mode, dark);
            assert_eq!(v.window_fill, palette.ground);
            assert_eq!(v.panel_fill, palette.ground);
            assert_eq!(v.weak_text_color, Some(palette.quiet));
            assert_eq!(v.widgets.noninteractive.fg_stroke.color, palette.ink);
            assert_eq!(v.selection.stroke.color, palette.accent);
            assert_eq!(v.hyperlink_color, palette.accent);
            assert_eq!(v.widgets.inactive.bg_stroke.color, palette.line);
            assert_eq!(v.selection.bg_fill, with_alpha(palette.accent, 56));
            assert!(v.selection.bg_fill.a() < u8::MAX);
        }
        assert_eq!(DARK.visuals(true).extreme_bg_color, DARK.shade);
    }

    #[test]
    fn install_sets_both_themes() {
        let ctx = egui::Context::default();
        install(&ctx);
        let accent = |theme| ctx.style_of(theme).visuals.selection.stroke.color;
        assert_eq!(accent(egui::Theme::Dark), DARK.accent);
        assert_eq!(accent(egui::Theme::Light), LIGHT.accent);
    }
}
