//! Display text: every string the GUI shows passes through here.

use super::model::DISPLAY_MAX;
use crate::{codec, text as plain};

/// A URL for display: credentials and query redacted, control characters escaped.
pub fn url(raw: &str) -> String {
    plain::escape(&codec::redact_url(raw), DISPLAY_MAX)
}

/// Arbitrary text for display: control characters escaped, length capped.
pub fn text(raw: &str) -> String {
    plain::escape(raw, DISPLAY_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_redacts() {
        let shown = url("https://u:pw@h/x?sig=1");
        assert!(!shown.contains("pw"), "{shown}");
        assert!(!shown.contains("sig"), "{shown}");
    }

    #[test]
    fn text_escapes_control_characters() {
        assert!(!text("a\u{1b}[31mb").contains('\u{1b}'));
    }
}
