//! Escaping of text that comes from a provider or a file system.

/// Whether `c` can forge a log line or drive a terminal: control characters
/// (C0, DEL, C1) and the bidirectional controls.
pub const fn is_unsafe_char(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{061C}'
                | '\u{200E}'
                | '\u{200F}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
        )
}

/// Escapes every [`is_unsafe_char`] character as `\u{..}`, then cuts the
/// result at a char boundary at or below `max_bytes` and appends
/// `...[+N bytes]` for the `N` bytes dropped.
///
/// Nothing is kept past the cap: the dropped bytes are only counted, so a huge
/// input costs no more memory than `max_bytes`.
pub fn escape(text: &str, max_bytes: usize) -> String {
    escape_display(&text, max_bytes)
}

/// [`escape`] for the text a value formats to, without building that text
/// first.
pub fn escape_display(value: &dyn std::fmt::Display, max_bytes: usize) -> String {
    use std::fmt::Write;
    let mut capped = Capped {
        out: String::new(),
        max: max_bytes,
        total: 0,
        full: false,
    };
    let _ = write!(capped, "{value}");
    let Capped { mut out, total, .. } = capped;
    if total > max_bytes {
        let _ = write!(out, "...[+{} bytes]", total - out.len());
    }
    out
}

/// A `fmt::Write` that escapes what it is given and keeps the first `max`
/// bytes of the result, counting the rest in `total`.
struct Capped {
    out: String,
    max: usize,
    total: usize,
    full: bool,
}

impl std::fmt::Write for Capped {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let mut piece = String::new();
        let mut buf = [0; 4];
        for c in s.chars() {
            let escaped: &str = if is_unsafe_char(c) {
                piece.clear();
                let _ = write!(piece, "\\u{{{:x}}}", u32::from(c));
                &piece
            } else {
                c.encode_utf8(&mut buf)
            };
            self.total += escaped.len();
            if !self.full {
                // Escapes are ASCII, so a cut inside one is a valid boundary.
                let end = escaped.floor_char_boundary(self.max - self.out.len());
                self.out.push_str(&escaped[..end]);
                self.full = end < escaped.len() || self.out.len() == self.max;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsafe_characters_are_escaped() {
        assert_eq!(escape("plain ünï/ok", usize::MAX), "plain ünï/ok");
        assert_eq!(
            escape("a\x1b[2J\n\u{202E}b\u{85}", usize::MAX),
            "a\\u{1b}[2J\\u{a}\\u{202e}b\\u{85}"
        );
    }

    #[test]
    fn the_cap_cuts_at_a_char_boundary_and_counts_the_rest() {
        assert_eq!(escape("abcdef", 6), "abcdef");
        assert_eq!(escape("abcdef", 4), "abcd...[+2 bytes]");
        // 'ü' is two bytes: a cut inside it moves back to the boundary.
        assert_eq!(escape("aüb", 2), "a...[+3 bytes]");
        // The cap applies to the escaped text.
        assert_eq!(escape("\n\n", 4), "\\u{a...[+6 bytes]");
    }

    #[test]
    fn a_huge_input_is_escaped_within_the_cap() {
        // 6 bytes per escaped char: 3_000_000 bytes of escapes in all.
        let text = "\u{1b}".repeat(500_000);
        let out = escape(&text, 4096);
        let suffix = "...[+2995904 bytes]";
        assert_eq!(out.len(), 4096 + suffix.len());
        assert!(out.starts_with("\\u{1b}\\u{1b}"));
        assert!(out.ends_with(suffix), "{}", &out[4096..]);
        assert!(out.capacity() < 100_000);
        // A plain tail is counted, not copied.
        let out = escape(&"x".repeat(10_000), 4);
        assert_eq!(out, "xxxx...[+9996 bytes]");
        // A value formatted in pieces gives the same result.
        let out = escape_display(&format_args!("{text}{text}"), 4096);
        assert!(out.ends_with("...[+5995904 bytes]"), "{}", &out[4096..]);
    }
}
