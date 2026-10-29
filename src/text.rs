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
pub fn escape(text: &str, max_bytes: usize) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if is_unsafe_char(c) {
            let _ = write!(out, "\\u{{{:x}}}", u32::from(c));
        } else {
            out.push(c);
        }
    }
    if out.len() > max_bytes {
        let end = out.floor_char_boundary(max_bytes);
        let dropped = out.len() - end;
        out.truncate(end);
        let _ = write!(out, "...[+{dropped} bytes]");
    }
    out
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
}
