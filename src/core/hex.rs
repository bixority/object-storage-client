//! Lower-case hex encoding and decoding.

use std::fmt::Write;

/// Encodes bytes as lower-case hex.
pub fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Decodes hex (either case). Returns `None` on odd length or a non-hex digit.
#[cfg(any(feature = "encryption", test))]
pub fn decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    // Preallocated so it never reallocates (callers may wrap the result in `Zeroizing`).
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        if let (Some(hi), Some(lo)) = (nibble(pair[0]), nibble(pair[1])) {
            out.push((hi << 4) | lo);
        } else {
            // The partial output may be key material: wipe before dropping.
            #[cfg(feature = "encryption")]
            zeroize::Zeroize::zeroize(&mut out);
            return None;
        }
    }
    Some(out)
}

#[cfg(any(feature = "encryption", test))]
const fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_rejects() {
        assert_eq!(encode(&[0x00, 0xab, 0xff]), "00abff");
        assert_eq!(decode("00ABff"), Some(vec![0x00, 0xab, 0xff]));
        assert_eq!(decode("abc"), None);
        assert_eq!(decode("zz"), None);
        assert_eq!(decode(""), Some(vec![]));
    }
}
