//! Encryption context: sorted string pairs, authenticated but not secret.

use std::collections::BTreeMap;

use super::header::FormatError;
pub use crate::codec::CallerContext;

/// Reserved key marking the profile version.
pub const KEY_FORMAT: &str = "osc:fmt";
/// Reserved key binding the ciphertext to its object key.
pub const KEY_OBJECT: &str = "osc:key";
/// Only supported value of [`KEY_FORMAT`].
pub const FORMAT_V1: &str = "1";

/// Largest serialized context a writer accepts. Chosen so that the header
/// (92 fixed bytes, up to a few KiB of EDKs) stays within the default reader
/// limit `DecodeLimits::default().max_header_len` (16 KiB).
pub const MAX_CONTEXT_LEN: usize = 12 * 1024;

/// The encryption context of one message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptionContext(pub(crate) BTreeMap<String, String>);

impl EncryptionContext {
    /// Context holding only the reserved entries.
    #[must_use]
    pub fn new_reserved(object_key: Option<&str>) -> Self {
        let mut map = BTreeMap::new();
        map.insert(KEY_FORMAT.to_owned(), FORMAT_V1.to_owned());
        if let Some(key) = object_key {
            map.insert(KEY_OBJECT.to_owned(), key.to_owned());
        }
        Self(map)
    }

    /// Adds caller pairs. Fails on a reserved prefix or when the serialized
    /// context would exceed [`MAX_CONTEXT_LEN`] (so the default reader accepts the
    /// resulting header) or the 16-bit length fields of the format. Writers
    /// must call this (even with an empty map) before serializing.
    ///
    /// # Errors
    /// [`FormatError::ReservedContextKey`] or [`FormatError::Malformed`].
    pub fn with_caller(mut self, caller: &CallerContext) -> Result<Self, FormatError> {
        for (key, value) in caller {
            if is_reserved(key) {
                return Err(FormatError::ReservedContextKey(key.clone()));
            }
            self.0.insert(key.clone(), value.clone());
        }
        self.check_size()?;
        Ok(self)
    }

    fn check_size(&self) -> Result<(), FormatError> {
        let fits = |n: usize| u16::try_from(n).is_ok();
        let mut total = 2usize;
        for (key, value) in &self.0 {
            if !fits(key.len()) || !fits(value.len()) {
                return Err(FormatError::Malformed("context entry too long"));
            }
            total += 4 + key.len() + value.len();
        }
        if !fits(self.0.len()) || total > MAX_CONTEXT_LEN {
            return Err(FormatError::Malformed("context too large"));
        }
        Ok(())
    }

    /// Value for `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Iterates pairs in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Number of pairs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// True when there are no pairs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// ESDK serialization: `count u16`, then per pair sorted by key bytes
    /// `klen u16 | key | vlen u16 | value`. An empty context is zero bytes.
    ///
    /// # Errors
    /// [`FormatError::Malformed`] when the pair count or a key or value length
    /// does not fit its 16-bit field (never silently truncated).
    pub fn serialize(&self) -> Result<Vec<u8>, FormatError> {
        if self.0.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        put_len(&mut out, self.0.len())?;
        for (key, value) in &self.0 {
            put_len(&mut out, key.len())?;
            out.extend_from_slice(key.as_bytes());
            put_len(&mut out, value.len())?;
            out.extend_from_slice(value.as_bytes());
        }
        Ok(out)
    }

    /// Parses exactly `region` (the `aad_len` bytes of a header). Pairs must be
    /// strictly sorted by key bytes so serialization reproduces the input.
    pub(crate) fn deserialize(region: &[u8]) -> Result<Self, FormatError> {
        if region.is_empty() {
            return Ok(Self(BTreeMap::new()));
        }
        let mut r = Reader(region);
        let count = r.u16()?;
        if count == 0 {
            return Err(FormatError::Malformed("empty context with count prefix"));
        }
        let mut map = BTreeMap::new();
        for _ in 0..count {
            let key = r.string()?;
            let value = r.string()?;
            if map.last_key_value().is_some_and(|(prev, _)| *prev >= key) {
                return Err(FormatError::Malformed(
                    "context keys not sorted or duplicated",
                ));
            }
            map.insert(key, value);
        }
        if !r.0.is_empty() {
            return Err(FormatError::Malformed("trailing bytes in context"));
        }
        Ok(Self(map))
    }
}

/// Whether `key` is in a reserved namespace. ASCII case-insensitive, so
/// `OSC:key` cannot pose as (or be read as) a library entry.
pub(crate) fn is_reserved(key: &str) -> bool {
    let has_prefix = |prefix: &str| {
        key.as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
    };
    has_prefix("osc:") || has_prefix("aws-crypto-")
}

/// Appends a `u16` length.
///
/// # Errors
/// [`FormatError::Malformed`] when `len` does not fit; nothing is appended.
pub(crate) fn put_len(out: &mut Vec<u8>, len: usize) -> Result<(), FormatError> {
    let len = u16::try_from(len)
        .map_err(|_| FormatError::Malformed("field length does not fit 16 bits"))?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    const fn take(&mut self, n: usize) -> Result<&'a [u8], FormatError> {
        if self.0.len() < n {
            return Err(FormatError::Malformed("context field overruns the context"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn u16(&mut self) -> Result<u16, FormatError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn string(&mut self) -> Result<String, FormatError> {
        let len = usize::from(self.u16()?);
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| FormatError::Malformed("context is not UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_is_byte_exact_and_sorted() {
        let caller: CallerContext = [("b", "2"), ("a", "1")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        let mut ctx = EncryptionContext(BTreeMap::new());
        for (k, v) in &caller {
            ctx.0.insert(k.clone(), v.clone());
        }
        let bytes = ctx.serialize().expect("serialize");
        assert_eq!(
            bytes,
            [0, 2, 0, 1, b'a', 0, 1, b'1', 0, 1, b'b', 0, 1, b'2']
        );
        assert_eq!(EncryptionContext::deserialize(&bytes), Ok(ctx));
    }

    #[test]
    fn empty_context_is_zero_bytes() {
        let ctx = EncryptionContext(BTreeMap::new());
        assert_eq!(ctx.serialize(), Ok(Vec::<u8>::new()));
        assert_eq!(EncryptionContext::deserialize(&[]), Ok(ctx.clone()));
        assert!(ctx.is_empty());
        assert_eq!(ctx.len(), 0);
        let one = EncryptionContext(BTreeMap::from([("a".to_owned(), "b".to_owned())]));
        assert!(!one.is_empty() && one.len() == 1);
    }

    #[test]
    fn reserved_context_with_caller_pairs() {
        let caller: CallerContext = [("tenant".to_owned(), "acme".to_owned())].into();
        let ctx = EncryptionContext::new_reserved(Some("dir/f"))
            .with_caller(&caller)
            .expect("valid");
        assert_eq!(ctx.get("osc:fmt"), Some("1"));
        assert_eq!(ctx.get("osc:key"), Some("dir/f"));
        assert_eq!(ctx.get("tenant"), Some("acme"));
        assert_eq!(ctx.len(), 3);
        assert_eq!(EncryptionContext::new_reserved(None).get("osc:key"), None);
    }

    #[test]
    fn reserved_prefixes_are_rejected() {
        for bad in [
            "osc:x",
            "aws-crypto-public-key",
            "osc:fmt",
            "OSC:key",
            "Osc:fmt",
            "oSc:x",
            "Aws-Crypto-x",
            "AWS-CRYPTO-public-key",
        ] {
            let caller: CallerContext = [(bad.to_owned(), "v".to_owned())].into();
            assert_eq!(
                EncryptionContext::new_reserved(None).with_caller(&caller),
                Err(FormatError::ReservedContextKey(bad.to_owned()))
            );
        }
    }

    #[test]
    fn reserved_check_is_ascii_case_insensitive_and_prefix_only() {
        for reserved in ["osc:", "OSC:a", "oSC:", "aws-crypto-", "AWS-Crypto-z"] {
            assert!(is_reserved(reserved), "{reserved}");
        }
        for free in [
            "",
            "os",
            "osc",
            "osc-x",
            "xosc:",
            "aws-crypto",
            "tenant",
            "ÖSC:x",
        ] {
            assert!(!is_reserved(free), "{free}");
        }
    }

    #[test]
    fn oversized_context_is_rejected() {
        let caller: CallerContext = [("k".to_owned(), "v".repeat(70_000))].into();
        assert!(matches!(
            EncryptionContext::new_reserved(None).with_caller(&caller),
            Err(FormatError::Malformed(_))
        ));
    }

    #[test]
    fn context_cap_is_exact() {
        // reserved "osc:fmt" pair is 12 bytes, count prefix 2, "k" pair 4 + 1 + v.
        let at_cap = MAX_CONTEXT_LEN - 19;
        let ok: CallerContext = [("k".to_owned(), "v".repeat(at_cap))].into();
        let ctx = EncryptionContext::new_reserved(None)
            .with_caller(&ok)
            .expect("at cap");
        assert_eq!(ctx.serialize().expect("serialize").len(), MAX_CONTEXT_LEN);
        let over: CallerContext = [("k".to_owned(), "v".repeat(at_cap + 1))].into();
        assert!(matches!(
            EncryptionContext::new_reserved(None).with_caller(&over),
            Err(FormatError::Malformed(_))
        ));
    }

    #[test]
    fn serialize_never_truncates_a_length() {
        // `new_reserved` skips `with_caller`'s validation: the serializer must
        // refuse instead of writing a saturated length.
        let ctx = EncryptionContext::new_reserved(Some(&"k".repeat(70_000)));
        assert!(matches!(ctx.serialize(), Err(FormatError::Malformed(_))));
        let mut out = vec![1];
        assert!(put_len(&mut out, 65_536).is_err());
        assert_eq!(out, [1]);
        put_len(&mut out, 65_535).expect("fits");
        assert_eq!(out, [1, 0xff, 0xff]);
    }

    #[test]
    fn deserialize_rejects_bad_input() {
        // unsorted
        let unsorted = [0, 2, 0, 1, b'b', 0, 0, 0, 1, b'a', 0, 0];
        assert!(matches!(
            EncryptionContext::deserialize(&unsorted),
            Err(FormatError::Malformed(_))
        ));
        // a pair that overruns the region
        assert_eq!(
            EncryptionContext::deserialize(&[0, 1, 0, 5, b'a']),
            Err(FormatError::Malformed("context field overruns the context"))
        );
        // trailing bytes
        assert!(matches!(
            EncryptionContext::deserialize(&[0, 1, 0, 1, b'a', 0, 0, 9]),
            Err(FormatError::Malformed(_))
        ));
        // invalid UTF-8
        assert!(matches!(
            EncryptionContext::deserialize(&[0, 1, 0, 1, 0xff, 0, 0]),
            Err(FormatError::Malformed(_))
        ));
        // duplicate key: the last one must not win
        let duplicate = [0, 2, 0, 1, b'a', 0, 0, 0, 1, b'a', 0, 1, b'x'];
        assert!(matches!(
            EncryptionContext::deserialize(&duplicate),
            Err(FormatError::Malformed(_))
        ));
        // a count prefix with no pairs
        assert!(matches!(
            EncryptionContext::deserialize(&[0, 0]),
            Err(FormatError::Malformed(_))
        ));
    }

    /// The count prefix and the length-prefixed pairs, in the given order.
    fn encode(pairs: &[(String, String)]) -> Vec<u8> {
        let len = |s: &str| u16::try_from(s.len()).expect("short").to_be_bytes();
        let mut out = u16::try_from(pairs.len())
            .expect("few")
            .to_be_bytes()
            .to_vec();
        for (k, v) in pairs {
            out.extend_from_slice(&len(k));
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(&len(v));
            out.extend_from_slice(v.as_bytes());
        }
        out
    }

    proptest::proptest! {
        /// The header tag covers the raw bytes while the unwrap AAD is the
        /// re-serialised context, so parsing must accept exactly the canonical
        /// encoding. The tiny alphabet makes duplicates and disorder common.
        #[test]
        fn deserialize_accepts_exactly_the_canonical_encoding(
            pairs in proptest::collection::vec(("[ab]{0,2}", "[xy]{0,2}"), 1..5),
        ) {
            let bytes = encode(&pairs);
            let strictly_increasing = pairs.windows(2).all(|w| w[0].0.as_bytes() < w[1].0.as_bytes());
            let parsed = EncryptionContext::deserialize(&bytes);
            proptest::prop_assert_eq!(parsed.is_ok(), strictly_increasing);
            if let Ok(ctx) = parsed {
                proptest::prop_assert_eq!(ctx.serialize().expect("serialize"), bytes);
            }
        }
    }
}
