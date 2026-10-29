//! ESDK message format v2 header (suite `0x0478`, framed) with decode limits.

use std::ops::Range;

use super::context::{EncryptionContext, FORMAT_V1, KEY_FORMAT, put_len};

const VERSION_2: u8 = 0x02;
const SUITE_0478: u16 = 0x0478;
const CONTENT_FRAMED: u8 = 0x02;
/// `content_type | frame_length | suite_data | auth_tag`
const TAIL_LEN: usize = 1 + 4 + 32 + 16;

/// Longest attacker-controlled string echoed into an error message.
const MAX_ECHO_CHARS: usize = 64;

/// `Debug` form of `s`, cut to [`MAX_ECHO_CHARS`] characters (with an
/// ellipsis) so hostile header fields cannot flood logs.
pub fn short_debug(s: &str) -> String {
    match s.char_indices().nth(MAX_ECHO_CHARS) {
        Some((end, _)) => format!("{:?}...", &s[..end]),
        None => format!("{s:?}"),
    }
}

/// Like [`short_debug`] but keeps the last [`MAX_ECHO_CHARS`] characters:
/// object keys and paths share prefixes and differ near the end.
pub fn short_debug_tail(s: &str) -> String {
    match s.char_indices().rev().nth(MAX_ECHO_CHARS - 1) {
        Some((start, _)) if start > 0 => format!("...{:?}", &s[start..]),
        _ => format!("{s:?}"),
    }
}

/// Header format errors. Never carries key material.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FormatError {
    /// Not an OSC/ESDK message (first byte is not `0x01` or `0x02`).
    #[error("not an encrypted object")]
    NotOsc,
    /// ESDK message version other than 2.
    #[error("unsupported message version")]
    UnsupportedVersion,
    /// Algorithm suite other than `0x0478`.
    #[error("unsupported algorithm suite 0x{0:04x}")]
    UnsupportedSuite(u16),
    /// Content type other than framed.
    #[error("content is not framed")]
    NotFramed,
    /// More EDKs than the configured limit.
    #[error("too many encrypted data keys: {0}")]
    TooManyEdks(u16),
    /// Frame length outside the configured range.
    #[error("frame length {0} out of range")]
    FrameLengthOutOfRange(u32),
    /// Header larger than the configured limit.
    #[error("header too long")]
    HeaderTooLong,
    /// Context lacks the `osc:fmt` marker.
    #[error("missing osc:fmt marker")]
    MissingFormatMarker,
    /// `osc:fmt` has an unknown value.
    #[error("unknown osc format {}", short_debug(.0))]
    UnknownFormat(String),
    /// Caller context uses a reserved key.
    #[error("reserved context key {0:?}")]
    ReservedContextKey(String),
    /// Input ended inside a field.
    #[error("truncated header")]
    Truncated,
    /// Structurally invalid header.
    #[error("malformed header: {0}")]
    Malformed(&'static str),
}

// Manual so that untrusted header strings are truncated in `{:?}` too.
impl std::fmt::Debug for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotOsc => f.write_str("NotOsc"),
            Self::UnsupportedVersion => f.write_str("UnsupportedVersion"),
            Self::UnsupportedSuite(v) => f.debug_tuple("UnsupportedSuite").field(v).finish(),
            Self::NotFramed => f.write_str("NotFramed"),
            Self::TooManyEdks(n) => f.debug_tuple("TooManyEdks").field(n).finish(),
            Self::FrameLengthOutOfRange(n) => {
                f.debug_tuple("FrameLengthOutOfRange").field(n).finish()
            }
            Self::HeaderTooLong => f.write_str("HeaderTooLong"),
            Self::MissingFormatMarker => f.write_str("MissingFormatMarker"),
            Self::UnknownFormat(v) => f
                .debug_tuple("UnknownFormat")
                .field(&format_args!("{}", short_debug(v)))
                .finish(),
            Self::ReservedContextKey(k) => f
                .debug_tuple("ReservedContextKey")
                .field(&format_args!("{}", short_debug(k)))
                .finish(),
            Self::Truncated => f.write_str("Truncated"),
            Self::Malformed(m) => f.debug_tuple("Malformed").field(m).finish(),
        }
    }
}

/// Limits applied while parsing untrusted headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum number of EDKs.
    pub max_edks: u16,
    /// Minimum accepted frame length.
    pub min_frame_length: u32,
    /// Maximum accepted frame length.
    pub max_frame_length: u32,
    /// Maximum header size in bytes, including the auth tag.
    pub max_header_len: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_edks: 8,
            min_frame_length: 4096,
            max_frame_length: 4 * 1024 * 1024,
            max_header_len: 16 * 1024,
        }
    }
}

impl DecodeLimits {
    /// Checks a frame length about to be written: not zero, within the limits.
    pub(crate) fn check_frame_length(&self, n: u32) -> Result<(), FormatError> {
        if (self.min_frame_length.max(1)..=self.max_frame_length).contains(&n) {
            Ok(())
        } else {
            Err(FormatError::FrameLengthOutOfRange(n))
        }
    }
}

/// One wrapped data key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedDataKey {
    /// Key namespace (`osc-file`).
    pub provider_id: String,
    /// Key name plus wrap parameters.
    pub provider_info: Vec<u8>,
    /// Wrapped DEK and tag.
    pub ciphertext: Vec<u8>,
}

/// A parsed message header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Random per-message ID (HKDF salt).
    pub message_id: [u8; 32],
    /// Encryption context.
    pub context: EncryptionContext,
    /// Wrapped data keys.
    pub edks: Vec<EncryptedDataKey>,
    /// Regular frame content length.
    pub frame_length: u32,
    /// Key commitment (`suite_data`).
    pub commitment: [u8; 32],
    /// Header authentication tag.
    pub auth_tag: [u8; 16],
}

/// Result of [`HeaderCodec::parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderParse {
    /// The buffer is a valid prefix; supply more bytes.
    NeedMore,
    /// A complete header.
    Done {
        /// The header.
        header: Header,
        /// Total header length including the auth tag.
        header_len: usize,
        /// Bytes covered by the header tag (`0..header_len - 16`).
        authenticated_bytes: Range<usize>,
    },
}

/// Header serializer and incremental parser.
pub struct HeaderCodec;

enum Stop {
    /// More bytes are needed; the payload is the smallest total buffer length
    /// that lets the parser get past the field it stopped at.
    More(usize),
    Fail(FormatError),
}

impl From<FormatError> for Stop {
    fn from(e: FormatError) -> Self {
        Self::Fail(e)
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
    max: usize,
}

impl<'a> Cursor<'a> {
    /// Takes `n` bytes. The limit is checked before availability so an
    /// oversized declared field fails without waiting for its bytes.
    fn take(&mut self, n: usize) -> Result<&'a [u8], Stop> {
        let end = self.pos.saturating_add(n);
        if end > self.max {
            return Err(FormatError::HeaderTooLong.into());
        }
        let slice = self.buf.get(self.pos..end).ok_or(Stop::More(end))?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, Stop> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, Stop> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, Stop> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Stop> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn vec16(&mut self) -> Result<Vec<u8>, Stop> {
        let len = usize::from(self.u16()?);
        Ok(self.take(len)?.to_vec())
    }
}

impl HeaderCodec {
    /// Parses a header from the start of `buf`, enforcing `limits` before
    /// allocating for each field.
    ///
    /// An empty buffer is `NeedMore`; the caller decides what a short object
    /// means at end of input.
    ///
    /// # Errors
    /// A [`FormatError`] as soon as the bytes seen so far are invalid or
    /// exceed a limit.
    pub fn parse(buf: &[u8], limits: &DecodeLimits) -> Result<HeaderParse, FormatError> {
        Self::parse_hinted(buf, limits).map(|(parse, _)| parse)
    }

    /// Like [`Self::parse`], also returning the smallest buffer length at
    /// which parsing can make further progress when the result is `NeedMore`
    /// (`0` otherwise). Parsing is sequential and depends only on the bytes
    /// before the point it stopped, so a caller may skip re-parsing until the
    /// buffer reaches that length and still fail at the same moment.
    pub(crate) fn parse_hinted(
        buf: &[u8],
        limits: &DecodeLimits,
    ) -> Result<(HeaderParse, usize), FormatError> {
        match Self::parse_inner(buf, limits) {
            Ok(done) => Ok((done, 0)),
            Err(Stop::More(need)) => Ok((HeaderParse::NeedMore, need)),
            Err(Stop::Fail(e)) => Err(e),
        }
    }

    fn parse_inner(buf: &[u8], limits: &DecodeLimits) -> Result<HeaderParse, Stop> {
        // Classification (not OSC, old version) needs three bytes; with fewer
        // every reader sees the same thing: an object too short to judge.
        if buf.len() < 3 {
            return Err(Stop::More(3));
        }
        let mut c = Cursor {
            buf,
            pos: 0,
            max: limits.max_header_len,
        };
        match c.u8()? {
            VERSION_2 => {}
            0x01 => return Err(FormatError::UnsupportedVersion.into()),
            _ => return Err(FormatError::NotOsc.into()),
        }
        let suite = c.u16()?;
        if suite != SUITE_0478 {
            return Err(FormatError::UnsupportedSuite(suite).into());
        }
        let message_id = c.array::<32>()?;

        let aad_len = usize::from(c.u16()?);
        let context = EncryptionContext::deserialize(c.take(aad_len)?)?;
        match context.get(KEY_FORMAT) {
            None => return Err(FormatError::MissingFormatMarker.into()),
            Some(FORMAT_V1) => {}
            Some(other) => return Err(FormatError::UnknownFormat(other.to_owned()).into()),
        }

        let edk_count = c.u16()?;
        if edk_count > limits.max_edks {
            return Err(FormatError::TooManyEdks(edk_count).into());
        }
        if edk_count == 0 {
            return Err(FormatError::Malformed("no encrypted data keys").into());
        }
        let mut edks = Vec::with_capacity(usize::from(edk_count));
        for _ in 0..edk_count {
            let provider_id = String::from_utf8(c.vec16()?)
                .map_err(|_| FormatError::Malformed("provider id is not UTF-8"))?;
            let provider_info = c.vec16()?;
            let ciphertext = c.vec16()?;
            edks.push(EncryptedDataKey {
                provider_id,
                provider_info,
                ciphertext,
            });
        }

        // Fail on the declared total before waiting for the tail.
        if c.pos.saturating_add(TAIL_LEN) > c.max {
            return Err(FormatError::HeaderTooLong.into());
        }
        if c.u8()? != CONTENT_FRAMED {
            return Err(FormatError::NotFramed.into());
        }
        let frame_length = c.u32()?;
        if frame_length < limits.min_frame_length || frame_length > limits.max_frame_length {
            return Err(FormatError::FrameLengthOutOfRange(frame_length).into());
        }
        let commitment = c.array::<32>()?;
        let auth_tag = c.array::<16>()?;

        let header_len = c.pos;
        Ok(HeaderParse::Done {
            header: Header {
                message_id,
                context,
                edks,
                frame_length,
                commitment,
                auth_tag,
            },
            header_len,
            authenticated_bytes: 0..header_len - 16,
        })
    }

    /// Serializes everything the header tag covers (`version` through
    /// `suite_data`).
    ///
    /// # Errors
    /// [`FormatError::Malformed`] when the context, the number of EDKs or an
    /// EDK field does not fit its 16-bit length field, or when there is no
    /// EDK (a header the parser would reject). Lengths are never truncated.
    pub fn serialize_unauthenticated(h: &Header) -> Result<Vec<u8>, FormatError> {
        if h.edks.is_empty() {
            return Err(FormatError::Malformed("no encrypted data keys"));
        }
        let ctx = h.context.serialize()?;
        let mut out = Vec::with_capacity(128 + ctx.len());
        out.push(VERSION_2);
        out.extend_from_slice(&SUITE_0478.to_be_bytes());
        out.extend_from_slice(&h.message_id);
        put_len(&mut out, ctx.len())?;
        out.extend_from_slice(&ctx);
        put_len(&mut out, h.edks.len())?;
        for edk in &h.edks {
            put_len(&mut out, edk.provider_id.len())?;
            out.extend_from_slice(edk.provider_id.as_bytes());
            put_len(&mut out, edk.provider_info.len())?;
            out.extend_from_slice(&edk.provider_info);
            put_len(&mut out, edk.ciphertext.len())?;
            out.extend_from_slice(&edk.ciphertext);
        }
        out.push(CONTENT_FRAMED);
        out.extend_from_slice(&h.frame_length.to_be_bytes());
        out.extend_from_slice(&h.commitment);
        Ok(out)
    }

    /// Full header bytes: the unauthenticated part plus the auth tag.
    ///
    /// # Errors
    /// As [`Self::serialize_unauthenticated`].
    #[cfg(test)]
    pub fn serialize(h: &Header) -> Result<Vec<u8>, FormatError> {
        let mut out = Self::serialize_unauthenticated(h)?;
        out.extend_from_slice(&h.auth_tag);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex;
    use std::collections::BTreeMap;

    #[test]
    fn unknown_format_is_truncated_in_debug_too() {
        let d = format!("{:?}", FormatError::UnknownFormat("x".repeat(10_000)));
        assert!(d.len() < 300, "{}", d.len());
        assert!(d.starts_with("UnknownFormat"), "{d}");
        let d = format!("{:?}", FormatError::ReservedContextKey("k".repeat(10_000)));
        assert!(d.len() < 300, "{}", d.len());
    }

    #[test]
    fn debug_of_every_format_variant_names_it() {
        let all = [
            (FormatError::NotOsc, "NotOsc"),
            (FormatError::UnsupportedVersion, "UnsupportedVersion"),
            (FormatError::UnsupportedSuite(1), "UnsupportedSuite"),
            (FormatError::NotFramed, "NotFramed"),
            (FormatError::TooManyEdks(9), "TooManyEdks"),
            (
                FormatError::FrameLengthOutOfRange(3),
                "FrameLengthOutOfRange",
            ),
            (FormatError::HeaderTooLong, "HeaderTooLong"),
            (FormatError::MissingFormatMarker, "MissingFormatMarker"),
            (FormatError::UnknownFormat("v".into()), "UnknownFormat"),
            (
                FormatError::ReservedContextKey("k".into()),
                "ReservedContextKey",
            ),
            (FormatError::Truncated, "Truncated"),
            (FormatError::Malformed("why"), "Malformed"),
        ];
        for (e, name) in all {
            assert!(format!("{e:?}").starts_with(name), "{e:?} vs {name}");
        }
    }

    fn sample() -> Header {
        Header {
            message_id: [7; 32],
            context: EncryptionContext::new_reserved(Some("dir/file.bin")),
            edks: vec![EncryptedDataKey {
                provider_id: "osc-file".into(),
                provider_info: b"key-1".to_vec(),
                ciphertext: vec![9; 48],
            }],
            frame_length: 65536,
            commitment: [3; 32],
            auth_tag: [5; 16],
        }
    }

    fn parse_ok(bytes: &[u8]) -> (Header, usize, Range<usize>) {
        match HeaderCodec::parse(bytes, &DecodeLimits::default()) {
            Ok(HeaderParse::Done {
                header,
                header_len,
                authenticated_bytes,
            }) => (header, header_len, authenticated_bytes),
            other => panic!("expected Done, got {other:?}"),
        }
    }

    fn parse_err(bytes: &[u8]) -> FormatError {
        parse_err_with(bytes, &DecodeLimits::default())
    }

    fn parse_err_with(bytes: &[u8], limits: &DecodeLimits) -> FormatError {
        HeaderCodec::parse(bytes, limits).expect_err("expected an error")
    }

    fn ser(h: &Header) -> Vec<u8> {
        HeaderCodec::serialize(h).expect("serialize")
    }

    fn ctx_len(h: &Header) -> usize {
        h.context.serialize().expect("serialize").len()
    }

    #[test]
    fn roundtrip() {
        let h = sample();
        let bytes = ser(&h);
        let (parsed, len, auth) = parse_ok(&bytes);
        assert_eq!(parsed, h);
        assert_eq!(len, bytes.len());
        assert_eq!(auth, 0..bytes.len() - 16);
        assert_eq!(
            HeaderCodec::serialize_unauthenticated(&h),
            Ok(bytes[..bytes.len() - 16].to_vec())
        );
    }

    #[test]
    fn serializer_refuses_lengths_that_do_not_fit() {
        let too_big = 70_000;
        let mut h = sample();
        h.edks[0].provider_info = vec![0; too_big];
        assert!(matches!(
            HeaderCodec::serialize(&h),
            Err(FormatError::Malformed(_))
        ));
        let mut h = sample();
        h.edks[0].ciphertext = vec![0; too_big];
        assert!(HeaderCodec::serialize_unauthenticated(&h).is_err());
        let mut h = sample();
        h.edks[0].provider_id = "p".repeat(too_big);
        assert!(HeaderCodec::serialize(&h).is_err());
        let mut h = sample();
        h.edks = vec![h.edks[0].clone(); 65_536];
        assert!(HeaderCodec::serialize(&h).is_err());
        let mut h = sample();
        h.context = EncryptionContext::new_reserved(Some(&"k".repeat(too_big)));
        assert!(HeaderCodec::serialize(&h).is_err());
        let mut h = sample();
        h.edks.clear();
        assert!(HeaderCodec::serialize(&h).is_err());
    }

    #[test]
    fn unknown_format_message_is_truncated() {
        let long = FormatError::UnknownFormat("x".repeat(10_000));
        let text = long.to_string();
        assert!(text.len() < 200, "{}", text.len());
        assert!(text.ends_with("..."));
        let exact = FormatError::UnknownFormat("y".repeat(64)).to_string();
        assert!(!exact.ends_with("..."));
        // Multi-byte characters are cut on a character boundary.
        let wide = FormatError::UnknownFormat("é".repeat(100)).to_string();
        assert!(wide.ends_with("..."));
    }

    #[test]
    fn trailing_body_bytes_are_not_consumed() {
        let h = sample();
        let mut bytes = ser(&h);
        let hl = bytes.len();
        bytes.extend_from_slice(&[0xAA; 100]);
        let (parsed, len, _) = parse_ok(&bytes);
        assert_eq!((parsed, len), (h, hl));
    }

    #[test]
    fn every_strict_prefix_needs_more() {
        let bytes = ser(&sample());
        for n in 0..bytes.len() {
            assert_eq!(
                HeaderCodec::parse(&bytes[..n], &DecodeLimits::default()),
                Ok(HeaderParse::NeedMore),
                "prefix {n}"
            );
        }
    }

    #[test]
    fn not_osc_and_version() {
        let mut b = ser(&sample());
        b[0] = 0x55;
        assert_eq!(parse_err(&b), FormatError::NotOsc);
        b[0] = 0x01;
        assert_eq!(parse_err(&b), FormatError::UnsupportedVersion);
        // Classified from three bytes; fewer always need more.
        for bad in [0x55, 0x01] {
            let limits = DecodeLimits::default();
            assert_eq!(
                HeaderCodec::parse(&[bad], &limits),
                Ok(HeaderParse::NeedMore)
            );
            assert!(HeaderCodec::parse(&[bad, 4, 0x78], &limits).is_err());
        }
    }

    #[test]
    fn unsupported_suite() {
        let mut b = ser(&sample());
        b[1..3].copy_from_slice(&0x0378u16.to_be_bytes());
        assert_eq!(parse_err(&b), FormatError::UnsupportedSuite(0x0378));
        // rejected from the first three bytes alone
        assert_eq!(parse_err(&b[..3]), FormatError::UnsupportedSuite(0x0378));
    }

    #[test]
    fn not_framed() {
        let h = sample();
        let mut b = ser(&h);
        let off = b.len() - TAIL_LEN;
        b[off] = 0x01;
        assert_eq!(parse_err(&b), FormatError::NotFramed);
    }

    #[test]
    fn edk_count_limit_is_checked_before_allocation() {
        let mut h = sample();
        h.context = EncryptionContext::new_reserved(None);
        let mut b = ser(&h);
        let off = 37 + ctx_len(&h);
        b[off..off + 2].copy_from_slice(&0xFFFFu16.to_be_bytes());
        // Only 64 bytes available: must not be NeedMore.
        assert_eq!(parse_err(&b[..64]), FormatError::TooManyEdks(0xFFFF));
        assert_eq!(parse_err(&b), FormatError::TooManyEdks(0xFFFF));
        b[off..off + 2].copy_from_slice(&9u16.to_be_bytes());
        assert_eq!(parse_err(&b), FormatError::TooManyEdks(9));
        b[off..off + 2].copy_from_slice(&0u16.to_be_bytes());
        assert!(matches!(parse_err(&b), FormatError::Malformed(_)));
    }

    #[test]
    fn frame_length_range() {
        for bad in [0u32, 4095, 4 * 1024 * 1024 + 1, u32::MAX] {
            let mut h = sample();
            h.frame_length = bad;
            assert_eq!(parse_err(&ser(&h)), FormatError::FrameLengthOutOfRange(bad));
        }
        for ok in [4096u32, 4 * 1024 * 1024] {
            let mut h = sample();
            h.frame_length = ok;
            parse_ok(&ser(&h));
        }
    }

    #[test]
    fn check_frame_length_rejects_zero_and_out_of_range() {
        let limits = DecodeLimits {
            min_frame_length: 0,
            ..DecodeLimits::default()
        };
        for bad in [0u32, 4 * 1024 * 1024 + 1] {
            assert_eq!(
                limits.check_frame_length(bad),
                Err(FormatError::FrameLengthOutOfRange(bad))
            );
        }
        assert_eq!(limits.check_frame_length(1), Ok(()));
        assert_eq!(limits.check_frame_length(4 * 1024 * 1024), Ok(()));
    }

    #[test]
    fn header_too_long() {
        let b = ser(&sample());
        let limits = DecodeLimits {
            max_header_len: b.len() - 1,
            ..DecodeLimits::default()
        };
        assert_eq!(parse_err_with(&b, &limits), FormatError::HeaderTooLong);
        let exact = DecodeLimits {
            max_header_len: b.len(),
            ..DecodeLimits::default()
        };
        assert!(HeaderCodec::parse(&b, &exact).is_ok());

        // A huge declared aad_len fails without waiting for its bytes.
        let mut head = vec![VERSION_2];
        head.extend_from_slice(&SUITE_0478.to_be_bytes());
        head.extend_from_slice(&[0; 32]);
        head.extend_from_slice(&u16::MAX.to_be_bytes());
        assert_eq!(parse_err(&head), FormatError::HeaderTooLong);

        // A huge EDK field fails without waiting for its bytes.
        let h = sample();
        let mut b = ser(&h);
        let off = 37 + ctx_len(&h) + 2; // provider id length
        b[off..off + 2].copy_from_slice(&u16::MAX.to_be_bytes());
        assert_eq!(parse_err(&b[..off + 2]), FormatError::HeaderTooLong);
    }

    #[test]
    fn format_marker() {
        let mut h = sample();
        h.context.0.remove("osc:fmt");
        assert_eq!(parse_err(&ser(&h)), FormatError::MissingFormatMarker);
        h.context.0.insert("osc:fmt".into(), "2".into());
        assert_eq!(parse_err(&ser(&h)), FormatError::UnknownFormat("2".into()));
        h.context = EncryptionContext(BTreeMap::new());
        assert_eq!(parse_err(&ser(&h)), FormatError::MissingFormatMarker);
    }

    #[test]
    fn overrunning_and_unsorted_context_are_malformed() {
        let mut b = vec![VERSION_2];
        b.extend_from_slice(&SUITE_0478.to_be_bytes());
        b.extend_from_slice(&[0; 32]);
        // aad_len 5: count 1, key length 5 but only one byte left
        b.extend_from_slice(&5u16.to_be_bytes());
        b.extend_from_slice(&[0, 1, 0, 5, b'a']);
        assert_eq!(
            parse_err(&b),
            FormatError::Malformed("context field overruns the context")
        );

        let mut h = sample();
        h.context.0.insert("a".into(), "x".into());
        let mut b = ser(&h);
        // swap order of the two keys' first bytes to break sorting: "a" -> "z"
        let pos = b
            .windows(5)
            .position(|w| w == [0, 1, b'a', 0, 1])
            .expect("a");
        b[pos + 2] = b'z';
        assert!(matches!(parse_err(&b), FormatError::Malformed(_)));
    }

    #[test]
    fn non_utf8_provider_id_is_malformed() {
        let h = sample();
        let mut b = ser(&h);
        let off = 37 + ctx_len(&h) + 2 + 2;
        b[off] = 0xff;
        assert!(matches!(parse_err(&b), FormatError::Malformed(_)));
    }

    fn unhex(v: &serde_json::Value) -> Vec<u8> {
        hex::decode(v.as_str().expect("string")).expect("hex")
    }

    #[test]
    fn esdk_vectors_parse_and_reserialize() {
        for v in &crate::encryption::fixtures::ESDK.vectors {
            let (file, bytes, insp) = (&v.file, &v.bytes, &v.inspect);
            let (h, header_len, auth) = parse_ok(bytes);

            assert_eq!(
                header_len,
                usize::try_from(insp["header_length"].as_u64().expect("len")).expect("usize"),
                "{file}"
            );
            assert_eq!(auth, 0..header_len - 16, "{file}");
            assert_eq!(
                h.message_id.to_vec(),
                unhex(&insp["message_id_hex"]),
                "{file}"
            );
            assert_eq!(
                h.context.serialize().expect("serialize"),
                unhex(&insp["serialized_context_hex"]),
                "{file}"
            );
            let want_ctx = insp["encryption_context"].as_object().expect("ctx");
            assert_eq!(h.context.len(), want_ctx.len(), "{file}");
            for (k, val) in want_ctx {
                assert_eq!(h.context.get(k), val.as_str(), "{file}");
            }
            let want_edks = insp["edks"].as_array().expect("edks");
            assert_eq!(h.edks.len(), want_edks.len(), "{file}");
            for (got, want) in h.edks.iter().zip(want_edks) {
                assert_eq!(
                    got.provider_id,
                    want["provider_id"].as_str().expect("id"),
                    "{file}"
                );
                assert_eq!(
                    got.provider_info,
                    unhex(&want["provider_info_hex"]),
                    "{file}"
                );
                assert_eq!(got.ciphertext, unhex(&want["ciphertext_hex"]), "{file}");
            }
            assert_eq!(
                u64::from(h.frame_length),
                insp["frame_length"].as_u64().expect("fl"),
                "{file}"
            );
            assert_eq!(insp["content_type"].as_u64(), Some(2), "{file}");
            assert_eq!(
                h.commitment.to_vec(),
                unhex(&insp["suite_data_hex"]),
                "{file}"
            );
            assert_eq!(
                h.auth_tag.to_vec(),
                unhex(&insp["header_auth_tag_hex"]),
                "{file}"
            );

            assert_eq!(
                HeaderCodec::serialize_unauthenticated(&h).expect("serialize"),
                bytes[..header_len - 16],
                "{file}"
            );
            assert_eq!(ser(&h), bytes[..header_len], "{file}");

            // Only the header: still parses (body is ignored)
            let only = HeaderCodec::parse(&bytes[..header_len], &DecodeLimits::default());
            assert!(matches!(only, Ok(HeaderParse::Done { .. })), "{file}");
            assert_eq!(
                HeaderCodec::parse(&bytes[..header_len - 1], &DecodeLimits::default()),
                Ok(HeaderParse::NeedMore),
                "{file}"
            );
        }
    }

    #[test]
    fn max_context_header_parses_with_default_limits() {
        let caller: crate::encryption::CallerContext = [(
            "k".to_owned(),
            "v".repeat(crate::encryption::context::MAX_CONTEXT_LEN - 19),
        )]
        .into();
        let mut h = sample();
        h.context = EncryptionContext::new_reserved(None)
            .with_caller(&caller)
            .expect("at cap");
        h.edks = vec![
            EncryptedDataKey {
                provider_id: "osc-file".into(),
                provider_info: vec![1; 200],
                ciphertext: vec![9; 48],
            };
            2
        ];
        let b = ser(&h);
        assert!(b.len() <= DecodeLimits::default().max_header_len);
        let (parsed, len, _) = parse_ok(&b);
        assert_eq!(parsed, h);
        assert_eq!(len, b.len());
    }
}
