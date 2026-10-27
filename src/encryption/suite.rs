//! Algorithm suite `0x0478` (AES-256-GCM, HKDF-SHA512, key commitment):
//! key derivation, commitment and header authentication.

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::constant_time::verify_slices_are_equal;
use aws_lc_rs::hkdf::{HKDF_SHA512, KeyType, Salt};
use aws_lc_rs::rand;

use super::header::{FormatError, short_debug_tail};
use super::keys::{DataKey, KeyError};

const SUITE_ID: [u8; 2] = 0x0478_u16.to_be_bytes();

/// Errors raised while decrypting a message.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecryptError {
    #[error("key commitment mismatch")]
    CommitmentMismatch,
    #[error("header authentication failed")]
    HeaderAuth,
    #[error("frame {seq} authentication failed")]
    FrameAuth { seq: u32 },
    #[error("unexpected frame sequence number: expected {expected}, got {got}")]
    BadSequence { expected: u32, got: u32 },
    #[error("bad IV in frame {seq}")]
    BadIv { seq: u32 },
    #[error("final frame longer than the frame length")]
    FinalFrameTooLong,
    #[error("missing final frame")]
    MissingFinalFrame,
    #[error("trailing bytes after the final frame")]
    TrailingBytes,
    /// `found` is read from the still unauthenticated header: never present it
    /// as verified data.
    #[error(
        "object key binding mismatch: expected {}, found {}",
        short_debug_tail(.expected),
        short_debug_tail(.found)
    )]
    BindingMismatch { expected: String, found: String },
    /// Input arrived while the decryptor waits for its data key.
    #[error("the data key must be supplied before more input is fed")]
    KeyRequired,
    /// An earlier call failed; the decryptor accepts nothing more.
    #[error("the decryptor already failed")]
    Failed,
    #[error("format error: {0}")]
    Format(#[from] FormatError),
    /// A call that is not valid in the decryptor's current state (for example
    /// supplying a data key before the header is parsed or twice).
    #[error("decryptor used out of order: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Key(#[from] super::keys::KeyError),
}

/// Keys derived from a data key and message ID.
pub struct MessageKeys {
    pub derived: LessSafeKey,
    pub commitment: [u8; 32],
}

struct Len32;

impl KeyType for Len32 {
    fn len(&self) -> usize {
        32
    }
}

/// Derives the message key and the key commitment (`suite_data`).
///
/// # Panics
/// Never in practice: HKDF-SHA512 can always produce 32 bytes, and a 32-byte
/// key is always valid for AES-256-GCM.
pub fn derive(dek: &DataKey, message_id: &[u8; 32]) -> MessageKeys {
    let prk = Salt::new(HKDF_SHA512, message_id).extract(dek.as_bytes());

    let mut derived = zeroize::Zeroizing::new([0u8; 32]);
    let info = [&SUITE_ID[..], b"DERIVEKEY"];
    prk.expand(&info, Len32)
        .and_then(|okm| okm.fill(&mut derived[..]))
        .expect("HKDF-SHA512 expands to 32 bytes");

    let mut commitment = [0u8; 32];
    prk.expand(&[b"COMMITKEY"], Len32)
        .and_then(|okm| okm.fill(&mut commitment))
        .expect("HKDF-SHA512 expands to 32 bytes");

    let key = UnboundKey::new(&AES_256_GCM, &derived[..]).expect("32-byte AES-256 key");
    MessageKeys {
        derived: LessSafeKey::new(key),
        commitment,
    }
}

/// Computes the header authentication tag over `header_bytes` (version
/// through `suite_data`).
///
/// The tag uses the fixed zero IV that ESDK v2 mandates, so this must be
/// called at most once per (data key, message ID) pair: never re-tag
/// different header bytes under the same message keys. That would reuse an
/// AES-GCM nonce and let an observer of both tags recover the GHASH key.
/// Changing a header's EDKs therefore needs a fresh data key and message ID.
///
/// # Panics
/// Never in practice: sealing an empty plaintext cannot fail.
pub fn header_tag(keys: &MessageKeys, header_bytes: &[u8]) -> [u8; 16] {
    let nonce = Nonce::assume_unique_for_key([0u8; 12]);
    let tag = keys
        .derived
        .seal_in_place_separate_tag(nonce, Aad::from(header_bytes), &mut [])
        .expect("seal empty plaintext");
    let mut out = [0u8; 16];
    out.copy_from_slice(tag.as_ref());
    out
}

/// Verifies the key commitment and then the header tag.
///
/// `header_bytes` is the authenticated region (`version` through
/// `suite_data`); the commitment checked is its last 32 bytes, so the value
/// compared is always the value the tag covers. The tag is checked with
/// AWS-LC's constant-time AEAD open over an empty ciphertext.
///
/// # Errors
/// [`DecryptError::CommitmentMismatch`] (also when `header_bytes` is too short
/// to hold a commitment) or [`DecryptError::HeaderAuth`].
pub fn verify_header(
    keys: &MessageKeys,
    header_bytes: &[u8],
    tag: &[u8; 16],
) -> Result<(), DecryptError> {
    let Some(commitment) = header_bytes
        .len()
        .checked_sub(32)
        .map(|at| &header_bytes[at..])
    else {
        return Err(DecryptError::CommitmentMismatch);
    };
    if verify_slices_are_equal(&keys.commitment, commitment).is_err() {
        return Err(DecryptError::CommitmentMismatch);
    }
    keys.derived
        .open_in_place_separate_tag(
            Nonce::assume_unique_for_key([0u8; 12]),
            Aad::from(header_bytes),
            tag,
            &mut [],
        )
        .map(|_| ())
        .map_err(|_| DecryptError::HeaderAuth)
}

/// Generates a fresh data key and message ID.
///
/// # Errors
/// [`KeyError::Rng`] if random generation fails (see that variant).
pub fn fresh_message_material() -> Result<(DataKey, [u8; 32]), KeyError> {
    let key = DataKey::random()?;
    let mut id = [0u8; 32];
    rand::fill(&mut id).map_err(|_| KeyError::Rng)?;
    Ok((key, id))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::encryption::fixtures::{ESDK, dek, esdk_provider};

    #[test]
    fn derive_is_deterministic_and_id_dependent() {
        let a = derive(&dek(1), &[2; 32]);
        let b = derive(&dek(1), &[2; 32]);
        let c = derive(&dek(1), &[3; 32]);
        assert_eq!(a.commitment, b.commitment);
        assert_eq!(header_tag(&a, b"h"), header_tag(&b, b"h"));
        assert_ne!(a.commitment, c.commitment);
        assert_ne!(header_tag(&a, b"h"), header_tag(&c, b"h"));
    }

    /// Header-like bytes ending in `commitment`, as `verify_header` expects.
    fn with_commitment(prefix: &[u8], commitment: &[u8; 32]) -> Vec<u8> {
        let mut v = prefix.to_vec();
        v.extend_from_slice(commitment);
        v
    }

    #[test]
    fn verify_header_detects_flips() {
        let k = derive(&dek(1), &[2; 32]);
        let header = with_commitment(b"some header bytes", &k.commitment);
        let tag = header_tag(&k, &header);
        verify_header(&k, &header, &tag).expect("valid");

        // A wrong commitment inside the (consistently tagged) bytes.
        let mut c = k.commitment;
        c[5] ^= 1;
        let bad_commit = with_commitment(b"some header bytes", &c);
        let tag_for_bad = header_tag(&k, &bad_commit);
        assert!(matches!(
            verify_header(&k, &bad_commit, &tag_for_bad),
            Err(DecryptError::CommitmentMismatch)
        ));

        let mut h = header.clone();
        h[0] ^= 1;
        assert!(matches!(
            verify_header(&k, &h, &tag),
            Err(DecryptError::HeaderAuth)
        ));
        let mut t = tag;
        t[15] ^= 0x80;
        assert!(matches!(
            verify_header(&k, &header, &t),
            Err(DecryptError::HeaderAuth)
        ));
    }

    #[test]
    fn commitment_is_checked_before_the_tag() {
        let k = derive(&dek(1), &[2; 32]);
        let mut c = k.commitment;
        c[0] ^= 1;
        let header = with_commitment(b"some header bytes", &c);
        // Both the commitment and the tag are wrong: the commitment wins.
        assert!(matches!(
            verify_header(&k, &header, &[0; 16]),
            Err(DecryptError::CommitmentMismatch)
        ));
        // Too short to hold a commitment.
        assert!(matches!(
            verify_header(&k, &[0; 31], &[0; 16]),
            Err(DecryptError::CommitmentMismatch)
        ));
    }

    #[test]
    fn binding_mismatch_shows_the_differing_tail() {
        let msg = |expected: String, found: String| {
            DecryptError::BindingMismatch { expected, found }.to_string()
        };
        let long = "p/".repeat(50);
        let m = msg(format!("{long}real/x.enc"), format!("{long}link/x.enc"));
        assert!(m.contains("real/x.enc") && m.contains("link/x.enc"), "{m}");
        assert!(m.len() < 200, "{m}");

        let m = msg("é".repeat(100), "b".into());
        assert!(m.contains(&format!("...{:?}", "é".repeat(64))), "{m}");

        let exact = "a".repeat(64);
        let m = msg(exact.clone(), exact.clone());
        assert!(!m.contains("..."), "{m}");
        assert!(m.contains(&exact));
    }

    #[test]
    fn no_repeat_in_1e6() {
        let mut deks = HashSet::new();
        let mut ids = HashSet::new();
        for _ in 0..1_000_000 {
            let (d, id) = fresh_message_material().expect("rng");
            assert!(deks.insert(*d.as_bytes()));
            assert!(ids.insert(id));
        }
    }

    #[test]
    fn esdk_vectors_derive_and_verify() {
        use crate::encryption::header::{DecodeLimits, HeaderCodec, HeaderParse};
        use crate::encryption::keys::KeyProvider;

        let provider = esdk_provider();
        for v in &ESDK.vectors {
            let (file, bytes) = (&v.file, &v.bytes);
            let HeaderParse::Done {
                header,
                header_len,
                authenticated_bytes,
            } = HeaderCodec::parse(bytes, &DecodeLimits::default()).expect("parse")
            else {
                panic!("{file}: incomplete header");
            };
            let dek = futures::executor::block_on(provider.unwrap(&header.edks, &header.context))
                .expect(file);
            let keys = derive(&dek, &header.message_id);
            assert_eq!(keys.commitment, header.commitment, "{file}");

            let auth = &bytes[authenticated_bytes.clone()];
            assert_eq!(auth[auth.len() - 32..], header.commitment, "{file}");
            verify_header(&keys, auth, &header.auth_tag).expect(file);
            assert_eq!(header_tag(&keys, auth), header.auth_tag, "{file}");

            // Flipping any single byte of the authenticated region must fail,
            // whether the flip is detected by the tag or the commitment.
            let mut tampered = auth.to_vec();
            for i in 0..tampered.len() {
                tampered[i] ^= 0x01;
                assert!(
                    verify_header(&keys, &tampered, &header.auth_tag).is_err(),
                    "{file}: flip at {i} accepted"
                );
                tampered[i] ^= 0x01;
            }
            assert_eq!(header_len, authenticated_bytes.end + 16, "{file}");
        }
    }
}
