//! ESDK Raw AES keyring compatible wrapping of data keys (AES-256-GCM, the
//! wrapping key is used directly, no KDF).
//!
//! `provider_info = key_name | tag_len_bits u32 (128) | iv_len u32 (12) | iv`,
//! `ciphertext = enc(DEK)[32] | tag[16]`, AAD = serialized encryption context.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::rand;
use zeroize::Zeroizing;

use super::{DataKey, KeyError};
use crate::encryption::header::EncryptedDataKey;

const TAG_BITS: u32 = 128;
const IV_LEN: usize = 12;
/// `IV_LEN` as written in `provider_info` (a byte count).
const IV_LEN_FIELD: u32 = 12;
const TAG_LEN: usize = 16;
/// `tag_len_bits u32 | iv_len u32 | iv`.
const INFO_SUFFIX: usize = 4 + 4 + IV_LEN;
const WRAPPED_LEN: usize = 32 + TAG_LEN;

/// A named AES-256 wrapping key. `Debug` shows the name only.
pub(crate) struct RawAesKey {
    name: String,
    key: LessSafeKey,
    wraps: AtomicU64,
}

impl std::fmt::Debug for RawAesKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawAesKey")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RawAesKey {
    /// # Panics
    /// Never in practice: a 32-byte key is always valid for AES-256-GCM.
    pub(crate) fn new(name: String, material: &[u8; 32]) -> Self {
        let key = UnboundKey::new(&AES_256_GCM, material).expect("32-byte AES-256 key");
        Self {
            name,
            key: LessSafeKey::new(key),
            wraps: AtomicU64::new(0),
        }
    }

    /// Number of wrap operations performed with this key.
    pub(crate) fn wraps(&self) -> u64 {
        self.wraps.load(Ordering::Relaxed)
    }

    /// Wraps `dek` with a fresh random IV, binding `aad`.
    ///
    /// # Errors
    /// [`KeyError::Rng`] if random generation fails (see that variant).
    pub(crate) fn wrap(
        &self,
        namespace: &str,
        dek: &DataKey,
        aad: &[u8],
    ) -> Result<EncryptedDataKey, KeyError> {
        let mut iv = [0u8; IV_LEN];
        rand::fill(&mut iv).map_err(|_| KeyError::Rng)?;
        let mut buf = Vec::with_capacity(WRAPPED_LEN);
        buf.extend_from_slice(dek.as_bytes());
        self.key
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(iv), Aad::from(aad), &mut buf)
            .expect("AES-GCM seal of 32 bytes");
        self.wraps.fetch_add(1, Ordering::Relaxed);

        let mut info = Vec::with_capacity(self.name.len() + INFO_SUFFIX);
        info.extend_from_slice(self.name.as_bytes());
        info.extend_from_slice(&TAG_BITS.to_be_bytes());
        info.extend_from_slice(&IV_LEN_FIELD.to_be_bytes());
        info.extend_from_slice(&iv);
        Ok(EncryptedDataKey {
            provider_id: namespace.to_owned(),
            provider_info: info,
            ciphertext: buf,
        })
    }

    /// Validates `provider_info` (exact name, tag bits, IV length, total
    /// length) and the ciphertext length, then opens the EDK.
    ///
    /// # Errors
    /// [`KeyError::UnwrapFailed`] for any mismatch or authentication failure.
    pub(crate) fn unwrap(&self, edk: &EncryptedDataKey, aad: &[u8]) -> Result<DataKey, KeyError> {
        let fail = || KeyError::UnwrapFailed {
            key_id: self.name.clone(),
        };
        let info = &edk.provider_info;
        let name = self.name.as_bytes();
        if info.len() != name.len() + INFO_SUFFIX
            || &info[..name.len()] != name
            || edk.ciphertext.len() != WRAPPED_LEN
        {
            return Err(fail());
        }
        let rest = &info[name.len()..];
        let tag_bits = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let iv_len = u32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]);
        if tag_bits != TAG_BITS || iv_len != IV_LEN_FIELD {
            return Err(fail());
        }
        let mut iv = [0u8; IV_LEN];
        iv.copy_from_slice(&rest[8..]);

        // Decrypted in place inside a zeroizing buffer; the key is then copied
        // straight into its own zeroizing storage.
        let mut buf = Zeroizing::new(edk.ciphertext.clone());
        let plain = self
            .key
            .open_in_place(Nonce::assume_unique_for_key(iv), Aad::from(aad), &mut buf)
            .map_err(|_| fail())?;
        DataKey::from_slice(plain).ok_or_else(fail)
    }
}

/// The key name an EDK claims (`provider_info` minus the 20-byte suffix).
pub(crate) fn edk_key_name(edk: &EncryptedDataKey) -> Option<&str> {
    let len = edk.provider_info.len().checked_sub(INFO_SUFFIX)?;
    std::str::from_utf8(&edk.provider_info[..len]).ok()
}

/// Opens the first EDK of `namespace` whose named key is in `keys`; an EDK
/// that fails to open does not stop the search.
///
/// # Errors
/// [`KeyError::UnknownKey`] when no such EDK exists (listing the key names of
/// the EDKs in `namespace` and the provider IDs of the others), otherwise the
/// last [`KeyError::UnwrapFailed`].
pub(crate) fn unwrap_edks(
    namespace: &str,
    keys: &BTreeMap<String, Arc<RawAesKey>>,
    edks: &[EncryptedDataKey],
    aad: &[u8],
) -> Result<DataKey, KeyError> {
    let mut seen = Vec::new();
    let mut foreign = Vec::new();
    let mut last = None;
    for edk in edks {
        if edk.provider_id != namespace {
            if !foreign.contains(&edk.provider_id) {
                foreign.push(edk.provider_id.clone());
            }
            continue;
        }
        let Some(name) = edk_key_name(edk) else {
            continue;
        };
        seen.push(name.to_owned());
        if let Some(key) = keys.get(name) {
            match key.unwrap(edk, aad) {
                Ok(dek) => return Ok(dek),
                Err(e) => last = Some(e),
            }
        }
    }
    Err(last.unwrap_or(KeyError::UnknownKey {
        key_ids: seen,
        foreign_provider_ids: foreign,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::context::EncryptionContext;
    use crate::encryption::keys::KeyProvider;
    use crate::encryption::keys::Keyring;

    const KEK: [u8; 32] = [0x5a; 32];

    fn key() -> RawAesKey {
        RawAesKey::new("kek-a".into(), &KEK)
    }

    fn dek() -> DataKey {
        DataKey::from_slice(&[7; 32]).expect("32 bytes")
    }

    #[test]
    fn wrap_unwrap_round_trip() {
        let k = key();
        let edk = k.wrap("osc-file", &dek(), b"ctx").expect("wrap");
        assert_eq!(edk.provider_id, "osc-file");
        assert_eq!(edk.ciphertext.len(), 48);
        assert_eq!(edk_key_name(&edk), Some("kek-a"));
        assert_eq!(k.unwrap(&edk, b"ctx").expect("unwrap").as_bytes(), &[7; 32]);
        assert_eq!(k.wraps(), 1);
    }

    #[test]
    fn wrap_uses_a_fresh_iv() {
        let k = key();
        let a = k.wrap("osc-file", &dek(), b"").expect("wrap");
        let b = k.wrap("osc-file", &dek(), b"").expect("wrap");
        assert_ne!(a.provider_info, b.provider_info);
        assert_eq!(k.wraps(), 2);
    }

    #[test]
    fn different_context_fails() {
        let k = key();
        let edk = k.wrap("osc-file", &dek(), b"one").expect("wrap");
        assert!(matches!(
            k.unwrap(&edk, b"two"),
            Err(KeyError::UnwrapFailed { key_id }) if key_id == "kek-a"
        ));
    }

    #[test]
    fn malformed_provider_info_fails() {
        let k = key();
        let good = k.wrap("osc-file", &dek(), b"c").expect("wrap");
        let n = "kek-a".len();
        let mutate = |f: &dyn Fn(&mut EncryptedDataKey)| {
            let mut e = good.clone();
            f(&mut e);
            assert!(matches!(
                k.unwrap(&e, b"c"),
                Err(KeyError::UnwrapFailed { .. })
            ));
        };
        mutate(&|e| e.provider_info[n + 7] ^= 1); // iv_len 12 -> 13
        mutate(&|e| e.provider_info[n + 3] ^= 1); // tag bits
        mutate(&|e| e.provider_info[0] ^= 1); // name
        mutate(&|e| e.provider_info.push(0)); // length
        mutate(&|e| e.provider_info.truncate(n + 8));
        mutate(&|e| e.ciphertext.push(0));
        mutate(&|e| e.ciphertext[0] ^= 1); // tampered
        mutate(&|e| e.provider_info[n + 12] ^= 1); // wrong IV
    }

    #[test]
    fn edks_without_a_readable_key_name_are_skipped_not_matched() {
        let k = key();
        let good = k.wrap("osc-file", &dek(), b"c").expect("wrap");
        let keys = BTreeMap::from([("kek-a".to_owned(), Arc::new(key()))]);
        let mut short = good.clone();
        short.provider_info.truncate(INFO_SUFFIX - 1);
        let mut not_utf8 = good.clone();
        not_utf8.provider_info[0] = 0xff;
        let err = unwrap_edks("osc-file", &keys, &[short, not_utf8], b"c")
            .err()
            .expect("no usable edk");
        assert!(
            matches!(&err, KeyError::UnknownKey { key_ids, foreign_provider_ids }
                if key_ids.is_empty() && foreign_provider_ids.is_empty()),
            "{err:?}"
        );
        // A usable EDK after the unreadable ones still opens.
        let mut bad = good.clone();
        bad.provider_info.truncate(INFO_SUFFIX - 1);
        let dek = unwrap_edks("osc-file", &keys, &[bad, good], b"c").expect("opens");
        assert_eq!(dek.as_bytes(), &[7; 32]);
    }

    #[test]
    fn debug_hides_key_material() {
        let text = format!("{:?}", key());
        assert!(text.contains("kek-a"));
        assert!(!text.contains(&crate::hex::encode(&KEK)));
        // `Debug` prints a `[u8; 32]` in decimal.
        assert!(!text.contains(&format!("{KEK:?}")));
        assert!(!text.contains("90, 90"));
    }

    #[tokio::test]
    async fn provider_unknown_key_lists_edk_names() {
        let p = Keyring::from_material("kek-a", &[("kek-a", &KEK)]);
        let other = RawAesKey::new("kek-b".into(), &[1; 32]);
        let ctx = EncryptionContext::new_reserved(Some("o"));
        let aad = ctx.serialize().expect("serialize");
        let edk = other.wrap("osc-file", &dek(), &aad).expect("wrap");
        match p.unwrap(&[edk], &ctx).await {
            Err(KeyError::UnknownKey { key_ids, .. }) => assert_eq!(key_ids, ["kek-b"]),
            other => panic!("unexpected: {:?}", other.map(|_| ())),
        }
    }

    #[tokio::test]
    async fn provider_round_trip_and_foreign_namespace_ignored() {
        let p = Keyring::from_material("kek-a", &[("kek-a", &KEK)]);
        let ctx = EncryptionContext::new_reserved(Some("o"));
        let edks = p.wrap(&dek(), &ctx).await.expect("wrap");
        assert_eq!(
            p.unwrap(&edks, &ctx).await.expect("unwrap").as_bytes(),
            &[7; 32]
        );

        let mut foreign = edks.clone();
        foreign[0].provider_id = "aws-kms".into();
        match p.unwrap(&foreign, &ctx).await {
            Err(e @ KeyError::UnknownKey { .. }) => {
                let KeyError::UnknownKey {
                    key_ids,
                    foreign_provider_ids,
                } = &e
                else {
                    unreachable!()
                };
                assert_eq!(key_ids.as_slice(), &[] as &[String]);
                assert_eq!(foreign_provider_ids, &["aws-kms"]);
                assert!(e.to_string().contains("aws-kms"), "{e}");
            }
            other => panic!("unexpected: {:?}", other.map(|_| ())),
        }
    }

    /// An EDK for `name` wrapped by a key with other material.
    fn edk_with(name: &str, material: u8, ctx_aad: &[u8]) -> EncryptedDataKey {
        RawAesKey::new(name.into(), &[material; 32])
            .wrap("osc-file", &dek(), ctx_aad)
            .expect("wrap")
    }

    fn keys_a() -> BTreeMap<String, Arc<RawAesKey>> {
        let mut keys = BTreeMap::new();
        keys.insert("kek-a".to_owned(), Arc::new(key()));
        keys
    }

    #[test]
    fn failing_edk_falls_through_to_the_next() {
        let keys = keys_a();
        let bad = edk_with("kek-a", 0x11, b"ctx"); // same name, wrong material
        let good = edk_with("kek-a", 0x5a, b"ctx");
        let got = unwrap_edks("osc-file", &keys, &[bad.clone(), good.clone()], b"ctx")
            .expect("second EDK opens");
        assert_eq!(got.as_bytes(), &[7; 32]);
        // The same list in the other order, and with unknown / foreign EDKs
        // in front.
        let unknown = edk_with("kek-z", 0x22, b"ctx");
        let mut foreign = good.clone();
        foreign.provider_id = "aws-kms".into();
        let got = unwrap_edks("osc-file", &keys, &[foreign, unknown, bad, good], b"ctx")
            .expect("fallthrough");
        assert_eq!(got.as_bytes(), &[7; 32]);
    }

    #[test]
    fn unwrap_failed_takes_precedence_over_unknown_key() {
        let keys = keys_a();
        let bad = edk_with("kek-a", 0x11, b"ctx");
        let unknown = edk_with("kek-z", 0x22, b"ctx");
        for edks in [[unknown.clone(), bad.clone()], [bad, unknown]] {
            assert!(matches!(
                unwrap_edks("osc-file", &keys, &edks, b"ctx"),
                Err(KeyError::UnwrapFailed { key_id }) if key_id == "kek-a"
            ));
        }
    }
}
