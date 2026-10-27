//! Shared fixtures of the unit tests and the fuzz harness.

use super::header::{DecodeLimits, EncryptedDataKey};
use super::keys::DataKey;

/// A data key of 32 copies of `b`.
pub fn dek(b: u8) -> DataKey {
    DataKey::from_slice(&[b; 32]).expect("32 bytes")
}

pub fn edk() -> EncryptedDataKey {
    EncryptedDataKey {
        provider_id: "osc-file".into(),
        provider_info: b"test-key".to_vec(),
        ciphertext: vec![7; 48],
    }
}

/// Limits that accept the tiny frames used to keep exhaustive loops and the
/// fuzzer fast.
pub fn small_limits() -> DecodeLimits {
    DecodeLimits {
        min_frame_length: 1,
        ..DecodeLimits::default()
    }
}

#[cfg(test)]
pub use test_only::*;

#[cfg(test)]
mod test_only {
    use std::sync::LazyLock;

    use bytes::BytesMut;

    use super::{dek, edk, small_limits};
    use crate::encryption::context::EncryptionContext;
    use crate::encryption::frame::Encryptor;
    use crate::encryption::keys::Keyring;

    /// `n` deterministic bytes, varied by `salt`.
    pub fn plain(n: usize, salt: usize) -> Vec<u8> {
        (0..n)
            .map(|i| u8::try_from((i * 7 + 3 + salt) & 0xff).expect("masked"))
            .collect()
    }

    /// Whole message and header length, from `data` with a given key and
    /// message ID byte, feeding the encryptor `chunk` bytes at a time.
    pub fn encrypt_with(data: &[u8], f: u32, key: u8, id: u8, chunk: usize) -> (Vec<u8>, usize) {
        let ctx = EncryptionContext::new_reserved(Some("obj/key"));
        let (header, mut enc) =
            Encryptor::with_limits(&dek(key), [id; 32], ctx, vec![edk()], f, &small_limits())
                .expect("encryptor");
        let mut out = BytesMut::from(&header[..]);
        for c in data.chunks(chunk.max(1)) {
            enc.update(c, &mut out);
        }
        enc.finish(&mut out);
        (out.to_vec(), header.len())
    }

    pub fn encrypt(data: &[u8], f: u32) -> (Vec<u8>, usize) {
        encrypt_with(data, f, 0x42, 9, usize::MAX)
    }

    /// One vector of `tests/vectors/esdk`.
    pub struct EsdkVector {
        pub file: String,
        pub bytes: Vec<u8>,
        pub sha256: Vec<u8>,
        pub size: u64,
        pub header_len: u64,
        pub frame_length: u32,
        /// The manifest's `inspect` object of this vector.
        pub inspect: serde_json::Value,
    }

    /// The ESDK vectors and their TEST-ONLY KEK, parsed once.
    pub struct Esdk {
        pub kek: [u8; 32],
        pub vectors: Vec<EsdkVector>,
    }

    pub static ESDK: LazyLock<Esdk> = LazyLock::new(|| {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/esdk");
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(format!("{dir}/manifest.json")).expect("read"))
                .expect("json");
        assert_eq!(manifest["key_name"], "kek-test-0001");
        let kek = crate::hex::decode(manifest["kek_hex"].as_str().expect("kek_hex"))
            .expect("hex")
            .try_into()
            .expect("32 bytes");
        let vectors: Vec<_> = manifest["vectors"]
            .as_array()
            .expect("vectors")
            .iter()
            .map(|v| {
                let file = v["file"].as_str().expect("file").to_owned();
                EsdkVector {
                    bytes: std::fs::read(format!("{dir}/{file}")).expect("vector"),
                    sha256: crate::hex::decode(v["plaintext_sha256"].as_str().expect("sha"))
                        .expect("hex"),
                    size: v["plaintext_size"].as_u64().expect("size"),
                    header_len: v["inspect"]["header_length"].as_u64().expect("hl"),
                    frame_length: u32::try_from(v["frame_length"].as_u64().expect("fl"))
                        .expect("u32"),
                    inspect: v["inspect"].clone(),
                    file,
                }
            })
            .collect();
        assert!(vectors.len() >= 20);
        Esdk { kek, vectors }
    });

    /// Provider holding the TEST-ONLY KEK of the ESDK vectors.
    pub fn esdk_provider() -> Keyring {
        Keyring::from_material("kek-test-0001", &[("kek-test-0001", &ESDK.kek)])
    }
}
