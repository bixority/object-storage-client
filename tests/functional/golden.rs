//! Golden ciphertexts: files written by this implementation at v1, which every
//! later version must keep decrypting.
//!
//! The files live in `tests/vectors/golden/v1/` and the test keyring in its
//! `keyring` file. The keys are TEST-ONLY material and protect nothing.
//! v1 is frozen: it means "written by v0.2.0" and is never regenerated. A new
//! format version adds a `v2` directory with its own generator (the v1
//! generator is in git history, commit 3a18816).
//!
//! The ciphertexts are written without object-key binding, because the bound
//! key of a `file://` object is its absolute path.

use futures::StreamExt;
use object_storage_client::encryption::{EncryptionConfig, Keyring};
use object_storage_client::{EncryptedObjectStorageClient, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The key IDs of the committed generation.
const V1_ACTIVE: &str = "golden-2026-10-a065e952";
const V1_OLD: &str = "golden-2026-09-6a235f95";

/// SHA-256 of each committed `.bin` (binary in `.gitattributes`, so the same on
/// every platform). The keyring and manifest are text and are pinned by key ID.
const V1_FILES: [(&str, &str); 6] = [
    (
        "context.bin",
        "478d2166c5ceb92badd004755446853a6c334ef1d9105bb5be4bf5fbb3e37793",
    ),
    (
        "old_key.bin",
        "e962c43aca2205ceca9dc005bf262f32acb18ba866c357d585482ee49acd7f86",
    ),
    (
        "size_0.bin",
        "8d1669eb2b76a042ad7b11ec91f61a64f60635f56e208541780237b7c6c809ba",
    ),
    (
        "size_1.bin",
        "84e62ee6a164309fbcb63951575cb9063f87c5f64aae176f7c59e4cb31f06cd3",
    ),
    (
        "size_200000.bin",
        "93315dc1404d6188082509708dc9544c90d033bab6fe12ece25a1f03493e9710",
    ),
    (
        "size_65536.bin",
        "005c7c863dbcde2f959f3c413a113582ddfeea29f566628ff1a71296a7f34fac",
    ),
];

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/golden/v1")
}

fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data);
    let mut out = String::new();
    for b in digest.as_ref() {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn client(keyring_text: &str) -> EncryptedObjectStorageClient {
    let provider = Keyring::parse(keyring_text).expect("parse keyring");
    EncryptedObjectStorageClient::new(
        EncryptionConfig::new(Arc::new(provider)).bind_object_key(false),
    )
}

#[tokio::test]
async fn golden_v1_decrypts() -> Result<()> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(golden_dir().join("manifest.json"))?)
            .expect("manifest");
    // Parsed from text: git does not keep the file mode `from_file` wants.
    let text = std::fs::read_to_string(golden_dir().join("keyring"))?;
    let active = manifest["active_key_id"].as_str().expect("active");
    assert!(text.contains(&format!("active={active}\n")));
    let client = client(&text);

    let entries = manifest["files"].as_array().expect("files");
    assert!(entries.len() >= 6);
    for e in entries {
        let name = e["file"].as_str().expect("file");
        let url = format!("file://{}/{name}", golden_dir().display());
        let want_hash = e["plaintext_sha256"].as_str().expect("hash");
        let want_len = usize::try_from(e["plaintext_len"].as_u64().expect("len")).expect("len");

        let whole = client.get(&url).await?;
        assert_eq!(whole.len(), want_len, "{name}");
        assert_eq!(sha256_hex(&whole), want_hash, "{name}");

        let mut streamed = Vec::new();
        let mut stream = client.get_stream(&url).await?;
        while let Some(chunk) = stream.next().await {
            streamed.extend_from_slice(&chunk?);
        }
        assert_eq!(sha256_hex(&streamed), want_hash, "{name} (stream)");

        let info = client.inspect_encryption(&url).await?;
        assert_eq!(
            info.key_names,
            vec![e["key_id"].as_str().expect("key_id").to_owned()],
            "{name}"
        );
        let want_ctx: BTreeMap<String, String> = e["context"]
            .as_object()
            .expect("context")
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().expect("value").to_owned()))
            .collect();
        for (k, v) in &want_ctx {
            assert_eq!(info.context.get(k), Some(v), "{name}");
        }
    }
    Ok(())
}

#[test]
fn golden_v1_files_are_the_committed_ones() {
    for (name, want) in V1_FILES {
        let bytes = std::fs::read(golden_dir().join(name)).expect("read golden file");
        assert_eq!(sha256_hex(&bytes), want, "{name} changed");
    }

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(golden_dir().join("manifest.json")).expect("read"))
            .expect("manifest");
    assert_eq!(manifest["active_key_id"], V1_ACTIVE);
    let entries = manifest["files"].as_array().expect("files");
    assert_eq!(entries.len(), V1_FILES.len());
    for e in entries {
        let name = e["file"].as_str().expect("file");
        let want = if name == "old_key.bin" {
            V1_OLD
        } else {
            V1_ACTIVE
        };
        assert_eq!(e["key_id"], want, "{name}");
        assert!(V1_FILES.iter().any(|(f, _)| *f == name), "{name}");
    }

    let text = std::fs::read_to_string(golden_dir().join("keyring")).expect("keyring");
    assert!(text.contains(&format!("active={V1_ACTIVE}\n")));
    let mut keys: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("key="))
        .filter_map(|l| l.split_whitespace().next())
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, [V1_OLD, V1_ACTIVE]);
}
