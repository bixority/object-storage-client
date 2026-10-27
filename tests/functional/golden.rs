//! Golden ciphertexts: files written by this implementation at v1, which every
//! later version must keep decrypting.
//!
//! The files live in `tests/vectors/golden/v1/` and the test keyring in its
//! `keyring` file. The keys are TEST-ONLY material and protect nothing.
//! Regeneration is never needed in CI. To regenerate (this replaces the keys,
//! so every file changes), run:
//!
//! ```text
//! cargo test --test functional regenerate_golden_v1 -- --ignored
//! ```
//!
//! The ciphertexts are written without object-key binding, because the bound
//! key of a `file://` object is its absolute path.

use futures::StreamExt;
use object_storage_client::encryption::{EncryptionConfig, Keyring, new_keyring, rotate_keyring};
use object_storage_client::{EncryptedObjectStorageClient, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/golden/v1")
}

fn plaintext(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i * 7 + i / 251) % 256).unwrap_or(0))
        .collect()
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

/// Writes the golden files. Replaces everything under `golden/v1`.
#[tokio::test]
#[ignore = "regenerates the committed golden files"]
async fn regenerate_golden_v1() -> Result<()> {
    let out = golden_dir();
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out)?;

    let day = |m, d| chrono::NaiveDate::from_ymd_opt(2026, m, d).expect("date");
    let first = new_keyring("golden", day(9, 1)).expect("old key");
    let both = rotate_keyring(&first, "golden", day(10, 26)).expect("new key");
    let id_of = |text: &str| Keyring::parse(text).expect("parse").status().active;
    let (old_id, new_id) = (id_of(&first), id_of(&both));
    std::fs::write(out.join("keyring"), both.as_bytes())?;

    let current = client(&both);
    // The same keyring with the older key active.
    let legacy = client(&both.replace(&format!("active={new_id}"), &format!("active={old_id}")));
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let plain_ctx = BTreeMap::new();

    // (file, length, context, written under the old key)
    let cases = [
        ("size_0.bin", 0, &plain_ctx, false),
        ("size_1.bin", 1, &plain_ctx, false),
        ("size_65536.bin", 65_536, &plain_ctx, false),
        ("size_200000.bin", 200_000, &plain_ctx, false),
        ("context.bin", 1000, &ctx, false),
        ("old_key.bin", 70_000, &plain_ctx, true),
    ];
    let mut files = Vec::new();
    for (name, len, context, old) in cases {
        let data = plaintext(len);
        let url = format!("file://{}/{name}", out.display());
        let writer = if old { &legacy } else { &current };
        writer.put_with_context(&url, data.clone(), context).await?;
        files.push(serde_json::json!({
            "file": name,
            "plaintext_len": len,
            "plaintext_sha256": sha256_hex(&data),
            "key_id": if old { &old_id } else { &new_id },
            "context": context,
        }));
    }
    let manifest = serde_json::json!({
        "note": "TEST-ONLY keys; see README.md",
        "active_key_id": new_id,
        "files": files,
    });
    std::fs::write(
        out.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("json"),
    )?;
    Ok(())
}
