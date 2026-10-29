//! Functional tests of the encrypted client over `file://` stores.

use crate::support::{chunked, drain, pattern};
use bytes::Bytes;
use chrono::NaiveDate;
use futures::StreamExt;
use object_storage_client::encryption::{
    DecodeLimits, DecryptError, EncryptionConfig, Keyring, keys::KeyError, new_keyring,
    rotate_keyring,
};
use object_storage_client::{EncryptedObjectStorageClient, Error, ObjectStorageClient, Result};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::{TempDir, tempdir};

fn config_in() -> EncryptionConfig {
    config_named("kek")
}

/// A fresh keyring with one key.
fn keyring_named(prefix: &str) -> Keyring {
    let today = NaiveDate::from_ymd_opt(2026, 10, 26).expect("date");
    Keyring::parse(&new_keyring(prefix, today).expect("keyring")).expect("parse")
}

/// Key IDs end in only 32 bits of the check value, so clients that must have
/// different keys also get different prefixes.
fn config_named(prefix: &str) -> EncryptionConfig {
    EncryptionConfig::new(Arc::new(keyring_named(prefix)))
}

fn config_for(text: &str) -> EncryptionConfig {
    EncryptionConfig::new(Arc::new(Keyring::parse(text).expect("parse")))
}

/// A standalone encrypted client over a fresh keyring.
fn secure() -> EncryptedObjectStorageClient {
    EncryptedObjectStorageClient::new(config_in())
}

fn url_of(dir: &TempDir, name: &str) -> String {
    format!("file://{}/{name}", dir.path().display())
}

async fn collect(client: &EncryptedObjectStorageClient, url: &str) -> Result<Vec<u8>> {
    drain(client.get_stream(url).await?).await
}

client_suite!(envelope, super::secure());

#[tokio::test]
async fn roundtrips_by_put_and_by_stream() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    for len in [0, 1, 65536, 3 * 65536 + 5] {
        let plain = pattern(len);

        let url = url_of(&dir, &format!("put{len}"));
        client.put(&url, plain.clone()).await?;
        assert_eq!(client.get(&url).await?.as_ref(), plain.as_slice());
        assert_eq!(collect(&client, &url).await?, plain);

        let url = url_of(&dir, &format!("stream{len}"));
        client
            .put_stream(&url, chunked(plain.clone().into(), 10_007))
            .await?;
        assert_eq!(client.get(&url).await?.as_ref(), plain.as_slice());
        assert_eq!(collect(&client, &url).await?, plain);
    }
    Ok(())
}

#[tokio::test]
async fn large_put_and_get_roundtrip() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = pattern(3 * 1024 * 1024 + 17);
    let url = url_of(&dir, "big");
    client.put(&url, plain.clone()).await?;
    assert_eq!(client.get(&url).await?.as_ref(), plain.as_slice());
    Ok(())
}

#[tokio::test]
async fn stored_bytes_are_an_esdk_message() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    for url in [url_of(&dir, "a"), url_of(&dir, "b")] {
        client.put(&url, &b"secret"[..]).await?;
    }
    client
        .put_stream(
            &url_of(&dir, "c"),
            chunked(Bytes::from_static(b"secret"), 2),
        )
        .await?;
    for name in ["a", "c"] {
        let stored = std::fs::read(dir.path().join(name))?;
        assert_eq!(stored[..3], [0x02, 0x04, 0x78]);
        assert!(!stored.windows(6).any(|w| w == b"secret"));
    }
    Ok(())
}

#[tokio::test]
async fn plain_view_reads_the_stored_bytes() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = client.without_encryption();
    let url = url_of(&dir, "a");
    client.put(&url, &b"hello"[..]).await?;
    let stored = plain.get(&url).await?;
    assert_eq!(
        stored.as_ref(),
        std::fs::read(dir.path().join("a"))?.as_slice()
    );
    assert_ne!(stored.as_ref(), b"hello");
    Ok(())
}

#[tokio::test]
async fn encrypted_view_refuses_plain_objects() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = client.without_encryption();
    for (name, body) in [("a", &b"just text, not encrypted"[..]), ("empty", b"")] {
        let url = url_of(&dir, name);
        plain.put(&url, body).await?;
        assert!(matches!(
            client.get(&url).await,
            Err(Error::NotEncrypted { .. })
        ));
        let mut stream = client.get_stream(&url).await?;
        assert!(matches!(
            stream.next().await,
            Some(Err(Error::NotEncrypted { .. }))
        ));
        assert!(stream.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn tampering_is_detected() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let url = url_of(&dir, "a");
    client.put(&url, pattern(200_000)).await?;

    let path = dir.path().join("a");
    let mut stored = std::fs::read(&path)?;
    let at = stored.len() / 2;
    stored[at] ^= 1;
    std::fs::write(&path, &stored)?;
    assert!(matches!(client.get(&url).await, Err(Error::Decryption(_))));
    assert!(collect(&client, &url).await.is_err());

    stored[at] ^= 1;
    stored.truncate(stored.len() - 40);
    std::fs::write(&path, &stored)?;
    assert!(matches!(client.get(&url).await, Err(Error::Decryption(_))));
    Ok(())
}

/// `get` (whole-object decrypt) and `get_stream` must fail the same way on
/// every kind of tampering, and never accept what the other rejects.
#[tokio::test]
async fn get_and_get_stream_fail_identically_on_tampered_objects() -> Result<()> {
    let dir = tempdir()?;
    let client = EncryptedObjectStorageClient::new(config_in().frame_length(4096)?);
    let url = url_of(&dir, "a");
    let plain = pattern(3 * 4096 + 77);
    client.put(&url, plain.clone()).await?;
    let path = dir.path().join("a");
    let stored = std::fs::read(&path)?;

    let mut cases: Vec<(String, Vec<u8>)> = Vec::new();
    for at in (0..stored.len()).step_by(61) {
        let mut m = stored.clone();
        m[at] ^= 0x10;
        cases.push((format!("flip {at}"), m));
    }
    for cut in (0..stored.len()).step_by(257) {
        cases.push((format!("cut {cut}"), stored[..cut].to_vec()));
    }
    let frame = 4096 + 32;
    cases.push(("drop final frame".into(), {
        let body_start = stored.len() - (3 * frame + 77 + 40);
        stored[..body_start + 3 * frame].to_vec()
    }));
    cases.push(("trailing byte".into(), [&stored[..], &[0]].concat()));
    cases.push(("doubled".into(), [&stored[..], &stored[..]].concat()));
    cases.push(("empty".into(), Vec::new()));

    for (what, bytes) in cases {
        std::fs::write(&path, &bytes)?;
        let got = client.get(&url).await.expect_err(&what);
        let streamed = collect(&client, &url).await.expect_err(&what);
        assert_eq!(got.to_string(), streamed.to_string(), "{what}");
    }
    std::fs::write(&path, &stored)?;
    assert_eq!(client.get(&url).await?.as_ref(), plain.as_slice());
    Ok(())
}

#[tokio::test]
async fn renamed_object_fails_the_binding() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    client.put(&url_of(&dir, "a"), &b"x"[..]).await?;
    std::fs::rename(dir.path().join("a"), dir.path().join("b"))?;
    assert!(matches!(
        client.get(&url_of(&dir, "b")).await,
        Err(Error::Decryption(DecryptError::BindingMismatch { .. }))
    ));

    let unbound = EncryptedObjectStorageClient::new(config_in().bind_object_key(false));
    unbound.put(&url_of(&dir, "c"), &b"x"[..]).await?;
    std::fs::rename(dir.path().join("c"), dir.path().join("d"))?;
    assert_eq!(unbound.get(&url_of(&dir, "d")).await?.as_ref(), b"x");

    // A binding reader refuses an object written without a binding.
    let cfg = config_in();
    let writer = EncryptedObjectStorageClient::new(cfg.clone().bind_object_key(false));
    writer.put(&url_of(&dir, "e"), &b"x"[..]).await?;
    let reader = EncryptedObjectStorageClient::new(cfg);
    assert!(matches!(
        reader.get(&url_of(&dir, "e")).await,
        Err(Error::Decryption(_))
    ));
    Ok(())
}

#[tokio::test]
async fn other_keyring_cannot_read() -> Result<()> {
    let dir = tempdir()?;
    let one = EncryptedObjectStorageClient::new(config_named("one"));
    let two = EncryptedObjectStorageClient::new(config_named("two"));
    let url = url_of(&dir, "a");
    one.put(&url, &b"x"[..]).await?;
    assert!(matches!(
        two.get(&url).await,
        Err(Error::Key(KeyError::UnknownKey { .. }))
    ));
    let mut stream = two.get_stream(&url).await?;
    assert!(matches!(
        stream.next().await,
        Some(Err(Error::Key(KeyError::UnknownKey { .. })))
    ));
    Ok(())
}

#[tokio::test]
async fn reserved_context_keys_fail_before_any_io() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    for key in ["osc:key", "osc:anything", "aws-crypto-public-key"] {
        let ctx = BTreeMap::from([(key.to_owned(), "x".to_owned())]);
        let url = url_of(&dir, "a");
        assert!(matches!(
            client.put_with_context(&url, &b"x"[..], &ctx).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            client
                .put_stream_with_context(&url, chunked(Bytes::from_static(b"x"), 1), ctx)
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert_eq!(std::fs::read_dir(dir.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn caller_context_is_accepted() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    client
        .put_with_context(&url_of(&dir, "a"), &b"x"[..], &ctx)
        .await?;
    client
        .put_stream_with_context(
            &url_of(&dir, "b"),
            chunked(Bytes::from_static(b"y"), 1),
            ctx,
        )
        .await?;
    assert_eq!(client.get(&url_of(&dir, "a")).await?.as_ref(), b"x");
    assert_eq!(client.get(&url_of(&dir, "b")).await?.as_ref(), b"y");
    Ok(())
}

#[tokio::test]
async fn views_share_one_registry() -> Result<()> {
    let dir = tempdir()?;
    let plain = ObjectStorageClient::new();
    let secure = plain.with_encryption(config_in());
    let url = ObjectStorageClient::parse_url(&url_of(&dir, "a"))?;

    let (a, _) = plain.get_store(&url)?;
    let (b, _) = secure.get_store(&url)?;
    let (c, _) = secure.without_encryption().get_store(&url)?;
    assert!(Arc::ptr_eq(&a, &b));
    assert!(Arc::ptr_eq(&a, &c));
    Ok(())
}

#[tokio::test]
async fn truncated_source_leaves_no_destination() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = client.without_encryption();
    client.put(&url_of(&dir, "src"), pattern(200_000)).await?;

    let path = dir.path().join("src");
    let mut stored = std::fs::read(&path)?;
    stored.truncate(stored.len() - 100);
    std::fs::write(&path, &stored)?;

    let source = client.get_stream(&url_of(&dir, "src")).await?;
    assert!(
        plain
            .put_stream(&url_of(&dir, "dst"), source)
            .await
            .is_err()
    );
    assert!(!plain.exists(&url_of(&dir, "dst")).await?);
    assert_eq!(std::fs::read_dir(dir.path())?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn unusual_object_keys_roundtrip() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    for name in ["with space", "100%25 raw%", "naïve/ключ/文件.txt"] {
        let url = url_of(&dir, name);
        client.put(&url, &b"data"[..]).await?;
        assert_eq!(client.get(&url).await?.as_ref(), b"data", "{name}");
        client
            .put_stream(&url, chunked(Bytes::from_static(b"more"), 2))
            .await?;
        assert_eq!(collect(&client, &url).await?, b"more", "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn binding_is_the_decoded_object_key() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let url = format!("file://{}/dir%20a/x%20%25%C3%BC.bin", dir.path().display());
    client.put(&url, &b"data"[..]).await?;

    let info = client.inspect_encryption(&url).await?;
    let key = format!(
        "{}/dir a/x %\u{fc}.bin",
        dir.path().display().to_string().trim_start_matches('/')
    );
    assert_eq!(info.context["osc:key"], key);
    assert_eq!(client.get(&url).await?.as_ref(), b"data");
    Ok(())
}

#[tokio::test]
async fn frame_length_is_validated() -> Result<()> {
    assert!(config_in().frame_length(1).is_err());
    assert!(config_in().frame_length(4096).is_ok());
    Ok(())
}

#[tokio::test]
async fn limits_bound_new_frames_and_untrusted_headers() -> Result<()> {
    let dir = tempdir()?;
    let url = url_of(&dir, "limited");
    let limits = DecodeLimits {
        max_frame_length: 8192,
        ..DecodeLimits::default()
    };
    let cfg = config_in();

    // The limits also bound the frame length chosen for new messages.
    assert!(
        cfg.clone()
            .limits(limits.clone())
            .frame_length(16384)
            .is_err()
    );
    assert!(
        cfg.clone()
            .limits(limits.clone())
            .frame_length(8192)
            .is_ok()
    );

    // A message within the default limits is refused by a reader that allows
    // smaller frames only.
    let writer = EncryptedObjectStorageClient::new(cfg.clone());
    writer.put(&url, Bytes::from(pattern(10_000))).await?;
    assert_eq!(writer.get(&url).await?.len(), 10_000);
    let strict = EncryptedObjectStorageClient::new(cfg.limits(limits));
    assert!(matches!(
        strict.get(&url).await,
        Err(Error::Decryption(DecryptError::Format(_)))
    ));
    Ok(())
}

#[tokio::test]
async fn metadata_reports_plaintext_and_stored_sizes() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    for len in [0usize, 1, 65536, 200_000] {
        let url = url_of(&dir, &format!("o{len}"));
        client.put(&url, pattern(len)).await?;
        let meta = client.get_object_metadata(&url).await?;
        let stored = std::fs::metadata(dir.path().join(format!("o{len}")))?.len();
        assert_eq!(meta.size_bytes, len as u64, "{len}");
        assert_eq!(meta.stored_size_bytes, stored, "{len}");
        assert!(meta.stored_size_bytes > meta.size_bytes);
        let info = meta.encryption.expect("encryption info");
        assert_eq!(info.format, "osc-esdk2-0478-v1");
    }
    Ok(())
}

#[tokio::test]
async fn metadata_of_plain_objects_is_not_encrypted() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = client.without_encryption();
    for (name, body) in [("a", &b"plain text object"[..]), ("empty", b"")] {
        let url = url_of(&dir, name);
        plain.put(&url, body).await?;
        assert!(matches!(
            client.get_object_metadata(&url).await,
            Err(Error::NotEncrypted { .. })
        ));
        assert!(matches!(
            client.inspect_encryption(&url).await,
            Err(Error::NotEncrypted { .. })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn inspect_encryption_reports_the_header() -> Result<()> {
    let dir = tempdir()?;
    let provider = keyring_named("kek");
    let key_id = provider.status().active;
    let client = EncryptedObjectStorageClient::new(EncryptionConfig::new(Arc::new(provider)));
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let url = url_of(&dir, "a");
    client.put_with_context(&url, &b"x"[..], &ctx).await?;

    let info = client.inspect_encryption(&url).await?;
    assert_eq!(info.format, "osc-esdk2-0478-v1");
    assert_eq!(info.key_names, vec![key_id]);
    assert_eq!(info.frame_length, 65536);
    assert_eq!(info.message_id.len(), 64);
    assert!(info.message_id.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(info.context["tenant"], "acme");
    assert!(info.context["osc:key"].ends_with('a'));

    // Another client without the key can inspect, but not read.
    let other = EncryptedObjectStorageClient::new(config_named("other"));
    assert_eq!(other.inspect_encryption(&url).await?, info);
    Ok(())
}

#[tokio::test]
async fn copy_to_another_key_reencrypts_and_keeps_caller_context() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let (src, dst) = (url_of(&dir, "src"), url_of(&dir, "naïve ключ/文件"));
    let plain = pattern(200_000);
    client.put_with_context(&src, plain.clone(), &ctx).await?;

    client.copy(&src, &dst).await?;
    assert_eq!(client.get(&dst).await?.as_ref(), plain.as_slice());
    let (a, b) = (
        client.inspect_encryption(&src).await?,
        client.inspect_encryption(&dst).await?,
    );
    assert_ne!(a.message_id, b.message_id);
    let without_key = |c: &BTreeMap<String, String>| {
        let mut c = c.clone();
        c.remove("osc:key");
        c
    };
    assert_eq!(without_key(&a.context), without_key(&b.context));
    assert_eq!(b.context["tenant"], "acme");
    // The bound key is the decoded object path.
    assert_ne!(a.context["osc:key"], b.context["osc:key"]);
    assert!(
        b.context["osc:key"].ends_with("/na\u{ef}ve \u{43a}\u{43b}\u{44e}\u{447}/\u{6587}\u{4ef6}")
    );
    assert_eq!(client.get(&src).await?.as_ref(), plain.as_slice());
    Ok(())
}

#[tokio::test]
async fn copy_and_move_keep_a_large_caller_context() -> Result<()> {
    // The header outgrows the 8 KiB chunks the file store streams.
    let dir = tempdir()?;
    let client = secure();
    let ctx = BTreeMap::from([
        ("big".to_owned(), "x".repeat(10_000)),
        ("tenant".to_owned(), "acme".to_owned()),
    ]);
    let (src, copy, moved) = (
        url_of(&dir, "src"),
        url_of(&dir, "copy"),
        url_of(&dir, "moved"),
    );
    let plain = pattern(100_000);
    client.put_with_context(&src, plain.clone(), &ctx).await?;

    client.copy(&src, &copy).await?;
    client.move_object(&src, &moved).await?;
    assert!(!client.without_encryption().exists(&src).await?);
    for url in [&copy, &moved] {
        assert_eq!(client.get(url).await?.as_ref(), plain.as_slice());
        let mut got = client.inspect_encryption(url).await?.context;
        got.remove("osc:key");
        got.remove("aws-crypto-public-key");
        assert_eq!(got["big"].len(), 10_000);
        assert_eq!(got["tenant"], "acme");
    }
    Ok(())
}

#[tokio::test]
async fn move_without_binding_is_a_byte_move() -> Result<()> {
    let dir = tempdir()?;
    let client = EncryptedObjectStorageClient::new(config_in().bind_object_key(false));
    client.put(&url_of(&dir, "a"), &b"data"[..]).await?;
    let before = std::fs::read(dir.path().join("a"))?;
    client
        .move_object(&url_of(&dir, "a"), &url_of(&dir, "b"))
        .await?;
    assert!(!dir.path().join("a").exists());
    assert_eq!(std::fs::read(dir.path().join("b"))?, before);
    assert_eq!(client.get(&url_of(&dir, "b")).await?.as_ref(), b"data");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn unbound_move_through_a_symlinked_directory_keeps_the_object() -> Result<()> {
    let dir = tempdir()?;
    let client = EncryptedObjectStorageClient::new(config_in().bind_object_key(false));
    std::fs::create_dir(dir.path().join("real"))?;
    std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("link"))?;
    let (via_link, real) = (url_of(&dir, "link/a"), url_of(&dir, "real/a"));
    client.put(&via_link, &b"data"[..]).await?;
    client.move_object(&via_link, &real).await?;
    assert_eq!(client.get(&real).await?.as_ref(), b"data");
    Ok(())
}

#[tokio::test]
async fn tiny_objects_are_not_encrypted_objects() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = client.without_encryption();
    for (i, body) in [&[0x01u8][..], &[0x02, 0x04], &[0x55]]
        .into_iter()
        .enumerate()
    {
        let url = url_of(&dir, &format!("t{i}"));
        plain.put(&url, body.to_vec()).await?;
        assert!(matches!(
            client.get(&url).await,
            Err(Error::NotEncrypted { .. })
        ));
        let mut stream = client.get_stream(&url).await?;
        assert!(matches!(
            stream.next().await,
            Some(Err(Error::NotEncrypted { .. }))
        ));
        assert!(matches!(
            client.get_object_metadata(&url).await,
            Err(Error::NotEncrypted { .. })
        ));
        assert!(matches!(
            client.inspect_encryption(&url).await,
            Err(Error::NotEncrypted { .. })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn tampered_context_is_a_decryption_error() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let url = url_of(&dir, "a");
    client.put_with_context(&url, &b"x"[..], &ctx).await?;
    let path = dir.path().join("a");
    let mut stored = std::fs::read(&path)?;
    let at = stored
        .windows(4)
        .position(|w| w == b"acme")
        .expect("context value");
    stored[at] = b'b';
    std::fs::write(&path, &stored)?;
    assert!(matches!(client.get(&url).await, Err(Error::Decryption(_))));
    let mut stream = client.get_stream(&url).await?;
    assert!(matches!(
        stream.next().await,
        Some(Err(Error::Decryption(_)))
    ));
    Ok(())
}

#[tokio::test]
async fn a_tampered_stream_yields_a_verified_prefix_then_an_error() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let plain = pattern(5 * 65536);
    let url = url_of(&dir, "a");
    client.put(&url, plain.clone()).await?;
    let path = dir.path().join("a");
    let mut stored = std::fs::read(&path)?;
    let at = stored.len() - 100;
    stored[at] ^= 1;
    std::fs::write(&path, &stored)?;

    let mut stream = client.get_stream(&url).await?;
    let mut out = Vec::new();
    let mut failed = false;
    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => out.extend_from_slice(&chunk),
            Err(e) => {
                assert!(matches!(e, Error::Decryption(_)));
                failed = true;
                break;
            }
        }
    }
    assert!(failed);
    assert!(out.len() < plain.len());
    assert_eq!(out, plain[..out.len()]);
    assert!(stream.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn move_of_a_tampered_source_fails_and_keeps_the_source() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    client.put(&url_of(&dir, "src"), pattern(200_000)).await?;
    let path = dir.path().join("src");
    let mut stored = std::fs::read(&path)?;
    let at = stored.len() / 2;
    stored[at] ^= 1;
    std::fs::write(&path, &stored)?;

    let result = client
        .move_object(&url_of(&dir, "src"), &url_of(&dir, "dst"))
        .await;
    assert!(matches!(result, Err(Error::Decryption(_))));
    assert!(
        client
            .without_encryption()
            .exists(&url_of(&dir, "src"))
            .await?
    );
    assert!(
        !client
            .without_encryption()
            .exists(&url_of(&dir, "dst"))
            .await?
    );
    assert_eq!(std::fs::read_dir(dir.path())?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn copy_of_a_plain_object_is_refused() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    client
        .without_encryption()
        .put(&url_of(&dir, "src"), &b"plain text object"[..])
        .await?;
    let result = client
        .copy(&url_of(&dir, "src"), &url_of(&dir, "dst"))
        .await;
    assert!(matches!(result, Err(Error::NotEncrypted { .. })));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_views_concurrent_one_backend() -> Result<()> {
    let dir = tempdir()?;
    let plain = ObjectStorageClient::new();
    let secure = plain.with_encryption(config_in());
    let mut tasks = Vec::new();
    for i in 0..32 {
        let (plain, secure) = (plain.clone(), secure.clone());
        let url = url_of(&dir, &format!("obj{i}"));
        tasks.push(tokio::spawn(async move {
            if i % 2 == 0 {
                plain.put(&url, &b"plain"[..]).await?;
                assert_eq!(plain.get(&url).await?.as_ref(), b"plain");
            } else {
                secure.put(&url, &b"secure"[..]).await?;
                assert_eq!(secure.get(&url).await?.as_ref(), b"secure");
            }
            Result::Ok(())
        }));
    }
    for t in tasks {
        t.await.expect("task panicked")?;
    }
    assert_eq!(plain.backend_count(), 1);
    assert_eq!(secure.backend_count(), 1);
    // Separate registries would each hold their own backend instance.
    let root = url::Url::parse(&url_of(&dir, "obj0")).expect("url");
    assert!(Arc::ptr_eq(
        &plain.get_store(&root)?.0,
        &secure.get_store(&root)?.0
    ));
    Ok(())
}

/// Peak resident set size in KiB (`VmHWM`).
#[cfg(target_os = "linux")]
fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("procfs")
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .expect("VmHWM")
}

/// Run manually: `cargo test --all-features -- --ignored pipe_multi_gb`.
/// Needs about 4 GiB of disk under the target directory.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "writes 4 GiB; run manually"]
async fn pipe_multi_gb_bounded_memory() -> Result<()> {
    const CHUNK: usize = 1 << 20;
    const TOTAL: usize = 4096;
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR"))?;
    let client = secure();
    let url = url_of(&dir, "big");

    let source = futures::stream::iter((0..TOTAL).map(|i| {
        let fill = u8::try_from(i % 251).unwrap_or(0);
        Ok(Bytes::from(vec![fill; CHUNK]))
    }));
    client.put_stream(&url, source).await?;

    let mut want = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    for i in 0..TOTAL {
        want.update(&vec![u8::try_from(i % 251).unwrap_or(0); CHUNK]);
    }
    let mut got = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    let mut len = 0;
    let mut stream = client.get_stream(&url).await?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        len += chunk.len();
        got.update(&chunk);
    }
    assert_eq!(len, TOTAL * CHUNK);
    assert_eq!(got.finish().as_ref(), want.finish().as_ref());

    let peak = peak_rss_kib();
    eprintln!("peak RSS: {} MiB", peak / 1024);
    assert!(peak < 256 * 1024, "peak RSS {peak} KiB");
    Ok(())
}

/// Two names of one origin: the encrypted move streams the object onto itself
/// and must keep it, and say so.
#[tokio::test]
async fn move_between_two_names_of_one_origin_keeps_the_encrypted_object() -> Result<()> {
    let server = crate::client::mini_http::Server::start().await;
    let client = secure();
    let (ip, name) = (
        format!("http://127.0.0.1:{}/x", server.port),
        format!("http://localhost:{}/x", server.port),
    );
    client.put(&ip, &b"data"[..]).await?;

    let err = client
        .move_object(&ip, &name)
        .await
        .expect_err("source changed");
    assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
    assert_eq!(client.get(&ip).await?.as_ref(), b"data");
    Ok(())
}

#[tokio::test]
async fn copy_onto_itself_does_not_rewrite_an_old_key_object() -> Result<()> {
    let dir = tempdir()?;
    let today = NaiveDate::from_ymd_opt(2026, 10, 26).expect("date");
    let first = new_keyring("old", today).expect("keyring");
    let old = EncryptedObjectStorageClient::new(config_for(&first));
    let url = url_of(&dir, "x");
    old.put(&url, &b"data"[..]).await?;
    let before = std::fs::read(dir.path().join("x"))?;
    let key_before = old.inspect_encryption(&url).await?.key_names;

    // The new keyring has a different active key and still holds the old one.
    let both = rotate_keyring(&first, "new", today).expect("rotate");
    let new = EncryptedObjectStorageClient::new(config_for(&both));
    new.copy(&url, &url).await?;

    assert_eq!(std::fs::read(dir.path().join("x"))?, before);
    assert_eq!(new.inspect_encryption(&url).await?.key_names, key_before);
    Ok(())
}

/// An old-key source is re-encrypted on a move; through another name of the
/// same origin the source is kept too.
#[tokio::test]
async fn reencrypting_move_between_two_names_of_one_origin_keeps_the_object() -> Result<()> {
    let server = crate::client::mini_http::Server::start().await;
    let today = NaiveDate::from_ymd_opt(2026, 10, 26).expect("date");
    let first = new_keyring("old", today).expect("keyring");
    let old = EncryptedObjectStorageClient::new(config_for(&first));
    let (ip, name) = (
        format!("http://127.0.0.1:{}/x", server.port),
        format!("http://localhost:{}/x", server.port),
    );
    old.put(&ip, &b"data"[..]).await?;
    // The new keyring still holds the old key.
    let both = rotate_keyring(&first, "new", today).expect("rotate");
    let new = EncryptedObjectStorageClient::new(config_for(&both));

    let err = new
        .move_object(&ip, &name)
        .await
        .expect_err("source changed");
    assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
    assert!(server.contents("/x").is_some());
    Ok(())
}

fn region(value: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("region".to_owned(), value.to_owned())])
}

/// Two stores holding the same-named object, written under {region: eu} and
/// {region: us} by one keyring. The binding is off, as it is when the same
/// path exists in two buckets; the objects are then swapped.
async fn swapped_pair() -> Result<(EncryptedObjectStorageClient, TempDir, String)> {
    let (eu, us) = (tempdir()?, tempdir()?);
    let client = EncryptedObjectStorageClient::new(config_in().bind_object_key(false));
    client
        .put_with_context(&url_of(&eu, "obj"), &b"eu data"[..], &region("eu"))
        .await?;
    client
        .put_with_context(&url_of(&us, "obj"), &b"us data"[..], &region("us"))
        .await?;
    // The us bytes replace the eu object.
    std::fs::copy(us.path().join("obj"), eu.path().join("obj"))?;
    let url = url_of(&eu, "obj");
    Ok((client, eu, url))
}

fn is_context_mismatch(e: &Error, key: &str) -> bool {
    matches!(e, Error::Decryption(DecryptError::ContextMismatch { key: k }) if k == key)
}

#[tokio::test]
async fn a_swapped_object_fails_a_required_context() -> Result<()> {
    let (client, _dir, url) = swapped_pair().await?;
    let err = client
        .get_with_context(&url, &region("eu"))
        .await
        .expect_err("swapped object");
    assert!(is_context_mismatch(&err, "region"), "{err}");
    // Neither the message nor the Debug output names a value.
    for text in [err.to_string(), format!("{err:?}")] {
        assert!(!text.contains("eu") && !text.contains("us"), "{text}");
    }

    let mut stream = client.get_stream_with_context(&url, &region("eu")).await?;
    let first = stream.next().await.expect("an item").expect_err("mismatch");
    assert!(is_context_mismatch(&first, "region"), "{first}");
    assert!(stream.next().await.is_none());

    // The matching context of the swapped-in object is what it is.
    assert_eq!(
        client.get_with_context(&url, &region("us")).await?.as_ref(),
        b"us data"
    );
    Ok(())
}

#[tokio::test]
async fn matching_and_subset_contexts_succeed_and_plain_reads_still_work() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let url = url_of(&dir, "obj");
    let stored = BTreeMap::from([
        ("region".to_owned(), "eu".to_owned()),
        ("tenant".to_owned(), "acme".to_owned()),
    ]);
    client.put_with_context(&url, &b"data"[..], &stored).await?;

    for required in [stored.clone(), region("eu"), BTreeMap::new()] {
        assert_eq!(
            client.get_with_context(&url, &required).await?.as_ref(),
            b"data"
        );
        let stream = client.get_stream_with_context(&url, &required).await?;
        assert_eq!(drain(stream).await?, b"data");
    }
    assert_eq!(client.get(&url).await?.as_ref(), b"data");

    // A pair that is absent, or has another value, fails.
    for required in [
        BTreeMap::from([("zone".to_owned(), "a".to_owned())]),
        BTreeMap::from([("region".to_owned(), "us".to_owned())]),
    ] {
        let err = client
            .get_with_context(&url, &required)
            .await
            .expect_err("mismatch");
        assert!(
            matches!(err, Error::Decryption(DecryptError::ContextMismatch { .. })),
            "{err}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_edited_context_value_that_matches_the_requirement_fails_authentication() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let url = url_of(&dir, "obj");
    client
        .put_with_context(&url, &b"data"[..], &region("us"))
        .await?;
    let path = dir.path().join("obj");
    let mut stored = std::fs::read(&path)?;
    let at = stored
        .windows(2)
        .position(|w| w == b"us")
        .expect("context value");
    stored[at..at + 2].copy_from_slice(b"eu");
    std::fs::write(&path, &stored)?;

    // The edited header satisfies the requirement, so the check passes and
    // authentication is what rejects it.
    let err = client
        .get_with_context(&url, &region("eu"))
        .await
        .expect_err("edited context");
    assert!(
        matches!(err, Error::Decryption(ref d) if !matches!(d, DecryptError::ContextMismatch { .. })),
        "{err}"
    );
    Ok(())
}

#[tokio::test]
async fn required_context_rejects_reserved_keys() -> Result<()> {
    let dir = tempdir()?;
    let client = secure();
    let url = url_of(&dir, "missing");
    for key in ["osc:key", "osc:anything", "aws-crypto-public-key"] {
        let required = BTreeMap::from([(key.to_owned(), "x".to_owned())]);
        // The object does not exist: only a check before any I/O gives
        // InvalidInput rather than NotFound.
        assert!(matches!(
            client.get_with_context(&url, &required).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            client.get_stream_with_context(&url, &required).await,
            Err(Error::InvalidInput(_))
        ));
    }
    Ok(())
}
