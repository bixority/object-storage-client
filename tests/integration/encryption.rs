//! Client-side encryption against Garage (S3) and Azurite (Azure Blob),
//! including copies and moves between the two, and reads and copies out of
//! fake-gcs-server (GCS; see `common::gcs` for why it cannot be written to
//! through the client). They need a container runtime
//! (docker or podman); run with `cargo test --test integration -- --ignored`.

use crate::common::gcs::seed;
use crate::common::provider::{Provider, base, per_provider};
use crate::common::{
    ACCESS_KEY, BUCKET, REGION, SECRET_KEY, both_client, garage_endpoint, garage_vars,
    unique_gcs_bucket, unique_prefix,
};
use crate::support::{TestResult, chunked, count_errors_to_end, drain, pattern};
use bytes::Bytes;
use chrono::NaiveDate;
use object_storage_client::encryption::keys::KeyError;
use object_storage_client::encryption::{EncryptionConfig, Keyring, new_keyring, rotate_keyring};
use object_storage_client::sign::{SignOptions, presign_s3};
use object_storage_client::{Client, Error, ObjectStorageClient, Result};
use object_store::{MultipartUpload, ObjectStoreExt};
use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

pub type Secure = Client<object_storage_client::encryption::Envelope>;

const fn today() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 10, 26).expect("date")
}

/// The text of a keyring with one fresh key.
fn ring_text(prefix: &str) -> String {
    new_keyring(prefix, today()).expect("keyring").to_string()
}

fn config_for(text: &str) -> EncryptionConfig {
    EncryptionConfig::new(Arc::new(Keyring::parse(text).expect("parse")))
}

fn active_id(text: &str) -> String {
    Keyring::parse(text).expect("parse").status().active
}

/// An encrypting view over `plain` with a new keyring, and the active key ID.
pub fn secure(plain: &ObjectStorageClient) -> (Secure, String) {
    let text = ring_text("kek");
    (plain.with_encryption(config_for(&text)), active_id(&text))
}

async fn collect(client: &Secure, url: &str) -> Result<Vec<u8>> {
    drain(client.get_stream(url).await?).await
}

/// Overwrites the stored bytes of `url` with `edit(stored)`, bypassing encryption.
async fn tamper(plain: &ObjectStorageClient, url: &str, edit: impl FnOnce(&mut Vec<u8>)) {
    let mut stored = plain.get(url).await.expect("stored").to_vec();
    edit(&mut stored);
    plain.put(url, stored).await.expect("overwrite");
}

#[expect(
    clippy::ptr_arg,
    reason = "matches the `FnOnce(&mut Vec<u8>)` of `tamper`"
)]
fn flip_last(stored: &mut Vec<u8>) {
    *stored.last_mut().expect("non-empty") ^= 1;
}

fn without_key(ctx: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut c = ctx.clone();
    c.remove("osc:key");
    c
}

async fn roundtrips(p: Provider) -> TestResult {
    let plain = both_client();
    let (client, _) = secure(&plain);
    let base = base(&plain, p, "roundtrip").await?;
    for len in [0, 1, 65536, 20 << 20] {
        let body = pattern(len);

        let url = format!("{base}put{len}");
        client.put(&url, body.clone()).await?;
        assert_eq!(client.get(&url).await?.as_ref(), body.as_slice());
        assert_eq!(collect(&client, &url).await?, body);
        assert_eq!(plain.get(&url).await?[..3], [0x02, 0x04, 0x78]);

        // Above one part, put_stream takes the multipart path.
        let url = format!("{base}stream{len}");
        client
            .put_stream(&url, chunked(body.clone().into(), 1 << 20))
            .await?;
        assert_eq!(client.get(&url).await?.as_ref(), body.as_slice());
        assert_eq!(collect(&client, &url).await?, body);
        assert_eq!(plain.get(&url).await?[..3], [0x02, 0x04, 0x78]);
    }
    Ok(())
}
per_provider!(roundtrip, roundtrips);

async fn metadata_and_inspect(p: Provider) -> TestResult {
    let plain = both_client();
    let (client, id) = secure(&plain);
    let base = base(&plain, p, "meta").await?;
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let url = format!("{base}obj");
    let body = pattern(200_000);
    client.put_with_context(&url, body.clone(), &ctx).await?;

    let meta = client.get_object_metadata(&url).await?;
    assert_eq!(meta.size_bytes, body.len() as u64);
    assert!(meta.stored_size_bytes > meta.size_bytes);
    assert_eq!(meta.stored_size_bytes, plain.get(&url).await?.len() as u64);
    let info = meta.encryption.expect("encryption info");
    assert_eq!(info.key_names, [id]);
    assert_eq!(info.context["tenant"], "acme");

    assert_eq!(client.inspect_encryption(&url).await?, info);
    Ok(())
}
per_provider!(metadata, metadata_and_inspect);

async fn tampered_or_truncated(p: Provider) -> TestResult {
    let plain = both_client();
    let (client, _) = secure(&plain);
    let base = base(&plain, p, "tamper").await?;
    let body = pattern(100_000);

    let flipped = format!("{base}flipped");
    client.put(&flipped, body.clone()).await?;
    tamper(&plain, &flipped, flip_last).await;
    assert!(matches!(
        client.get(&flipped).await,
        Err(Error::Decryption(_))
    ));

    let cut = format!("{base}cut");
    client.put(&cut, body).await?;
    tamper(&plain, &cut, |s| s.truncate(s.len() - 20)).await;
    assert!(matches!(client.get(&cut).await, Err(Error::Decryption(_))));
    let streamed = collect(&client, &cut).await;
    assert!(
        matches!(streamed, Err(Error::Decryption(_))),
        "a truncated stream must end in a decryption error"
    );
    let errors = count_errors_to_end(client.get_stream(&cut).await?).await;
    assert_eq!(errors, 1, "a failed stream reports one error, then ends");
    Ok(())
}
per_provider!(tamper_detected, tampered_or_truncated);

async fn plain_and_foreign(p: Provider) -> TestResult {
    let plain = both_client();
    let (client, _) = secure(&plain);
    let base = base(&plain, p, "refuse").await?;

    let text = format!("{base}text");
    plain.put(&text, &b"just text, not encrypted"[..]).await?;
    assert!(matches!(
        client.get(&text).await,
        Err(Error::NotEncrypted { .. })
    ));

    let url = format!("{base}obj");
    client.put(&url, &b"secret"[..]).await?;
    let (foreign, _) = secure(&plain);
    // Both keyrings use the "kek" prefix; only the check value differs.
    assert!(matches!(
        foreign.get(&url).await,
        Err(Error::Key(KeyError::UnknownKey { .. }))
    ));
    Ok(())
}
per_provider!(refusals, plain_and_foreign);

/// Same-store copy and move on the active key are byte copies; copies from an
/// old key, or to another path with binding on, are re-encrypted.
async fn same_store_copy_and_move(p: Provider) -> TestResult {
    let plain = both_client();
    let base = base(&plain, p, "copy").await?;
    // The new keyring holds the old key too.
    let old_text = ring_text("kek-old");
    let both = rotate_keyring(&old_text, "kek-new", today()).expect("rotate");
    let new_id = active_id(&both);
    let old = plain.with_encryption(config_for(&old_text).bind_object_key(false));
    let new = plain.with_encryption(config_for(&both).bind_object_key(false));
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let body = pattern(300_000);

    // Active key: the stored bytes are copied unchanged.
    let (src, copy, moved) = (
        format!("{base}src"),
        format!("{base}copy"),
        format!("{base}moved"),
    );
    old.put_with_context(&src, body.clone(), &ctx).await?;
    let stored = plain.get(&src).await?;
    old.copy(&src, &copy).await?;
    assert_eq!(plain.get(&copy).await?, stored);
    old.move_object(&copy, &moved).await?;
    assert!(!plain.exists(&copy).await?);
    assert_eq!(plain.get(&moved).await?, stored);
    assert_eq!(old.get(&moved).await?.as_ref(), body.as_slice());

    // Old key: the other client decrypts and re-encrypts on its active key.
    let reenc = format!("{base}reenc");
    new.copy(&src, &reenc).await?;
    assert_ne!(plain.get(&reenc).await?, stored);
    let (a, b) = (
        old.inspect_encryption(&src).await?,
        new.inspect_encryption(&reenc).await?,
    );
    assert_ne!(a.message_id, b.message_id);
    assert_eq!(b.key_names, std::slice::from_ref(&new_id));
    assert_eq!(without_key(&a.context), without_key(&b.context));
    assert_eq!(new.get(&reenc).await?.as_ref(), body.as_slice());

    // Binding on: a copy to another path is a re-encrypt on the same key.
    let bound = plain.with_encryption(config_for(&both));
    let (bsrc, bdst) = (format!("{base}bsrc"), format!("{base}bdst"));
    bound.put_with_context(&bsrc, body.clone(), &ctx).await?;
    bound.copy(&bsrc, &bdst).await?;
    assert_ne!(plain.get(&bdst).await?, plain.get(&bsrc).await?);
    let info = bound.inspect_encryption(&bdst).await?;
    assert_eq!(info.context["tenant"], "acme");
    assert!(info.context["osc:key"].ends_with("bdst"));
    assert_eq!(bound.get(&bdst).await?.as_ref(), body.as_slice());
    Ok(())
}
per_provider!(same_store, same_store_copy_and_move);

/// Garage <-> Azurite copies and moves, byte-verified and re-encrypting.
async fn cross_provider(from: Provider, to: Provider) -> TestResult {
    let plain = both_client();
    let (src_base, dst_base) = (
        base(&plain, from, "xsrc").await?,
        base(&plain, to, "xdst").await?,
    );
    let text = ring_text("kek");
    let unbound = plain.with_encryption(config_for(&text).bind_object_key(false));
    let bound = plain.with_encryption(config_for(&text));
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let body = pattern(9 << 20);
    let url = |b: &str, n: &str| format!("{b}{n}");

    // Binding off: the verified stored bytes arrive unchanged.
    let src = url(&src_base, "src");
    unbound.put_with_context(&src, body.clone(), &ctx).await?;
    let stored = plain.get(&src).await?;
    let (copy, moved) = (url(&dst_base, "copy"), url(&dst_base, "moved"));
    unbound.copy(&src, &copy).await?;
    assert_eq!(plain.get(&copy).await?, stored);
    unbound.move_object(&src, &moved).await?;
    assert!(!plain.exists(&src).await?);
    assert_eq!(plain.get(&moved).await?, stored);
    assert_eq!(unbound.get(&moved).await?.as_ref(), body.as_slice());

    // Binding on: the destination path is part of the context, so re-encrypt.
    let (bsrc, bcopy, bmoved) = (
        url(&src_base, "bsrc"),
        url(&dst_base, "bcopy"),
        url(&dst_base, "bmoved"),
    );
    bound.put_with_context(&bsrc, body.clone(), &ctx).await?;
    let bstored = plain.get(&bsrc).await?;
    bound.copy(&bsrc, &bcopy).await?;
    assert_ne!(plain.get(&bcopy).await?, bstored);
    assert_eq!(bound.get(&bcopy).await?.as_ref(), body.as_slice());
    assert_eq!(
        bound.inspect_encryption(&bcopy).await?.context["tenant"],
        "acme"
    );
    bound.move_object(&bsrc, &bmoved).await?;
    assert!(!plain.exists(&bsrc).await?);
    assert_eq!(bound.get(&bmoved).await?.as_ref(), body.as_slice());
    Ok(())
}

async fn cross_provider_tampered(from: Provider, to: Provider) -> TestResult {
    let plain = both_client();
    let (src_base, dst_base) = (
        base(&plain, from, "xtsrc").await?,
        base(&plain, to, "xtdst").await?,
    );
    let text = ring_text("kek");
    let body = pattern(300_000);

    for (name, bind) in [("unbound", false), ("bound", true)] {
        let client = plain.with_encryption(config_for(&text).bind_object_key(bind));
        let src = format!("{src_base}{name}");
        let dst = format!("{dst_base}{name}");
        client.put(&src, body.clone()).await?;
        tamper(&plain, &src, flip_last).await;
        let tampered = plain.get(&src).await?;

        assert!(matches!(
            client.copy(&src, &dst).await,
            Err(Error::Decryption(_))
        ));
        assert!(!plain.exists(&dst).await?, "{name}: no destination");
        assert!(matches!(
            client.move_object(&src, &dst).await,
            Err(Error::Decryption(_))
        ));
        assert!(!plain.exists(&dst).await?, "{name}: no destination");
        assert_eq!(plain.get(&src).await?, tampered, "{name}: source kept");
    }
    Ok(())
}

macro_rules! cross_tests {
    ($($name:ident: $from:ident => $to:ident,)*) => {
        mod cross {
            use super::{Provider, TestResult};
            $(
                mod $name {
                    use super::{Provider, TestResult};

                    #[tokio::test]
                    #[ignore = "needs a container runtime (docker or podman)"]
                    async fn copy_and_move() -> TestResult {
                        super::super::cross_provider(Provider::$from, Provider::$to).await
                    }

                    #[tokio::test]
                    #[ignore = "needs a container runtime (docker or podman)"]
                    async fn tampered_source_is_not_copied() -> TestResult {
                        super::super::cross_provider_tampered(Provider::$from, Provider::$to)
                            .await
                    }
                }
            )*
        }
    };
}
cross_tests! {
    garage_to_azure: Garage => Azure,
    azure_to_garage: Azure => Garage,
}

/// A fresh GCS bucket (`gs://it-<unique>/`).
async fn gcs_bucket(plain: &ObjectStorageClient) -> Result<String> {
    let url = unique_gcs_bucket();
    plain.create_bucket(&url).await?;
    Ok(url)
}

/// Stores the exact bytes `src` holds into the GCS bucket as `name`, via the
/// emulator's JSON API (the emulator rejects the client's own writes).
async fn seed_from(plain: &ObjectStorageClient, src: &str, bucket: &str, name: &str) -> TestResult {
    seed(
        bucket,
        name,
        "application/octet-stream",
        plain.get(src).await?,
    )
    .await
}

/// Encrypted objects written to Garage (including a multipart one) and moved
/// byte for byte into GCS decrypt there, in full and streamed; tampering with
/// the GCS copy is a decryption error.
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gcs_read_encrypted_and_tampered() -> TestResult {
    let plain = both_client();
    let text = ring_text("kek");
    let id = active_id(&text);
    // Unbound: the ciphertext does not depend on where it is stored.
    let client = plain.with_encryption(config_for(&text).bind_object_key(false));
    let (garage, bucket) = (unique_prefix("gcsread"), gcs_bucket(&plain).await?);
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);

    for (name, len, multipart) in [
        ("empty", 0, false),
        ("small", 65536, false),
        ("big", 20 << 20, true),
    ] {
        let body = pattern(len);
        let src = format!("{garage}{name}");
        if multipart {
            client
                .put_stream(&src, chunked(body.clone().into(), 1 << 20))
                .await?;
        } else {
            client.put_with_context(&src, body.clone(), &ctx).await?;
        }
        seed_from(&plain, &src, &bucket, name).await?;

        let url = format!("{bucket}{name}");
        assert_eq!(plain.get(&url).await?[..3], [0x02, 0x04, 0x78]);
        assert_eq!(client.get(&url).await?.as_ref(), body.as_slice(), "{name}");
        assert_eq!(collect(&client, &url).await?, body, "{name} streamed");
        let meta = client.get_object_metadata(&url).await?;
        assert_eq!(meta.size_bytes, len as u64, "{name} plaintext size");
        assert_eq!(meta.stored_size_bytes, plain.get(&url).await?.len() as u64);
    }

    let info = client.inspect_encryption(&format!("{bucket}small")).await?;
    assert_eq!(info.key_names, [id]);
    assert_eq!(info.context["tenant"], "acme");

    // Tampered or truncated in GCS.
    let src = format!("{garage}small");
    let mut stored = plain.get(&src).await?.to_vec();
    flip_last(&mut stored);
    seed(&bucket, "flipped", "application/octet-stream", stored).await?;
    assert!(matches!(
        client.get(&format!("{bucket}flipped")).await,
        Err(Error::Decryption(_))
    ));
    let mut stored = plain.get(&src).await?.to_vec();
    stored.truncate(stored.len() - 20);
    seed(&bucket, "cut", "application/octet-stream", stored).await?;
    let cut = format!("{bucket}cut");
    assert!(matches!(client.get(&cut).await, Err(Error::Decryption(_))));
    let streamed = collect(&client, &cut).await;
    assert!(
        matches!(streamed, Err(Error::Decryption(_))),
        "a truncated stream must end in a decryption error"
    );
    let errors = count_errors_to_end(client.get_stream(&cut).await?).await;
    assert_eq!(errors, 1, "a failed stream reports one error, then ends");
    Ok(())
}

/// GCS -> Garage copy and move of encrypted objects: stored bytes arrive
/// unchanged, a move deletes the source, a tampered source is not copied.
/// (Garage -> GCS is not possible: the emulator rejects the client's writes.)
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gcs_to_garage_copy_and_move() -> TestResult {
    let plain = both_client();
    let text = ring_text("kek");
    let client = plain.with_encryption(config_for(&text).bind_object_key(false));
    let (garage, bucket) = (unique_prefix("gcsxfer"), gcs_bucket(&plain).await?);
    let body = pattern(9 << 20);

    let origin = format!("{garage}origin");
    client
        .put_stream(&origin, chunked(body.clone().into(), 1 << 20))
        .await?;
    let stored = plain.get(&origin).await?;
    seed_from(&plain, &origin, &bucket, "src").await?;
    let src = format!("{bucket}src");

    let (copy, moved) = (format!("{garage}copy"), format!("{garage}moved"));
    client.copy(&src, &copy).await?;
    assert_eq!(plain.get(&copy).await?, stored);
    assert!(plain.exists(&src).await?, "copy keeps the source");
    client.move_object(&src, &moved).await?;
    assert!(!plain.exists(&src).await?, "move removes the source");
    assert_eq!(plain.get(&moved).await?, stored);
    assert_eq!(client.get(&moved).await?.as_ref(), body.as_slice());

    // A tampered source is refused and nothing is written or deleted.
    let mut bad = stored.to_vec();
    flip_last(&mut bad);
    seed(&bucket, "bad", "application/octet-stream", bad.clone()).await?;
    let (bad_src, bad_dst) = (format!("{bucket}bad"), format!("{garage}bad"));
    assert!(matches!(
        client.copy(&bad_src, &bad_dst).await,
        Err(Error::Decryption(_))
    ));
    assert!(matches!(
        client.move_object(&bad_src, &bad_dst).await,
        Err(Error::Decryption(_))
    ));
    assert!(!plain.exists(&bad_dst).await?, "no destination");
    assert_eq!(plain.get(&bad_src).await?.as_ref(), bad.as_slice());
    Ok(())
}

/// Pending multipart uploads under `prefix`, via a pre-signed `ListMultipartUploads`.
async fn pending_uploads(prefix: &str) -> std::result::Result<String, Box<dyn std::error::Error>> {
    let mut base = url::Url::parse(&format!("{}/{BUCKET}", garage_endpoint()))?;
    base.query_pairs_mut()
        .append_pair("uploads", "")
        .append_pair("prefix", prefix);
    let cred = object_store::aws::AwsCredential {
        key_id: ACCESS_KEY.into(),
        secret_key: SECRET_KEY.into(),
        token: None,
    };
    let signed = presign_s3(
        &base,
        &http::Method::GET,
        Duration::from_secs(60),
        &cred,
        REGION,
        &SignOptions::default(),
    );
    let response = reqwest::get(signed).await?;
    assert!(response.status().is_success(), "{}", response.status());
    Ok(response.text().await?)
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn failed_multipart_upload_leaves_nothing_behind() -> TestResult {
    let plain = both_client();
    let (client, _) = secure(&plain);
    let base = unique_prefix("abort");
    let prefix = base.trim_start_matches(&format!("s3://{BUCKET}/"));
    let url = format!("{base}obj");

    // Three 4 MiB chunks pass the part size, then the input fails.
    let input = futures::stream::iter(vec![
        Ok(Bytes::from(pattern(4 << 20))),
        Ok(Bytes::from(pattern(4 << 20))),
        Ok(Bytes::from(pattern(4 << 20))),
        Err(Error::Generic("input failed".into())),
    ]);
    let err = client.put_stream(&url, input).await.expect_err("must fail");
    assert!(matches!(err, Error::Generic(m) if m == "input failed"));
    assert!(!plain.exists(&url).await?);

    // `put_stream` awaits the abort before returning the error, so one listing suffices.
    let listing = pending_uploads(prefix).await?;
    assert!(!listing.contains("<Upload>"), "pending upload: {listing}");

    // Guard against a listing that can never show anything: a started upload is visible.
    let (store, path) = plain.get_store(&url::Url::parse(&url)?)?;
    let mut upload = store.put_multipart(&path).await?;
    assert!(pending_uploads(prefix).await?.contains("<Upload>"));
    upload.abort().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn cli_encrypted_put_get_against_garage() -> TestResult {
    use std::os::unix::fs::PermissionsExt;
    let work = tempdir()?;
    let keys = tempdir()?;
    let path = keys.path().join("keyring");
    std::fs::write(&path, ring_text("kek"))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let body = pattern(3 << 20);
    let (src, dst) = (work.path().join("src.bin"), work.path().join("dst.bin"));
    std::fs::write(&src, &body)?;
    let url = format!("{}cli-obj", unique_prefix("cli"));

    let osc = |args: &[&str]| {
        let out = Command::new(env!("CARGO_BIN_EXE_osc"))
            .arg("--keyring")
            .arg(&path)
            .args(args)
            .env_remove("OSC_KEYRING_COMMAND")
            .envs(garage_vars())
            .output()
            .expect("run osc");
        assert!(
            out.status.success(),
            "osc {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    osc(&["put", src.to_str().expect("utf-8"), &url]);
    let stored = both_client().get(&url).await?;
    assert_eq!(stored[..3], [0x02, 0x04, 0x78]);
    assert_ne!(stored.as_ref(), body.as_slice());
    osc(&["get", &url, dst.to_str().expect("utf-8")]);
    assert_eq!(std::fs::read(&dst)?, body);
    Ok(())
}
