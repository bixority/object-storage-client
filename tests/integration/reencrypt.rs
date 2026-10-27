//! Re-encryption against Garage (S3) and Azurite (Azure Blob), and what each
//! backend does with a conditional put. They need a container runtime
//! (docker or podman); run with `cargo test --test integration -- --ignored`.

use crate::common::both_client;
use crate::common::provider::{Provider, base, per_provider};
use crate::support::{TestResult, chunked, pattern};
use bytes::Bytes;
use chrono::NaiveDate;
use object_storage_client::encryption::{EncryptionConfig, Envelope, Keyring};
use object_storage_client::encryption::{new_keyring, rotate_keyring};
use object_storage_client::{Client, ReencryptOptions, Reencrypted};
use object_store::{ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use std::collections::BTreeMap;
use std::sync::Arc;

type Secure = Client<Envelope>;

const fn day(d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 10, d).expect("date")
}

fn config_for(text: &str) -> EncryptionConfig {
    EncryptionConfig::new(Arc::new(Keyring::parse(text).expect("parse")))
}

fn active_id(text: &str) -> String {
    Keyring::parse(text).expect("parse").status().active
}

/// Clients under the old keyring and the rotated one, and their key IDs.
struct Rotation {
    old: Secure,
    new: Secure,
    old_id: String,
    new_id: String,
}

fn rotation(plain: &object_storage_client::ObjectStorageClient) -> Rotation {
    let old_text = new_keyring("old", day(26)).expect("keyring").to_string();
    let new_text = rotate_keyring(&old_text, "new", day(27))
        .expect("rotate")
        .to_string();
    Rotation {
        old: plain.with_encryption(config_for(&old_text)),
        new: plain.with_encryption(config_for(&new_text)),
        old_id: active_id(&old_text),
        new_id: active_id(&new_text),
    }
}

fn from(id: &str) -> ReencryptOptions {
    ReencryptOptions {
        from: id.to_owned(),
        before: None,
        dry_run: false,
    }
}

async fn rewrites_and_skips(p: Provider) -> TestResult {
    let plain = both_client();
    let r = rotation(&plain);
    let base = base(&plain, p, "reencrypt").await?;
    let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
    let opts = from(&r.old_id);

    let small = format!("{base}small");
    let small_body = pattern(100_000);
    r.old
        .put_with_context(&small, small_body.clone(), &ctx)
        .await?;
    // 20 MiB is stored with a multipart upload, and rewritten with one.
    let big = format!("{base}dir/big");
    let big_body = Bytes::from(pattern(20 << 20));
    r.old
        .put_stream_with_context(
            &big,
            chunked::<object_storage_client::Error>(big_body.clone(), 1 << 20),
            ctx.clone(),
        )
        .await?;
    let other = format!("{base}other");
    r.new.put(&other, &b"already new"[..]).await?;
    let text = format!("{base}text");
    plain.put(&text, &b"not encrypted"[..]).await?;

    assert_eq!(
        r.new.reencrypt(&text, &opts).await?,
        Reencrypted::NotEncrypted
    );
    assert_eq!(r.new.reencrypt(&other, &opts).await?, Reencrypted::Skipped);
    let dry = ReencryptOptions {
        dry_run: true,
        ..opts.clone()
    };
    assert_eq!(
        r.new.reencrypt(&small, &dry).await?,
        Reencrypted::WouldRewrite
    );
    assert_eq!(
        r.old.inspect_encryption(&small).await?.key_names,
        [r.old_id.as_str()]
    );
    assert!(r.new.reencrypt(&small, &from(&r.new_id)).await.is_err());

    for url in [&small, &big] {
        assert_eq!(
            r.new.reencrypt(url, &opts).await?,
            Reencrypted::Rewritten,
            "{url}"
        );
        assert_eq!(
            r.new.reencrypt(url, &opts).await?,
            Reencrypted::Skipped,
            "{url}"
        );
    }
    assert_eq!(r.new.get(&small).await?.as_ref(), small_body.as_slice());
    assert_eq!(r.new.get(&big).await?, big_body);

    // What a scan would show: only the new key is left, with the context.
    for name in r.new.list(&base).await? {
        let url = format!("{base}{name}");
        if url == text {
            continue;
        }
        let info = r.new.inspect_encryption(&url).await?;
        assert_eq!(info.key_names, [r.new_id.as_str()], "{url}");
    }
    let info = r.new.inspect_encryption(&big).await?;
    assert_eq!(info.context["tenant"], "acme");
    assert!(
        info.context["osc:key"].ends_with("dir/big"),
        "{:?}",
        info.context
    );
    // The old keyring can no longer read the rewritten object.
    assert!(r.old.get(&small).await.is_err());
    Ok(())
}
per_provider!(rewrites, rewrites_and_skips);

/// What a store does with a put conditional on a stale version, recorded per
/// backend. The expectations below are what was observed; `reencrypt` closes
/// the race itself by a HEAD re-check before the commit, so it does not rely
/// on them.
async fn conditional_put_probe(p: Provider) -> TestResult {
    let plain = both_client();
    let base = base(&plain, p, "probe").await?;
    let url = url::Url::parse(&format!("{base}probe"))?;
    let (store, path) = plain.get_store(&url)?;
    let behaviour = probe(&store, &path).await?;
    let expected = match p {
        Provider::Garage => PROBE_GARAGE,
        Provider::Azure => PROBE_AZURE,
    };
    assert_eq!(behaviour, expected);
    Ok(())
}
per_provider!(conditional_put, conditional_put_probe);

/// Puts, overwrites, then puts conditional on the first version: `"overwrote"`
/// if the store ignored the condition, `"precondition"` if it rejected it,
/// `"not-implemented"` if it cannot do conditional puts.
async fn probe(
    store: &Arc<dyn object_store::ObjectStore>,
    path: &object_store::path::Path,
) -> Result<&'static str, Box<dyn std::error::Error>> {
    store.put(path, Bytes::from_static(b"v1").into()).await?;
    let v1 = store.head(path).await?;
    store.put(path, Bytes::from_static(b"v2").into()).await?;
    let v2 = store.head(path).await?;
    assert_ne!(v1.e_tag, v2.e_tag, "an overwrite changes the e-tag");
    let stale = UpdateVersion {
        e_tag: v1.e_tag.clone(),
        version: v1.version.clone(),
    };
    let opts = PutOptions {
        mode: PutMode::Update(stale),
        ..PutOptions::default()
    };
    let got = store
        .put_opts(path, Bytes::from_static(b"v3").into(), opts)
        .await;
    let behaviour = match &got {
        Ok(_) => "overwrote",
        Err(object_store::Error::Precondition { .. }) => "precondition",
        Err(object_store::Error::NotImplemented { .. }) => "not-implemented",
        Err(_) => "error",
    };
    let now = store.get(path).await?.bytes().await?;
    eprintln!(
        "conditional put probe: e_tag={:?} version={:?} -> {behaviour} (stored {:?})",
        v1.e_tag.is_some(),
        v1.version.is_some(),
        String::from_utf8_lossy(&now)
    );
    let stored = if behaviour == "overwrote" {
        b"v3"
    } else {
        b"v2"
    };
    assert_eq!(now.as_ref(), stored, "{got:?}");
    Ok(behaviour)
}

/// `file://` cannot do conditional puts either way: `reencrypt` falls back
/// to its HEAD re-check (and the private staging writer's own check).
#[tokio::test]
async fn conditional_put_probe_file() -> TestResult {
    let dir = tempfile::tempdir()?;
    let url = url::Url::from_file_path(dir.path().join("probe")).map_err(|()| "path")?;
    let plain = object_storage_client::ObjectStorageClient::new();
    let (store, path) = plain.get_store(&url)?;
    assert_eq!(probe(&store, &path).await?, "not-implemented");
    Ok(())
}

/// Garage ignores `If-Match` on a PUT and overwrites; Azurite rejects the
/// stale version with 412.
const PROBE_GARAGE: &str = "overwrote";
const PROBE_AZURE: &str = "precondition";
