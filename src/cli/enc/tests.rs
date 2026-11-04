use super::*;
use crate::encryption::{Keyring, new_keyring, rotate_keyring};
use async_trait::async_trait;
use chrono::NaiveDate;
use futures::stream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

/// An `InMemory` store that also lists the console folder marker `p/sub/` the
/// way S3 does: as `p/sub`, with size 0. The marker is not an addressable key.
#[derive(Debug, Default)]
struct MarkerStore {
    inner: InMemory,
    reads: Mutex<Vec<String>>,
}

impl std::fmt::Display for MarkerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MarkerStore")
    }
}

#[async_trait]
impl ObjectStore for MarkerStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.reads
            .lock()
            .expect("lock")
            .push(location.as_ref().to_owned());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        let marker = ObjectMeta {
            location: ObjectPath::from("p/sub"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 0,
            e_tag: None,
            version: None,
        };
        stream::iter([Ok(marker)])
            .chain(self.inner.list(prefix))
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 1, 1).expect("date")
}

fn write_ring(dir: &std::path::Path, text: &str) -> PathBuf {
    let path = dir.join("keyring");
    std::fs::write(&path, text).expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode");
    }
    path
}

/// A client over a `MarkerStore` holding `p/sub/x` encrypted under the old
/// key, and a keyring file that has been rotated since.
async fn fixture(dir: &std::path::Path) -> (ObjectStorageClient, Arc<MarkerStore>, KeyFixture) {
    let store = Arc::new(MarkerStore::default());
    let client = ObjectStorageClient::with_vars(std::collections::HashMap::new());
    client.preload_store("s3://b/", store.clone());

    let old_text = new_keyring("old", day()).expect("keyring");
    let old = Keyring::parse(&old_text).expect("parse");
    let old_id = old.status().active;
    let sealed = client.with_encryption(EncryptionConfig::new(Arc::new(old)));
    sealed
        .put("s3://b/p/sub/x", b"payload".to_vec())
        .await
        .expect("put");

    let new_text = rotate_keyring(&old_text, "new", day()).expect("rotate");
    let file = write_ring(dir, &new_text);
    (client, store, KeyFixture { file, old_id })
}

struct KeyFixture {
    file: PathBuf,
    old_id: String,
}

impl KeyFixture {
    fn source(&self) -> keyring::Source {
        keyring::Source::File(self.file.clone())
    }
}

#[tokio::test]
async fn scan_counts_a_listed_empty_marker_as_not_encrypted() {
    let dir = tempdir().expect("dir");
    let (client, store, _keys) = fixture(dir.path()).await;
    for json in [false, true] {
        let got = scan(&client, "s3://b/p/", 4, None, json).await;
        assert!(got.is_ok(), "json {json}: {got:?}");
    }
    // The marker was never read.
    assert!(
        !store
            .reads
            .lock()
            .expect("lock")
            .iter()
            .any(|p| p == "p/sub")
    );
}

#[tokio::test]
async fn reencrypt_counts_a_listed_empty_marker_as_not_encrypted() {
    let dir = tempdir().expect("dir");
    let (client, store, keys) = fixture(dir.path()).await;

    for dry_run in [true, false] {
        let opts = ReencryptOptions {
            from: keys.old_id.clone(),
            before: None,
            dry_run,
        };
        let got = reencrypt(&client, Some(keys.source()), "s3://b/p/", opts, 4).await;
        assert!(got.is_ok(), "dry_run {dry_run}: {got:?}");
    }
    assert!(
        !store
            .reads
            .lock()
            .expect("lock")
            .iter()
            .any(|p| p == "p/sub")
    );
}

#[tokio::test]
async fn a_large_object_takes_the_large_weight_only_when_it_is_rewritten() {
    use std::time::Duration;
    let dir = tempdir().expect("dir");
    let (client, _store, keys) = fixture(dir.path()).await;
    let ring = keyring::open(keys.source()).await.expect("ring");
    client
        .put("s3://b/plain", b"text".to_vec())
        .await
        .expect("put");
    let client = client.with_encryption(EncryptionConfig::new(ring));
    let large = 16 * 1024 * 1024;
    let jobs = 4;
    // Three of the four permits are taken, so only a one-permit call can run.
    let budget = Semaphore::new(jobs);
    let held = budget.acquire_many(3).await.expect("permits");
    let run = |url: &'static str| {
        let opts = ReencryptOptions {
            from: keys.old_id.clone(),
            before: None,
            dry_run: false,
        };
        let probe = ReencryptOptions {
            dry_run: true,
            ..opts.clone()
        };
        let (client, budget) = (&client, &budget);
        async move {
            let got = reencrypt_one(client, budget, url, large, &opts, &probe, jobs);
            tokio::time::timeout(Duration::from_millis(300), got).await
        }
    };

    // A plain object is decided on the probe alone.
    let got = run("s3://b/plain").await.expect("no large weight");
    assert_eq!(got.expect("outcome"), Reencrypted::NotEncrypted);

    // Under the `from` key: the rewrite waits for the large weight.
    assert!(run("s3://b/p/sub/x").await.is_err());
    drop(held);
    let got = run("s3://b/p/sub/x").await.expect("free");
    assert_eq!(got.expect("outcome"), Reencrypted::Rewritten);
}
