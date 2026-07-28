use object_storage_client::{
    Error, ObjectStorageClient, Result, SignMethod, SignOptions,
};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use url::Url;

#[tokio::test]
async fn test_local_file_ops() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file_path = dir.path().join("test.txt");
    let url = format!("file://{}", file_path.display());

    let client = ObjectStorageClient::new();

    // Put
    let data = b"hello world";
    client.put(&url, &data[..]).await?;

    // Get
    let retrieved = client.get(&url).await?;
    assert_eq!(retrieved.as_ref(), data);

    // List (using directory URL)
    let dir_url = format!("file://{}", dir.path().display());
    let list = client.list(&dir_url).await?;
    assert!(list.iter().any(|p| p.ends_with("test.txt")));

    // Object metadata (size, location, content type) for an existing object.
    let stored = client.get_object_metadata(&url).await?;
    assert_eq!(stored.size_bytes, data.len() as u64);
    assert!(stored.location.ends_with("test.txt"));
    // LocalFileSystem reports no content type.
    assert_eq!(stored.content_type, None);

    // Delete
    client.delete(&url).await?;
    assert!(fs::metadata(&file_path).is_err());

    // Metadata for a missing object errors with NotFound.
    let err = client
        .get_object_metadata(&url)
        .await
        .expect_err("metadata for a missing object must error");
    assert!(matches!(
        err,
        Error::ObjectStore(object_store::Error::NotFound { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn test_exists() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file_path = dir.path().join("exists.txt");
    let url = format!("file://{}", file_path.display());

    let client = ObjectStorageClient::new();

    // Missing object reports false rather than erroring.
    assert!(!client.exists(&url).await?);

    client.put(&url, &b"data"[..]).await?;
    assert!(client.exists(&url).await?);

    client.delete(&url).await?;
    assert!(!client.exists(&url).await?);

    Ok(())
}

#[tokio::test]
async fn test_create_bucket_local() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let bucket_path = dir.path().join("new-bucket");
    let url = format!("file://{}", bucket_path.display());

    let client = ObjectStorageClient::new();

    client.create_bucket(&url).await?;
    assert!(bucket_path.is_dir());

    // Idempotent: creating an existing bucket succeeds.
    client.create_bucket(&url).await?;

    // Objects can be written into the freshly-created bucket.
    let obj_url = format!("file://{}", bucket_path.join("o.txt").display());
    client.put(&obj_url, &b"hi"[..]).await?;
    assert!(client.exists(&obj_url).await?);

    Ok(())
}

#[tokio::test]
async fn test_create_bucket_unsupported_scheme() {
    let client = ObjectStorageClient::new();
    let err = client
        .create_bucket("https://example.com/foo")
        .await
        .expect_err("https must not support bucket creation");

    assert!(matches!(err, Error::BucketCreationUnsupported(scheme) if scheme == "https"));
}

#[tokio::test]
async fn test_bucket_exists_local() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let bucket_path = dir.path().join("probe-bucket");
    let url = format!("file://{}", bucket_path.display());

    let client = ObjectStorageClient::new();

    // Missing bucket reports false rather than erroring.
    assert!(!client.bucket_exists(&url).await?);

    client.create_bucket(&url).await?;
    assert!(client.bucket_exists(&url).await?);

    // A plain file at the path is not a bucket.
    let file_path = dir.path().join("plain.txt");
    let file_url = format!("file://{}", file_path.display());
    client.put(&file_url, &b"hi"[..]).await?;
    assert!(!client.bucket_exists(&file_url).await?);

    Ok(())
}

#[tokio::test]
async fn test_bucket_exists_unsupported_scheme() {
    let client = ObjectStorageClient::new();
    let err = client
        .bucket_exists("https://example.com/foo")
        .await
        .expect_err("https must not support bucket existence checks");

    assert!(matches!(err, Error::BucketExistenceUnsupported(scheme) if scheme == "https"));
}

#[tokio::test]
async fn test_copy_move() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");
    let src_url = format!("file://{}", src_path.display());
    let dst_url = format!("file://{}", dst_path.display());

    let client = ObjectStorageClient::new();
    let data = b"copy move test";

    // Test Copy
    client.put(&src_url, &data[..]).await?;
    client.copy(&src_url, &dst_url).await?;
    assert_eq!(client.get(&dst_url).await?.as_ref(), data);
    assert_eq!(client.get(&src_url).await?.as_ref(), data);

    // Test Move
    let dst_url2 = format!("file://{}.2", dst_path.display());
    client.move_object(&dst_url, &dst_url2).await?;
    assert_eq!(client.get(&dst_url2).await?.as_ref(), data);
    Ok(())
}

#[tokio::test]
async fn test_store_caching() -> Result<()> {
    let client = ObjectStorageClient::new();
    let url = Url::parse("file:///tmp")?;

    let (store1, _) = client.get_store(&url)?;
    let (store2, _) = client.get_store(&url)?;

    // Check if both Arcs point to the same ObjectStore instance.
    // We can compare the raw pointers of the trait objects.
    assert!(Arc::ptr_eq(&store1, &store2));

    // Different URL but same store (scheme + host)
    let url3 = Url::parse("file:///other")?;
    let (store3, _) = client.get_store(&url3)?;
    assert!(Arc::ptr_eq(&store1, &store3));

    Ok(())
}

#[tokio::test]
async fn test_get_pre_signed_url_unsupported_for_local() {
    let client = ObjectStorageClient::new();
    let err = client
        .get_pre_signed_url(
            "file:///tmp/foo.txt",
            SignMethod::Get,
            Duration::from_secs(60),
            &SignOptions::default(),
        )
        .await
        .expect_err("local filesystem must not support signing");

    assert!(matches!(err, Error::SigningUnsupported(scheme) if scheme == "file"));
}

#[tokio::test]
async fn test_get_pre_signed_urls_rejects_mixed_backends() {
    let client = ObjectStorageClient::new();
    let err = client
        .get_pre_signed_urls(
            &["file:///tmp/a.txt", "s3://bucket/b.txt"],
            SignMethod::Get,
            Duration::from_secs(60),
            &SignOptions::default(),
        )
        .await
        .expect_err("mixed backends must be rejected");

    // The URLs resolve to different backends (file vs s3), which is
    // detected before any signing is attempted.
    assert!(matches!(err, Error::Generic(msg) if msg.contains("same backend")));
}

#[tokio::test]
async fn test_get_backend_s3_bucket_only() -> Result<()> {
    // A bucket-only S3 URL has an empty path; this used to panic on
    // `&url.path()[1..]`. It must resolve to the s3 backend with an empty
    // object key.
    let client = ObjectStorageClient::new();
    let url = Url::parse("s3://bucket")?;

    let (backend, path) = client.get_backend(&url)?;
    assert_eq!(backend.scheme(), "s3");
    assert_eq!(path.to_string(), "");

    Ok(())
}

#[tokio::test]
async fn test_get_backend_s3_bucket_and_path() -> Result<()> {
    // The host is the bucket and the leading `/` is stripped from the key.
    let client = ObjectStorageClient::new();
    let url = Url::parse("s3://bucket/path")?;

    let (backend, path) = client.get_backend(&url)?;
    assert_eq!(backend.scheme(), "s3");
    assert_eq!(path.to_string(), "path");

    Ok(())
}

#[tokio::test]
async fn test_get_backend_file_with_host_and_path() -> Result<()> {
    // For `file://tmp/path` the URL authority ("tmp") is parsed as the host
    // and "/path" as the path. The file backend keeps `url.path()` verbatim,
    // and `ObjectPath` normalises away the leading slash.
    let client = ObjectStorageClient::new();
    let url = Url::parse("file://tmp/path")?;

    let (backend, path) = client.get_backend(&url)?;
    assert_eq!(backend.scheme(), "file");
    assert_eq!(path.to_string(), "path");

    Ok(())
}

#[tokio::test]
async fn test_get_backend_s3_caches_by_bucket_host() -> Result<()> {
    // Bucket-only and keyed URLs share the (scheme, host) cache entry, so
    // the same backend instance is reused.
    let client = ObjectStorageClient::new();

    let (b1, _) = client.get_backend(&Url::parse("s3://bucket")?)?;
    let (b2, _) = client.get_backend(&Url::parse("s3://bucket/path")?)?;
    assert!(Arc::ptr_eq(&b1, &b2));

    Ok(())
}

#[tokio::test]
async fn test_clone_client() -> Result<()> {
    let client = ObjectStorageClient::new();
    let client_clone = client.clone();
    let url = Url::parse("file:///tmp")?;

    let (store1, _) = client.get_store(&url)?;
    let (store2, _) = client_clone.get_store(&url)?;

    // Cloned client should share the same stores.
    assert!(Arc::ptr_eq(&store1, &store2));

    Ok(())
}

#[tokio::test]
async fn test_relative_path_no_scheme() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();
    // Cargo.toml should exist in the project root
    assert!(client.exists("Cargo.toml").await?);
    Ok(())
}

#[tokio::test]
async fn test_absolute_path_no_scheme() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file_path = dir.path().join("abs.txt");
    let path_str = file_path.to_str().ok_or("tempdir path is valid UTF-8")?;

    let client = ObjectStorageClient::new();
    let data = b"abs path content";

    client.put(path_str, &data[..]).await?;
    assert!(client.exists(path_str).await?);

    let retrieved = client.get(path_str).await?;
    assert_eq!(retrieved.as_ref(), data);

    client.delete(path_str).await?;
    assert!(!client.exists(path_str).await?);

    Ok(())
}

#[tokio::test]
async fn test_tilde_expansion() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let home = homedir::my_home()?.ok_or("should be able to get home directory")?;

    // Test ~
    let url = ObjectStorageClient::parse_url("~")?;
    assert_eq!(url.scheme(), "file");
    assert_eq!(url, Url::from_file_path(&home).map_err(|()| "invalid home path")?);

    // Test ~/path
    let url = ObjectStorageClient::parse_url("~/test.txt")?;
    assert_eq!(url.scheme(), "file");
    assert_eq!(url, Url::from_file_path(home.join("test.txt")).map_err(|()| "invalid path")?);

    Ok(())
}
