//! Storage operations on every provider that accepts writes: S3 (Garage) and
//! Azure (Azurite). They need a container runtime (docker or podman); run with
//! `cargo test --test integration -- --ignored`. GCS is read-only through the
//! emulator, see `gcs_storage_ops`.

use crate::common::provider::{Provider, base, per_provider};
use crate::common::{BUCKET, both_client, unique_bucket, unique_container};
use crate::support::{TestResult, chunked, drain, pattern};
use object_storage_client::{SignMethod, SignOptions};
use std::time::Duration;

async fn object_lifecycle(p: Provider) -> TestResult {
    let client = both_client();
    let bucket_url = base(&client, p, "lifecycle").await?;

    let file_url = format!("{bucket_url}test_file.txt");
    client
        .put(&file_url, &b"Hello, Object Storage!"[..])
        .await?;
    let list = client.list(&bucket_url).await?;
    assert!(
        list.iter().any(|item| item == "test_file.txt"),
        "File should be in the list"
    );

    let moved_file_url = format!("{bucket_url}moved_test_file.txt");
    client.move_object(&file_url, &moved_file_url).await?;
    let list_after_move = client.list(&bucket_url).await?;
    assert!(
        !list_after_move.iter().any(|item| item == "test_file.txt"),
        "Old file should NOT be in the list"
    );
    assert!(
        list_after_move
            .iter()
            .any(|item| item == "moved_test_file.txt"),
        "Moved file should be in the list"
    );

    client.delete(&moved_file_url).await?;
    let list_after_delete = client.list(&bucket_url).await?;
    assert!(
        !list_after_delete
            .iter()
            .any(|item| item == "moved_test_file.txt"),
        "Deleted file should NOT be in the list"
    );
    Ok(())
}
per_provider!(lifecycle, object_lifecycle);

/// Verifies bucket (container) creation and object existence checks.
async fn bucket_and_exists(p: Provider) -> TestResult {
    let client = both_client();
    let (bucket_url, missing_bucket_url) = match p {
        Provider::Garage => (unique_bucket(), format!("s3://{BUCKET}-does-not-exist/")),
        Provider::Azure => (unique_container(), unique_container()),
    };

    assert!(
        !client.bucket_exists(&bucket_url).await?,
        "bucket should not exist before creation"
    );

    // Creating the bucket should succeed, and be idempotent.
    client.create_bucket(&bucket_url).await?;
    client.create_bucket(&bucket_url).await?;
    assert!(
        client.bucket_exists(&bucket_url).await?,
        "created bucket should exist"
    );
    assert!(
        !client.bucket_exists(&missing_bucket_url).await?,
        "nonexistent bucket should not exist"
    );

    let file_url = format!("{}exists_probe.txt", base(&client, p, "exists").await?);
    assert!(
        !client.exists(&file_url).await?,
        "object should not exist yet"
    );
    client.put(&file_url, &b"present"[..]).await?;
    assert!(client.exists(&file_url).await?, "object should exist now");
    client.delete(&file_url).await?;
    assert!(
        !client.exists(&file_url).await?,
        "object should not exist after delete"
    );
    Ok(())
}
per_provider!(buckets, bucket_and_exists);

async fn put_get_and_stream(p: Provider) -> TestResult {
    let client = both_client();
    let base = base(&client, p, "put-get").await?;

    let small = format!("{base}small.txt");
    client.put(&small, &b"hello storage"[..]).await?;
    assert_eq!(client.get(&small).await?.as_ref(), b"hello storage");
    assert_eq!(
        drain(client.get_stream(&small).await?).await?,
        b"hello storage"
    );

    // put_stream: below one part (single put) and above it (multipart).
    for (name, size) in [
        ("stream-small.bin", 1024 * 1024),
        ("stream-big.bin", 20 << 20),
    ] {
        let url = format!("{base}{name}");
        let data = pattern(size);
        client
            .put_stream(&url, chunked(data.clone().into(), 1 << 20))
            .await?;
        let back = client.get(&url).await?;
        assert_eq!(back.len(), data.len(), "{name} size");
        assert!(back.as_ref() == data.as_slice(), "{name} content");
    }
    Ok(())
}
per_provider!(put_get, put_get_and_stream);

async fn list_and_delete_by_prefix(p: Provider) -> TestResult {
    let client = both_client();
    let base = base(&client, p, "prefix").await?;

    for name in ["a/1.txt", "a/2.txt", "b/3.txt"] {
        client.put(&format!("{base}{name}"), &b"x"[..]).await?;
    }
    let mut listed = client.list(&format!("{base}a/")).await?;
    listed.sort();
    assert_eq!(listed, ["1.txt", "2.txt"]);

    client.delete(&format!("{base}a/1.txt")).await?;
    assert!(!client.exists(&format!("{base}a/1.txt")).await?);
    assert!(client.exists(&format!("{base}a/2.txt")).await?);

    client.delete_by_prefix(&format!("{base}a/")).await?;
    let left = client.list(&format!("{base}a/")).await?;
    assert!(left.is_empty(), "left: {left:?}");
    assert!(client.exists(&format!("{base}b/3.txt")).await?);
    Ok(())
}
per_provider!(prefixes, list_and_delete_by_prefix);

async fn copy_and_move(p: Provider) -> TestResult {
    let client = both_client();
    let one = base(&client, p, "copy-one").await?;
    let two = base(&client, p, "copy-two").await?;

    let src = format!("{one}src.txt");
    client.put(&src, &b"payload"[..]).await?;

    // Within a bucket.
    let copied = format!("{one}copied.txt");
    client.copy(&src, &copied).await?;
    assert_eq!(client.get(&copied).await?.as_ref(), b"payload");
    assert!(client.exists(&src).await?, "copy keeps the source");

    // Across buckets.
    let cross = format!("{two}cross.txt");
    client.copy(&src, &cross).await?;
    assert_eq!(client.get(&cross).await?.as_ref(), b"payload");

    // Move within and across buckets removes the source.
    let moved = format!("{one}moved.txt");
    client.move_object(&copied, &moved).await?;
    assert!(!client.exists(&copied).await?);
    assert_eq!(client.get(&moved).await?.as_ref(), b"payload");

    let moved_across = format!("{two}moved.txt");
    client.move_object(&moved, &moved_across).await?;
    assert!(!client.exists(&moved).await?);
    assert_eq!(client.get(&moved_across).await?.as_ref(), b"payload");
    Ok(())
}
per_provider!(copies, copy_and_move);

/// A move of an object onto itself must keep the object: a store rename is
/// copy then delete, which would destroy it.
async fn move_onto_itself(p: Provider) -> TestResult {
    let client = both_client();
    let url = format!("{}same.txt", base(&client, p, "move-self").await?);
    client.put(&url, &b"keep me"[..]).await?;

    client.move_object(&url, &url).await?;

    assert_eq!(client.get(&url).await?.as_ref(), b"keep me");
    client.delete(&url).await?;
    Ok(())
}
per_provider!(move_self, move_onto_itself);

/// A pre-signed GET URL can be used to retrieve an object without credentials.
async fn presigned_get_roundtrip(p: Provider) -> TestResult {
    let client = both_client();
    let url = format!("{}presign.txt", base(&client, p, "presign").await?);
    client.put(&url, &b"presigned content"[..]).await?;

    let signed = client
        .get_pre_signed_url(
            &url,
            SignMethod::Get,
            Duration::from_mins(5),
            &SignOptions::default(),
        )
        .await?;
    let marker = match p {
        Provider::Garage => "X-Amz-Signature",
        Provider::Azure => "sig=",
    };
    assert!(
        signed.contains(marker),
        "signed URL must carry a signature: {signed}"
    );

    // The signed URL is usable by any plain HTTP client without credentials.
    let body = reqwest::get(&signed).await?.bytes().await?;
    assert_eq!(body.as_ref(), b"presigned content");
    Ok(())
}
per_provider!(presigned_get, presigned_get_roundtrip);
