//! GCS integration tests against a fake-gcs-server container (see
//! `common::gcs`). They need a container runtime (docker or podman); run with
//! `cargo test --test integration -- --ignored`.
//!
//! Emulator limitations (details and evidence in `common::gcs`):
//! - Writes through `object_store` (`put`, `put_stream` single and multipart,
//!   `copy`, `move_object`) are rejected by fake-gcs-server with
//!   `400 invalid uploadType`, because `object_store` uses the XML API for
//!   them; `test_gcs_xml_api_writes_are_rejected_by_the_emulator` pins that
//!   behaviour, so it fails (and these tests can be extended) once the
//!   emulator supports it. Objects are seeded through the JSON upload API.
//! - Pre-signed URLs are not tested: `object_store` signs GCS URLs with a real
//!   RSA service-account key, and the emulator setup uses an empty one
//!   (`disable_oauth`), so there is nothing to sign or verify.

#[cfg(feature = "gui")]
use crate::common::assert_root_lists;
use crate::common::gcs::{gcs_client, seed};
use crate::common::unique_gcs_bucket;
use crate::support::TestResult;
use futures::StreamExt;
use object_storage_client::ObjectStorageClient;

/// Creates a fresh bucket and returns its URL (`gs://it-<unique>/`).
async fn new_bucket(client: &ObjectStorageClient) -> Result<String, Box<dyn std::error::Error>> {
    let url = unique_gcs_bucket();
    client.create_bucket(&url).await?;
    Ok(url)
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_create_bucket_and_exists() -> TestResult {
    let client = gcs_client();
    let url = unique_gcs_bucket();

    assert!(!client.bucket_exists(&url).await?, "must not exist yet");

    // Creating is idempotent.
    client.create_bucket(&url).await?;
    client.create_bucket(&url).await?;
    assert!(client.bucket_exists(&url).await?, "created bucket exists");
    assert!(!client.bucket_exists(&unique_gcs_bucket()).await?);

    let object = format!("{url}probe.txt");
    assert!(!client.exists(&object).await?);
    seed(&url, "probe.txt", "text/plain", "present").await?;
    assert!(client.exists(&object).await?);
    client.delete(&object).await?;
    assert!(!client.exists(&object).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_get_and_stream() -> TestResult {
    let client = gcs_client();
    let bucket = new_bucket(&client).await?;

    // Larger than any single read buffer, so the stream has several chunks.
    let data: Vec<u8> = (0..(5u32 << 20))
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    seed(&bucket, "big.bin", "application/octet-stream", data.clone()).await?;
    let url = format!("{bucket}big.bin");

    let back = client.get(&url).await?;
    assert!(back.as_ref() == data.as_slice(), "get content");

    let mut stream = client.get_stream(&url).await?;
    let mut got = Vec::new();
    while let Some(chunk) = stream.next().await {
        got.extend_from_slice(&chunk?);
    }
    assert!(got == data, "get_stream content");

    assert!(client.get(&format!("{bucket}missing")).await.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_list_delete_and_delete_by_prefix() -> TestResult {
    let client = gcs_client();
    let bucket = new_bucket(&client).await?;

    for name in ["a/1.txt", "a/2.txt", "b/3.txt"] {
        seed(&bucket, name, "text/plain", "x").await?;
    }

    let mut listed = client.list(&format!("{bucket}a/")).await?;
    listed.sort();
    assert_eq!(listed, ["1.txt", "2.txt"]);

    client.delete(&format!("{bucket}a/1.txt")).await?;
    assert!(!client.exists(&format!("{bucket}a/1.txt")).await?);
    assert!(client.exists(&format!("{bucket}a/2.txt")).await?);

    client.delete_by_prefix(&format!("{bucket}a/")).await?;
    let left = client.list(&format!("{bucket}a/")).await?;
    assert!(left.is_empty(), "left: {left:?}");
    assert!(client.exists(&format!("{bucket}b/3.txt")).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_metadata() -> TestResult {
    let client = gcs_client();
    let bucket = new_bucket(&client).await?;
    seed(&bucket, "meta.bin", "application/x-test", "data").await?;

    let meta = client
        .get_object_metadata(&format!("{bucket}meta.bin"))
        .await?;
    assert_eq!(meta.size_bytes, 4);
    assert_eq!(meta.content_type.as_deref(), Some("application/x-test"));
    assert!(meta.e_tag.is_some());
    Ok(())
}

/// A copy or move between two buckets is done server-side by `objects.rewrite`
/// (a streamed copy would need the XML API write the emulator rejects), keeps
/// the content type, and a move removes the source only afterwards.
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_copy_and_move_across_buckets() -> TestResult {
    let client = gcs_client();
    let (one, two) = (new_bucket(&client).await?, new_bucket(&client).await?);
    let data: Vec<u8> = (0..(3u32 << 20))
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    seed(&one, "dir/src.bin", "application/x-test", data.clone()).await?;
    let src = format!("{one}dir/src.bin");

    let copied = format!("{two}copied/a b.bin");
    client.copy(&src, &copied).await?;
    assert!(
        client.get(&copied).await?.as_ref() == data.as_slice(),
        "copied content"
    );
    assert!(client.exists(&src).await?, "copy keeps the source");
    let meta = client.get_object_metadata(&copied).await?;
    assert_eq!(meta.content_type.as_deref(), Some("application/x-test"));

    let moved = format!("{two}moved.bin");
    client.move_object(&src, &moved).await?;
    assert!(!client.exists(&src).await?, "move removes the source");
    assert!(
        client.get(&moved).await?.as_ref() == data.as_slice(),
        "moved content"
    );

    // A missing source fails and writes nothing.
    assert!(client.copy(&src, &format!("{two}never")).await.is_err());
    assert!(!client.exists(&format!("{two}never")).await?);
    Ok(())
}

/// Evidence for the limitation described in the module docs: when this fails
/// because writes succeed, the emulator gained XML API support and the
/// write-path tests (put, `put_stream`, multipart, copy, move) can be added.
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_xml_api_writes_are_rejected_by_the_emulator() -> TestResult {
    let client = gcs_client();
    let bucket = new_bucket(&client).await?;
    seed(&bucket, "src", "text/plain", "x").await?;

    for result in [
        client.put(&format!("{bucket}put"), &b"x"[..]).await,
        client
            .copy(&format!("{bucket}src"), &format!("{bucket}dst"))
            .await,
    ] {
        let err = result
            .expect_err("emulator rejects XML API writes")
            .to_string();
        assert!(
            err.contains("invalid uploadType"),
            "unexpected error: {err}"
        );
    }
    Ok(())
}

/// A provider root lists the project's buckets.
#[cfg(feature = "gui")]
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_gcs_provider_root_lists_buckets() -> TestResult {
    let client = gcs_client();
    let (a, b) = (new_bucket(&client).await?, new_bucket(&client).await?);
    assert_root_lists(&client, "gs://", &[&a, &b]).await
}
