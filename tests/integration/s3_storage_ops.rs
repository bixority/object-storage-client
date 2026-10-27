//! S3-only integration tests against a Garage container (see `common`); the
//! operations shared with Azure are in `storage_ops`. They need a
//! container runtime (docker or podman); run with
//! `cargo test --test integration -- --ignored`.

use crate::common::{BUCKET, garage_client, unique_prefix};
use object_storage_client::{Error, SignMethod, SignOptions};
use std::time::Duration;

/// Pre-signing refuses inputs the URL parser would rewrite into another key or
/// the bucket root, instead of signing them.
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_s3_presign_rejects_rewritten_paths() -> Result<(), Box<dyn std::error::Error>> {
    let client = garage_client();
    let prefix = unique_prefix("presign-reject");
    for bad in [
        format!("{prefix}../other/x"),
        format!("{prefix}%2e%2e/other/x"),
        format!("s3://{BUCKET}"),
        format!("s3://{BUCKET}/"),
        format!("s3://{BUCKET}/.."),
        format!("{prefix}x?versionId=1"),
    ] {
        let err = client
            .get_pre_signed_url(
                &bad,
                SignMethod::Delete,
                Duration::from_mins(5),
                &SignOptions::default(),
            )
            .await
            .expect_err(&bad);
        assert!(matches!(err, Error::InvalidInput(_)), "{bad}: {err:?}");
    }
    Ok(())
}

/// Verifies that a pre-signed PUT URL with a bound `Content-Length` and
/// `Content-Type` can be used to upload directly, and that the object store
/// enforces those header values (rejecting a mismatched upload).
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_s3_presigned_put_with_bound_headers() -> Result<(), Box<dyn std::error::Error>> {
    let client = garage_client();
    let file_url = format!("{}presign_put_test.bin", unique_prefix("presign-put"));
    let content = b"exact-size-and-type payload";
    let content_type = "application/octet-stream";

    let options = SignOptions {
        content_length: Some(content.len() as u64),
        content_type: Some(content_type.to_string()),
    };

    let signed = client
        .get_pre_signed_url(&file_url, SignMethod::Put, Duration::from_mins(5), &options)
        .await?;

    assert!(signed.contains("X-Amz-SignedHeaders=content-length%3Bcontent-type%3Bhost"));

    let http = reqwest::Client::new();

    // Upload matching the bound headers must succeed.
    let ok = http
        .put(&signed)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(content.to_vec())
        .send()
        .await?;
    assert!(ok.status().is_success(), "matching upload should succeed");

    // A mismatched Content-Type must be rejected by the store.
    let rejected = http
        .put(&signed)
        .header(reqwest::header::CONTENT_TYPE, "text/plain")
        .body(content.to_vec())
        .send()
        .await?;
    assert!(
        rejected.status().is_client_error(),
        "mismatched content type must be rejected, got: {}",
        rejected.status()
    );

    // The matching upload should be retrievable.
    let body = client.get(&file_url).await?;
    assert_eq!(body.as_ref(), content);

    client.delete(&file_url).await?;

    Ok(())
}
