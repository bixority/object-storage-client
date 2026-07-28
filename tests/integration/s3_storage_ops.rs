use object_storage_client::{ObjectStorageClient, SignMethod, SignOptions};
use std::time::Duration;

/// Reads the `S3_BUCKET` environment variable required by the functional tests,
/// returning a descriptive error (rather than panicking) when it is unset, and
/// trimming any trailing slash for consistent URL joining.
fn s3_bucket() -> Result<String, Box<dyn std::error::Error>> {
    let bucket = std::env::var("S3_BUCKET")
        .map_err(|_| "S3_BUCKET environment variable must be set to run this test")?;
    Ok(bucket.trim_end_matches('/').to_string())
}

/// This test is ignored by default because it requires S3 credentials and a bucket.
/// To run it, set the following environment variables:
/// S3_BUCKETL=your-bucket-name
/// `AWS_ACCESS_KEY_ID`=...
/// `AWS_SECRET_ACCESS_KEY`=...
/// `AWS_REGION`=...
/// Then run with: cargo test --test `s3_storage_ops` -- --ignored
#[tokio::test]
#[ignore = "integration"]
async fn test_s3_object_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = s3_bucket()?;
    let bucket_url = format!("s3://{bucket}/");

    let client = ObjectStorageClient::new();

    // 1. Create a file and put it to storage
    let file_url = format!("{bucket_url}test_file_s3.txt");
    let content = b"Hello, S3 Object Storage!";
    client.put(&file_url, &content[..]).await?;

    // 2. List it in bucket
    let list = client.list(&bucket_url).await?;
    assert!(
        list.iter().any(|item| item == "test_file_s3.txt"),
        "File should be in the list"
    );

    // 3. Move it
    let moved_file_url = format!("{bucket_url}moved_test_file_s3.txt");
    client.move_object(&file_url, &moved_file_url).await?;

    // 4. List again to check the movement
    let list_after_move = client.list(&bucket_url).await?;
    assert!(
        !list_after_move
            .iter()
            .any(|item| item == "test_file_s3.txt"),
        "Old file should NOT be in the list"
    );
    assert!(
        list_after_move
            .iter()
            .any(|item| item == "moved_test_file_s3.txt"),
        "Moved file should be in the list"
    );

    // 5. Delete
    client.delete(&moved_file_url).await?;

    // 6. List again to check the deletion
    let list_after_delete = client.list(&bucket_url).await?;
    assert!(
        !list_after_delete
            .iter()
            .any(|item| item == "moved_test_file_s3.txt"),
        "Deleted file should NOT be in the list"
    );

    Ok(())
}

/// Verifies bucket creation and object existence checks against a live S3
/// backend.
///
/// Set `S3_BUCKET` to a bucket name that does NOT yet exist; the test creates
/// it and writes/checks an object inside it. Requires the same credentials as
/// `test_s3_object_lifecycle`.
/// Run with: `cargo test --test s3_storage_ops -- --ignored`
#[tokio::test]
#[ignore = "integration"]
async fn test_s3_create_bucket_and_exists() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = s3_bucket()?;
    let bucket_url = format!("s3://{bucket}/");

    let client = ObjectStorageClient::new();

    // Creating the bucket should succeed, and be idempotent.
    client.create_bucket(&bucket_url).await?;
    client.create_bucket(&bucket_url).await?;

    // The freshly-created bucket should report as existing, while a clearly
    // bogus bucket name should not.
    assert!(
        client.bucket_exists(&bucket_url).await?,
        "created bucket should exist"
    );
    let missing_bucket_url = format!("s3://{bucket}-does-not-exist/");
    assert!(
        !client.bucket_exists(&missing_bucket_url).await?,
        "nonexistent bucket should not exist"
    );

    let file_url = format!("{bucket_url}exists_probe.txt");

    // A not-yet-written object does not exist.
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

/// Verifies that a pre-signed GET URL can be generated for S3 and used to
/// retrieve an object without supplying credentials directly.
///
/// Requires the same environment variables as `test_s3_object_lifecycle`.
/// Run with: `cargo test --test s3_storage_ops -- --ignored`
#[tokio::test]
#[ignore = "integration"]
async fn test_s3_presigned_url_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = s3_bucket()?;

    let client = ObjectStorageClient::new();
    let file_url = format!("s3://{bucket}/presign_test.txt");
    let content = b"presigned content";

    client.put(&file_url, &content[..]).await?;

    let signed = client
        .get_pre_signed_url(
            &file_url,
            SignMethod::Get,
            Duration::from_mins(5),
            &SignOptions::default(),
        )
        .await?;

    assert!(
        signed.contains("X-Amz-Signature"),
        "signed URL must contain a SigV4 signature, got: {signed}"
    );

    // The signed URL should be usable by any plain HTTP client without creds.
    let body = reqwest::get(&signed).await?.bytes().await?;
    assert_eq!(body.as_ref(), content);

    client.delete(&file_url).await?;

    Ok(())
}

/// Verifies that a pre-signed PUT URL with a bound `Content-Length` and
/// `Content-Type` can be used to upload directly, and that the object store
/// enforces those header values (rejecting a mismatched upload).
///
/// Requires the same environment variables as `test_s3_object_lifecycle`.
/// Run with: `cargo test --test s3_storage_ops -- --ignored`
#[tokio::test]
#[ignore = "integration"]
async fn test_s3_presigned_put_with_bound_headers() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = s3_bucket()?;

    let client = ObjectStorageClient::new();
    let file_url = format!("s3://{bucket}/presign_put_test.bin");
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
