use object_storage_client::{ObjectStorageClient, SignMethod};
use std::time::Duration;

/// This test is ignored by default because it requires S3 credentials and a bucket.
/// To run it, set the following environment variables:
/// S3_BUCKETL=your-bucket-name
/// `AWS_ACCESS_KEY_ID`=...
/// `AWS_SECRET_ACCESS_KEY`=...
/// `AWS_REGION`=...
/// Then run with: cargo test --test `s3_storage_ops` -- --ignored
#[tokio::test]
#[ignore = "functional"]
async fn test_s3_object_lifecycle() -> anyhow::Result<()> {
    let bucket = std::env::var("S3_BUCKET")
        .expect("S3_BUCKET environment variable must be set to run this test");

    // Ensure bucket doesn't end with a slash for consistent joining
    let bucket = bucket.trim_end_matches('/');
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

/// Verifies that a pre-signed GET URL can be generated for S3 and used to
/// retrieve an object without supplying credentials directly.
///
/// Requires the same environment variables as `test_s3_object_lifecycle`.
/// Run with: `cargo test --test s3_storage_ops -- --ignored`
#[tokio::test]
#[ignore = "functional"]
async fn test_s3_presigned_url_roundtrip() -> anyhow::Result<()> {
    let bucket = std::env::var("S3_BUCKET")
        .expect("S3_BUCKET environment variable must be set to run this test");
    let bucket = bucket.trim_end_matches('/');

    let client = ObjectStorageClient::new();
    let file_url = format!("s3://{bucket}/presign_test.txt");
    let content = b"presigned content";

    client.put(&file_url, &content[..]).await?;

    let signed = client
        .get_pre_signed_url(&file_url, SignMethod::Get, Duration::from_secs(300))
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
