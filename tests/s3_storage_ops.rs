use object_storage_client::ObjectStorageClient;

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
    client.put(&file_url, content.to_vec()).await?;

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
