use object_storage_client::ObjectStorageClient;
use tempfile::tempdir;

#[tokio::test]
#[ignore = "functional"]
async fn test_object_lifecycle() -> anyhow::Result<()> {
    // Setup a temporary directory to act as our local storage "bucket"
    let tmp_dir = tempdir()?;
    let bucket_path = tmp_dir.path().to_string_lossy();
    let bucket_url = if cfg!(windows) {
        format!("file:///{}", bucket_path.replace('\\', "/"))
    } else {
        format!("file://{bucket_path}")
    };

    let client = ObjectStorageClient::new();

    // 1. Create a file and put it to storage
    let file_url = format!("{bucket_url}/test_file.txt");
    let content = b"Hello, Object Storage!";
    client.put(&file_url, content).await?;

    // 2. List it in bucket
    let list = client.list(&bucket_url).await?;
    assert!(
        list.iter().any(|item| item == "test_file.txt"),
        "File should be in the list"
    );

    // 3. Move it
    let moved_file_url = format!("{bucket_url}/moved_test_file.txt");
    client.move_object(&file_url, &moved_file_url).await?;

    // 4. List again to check the movement
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

    // 5. Delete
    client.delete(&moved_file_url).await?;

    // 6. List again to check the deletion
    let list_after_delete = client.list(&bucket_url).await?;
    assert!(
        !list_after_delete
            .iter()
            .any(|item| item == "moved_test_file.txt"),
        "Deleted file should NOT be in the list"
    );

    Ok(())
}
