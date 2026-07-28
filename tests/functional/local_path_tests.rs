use object_storage_client::ObjectStorageClient;
use tempfile::tempdir;

#[tokio::test]
async fn test_absolute_path_handling() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();
    let tmp = tempdir()?;
    let file_path = tmp.path().join("absolute_test.txt");
    let path_str = file_path.to_str().ok_or("Invalid path")?;

    let data = b"content for absolute path";
    client.put(path_str, data.to_vec()).await?;

    assert!(
        client.exists(path_str).await?,
        "File should exist at absolute path"
    );
    let recovered = client.get(path_str).await?;
    assert_eq!(recovered, data.as_slice(), "Recovered data should match");

    client.delete(path_str).await?;
    assert!(!client.exists(path_str).await?, "File should be deleted");

    Ok(())
}

#[tokio::test]
async fn test_relative_path_handling() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();

    // Verify that we can see Cargo.toml in the current directory (project root during tests)
    assert!(
        client.exists("Cargo.toml").await?,
        "Cargo.toml should be visible via relative path"
    );

    // We can also check a non-existent relative path
    assert!(
        !client.exists("non_existent_file_in_cwd.txt").await?,
        "Non-existent relative path should return false"
    );

    Ok(())
}

#[tokio::test]
async fn test_tilde_expansion_handling() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();

    // Test ~ expansion (should at least parse and check existence without error)
    // Note: client.exists("~") might return false because it's a directory and object_store::head
    // is typically for files. We just ensure it doesn't return an Error.
    let _ = client.exists("~").await?;

    // Test ~/ expansion for a non-existent file
    let non_existent = "~/this_file_should_not_exist_ever_12345.txt";
    assert!(
        !client.exists(non_existent).await?,
        "Non-existent tilde path should return false, not error"
    );

    Ok(())
}

#[tokio::test]
async fn test_mixed_local_paths() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();
    let tmp = tempdir()?;
    let file_name = "mixed_test.txt";
    let abs_path = tmp.path().join(file_name);
    let abs_path_str = abs_path.to_str().ok_or("Invalid path")?;

    let data = b"mixed path data";

    // Put using absolute path
    client.put(abs_path_str, data.to_vec()).await?;

    // Verify using file:// URL (manually constructed)
    let file_url = if cfg!(windows) {
        format!("file:///{}", abs_path_str.replace('\\', "/"))
    } else {
        format!("file://{}", abs_path_str)
    };
    assert!(
        client.exists(&file_url).await?,
        "Should be visible via file:// URL"
    );

    // Cleanup
    client.delete(abs_path_str).await?;

    Ok(())
}

#[tokio::test]
async fn test_single_letter_scheme_handling() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();

    // In our implementation, a single-letter scheme (like C: in Windows)
    // is treated as a local filesystem path.
    // On Linux, "c:/some/path" is technically a relative path if it doesn't start with /
    // but Url::parse might see "c" as a scheme.
    let result = client.exists("c:/non_existent_path_12345").await;

    match result {
        Ok(false) => {
            // Correct: it was treated as a local path and not found.
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            assert!(
                !err_str.contains("UnsupportedScheme(\"c\")"),
                "Single-letter scheme should be treated as local path, not unsupported scheme: {}",
                err_str
            );
        }
        Ok(true) => {
            // Unlikely but possible if such a file exists
        }
    }

    Ok(())
}
