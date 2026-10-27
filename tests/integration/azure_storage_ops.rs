//! Azure-only integration tests against an Azurite container (see
//! `common::azurite`); the operations shared with S3 are in `storage_ops`. They need a container runtime (docker or podman); run
//! with `cargo test --test integration -- --ignored`.

use crate::common::azurite::azurite_client;
use crate::common::provider::{Provider, base};
use crate::support::TestResult;
use object_storage_client::{SignMethod, SignOptions};
use std::time::Duration;

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn test_azure_metadata_content_type() -> TestResult {
    let client = azurite_client();
    let container = base(&client, Provider::Azure, "meta").await?;
    let url = format!("{container}meta.bin");

    // Binding headers into a pre-signed URL is S3-only and must be refused.
    let bound = SignOptions {
        content_length: Some(4),
        content_type: Some("application/x-test".into()),
    };
    assert!(
        client
            .get_pre_signed_url(&url, SignMethod::Put, Duration::from_mins(5), &bound)
            .await
            .is_err()
    );

    // Plain `put` sets no content type, so store one via an unbound SAS PUT.
    let signed = client
        .get_pre_signed_url(
            &url,
            SignMethod::Put,
            Duration::from_mins(5),
            &SignOptions::default(),
        )
        .await?;
    let resp = reqwest::Client::new()
        .put(&signed)
        .header("x-ms-blob-type", "BlockBlob")
        .header(reqwest::header::CONTENT_TYPE, "application/x-test")
        .body("data")
        .send()
        .await?;
    assert!(resp.status().is_success(), "signed PUT: {}", resp.status());

    let meta = client.get_object_metadata(&url).await?;
    assert_eq!(meta.size_bytes, 4);
    assert_eq!(meta.content_type.as_deref(), Some("application/x-test"));
    assert!(meta.e_tag.is_some());
    Ok(())
}
