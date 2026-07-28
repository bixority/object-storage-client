use chrono::{TimeZone, Utc};
use http::Method;
use object_storage_client::sign::{BoundHeaders, SignMethod, presign_s3, presign_s3_at};
use object_store::aws::AwsCredential;
use std::time::Duration;
use url::Url;

#[test]
fn sign_method_from_str() -> std::result::Result<(), Box<dyn std::error::Error>> {
    assert_eq!("get".parse::<SignMethod>()?, SignMethod::Get);
    assert_eq!("PUT".parse::<SignMethod>()?, SignMethod::Put);
    assert_eq!("Delete".parse::<SignMethod>()?, SignMethod::Delete);
    assert!("PATCH".parse::<SignMethod>().is_err());
    Ok(())
}

/// Reproduces the canonical AWS `SigV4` "Example: GET Object" pre-signed URL
/// from the AWS documentation, pinning the signing timestamp so the
/// resulting signature can be compared against the published value. This
/// guards the whole canonical-request / string-to-sign / signing pipeline.
#[test]
fn matches_aws_documented_vector() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cred = AwsCredential {
        key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        token: None,
    };
    // Virtual-hosted style base URL, as in the AWS example.
    let base = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt")?;
    let date = Utc
        .with_ymd_and_hms(2013, 5, 24, 0, 0, 0)
        .single()
        .ok_or("invalid test timestamp")?;

    let signed = presign_s3_at(
        &base,
        &Method::GET,
        Duration::from_hours(24),
        &cred,
        "us-east-1",
        BoundHeaders::default(),
        date,
    );

    let signature = signed
        .query_pairs()
        .find(|(k, _)| k == "X-Amz-Signature")
        .map(|(_, v)| v.into_owned())
        .ok_or("missing X-Amz-Signature in signed URL")?;

    assert_eq!(
        signature,
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
    );
    Ok(())
}

fn test_credential() -> AwsCredential {
    AwsCredential {
        key_id: "AKIDEXAMPLE".to_string(),
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        token: None,
    }
}

#[test]
fn presign_binds_content_headers() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let base = Url::parse("https://s3.amazonaws.com/my-bucket/some/key.bin")?;
    let signed = presign_s3(
        &base,
        &Method::PUT,
        Duration::from_mins(15),
        &test_credential(),
        "us-east-1",
        BoundHeaders {
            content_length: Some(1234),
            content_type: Some("application/octet-stream"),
        },
    );

    let query = signed.query().ok_or("signed URL has no query string")?;
    assert!(signed.as_str().contains("X-Amz-Signature="));
    // Signed headers are sorted and include the bound content headers.
    assert!(query.contains("X-Amz-SignedHeaders=content-length%3Bcontent-type%3Bhost"));
    assert!(query.contains("X-Amz-Expires=900"));
    Ok(())
}

#[test]
fn presign_host_only_when_no_headers() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let base = Url::parse("https://s3.amazonaws.com/my-bucket/key")?;
    let signed = presign_s3(
        &base,
        &Method::GET,
        Duration::from_mins(1),
        &test_credential(),
        "eu-west-1",
        BoundHeaders::default(),
    );

    let query = signed.query().ok_or("signed URL has no query string")?;
    assert!(query.contains("X-Amz-SignedHeaders=host"));
    Ok(())
}

#[test]
fn security_token_is_included_when_present() -> std::result::Result<(), Box<dyn std::error::Error>>
{
    let mut cred = test_credential();
    cred.token = Some("session-token-value".to_string());
    let base = Url::parse("https://s3.amazonaws.com/bucket/key")?;
    let signed = presign_s3(
        &base,
        &Method::PUT,
        Duration::from_mins(5),
        &cred,
        "us-east-1",
        BoundHeaders {
            content_length: Some(10),
            content_type: None,
        },
    );

    assert!(
        signed
            .as_str()
            .contains("X-Amz-Security-Token=session-token-value")
    );
    Ok(())
}
