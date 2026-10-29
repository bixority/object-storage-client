use chrono::{TimeZone, Utc};
use http::Method;
use object_storage_client::Error;
use object_storage_client::sign::{SignMethod, SignOptions, presign_s3, presign_s3_at};
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

#[test]
fn an_unknown_method_is_invalid_input() {
    let err = "FETCH".parse::<SignMethod>().expect_err("unknown method");
    assert!(
        matches!(&err, Error::InvalidInput(m) if m.contains("FETCH") && m.contains("expected GET")),
        "{err:?}"
    );
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
        &SignOptions::default(),
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

// Known answers for header-bound pre-signing, computed by an independent
// signer: botocore's `S3SigV4QueryAuth` (any recent botocore), with the clock
// fixed at 2013-05-24T00:00:00Z. Generating snippet:
//
//     auth = S3SigV4QueryAuth(Credentials("AKIDEXAMPLE", SECRET, token),
//                             "s3", "us-east-1", expires=900)
//     req = AWSRequest(method="PUT", headers=HEADERS,
//                      url="https://s3.amazonaws.com/my-bucket/some/key.bin")
//     auth.add_auth(req)   # read X-Amz-Signature and X-Amz-SignedHeaders
//
// where `HEADERS` is the case's header set below.

/// Signs a PUT of `my-bucket/some/key.bin` at the fixed time and returns the
/// `(X-Amz-Signature, X-Amz-SignedHeaders)` values.
fn signed_put(
    options: &SignOptions,
    token: Option<&str>,
) -> std::result::Result<(String, String), Box<dyn std::error::Error>> {
    let mut cred = test_credential();
    cred.token = token.map(str::to_owned);
    let date = Utc
        .with_ymd_and_hms(2013, 5, 24, 0, 0, 0)
        .single()
        .ok_or("invalid test timestamp")?;
    let signed = presign_s3_at(
        &Url::parse("https://s3.amazonaws.com/my-bucket/some/key.bin")?,
        &Method::PUT,
        Duration::from_mins(15),
        &cred,
        "us-east-1",
        options,
        date,
    );
    let get = |name: &str| {
        signed
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
            .ok_or_else(|| format!("missing {name}"))
    };
    Ok((get("X-Amz-Signature")?, get("X-Amz-SignedHeaders")?))
}

fn assert_signed(got: &(String, String), signature: &str, headers: &str) {
    assert_eq!((got.0.as_str(), got.1.as_str()), (signature, headers));
}

#[test]
fn signs_content_length_like_botocore() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let options = SignOptions {
        content_length: Some(1234),
        content_type: None,
    };
    assert_signed(
        &signed_put(&options, None)?,
        "77e50c3fd5991426704820b4ab6ffa83726c28c5b461bf9965bf6b2a0a20b089",
        "content-length;host",
    );
    Ok(())
}

#[test]
fn signs_content_type_like_botocore() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let options = SignOptions {
        content_length: None,
        content_type: Some("application/octet-stream".into()),
    };
    assert_signed(
        &signed_put(&options, None)?,
        "15c837f28a018726eef5e6b42e4605fb6feb56993de516850dda1b230e0af9a5",
        "content-type;host",
    );
    Ok(())
}

fn both() -> SignOptions {
    SignOptions {
        content_length: Some(1234),
        content_type: Some("application/octet-stream".into()),
    }
}

#[test]
fn signs_both_headers_like_botocore() -> std::result::Result<(), Box<dyn std::error::Error>> {
    assert_signed(
        &signed_put(&both(), None)?,
        "e555396cd8ac98b83d256008f0a638f020ace47ddbba7ec9ad0a0f6c246191c7",
        "content-length;content-type;host",
    );
    Ok(())
}

#[test]
fn signs_a_session_token_like_botocore() -> std::result::Result<(), Box<dyn std::error::Error>> {
    assert_signed(
        &signed_put(&both(), Some("session-token-value"))?,
        "8f225a40e454eb165e7f3b866b5ab8f443a8cdffc1f1aafde0462de4345756e3",
        "content-length;content-type;host",
    );
    Ok(())
}

#[test]
fn signs_a_padded_content_type_like_botocore() -> std::result::Result<(), Box<dyn std::error::Error>>
{
    // Surrounding and repeated spaces are normalised in the canonical header.
    let options = SignOptions {
        content_length: None,
        content_type: Some("  text/plain;   charset=utf-8  ".into()),
    };
    assert_signed(
        &signed_put(&options, None)?,
        "3cb7bd1b98b947415e4d685aabaf3f4a8e802dfc89e0ada53942fe78ec9c2a74",
        "content-type;host",
    );
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
        &SignOptions::default(),
    );

    let query = signed.query().ok_or("signed URL has no query string")?;
    assert!(query.contains("X-Amz-SignedHeaders=host"));
    Ok(())
}
