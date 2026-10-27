//! Uploads a file of random bytes, remembers its SHA-256, downloads it back
//! and compares checksums, against Garage (S3) and Azurite (Azure Blob), for
//! the plain and the encrypting client, at 4 KiB, 4 MiB and 4 GiB. Upload and
//! download stream in chunks; neither holds the object in memory. GCS is
//! excluded: fake-gcs-server rejects the client's uploads (see `common::gcs`).
//!
//! The `_4gib` tests are local-only (CI skips them with `--skip _4gib`); run
//! them with `cargo test --all-features --test integration -- --ignored _4gib`.

use crate::common::both_client;
use crate::common::provider::{Provider, base};
use crate::support::TestResult;
use aws_lc_rs::digest::{Context, Digest, SHA256};
use aws_lc_rs::rand;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_storage_client::Result;
use object_storage_client::codec::ByteStream;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const CHUNK: usize = 1 << 20;

/// A temp file of `size` random bytes (on disk, not in `/tmp`) and its SHA-256.
fn random_file(size: u64) -> std::io::Result<(NamedTempFile, Digest)> {
    let mut file = NamedTempFile::new_in(env!("CARGO_TARGET_TMPDIR"))?;
    let mut hash = Context::new(&SHA256);
    let mut block = vec![0u8; CHUNK];
    let mut left = size;
    while left > 0 {
        let n = block.len().min(usize::try_from(left).unwrap_or(CHUNK));
        rand::fill(&mut block[..n]).expect("random bytes");
        hash.update(&block[..n]);
        file.write_all(&block[..n])?;
        left -= n as u64;
    }
    file.flush()?;
    Ok((file, hash.finish()))
}

/// The file's content as a stream of 1 MiB chunks.
async fn file_stream(path: &Path) -> std::io::Result<ByteStream> {
    let file = File::open(path).await?;
    Ok(futures::stream::try_unfold(file, |mut file| async move {
        let mut buf = vec![0u8; CHUNK];
        let mut filled = 0;
        while filled < CHUNK {
            let n = file.read(&mut buf[filled..]).await?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        buf.truncate(filled);
        Ok::<_, object_storage_client::Error>((filled > 0).then(|| (Bytes::from(buf), file)))
    })
    .boxed())
}

/// The SHA-256 and length of everything the stream yields.
async fn hash_stream(stream: ByteStream) -> Result<(Digest, u64)> {
    let (hash, len) = stream
        .try_fold(
            (Context::new(&SHA256), 0u64),
            |(mut hash, len), chunk| async move {
                hash.update(&chunk);
                Ok((hash, len + chunk.len() as u64))
            },
        )
        .await?;
    Ok((hash.finish(), len))
}

async fn roundtrip(p: Provider, encrypted: bool, size: u64) -> TestResult {
    let plain = both_client();
    #[cfg(feature = "encryption")]
    let (secure, _) = crate::encryption::secure(&plain);
    #[cfg(not(feature = "encryption"))]
    let secure = &plain; // `encrypted` is never set without the feature
    let url = format!("{}roundtrip", base(&plain, p, "file-roundtrip").await?);
    let (file, sent) = random_file(size)?;

    // Compute every result first, delete the object, only then assert.
    let result: Result<_> = async {
        let data = file_stream(file.path()).await?;
        let stored = if encrypted {
            secure.put_stream(&url, data).await?;
            secure.get_stream(&url).await?
        } else {
            plain.put_stream(&url, data).await?;
            plain.get_stream(&url).await?
        };
        let (got, len) = hash_stream(stored).await?;
        Ok((got, len, plain.get_object_metadata(&url).await?.size_bytes))
    }
    .await;
    let deleted = plain.delete(&url).await;

    let (got, len, stored_size) = result?;
    deleted?;
    assert_eq!(len, size, "downloaded length");
    assert_eq!(got.as_ref(), sent.as_ref(), "downloaded checksum");
    if encrypted {
        assert!(
            stored_size > size,
            "stored size {stored_size} not encrypted"
        );
    } else {
        assert_eq!(stored_size, size, "stored size");
    }
    Ok(())
}

/// Registers `<name>::{plain,encrypted}_{4kib,4mib,4gib}` tests on `$provider`.
macro_rules! per_size {
    ($name:ident, $provider:expr) => {
        mod $name {
            use super::{Provider, TestResult, roundtrip};

            per_size!(@test plain_4kib, $provider, false, 4 << 10);
            per_size!(@test plain_4mib, $provider, false, 4 << 20);
            per_size!(@test plain_4gib, $provider, false, 4 << 30);
            per_size!(@test #[cfg(feature = "encryption")] encrypted_4kib, $provider, true, 4 << 10);
            per_size!(@test #[cfg(feature = "encryption")] encrypted_4mib, $provider, true, 4 << 20);
            per_size!(@test #[cfg(feature = "encryption")] encrypted_4gib, $provider, true, 4 << 30);
        }
    };
    (@test $(#[$cfg:meta])? $test:ident, $provider:expr, $enc:literal, $size:expr) => {
        $(#[$cfg])?
        #[tokio::test]
        #[ignore = "needs a container runtime (docker or podman)"]
        async fn $test() -> TestResult {
            roundtrip($provider, $enc, $size).await
        }
    };
}

per_size!(garage, Provider::Garage);
per_size!(azure, Provider::Azure);
