use object_storage_client::{Error, ObjectStorageClient, Result, SignMethod, SignOptions};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use url::Url;

#[tokio::test]
async fn test_create_bucket_unsupported_scheme() {
    let client = ObjectStorageClient::new();
    let err = client
        .create_bucket("https://example.com/foo")
        .await
        .expect_err("https must not support bucket creation");

    assert!(matches!(err, Error::BucketCreationUnsupported(scheme) if scheme == "https"));
}

#[tokio::test]
async fn test_bucket_exists_unsupported_scheme() {
    let client = ObjectStorageClient::new();
    let err = client
        .bucket_exists("https://example.com/foo")
        .await
        .expect_err("https must not support bucket existence checks");

    assert!(matches!(err, Error::BucketExistenceUnsupported(scheme) if scheme == "https"));
}

#[tokio::test]
async fn test_store_caching() -> Result<()> {
    let client = ObjectStorageClient::new();
    let url = Url::parse("file:///tmp")?;

    let (store1, _) = client.get_store(&url)?;
    let (store2, _) = client.get_store(&url)?;

    // Check if both Arcs point to the same ObjectStore instance.
    // We can compare the raw pointers of the trait objects.
    assert!(Arc::ptr_eq(&store1, &store2));

    // Different URL but same store (scheme + host)
    let url3 = Url::parse("file:///other")?;
    let (store3, _) = client.get_store(&url3)?;
    assert!(Arc::ptr_eq(&store1, &store3));

    Ok(())
}

#[tokio::test]
async fn test_get_pre_signed_url_unsupported_for_local() {
    let client = ObjectStorageClient::new();
    let err = client
        .get_pre_signed_url(
            "file:///tmp/foo.txt",
            SignMethod::Get,
            Duration::from_mins(1),
            &SignOptions::default(),
        )
        .await
        .expect_err("local filesystem must not support signing");

    assert!(matches!(err, Error::SigningUnsupported(scheme) if scheme == "file"));
}

#[tokio::test]
async fn test_get_pre_signed_urls_rejects_mixed_backends() {
    let client = ObjectStorageClient::with_vars(HashMap::new());
    let err = client
        .get_pre_signed_urls(
            &["file:///tmp/a.txt", "s3://bucket/b.txt"],
            SignMethod::Get,
            Duration::from_mins(1),
            &SignOptions::default(),
        )
        .await
        .expect_err("mixed backends must be rejected");

    // The URLs resolve to different backends (file vs s3), which is
    // detected before any signing is attempted.
    assert!(matches!(err, Error::InvalidInput(msg) if msg.contains("same backend")));
}

#[tokio::test]
async fn test_get_pre_signed_urls_rejects_a_different_port_or_userinfo() {
    async fn sign(urls: &[&str]) -> Error {
        ObjectStorageClient::new()
            .get_pre_signed_urls(
                urls,
                SignMethod::Get,
                Duration::from_mins(1),
                &SignOptions::default(),
            )
            .await
            .expect_err("must be rejected")
    }
    for urls in [
        ["http://h:1/a", "http://h:2/b"],
        ["http://h/a", "https://h/b"],
    ] {
        let err = sign(&urls).await;
        assert!(
            matches!(&err, Error::InvalidInput(msg) if msg.contains("same backend")),
            "{urls:?}: {err:?}"
        );
    }
    // Userinfo is refused outright, before the origins are compared.
    for urls in [
        ["http://u@h/a", "http://h/b"],
        ["http://u:p@h/a", "http://u:q@h/b"],
    ] {
        let err = sign(&urls).await;
        assert!(matches!(err, Error::InvalidInput(_)), "{urls:?}: {err:?}");
    }
}

#[tokio::test]
async fn test_clone_client() -> Result<()> {
    let client = ObjectStorageClient::new();
    let client_clone = client.clone();
    let url = Url::parse("file:///tmp")?;

    let (store1, _) = client.get_store(&url)?;
    let (store2, _) = client_clone.get_store(&url)?;

    // Cloned client should share the same stores.
    assert!(Arc::ptr_eq(&store1, &store2));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_first_use_builds_one_backend() -> Result<()> {
    let url = Url::parse("file:///tmp")?;

    for _ in 0..50 {
        let client = ObjectStorageClient::new();

        let mut handles = Vec::with_capacity(64);
        for _ in 0..64 {
            let client = client.clone();
            let url = url.clone();
            handles.push(tokio::spawn(async move {
                let (store, _path) = client.get_store(&url)?;
                Ok::<_, Error>(store)
            }));
        }

        let mut first = None;
        for handle in handles {
            let store = handle.await.map_err(|e| Error::Generic(e.to_string()))??;
            match &first {
                None => first = Some(store),
                Some(f) => assert!(Arc::ptr_eq(f, &store)),
            }
        }
    }

    Ok(())
}

/// A tiny in-memory HTTP object server (GET, HEAD, PUT, DELETE) for tests of
/// aliased origins: `127.0.0.1` and `localhost` reach the same server.
pub mod mini_http {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    type Objects = Arc<Mutex<HashMap<String, (Vec<u8>, u64)>>>;

    pub struct Server {
        pub port: u16,
        objects: Objects,
    }

    impl Server {
        pub async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let port = listener.local_addr().expect("addr").port();
            let objects = Objects::default();
            let shared = Arc::clone(&objects);
            tokio::spawn(async move {
                let version = Arc::new(AtomicU64::new(0));
                loop {
                    let Ok((sock, _)) = listener.accept().await else {
                        return;
                    };
                    tokio::spawn(serve(sock, Arc::clone(&shared), Arc::clone(&version)));
                }
            });
            Self { port, objects }
        }

        pub fn contents(&self, path: &str) -> Option<Vec<u8>> {
            let objects = self.objects.lock().expect("lock");
            objects.get(path).map(|(body, _)| body.clone())
        }
    }

    async fn serve(sock: tokio::net::TcpStream, objects: Objects, version: Arc<AtomicU64>) {
        let (read, mut write) = sock.into_split();
        let mut read = BufReader::new(read);
        loop {
            let mut request = String::new();
            if read.read_line(&mut request).await.unwrap_or(0) == 0 {
                return;
            }
            let mut parts = request.split_whitespace();
            let (method, path) = (
                parts.next().unwrap_or_default().to_owned(),
                parts.next().unwrap_or_default().to_owned(),
            );
            let mut length = 0usize;
            let mut range: Option<(usize, usize)> = None;
            loop {
                let mut line = String::new();
                if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                if line.trim().is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap_or(0);
                    } else if name.eq_ignore_ascii_case("range") {
                        // Only `bytes=a-b` is understood.
                        range = value
                            .trim()
                            .strip_prefix("bytes=")
                            .and_then(|r| r.split_once('-'))
                            .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)));
                    }
                }
            }
            let mut body = vec![0u8; length];
            if read.read_exact(&mut body).await.is_err() {
                return;
            }
            let reply = {
                let mut objects = objects.lock().expect("lock");
                match method.as_str() {
                    "PUT" => {
                        let v = version.fetch_add(1, Ordering::SeqCst) + 1;
                        objects.insert(path, (body, v));
                        b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n".to_vec()
                    }
                    "DELETE" => {
                        objects.remove(&path);
                        b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()
                    }
                    "GET" | "HEAD" => match objects.get(&path) {
                        Some((data, v)) if method == "GET" && range.is_some() => {
                            let (from, to) = range.unwrap_or_default();
                            let from = from.min(data.len());
                            let to = to.min(data.len().saturating_sub(1)) + 1;
                            let part = &data[from..to.max(from)];
                            let mut head = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
                                 Content-Range: bytes {from}-{}/{}\r\nETag: \"v{v}\"\r\n\
                                 Last-Modified: Tue, 01 Jan 2030 00:00:00 GMT\r\n\r\n",
                                part.len(),
                                to.max(from + 1) - 1,
                                data.len()
                            )
                            .into_bytes();
                            head.extend_from_slice(part);
                            head
                        }
                        Some((data, v)) => {
                            let mut head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v{v}\"\r\n\
                                 Last-Modified: Tue, 01 Jan 2030 00:00:00 GMT\r\n\r\n",
                                data.len()
                            )
                            .into_bytes();
                            if method == "GET" {
                                head.extend_from_slice(data);
                            }
                            head
                        }
                        None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
                    },
                    _ => b"HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n\r\n".to_vec(),
                }
            };
            if write.write_all(&reply).await.is_err() {
                return;
            }
        }
    }
}

/// Two names of one origin cannot be told apart by URL; the move streams the
/// object onto itself and must then keep it, and say so.
#[tokio::test]
async fn move_between_two_names_of_one_origin_keeps_the_object() -> Result<()> {
    let server = mini_http::Server::start().await;
    let client = ObjectStorageClient::new();
    let (ip, name) = (
        format!("http://127.0.0.1:{}/x", server.port),
        format!("http://localhost:{}/x", server.port),
    );
    client.put(&ip, &b"data"[..]).await?;

    let err = client
        .move_object(&ip, &name)
        .await
        .expect_err("source changed");
    assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
    assert_eq!(server.contents("/x").as_deref(), Some(&b"data"[..]));
    assert_eq!(client.get(&ip).await?.as_ref(), b"data");
    Ok(())
}

/// A move across origins still deletes the source.
#[tokio::test]
async fn move_between_origins_deletes_the_source() -> Result<()> {
    let (one, two) = (
        mini_http::Server::start().await,
        mini_http::Server::start().await,
    );
    let client = ObjectStorageClient::new();
    let (from, to) = (
        format!("http://127.0.0.1:{}/x", one.port),
        format!("http://127.0.0.1:{}/y", two.port),
    );
    client.put(&from, &b"data"[..]).await?;

    client.move_object(&from, &to).await?;
    assert_eq!(one.contents("/x"), None);
    assert_eq!(two.contents("/y").as_deref(), Some(&b"data"[..]));
    Ok(())
}

/// `delete_by_prefix` on `file://` must not follow symlinks below the prefix.
#[cfg(unix)]
mod delete_by_prefix_local {
    use super::*;
    use std::os::unix::fs::symlink;

    fn file_url(path: &std::path::Path) -> String {
        format!("file://{}", path.display())
    }

    #[tokio::test]
    async fn does_not_follow_symlinks_out_of_the_prefix() -> Result<()> {
        let tmp = tempdir()?;
        let prefix = tmp.path().join("prefix");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(prefix.join("nested/deeper"))?;
        fs::create_dir_all(&outside)?;
        fs::write(outside.join("victim.txt"), b"keep")?;
        fs::write(tmp.path().join("outside_file.txt"), b"keep")?;
        fs::write(prefix.join("a.txt"), b"x")?;
        fs::write(prefix.join("nested/deeper/b.txt"), b"x")?;
        symlink(&outside, prefix.join("dirlink"))?;
        symlink(&outside, prefix.join("nested/deeper/dirlink"))?;
        symlink(tmp.path().join("outside_file.txt"), prefix.join("filelink"))?;
        symlink(tmp.path().join("missing"), prefix.join("brokenlink"))?;

        ObjectStorageClient::new()
            .delete_by_prefix(&file_url(&prefix))
            .await?;

        assert_eq!(fs::read(outside.join("victim.txt"))?, b"keep");
        assert_eq!(fs::read(tmp.path().join("outside_file.txt"))?, b"keep");
        for gone in [
            "a.txt",
            "nested/deeper/b.txt",
            "dirlink",
            "nested/deeper/dirlink",
            "filelink",
            "brokenlink",
        ] {
            assert!(
                fs::symlink_metadata(prefix.join(gone)).is_err(),
                "{gone} should be deleted"
            );
        }
        // Directories are left in place, as object_store's local store does.
        assert!(prefix.join("nested/deeper").is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn a_symlinked_prefix_is_resolved_as_named() -> Result<()> {
        let tmp = tempdir()?;
        let real = tmp.path().join("real");
        fs::create_dir(&real)?;
        fs::write(real.join("f.txt"), b"x")?;
        symlink(&real, tmp.path().join("alias"))?;

        ObjectStorageClient::new()
            .delete_by_prefix(&file_url(&tmp.path().join("alias")))
            .await?;
        assert!(!real.join("f.txt").exists());
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_file_prefix_is_ok() -> Result<()> {
        let tmp = tempdir()?;
        let client = ObjectStorageClient::new();
        client
            .delete_by_prefix(&file_url(&tmp.path().join("missing")))
            .await?;

        let file = tmp.path().join("plain.txt");
        fs::write(&file, b"x")?;
        client.delete_by_prefix(&file_url(&file)).await?;
        assert!(file.exists(), "a file is not a prefix; nothing is deleted");
        Ok(())
    }

    #[tokio::test]
    async fn only_deletes_segments_under_the_prefix() -> Result<()> {
        let tmp = tempdir()?;
        fs::create_dir(tmp.path().join("p"))?;
        fs::write(tmp.path().join("p/in.txt"), b"x")?;
        fs::write(tmp.path().join("pq.txt"), b"x")?;
        fs::create_dir(tmp.path().join("pdir"))?;
        fs::write(tmp.path().join("pdir/other.txt"), b"x")?;

        ObjectStorageClient::new()
            .delete_by_prefix(&file_url(&tmp.path().join("p")))
            .await?;
        assert!(!tmp.path().join("p/in.txt").exists());
        assert!(tmp.path().join("pq.txt").exists());
        assert!(tmp.path().join("pdir/other.txt").exists());
        Ok(())
    }

    /// Like `LocalFileSystem::list`, in-flight upload staging files
    /// (`name#<digits>`) are not touched; other `#` names are.
    #[tokio::test]
    async fn skips_upload_staging_files() -> Result<()> {
        let tmp = tempdir()?;
        let prefix = tmp.path().join("p");
        fs::create_dir(&prefix)?;
        fs::write(prefix.join("obj#1"), b"staging")?;
        fs::write(prefix.join("obj#abc"), b"object")?;
        fs::write(prefix.join("obj"), b"object")?;

        ObjectStorageClient::new()
            .delete_by_prefix(&file_url(&prefix))
            .await?;
        assert!(prefix.join("obj#1").exists());
        assert!(!prefix.join("obj#abc").exists());
        assert!(!prefix.join("obj").exists());
        Ok(())
    }
}

mod url_input {
    use super::*;
    use std::collections::HashMap;

    /// An S3 client whose endpoint refuses connections: a request that escapes
    /// the input check fails to connect and can never yield `InvalidInput`.
    fn client() -> ObjectStorageClient {
        ObjectStorageClient::with_vars(HashMap::from([
            ("S3_ENDPOINT".into(), "http://127.0.0.1:1".into()),
            ("S3_REGION".into(), "us-east-1".into()),
            ("S3_ACCESS_KEY_ID".into(), "AKIDEXAMPLE".into()),
            ("S3_SECRET_ACCESS_KEY".into(), "secret".into()),
            ("S3_ALLOW_HTTP".into(), "true".into()),
        ]))
    }

    fn assert_invalid<T: std::fmt::Debug>(what: &str, result: Result<T>) {
        let err = result.expect_err(what);
        assert!(matches!(err, Error::InvalidInput(_)), "{what}: {err:?}");
    }

    /// Runs every object operation on `url`.
    async fn object_calls(
        client: &ObjectStorageClient,
        url: &str,
    ) -> Vec<(&'static str, Result<()>)> {
        vec![
            ("put", client.put(url, &b"x"[..]).await),
            ("get", client.get(url).await.map(drop)),
            ("delete", client.delete(url).await),
            ("exists", client.exists(url).await.map(drop)),
            (
                "get_object_metadata",
                client.get_object_metadata(url).await.map(drop),
            ),
        ]
    }

    #[tokio::test]
    async fn queries_and_fragments_are_refused_everywhere() {
        let client = client();
        for url in ["s3://b/a#b", "s3://b/a?x", "s3://b/a?"] {
            for (what, result) in object_calls(&client, url).await {
                assert_invalid(&format!("{what} {url}"), result);
            }
        }
        assert_invalid("copy from", client.copy("s3://b/a#1", "s3://b/z").await);
        assert_invalid("copy to", client.copy("s3://b/a", "s3://b/z#1").await);
        assert_invalid(
            "move from",
            client.move_object("s3://b/a#1", "s3://b/z").await,
        );
        assert_invalid(
            "move to",
            client.move_object("s3://b/a", "s3://b/z#1").await,
        );
        let prefix = "s3://b/p?x";
        assert_invalid("list", client.list(prefix).await);
        assert_invalid("delete_by_prefix", client.delete_by_prefix(prefix).await);
        assert_invalid("create_bucket", client.create_bucket(prefix).await);
        assert_invalid("bucket_exists", client.bucket_exists(prefix).await);
    }

    #[tokio::test]
    async fn dot_segments_are_refused_off_file() {
        let client = client();
        assert_invalid(
            "delete_by_prefix",
            client.delete_by_prefix("s3://b/t/x/../..").await,
        );
        assert_invalid(
            "delete_by_prefix encoded",
            client.delete_by_prefix("s3://b/t/%2e%2e").await,
        );
        assert_invalid("get", client.get("s3://b/t/%2e%2e/u/k").await);
        assert_invalid("put", client.put("s3://b/a/./b", &b"x"[..]).await);
        assert_invalid("get http", client.get("http://127.0.0.1:1/a/../b").await);
        assert_invalid("copy", client.copy("s3://b/a", "s3://b/x/../y").await);
    }

    #[tokio::test]
    async fn bucket_only_urls_are_refused_for_object_calls() {
        let client = client();
        for url in ["s3://b", "s3://b/"] {
            for (what, result) in object_calls(&client, url).await {
                assert_invalid(&format!("{what} {url}"), result);
            }
            assert_invalid("copy from", client.copy(url, "s3://b/z").await);
            assert_invalid("copy to", client.copy("s3://b/a", url).await);
            assert_invalid("move from", client.move_object(url, "s3://b/z").await);
            assert_invalid("move to", client.move_object("s3://b/a", url).await);
            // Listing a bucket is not an object call: it reaches the endpoint.
            let err = client.list(url).await.expect_err("nothing listens");
            assert!(!matches!(err, Error::InvalidInput(_)), "{err:?}");
        }
    }

    /// A bucket or a prefix is not an object: the trailing `/` would be dropped
    /// from the key, so the call would address another object. The URL is
    /// refused before a backend is built, so no variables are needed.
    #[tokio::test]
    async fn urls_naming_a_bucket_or_prefix_are_refused_before_a_backend_is_built() {
        let client = ObjectStorageClient::with_vars(HashMap::new());
        for url in ["s3://b", "s3://b/", "s3://b/k/", "file:///tmp/dir/"] {
            for (what, result) in object_calls(&client, url).await {
                assert_invalid(&format!("{what} {url}"), result);
            }
            assert_invalid("copy from", client.copy(url, "s3://b/z").await);
            assert_invalid("copy to", client.copy("s3://b/a", url).await);
            assert_invalid("move from", client.move_object(url, "s3://b/z").await);
            assert_invalid("move to", client.move_object("s3://b/a", url).await);
            assert_invalid(
                "get_pre_signed_url",
                client
                    .get_pre_signed_url(
                        url,
                        SignMethod::Get,
                        Duration::from_mins(1),
                        &SignOptions::default(),
                    )
                    .await,
            );
        }
        assert_eq!(client.backend_count(), 0, "refused before any backend");

        // Prefix operations keep accepting the URL.
        let listing = client.list_stream("s3://b/");
        assert!(listing.is_ok(), "a prefix is listable: {:?}", listing.err());
    }

    #[tokio::test]
    async fn whitespace_the_parser_drops_is_refused() {
        let client = client();
        for url in [
            "s3://b/a ",
            " s3://b/a",
            "s3://b/a\tb",
            "s3://b/a\nb",
            "s3://b/a\r",
        ] {
            assert_invalid(url, client.get(url).await);
        }
    }

    #[tokio::test]
    async fn ordinary_keys_still_reach_the_store() {
        let client = client();
        for url in ["s3://b/a..b", "s3://b/a%2Fb"] {
            let err = client.get(url).await.expect_err("nothing listens");
            assert!(!matches!(err, Error::InvalidInput(_)), "{url}: {err:?}");
        }
    }

    #[tokio::test]
    async fn userinfo_is_refused() {
        let client = client();
        for url in [
            "http://u:p4ss@127.0.0.1:1/x",
            "http://u@127.0.0.1:1/x",
            "http://:p4ss@127.0.0.1:1/x",
            "s3://u:p4ss@b/k",
            "abfss://c@acct.dfs.core.windows.net/p",
        ] {
            let err = client.get(url).await.expect_err(url);
            assert!(matches!(err, Error::InvalidInput(_)), "{url}: {err:?}");
            assert!(!format!("{err} {err:?}").contains("p4ss"), "{url}: {err:?}");
        }
    }
}

mod presign_input {
    use super::*;
    use std::collections::HashMap;

    /// An S3 client that signs offline: nothing here sends a request.
    fn s3_client() -> ObjectStorageClient {
        ObjectStorageClient::with_vars(HashMap::from([
            ("S3_ENDPOINT".into(), "http://127.0.0.1:1".into()),
            ("S3_REGION".into(), "us-east-1".into()),
            ("S3_ACCESS_KEY_ID".into(), "AKIDEXAMPLE".into()),
            ("S3_SECRET_ACCESS_KEY".into(), "secret".into()),
            ("S3_ALLOW_HTTP".into(), "true".into()),
        ]))
    }

    async fn sign(url: &str) -> Result<String> {
        s3_client()
            .get_pre_signed_url(
                url,
                SignMethod::Get,
                Duration::from_mins(1),
                &SignOptions::default(),
            )
            .await
    }

    #[tokio::test]
    async fn rejects_dot_segments_queries_fragments_and_bucket_roots() {
        for bad in [
            "s3://b/uploads/../other/x",
            "s3://b/../x",
            "s3://b/%2e%2e/x",
            "s3://b/%2E%2e/x",
            "s3://b/a/%2e./x",
            "s3://b/./x",
            "s3://b/a/.",
            "s3://b/a/..",
            "s3://b/a/%2E",
            "s3://b/..",
            "s3://b/%2e%2e",
            "s3://b/a/.\t./x",
            "s3://b/a?x=1",
            "s3://b/a#y",
            "s3://b/a?",
            "s3://b",
            "s3://b/",
            "s3://b//",
            "s3://../victim",
            "s3://%2e%2e/victim",
            "s3://%2E./victim",
            "s3://./victim/x",
            "s3://../victim/x",
        ] {
            let err = sign(bad).await.expect_err(bad);
            assert!(matches!(err, Error::InvalidInput(_)), "{bad}: {err:?}");
        }
    }

    #[tokio::test]
    async fn rejects_expiry_outside_one_second_to_seven_days() {
        let client = s3_client();
        for expires in [
            Duration::ZERO,
            Duration::from_millis(999),
            Duration::from_secs(604_801),
            Duration::from_secs(u64::MAX),
        ] {
            let err = client
                .get_pre_signed_url(
                    "s3://b/k",
                    SignMethod::Get,
                    expires,
                    &SignOptions::default(),
                )
                .await
                .expect_err("single");
            assert!(
                matches!(err, Error::InvalidInput(_)),
                "{expires:?}: {err:?}"
            );
            let err = client
                .get_pre_signed_urls(
                    &["s3://b/k"],
                    SignMethod::Get,
                    expires,
                    &SignOptions::default(),
                )
                .await
                .expect_err("batch");
            assert!(
                matches!(err, Error::InvalidInput(_)),
                "{expires:?}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn azure_expiry_beyond_chrono_range_is_invalid_input() {
        let client = ObjectStorageClient::with_vars(HashMap::new());
        let err = client
            .get_pre_signed_url(
                "az://c/b",
                SignMethod::Get,
                Duration::from_secs(10_000_000_000_000),
                &SignOptions::default(),
            )
            .await
            .expect_err("expiry");
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
    }

    #[tokio::test]
    async fn seven_days_is_accepted() -> Result<()> {
        let signed = s3_client()
            .get_pre_signed_url(
                "s3://b/k",
                SignMethod::Get,
                Duration::from_hours(168),
                &SignOptions::default(),
            )
            .await?;
        assert!(signed.contains("X-Amz-Expires=604800"), "{signed}");
        Ok(())
    }

    #[tokio::test]
    async fn signs_ordinary_keys() -> Result<()> {
        for ok in [
            "s3://b/x",
            "s3://b/a/b/x.txt",
            "s3://b/a..b",
            "s3://b/a/..b/c",
            "s3://b/a/b..",
            "s3://b/a/.hidden",
            "s3://b/a%2Fb",
        ] {
            let signed = sign(ok).await?;
            assert!(signed.contains("X-Amz-Signature"), "{ok}: {signed}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn batch_signing_applies_the_same_rules() {
        let client = s3_client();
        for bad in [
            ["s3://b/ok", "s3://b/../x"],
            ["s3://b/../x", "s3://b/ok"],
            ["s3://b/ok", "s3://b/"],
            ["s3://b/ok", "s3://b/a?x"],
            ["s3://../victim", "s3://../victim"],
            ["s3://%2e%2e/victim", "s3://%2e%2e/victim"],
            ["s3://./victim/x", "s3://./victim/x"],
        ] {
            let err = client
                .get_pre_signed_urls(
                    &bad,
                    SignMethod::Get,
                    Duration::from_mins(1),
                    &SignOptions::default(),
                )
                .await
                .expect_err(&format!("{bad:?}"));
            assert!(matches!(err, Error::InvalidInput(_)), "{bad:?}: {err:?}");
        }
        let signed = client
            .get_pre_signed_urls(
                &["s3://b/a..b", "s3://b/c"],
                SignMethod::Get,
                Duration::from_mins(1),
                &SignOptions::default(),
            )
            .await;
        assert_eq!(signed.map(|s| s.len()).ok(), Some(2));
    }

    #[tokio::test]
    async fn local_path_inputs_follow_the_segment_rule() {
        let err = sign("/tmp/a/../b").await.expect_err("dot segment");
        assert!(matches!(err, Error::InvalidInput(_)));
        let err = sign("../b").await.expect_err("dot segment");
        assert!(matches!(err, Error::InvalidInput(_)));
        // A clean local path reaches the backend, which does not sign.
        let err = sign("/tmp/a.txt").await.expect_err("local");
        assert!(matches!(err, Error::SigningUnsupported(_)));
    }
}

#[tokio::test]
async fn test_absolute_path_handling() -> std::result::Result<(), Box<dyn std::error::Error>> {
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
async fn test_mixed_local_paths() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();
    let tmp = tempdir()?;
    let file_name = "mixed_test.txt";
    let abs_path = tmp.path().join(file_name);
    let abs_path_str = abs_path.to_str().ok_or("Invalid path")?;

    let data = b"mixed path data";

    // Put using absolute path
    client.put(abs_path_str, data.to_vec()).await?;

    // Verify using the file:// URL of the same path
    let file_url = Url::from_file_path(&abs_path).map_err(|()| "absolute path")?;
    assert!(
        client.exists(file_url.as_str()).await?,
        "Should be visible via file:// URL"
    );

    // Cleanup
    client.delete(abs_path_str).await?;

    Ok(())
}

/// Writes to `file://` keep files private while they are written and follow
/// the destination's, the source's or the umask's mode when they are complete.
#[cfg(unix)]
mod modes {
    use crate::support::names;
    use bytes::Bytes;
    use futures::stream;
    use object_storage_client::{Error, ObjectStorageClient, Result};
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::Path;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    /// The process umask, learned from the mode of a file created with 0o666,
    /// so that it works on every Unix.
    fn umask() -> u32 {
        use std::os::unix::fs::OpenOptionsExt;
        let dir = tempdir().expect("tempdir");
        let probe = dir.path().join("probe");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&probe)
            .expect("probe");
        !mode(&probe) & 0o777
    }

    fn mode(path: &Path) -> u32 {
        fs::metadata(path).expect("stat").permissions().mode() & 0o7777
    }

    fn url(path: &Path) -> String {
        format!("file://{}", path.display())
    }

    /// A stream fed by hand, so a test can look at the disk mid-upload.
    fn fed() -> (
        mpsc::Sender<Result<Bytes>>,
        impl futures::Stream<Item = Result<Bytes>> + Send + 'static,
    ) {
        let (tx, rx) = mpsc::channel(1);
        (
            tx,
            stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|c| (c, rx)) }),
        )
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        for _ in 0..400 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    #[tokio::test]
    async fn put_and_put_stream_create_files_with_the_umask_default() -> Result<()> {
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();
        let want = 0o666 & !umask();
        let a = dir.path().join("a");
        client.put(&url(&a), b"plain".to_vec()).await?;
        assert_eq!(mode(&a), want);
        let b = dir.path().join("sub/b");
        let chunks = stream::iter([
            Ok(Bytes::from_static(b"one")),
            Ok(Bytes::from_static(b"two")),
        ]);
        client.put_stream(&url(&b), chunks).await?;
        assert_eq!(mode(&b), want);
        assert_eq!(fs::read(&b)?, b"onetwo");
        let empty = dir.path().join("empty");
        client.put(&url(&empty), Vec::new()).await?;
        assert_eq!(fs::read(&empty)?, b"");
        assert_eq!(names(dir.path()), ["a", "empty", "sub"]);
        Ok(())
    }

    #[tokio::test]
    async fn staging_file_is_private_while_it_is_written() -> Result<()> {
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();
        let dest = dir.path().join("obj");
        let (tx, rx) = fed();
        let target = url(&dest);
        let task = tokio::spawn(async move { client.put_stream(&target, rx).await });
        tx.send(Ok(Bytes::from_static(b"secret")))
            .await
            .expect("send");
        let staging = dir.path().join("obj#1");
        wait_for(|| fs::metadata(&staging).is_ok_and(|m| m.len() == 6)).await;
        assert_eq!(mode(&staging), 0o600);
        assert!(!dest.exists());
        drop(tx);
        task.await.expect("join")?;
        assert_eq!(mode(&dest), 0o666 & !umask());
        assert_eq!(fs::read(&dest)?, b"secret");
        assert_eq!(names(dir.path()), ["obj"]);
        Ok(())
    }

    #[tokio::test]
    async fn replacing_an_own_private_file_keeps_its_mode() -> Result<()> {
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();
        let dest = dir.path().join("obj");
        fs::write(&dest, b"old")?;
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o600))?;
        client.put(&url(&dest), b"new".to_vec()).await?;
        assert_eq!((mode(&dest), fs::read(&dest)?), (0o600, b"new".to_vec()));
        let more = stream::iter([Ok(Bytes::from_static(b"newer"))]);
        client.put_stream(&url(&dest), more).await?;
        assert_eq!((mode(&dest), fs::read(&dest)?), (0o600, b"newer".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn setuid_bits_of_the_destination_are_not_kept() -> Result<()> {
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();
        let dest = dir.path().join("obj");
        fs::write(&dest, b"old")?;
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o4755))?;
        client.put(&url(&dest), b"new".to_vec()).await?;
        assert_eq!(mode(&dest), 0o755);
        Ok(())
    }

    #[tokio::test]
    async fn streamed_copy_of_a_private_file_stays_private() -> Result<()> {
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();
        let src = dir.path().join("src");
        fs::write(&src, b"secret")?;
        fs::set_permissions(&src, fs::Permissions::from_mode(0o600))?;
        // Another host name is another backend, so the bytes are streamed
        // (as for a cross-device move) instead of copied server side.
        let dst = dir.path().join("dst");
        let to = format!("file://127.0.0.1{}", dst.display());
        client.copy(&url(&src), &to).await?;
        assert_eq!(fs::read(&dst)?, b"secret");
        assert_eq!(mode(&dst), 0o600);
        assert_ne!(fs::metadata(&src)?.ino(), fs::metadata(&dst)?.ino());
        assert_eq!(names(dir.path()), ["dst", "src"]);
        Ok(())
    }

    #[tokio::test]
    async fn failed_or_dropped_uploads_leave_nothing_behind() -> Result<()> {
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();
        let dest = dir.path().join("obj");
        let failing = stream::iter([
            Ok(Bytes::from_static(b"part")),
            Err(Error::InvalidInput("boom".into())),
        ]);
        assert!(client.put_stream(&url(&dest), failing).await.is_err());
        assert_eq!(names(dir.path()), Vec::<String>::new());

        let (tx, rx) = fed();
        let target = url(&dest);
        let task = tokio::spawn(async move { client.put_stream(&target, rx).await });
        tx.send(Ok(Bytes::from_static(b"part")))
            .await
            .expect("send");
        wait_for(|| dir.path().join("obj#1").exists()).await;
        task.abort();
        assert!(task.await.is_err());
        assert_eq!(names(dir.path()), Vec::<String>::new());
        Ok(())
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn encrypted_writes_follow_the_same_modes() -> Result<()> {
        use chrono::NaiveDate;
        use object_storage_client::EncryptedObjectStorageClient;
        use object_storage_client::encryption::{EncryptionConfig, Keyring, new_keyring};
        use std::sync::Arc;

        let today = NaiveDate::from_ymd_opt(2026, 10, 26).expect("date");
        let provider = Keyring::parse(&new_keyring("kek", today).expect("keyring")).expect("parse");
        let client = EncryptedObjectStorageClient::new(EncryptionConfig::new(Arc::new(provider)));

        let dir = tempdir()?;
        let a = dir.path().join("a");
        client.put(&url(&a), b"plain".to_vec()).await?;
        assert_eq!(mode(&a), 0o666 & !umask());
        fs::set_permissions(&a, fs::Permissions::from_mode(0o600))?;
        client.put(&url(&a), b"again".to_vec()).await?;
        assert_eq!(mode(&a), 0o600);

        // A copy to another backend re-reads and streams the bytes.
        let b = dir.path().join("b");
        client
            .copy(&url(&a), &format!("file://127.0.0.1{}", b.display()))
            .await?;
        assert_eq!(mode(&b), 0o600);
        assert_eq!(client.get(&url(&b)).await?.as_ref(), b"again");
        assert_eq!(names(dir.path()), ["a", "b"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_special_file_destination_is_refused_and_kept() -> Result<()> {
        use std::os::unix::fs::FileTypeExt;
        let dir = tempdir()?;
        let client = ObjectStorageClient::new();

        let socket = dir.path().join("sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
        let err = client
            .put(&url(&socket), b"x".to_vec())
            .await
            .expect_err("socket");
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
        assert!(fs::symlink_metadata(&socket)?.file_type().is_socket());

        #[cfg(target_os = "linux")]
        {
            let fifo = dir.path().join("fifo");
            rustix::fs::mknodat(
                rustix::fs::CWD,
                &fifo,
                rustix::fs::FileType::Fifo,
                rustix::fs::Mode::from_raw_mode(0o600),
                0,
            )
            .map_err(std::io::Error::from)?;
            let err = client
                .put(&url(&fifo), b"x".to_vec())
                .await
                .expect_err("fifo");
            assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
            assert!(fs::symlink_metadata(&fifo)?.file_type().is_fifo());
        }

        let mut left = names(dir.path());
        left.retain(|n| n.contains('#'));
        assert!(left.is_empty(), "staging files left: {left:?}");
        Ok(())
    }

    #[tokio::test]
    async fn a_directory_destination_is_refused() -> Result<()> {
        let dir = tempdir()?;
        let target = dir.path().join("d");
        fs::create_dir(&target)?;
        fs::write(target.join("inner"), b"keep")?;
        let err = ObjectStorageClient::new()
            .put(&url(&target), b"x".to_vec())
            .await
            .expect_err("directory");
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
        assert!(fs::metadata(&target)?.is_dir());
        assert_eq!(fs::read(target.join("inner"))?, b"keep");
        assert_eq!(names(dir.path()), ["d"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_symlink_destination_is_replaced_not_followed() -> Result<()> {
        let dir = tempdir()?;
        let target = dir.path().join("target");
        let link = dir.path().join("link");
        fs::write(&target, b"keep")?;
        std::os::unix::fs::symlink(&target, &link)?;

        ObjectStorageClient::new()
            .put(&url(&link), b"new".to_vec())
            .await?;
        assert!(fs::symlink_metadata(&link)?.file_type().is_file());
        assert_eq!(fs::read(&link)?, b"new");
        assert_eq!(fs::read(&target)?, b"keep");
        Ok(())
    }
}

#[tokio::test]
async fn an_empty_url_list_is_invalid_input() {
    let err = ObjectStorageClient::new()
        .get_pre_signed_urls(
            &[],
            SignMethod::Get,
            Duration::from_mins(1),
            &SignOptions::default(),
        )
        .await
        .expect_err("no URLs");
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
}

#[tokio::test]
async fn content_binding_on_a_non_s3_store_is_invalid_input() {
    use std::collections::HashMap;
    // The public Azurite development account; nothing is sent anywhere.
    let client = ObjectStorageClient::with_vars(HashMap::from([
        ("AZURE_STORAGE_ACCOUNT_NAME".into(), "devstoreaccount1".into()),
        (
            "AZURE_STORAGE_ACCOUNT_KEY".into(),
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                .into(),
        ),
        (
            "AZURE_STORAGE_ENDPOINT".into(),
            "http://127.0.0.1:1/devstoreaccount1".into(),
        ),
        ("AZURE_ALLOW_HTTP".into(), "true".into()),
    ]));
    let options = SignOptions {
        content_length: Some(10),
        content_type: None,
    };
    let err = client
        .get_pre_signed_url(
            "az://c/b",
            SignMethod::Put,
            Duration::from_mins(1),
            &options,
        )
        .await
        .expect_err("binding is S3-only");
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
}
