/// Shared functional suite for any `Client<C>`; `$ctor` builds a fresh client.
macro_rules! client_suite {
    ($m:ident, $ctor:expr) => {
        mod $m {
            use crate::support::{chunked, drain, names};
            use futures::StreamExt;
            use object_storage_client::{Error, Result};
            use tempfile::tempdir;

            fn url_of(dir: &tempfile::TempDir, name: &str) -> String {
                format!("file://{}/{name}", dir.path().display())
            }

            #[tokio::test]
            async fn lifecycle() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let url = url_of(&dir, "a.bin");

                assert!(!client.exists(&url).await?);
                client.put(&url, &b"hello"[..]).await?;
                assert!(client.exists(&url).await?);
                assert_eq!(client.get(&url).await?.as_ref(), b"hello");
                client.delete(&url).await?;
                assert!(!client.exists(&url).await?);
                Ok(())
            }

            #[tokio::test]
            async fn get_stream_collects() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let url = url_of(&dir, "s.bin");
                let data = vec![7u8; 300_000];
                client.put(&url, data.clone()).await?;

                let out = drain(client.get_stream(&url).await?).await?;
                assert_eq!(out, data);
                Ok(())
            }

            #[tokio::test]
            async fn list_and_delete_by_prefix() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                for name in ["p/one", "p/two", "p/sub/three", "other"] {
                    client.put(&url_of(&dir, name), &b"x"[..]).await?;
                }

                let mut names = client.list(&url_of(&dir, "p")).await?;
                names.sort();
                assert_eq!(names, ["one", "sub/three", "two"]);
                assert_eq!(client.list(&url_of(&dir, "")).await?.len(), 4);

                // A nested prefix without a trailing slash, then the folder.
                client.delete_by_prefix(&url_of(&dir, "p/sub")).await?;
                let mut names = client.list(&url_of(&dir, "p")).await?;
                names.sort();
                assert_eq!(names, ["one", "two"]);

                client.delete_by_prefix(&url_of(&dir, "p/")).await?;
                assert!(client.list(&url_of(&dir, "p")).await?.is_empty());
                // The sibling outside the prefix survives.
                assert_eq!(client.list(&url_of(&dir, "")).await?, ["other"]);
                Ok(())
            }

            #[tokio::test]
            async fn list_stream_yields_the_names_list_returns() -> Result<()> {
                use futures::TryStreamExt;
                let dir = tempdir()?;
                let client = $ctor;
                for name in ["p/one", "p/sub/two", "other"] {
                    client.put(&url_of(&dir, name), &b"x"[..]).await?;
                }
                for prefix in ["p", "p/", ""] {
                    let url = url_of(&dir, prefix);
                    let mut streamed: Vec<String> = client.list_stream(&url)?.try_collect().await?;
                    let mut listed = client.list(&url).await?;
                    streamed.sort();
                    listed.sort();
                    assert_eq!(streamed, listed);
                }
                let mut under: Vec<String> = client
                    .list_stream(&url_of(&dir, "p"))?
                    .try_collect()
                    .await?;
                under.sort();
                assert_eq!(under, ["one", "sub/two"]);
                Ok(())
            }

            #[tokio::test]
            async fn buckets_on_file_scheme() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let url = url_of(&dir, "bucket");

                assert!(!client.bucket_exists(&url).await?);
                client.create_bucket(&url).await?;
                assert!(client.bucket_exists(&url).await?);
                assert!(dir.path().join("bucket").is_dir());
                // Idempotent: creating an existing bucket succeeds.
                client.create_bucket(&url).await?;

                // Objects can be written into the freshly created bucket.
                let obj = url_of(&dir, "bucket/o.txt");
                client.put(&obj, &b"hi"[..]).await?;
                assert!(client.exists(&obj).await?);
                // A plain file at the path is not a bucket.
                assert!(!client.bucket_exists(&obj).await?);
                Ok(())
            }

            #[tokio::test]
            async fn put_stream_roundtrips() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                for (i, len) in [0usize, 1, 20 * 1024 * 1024].into_iter().enumerate() {
                    let url = url_of(&dir, &format!("ps{i}"));
                    let data: Vec<u8> = (0..len)
                        .map(|b| u8::try_from(b % 251).unwrap_or(0))
                        .collect();
                    client
                        .put_stream(&url, chunked(data.clone().into(), 1_000_003))
                        .await?;
                    assert_eq!(client.get(&url).await?.as_ref(), data.as_slice());
                }
                Ok(())
            }

            #[tokio::test]
            async fn put_stream_error_leaves_no_file_and_no_staging_file() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let url = url_of(&dir, "bad");
                let data = vec![5u8; 10 * 1024 * 1024];
                let failing =
                    chunked(data.clone().into(), 1_000_003).chain(futures::stream::once(async {
                        Err(Error::Generic("boom".into()))
                    }));

                assert!(client.put_stream(&url, failing).await.is_err());
                assert!(!client.exists(&url).await?);
                assert_eq!(std::fs::read_dir(dir.path())?.count(), 0);
                Ok(())
            }

            #[tokio::test]
            async fn missing_object_is_not_found() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let url = url_of(&dir, "missing");

                assert!(matches!(
                    client.get(&url).await,
                    Err(Error::ObjectStore(object_store::Error::NotFound { .. }))
                ));
                assert!(matches!(
                    client.get_stream(&url).await,
                    Err(Error::ObjectStore(object_store::Error::NotFound { .. }))
                ));
                Ok(())
            }

            #[tokio::test]
            async fn copy_and_move() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let (a, b, c) = (url_of(&dir, "a"), url_of(&dir, "b"), url_of(&dir, "c"));
                client.put(&a, &b"data"[..]).await?;

                client.copy(&a, &b).await?;
                assert_eq!(client.get(&a).await?.as_ref(), b"data");
                assert_eq!(client.get(&b).await?.as_ref(), b"data");

                client.move_object(&b, &c).await?;
                assert!(!client.exists(&b).await?);
                assert_eq!(client.get(&c).await?.as_ref(), b"data");
                Ok(())
            }

            #[tokio::test]
            async fn move_onto_itself_keeps_the_object() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let a = url_of(&dir, "a");
                client.put(&a, &b"data"[..]).await?;

                client.move_object(&a, &a).await?;
                assert_eq!(client.get(&a).await?.as_ref(), b"data");
                Ok(())
            }

            /// URL paths are percent-decoded into object keys: the file lands
            /// under its literal name, and listings report literal names.
            #[tokio::test]
            async fn percent_encoded_url_paths_name_literal_keys() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let root = format!("file://{}", dir.path().display());
                let spaced = format!("{root}/dir%20a/x%20%25%C3%BC.bin");
                // `%41` is a literal name here, not an escape for `A`.
                let plus = format!("{root}/a+b%2541");
                let literal = dir.path().join("dir a").join("x %\u{fc}.bin");

                client.put(&spaced, &b"one"[..]).await?;
                client.put(&plus, &b"two"[..]).await?;
                assert!(std::fs::metadata(&literal)?.is_file());
                assert!(std::fs::metadata(dir.path().join("a+b%41"))?.is_file());
                assert!(!dir.path().join("dir%20a").exists());
                assert!(client.exists(&spaced).await?);
                assert_eq!(client.get(&spaced).await?.as_ref(), b"one");

                let mut all = client.list(&root).await?;
                all.sort();
                assert_eq!(all, ["a+b%41", "dir a/x %\u{fc}.bin"]);
                assert_eq!(
                    client.list(&format!("{root}/dir%20a")).await?,
                    ["x %\u{fc}.bin"]
                );

                let copy = format!("{root}/dir%20a/y%20%25.bin");
                client.copy(&spaced, &copy).await?;
                assert_eq!(client.get(&copy).await?.as_ref(), b"one");
                let moved = format!("{root}/moved%20here");
                client.move_object(&copy, &moved).await?;
                assert!(!client.exists(&copy).await?);
                assert_eq!(client.get(&moved).await?.as_ref(), b"one");
                assert!(std::fs::metadata(dir.path().join("moved here"))?.is_file());

                client.delete(&spaced).await?;
                assert!(!literal.exists());
                client.delete_by_prefix(&format!("{root}/dir%20a")).await?;
                Ok(())
            }

            #[tokio::test]
            async fn move_onto_itself_through_a_host_alias_keeps_the_object() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let a = url_of(&dir, "a");
                // The local backend ignores the host: this names the same file.
                let alias = format!("file://127.0.0.1{}/a", dir.path().display());
                client.put(&a, &b"data"[..]).await?;

                client.move_object(&a, &alias).await?;
                client.move_object(&alias, &a).await?;
                assert_eq!(client.get(&a).await?.as_ref(), b"data");
                Ok(())
            }

            /// `link -> real`: both URLs reach one file, so a move must not
            /// delete what it just wrote.
            #[cfg(unix)]
            #[tokio::test]
            async fn move_through_a_symlinked_directory_keeps_the_object() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                std::fs::create_dir(dir.path().join("real"))?;
                std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("link"))?;
                let (via_link, real) = (url_of(&dir, "link/a"), url_of(&dir, "real/a"));
                client.put(&via_link, &b"data"[..]).await?;

                client.move_object(&via_link, &real).await?;
                assert_eq!(client.get(&real).await?.as_ref(), b"data");
                // The destination reads back; reading the other name would
                // fail the encrypted client's path binding.
                client.move_object(&real, &via_link).await?;
                assert_eq!(client.get(&via_link).await?.as_ref(), b"data");
                Ok(())
            }

            /// A local copy onto itself would hard-link `a#N` and rename it
            /// over `a`, leaving a hidden link to the old content.
            #[tokio::test]
            async fn copy_onto_itself_leaves_no_hidden_links() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let a = url_of(&dir, "a");
                client.put(&a, &b"data"[..]).await?;

                client.copy(&a, &a).await?;
                client.copy(&a, &a).await?;
                assert_eq!(names(dir.path()), ["a"]);
                assert_eq!(client.get(&a).await?.as_ref(), b"data");
                Ok(())
            }

            #[cfg(unix)]
            #[tokio::test]
            async fn copy_onto_itself_through_a_symlink_leaves_no_hidden_links() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                std::fs::create_dir(dir.path().join("real"))?;
                std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("link"))?;
                let (via_link, real) = (url_of(&dir, "link/a"), url_of(&dir, "real/a"));
                client.put(&real, &b"data"[..]).await?;

                // The encrypted client re-binds the object to its new name.
                client.copy(&real, &via_link).await?;
                assert_eq!(names(&dir.path().join("real")), ["a"]);
                assert_eq!(client.get(&via_link).await?.as_ref(), b"data");
                Ok(())
            }

            #[tokio::test]
            async fn delete_by_prefix_rejects_an_empty_prefix() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let a = url_of(&dir, "a");
                client.put(&a, &b"data"[..]).await?;
                let here = std::env::current_dir()?;

                for bad in ["", "  ", "\t\n", "file:///", "file://localhost/", "/"] {
                    let err = client.delete_by_prefix(bad).await.expect_err(bad);
                    assert!(matches!(err, Error::InvalidInput(_)), "{bad:?}: {err:?}");
                }
                assert_eq!(std::env::current_dir()?, here);
                assert!(client.exists(&a).await?);
                Ok(())
            }
        }
    };
}

/// Tests that only hold for the plain client: sizes of encrypted objects are
/// covered in `encryption.rs`.
macro_rules! plain_only_suite {
    ($m:ident, $ctor:expr) => {
        mod $m {
            use object_storage_client::{Error, Result};
            use tempfile::tempdir;

            fn url_of(dir: &tempfile::TempDir, name: &str) -> String {
                format!("file://{}/{name}", dir.path().display())
            }

            #[tokio::test]
            async fn metadata_reports_size() -> Result<()> {
                let dir = tempdir()?;
                let client = $ctor;
                let url = url_of(&dir, "m.bin");
                client.put(&url, &b"12345"[..]).await?;

                let meta = client.get_object_metadata(&url).await?;
                assert_eq!(meta.size_bytes, 5);
                assert_eq!(meta.stored_size_bytes, 5);
                assert!(meta.encryption.is_none());
                // LocalFileSystem reports no content type.
                assert!(meta.location.ends_with("m.bin"));
                assert_eq!(meta.content_type, None);

                // Metadata for a missing object errors with NotFound.
                client.delete(&url).await?;
                let err = client
                    .get_object_metadata(&url)
                    .await
                    .expect_err("metadata for a missing object must error");
                assert!(matches!(
                    err,
                    Error::ObjectStore(object_store::Error::NotFound { .. })
                ));
                Ok(())
            }
        }
    };
}

client_suite!(plain, object_storage_client::ObjectStorageClient::new());
plain_only_suite!(
    plain_only,
    object_storage_client::ObjectStorageClient::new()
);
