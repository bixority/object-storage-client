//! Reading the body of a `GetResult`.

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::{GetResult, GetResultPayload};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

/// Bytes read from a local file per blocking task. `object_store`'s own
/// stream reads 8 KiB, which makes a large local read dominated by task hops.
const FILE_CHUNK: u64 = 512 * 1024;

/// The body of `result` as a stream of chunks.
///
/// A local file is read in [`FILE_CHUNK`] pieces: one seek to the start of the
/// range, then exactly the bytes of the range. A file that ends early (it was
/// truncated after the open) is an error rather than a short body, which a
/// plain reader could not tell from a complete one. Any other payload is
/// `object_store`'s own stream.
pub fn body_stream(result: GetResult) -> BoxStream<'static, object_store::Result<Bytes>> {
    let range = result.range.clone();
    match result.payload {
        GetResultPayload::File(file, path) => {
            file_stream(file, path, range.start, range.end - range.start)
        }
        GetResultPayload::Stream(stream) => stream,
    }
}

fn read_error(source: std::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "LocalFileSystem",
        source: Box::new(source),
    }
}

struct Reader {
    file: File,
    path: PathBuf,
    /// Where the next read starts; `Some` until the first read has seeked.
    seek_to: Option<u64>,
    remaining: u64,
}

impl Reader {
    /// Reads the next chunk, or `None` at the end of the range.
    fn next_chunk(&mut self) -> object_store::Result<Option<Bytes>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let fail = |e: std::io::Error, path: &PathBuf, what: &str| {
            read_error(std::io::Error::new(
                e.kind(),
                format!("Unable to {what} {}: {e}", path.display()),
            ))
        };
        if let Some(start) = self.seek_to.take() {
            self.file
                .seek(SeekFrom::Start(start))
                .map_err(|e| fail(e, &self.path, "seek in"))?;
        }
        let want = self.remaining.min(FILE_CHUNK);
        let mut buf = Vec::with_capacity(usize::try_from(want).unwrap_or(usize::MAX));
        (&mut self.file)
            .take(want)
            .read_to_end(&mut buf)
            .map_err(|e| fail(e, &self.path, "read"))?;
        if (buf.len() as u64) < want {
            return Err(fail(
                std::io::ErrorKind::UnexpectedEof.into(),
                &self.path,
                "read the whole range of the shortened file",
            ));
        }
        self.remaining -= want;
        Ok(Some(buf.into()))
    }
}

fn file_stream(
    file: File,
    path: PathBuf,
    start: u64,
    len: u64,
) -> BoxStream<'static, object_store::Result<Bytes>> {
    let reader = Reader {
        file,
        path,
        seek_to: Some(start),
        remaining: len,
    };
    stream::try_unfold(reader, |mut reader| async move {
        let joined = tokio::task::spawn_blocking(move || {
            let chunk = reader.next_chunk();
            (reader, chunk)
        })
        .await;
        let (reader, chunk) = joined.map_err(|e| read_error(std::io::Error::other(e)))?;
        Ok(chunk?.map(|chunk| (chunk, reader)))
    })
    .boxed()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;
    use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};

    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect()
    }

    fn store(dir: &tempfile::TempDir) -> LocalFileSystem {
        LocalFileSystem::new_with_prefix(dir.path()).unwrap()
    }

    #[tokio::test]
    async fn whole_files_and_ranges_are_read_byte_exact() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let want = data(3 * usize::try_from(FILE_CHUNK).unwrap() + 12_345);
        let path = ObjectPath::from("f");
        store.put(&path, want.clone().into()).await.unwrap();

        let all = body_stream(store.get(&path).await.unwrap());
        let chunks: Vec<Bytes> = all.try_collect().await.unwrap();
        assert!(chunks.iter().all(|c| c.len() as u64 <= FILE_CHUNK));
        assert!(chunks.len() <= 5, "{} chunks", chunks.len());
        assert_eq!(chunks.concat(), want);

        for (start, end) in [
            (0, 1),
            (7, 7 + FILE_CHUNK),
            (FILE_CHUNK - 1, 2 * FILE_CHUNK + 1),
            (100, want.len() as u64),
        ] {
            let options = GetOptions {
                range: Some(GetRange::Bounded(start..end)),
                ..GetOptions::default()
            };
            let got = store.get_opts(&path, options).await.unwrap();
            let body: Vec<Bytes> = body_stream(got).try_collect().await.unwrap();
            assert_eq!(
                body.concat(),
                want[usize::try_from(start).unwrap()..usize::try_from(end).unwrap()],
                "{start}..{end}"
            );
        }
    }

    #[tokio::test]
    async fn a_file_that_shrinks_mid_read_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let path = ObjectPath::from("f");
        let len = 4 * usize::try_from(FILE_CHUNK).unwrap();
        store.put(&path, data(len).into()).await.unwrap();

        let mut body = body_stream(store.get(&path).await.unwrap());
        let first = body.next().await.unwrap().unwrap();
        assert_eq!(first.len() as u64, FILE_CHUNK);
        let local = dir.path().join("f");
        std::fs::OpenOptions::new()
            .write(true)
            .open(local)
            .unwrap()
            .set_len(FILE_CHUNK + 10)
            .unwrap();

        let mut got = first.len();
        let mut failed = false;
        while let Some(item) = body.next().await {
            match item {
                Ok(chunk) => got += chunk.len(),
                Err(e) => {
                    assert!(e.to_string().contains("shortened"), "{e}");
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed, "a shrunken file must not end the body cleanly");
        assert!(got < len);
    }

    #[tokio::test]
    async fn a_file_shorter_than_its_range_is_an_error_not_eof() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let path = ObjectPath::from("f");
        store.put(&path, data(1000).into()).await.unwrap();
        let got = store.get(&path).await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("f"))
            .unwrap()
            .set_len(10)
            .unwrap();
        assert!(body_stream(got).try_collect::<Vec<_>>().await.is_err());
    }
}
