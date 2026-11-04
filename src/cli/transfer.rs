//! `put`, `get` and `get-stream`: local files to and from the store.

use super::output::{show_path, show_url};
use super::tree::{SourceTree, destination_url};
use super::{CliError, to_url};
use crate::client::Error as StorageError;
use crate::{Client, Codec};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use std::fs;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;

// Read and write buffer size for local files: the local store's read chunk,
// and above the upload coalescing threshold, so uploaded chunks go into parts
// without a copy.
use crate::backend::FILE_BUF;

/// Files `put -r` uploads at once.
const FILES_IN_FLIGHT: usize = 8;

/// Size from which a file counts as large for `put -r` (one upload part).
const LARGE_FILE: u64 = 8 * 1024 * 1024;

/// Permits a large file holds, so at most `FILES_IN_FLIGHT / 4` run together.
const LARGE_FILE_WEIGHT: u32 = 4;

/// Permits a file of `size` takes from a budget of `budget` permits: one for a
/// small file, `LARGE_FILE_WEIGHT` (at most the whole budget) for a large one.
pub(super) fn weight(size: u64, budget: usize) -> u32 {
    if size < LARGE_FILE {
        1
    } else {
        LARGE_FILE_WEIGHT.min(u32::try_from(budget).unwrap_or(u32::MAX))
    }
}

/// Runs `job` while holding `weight` permits of `budget`. The callers' local
/// semaphores are never closed, so acquiring cannot fail.
pub(super) async fn run_weighted<T>(
    budget: &Semaphore,
    weight: u32,
    job: impl Future<Output = T>,
) -> T {
    let _permit = budget.acquire_many(weight).await.ok();
    job.await
}

/// Longest file name the common file systems accept, in bytes.
const NAME_MAX: usize = 255;

/// Wraps an I/O failure on the local source `path`.
pub(super) fn read_err(path: &Path) -> impl Fn(std::io::Error) -> CliError {
    |source| CliError::ReadFile {
        path: path.display().to_string(),
        source,
    }
}

pub(super) async fn put<C: Codec>(
    client: &Client<C>,
    src: &Path,
    dst: &str,
    recursive: bool,
) -> Result<(), CliError> {
    let src_metadata = fs::metadata(src).map_err(read_err(src))?;

    if src_metadata.is_dir() {
        if !recursive {
            return Err(CliError::DirectoryRecursiveRequired {
                path: src.display().to_string(),
            });
        }

        let mut dst_root = to_url(dst)?;
        if !dst_root.ends_with('/') {
            dst_root.push('/');
        }
        refuse_destination_inside(src, &dst_root)?;

        outln!(
            "Recursive upload {} -> {}",
            show_path(src),
            show_url(&dst_root)
        )
        .await?;
        upload_dir_recursive(client, src, &dst_root).await?;
    } else {
        let dst_url = to_url(dst)?;

        outln!("Upload {} -> {}", show_path(src), show_url(&dst_url)).await?;
        let file = tokio::fs::File::open(src).await.map_err(read_err(src))?;
        put_file(client, file, &dst_url, src).await?;
    }

    Ok(())
}

/// The chunks of `file`, read to its end. A regular file that ends short of
/// `expected` bytes and is really shorter now is an error, not a clean end, so
/// a file truncated during the upload commits nothing. A file that grows is
/// read in full, and so is one whose size never matches its content (`/proc`,
/// sysfs).
async fn file_chunks(
    file: tokio::fs::File,
    expected: Option<u64>,
    label: String,
) -> BoxStream<'static, crate::Result<Bytes>> {
    struct State {
        chunks: Option<ReaderStream<tokio::fs::File>>,
        count: u64,
        expected: Option<u64>,
        probe: Option<tokio::fs::File>,
        label: String,
    }
    // A second handle to the same file, to see its size at the end.
    let probe = match expected {
        Some(_) => file.try_clone().await.ok(),
        None => None,
    };
    let state = State {
        chunks: Some(ReaderStream::with_capacity(file, FILE_BUF)),
        count: 0,
        expected,
        probe,
        label,
    };
    futures::stream::unfold(state, |mut state| async move {
        let mut chunks = state.chunks.take()?;
        match chunks.next().await {
            Some(Ok(chunk)) => {
                state.count += chunk.len() as u64;
                state.chunks = Some(chunks);
                Some((Ok(chunk), state))
            }
            Some(Err(e)) => Some((Err(StorageError::from(e)), state)),
            None => {
                let short = match (state.expected, &state.probe) {
                    (Some(size), probe) if state.count < size => match probe {
                        Some(probe) => probe.metadata().await.map_or(true, |m| m.len() < size),
                        None => true,
                    },
                    _ => false,
                };
                short.then(|| {
                    let e = std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("{} shrank during the upload", state.label),
                    );
                    (Err(StorageError::from(e)), state)
                })
            }
        }
    })
    .boxed()
}

/// Canonicalizes `path`; the part that does not exist yet is appended as is.
fn canonicalize_lenient(path: &Path) -> std::io::Result<PathBuf> {
    let mut missing = Vec::new();
    let mut existing = path;
    loop {
        match fs::canonicalize(existing) {
            Ok(mut found) => {
                found.extend(missing.iter().rev());
                return Ok(found);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let (Some(name), Some(parent)) = (existing.file_name(), existing.parent()) else {
                    return Err(e);
                };
                missing.push(name);
                existing = parent;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Refuses a `file://` destination that is the source directory or inside it:
/// the walk would pick up the copies and staging files the upload writes.
/// Another mount of the same directory is not seen.
fn refuse_destination_inside(src: &Path, dst_root: &str) -> Result<(), CliError> {
    let Some(dst) = url::Url::parse(dst_root)
        .ok()
        .and_then(|u| u.to_file_path().ok())
    else {
        return Ok(());
    };
    let src = fs::canonicalize(src).map_err(read_err(src))?;
    let dst = canonicalize_lenient(&dst).map_err(read_err(&dst))?;
    if dst.starts_with(&src) {
        return Err(CliError::UnsafePath {
            path: dst.display().to_string(),
            reason: "destination is inside the source directory",
        });
    }
    Ok(())
}

/// Streams one local file to `dst_url`, so memory use does not grow with the
/// file size.
async fn put_file<C: Codec>(
    client: &Client<C>,
    file: tokio::fs::File,
    dst_url: &str,
    path: &Path,
) -> Result<(), CliError> {
    let meta = file.metadata().await.ok();
    // A `file://` destination keeps the source's permission bits, as a copy does.
    #[cfg(unix)]
    let source_mode = {
        use std::os::unix::fs::PermissionsExt;
        meta.as_ref().map(|m| m.permissions().mode() & 0o777)
    };
    #[cfg(not(unix))]
    let source_mode = None;
    let size = meta.as_ref().map(std::fs::Metadata::len);
    let expected = meta
        .as_ref()
        .filter(|m| m.is_file())
        .map(std::fs::Metadata::len);
    put_opened(client, file, dst_url, path, (size, expected, source_mode)).await
}

/// [`put_file`] for a file whose size, expected length (a regular file) and
/// mode are known already.
async fn put_opened<C: Codec>(
    client: &Client<C>,
    file: tokio::fs::File,
    dst_url: &str,
    path: &Path,
    (size, expected, source_mode): (Option<u64>, Option<u64>, Option<u32>),
) -> Result<(), CliError> {
    let chunks = file_chunks(file, expected, show_path(path)).await;
    client
        .put_local_file(dst_url, chunks, source_mode, size)
        .await?;
    Ok(())
}

/// Removes a file when dropped, unless [`Self::keep`] was called. Covers
/// errors and a cancelled download alike.
pub(super) struct TempFile(Option<PathBuf>);

impl TempFile {
    fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

/// What differs by platform: the owner of a file.
#[cfg(unix)]
mod platform {
    use std::fs::Metadata;
    use std::os::unix::fs::MetadataExt;

    /// Permission bits and owner of the file being replaced.
    #[expect(clippy::unnecessary_wraps, reason = "`None` on other platforms")]
    pub fn mode_and_owner(meta: &Metadata) -> Option<(u32, u32)> {
        Some((meta.mode(), meta.uid()))
    }

    pub fn effective_uid() -> u32 {
        rustix::process::geteuid().as_raw()
    }
}

#[cfg(not(unix))]
mod platform {
    use std::fs::Metadata;

    pub const fn mode_and_owner(_: &Metadata) -> Option<(u32, u32)> {
        None
    }

    pub const fn effective_uid() -> u32 {
        0
    }
}

/// Decides the modes of the download temp file: the mode it is created with
/// and the mode it gets just before it replaces the destination.
///
/// The mode of a file we own is kept for both; anything else is a new file
/// ([`crate::fsmode::modes`] with no source): created private (0600), then
/// `0o666 & !umask`.
#[must_use]
pub(super) fn download_modes(
    existing: Option<(u32, u32)>,
    euid: u32,
    umask: Option<u32>,
) -> (Option<u32>, Option<u32>) {
    crate::fsmode::owned_mode(existing, euid).map_or_else(
        || crate::fsmode::modes(None, euid, None, false, umask),
        |mode| (Some(mode), Some(mode)),
    )
}

/// Name of the download temp file for `name`: `<name>.osc-tmp-<suffix>`, with
/// the name part cut (on a character boundary) so the whole stays within
/// `NAME_MAX` bytes.
#[must_use]
pub(super) fn temp_file_name(name: &std::ffi::OsStr, suffix: &str) -> std::ffi::OsString {
    let tail = format!(".osc-tmp-{suffix}");
    let mut name = name.to_string_lossy().into_owned();
    let room = NAME_MAX.saturating_sub(tail.len());
    if name.len() > room {
        name.truncate(name.floor_char_boundary(room));
    }
    name.push_str(&tail);
    name.into()
}

/// Creates the download temp file exclusively. With `mode` (Unix only) it is
/// created with at most those bits, so it is never more readable than the file
/// it replaces, not even before the data is written. The file and its cleanup
/// guard are made in one blocking task, so a dropped future cannot leave the
/// file behind.
pub(super) async fn create_temp_file(
    path: &Path,
    mode: Option<u32>,
) -> std::io::Result<(tokio::fs::File, TempFile)> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(mode & 0o777);
        }
        #[cfg(not(unix))]
        let _ = mode;
        let file = opts.open(&path)?;
        Ok((tokio::fs::File::from_std(file), TempFile(Some(path))))
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Downloads to a temporary file next to `dst` and renames it into place only
/// after the stream ended cleanly, so a failed or tampered download never
/// leaves a partial `dst`.
pub(super) async fn get<C: Codec>(
    client: &Client<C>,
    src: &str,
    dst: &Path,
) -> Result<(), CliError> {
    let src_url = to_url(src)?;

    outln!("Download {} -> {}", show_url(&src_url), show_path(dst)).await?;

    let write_err = |source| CliError::WriteFile {
        path: dst.display().to_string(),
        source,
    };

    let mut stream = client.get_stream(&src_url).await?;

    // Only a regular file (or absent path) can be replaced atomically. A FIFO,
    // device or `/dev/fd/N` is written through.
    let meta = tokio::fs::metadata(dst).await.ok();
    let direct = meta.as_ref().is_some_and(|m| !m.is_file());
    // A symlink is followed, so the temp file and the rename target are the
    // link's target.
    let dst = if meta.is_some() {
        tokio::fs::canonicalize(dst).await.map_err(write_err)?
    } else {
        dst.to_path_buf()
    };
    let dst = dst.as_path();

    let mut final_mode: Option<u32> = None;
    let (file, guard) = if direct {
        let file = tokio::fs::File::create(dst).await.map_err(write_err)?;
        (file, TempFile(None))
    } else {
        let name = dst.file_name().ok_or_else(|| {
            write_err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no file name",
            ))
        })?;
        let mut suffix = [0u8; 8];
        aws_lc_rs::rand::fill(&mut suffix)
            .map_err(|_| write_err(std::io::Error::other("random number generator failed")))?;
        let tmp = dst.with_file_name(temp_file_name(name, &crate::hex::encode(&suffix)));

        // The mode of a file we own is kept; anything else starts private
        // (0600) and gets the default mode just before the rename, so a
        // partial plaintext is never readable by others.
        let (create_mode, mode) = download_modes(
            meta.as_ref().and_then(platform::mode_and_owner),
            platform::effective_uid(),
            crate::fsmode::umask_near(dst),
        );
        final_mode = mode;
        create_temp_file(&tmp, create_mode)
            .await
            .map_err(write_err)?
    };

    let mut out = BufWriter::with_capacity(FILE_BUF, file);
    write_stream(&mut stream, &mut out, write_err).await?;
    if let Some(tmp) = &guard.0 {
        out.get_ref().sync_all().await.map_err(write_err)?;
        #[cfg(unix)]
        if let Some(mode) = final_mode {
            use std::os::unix::fs::PermissionsExt;
            out.get_ref()
                .set_permissions(fs::Permissions::from_mode(mode))
                .await
                .map_err(write_err)?;
        }
        #[cfg(not(unix))]
        let _ = final_mode;
        drop(out);
        tokio::fs::rename(tmp, dst).await.map_err(write_err)?;
        sync_parent(dst).await.map_err(write_err)?;
    }
    guard.keep();
    Ok(())
}

/// Makes a rename into `path` durable by syncing its directory (Unix only).
async fn sync_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        tokio::task::spawn_blocking(move || fs::File::open(dir)?.sync_all())
            .await
            .map_err(std::io::Error::other)??;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Stream an object to standard output, or to FILE exactly like `get`.
pub(super) async fn get_stream<C: Codec>(
    client: &Client<C>,
    src: &str,
    dst: Option<&Path>,
) -> Result<(), CliError> {
    if let Some(dst) = dst {
        return get(client, src, dst).await;
    }
    let src_url = to_url(src)?;

    // Standard output carries the object bytes only.
    errln!("Streaming {}", show_url(&src_url));

    let mut stream = client.get_stream(&src_url).await?;
    let mut out = BufWriter::with_capacity(FILE_BUF, tokio::io::stdout());
    write_stream(&mut stream, &mut out, CliError::Output).await
}

/// Writes every chunk of `stream` to `out` and flushes it; `err` wraps a write
/// failure. A stream error (a failed or tampered read) is returned as is.
async fn write_stream<W: AsyncWrite + Unpin>(
    stream: &mut (impl Stream<Item = crate::Result<Bytes>> + Unpin),
    out: &mut BufWriter<W>,
    err: impl Fn(std::io::Error) -> CliError,
) -> Result<(), CliError> {
    while let Some(chunk) = stream.next().await {
        out.write_all(&chunk?).await.map_err(&err)?;
    }
    out.flush().await.map_err(err)?;
    Ok(())
}

/// Recursively upload the contents of `src_dir` to the destination prefix.
async fn upload_dir_recursive<C: Codec>(
    client: &Client<C>,
    src_dir: &Path,
    dst_root_url: &str,
) -> Result<(), CliError> {
    let root = src_dir.to_path_buf();
    let tree = tokio::task::spawn_blocking(move || SourceTree::open_root(&root))
        .await
        .map_err(CliError::Task)??;
    let tree = Arc::new(tree);

    // The walk runs ahead of the uploads by at most the channel's capacity, so
    // memory does not grow with the number of files. When this function stops
    // early the receiver is dropped, the next send fails and the walk ends.
    let (tx, mut rx) = tokio::sync::mpsc::channel(2 * FILES_IN_FLIGHT);
    let producer = Arc::clone(&tree);
    drop(tokio::task::spawn_blocking(move || {
        let walked = producer.walk(&mut |_| {}, &mut |file| match tx.blocking_send(Ok(file)) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        });
        if let Err(e) = walked {
            let _ = tx.blocking_send(Err(e));
        }
    }));
    let files = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
    let tree = &tree;

    // Files in flight at once, and the budget that large files draw on. A file
    // of at least one part buffers several parts while it uploads, so it takes
    // `LARGE_FILE_WEIGHT` of the `FILES_IN_FLIGHT` permits and only a couple of
    // them run together, which bounds memory to a few part buffers.
    let budget = Semaphore::new(FILES_IN_FLIGHT);

    // `try_collect` stops at the first failure: no further file is started and
    // the uploads still running are dropped, which aborts them. `main` then
    // waits for those aborts to finish.
    files
        .map(|file| {
            let budget = &budget;
            async move {
                let file = file?;
                let dst_url = destination_url(dst_root_url, &file.rel)?;
                let path = tree.display_path(&file);

                outln!("Upload {} -> {}", show_path(&path), show_url(&dst_url)).await?;
                // Each open walks the directory chain from the root: on a slow
                // file system that must not stall the uploads in flight.
                let opener = Arc::clone(tree);
                let local = tokio::task::spawn_blocking(move || opener.open(&file))
                    .await
                    .map_err(CliError::Task)??;
                let size = local.size;
                let source = tokio::fs::File::from_std(local.file);
                // A regular file's mode is copied as it is on a `file://` destination.
                let mode = cfg!(unix).then_some(local.mode);
                run_weighted(
                    budget,
                    weight(size, FILES_IN_FLIGHT),
                    put_opened(
                        client,
                        source,
                        &dst_url,
                        &path,
                        (Some(size), Some(size), mode),
                    ),
                )
                .await
            }
        })
        .buffer_unordered(FILES_IN_FLIGHT)
        .try_collect::<()>()
        .await
}

#[cfg(test)]
mod tests;
