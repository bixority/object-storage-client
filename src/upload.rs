//! Streaming uploads: one `put` for small input, bounded multipart otherwise.

use crate::backend::Backend;
use crate::client::{Error, Result};
use crate::codec::ByteStream;
use bytes::Bytes;
use futures::{StreamExt, stream};
use object_store::{
    Attributes, MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutPayloadMut, PutResult, UpdateVersion, UploadPart, WriteMultipart,
    path::Path as ObjectPath,
};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;
use tokio::sync::Notify;

/// Tuning for [`upload_stream`].
pub struct UploadOptions {
    /// Bytes buffered before switching to multipart; also the part size.
    pub part_size: usize,
    /// Maximum part uploads in flight; fewer when parts are so large that
    /// more would exceed the memory budget (32 MiB).
    pub max_concurrency: usize,
    /// The permission bits of the local file a copy or move reads from. A new
    /// `file://` destination takes them (less the umask) instead of
    /// `0o666 & !umask`, so a streamed copy keeps a private file private.
    pub source_mode: Option<u32>,
    /// The expected size of the input in bytes, if known. Providers allow at
    /// most 10,000 parts per upload, so a large input gets parts of about
    /// `size / 9000` (never below `part_size`) instead of failing past
    /// 10,000 * `part_size` (78 GiB by default). It is a hint, not a limit:
    /// input that turns out larger still uploads while the part count allows.
    pub size_hint: Option<u64>,
    /// Commit only if the object at the destination still has this version:
    /// the store is asked again immediately before the final write (the single
    /// `put`, the multipart completion or the `file://` rename), and a single
    /// `put` is also made conditional where the store supports it. A changed
    /// or vanished object aborts the upload with [`Error::Changed`].
    pub if_unchanged: Option<UpdateVersion>,
    /// Content attributes (type, encoding, cache control, metadata) stored
    /// with the object. A `file://` destination ignores them. Empty by
    /// default.
    pub attributes: Attributes,
    /// The most a backend without multipart upload may buffer for its single
    /// `put`. Larger input fails with [`Error::InvalidInput`] before any
    /// request. Tests lower it.
    pub max_single_put: usize,
    /// The memory budget for parts in flight; see [`UPLOAD_BUDGET`].
    #[cfg(test)]
    pub byte_budget: usize,
}

/// The default of [`UploadOptions::max_single_put`] (1 GiB). `object_store`'s
/// HTTP store cannot stream a body, so the buffer can only be bounded.
const MAX_SINGLE_PUT: usize = 1 << 30;

/// Part bytes a multipart upload keeps in memory, in flight or filling, apart
/// from the one part being filled. Uploads in 8 MiB parts keep four parts in
/// flight; larger parts (a big `size_hint`) get fewer, down to one.
const UPLOAD_BUDGET: usize = 32 * 1024 * 1024;

/// The largest object the providers store (5 TiB). A larger hint is a
/// claim by the source, not a fact, and must not inflate the part size.
const MAX_OBJECT_SIZE: u64 = 5 << 40;

impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            part_size: 8 * 1024 * 1024,
            max_concurrency: 4,
            source_mode: None,
            size_hint: None,
            if_unchanged: None,
            attributes: Attributes::new(),
            max_single_put: MAX_SINGLE_PUT,
            #[cfg(test)]
            byte_budget: UPLOAD_BUDGET,
        }
    }
}

impl UploadOptions {
    /// The part size to use: `part_size`, or for a large `size_hint` the
    /// smallest size that keeps the input within 9000 parts, which leaves a
    /// 1% margin for the growth of re-encryption (about 28 bytes per 64 KiB)
    /// and a hint that is slightly off.
    fn effective_part_size(&self) -> usize {
        let Some(size) = self.size_hint else {
            return self.part_size;
        };
        let wanted = (u128::from(size.min(MAX_OBJECT_SIZE)) * 101).div_ceil(100 * 9000);
        usize::try_from(wanted).map_or(usize::MAX, |n| n.max(self.part_size))
    }

    /// How many parts of `part_size` bytes may be in flight: `max_concurrency`,
    /// reduced so that they fit the memory budget (at least one).
    fn parallelism(&self, part_size: usize) -> usize {
        #[cfg(test)]
        let budget = self.byte_budget;
        #[cfg(not(test))]
        let budget = UPLOAD_BUDGET;
        (budget / part_size.max(1)).clamp(1, self.max_concurrency.max(1))
    }

    /// Options for a copy or move of `size` bytes whose source is `path` on
    /// `backend`.
    pub(crate) fn copying_from(backend: &dyn Backend, path: &ObjectPath, size: u64) -> Self {
        Self {
            source_mode: local::source_mode(backend, path),
            size_hint: Some(size),
            ..Self::default()
        }
    }
}

/// The check made right before an upload commits; see
/// [`UploadOptions::if_unchanged`].
#[derive(Debug, Clone)]
struct Guard {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    expect: UpdateVersion,
}

impl Guard {
    fn new(backend: &dyn Backend, path: &ObjectPath, opts: &UploadOptions) -> Option<Self> {
        Some(Self {
            store: Arc::clone(backend.store()),
            path: path.clone(),
            expect: opts.if_unchanged.clone()?,
        })
    }

    /// Fails with a `Precondition` error unless the object is still the
    /// expected one. Only the identifiers the store reports are compared.
    async fn verify(&self) -> object_store::Result<()> {
        let changed = || object_store::Error::Precondition {
            path: self.path.to_string(),
            source: "the object changed during the update".into(),
        };
        let meta = match self.store.head(&self.path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Err(changed()),
            Err(e) => return Err(e),
        };
        let differs = |want: &Option<String>, got: &Option<String>| want.is_some() && want != got;
        if differs(&self.expect.e_tag, &meta.e_tag) || differs(&self.expect.version, &meta.version)
        {
            return Err(changed());
        }
        Ok(())
    }
}

/// With a guard, a `Precondition` failure is the dedicated [`Error::Changed`].
fn changed_or(guard: Option<&Guard>, e: object_store::Error) -> Error {
    match e {
        object_store::Error::Precondition { .. } if guard.is_some() => Error::Changed,
        e => e.into(),
    }
}

/// Stores `data` at `path`. A `file://` destination goes through the
/// private-staging local writer. Data of at least one part (8 MiB) goes to a
/// backend with multipart support as a multipart upload, like `put_stream`
/// input, so it is neither refused for its size nor bound to one request's
/// timeout; everything else is one `put`.
pub async fn put_bytes(backend: &dyn Backend, path: &ObjectPath, data: Bytes) -> Result<()> {
    if let Some(dest) = backend.local_path(path)? {
        let chunk: Result<Bytes> = Ok(data);
        return local::write(dest, stream::iter([chunk]).boxed(), None, None).await;
    }
    let defaults = UploadOptions::default();
    if backend.supports_multipart() && data.len() >= defaults.part_size {
        let opts = UploadOptions {
            size_hint: Some(data.len() as u64),
            ..defaults
        };
        let chunk: Result<Bytes> = Ok(data);
        return upload_stream(backend, path, stream::iter([chunk]).boxed(), &opts).await;
    }
    backend.store().put(path, data.into()).await?;
    Ok(())
}

/// Spawns `abort()` on the wrapped upload if dropped before it completed or
/// was aborted, including mid-`finish`.
#[derive(Debug)]
struct AbortOnDrop(Option<Box<dyn MultipartUpload>>, Option<Guard>);

impl AbortOnDrop {
    fn inner(&mut self) -> object_store::Result<&mut Box<dyn MultipartUpload>> {
        self.0.as_mut().ok_or_else(|| object_store::Error::Generic {
            store: "upload",
            source: "multipart upload already finished".into(),
        })
    }
}

#[async_trait::async_trait]
impl MultipartUpload for AbortOnDrop {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        match self.inner() {
            Ok(upload) => upload.put_part(data),
            Err(e) => Box::pin(async { Err(e) }),
        }
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if let Some(guard) = &self.1 {
            guard.verify().await?;
        }
        let result = self.inner()?.complete().await?;
        self.0 = None;
        Ok(result)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let result = self.inner()?.abort().await;
        self.0 = None;
        result
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(mut upload) = self.0.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let pending = PendingAbort::new();
            handle.spawn(async move {
                let _ = upload.abort().await;
                drop(pending);
            });
        }
    }
}

/// How many abort tasks spawned by dropped uploads are still alive.
static PENDING_ABORTS: AtomicUsize = AtomicUsize::new(0);
/// Notified whenever a [`PendingAbort`] is dropped.
static ABORT_DONE: Notify = Notify::const_new();

/// Counts one abort task in [`PENDING_ABORTS`] for as long as it lives, so a
/// cancelled or panicked task is uncounted too.
struct PendingAbort;

impl PendingAbort {
    fn new() -> Self {
        PENDING_ABORTS.fetch_add(1, SeqCst);
        Self
    }
}

impl Drop for PendingAbort {
    fn drop(&mut self) {
        PENDING_ABORTS.fetch_sub(1, SeqCst);
        ABORT_DONE.notify_waiters();
    }
}

/// Waits, for at most `timeout`, for the aborts of uploads that were dropped
/// mid-way (cancelled commands) to finish, so a pending multipart upload or a
/// staging file is not left behind when the process exits right after.
#[cfg_attr(not(any(feature = "cli", feature = "python")), allow(dead_code))]
#[doc(hidden)]
pub async fn drain_pending_aborts(timeout: Duration) {
    let drained = async {
        loop {
            let mut done = pin!(ABORT_DONE.notified());
            // Registered before the check, so a drop in between is not missed.
            done.as_mut().enable();
            if PENDING_ABORTS.load(SeqCst) == 0 {
                return;
            }
            done.await;
        }
    };
    let _ = tokio::time::timeout(timeout, drained).await;
}

/// Chunks below this size are copied into the writer's own buffer instead of
/// being queued as separate segments, so many tiny chunks cannot pin memory.
const COALESCE_BELOW: usize = 64 * 1024;

/// Queues `bytes`, waiting for capacity first. Small chunks are coalesced by
/// the writer; larger ones are handed over zero-copy in slices of `part_size`.
async fn write(
    writer: &mut WriteMultipart,
    mut bytes: Bytes,
    part_size: usize,
    parallel: usize,
) -> Result<()> {
    if bytes.len() < COALESCE_BELOW {
        writer.wait_for_capacity(parallel).await?;
        writer.write(&bytes);
        return Ok(());
    }
    while !bytes.is_empty() {
        let n = bytes.len().min(part_size);
        writer.wait_for_capacity(parallel).await?;
        writer.put(bytes.split_to(n));
    }
    Ok(())
}

/// Uploads `data` to `path`, committing only if the input ends cleanly.
///
/// Input shorter than one part is stored with a single `put`. So is any input
/// for a backend without multipart support, which buffers it all and refuses
/// more than [`UploadOptions::max_single_put`].
///
/// # Errors
///
/// [`Error::InvalidInput`] when a backend without multipart support is given
/// more than the cap, before anything is sent.
pub async fn upload_stream(
    backend: &dyn Backend,
    path: &ObjectPath,
    mut data: ByteStream,
    opts: &UploadOptions,
) -> Result<()> {
    if let Some(dest) = backend.local_path(path)? {
        let guard = Guard::new(backend, path, opts);
        return local::write(dest, data, opts.source_mode, guard.as_ref()).await;
    }
    let store = backend.store();
    let multipart = backend.supports_multipart();
    let part_size = opts.effective_part_size();
    let too_big = || {
        Error::InvalidInput(format!(
            "uploads to {} are buffered in memory and limited to {} bytes",
            backend.scheme(),
            opts.max_single_put
        ))
    };
    // The hint comes from the source, but a source that says it is too big is
    // refused before any of it is read.
    if !multipart
        && opts
            .size_hint
            .is_some_and(|size| size > opts.max_single_put as u64)
    {
        return Err(too_big());
    }
    // Nothing is sized by the hint: memory is allocated as data arrives. Small
    // chunks are copied into blocks of their own, so they cannot pin the
    // chunks they came in; large ones are kept as they are.
    let mut payload = PutPayloadMut::new().with_block_size(COALESCE_BELOW);

    while let Some(chunk) = data.next().await {
        let chunk = chunk?;
        if !multipart && payload.content_length().saturating_add(chunk.len()) > opts.max_single_put
        {
            return Err(too_big());
        }
        if chunk.len() < COALESCE_BELOW {
            payload.extend_from_slice(&chunk);
        } else {
            payload.push(chunk);
        }
        if multipart && payload.content_length() >= part_size {
            return upload_multipart(backend, path, payload.freeze(), data, part_size, opts).await;
        }
    }

    let payload = payload.freeze();
    match Guard::new(backend, path, opts) {
        None => {
            let put = PutOptions {
                attributes: opts.attributes.clone(),
                ..PutOptions::default()
            };
            store.put_opts(path, payload, put).await?;
            Ok(())
        }
        Some(guard) => put_if_unchanged(&guard, payload, &opts.attributes).await,
    }
}

/// The single `put` of an upload with a guard: verified first, then
/// conditional on the version where the store supports that. A store that does
/// not (or reports no e-tag to match) gets an unconditional put, the check
/// before it being all there is.
async fn put_if_unchanged(
    guard: &Guard,
    payload: PutPayload,
    attributes: &Attributes,
) -> Result<()> {
    let guarded = Some(guard);
    guard.verify().await.map_err(|e| changed_or(guarded, e))?;
    if guard.expect.e_tag.is_some() {
        let opts = PutOptions {
            mode: PutMode::Update(guard.expect.clone()),
            attributes: attributes.clone(),
            ..PutOptions::default()
        };
        match guard
            .store
            .put_opts(&guard.path, payload.clone(), opts)
            .await
        {
            Err(object_store::Error::NotImplemented { .. }) => {}
            result => return result.map(drop).map_err(|e| changed_or(guarded, e)),
        }
    }
    let opts = PutOptions {
        attributes: attributes.clone(),
        ..PutOptions::default()
    };
    guard.store.put_opts(&guard.path, payload, opts).await?;
    Ok(())
}

async fn upload_multipart(
    backend: &dyn Backend,
    path: &ObjectPath,
    head: PutPayload,
    mut data: ByteStream,
    part_size: usize,
    opts: &UploadOptions,
) -> Result<()> {
    let multipart = PutMultipartOptions {
        attributes: opts.attributes.clone(),
        ..PutMultipartOptions::default()
    };
    let upload = backend.store().put_multipart_opts(path, multipart).await?;
    let guard = Guard::new(backend, path, opts);
    let mut writer = WriteMultipart::new_with_chunk_size(
        Box::new(AbortOnDrop(Some(upload), guard.clone())),
        part_size,
    );

    let parallel = opts.parallelism(part_size);
    let fed = async {
        for chunk in head {
            write(&mut writer, chunk, part_size, parallel).await?;
        }
        while let Some(chunk) = data.next().await {
            write(&mut writer, chunk?, part_size, parallel).await?;
        }
        Ok(())
    }
    .await;

    match fed {
        // `finish` consumes the writer and itself awaits `abort()` on any
        // failure (part or `complete`) before returning the error, so no
        // upload is left to the drop guard.
        Ok(()) => {
            // `finish` queues the last, partial part without waiting, so make
            // room for it first: no more than `parallel` parts ever run.
            if let Err(e) = writer.wait_for_capacity(parallel).await {
                let _ = writer.abort().await;
                return Err(changed_or(guard.as_ref(), e));
            }
            writer
                .finish()
                .await
                .map_err(|e| changed_or(guard.as_ref(), e))?;
            Ok(())
        }
        Err(e) => {
            let _ = writer.abort().await;
            Err(e)
        }
    }
}

/// Writes to `file://` destinations. `object_store`'s local `put` creates its
/// staging file with the umask default mode and so exposes the data, and
/// replaces a private destination with a world-readable file. Here the staging
/// file is private until it is complete.
/// Destinations whose next `file://` sync fails, for tests.
#[cfg(test)]
pub static SYNC_FAULTS: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

mod local {
    use super::{Backend, ByteStream, Guard, ObjectPath, Result};
    #[cfg(unix)]
    use crate::client::Error;
    #[cfg(unix)]
    use crate::fsmode;
    #[cfg(unix)]
    use futures::StreamExt;
    #[cfg(unix)]
    use std::io;
    use std::path::{Path, PathBuf};

    /// Mode bits a copy takes from a local source, if `path` is one.
    pub(super) fn source_mode(backend: &dyn Backend, path: &ObjectPath) -> Option<u32> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let local = backend.local_path(path).ok()??;
            Some(std::fs::metadata(local).ok()?.permissions().mode() & 0o777)
        }
        #[cfg(not(unix))]
        {
            let _ = (backend, path);
            None
        }
    }

    /// Removes the staging file unless disarmed, also when the upload future
    /// is dropped.
    struct Staging(Option<PathBuf>);

    impl Staging {
        fn path(&self) -> &Path {
            self.0.as_deref().unwrap_or_else(|| Path::new(""))
        }

        fn disarm(&mut self) {
            self.0 = None;
        }
    }

    impl Drop for Staging {
        fn drop(&mut self) {
            if let Some(path) = self.0.take() {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    #[cfg(unix)]
    fn io_err(e: &io::Error, what: &str, path: &Path) -> Error {
        Error::Io(io::Error::new(
            e.kind(),
            format!("Unable to {what} {}: {e}", path.display()),
        ))
    }

    /// Creates `{dest}#{n}` exclusively (the name `object_store`'s own staging
    /// files use, which its listings skip), with the parent directories.
    #[cfg(unix)]
    fn create_staging(dest: &Path, create_mode: Option<u32>) -> Result<(std::fs::File, Staging)> {
        use std::os::unix::fs::OpenOptionsExt;
        let mut n = 1_u64;
        loop {
            let mut name = dest.as_os_str().to_owned();
            name.push(format!("#{n}"));
            let path = PathBuf::from(name);
            let mut options = std::fs::OpenOptions::new();
            options
                .write(true)
                .create_new(true)
                .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits().cast_signed());
            if let Some(mode) = create_mode {
                options.mode(mode);
            }
            match options.open(&path) {
                Ok(file) => return Ok((file, Staging(Some(path)))),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => n += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    let parent = dest.parent().ok_or_else(|| io_err(&e, "create", &path))?;
                    std::fs::create_dir_all(parent)
                        .map_err(|e| io_err(&e, "create directory", parent))?;
                }
                Err(e) => return Err(io_err(&e, "create", &path)),
            }
        }
    }

    /// Streams `data` into a private staging file next to `dest`, applies the
    /// final mode and renames it over `dest`. On any error, or if the future
    /// is dropped, the staging file is removed and `dest` is untouched.
    #[cfg(unix)]
    pub(super) async fn write(
        dest: PathBuf,
        mut data: ByteStream,
        source_mode: Option<u32>,
        guard: Option<&Guard>,
    ) -> Result<()> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        use tokio::io::AsyncWriteExt;

        let target = dest.clone();
        // The closure owns the guard, so a dropped future cannot leak the file.
        let (file, mut staging, last) = tokio::task::spawn_blocking(move || {
            // A regular file keeps its mode; a symlink is replaced, never
            // followed, and counts as a new file; nothing else is touched.
            let existing = match std::fs::symlink_metadata(&target) {
                Ok(m) if m.is_file() => Some((m.mode(), m.uid())),
                Ok(m) if m.is_symlink() => None,
                Ok(_) => {
                    return Err(Error::InvalidInput(format!(
                        "{} is not a regular file",
                        target.display()
                    )));
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                Err(e) => return Err(io_err(&e, "stat", &target)),
            };
            let euid = rustix::process::geteuid().as_raw();
            let (create, last) =
                fsmode::modes(existing, euid, source_mode, fsmode::umask_near(&target));
            create_staging(&target, create).map(|(f, s)| (f, s, last))
        })
        .await
        .map_err(|e| Error::Generic(format!("Local write task failed: {e}")))??;
        let staged = staging.path().to_owned();

        let mut file = tokio::fs::File::from_std(file);
        while let Some(chunk) = data.next().await {
            file.write_all(&chunk?)
                .await
                .map_err(|e| io_err(&e, "write", &staged))?;
        }
        file.flush()
            .await
            .map_err(|e| io_err(&e, "write", &staged))?;
        if let Some(mode) = last {
            file.set_permissions(std::fs::Permissions::from_mode(mode))
                .await
                .map_err(|e| io_err(&e, "set permissions of", &staged))?;
        }
        // Only a successful fsync reports a deferred write error (full disk,
        // quota, I/O error), and `drop` would discard a failing close. A
        // failure leaves the staging guard to remove the file, so the old
        // destination stays.
        #[cfg(test)]
        if super::SYNC_FAULTS
            .lock()
            .is_ok_and(|set| set.contains(&dest))
        {
            return Err(io_err(&io::Error::other("injected"), "sync", &staged));
        }
        file.sync_all()
            .await
            .map_err(|e| io_err(&e, "sync", &staged))?;
        drop(file);
        if let Some(guard) = guard {
            // On a mismatch the staging guard removes the file.
            guard
                .verify()
                .await
                .map_err(|e| super::changed_or(Some(guard), e))?;
        }
        tokio::fs::rename(&staged, &dest)
            .await
            .map_err(|e| io_err(&e, "rename onto", &dest))?;
        staging.disarm();
        // Make the rename durable before a move deletes its source.
        let parent = dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_owned();
        tokio::task::spawn_blocking(move || {
            std::fs::File::open(&parent)
                .and_then(|dir| dir.sync_all())
                .map_err(|e| io_err(&e, "sync directory", &parent))
        })
        .await
        .map_err(|e| Error::Generic(format!("Local write task failed: {e}")))?
    }

    #[cfg(not(unix))]
    pub(super) async fn write(
        _dest: PathBuf,
        _data: ByteStream,
        _source_mode: Option<u32>,
        _guard: Option<&Guard>,
    ) -> Result<()> {
        unreachable!("only Unix backends have a local path")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::TestBackend;
    use crate::client::Error;
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, PutMultipartOptions, PutOptions,
    };
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct Counters {
        put: AtomicUsize,
        multipart: AtomicUsize,
        abort: AtomicUsize,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        /// Bytes handed to `put_part` whose upload has not finished.
        bytes_in_flight: AtomicUsize,
        /// Largest value `bytes_in_flight` reached.
        peak_bytes: AtomicUsize,
        parts: AtomicUsize,
        /// Largest number of payload segments seen in a single part.
        max_segments: AtomicUsize,
        /// Zero-based index of a part whose upload fails.
        fail_part: Option<usize>,
        /// Start addresses of every payload segment put, to tell a handed-over
        /// chunk from a copy of it.
        segment_ptrs: Mutex<Vec<usize>>,
        /// Zero-based index from which every part upload fails (a provider's
        /// part limit).
        fail_from_part: Option<usize>,
        /// Extra latency added to every part upload.
        part_delay_ms: u64,
        /// Makes `complete` fail, i.e. the failure happens inside `finish`.
        fail_complete: bool,
    }

    /// An `InMemory` wrapper that counts calls and tracks part concurrency.
    #[derive(Debug)]
    struct FaultStore {
        inner: Arc<InMemory>,
        c: Arc<Counters>,
    }

    impl fmt::Display for FaultStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FaultStore")
        }
    }

    #[derive(Debug)]
    struct FaultUpload {
        inner: Box<dyn MultipartUpload>,
        c: Arc<Counters>,
    }

    #[async_trait]
    impl MultipartUpload for FaultUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let idx = self.c.parts.fetch_add(1, SeqCst);
            self.c.max_segments.fetch_max(data.iter().count(), SeqCst);
            self.c
                .segment_ptrs
                .lock()
                .unwrap()
                .extend(data.iter().map(|b| b.as_ptr() as usize));
            let len = data.content_length();
            let held = self.c.bytes_in_flight.fetch_add(len, SeqCst) + len;
            self.c.peak_bytes.fetch_max(held, SeqCst);
            let fut = self.inner.put_part(data);
            let c = Arc::clone(&self.c);
            Box::pin(async move {
                let now = c.in_flight.fetch_add(1, SeqCst) + 1;
                c.peak.fetch_max(now, SeqCst);
                tokio::time::sleep(Duration::from_millis(5 + c.part_delay_ms)).await;
                c.in_flight.fetch_sub(1, SeqCst);
                c.bytes_in_flight.fetch_sub(len, SeqCst);
                if c.fail_part == Some(idx) || c.fail_from_part.is_some_and(|from| idx >= from) {
                    return Err(object_store::Error::Generic {
                        store: "fault",
                        source: "part failed".into(),
                    });
                }
                fut.await
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            if self.c.fail_complete {
                return Err(object_store::Error::Generic {
                    store: "fault",
                    source: "complete failed".into(),
                });
            }
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.c.abort.fetch_add(1, SeqCst);
            self.inner.abort().await
        }
    }

    #[async_trait]
    impl ObjectStore for FaultStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.c.put.fetch_add(1, SeqCst);
            self.c
                .segment_ptrs
                .lock()
                .unwrap()
                .extend(payload.iter().map(|b| b.as_ptr() as usize));
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.c.multipart.fetch_add(1, SeqCst);
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(FaultUpload {
                inner,
                c: Arc::clone(&self.c),
            }))
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn fixture(
        multipart: bool,
        fail_part: Option<usize>,
    ) -> (TestBackend, Arc<FaultStore>, Arc<Counters>) {
        fixture_with(
            multipart,
            Counters {
                fail_part,
                ..Counters::default()
            },
        )
    }

    fn fixture_with(multipart: bool, c: Counters) -> (TestBackend, Arc<FaultStore>, Arc<Counters>) {
        let c = Arc::new(c);
        let store = Arc::new(FaultStore {
            inner: Arc::new(InMemory::new()),
            c: Arc::clone(&c),
        });
        let backend = TestBackend {
            store: store.clone(),
            multipart,
        };
        (backend, store, c)
    }

    fn path() -> ObjectPath {
        ObjectPath::from("obj")
    }

    /// Deterministic pseudo-random bytes split into irregular chunks.
    fn chunks(total: usize) -> (Vec<u8>, Vec<Bytes>) {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let data: Vec<u8> = (0..total).map(|_| next().to_le_bytes()[0]).collect();
        let mut out = Vec::new();
        let mut at = 0;
        while at < total {
            let n = (usize::try_from(next() % 3_000_000).unwrap_or(1) + 1).min(total - at);
            out.push(Bytes::copy_from_slice(&data[at..at + n]));
            at += n;
        }
        (data, out)
    }

    fn input(chunks: Vec<Bytes>) -> ByteStream {
        stream::iter(chunks.into_iter().map(Ok)).boxed()
    }

    async fn stored(store: &FaultStore) -> Bytes {
        store.get(&path()).await.unwrap().bytes().await.unwrap()
    }

    #[tokio::test]
    async fn small_input_single_put() {
        let (b, store, c) = fixture(true, None);
        let parts = vec![
            Bytes::from(vec![1u8; 400]),
            Bytes::from(vec![2u8; 400]),
            Bytes::from(vec![3u8; 224]),
        ];
        upload_stream(&b, &path(), input(parts), &UploadOptions::default())
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
        assert_eq!(stored(&store).await.len(), 1024);
    }

    #[tokio::test]
    async fn empty_input_creates_empty_object() {
        let (b, store, c) = fixture(true, None);
        upload_stream(&b, &path(), input(vec![]), &UploadOptions::default())
            .await
            .unwrap();
        assert_eq!(c.put.load(SeqCst), 1);
        assert!(stored(&store).await.is_empty());
    }

    #[tokio::test]
    async fn large_input_multipart_roundtrip() {
        let (b, store, c) = fixture(true, None);
        let (data, parts) = chunks(20 * 1024 * 1024);
        upload_stream(&b, &path(), input(parts), &UploadOptions::default())
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (0, 1));
        assert_eq!(c.abort.load(SeqCst), 0);
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn input_error_aborts_and_leaves_no_object() {
        let (b, store, c) = fixture(true, None);
        let (_, mut parts) = chunks(10 * 1024 * 1024);
        parts.push(Bytes::from_static(b"x"));
        let failing = stream::iter(parts.into_iter().map(Ok))
            .chain(stream::once(async { Err(Error::Generic("boom".into())) }))
            .boxed();
        let err = upload_stream(&b, &path(), failing, &UploadOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Generic(_)));
        assert_eq!(c.abort.load(SeqCst), 1);
        assert!(matches!(
            store.head(&path()).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn failed_part_aborts_and_leaves_no_object() {
        let (b, store, c) = fixture(true, Some(1));
        let (_, parts) = chunks(20 * 1024 * 1024);
        upload_stream(&b, &path(), input(parts), &UploadOptions::default())
            .await
            .unwrap_err();
        assert!(store.head(&path()).await.is_err());
        assert!(c.abort.load(SeqCst) >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_future_aborts() {
        let (b, _store, c) = fixture(true, None);
        let (_, parts) = chunks(10 * 1024 * 1024);
        let hang = stream::iter(parts.into_iter().map(Ok))
            .chain(stream::pending())
            .boxed();
        let timed = tokio::time::timeout(
            Duration::from_millis(100),
            upload_stream(&b, &path(), hang, &UploadOptions::default()),
        )
        .await;
        assert!(timed.is_err());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(c.abort.load(SeqCst), 1);
    }

    #[tokio::test]
    async fn in_flight_parts_bounded() {
        let (b, _store, c) = fixture(true, None);
        // One huge chunk must still be sliced and throttled.
        let big = vec![Bytes::from(vec![9u8; 64 * 1024 * 1024])];
        upload_stream(&b, &path(), input(big), &UploadOptions::default())
            .await
            .unwrap();
        let peak = c.peak.load(SeqCst);
        assert_eq!(peak, 4);
        let opts = UploadOptions::default();
        let held = c.peak_bytes.load(SeqCst);
        assert!(
            held <= opts.max_concurrency * opts.part_size,
            "{held} bytes were in flight"
        );
    }

    #[tokio::test]
    async fn small_chunks_keep_bytes_in_flight_bounded() {
        let (b, store, c) = fixture_with(
            true,
            Counters {
                part_delay_ms: 20,
                ..Counters::default()
            },
        );
        let opts = UploadOptions {
            part_size: 256 * 1024,
            max_concurrency: 4,
            ..UploadOptions::default()
        };
        let data: Vec<u8> = (0..9 * opts.part_size + 100)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let chunks: Vec<Bytes> = data.chunks(4096).map(Bytes::copy_from_slice).collect();
        upload_stream(&b, &path(), input(chunks), &opts)
            .await
            .unwrap();
        assert!(c.parts.load(SeqCst) >= 8, "{} parts", c.parts.load(SeqCst));
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
        let (parts, held) = (c.peak.load(SeqCst), c.peak_bytes.load(SeqCst));
        assert!(parts <= opts.max_concurrency, "{parts} parts in flight");
        assert!(
            held <= opts.max_concurrency * opts.part_size,
            "{held} bytes in flight"
        );
    }

    #[tokio::test]
    async fn tiny_chunks_are_coalesced() {
        let (b, store, c) = fixture(true, None);
        let opts = UploadOptions {
            part_size: 256 * 1024,
            max_concurrency: 4,
            ..UploadOptions::default()
        };
        let data: Vec<u8> = (0..3 * opts.part_size + 100)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let parts: Vec<Bytes> = data.chunks(16).map(Bytes::copy_from_slice).collect();
        upload_stream(&b, &path(), input(parts), &opts)
            .await
            .unwrap();
        assert_eq!(c.multipart.load(SeqCst), 1);
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
        let segments = c.max_segments.load(SeqCst);
        assert!(segments <= 64, "a part had {segments} segments");
    }

    #[tokio::test]
    async fn large_first_chunk_skips_buffer() {
        let (b, store, c) = fixture(true, None);
        let opts = UploadOptions {
            part_size: 256 * 1024,
            max_concurrency: 4,
            ..UploadOptions::default()
        };
        let data = vec![7u8; 2 * opts.part_size + 5];
        let parts = vec![Bytes::from(data.clone()), Bytes::from_static(b"tail")];
        upload_stream(&b, &path(), input(parts), &opts)
            .await
            .unwrap();
        assert_eq!(c.multipart.load(SeqCst), 1);
        let mut want = data;
        want.extend_from_slice(b"tail");
        assert_eq!(stored(&store).await.as_ref(), want.as_slice());
    }

    #[tokio::test]
    async fn large_chunk_after_a_small_one_is_not_copied() {
        let (b, store, c) = fixture(true, None);
        let opts = UploadOptions {
            part_size: 256 * 1024,
            max_concurrency: 4,
            ..UploadOptions::default()
        };
        let head = Bytes::from(vec![1u8; 1000]);
        let big = Bytes::from(
            (0..3 * opts.part_size)
                .map(|i| u8::try_from(i % 253).unwrap())
                .collect::<Vec<_>>(),
        );
        let mut want = head.to_vec();
        want.extend_from_slice(&big);
        let second_part = big.as_ptr() as usize + (opts.part_size - head.len());
        let parts = vec![head, big, Bytes::from_static(b"tail")];
        want.extend_from_slice(b"tail");
        upload_stream(&b, &path(), input(parts), &opts)
            .await
            .unwrap();
        assert_eq!(c.multipart.load(SeqCst), 1);
        assert_eq!(stored(&store).await.as_ref(), want.as_slice());
        assert!(
            c.segment_ptrs.lock().unwrap().contains(&second_part),
            "the large chunk was copied"
        );
    }

    #[tokio::test]
    async fn size_hint_keeps_a_large_upload_within_the_part_limit() {
        let limit = || Counters {
            fail_from_part: Some(10_000),
            ..Counters::default()
        };
        // 12 000 parts at the base size, about 8 600 at the hinted size.
        let data: Vec<u8> = (0..12_000 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let opts = |size_hint| UploadOptions {
            part_size: 1024,
            max_concurrency: 256,
            size_hint,
            ..UploadOptions::default()
        };
        let one = || input(vec![Bytes::from(data.clone())]);

        let (b, store, c) = fixture_with(true, limit());
        upload_stream(&b, &path(), one(), &opts(None))
            .await
            .unwrap_err();
        assert!(store.head(&path()).await.is_err());
        assert_eq!(c.abort.load(SeqCst), 1);

        let (b, store, c) = fixture_with(true, limit());
        let hint = Some(data.len() as u64);
        upload_stream(&b, &path(), one(), &opts(hint))
            .await
            .unwrap();
        assert!(c.parts.load(SeqCst) <= 9_000);
        assert_eq!(c.abort.load(SeqCst), 0);
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn a_huge_size_hint_does_not_preallocate() {
        // Without multipart the largest hint the cap allows is 1 GiB.
        for (multipart, hint) in [(true, 1_u64 << 60), (false, 1 << 30)] {
            let (b, store, _) = fixture(multipart, None);
            let opts = UploadOptions {
                size_hint: Some(hint),
                ..UploadOptions::default()
            };
            upload_stream(
                &b,
                &path(),
                input(vec![Bytes::from(vec![7_u8; 1024])]),
                &opts,
            )
            .await
            .unwrap();
            assert_eq!(stored(&store).await.as_ref(), vec![7_u8; 1024].as_slice());
        }
        let capped = |hint| {
            UploadOptions {
                size_hint: Some(hint),
                ..UploadOptions::default()
            }
            .effective_part_size()
        };
        assert_eq!(capped(u64::MAX), capped(5 << 40));
        assert!(capped(5 << 40) > 8 * 1024 * 1024);
    }

    #[test]
    fn the_hinted_part_size_never_drops_below_the_base_size() {
        let opts = |size_hint| UploadOptions {
            size_hint,
            ..UploadOptions::default()
        };
        let base = 8 * 1024 * 1024;
        assert_eq!(opts(None).effective_part_size(), base);
        assert_eq!(opts(Some(0)).effective_part_size(), base);
        assert_eq!(opts(Some(1 << 30)).effective_part_size(), base);
        let tib = 1_u64 << 40;
        let part = opts(Some(tib)).effective_part_size();
        assert!(part > base);
        assert!(tib.div_ceil(part as u64) < 9_000);
        assert!(opts(Some(u64::MAX)).effective_part_size() > base);
    }

    #[tokio::test]
    async fn no_multipart_backend_buffers_then_puts() {
        let (b, store, c) = fixture(false, None);
        let (data, parts) = chunks(20 * 1024 * 1024);
        upload_stream(&b, &path(), input(parts), &UploadOptions::default())
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn small_input_error_puts_nothing() {
        let (b, store, c) = fixture(true, None);
        let failing = stream::iter([Ok(Bytes::from(vec![1u8; 1024]))])
            .chain(stream::once(async { Err(Error::Generic("boom".into())) }))
            .boxed();
        upload_stream(&b, &path(), failing, &UploadOptions::default())
            .await
            .unwrap_err();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (0, 0));
        assert!(matches!(
            store.head(&path()).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_during_finish_aborts() {
        let (b, store, c) = fixture_with(
            true,
            Counters {
                part_delay_ms: 300,
                ..Counters::default()
            },
        );
        // Input ends cleanly; parts are still uploading when `finish` runs.
        let (_, parts) = chunks(20 * 1024 * 1024);
        let timed = tokio::time::timeout(
            Duration::from_millis(100),
            upload_stream(&b, &path(), input(parts), &UploadOptions::default()),
        )
        .await;
        assert!(timed.is_err());
        // The whole input was consumed and every part queued (20 MiB in 8 MiB
        // parts), so the drop happened inside `finish`.
        assert_eq!(c.parts.load(SeqCst), 3);
        assert_eq!(c.abort.load(SeqCst), 0, "not aborted before the drop");
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(store.head(&path()).await.is_err());
        assert_eq!(
            c.abort.load(SeqCst),
            1,
            "upload leaked after drop in finish"
        );
    }

    #[tokio::test]
    async fn failed_finish_aborts_explicitly() {
        let (b, store, c) = fixture_with(
            true,
            Counters {
                fail_complete: true,
                ..Counters::default()
            },
        );
        let (_, parts) = chunks(20 * 1024 * 1024);
        upload_stream(&b, &path(), input(parts), &UploadOptions::default())
            .await
            .unwrap_err();
        // Checked immediately, with no yield for a spawned drop-guard abort:
        // the abort must already have been awaited by the time we return.
        assert_eq!(c.abort.load(SeqCst), 1);
        assert!(store.head(&path()).await.is_err());
    }

    #[tokio::test]
    async fn a_finished_multipart_upload_refuses_further_use() {
        let store = InMemory::new();
        let path = ObjectPath::from("done");
        let upload = store.put_multipart(&path).await.unwrap();
        let mut guarded = AbortOnDrop(Some(upload), None);
        guarded
            .put_part(Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        guarded.complete().await.unwrap();

        assert!(
            guarded
                .put_part(Bytes::from_static(b"x").into())
                .await
                .is_err()
        );
        assert!(guarded.complete().await.is_err());
        assert!(guarded.abort().await.is_err());
        drop(guarded);
        // The committed object is untouched by the refused calls and the drop.
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), b"abc");
    }

    /// A multipart upload whose `abort` takes a while and records its end.
    #[derive(Debug)]
    struct SlowAbort(Arc<AtomicUsize>);

    #[async_trait]
    impl MultipartUpload for SlowAbort {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(async { Ok(()) })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            unreachable!("never completed")
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.0.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn drain_pending_aborts_waits_for_a_dropped_uploads_abort() {
        let done = Arc::new(AtomicUsize::new(0));
        drop(AbortOnDrop(
            Some(Box::new(SlowAbort(Arc::clone(&done)))),
            None,
        ));
        assert_eq!(done.load(SeqCst), 0, "abort runs in the background");

        drain_pending_aborts(Duration::from_secs(10)).await;
        assert_eq!(done.load(SeqCst), 1, "drain awaited the abort");
    }

    #[tokio::test(start_paused = true)]
    async fn drain_pending_aborts_is_bounded_by_its_timeout() {
        let done = Arc::new(AtomicUsize::new(0));
        drop(AbortOnDrop(
            Some(Box::new(SlowAbort(Arc::clone(&done)))),
            None,
        ));
        let started = tokio::time::Instant::now();
        drain_pending_aborts(Duration::from_millis(1)).await;
        // Virtual time: the drain gave up at its timeout, before the 100 ms
        // abort could finish.
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(done.load(SeqCst), 0, "the abort had not completed");
    }

    /// The version of `path` in `store`, as a reencrypt would have read it.
    async fn version_of(store: &dyn ObjectStore, path: &ObjectPath) -> Option<UpdateVersion> {
        let meta = store.head(path).await.ok()?;
        Some(UpdateVersion {
            e_tag: meta.e_tag,
            version: meta.version,
        })
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_sync_leaves_the_destination_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("obj");
        std::fs::write(&file, b"old").unwrap();
        let url = url::Url::from_file_path(&file).unwrap();
        let client = crate::Client::<crate::Plain>::default();
        let (backend, path) = client.get_backend(&url).unwrap();
        let dest = backend.local_path(&path).unwrap().unwrap();

        SYNC_FAULTS.lock().unwrap().push(dest.clone());
        let err = put_bytes(backend.as_ref(), &path, Bytes::from_static(b"new")).await;
        SYNC_FAULTS.lock().unwrap().retain(|p| *p != dest);

        let err = err.unwrap_err();
        assert!(err.to_string().contains("sync"), "{err}");
        assert_eq!(std::fs::read(&file).unwrap(), b"old");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["obj"]);

        // Without the fault the same write goes through.
        put_bytes(backend.as_ref(), &path, Bytes::from_static(b"new"))
            .await
            .unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"new");
    }

    /// The default creation mode of a new file in `dir`: `0o666` less the
    /// umask, learned from a file created here.
    #[cfg(unix)]
    fn default_mode_in(dir: &std::path::Path) -> u32 {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let path = dir.join("independent-probe");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&path)
            .unwrap();
        let mode = file.metadata().unwrap().permissions().mode() & 0o777;
        std::fs::remove_file(path).unwrap();
        mode
    }

    #[cfg(unix)]
    fn mode_of(path: &std::path::Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn new_file_staging_is_private_without_proc() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("obj");
        let url = url::Url::from_file_path(&file).unwrap();
        let client = crate::Client::<crate::Plain>::default();
        let (backend, path) = client.get_backend(&url).unwrap();
        let dest = backend.local_path(&path).unwrap().unwrap();
        let want = default_mode_in(dir.path());

        crate::fsmode::PROBE_ONLY.lock().unwrap().push(dest.clone());
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(1);
        let data = stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed();
        let task = tokio::spawn(async move {
            upload_stream(backend.as_ref(), &path, data, &UploadOptions::default()).await
        });
        tx.send(Ok(Bytes::from_static(b"secret"))).await.unwrap();
        let staging = dir.path().join("obj#1");
        for _ in 0..400 {
            if std::fs::metadata(&staging).is_ok_and(|m| m.len() == 6) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let private = mode_of(&staging);
        drop(tx);
        let done = task.await.unwrap();
        crate::fsmode::PROBE_ONLY
            .lock()
            .unwrap()
            .retain(|p| *p != dest);

        done.unwrap();
        assert_eq!(private, 0o600, "staging file while the write is running");
        assert_eq!(mode_of(&file), want);
        assert_eq!(std::fs::read(&file).unwrap(), b"secret");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["obj"], "no staging or probe file is left");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_private_source_stays_private_when_the_probe_fails() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("obj");
        let url = url::Url::from_file_path(&file).unwrap();
        let client = crate::Client::<crate::Plain>::default();
        let (backend, path) = client.get_backend(&url).unwrap();
        let dest = backend.local_path(&path).unwrap().unwrap();

        crate::fsmode::PROBE_ONLY.lock().unwrap().push(dest.clone());
        crate::fsmode::PROBE_FAULTS
            .lock()
            .unwrap()
            .push(dest.clone());
        let opts = UploadOptions {
            source_mode: Some(0o600),
            ..UploadOptions::default()
        };
        let done = upload_stream(
            backend.as_ref(),
            &path,
            input(vec![Bytes::from_static(b"x")]),
            &opts,
        )
        .await;
        crate::fsmode::PROBE_ONLY
            .lock()
            .unwrap()
            .retain(|p| *p != dest);
        crate::fsmode::PROBE_FAULTS
            .lock()
            .unwrap()
            .retain(|p| *p != dest);

        done.unwrap();
        assert_eq!(mode_of(&file), 0o600);
    }

    #[tokio::test]
    async fn file_destination_is_renamed_only_if_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("obj");
        let url = url::Url::from_file_path(&file).unwrap();
        let client = crate::Client::<crate::Plain>::default();
        let (backend, path) = client.get_backend(&url).unwrap();
        let names = || {
            let mut names: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            names.sort();
            names
        };

        std::fs::write(&file, b"old").unwrap();
        let version = version_of(backend.store().as_ref(), &path).await.unwrap();
        let opts = UploadOptions {
            if_unchanged: Some(version.clone()),
            ..UploadOptions::default()
        };
        upload_stream(
            backend.as_ref(),
            &path,
            input(vec![Bytes::from_static(b"newer")]),
            &opts,
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"newer");

        // The object changed since `version` was taken: nothing is renamed
        // and no staging file stays.
        let err = upload_stream(
            backend.as_ref(),
            &path,
            input(vec![Bytes::from_static(b"newest")]),
            &opts,
        )
        .await;
        assert!(matches!(err, Err(Error::Changed)), "{err:?}");
        assert_eq!(std::fs::read(&file).unwrap(), b"newer");
        assert_eq!(names(), ["obj"]);

        // A vanished object counts as changed too, and is not recreated.
        let opts = UploadOptions {
            if_unchanged: version_of(backend.store().as_ref(), &path).await,
            ..UploadOptions::default()
        };
        std::fs::remove_file(&file).unwrap();
        let err = upload_stream(
            backend.as_ref(),
            &path,
            input(vec![Bytes::from_static(b"x")]),
            &opts,
        )
        .await;
        assert!(matches!(err, Err(Error::Changed)), "{err:?}");
        assert_eq!(names().len(), 0);
    }

    #[tokio::test]
    async fn guarded_single_put_and_multipart_commit_only_if_unchanged() {
        for multipart in [false, true] {
            let (b, store, c) = fixture(multipart, None);
            store
                .put(&path(), Bytes::from_static(b"v1").into())
                .await
                .unwrap();
            let version = version_of(&store, &path()).await;
            let opts = UploadOptions {
                if_unchanged: version,
                ..UploadOptions::default()
            };
            let (data, parts) = chunks(9 * 1024 * 1024);
            // A concurrent writer replaces the object before the commit.
            store
                .put(&path(), Bytes::from_static(b"v2").into())
                .await
                .unwrap();
            let err = upload_stream(&b, &path(), input(parts), &opts).await;
            assert!(matches!(err, Err(Error::Changed)), "{err:?}");
            assert_eq!(stored(&store).await.as_ref(), b"v2");
            assert_eq!(c.abort.load(SeqCst), usize::from(multipart));

            // Still the version it was: committed.
            let opts = UploadOptions {
                if_unchanged: version_of(&store, &path()).await,
                ..UploadOptions::default()
            };
            let (_, parts) = chunks(9 * 1024 * 1024);
            upload_stream(&b, &path(), input(parts), &opts)
                .await
                .unwrap();
            assert_eq!(stored(&store).await.as_ref(), data.as_slice());
        }
    }

    // ---- the cap on non-multipart uploads (CORE-HTTPCAP) ----

    fn capped(multipart: bool) -> (TestBackend, Arc<Counters>, UploadOptions) {
        let (b, _, c) = fixture(multipart, None);
        let opts = UploadOptions {
            max_single_put: 1 << 20,
            ..UploadOptions::default()
        };
        (b, c, opts)
    }

    #[tokio::test]
    async fn a_non_multipart_upload_over_the_cap_fails_before_any_put() {
        let (b, c, opts) = capped(false);
        let chunks = vec![Bytes::from(vec![1_u8; 512 * 1024]); 3];
        let err = upload_stream(&b, &path(), input(chunks), &opts)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
        assert!(err.to_string().contains(&(1 << 20).to_string()), "{err}");
        assert_eq!(c.put.load(SeqCst), 0);
    }

    #[tokio::test]
    async fn a_hinted_oversize_upload_fails_without_reading() {
        let (b, c, mut opts) = capped(false);
        opts.size_hint = Some(2 << 20);
        let polled = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&polled);
        let data = stream::poll_fn(move |_| {
            seen.fetch_add(1, SeqCst);
            std::task::Poll::Ready(None)
        })
        .boxed();
        let err = upload_stream(&b, &path(), data, &opts).await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
        assert_eq!(polled.load(SeqCst), 0, "the input was read");
        assert_eq!(c.put.load(SeqCst), 0);
    }

    #[tokio::test]
    async fn an_upload_at_the_cap_succeeds() {
        let (b, c, opts) = capped(false);
        let chunks = vec![Bytes::from(vec![1_u8; 512 * 1024]); 2];
        upload_stream(&b, &path(), input(chunks), &opts)
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
    }

    // ---- large put bodies (CORE-BIGPUT) ----

    #[tokio::test]
    async fn a_large_put_uses_multipart() {
        let (b, store, c) = fixture(true, None);
        let (data, _) = chunks(20 * 1024 * 1024);
        put_bytes(&b, &path(), Bytes::from(data.clone()))
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (0, 1));
        assert_eq!(c.abort.load(SeqCst), 0);
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn a_small_put_stays_single() {
        let (b, store, c) = fixture(true, None);
        put_bytes(&b, &path(), Bytes::from(vec![5_u8; 1024]))
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
        assert_eq!(stored(&store).await.len(), 1024);
    }

    #[tokio::test]
    async fn a_large_put_without_multipart_stays_single() {
        let (b, store, c) = fixture(false, None);
        put_bytes(&b, &path(), Bytes::from(vec![5_u8; 20 * 1024 * 1024]))
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
        assert_eq!(stored(&store).await.len(), 20 * 1024 * 1024);
    }

    // ---- single-put payload (CORE-PUTPAYLOAD) ----

    #[tokio::test]
    async fn a_large_single_put_chunk_is_not_copied() {
        let (b, store, c) = fixture(true, None);
        let chunk = Bytes::from(vec![3_u8; 1024 * 1024]);
        let ptr = chunk.as_ptr() as usize;
        upload_stream(
            &b,
            &path(),
            input(vec![chunk.clone()]),
            &UploadOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
        assert_eq!(
            *c.segment_ptrs.lock().unwrap(),
            [ptr],
            "the chunk was copied"
        );
        assert_eq!(stored(&store).await, chunk);
    }

    #[tokio::test]
    async fn tiny_chunks_are_coalesced_for_a_single_put() {
        let (b, store, c) = fixture(true, None);
        let data: Vec<u8> = (0..1000 * 16)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let tiny: Vec<Bytes> = data.chunks(16).map(Bytes::copy_from_slice).collect();
        upload_stream(&b, &path(), input(tiny), &UploadOptions::default())
            .await
            .unwrap();
        assert_eq!((c.put.load(SeqCst), c.multipart.load(SeqCst)), (1, 0));
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
        // 16 000 bytes fit one block; a segment per chunk would be 1000.
        let segments = c.segment_ptrs.lock().unwrap().len();
        assert_eq!(segments, 1, "{segments} segments");
    }

    // ---- the memory budget (CORE-UPLOAD-BUDGET) ----

    #[tokio::test]
    async fn parts_in_flight_fit_the_byte_budget() {
        let (b, store, c) = fixture_with(
            true,
            Counters {
                part_delay_ms: 20,
                ..Counters::default()
            },
        );
        let mib = 1024 * 1024;
        // The hint makes parts of 1 000 000 bytes; the budget holds two of them.
        let opts = UploadOptions {
            part_size: 64 * 1024,
            max_concurrency: 4,
            size_hint: Some(8_910_891_000),
            byte_budget: 2 * mib,
            ..UploadOptions::default()
        };
        assert_eq!(opts.effective_part_size(), 1_000_000);
        let (data, parts) = chunks(12 * mib);
        upload_stream(&b, &path(), input(parts), &opts)
            .await
            .unwrap();
        assert!(c.parts.load(SeqCst) >= 10, "{} parts", c.parts.load(SeqCst));
        let (in_flight, held) = (c.peak.load(SeqCst), c.peak_bytes.load(SeqCst));
        assert_eq!(in_flight, 2, "parts in flight");
        assert!(held <= 2 * mib, "{held} bytes in flight");
        assert_eq!(c.abort.load(SeqCst), 0);
        assert_eq!(stored(&store).await.as_ref(), data.as_slice());
    }

    #[test]
    fn the_default_budget_keeps_four_parts_up_to_8_mib() {
        let mib = 1024 * 1024;
        let opts = UploadOptions::default();
        assert_eq!(opts.byte_budget, 32 * mib);
        assert_eq!(opts.parallelism(8 * mib), 4);
        assert_eq!(opts.parallelism(5 * mib), 4);
        assert_eq!(opts.parallelism(16 * mib), 2);
        assert_eq!(opts.parallelism(100 * mib), 1);
    }
}
