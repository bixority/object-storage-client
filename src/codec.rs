//! The [`Codec`] seam between [`Client`](crate::Client) and stored bytes.
//!
//! [`Plain`] is the identity codec. The trait is sealed: only this crate
//! provides implementations.

use crate::backend::{Backend, body_stream};
use crate::client::{Error, ObjectMetadata, Result};
use crate::upload::{UploadOptions, upload_stream};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attribute, GetOptions, GetResult, GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt,
    path::Path as ObjectPath,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

/// The most memory reserved up front for a body whose size the provider
/// declared. Buffers grow as data arrives, so a hostile length costs nothing.
pub(crate) const MAX_PREALLOC: usize = 16 << 20;

/// Caller-supplied key/value pairs a codec may bind to an object. The
/// encrypted client reserves keys starting with `osc:` or `aws-crypto-`.
pub type CallerContext = BTreeMap<String, String>;

/// A boxed stream of byte chunks.
pub type ByteStream = BoxStream<'static, Result<Bytes>>;

pub(crate) mod sealed {
    use super::{
        Backend, ByteStream, Bytes, CallerContext, GetResult, ObjectMetadata, ObjectPath, Result,
        Url,
    };
    use futures::stream::BoxStream;
    use std::sync::Arc;

    /// Where an object lives: its parsed URL and its path within the store.
    #[derive(Debug, Clone)]
    pub struct Location {
        pub url: Url,
        pub path: ObjectPath,
    }

    /// A [`Location`] together with the backend serving it.
    #[derive(Clone)]
    pub struct Resolved {
        pub backend: Arc<dyn Backend>,
        pub loc: Location,
        /// Identity of the registry (client) that resolved it.
        pub registry: usize,
    }

    /// The seam methods of a [`Codec`](super::Codec).
    pub trait CodecImpl {
        /// Encodes a whole object for storage.
        fn encode(
            &self,
            loc: &Location,
            ctx: &CallerContext,
            data: Bytes,
        ) -> impl Future<Output = Result<Bytes>> + Send;

        /// Rejects a write context the codec would refuse, before any I/O.
        fn check_write(&self, loc: &Location, ctx: &CallerContext) -> Result<()>;

        /// Encodes a stream of plaintext chunks into stored bytes.
        fn encode_stream(&self, loc: Location, ctx: CallerContext, plain: ByteStream)
        -> ByteStream;

        /// Decodes a stream of stored bytes into plaintext chunks.
        fn decode(
            &self,
            loc: Location,
            stored: BoxStream<'static, object_store::Result<Bytes>>,
        ) -> ByteStream;

        /// Reads and decodes a whole object.
        fn get_bytes(
            &self,
            loc: &Location,
            result: GetResult,
        ) -> impl Future<Output = Result<Bytes>> + Send;

        /// Describes an object: a HEAD for [`Plain`]; for the encrypted codec,
        /// one ranged read of the header, with `size_bytes` and `encryption` taken
        /// from unauthenticated data.
        fn describe(
            &self,
            backend: &dyn Backend,
            loc: &Location,
        ) -> impl Future<Output = Result<ObjectMetadata>> + Send;

        /// Copies (or moves, when `delete_source`) an object between locations.
        fn copy(
            &self,
            src: &Resolved,
            dst: &Resolved,
            delete_source: bool,
        ) -> impl Future<Output = Result<()>> + Send;

        /// Copies an object whose size the caller knows from a listing. The
        /// plain codec skips the `HEAD` that otherwise finds the size; a
        /// codec that streams every copy ignores it.
        fn copy_sized(
            &self,
            src: &Resolved,
            dst: &Resolved,
            _size: Option<u64>,
        ) -> impl Future<Output = Result<()>> + Send {
            self.copy(src, dst, false)
        }
    }
}

pub(crate) use sealed::{Location, Resolved};

impl Location {
    /// The `osc:key` an encrypted message written to this location carries
    /// when keys are bound: the object path, which for `file://` is the
    /// absolute path.
    pub(crate) fn bound_key(&self) -> &str {
        self.path.as_ref()
    }
}

/// The first bytes of every envelope message: version 2 and suite `0x0478`.
const OSC_PREFIX: [u8; 3] = [0x02, 0x04, 0x78];

/// Transforms object bytes on their way to and from a store. Sealed: only this
/// crate provides implementations, and the seam methods are private to it.
pub trait Codec: sealed::CodecImpl + Clone + Send + Sync + 'static {}

/// The identity codec: objects are stored as given.
#[derive(Clone, Copy, Debug, Default)]
pub struct Plain;

impl Codec for Plain {}

impl sealed::CodecImpl for Plain {
    fn encode(
        &self,
        _loc: &Location,
        _ctx: &CallerContext,
        data: Bytes,
    ) -> impl Future<Output = Result<Bytes>> + Send {
        std::future::ready(Ok(data))
    }

    fn check_write(&self, _loc: &Location, _ctx: &CallerContext) -> Result<()> {
        Ok(())
    }

    fn encode_stream(&self, _loc: Location, _ctx: CallerContext, plain: ByteStream) -> ByteStream {
        plain
    }

    fn decode(
        &self,
        _loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
    ) -> ByteStream {
        stored.map(|chunk| chunk.map_err(Error::from)).boxed()
    }

    async fn get_bytes(&self, _loc: &Location, result: GetResult) -> Result<Bytes> {
        if matches!(result.payload, GetResultPayload::File(..)) {
            return Ok(result.bytes().await?);
        }
        // The range is the provider's claim: reserve a bounded amount and
        // grow as data arrives instead of trusting it.
        let declared = usize::try_from(result.range.end - result.range.start).unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(declared.min(MAX_PREALLOC));
        let mut stream = body_stream(result);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if out.capacity() - out.len() < chunk.len() {
                // Grow by at most the bytes already received, so a false size
                // cannot force a large allocation; an honest one ends with
                // no spare room, which `Bytes` would otherwise keep.
                let left = declared.saturating_sub(out.len());
                let more = if left > 0 {
                    left.min(out.len())
                } else {
                    out.len()
                };
                out.reserve_exact(more.max(chunk.len()));
            }
            out.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(out))
    }

    async fn describe(&self, backend: &dyn Backend, loc: &Location) -> Result<ObjectMetadata> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };

        let result = backend.store().get_opts(&loc.path, options).await?;
        let content_type = result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_string());
        let meta = result.meta;

        Ok(ObjectMetadata {
            location: meta.location.to_string(),
            last_modified: meta.last_modified,
            size_bytes: meta.size,
            stored_size_bytes: meta.size,
            content_type,
            e_tag: meta.e_tag,
            version: meta.version,
            encryption: None,
        })
    }

    async fn copy(&self, src: &Resolved, dst: &Resolved, delete_source: bool) -> Result<()> {
        self.copy_known(src, dst, delete_source, None).await
    }

    async fn copy_sized(&self, src: &Resolved, dst: &Resolved, size: Option<u64>) -> Result<()> {
        self.copy_known(src, dst, false, size).await
    }
}

impl Plain {
    /// [`copy`](sealed::CodecImpl::copy) with the source size when it is known.
    async fn copy_known(
        &self,
        src: &Resolved,
        dst: &Resolved,
        delete_source: bool,
        size: Option<u64>,
    ) -> Result<()> {
        // A copy or move onto itself is a no-op: a rename of an object onto
        // itself is copy-then-delete on cloud stores and would destroy it.
        if same_object(src, dst).await {
            return Ok(());
        }
        refuse_special_source(src).await?;
        refuse_to_unbind(src, dst).await?;
        if server_side_copy(src, dst, delete_source, size).await? {
            return Ok(());
        }
        let from_store = src.backend.store();
        let got = from_store.get(&src.loc.path).await?;
        let read = got.meta.clone();
        let attributes = copied_attributes(
            &got.attributes,
            store_identity(&src.loc.url).0 == store_identity(&dst.loc.url).0,
        );
        let stored = body_stream(got)
            .map(|chunk| chunk.map_err(Error::from))
            .boxed();
        let options = UploadOptions {
            attributes,
            move_source: delete_source,
            ..UploadOptions::copying_from(src.backend.as_ref(), &src.loc.path, read.size)
        };
        upload_stream(dst.backend.as_ref(), &dst.loc.path, stored, &options).await?;
        if delete_source {
            delete_unchanged_source(src, &read).await?;
        }
        Ok(())
    }
}

/// Refuses a byte copy that would leave an encrypted object unreadable, with
/// a ranged GET of the source's first 1 KiB (more only for a larger header). The header binds the object key
/// (`osc:key`) in clear text, so no keys are needed. An object that is not an
/// envelope message passes; so does one with no binding or with the
/// destination's own key. A header that starts like ours but does not parse
/// is refused as well.
///
/// # Errors
/// [`Error::InvalidInput`] for an object bound to another name, and storage
/// errors of the probe.
async fn refuse_to_unbind(src: &Resolved, dst: &Resolved) -> Result<()> {
    // A typical header is a few hundred bytes: read 1 KiB, and the rest only
    // when the header does not fit.
    #[cfg(feature = "encryption")]
    let (first, max) = (
        1024,
        crate::encryption::DecodeLimits::default().max_header_len,
    );
    #[cfg(not(feature = "encryption"))]
    let first = OSC_PREFIX.len();
    #[cfg_attr(not(feature = "encryption"), allow(unused_mut))]
    let mut head = read_start(src, first as u64).await?;
    if !head.starts_with(&OSC_PREFIX) {
        return Ok(());
    }
    #[cfg(feature = "encryption")]
    if head.len() == first
        && max > first
        && matches!(
            crate::encryption::header::HeaderCodec::parse(
                &head,
                &crate::encryption::DecodeLimits::default()
            ),
            Ok(crate::encryption::header::HeaderParse::NeedMore)
        )
    {
        head = read_start(src, max as u64).await?;
    }
    let url = redacted(&src.loc.url);
    #[cfg(feature = "encryption")]
    {
        use crate::encryption::context::KEY_OBJECT;
        use crate::encryption::header::{HeaderCodec, HeaderParse};
        match HeaderCodec::parse(&head, &crate::encryption::DecodeLimits::default()) {
            Ok(HeaderParse::Done { header, .. }) => {
                if header
                    .context
                    .get(KEY_OBJECT)
                    .is_none_or(|bound| bound == dst.loc.bound_key())
                {
                    return Ok(());
                }
                return Err(Error::InvalidInput(format!(
                    "{url} is encrypted and bound to its name; copy it with the encrypted client, which re-encrypts"
                )));
            }
            Ok(HeaderParse::NeedMore) | Err(_) => {}
        }
    }
    // Without the `encryption` feature the header cannot be read, so any
    // object that starts like a message is treated as bound to its own name.
    #[cfg(not(feature = "encryption"))]
    if src.loc.bound_key() == dst.loc.bound_key() {
        return Ok(());
    }
    Err(Error::InvalidInput(format!(
        "{url} looks encrypted and may be bound to its name; copy it with the encrypted client, which re-encrypts"
    )))
}

/// Up to `len` bytes from the start of an object, with one ranged GET. When
/// the range is refused (an empty object, which stores refuse to range over,
/// or an http(s) server without range support), the start of a plain GET is
/// read instead and the rest of its body is dropped.
async fn read_start(src: &Resolved, len: u64) -> Result<Bytes> {
    let options = GetOptions {
        range: Some(object_store::GetRange::Bounded(0..len)),
        ..Default::default()
    };
    let store = src.backend.store();
    let result = match store.get_opts(&src.loc.path, options).await {
        Ok(r) => r,
        Err(e @ object_store::Error::NotFound { .. }) => return Err(e.into()),
        Err(e) => match store.get(&src.loc.path).await {
            Ok(r) => r,
            Err(_) => return Err(e.into()),
        },
    };
    // A store may answer with more than was asked for: keep only `len`.
    let len = usize::try_from(len).unwrap_or(usize::MAX);
    let mut buf = BytesMut::new();
    let mut body = body_stream(result);
    while buf.len() < len
        && let Some(chunk) = body.next().await
    {
        let chunk = chunk?;
        let take = chunk.len().min(len - buf.len());
        buf.extend_from_slice(&chunk[..take]);
    }
    Ok(buf.freeze())
}

/// The attributes a streamed copy keeps: the content headers always, user
/// metadata only within one kind of store (`same_provider`), and never the
/// storage class, whose values are provider-specific.
fn copied_attributes(
    attributes: &object_store::Attributes,
    same_provider: bool,
) -> object_store::Attributes {
    attributes
        .iter()
        .filter(|(name, _)| match name {
            Attribute::ContentType
            | Attribute::ContentEncoding
            | Attribute::ContentDisposition
            | Attribute::ContentLanguage
            | Attribute::CacheControl => true,
            Attribute::Metadata(_) => same_provider,
            _ => false,
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Deletes the source of a streamed move, but only if it is still the object
/// that was read and copied: `read` is the metadata of the body GET.
///
/// Two names of one origin (an IP and a hostname, a CNAME) cannot be told
/// apart by comparing URLs, and then the copy overwrote the source itself.
/// A concurrent writer is caught the same way. A source that is gone is left
/// alone; one that differs in any metadata field the store provides is kept
/// and reported as [`Error::SourceChanged`].
///
/// This is a best effort: a rewrite with identical bytes, in the same second,
/// on a store that reports neither e-tag nor version is indistinguishable
/// from the original, and a writer can still land between the HEAD and the
/// delete.
pub(crate) async fn delete_unchanged_source(src: &Resolved, read: &ObjectMeta) -> Result<()> {
    let store = src.backend.store();
    match store.head(&src.loc.path).await {
        Ok(now) if now == *read => {}
        Ok(_) => {
            return Err(Error::SourceChanged {
                url: redacted(&src.loc.url),
            });
        }
        Err(object_store::Error::NotFound { .. }) => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    match store.delete(&src.loc.path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// A URL for messages: no userinfo, query or fragment.
pub(crate) fn redacted(url: &Url) -> String {
    let mut url = url.clone();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

/// Removes what a URL carries beyond its location: the user name and
/// password, the query and the fragment, which may hold tokens (the library's
/// [`crate::codec::redacted`] policy). A dropped query or fragment leaves `?…`
/// behind. Text that does not parse as a URL is cut at the first `?` or `#`
/// and loses any `user:password@` in its authority.
#[must_use]
#[cfg_attr(not(feature = "cli"), allow(dead_code))]
pub(crate) fn redact_url(text: &str) -> String {
    if let Ok(url) = url::Url::parse(text) {
        let mut out = crate::codec::redacted(&url);
        if url.query().is_some() || url.fragment().is_some() {
            out.push_str("?…");
        }
        return out;
    }
    let cut = text.find(['?', '#']);
    let head = cut.map_or(text, |i| &text[..i]);
    let head = match head.split_once("://") {
        Some((scheme, rest)) => {
            let (auth, path) = rest
                .split_once('/')
                .map_or((rest, None), |(a, p)| (a, Some(p)));
            let auth = auth.rsplit_once('@').map_or(auth, |(_, host)| host);
            path.map_or_else(
                || format!("{scheme}://{auth}"),
                |p| format!("{scheme}://{auth}/{p}"),
            )
        }
        None => head.to_string(),
    };
    if cut.is_some() {
        format!("{head}?…")
    } else {
        head
    }
}

/// The store a URL addresses, normalised so aliases compare equal: `gs` and
/// `gcs`; the Azure schemes (`az`, `wasb`, `wasbs`, `abfs`, `abfss`), which all
/// take the URL host as the container of the one configured account; and
/// `http` with `https` (host and explicit port, scheme default ports being
/// ignored). HTTP hosts compare case-insensitively and without one trailing
/// dot; S3 bucket names compare exactly, because legacy and S3-compatible
/// buckets are case-sensitive (GCS and Azure names are lowercase anyway).
/// `file` ignores the host, as the local backend does (`file://127.0.0.1/D/x`
/// aliases `file:///D/x`).
///
/// This is only an identity for deciding that two URLs name the same object;
/// it cannot see that two host names reach one origin. It says nothing about
/// which backend serves a URL: the registry keys backends on the full scheme,
/// userinfo, host and port, and a server-side copy needs the same backend
/// (see [`server_side_copy`]).
fn store_identity(url: &Url) -> (String, Option<String>, Option<u16>) {
    let lower = || url.host_str().map(str::to_ascii_lowercase);
    match url.scheme() {
        "file" => ("file".into(), None, None),
        "gs" | "gcs" => ("gcs".into(), lower(), None),
        "az" | "wasb" | "wasbs" | "abfs" | "abfss" => ("azure".into(), lower(), None),
        "http" | "https" => {
            let host = lower().map(|h| h.strip_suffix('.').unwrap_or(&h).to_owned());
            ("http".into(), host, url.port())
        }
        other => (other.into(), url.host_str().map(str::to_owned), None),
    }
}

/// Whether two URLs are served by the same store (see [`store_identity`]).
pub(crate) fn same_store(a: &Url, b: &Url) -> bool {
    store_identity(a) == store_identity(b)
}

/// Whether two existing paths are the same file, whatever route reaches it
/// (symlinked directories, a case-insensitive file system). A path that does
/// not exist is not the same file as anything.
fn same_file(a: &std::path::Path, b: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(ma), Ok(mb)) => ma.dev() == mb.dev() && ma.ino() == mb.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
            (Ok(ca), Ok(cb)) => ca == cb,
            _ => false,
        }
    }
}

/// Whether two locations carry the same name in the same store, without
/// looking at the file system or the registry.
pub(crate) fn same_name(a: &Location, b: &Location) -> bool {
    same_store(&a.url, &b.url) && a.path == b.path
}

/// Whether two resolved locations name the same object: the same path in the
/// same store of one registry, or, for `file`, two paths that reach one
/// existing file. The file system is read on a blocking thread.
pub(crate) async fn same_object(a: &Resolved, b: &Resolved) -> bool {
    // Two registries (clients) may hold different credentials or reach
    // different stores under the same URL.
    if a.registry != b.registry || !same_store(&a.loc.url, &b.loc.url) {
        return false;
    }
    if a.loc.path == b.loc.path {
        return true;
    }
    if a.loc.url.scheme() != "file" {
        return false;
    }
    // A path with no file system mapping is never the same object.
    let (Ok(a), Ok(b)) = (
        crate::backend::fs_path(&a.loc.path),
        crate::backend::fs_path(&b.loc.path),
    ) else {
        return false;
    };
    tokio::task::spawn_blocking(move || same_file(&a, &b))
        .await
        .unwrap_or(false)
}

/// Refuses a `file` source that is a directory, FIFO, socket or device with
/// [`Error::InvalidInput`], before anything reads it. A symlink passes: a move
/// renames the link, a copy reads its target.
async fn refuse_special_source(src: &Resolved) -> Result<()> {
    let Some(path) = src.backend.local_path(&src.loc.path)? else {
        return Ok(());
    };
    tokio::task::spawn_blocking(move || {
        match std::fs::symlink_metadata(&path) {
            // A source that cannot be examined is left to the read to report.
            Ok(m) if !(m.is_file() || m.is_symlink()) => Err(Error::InvalidInput(format!(
                "{} is not a regular file",
                path.display()
            ))),
            _ => Ok(()),
        }
    })
    .await
    .map_err(|e| Error::Generic(format!("Local stat task failed: {e}")))?
}

/// Whether a `file` move may be a plain `rename(2)`: the source is a regular
/// file or a symlink (renamed as a link, as `mv(1)` does), and the
/// destination, if it exists, is a regular file or a symlink. Otherwise the
/// streamed path decides and refuses what cannot be replaced. A path that
/// cannot be examined is left to the rename to report.
fn plain_rename(from: &std::path::Path, to: &std::path::Path) -> bool {
    let movable = |m: &std::fs::Metadata| m.is_file() || m.is_symlink();
    std::fs::symlink_metadata(from).map_or_else(|_| true, |m| movable(&m))
        && std::fs::symlink_metadata(to).map_or_else(|_| true, |m| movable(&m))
}

/// The largest source a single S3 `CopyObject` accepts (5 GiB).
const S3_MAX_COPY_SOURCE: u64 = 5 << 30;

/// Copies or renames inside one store. Returns `false` when the caller must
/// stream instead: the locations are not served by the very same backend
/// (the same registry entry, not merely equal-looking names, which may differ
/// in bucket case or credentials) and the destination backend does not
/// accept the source's ([`Backend::copies_from`]) or cannot copy it
/// ([`Backend::copy_from`]), a `file` copy (`object_store` makes it a hard
/// link, so file copies always stream through the private-staging writer), a
/// `file` rename crossed devices, an S3 source is too large for one
/// `CopyObject`, or an http(s) destination already exists. `size` is the source
/// size when the caller knows it; S3 looks
/// it up otherwise.
///
/// A `file` move is an atomic rename when both ends are regular files or
/// symlinks (see [`plain_rename`]); the file keeps its inode and its mode. The
/// check and the rename are not atomic together. Every other move is a copy followed by
/// a delete that happens only if the source is still what was copied
/// ([`delete_unchanged_source`]), because `object_store`'s default rename
/// deletes unconditionally.
pub(crate) async fn server_side_copy(
    src: &Resolved,
    dst: &Resolved,
    delete_source: bool,
    size: Option<u64>,
) -> Result<bool> {
    // Two backends of one provider may still copy server-side across buckets.
    let same_backend = Arc::ptr_eq(&src.backend, &dst.backend);
    if !same_backend && !dst.backend.copies_from(src.backend.as_ref()) {
        return Ok(false);
    }
    let store = src.backend.store();
    let scheme = src.loc.url.scheme();
    if scheme == "file" {
        if !delete_source {
            return Ok(false);
        }
        if let (Some(from), Some(to)) = (
            src.backend.local_path(&src.loc.path)?,
            dst.backend.local_path(&dst.loc.path)?,
        ) {
            let plain = tokio::task::spawn_blocking(move || plain_rename(&from, &to))
                .await
                .map_err(|e| Error::Generic(format!("Local stat task failed: {e}")))?;
            if !plain {
                return Ok(false);
            }
        }
        return match store.rename(&src.loc.path, &dst.loc.path).await {
            Ok(()) => Ok(true),
            Err(e) if crosses_devices(&e) => Ok(false),
            Err(e) => Err(e.into()),
        };
    }
    let before = if delete_source {
        Some(store.head(&src.loc.path).await?)
    } else {
        None
    };
    let mut looked_up = None;
    if scheme == "s3" {
        let known = match (size, &before) {
            (Some(size), _) => size,
            (None, Some(meta)) => meta.size,
            (None, None) => {
                let meta = store.head(&src.loc.path).await?;
                let size = meta.size;
                looked_up = Some(meta);
                size
            }
        };
        if known > S3_MAX_COPY_SOURCE {
            return Ok(false);
        }
    }
    if !same_backend {
        let known = before.as_ref().or(looked_up.as_ref());
        let copied = dst
            .backend
            .copy_from(src.backend.as_ref(), &src.loc.path, &dst.loc.path, known)
            .await;
        match copied {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(Error::SourceChanged { .. }) => {
                return Err(Error::SourceChanged {
                    url: redacted(&src.loc.url),
                });
            }
            Err(e) => return Err(e),
        }
    } else if matches!(scheme, "http" | "https") {
        // A WebDAV COPY without `Overwrite: F` replaces the destination,
        // and a collection there is deleted with all its members. Overwriting
        // an existing file is left to a streamed PUT, which a server refuses
        // on a collection.
        match store.copy_if_not_exists(&src.loc.path, &dst.loc.path).await {
            Ok(()) => {}
            Err(object_store::Error::AlreadyExists { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        }
    } else {
        store.copy(&src.loc.path, &dst.loc.path).await?;
    }
    dst.backend.finish_copy(&dst.loc.path).await?;
    if let Some(before) = before {
        if !same_backend {
            // A second guard before the source goes: the destination must
            // exist and be as large as the source that was read.
            let written = dst.backend.store().head(&dst.loc.path).await?;
            if written.size != before.size {
                return Err(Error::Generic(format!(
                    "The copy to {} has {} bytes, the source {}; the source was kept",
                    redacted(&dst.loc.url),
                    written.size,
                    before.size
                )));
            }
        }
        delete_unchanged_source(src, &before).await?;
    }
    Ok(true)
}

/// Whether an I/O error of kind `CrossesDevices` (EXDEV) is anywhere in the
/// error's source chain.
fn crosses_devices(err: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(err), |e| e.source()).any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::CrossesDevices)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_drops_secrets() {
        assert_eq!(
            redact_url("https://user:secret@host.example/a/b?X-Amz-Signature=abc#frag"),
            "https://host.example/a/b?…"
        );
        assert_eq!(redact_url("s3://bucket/key"), "s3://bucket/key");
        assert_eq!(redact_url("https://h/x?sig=1"), "https://h/x?…");
        assert_eq!(redact_url("https://:pw@h/x"), "https://h/x");
        assert_eq!(redact_url("https://tok@h/x?q=1"), "https://h/x?…");
        // Not parseable as a URL: still cut.
        let odd = redact_url("http://u:pw@[bad/x?token=1");
        assert!(!odd.contains("pw") && !odd.contains("token"), "{odd}");
    }

    fn at(url: &str) -> Location {
        let url = Url::parse(url).expect("url");
        let path = ObjectPath::from_url_path(url.path()).expect("path");
        Location { url, path }
    }

    #[tokio::test]
    async fn file_urls_are_the_same_store_whatever_the_host() {
        // `same_object` stats file paths: keep them in a private tempdir.
        let tmp = tempfile::tempdir().expect("tempdir");
        let file = |name: &str| {
            url::Url::from_file_path(tmp.path().join(name))
                .expect("file URL")
                .to_string()
        };
        let (_, backend) = memory_backend();
        let at = |url: &str| resolved(&backend, url);
        let x = file("x");
        let (a, b) = (at(&x), at(&x.replacen("file://", "file://127.0.0.1", 1)));
        assert!(same_object(&a, &b).await);
        assert!(!same_object(&a, &at(&file("y"))).await);
        assert!(!same_object(&at("s3://one/x"), &at("s3://two/x")).await);
        assert!(same_object(&at("s3://one/x"), &at("s3://one/x")).await);
        assert!(!same_store(
            &Url::parse("s3://b/x").expect("url"),
            &Url::parse("gs://b/x").expect("url")
        ));
    }

    #[tokio::test]
    async fn get_bytes_does_not_trust_the_declared_length() {
        let big = 1_u64 << 50;
        let chunks: Vec<object_store::Result<Bytes>> =
            vec![Ok(Bytes::from_static(b"ab")), Ok(Bytes::from_static(b"cd"))];
        let result = GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter(chunks).boxed()),
            meta: ObjectMeta {
                location: ObjectPath::from("a"),
                last_modified: chrono::DateTime::UNIX_EPOCH,
                size: big,
                e_tag: None,
                version: None,
            },
            range: 0..big,
            attributes: object_store::Attributes::new(),
            extensions: http::Extensions::new(),
        };
        let got = Plain
            .get_bytes(&at("test://b/a"), result)
            .await
            .expect("bytes");
        assert_eq!(got.as_ref(), b"abcd");
    }

    /// A streamed result that declares `declared` bytes and sends `chunks`.
    fn declared_result(declared: u64, chunks: Vec<Bytes>) -> GetResult {
        let chunks: Vec<object_store::Result<Bytes>> = chunks.into_iter().map(Ok).collect();
        GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter(chunks).boxed()),
            meta: ObjectMeta {
                location: ObjectPath::from("a"),
                last_modified: chrono::DateTime::UNIX_EPOCH,
                size: declared,
                e_tag: None,
                version: None,
            },
            range: 0..declared,
            attributes: object_store::Attributes::new(),
            extensions: http::Extensions::new(),
        }
    }

    #[tokio::test]
    async fn get_bytes_of_an_honest_length_holds_no_spare_capacity() {
        let chunk = Bytes::from(vec![7_u8; 64 * 1024]);
        let total = 20 << 20;
        let count = total / chunk.len();
        for declared in [total as u64, total as u64 / 2, 1] {
            let result = declared_result(declared, vec![chunk.clone(); count]);
            let got = Plain
                .get_bytes(&at("test://b/a"), result)
                .await
                .expect("bytes");
            assert_eq!(got.len(), total);
            // Only a declared length that was true ends with an exact fit.
            let spare = got.try_into_mut().expect("unique").capacity() - total;
            if declared == total as u64 {
                assert_eq!(spare, 0);
            }
        }
    }

    fn store_eq(a: &str, b: &str) -> bool {
        same_store(&Url::parse(a).expect("url"), &Url::parse(b).expect("url"))
    }

    #[test]
    fn store_identity_normalises_aliases() {
        assert!(store_eq("gs://b/x", "gcs://b/y"));
        assert!(store_eq("gs://B/x", "gcs://b/x"));
        assert!(!store_eq("gs://b/x", "gcs://c/x"));
        for s in ["az", "wasb", "wasbs", "abfs", "abfss"] {
            assert!(store_eq("az://c/x", &format!("{s}://c/x")), "{s}");
            assert!(!store_eq("az://c/x", &format!("{s}://d/x")), "{s}");
        }
        assert!(!store_eq("az://b/x", "gs://b/x"));
        assert!(!store_eq("s3://b/x", "gs://b/x"));
        assert!(store_eq("http://h/x", "https://H/x"));
        assert!(store_eq("http://h:8080/x", "https://h:8080/x"));
        assert!(!store_eq("http://h:8080/x", "http://h/x"));
        assert!(!store_eq("http://h/x", "http://other/x"));
        assert!(!store_eq("http://h:1/x", "http://h:2/x"));
        // S3 bucket names are case-sensitive on legacy and compatible stores.
        assert!(!store_eq("s3://Bucket/x", "s3://bucket/x"));
        assert!(store_eq("s3://bucket/x", "s3://bucket/y"));
        // One trailing dot of an HTTP host is the same host.
        assert!(store_eq("http://h./x", "http://h/x"));
        assert!(store_eq("https://H.:8080/x", "http://h:8080/x"));
        assert!(!store_eq("http://h../x", "http://h/x"));
        assert!(!store_eq("s3://one/x", "s3://two/x"));
    }

    #[tokio::test]
    async fn equal_urls_on_two_backends_are_not_the_same_object() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let url = url::Url::from_file_path(tmp.path().join("x"))
            .expect("file URL")
            .to_string();
        for url in [url.as_str(), "s3://b/x"] {
            let (one, two) = (memory_backend().1, memory_backend().1);
            let (a, b) = (resolved(&one, url), resolved(&two, url));
            assert!(!same_object(&a, &b).await, "{url}");
            assert!(same_object(&a, &resolved(&one, url)).await, "{url}");
        }
        // Aliases on two registries are two objects too.
        let (one, two) = (memory_backend().1, memory_backend().1);
        let (a, b) = (resolved(&one, "gs://b/x"), resolved(&two, "gcs://b/x"));
        assert!(!same_object(&a, &b).await);
    }

    #[tokio::test]
    async fn aliased_urls_name_the_same_object() {
        let (_, backend) = memory_backend();
        let at = |url: &str| resolved(&backend, url);
        assert!(same_object(&at("gs://b/x"), &at("gcs://b/x")).await);
        assert!(!same_object(&at("gs://b/x"), &at("gcs://b/y")).await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_paths_through_a_symlink_are_the_same_object() {
        let (_, backend) = memory_backend();
        let at = |url: &str| resolved(&backend, url);
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real");
        std::fs::create_dir(&real).expect("mkdir");
        std::os::unix::fs::symlink(&real, dir.path().join("link")).expect("symlink");
        std::fs::write(real.join("a"), b"x").expect("write");
        let url = |p: &str| format!("file://{}/{p}", dir.path().display());
        assert!(same_object(&at(&url("link/a")), &at(&url("real/a"))).await);
        // The destination must exist to be the same file.
        assert!(!same_object(&at(&url("link/a")), &at(&url("real/b"))).await);
        assert!(!same_object(&at(&url("link/missing")), &at(&url("real/b"))).await);
    }

    use crate::codec::sealed::CodecImpl as _;
    use object_store::memory::InMemory;

    /// One backend stands for one registry.
    fn resolved(backend: &Arc<dyn Backend>, url: &str) -> Resolved {
        Resolved {
            backend: Arc::clone(backend),
            registry: Arc::as_ptr(backend).cast::<()>() as usize,
            loc: at(url),
        }
    }

    fn memory_backend() -> (Arc<InMemory>, Arc<dyn Backend>) {
        let store = Arc::new(InMemory::new());
        let backend: Arc<dyn Backend> = Arc::new(crate::backend::TestBackend {
            store: store.clone(),
            multipart: false,
        });
        (store, backend)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_move_whose_sync_fails_keeps_its_source() {
        let (store, memory) = memory_backend();
        store
            .put(&ObjectPath::from("a"), "payload".into())
            .await
            .expect("put");
        let dir = tempfile::tempdir().expect("tempdir");
        let out = dir.path().join("out");
        let url = Url::from_file_path(&out).expect("url");
        let client = crate::Client::<Plain>::default();
        let (local, path) = client.get_backend(&url).expect("backend");
        let target = local.local_path(&path).expect("path").expect("local");
        let src = resolved(&memory, "test://b/a");
        let dst = Resolved {
            backend: local,
            registry: 0,
            loc: Location { url, path },
        };

        crate::upload::SYNC_FAULTS
            .lock()
            .expect("lock")
            .push(target.clone());
        let moved = Plain.copy(&src, &dst, true).await;
        crate::upload::SYNC_FAULTS
            .lock()
            .expect("lock")
            .retain(|p| *p != target);

        assert!(moved.is_err());
        assert!(store.head(&ObjectPath::from("a")).await.is_ok());
        assert_eq!(std::fs::read_dir(dir.path()).expect("dir").count(), 0);
    }

    /// Equal-looking names do not make a server-side copy: only the very same
    /// backend does, otherwise the copy would run as the source's bucket or
    /// credentials.
    #[tokio::test]
    async fn server_side_copy_needs_the_same_backend() {
        let (store, one) = memory_backend();
        let (other_store, two) = memory_backend();
        store
            .put(&ObjectPath::from("a"), "x".into())
            .await
            .expect("put");

        let src = resolved(&one, "test://b/a");
        let same = resolved(&one, "test://b/c");
        let elsewhere = resolved(&two, "test://B/c");
        assert!(
            server_side_copy(&src, &same, false, None)
                .await
                .expect("copy")
        );
        assert!(store.head(&ObjectPath::from("c")).await.is_ok());
        assert!(
            !server_side_copy(&src, &elsewhere, false, None)
                .await
                .expect("skip")
        );
        assert!(other_store.head(&ObjectPath::from("c")).await.is_err());
    }

    /// A same-origin http copy sends `Overwrite: F`, so a server never
    /// replaces a destination collection; a 412 means the destination exists
    /// and the caller must stream.
    #[tokio::test]
    async fn http_copy_never_overwrites_and_streams_onto_an_existing_destination() {
        use crate::backend::stub::{Reply, serve};
        let stub = serve(vec![Reply::new(201, ""), Reply::new(412, "")]).await;
        let url = Url::parse(&format!("{}/a", stub.base)).expect("url");
        let backend = crate::backend::build(
            &url,
            &crate::backend::Vars::Env,
            None,
            &crate::backend::Shared::default(),
        )
        .expect("backend");
        let (src, dst) = (
            resolved(&backend, &format!("{}/a", stub.base)),
            resolved(&backend, &format!("{}/b", stub.base)),
        );
        assert!(
            server_side_copy(&src, &dst, false, None)
                .await
                .expect("copy")
        );
        assert!(
            !server_side_copy(&src, &dst, false, None)
                .await
                .expect("exists")
        );
        let requests = stub.requests();
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(request.line, "COPY /a HTTP/1.1");
            let headers = request.headers.to_ascii_lowercase();
            assert!(headers.contains("overwrite: f"), "{headers}");
            assert!(headers.contains("destination: "), "{headers}");
        }
    }

    #[tokio::test]
    async fn plain_copy_between_backends_with_equal_names_streams() {
        let (store, one) = memory_backend();
        let (other_store, two) = memory_backend();
        store
            .put(&ObjectPath::from("a"), "x".into())
            .await
            .expect("put");
        let (src, dst) = (resolved(&one, "s3://Logs/a"), resolved(&two, "s3://logs/b"));

        Plain.copy(&src, &dst, true).await.expect("move");
        assert!(store.head(&ObjectPath::from("a")).await.is_err());
        assert!(store.head(&ObjectPath::from("b")).await.is_err());
        assert!(other_store.head(&ObjectPath::from("b")).await.is_ok());
    }

    /// What the [`Spy`] store does to the source right after a copy.
    #[derive(Debug, Default)]
    enum AfterCopy {
        #[default]
        Nothing,
        /// A concurrent writer replaces the source.
        Rewrite,
        /// A concurrent deleter removes the source.
        Delete,
    }

    /// An `InMemory` store that counts `HEAD` and copy calls and can change the
    /// source right after a copy.
    #[derive(Debug, Default)]
    struct Spy {
        inner: InMemory,
        heads: std::sync::atomic::AtomicUsize,
        copies: std::sync::atomic::AtomicUsize,
        ranges: std::sync::Mutex<Vec<Option<object_store::GetRange>>>,
        after_copy: AfterCopy,
    }

    impl std::fmt::Display for Spy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Spy")
        }
    }

    forward_object_store! {
        impl ObjectStore for Spy => inner;
        forward [put_opts, put_multipart_opts, delete_stream, list, list_with_delimiter];
        {
            async fn get_opts(
                &self,
                location: &ObjectPath,
                options: GetOptions,
            ) -> object_store::Result<GetResult> {
                if options.head {
                    self.heads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                } else {
                    self.ranges
                        .lock()
                        .expect("ranges")
                        .push(options.range.clone());
                }
                self.inner.get_opts(location, options).await
            }

            async fn copy_opts(
                &self,
                from: &ObjectPath,
                to: &ObjectPath,
                options: object_store::CopyOptions,
            ) -> object_store::Result<()> {
                self.copies
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.inner.copy_opts(from, to, options).await?;
                match self.after_copy {
                    AfterCopy::Nothing => {}
                    AfterCopy::Rewrite => {
                        self.inner.put(from, "newer bytes".into()).await?;
                    }
                    AfterCopy::Delete => self.inner.delete(from).await?,
                }
                Ok(())
            }
        }
    }

    impl Spy {
        fn count(counter: &std::sync::atomic::AtomicUsize) -> usize {
            counter.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    async fn spy_backend(after_copy: AfterCopy) -> (Arc<Spy>, Arc<dyn Backend>) {
        let spy = Arc::new(Spy {
            after_copy,
            ..Spy::default()
        });
        spy.inner
            .put(&ObjectPath::from("a"), "old bytes".into())
            .await
            .expect("put");
        let backend: Arc<dyn Backend> = Arc::new(crate::backend::TestBackend {
            store: spy.clone(),
            multipart: false,
        });
        (spy, backend)
    }

    #[tokio::test]
    async fn a_same_store_move_keeps_a_source_rewritten_after_the_copy() {
        let (spy, backend) = spy_backend(AfterCopy::Rewrite).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        let err = Plain.copy(&src, &dst, true).await.expect_err("changed");
        assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
        let source = spy.inner.get(&ObjectPath::from("a")).await.expect("source");
        assert_eq!(
            source.bytes().await.expect("bytes").as_ref(),
            b"newer bytes"
        );
        let copy = spy.inner.get(&ObjectPath::from("c")).await.expect("copy");
        assert_eq!(copy.bytes().await.expect("bytes").as_ref(), b"old bytes");
    }

    #[tokio::test]
    async fn a_same_store_move_deletes_an_unchanged_source() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        Plain.copy(&src, &dst, true).await.expect("move");
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_ok());
    }

    #[tokio::test]
    async fn a_same_store_move_of_a_vanished_source_is_ok() {
        let (spy, backend) = spy_backend(AfterCopy::Delete).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        Plain.copy(&src, &dst, true).await.expect("move");
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_ok());
    }

    #[tokio::test]
    async fn an_s3_source_over_5_gib_streams_instead() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        let done = server_side_copy(&src, &dst, false, Some(6 << 30))
            .await
            .expect("decision");
        assert!(!done);
        assert_eq!(Spy::count(&spy.copies), 0);
        assert_eq!(Spy::count(&spy.heads), 0);
    }

    #[tokio::test]
    async fn a_small_s3_source_copies_server_side() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        assert!(
            server_side_copy(&src, &dst, false, Some(1))
                .await
                .expect("copy")
        );
        assert_eq!((Spy::count(&spy.heads), Spy::count(&spy.copies)), (0, 1));

        assert!(
            server_side_copy(&src, &dst, false, None)
                .await
                .expect("copy")
        );
        assert_eq!((Spy::count(&spy.heads), Spy::count(&spy.copies)), (1, 2));
    }

    #[tokio::test]
    async fn other_schemes_ignore_the_s3_limit() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "gs://b/a"),
            resolved(&backend, "gs://b/c"),
        );

        assert!(
            server_side_copy(&src, &dst, false, Some(6 << 30))
                .await
                .expect("copy")
        );
        assert_eq!(Spy::count(&spy.copies), 1);
    }

    #[tokio::test]
    async fn a_file_copy_never_runs_server_side() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "file:///a"),
            resolved(&backend, "file:///c"),
        );

        assert!(
            !server_side_copy(&src, &dst, false, None)
                .await
                .expect("copy")
        );
        assert_eq!(Spy::count(&spy.copies), 0);

        // A move on one device stays a rename.
        assert!(
            server_side_copy(&src, &dst, true, None)
                .await
                .expect("move")
        );
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_ok());
    }

    async fn put_with_attributes(store: &InMemory, name: &str, body: Bytes) {
        let mut attributes = object_store::Attributes::new();
        attributes.insert(Attribute::ContentType, "image/png".into());
        attributes.insert(Attribute::CacheControl, "max-age=60".into());
        attributes.insert(Attribute::StorageClass, "COLD".into());
        attributes.insert(Attribute::Metadata("owner".into()), "me".into());
        let opts = object_store::PutOptions {
            attributes,
            ..Default::default()
        };
        store
            .put_opts(&ObjectPath::from(name), body.into(), opts)
            .await
            .expect("put");
    }

    async fn attributes_of(store: &InMemory, name: &str) -> object_store::Attributes {
        let head = GetOptions {
            head: true,
            ..Default::default()
        };
        store
            .get_opts(&ObjectPath::from(name), head)
            .await
            .expect("head")
            .attributes
    }

    #[tokio::test]
    async fn a_streamed_copy_keeps_the_content_type() {
        for (multipart, len) in [(false, 5), (true, 9 << 20)] {
            let store = Arc::new(InMemory::new());
            let target = Arc::new(InMemory::new());
            let backend = |store: &Arc<InMemory>| -> Arc<dyn Backend> {
                Arc::new(crate::backend::TestBackend {
                    store: store.clone(),
                    multipart,
                })
            };
            let (one, two) = (backend(&store), backend(&target));
            put_with_attributes(&store, "a", Bytes::from(vec![7_u8; len])).await;

            let (src, dst) = (resolved(&one, "s3://b/a"), resolved(&two, "s3://c/b"));
            Plain.copy(&src, &dst, false).await.expect("copy");

            let kept = attributes_of(&target, "b").await;
            assert_eq!(
                kept.get(&Attribute::ContentType).map(AsRef::as_ref),
                Some("image/png"),
                "multipart={multipart}"
            );
            assert_eq!(
                kept.get(&Attribute::CacheControl).map(AsRef::as_ref),
                Some("max-age=60"),
                "multipart={multipart}"
            );
            assert!(kept.get(&Attribute::StorageClass).is_none());
        }
    }

    #[tokio::test]
    async fn metadata_is_kept_only_within_one_provider() {
        let store = Arc::new(InMemory::new());
        let target = Arc::new(InMemory::new());
        let backend = |store: &Arc<InMemory>| -> Arc<dyn Backend> {
            Arc::new(crate::backend::TestBackend {
                store: store.clone(),
                multipart: false,
            })
        };
        let (one, two) = (backend(&store), backend(&target));
        put_with_attributes(&store, "a", Bytes::from_static(b"x")).await;
        let owner = Attribute::Metadata("owner".into());

        let src = resolved(&one, "s3://b/a");
        Plain
            .copy(&src, &resolved(&two, "s3://c/same"), false)
            .await
            .expect("s3 to s3");
        Plain
            .copy(&src, &resolved(&two, "gs://c/other"), false)
            .await
            .expect("s3 to gs");

        let same = attributes_of(&target, "same").await;
        assert_eq!(same.get(&owner).map(AsRef::as_ref), Some("me"));
        let other = attributes_of(&target, "other").await;
        assert!(other.get(&owner).is_none());
        assert!(other.get(&Attribute::ContentType).is_some());
    }

    #[test]
    fn copying_from_carries_no_attributes() {
        let (_, backend) = memory_backend();
        let options = UploadOptions::copying_from(backend.as_ref(), &ObjectPath::from("a"), 1);
        assert!(options.attributes.is_empty());
    }

    async fn source_meta(store: &InMemory, name: &str) -> ObjectMeta {
        store.head(&ObjectPath::from(name)).await.expect("head")
    }

    #[tokio::test]
    async fn a_source_is_deleted_only_if_it_is_what_was_copied() {
        let (store, backend) = memory_backend();
        let path = ObjectPath::from("a");
        let src = resolved(&backend, "test://b/a");

        // Unchanged: deleted.
        store.put(&path, "one".into()).await.expect("put");
        let read = source_meta(&store, "a").await;
        delete_unchanged_source(&src, &read).await.expect("delete");
        assert!(store.head(&path).await.is_err());

        // Gone: nothing to do.
        delete_unchanged_source(&src, &read).await.expect("gone");

        // Overwritten after the read (even with the same size): kept.
        store.put(&path, "one".into()).await.expect("put");
        let read = source_meta(&store, "a").await;
        store.put(&path, "two".into()).await.expect("put");
        let err = delete_unchanged_source(&src, &read)
            .await
            .expect_err("changed");
        assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
        assert!(store.head(&path).await.is_ok());
    }

    #[test]
    fn a_cross_device_error_is_found_through_the_source_chain() {
        let wrap = |source: std::io::Error| object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(source),
        };
        assert!(crosses_devices(&wrap(std::io::Error::from(
            std::io::ErrorKind::CrossesDevices
        ))));
        assert!(!crosses_devices(&wrap(std::io::Error::from_raw_os_error(
            2
        ))));
    }

    #[test]
    fn source_changed_names_the_object_without_credentials() {
        let e = Error::SourceChanged {
            url: redacted(&Url::parse("http://u:pw@h/a?sig=1#f").expect("url")),
        };
        assert_eq!(
            e.to_string(),
            "source changed during move; it was not deleted: http://h/a"
        );
    }

    /// Puts `bytes` at `a` of a fresh counting store.
    async fn spy_with(bytes: Bytes) -> (Arc<Spy>, Arc<dyn Backend>) {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        spy.inner
            .put(&ObjectPath::from("a"), bytes.into())
            .await
            .expect("put");
        (spy, backend)
    }

    /// A plain copy, then a plain move, of `a` to `c` in one store.
    async fn plain_copies(spy: &Spy, backend: &Arc<dyn Backend>) -> Vec<Result<()>> {
        let (src, dst) = (resolved(backend, "s3://b/a"), resolved(backend, "s3://b/c"));
        let mut out = Vec::new();
        for delete in [false, true] {
            let result = Plain.copy(&src, &dst, delete).await;
            if result.is_ok() && !delete {
                spy.inner.delete(&ObjectPath::from("c")).await.expect("rm");
            }
            out.push(result);
        }
        out
    }

    #[tokio::test]
    async fn a_plain_object_copies_server_side() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        for result in plain_copies(&spy, &backend).await {
            result.expect("copied");
        }
        assert_eq!(Spy::count(&spy.copies), 2);
        // A source starting like a message but too short to be one is plain.
        let (spy, backend) = spy_with(Bytes::from_static(&[0x02, 0x04])).await;
        for result in plain_copies(&spy, &backend).await {
            result.expect("copied");
        }
        // The empty object is plain too.
        let (spy, backend) = spy_with(Bytes::new()).await;
        for result in plain_copies(&spy, &backend).await {
            result.expect("copied");
        }
    }

    #[cfg(not(feature = "encryption"))]
    #[tokio::test]
    async fn without_the_feature_the_prefix_alone_blocks_a_copy_to_another_name() {
        let message = Bytes::from_static(&[0x02, 0x04, 0x78, 0, 0, 0]);
        let (spy, backend) = spy_with(message).await;
        for result in plain_copies(&spy, &backend).await {
            let err = result.expect_err("refused");
            assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
        }
        assert_eq!(Spy::count(&spy.copies), 0);
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_ok());

        // The same name in another store stays copyable, and so does an object
        // that only starts with two of the three bytes.
        let (other, other_backend) = memory_backend();
        let (src, dst) = (
            resolved(&backend, "s3://one/a"),
            resolved(&other_backend, "s3://two/a"),
        );
        Plain.copy(&src, &dst, false).await.expect("same name");
        assert!(other.head(&ObjectPath::from("a")).await.is_ok());
        let (spy, backend) = spy_with(Bytes::from_static(&[0x02, 0x04, 0x79])).await;
        for result in plain_copies(&spy, &backend).await {
            result.expect("copied");
        }
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::encryption::keys::Keyring;
        use crate::encryption::{EncryptionConfig, Envelope};

        fn envelope(bind: bool) -> Envelope {
            let ring = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
            Envelope::new(EncryptionConfig::new(Arc::new(ring)).bind_object_key(bind))
        }

        async fn message(env: &Envelope, name: &str) -> Bytes {
            env.encode(
                &at(&format!("s3://b/{name}")),
                &CallerContext::new(),
                Bytes::from_static(b"payload"),
            )
            .await
            .expect("encode")
        }

        #[tokio::test]
        async fn a_bound_object_is_not_copied_or_moved_to_another_name() {
            let bytes = message(&envelope(true), "a").await;
            let (spy, backend) = spy_with(bytes.clone()).await;
            for result in plain_copies(&spy, &backend).await {
                let err = result.expect_err("refused");
                assert!(
                    matches!(&err, Error::InvalidInput(m) if m.contains("bound")),
                    "{err:?}"
                );
            }
            assert_eq!(Spy::count(&spy.copies), 0);
            assert!(spy.inner.head(&ObjectPath::from("c")).await.is_err());
            let kept = spy.inner.get(&ObjectPath::from("a")).await.expect("kept");
            assert_eq!(kept.bytes().await.expect("bytes"), bytes);
        }

        #[tokio::test]
        async fn the_copy_guard_reads_1_kib_and_more_only_for_a_large_header() {
            use object_store::GetRange::Bounded;
            let env = envelope(false);
            let max = crate::encryption::DecodeLimits::default().max_header_len as u64;
            let ctx = CallerContext::from([("big".to_owned(), "x".repeat(6000))]);
            for (ctx, want) in [
                (CallerContext::new(), vec![Some(Bounded(0..1024))]),
                (ctx, vec![Some(Bounded(0..1024)), Some(Bounded(0..max))]),
            ] {
                let bytes = env
                    .encode(&at("s3://b/a"), &ctx, Bytes::from_static(b"payload"))
                    .await
                    .expect("encode");
                let (spy, backend) = spy_with(bytes).await;
                let (s, d) = (
                    resolved(&backend, "s3://b/a"),
                    resolved(&backend, "s3://b/c"),
                );
                Plain.copy(&s, &d, false).await.expect("copied");
                assert_eq!(*spy.ranges.lock().expect("ranges"), want);
            }
        }

        #[tokio::test]
        async fn a_bound_object_copies_under_its_own_name_to_another_store() {
            let env = envelope(true);
            let bytes = message(&env, "a").await;
            let (spy, backend) = spy_with(bytes).await;
            let (other, other_backend) = memory_backend();
            let (src, dst) = (
                resolved(&backend, "s3://one/a"),
                resolved(&other_backend, "s3://two/a"),
            );
            Plain.copy(&src, &dst, true).await.expect("move");
            assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
            let got = other.get(&ObjectPath::from("a")).await.expect("get");
            let plain = env.get_bytes(&dst.loc, got).await.expect("decrypt");
            assert_eq!(plain.as_ref(), b"payload");
        }

        #[tokio::test]
        async fn an_unbound_object_copies_to_another_name() {
            let env = envelope(false);
            let (spy, backend) = spy_with(message(&env, "a").await).await;
            for result in plain_copies(&spy, &backend).await {
                result.expect("copied");
            }
            assert_eq!(Spy::count(&spy.copies), 2);
        }

        #[tokio::test]
        async fn a_header_that_starts_like_ours_but_does_not_parse_is_refused() {
            let good = message(&envelope(true), "a").await;
            let truncated = good.slice(..40);
            let garbage = Bytes::from_static(&[0x02, 0x04, 0x78, 0xff, 0xff, 0xff, 0xff]);
            for damaged in [truncated, garbage] {
                let (spy, backend) = spy_with(damaged).await;
                for result in plain_copies(&spy, &backend).await {
                    let err = result.expect_err("refused");
                    assert!(
                        matches!(&err, Error::InvalidInput(m) if m.contains("looks encrypted")),
                        "{err:?}"
                    );
                }
                assert_eq!(Spy::count(&spy.copies), 0);
                assert!(spy.inner.head(&ObjectPath::from("c")).await.is_err());
            }
        }

        #[tokio::test]
        async fn the_message_prefix_matches_the_format() {
            let bytes = message(&envelope(true), "a").await;
            assert!(bytes.starts_with(&OSC_PREFIX));
        }
    }
}
