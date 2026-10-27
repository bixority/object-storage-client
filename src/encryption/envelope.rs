//! The [`Envelope`] codec: ESDK-compatible client-side encryption behind the
//! [`Codec`] seam.

use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::{
    Attribute, Attributes, GetOptions, GetRange, GetResult, ObjectMeta, ObjectStore,
    ObjectStoreExt, UpdateVersion,
};
use url::Url;

use super::context::{EncryptionContext, KEY_OBJECT, is_reserved};
use super::frame::{Decryptor, Encryptor, FrameLayout, Progress};
use super::header::{DecodeLimits, FormatError, Header, HeaderCodec, HeaderParse};
use super::keys::raw_aes::edk_key_name;
use super::keys::{KeyError, KeyProvider};
use super::suite::{DecryptError, fresh_message_material};
use crate::backend::{Backend, body_stream};
use crate::client::{EncryptionInfo, Error, ObjectMetadata, ReencryptOptions, Reencrypted, Result};
use crate::codec::sealed::CodecImpl as _;
use crate::codec::{
    ByteStream, CallerContext, Codec, Location, Resolved, delete_unchanged_source, redacted,
    same_object, sealed, server_side_copy,
};
use crate::hex;
use crate::upload::{UploadOptions, upload_stream};

/// Default frame length of new messages.
const DEFAULT_FRAME_LENGTH: u32 = 65536;
/// Most plaintext handled between two await points (spec §9).
const SLICE: usize = 1 << 20;
/// Initial buffer cap when collecting a whole object.
const MAX_PREALLOC: usize = 16 << 20;

/// The `format` reported by [`EncryptionInfo`].
const FORMAT_NAME: &str = "osc-esdk2-0478-v1";

/// A header read from the start of a stored object, with the object's
/// metadata. Nothing in it is authenticated.
pub(crate) struct StoredHeader {
    pub header: Header,
    pub header_len: usize,
    pub meta: ObjectMeta,
    pub attributes: Attributes,
}

pub(crate) fn encryption_info(h: &Header) -> EncryptionInfo {
    EncryptionInfo {
        format: FORMAT_NAME.to_owned(),
        message_id: hex::encode(&h.message_id),
        key_names: h
            .edks
            .iter()
            .filter_map(|e| edk_key_name(e).map(str::to_owned))
            .collect(),
        frame_length: h.frame_length,
        context: h.context.0.clone(),
    }
}

/// Settings of the encrypted client.
#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    /// Wraps and unwraps data keys.
    pub key_provider: Arc<dyn KeyProvider>,
    /// Bind each ciphertext to its object key (`osc:key`); on by default.
    pub bind_object_key: bool,
    /// Frame length of new messages.
    pub frame_length: u32,
    /// Limits applied to untrusted headers, and checked on write.
    pub limits: DecodeLimits,
}

impl EncryptionConfig {
    /// Defaults: object key bound, 64 KiB frames, default limits.
    #[must_use]
    pub fn new(key_provider: Arc<dyn KeyProvider>) -> Self {
        Self {
            key_provider,
            bind_object_key: true,
            frame_length: DEFAULT_FRAME_LENGTH,
            limits: DecodeLimits::default(),
        }
    }

    /// Sets whether ciphertexts are bound to their object key.
    #[must_use]
    pub const fn bind_object_key(mut self, bind: bool) -> Self {
        self.bind_object_key = bind;
        self
    }

    /// Sets the frame length of new messages.
    ///
    /// # Errors
    /// [`Error::InvalidInput`] when it is outside the current [`DecodeLimits`]. Set
    /// [`Self::limits`] first when changing both.
    pub fn frame_length(mut self, frame_length: u32) -> Result<Self> {
        self.limits
            .check_frame_length(frame_length)
            .map_err(|e| write_err(&e))?;
        self.frame_length = frame_length;
        Ok(self)
    }

    /// Sets the header limits.
    #[must_use]
    pub const fn limits(mut self, limits: DecodeLimits) -> Self {
        self.limits = limits;
        self
    }
}

/// The encrypting codec. Obtain a client with it through
/// [`Client::with_encryption`](crate::Client::with_encryption) or
/// [`Client::new`](crate::Client::<Envelope>::new).
#[derive(Clone)]
pub struct Envelope {
    cfg: Arc<EncryptionConfig>,
}

impl Codec for Envelope {}

fn write_err(e: &FormatError) -> Error {
    Error::InvalidInput(e.to_string())
}

fn read_err(url: &Url, e: DecryptError) -> Error {
    match e {
        DecryptError::Format(FormatError::NotOsc) => {
            // Never carry URL credentials or tokens into an error message.
            Error::NotEncrypted { url: redacted(url) }
        }
        // The keyring's check value rules out a genuine ID or material
        // mismatch, so a failed unwrap means a tampered EDK or context.
        DecryptError::Key(k @ KeyError::UnwrapFailed { .. }) => {
            Error::Decryption(DecryptError::Key(k))
        }
        DecryptError::Key(k) => Error::Key(k),
        e => Error::Decryption(e),
    }
}

/// Collects at most `limit` bytes of `result` and drops the rest of the body
/// unread, so a store that answers a range request with more than was asked
/// for cannot make a header probe buffer a whole object.
async fn read_prefix(result: GetResult, limit: u64) -> Result<Bytes> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut buf = BytesMut::new();
    let mut body = body_stream(result);
    while buf.len() < limit
        && let Some(chunk) = body.next().await
    {
        let chunk = chunk?;
        let take = chunk.len().min(limit - buf.len());
        buf.extend_from_slice(&chunk[..take]);
    }
    Ok(buf.freeze())
}

/// Reads and parses a message header with one ranged GET of up to
/// `max_header_len` bytes, which covers any header the limits accept. Needs
/// no keys.
async fn read_header(
    backend: &dyn Backend,
    loc: &Location,
    limits: &DecodeLimits,
) -> Result<StoredHeader> {
    let end = limits.max_header_len as u64;
    let options = GetOptions {
        range: Some(GetRange::Bounded(0..end)),
        ..Default::default()
    };
    let result = match backend.store().get_opts(&loc.path, options).await {
        Ok(r) => r,
        Err(e) => return Err(empty_or(backend, loc, e).await),
    };
    let (meta, attributes) = (result.meta.clone(), result.attributes.clone());
    let buf = read_prefix(result, end).await?;
    if buf.len() < 3 {
        return Err(read_err(
            &loc.url,
            DecryptError::Format(FormatError::NotOsc),
        ));
    }
    let parsed = HeaderCodec::parse(&buf, limits)
        .map_err(|e| read_err(&loc.url, DecryptError::Format(e)))?;
    match parsed {
        HeaderParse::Done {
            header, header_len, ..
        } => Ok(StoredHeader {
            header,
            header_len,
            meta,
            attributes,
        }),
        HeaderParse::NeedMore => Err(read_err(
            &loc.url,
            DecryptError::Format(FormatError::Truncated),
        )),
    }
}

/// Stores reject a range over an empty object; that is a plain empty object,
/// not an error of the store. Any other failure is returned as is.
async fn empty_or(backend: &dyn Backend, loc: &Location, e: object_store::Error) -> Error {
    if matches!(e, object_store::Error::NotFound { .. }) {
        return e.into();
    }
    let head = GetOptions {
        head: true,
        ..Default::default()
    };
    match backend.store().get_opts(&loc.path, head).await {
        Ok(r) if r.meta.size == 0 => read_err(&loc.url, DecryptError::Format(FormatError::NotOsc)),
        _ => e.into(),
    }
}

/// The envelope details of an object read from its header, with default
/// limits and no keys. Unauthenticated, like
/// [`Client::inspect_encryption`](crate::Client::<Envelope>::inspect_encryption).
///
/// # Errors
/// [`Error::NotEncrypted`] for objects that are not envelope messages,
/// [`Error::Decryption`] for malformed headers, and storage errors.
#[cfg_attr(not(feature = "cli"), allow(dead_code))]
pub(crate) async fn inspect_encryption_raw(
    client: &crate::ObjectStorageClient,
    url: &str,
) -> Result<EncryptionInfo> {
    let r = client.resolve(url)?;
    let h = read_header(r.backend.as_ref(), &r.loc, &DecodeLimits::default()).await?;
    Ok(encryption_info(&h.header))
}

impl Envelope {
    pub(crate) fn new(cfg: EncryptionConfig) -> Self {
        Self { cfg: Arc::new(cfg) }
    }

    /// Reads and parses the header; see [`read_header`].
    pub(crate) async fn read_header(
        &self,
        backend: &dyn Backend,
        loc: &Location,
    ) -> Result<StoredHeader> {
        read_header(backend, loc, &self.cfg.limits).await
    }

    /// The header details of an object, without keys and without verifying
    /// the header tag.
    pub(crate) async fn inspect(
        &self,
        backend: &dyn Backend,
        loc: &Location,
    ) -> Result<EncryptionInfo> {
        Ok(encryption_info(
            &self.read_header(backend, loc).await?.header,
        ))
    }

    /// Decrypting stream; with `expect`, fails unless the authenticated
    /// header equals it. With `echo`, yields the stored chunks unchanged
    /// instead of the plaintext: a verifying pass-through.
    fn decode_expecting(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
        expect: Option<Header>,
        echo: bool,
    ) -> ByteStream {
        self.decode_stream(loc, stored, expect, echo, None)
    }

    /// Decrypting stream that fills `slot` with the caller context of the
    /// authenticated header, before it yields any plaintext.
    fn decode_filling(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
        slot: Arc<OnceLock<CallerContext>>,
    ) -> ByteStream {
        self.decode_stream(loc, stored, None, false, Some(slot))
    }

    fn decode_stream(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
        expect: Option<Header>,
        echo: bool,
        slot: Option<Arc<OnceLock<CallerContext>>>,
    ) -> ByteStream {
        let expected = self.cfg.bind_object_key.then(|| loc.path.to_string());
        let dec = Decryptor::new(self.cfg.limits.clone(), expected);
        stream::unfold(
            DecodeState {
                stored,
                cur: Bytes::new(),
                dec: Some(dec),
                ready: VecDeque::new(),
                cfg: Arc::clone(&self.cfg),
                url: loc.url,
                expect,
                echo,
                slot,
            },
            DecodeState::step,
        )
        .boxed()
    }

    /// Encrypting stream for `loc` whose caller context is taken from `slot`
    /// when `plain` first yields; see [`EncodeState::Deferred`].
    fn encode_deferred(
        &self,
        loc: Location,
        slot: Arc<OnceLock<CallerContext>>,
        plain: ByteStream,
    ) -> ByteStream {
        stream::unfold(
            EncodeState::Deferred {
                envelope: self.clone(),
                loc,
                slot,
                plain: plain.fuse().boxed(),
            },
            EncodeState::step,
        )
        .boxed()
    }

    /// Whether an EDK of the header is wrapped by the key `name`.
    fn names_key(&self, header: &Header, name: &str) -> bool {
        let provider = &self.cfg.key_provider;
        header
            .edks
            .iter()
            .any(|e| e.provider_id == provider.namespace() && edk_key_name(e) == Some(name))
    }

    /// Rewrites the object at `r` from key `opts.from` to the active key; see
    /// [`Client::reencrypt`](crate::Client::<Envelope>::reencrypt).
    pub(crate) async fn reencrypt(
        &self,
        r: &Resolved,
        opts: &ReencryptOptions,
    ) -> Result<Reencrypted> {
        let active = self.cfg.key_provider.active_key_name();
        if opts.from == active {
            return Err(Error::InvalidInput(format!(
                "cannot re-encrypt from the active key {active:?}; rotate the keyring first"
            )));
        }
        let stored = match self.read_header(r.backend.as_ref(), &r.loc).await {
            Ok(stored) => stored,
            Err(Error::NotEncrypted { .. }) => return Ok(Reencrypted::NotEncrypted),
            Err(e) => return Err(e),
        };
        if !self.names_key(&stored.header, &opts.from) {
            return Ok(Reencrypted::Skipped);
        }
        if opts.before.is_some_and(|b| stored.meta.last_modified >= b) {
            return Ok(Reencrypted::AfterCutoff);
        }
        if opts.dry_run {
            return Ok(Reencrypted::WouldRewrite);
        }
        // Without an e-tag or version neither the pinned body read nor the
        // check before the commit could notice a concurrent writer.
        if stored.meta.e_tag.is_none() && stored.meta.version.is_none() {
            return Err(Error::Generic(format!(
                "{} reports neither an e-tag nor a version, so a concurrent change could not be detected; not re-encrypted",
                redacted(&r.loc.url)
            )));
        }

        // The body is pinned to the version whose header was just read.
        let version = UpdateVersion {
            e_tag: stored.meta.e_tag.clone(),
            version: stored.meta.version.clone(),
        };
        let get = GetOptions {
            if_match: version.e_tag.clone(),
            version: version
                .e_tag
                .is_none()
                .then(|| version.version.clone())
                .flatten(),
            ..GetOptions::default()
        };
        let got = match r.backend.store().get_opts(&r.loc.path, get).await {
            Ok(got) => got,
            Err(
                object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. },
            ) => return Ok(Reencrypted::Changed),
            Err(e) => return Err(e.into()),
        };
        let size = got.meta.size;
        let ctx = caller_context(&stored.header);
        let plain =
            self.decode_expecting(r.loc.clone(), body_stream(got), Some(stored.header), false);
        let upload = self.encode_stream(r.loc.clone(), ctx, plain);
        let options = UploadOptions {
            if_unchanged: Some(version),
            ..UploadOptions::copying_from(r.backend.as_ref(), &r.loc.path, size)
        };
        match upload_stream(r.backend.as_ref(), &r.loc.path, upload, &options).await {
            Ok(()) => {}
            Err(Error::Changed) => return Ok(Reencrypted::Changed),
            Err(e) => return Err(e),
        }

        let fresh = self.read_header(r.backend.as_ref(), &r.loc).await?;
        let usable = self
            .cfg
            .key_provider
            .unwrap(&fresh.header.edks, &fresh.header.context)
            .await;
        if self.names_key(&fresh.header, &opts.from) || usable.is_err() {
            return Err(Error::Generic(format!(
                "re-encrypted object {} failed verification: it must name a key of the keyring and not {:?}",
                redacted(&r.loc.url),
                opts.from
            )));
        }
        Ok(Reencrypted::Rewritten)
    }

    /// Whether an EDK of the header is wrapped by the active key.
    fn names_active_key(&self, header: &Header) -> bool {
        let provider = &self.cfg.key_provider;
        let active = provider.active_key_name();
        header.edks.iter().any(|e| {
            e.provider_id == provider.namespace() && edk_key_name(e) == Some(active.as_str())
        })
    }

    /// The context of a message written to `loc`. Fails on reserved caller
    /// keys, before any I/O.
    fn context(&self, loc: &Location, ctx: &CallerContext) -> Result<EncryptionContext> {
        // The config fields are public and can bypass the builder's check.
        self.cfg
            .limits
            .check_frame_length(self.cfg.frame_length)
            .map_err(|e| write_err(&e))?;
        let bound = self.cfg.bind_object_key.then(|| loc.path.as_ref());
        EncryptionContext::new_reserved(bound)
            .with_caller(ctx)
            .map_err(|e| write_err(&e))
    }

    /// Fresh DEK and message ID, wrapped key, and the encoded header.
    async fn begin(&self, context: EncryptionContext) -> Result<(Bytes, Encryptor)> {
        let (dek, message_id) = fresh_message_material()?;
        let edks = self.cfg.key_provider.wrap(&dek, &context).await?;
        Encryptor::with_limits(
            &dek,
            message_id,
            context,
            edks,
            self.cfg.frame_length,
            &self.cfg.limits,
        )
        .map_err(|e| write_err(&e))
    }
}

/// The caller-chosen context entries of a header: all but the reserved ones.
fn caller_context(header: &Header) -> CallerContext {
    header
        .context
        .iter()
        .filter(|(k, _)| !is_reserved(k))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

fn seal_all(header: &Bytes, mut enc: Encryptor, data: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(header.len() + enc.sealed_len_bound(data.len()));
    out.extend_from_slice(header);
    enc.update(data, &mut out);
    enc.finish(&mut out);
    out.freeze()
}

/// State of an encrypting stream: header, frames, then done.
enum EncodeState {
    Start {
        envelope: Envelope,
        context: EncryptionContext,
        plain: ByteStream,
    },
    /// Like `Start`, but the caller context comes from `slot`, which the
    /// decrypting `plain` fills before it yields anything. The first item of
    /// `plain` is read before the header is written. `plain` must be fused.
    Deferred {
        envelope: Envelope,
        loc: Location,
        slot: Arc<OnceLock<CallerContext>>,
        plain: ByteStream,
    },
    Run {
        enc: Encryptor,
        plain: ByteStream,
        cur: Bytes,
    },
    Done,
}

impl EncodeState {
    async fn step(self) -> Option<(Result<Bytes>, Self)> {
        match self {
            Self::Deferred {
                envelope,
                loc,
                slot,
                mut plain,
            } => {
                let first = match plain.next().await {
                    Some(Err(e)) => return Some((Err(e), Self::Done)),
                    Some(Ok(chunk)) => chunk,
                    None => Bytes::new(),
                };
                // An empty plaintext ends the stream only after its header
                // verified, so an empty slot here is a bug, not an input.
                let context = slot
                    .get()
                    .ok_or_else(|| Error::Generic("source header was not verified".to_owned()))
                    .and_then(|ctx| envelope.context(&loc, ctx));
                let begun = match context {
                    Ok(context) => envelope.begin(context).await,
                    Err(e) => Err(e),
                };
                Some(match begun {
                    Ok((header, enc)) => (
                        Ok(header),
                        Self::Run {
                            enc,
                            plain,
                            cur: first,
                        },
                    ),
                    Err(e) => (Err(e), Self::Done),
                })
            }
            Self::Start {
                envelope,
                context,
                plain,
            } => Some(match envelope.begin(context).await {
                Ok((header, enc)) => (
                    Ok(header),
                    Self::Run {
                        enc,
                        plain,
                        cur: Bytes::new(),
                    },
                ),
                Err(e) => (Err(e), Self::Done),
            }),
            Self::Run {
                mut enc,
                mut plain,
                mut cur,
            } => loop {
                if cur.is_empty() {
                    match plain.next().await {
                        Some(Ok(chunk)) => cur = chunk,
                        Some(Err(e)) => return Some((Err(e), Self::Done)),
                        None => {
                            let mut out = BytesMut::new();
                            enc.finish(&mut out);
                            return Some((Ok(out.freeze()), Self::Done));
                        }
                    }
                    continue;
                }
                let slice = cur.split_to(cur.len().min(SLICE));
                // Allocate only when a frame completes, and exactly its size.
                let emitted = enc.emitted_len(slice.len());
                if emitted == 0 {
                    enc.update(&slice, &mut BytesMut::new());
                    continue;
                }
                let mut out = BytesMut::with_capacity(emitted);
                enc.update(&slice, &mut out);
                debug_assert_eq!(out.len(), emitted);
                return Some((Ok(out.freeze()), Self::Run { enc, plain, cur }));
            },
            Self::Done => None,
        }
    }
}

/// State of a decrypting stream. `dec` is `None` once it has failed or ended.
struct DecodeState {
    stored: BoxStream<'static, object_store::Result<Bytes>>,
    /// The rest of a stored chunk longer than `SLICE`, fed one slice at a time.
    cur: Bytes,
    dec: Option<Decryptor>,
    ready: VecDeque<Bytes>,
    cfg: Arc<EncryptionConfig>,
    url: Url,
    /// A header read earlier (copy only): the authenticated one must equal it.
    expect: Option<Header>,
    /// Yield the stored chunks, not the plaintext (the plaintext is dropped).
    echo: bool,
    /// Receives the caller context of the authenticated header (re-encrypting
    /// copy only), once the data key and the binding verified.
    slot: Option<Arc<OnceLock<CallerContext>>>,
}

impl DecodeState {
    async fn step(mut self) -> Option<(Result<Bytes>, Self)> {
        loop {
            if let Some(frame) = self.ready.pop_front() {
                return Some((Ok(frame), self));
            }
            let mut dec = self.dec.take()?;
            let item = if self.cur.is_empty() {
                self.stored.next().await
            } else {
                Some(Ok(std::mem::take(&mut self.cur)))
            };
            match item {
                Some(Ok(mut chunk)) => {
                    // Bound the work between two await points whatever the
                    // store's chunking. In echo mode the raw slice is pushed
                    // below, only after it was fed and accepted.
                    if chunk.len() > SLICE {
                        self.cur = chunk.split_off(SLICE);
                    }
                    let raw = self.echo.then(|| chunk.clone());
                    match push(&mut dec, chunk, self.cfg.key_provider.as_ref()).await {
                        Ok(frames) => {
                            // `push` unwraps the key before returning, so the
                            // header is authenticated as soon as it exists.
                            // Compare it once, then stop comparing.
                            if let Some(got) = dec.header()
                                && self.expect.take().is_some_and(|x| &x != got)
                            {
                                return Some((
                                    Err(Error::Generic(
                                        "source changed while it was being copied".to_owned(),
                                    )),
                                    self,
                                ));
                            }
                            if let Some(slot) = &self.slot
                                && self.cfg.bind_object_key
                                && let Some(got) = dec.header()
                            {
                                // Same point as above: the header tag, and
                                // with binding on the `osc:key` entry, have
                                // verified. The slot is set once.
                                let _ = slot.set(caller_context(got));
                            }
                            match raw {
                                Some(raw) => self.ready.push_back(raw),
                                None => self.ready.extend(frames),
                            }
                            self.dec = Some(dec);
                        }
                        Err(e) => return Some((Err(read_err(&self.url, e)), self)),
                    }
                }
                Some(Err(e)) => return Some((Err(e.into()), self)),
                None => {
                    return dec
                        .finish()
                        .err()
                        .map(|e| (Err(read_err(&self.url, e)), self));
                }
            }
        }
    }
}

/// Feeds one stored chunk, unwrapping the data key as soon as it is needed.
async fn push(
    dec: &mut Decryptor,
    chunk: Bytes,
    provider: &dyn KeyProvider,
) -> std::result::Result<Vec<Bytes>, DecryptError> {
    let mut progress = dec.feed(chunk)?;
    if matches!(progress, Progress::NeedKey) {
        progress = dec.unwrap_with(provider).await?;
    }
    Ok(match progress {
        Progress::Plaintext(frames) => frames,
        Progress::NeedMore | Progress::NeedKey => Vec::new(),
    })
}

impl sealed::CodecImpl for Envelope {
    async fn encode(&self, loc: &Location, ctx: &CallerContext, data: Bytes) -> Result<Bytes> {
        let context = self.context(loc, ctx)?;
        let (header, enc) = self.begin(context).await?;
        if data.len() <= SLICE {
            return Ok(seal_all(&header, enc, &data));
        }
        tokio::task::spawn_blocking(move || seal_all(&header, enc, &data))
            .await
            .map_err(|e| Error::Generic(format!("encryption task failed: {e}")))
    }

    fn encode_stream(&self, loc: Location, ctx: CallerContext, plain: ByteStream) -> ByteStream {
        match self.context(&loc, &ctx) {
            Ok(context) => stream::unfold(
                EncodeState::Start {
                    envelope: self.clone(),
                    context,
                    plain,
                },
                EncodeState::step,
            )
            .boxed(),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    fn decode(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
    ) -> ByteStream {
        self.decode_expecting(loc, stored, None, false)
    }

    async fn get_bytes(&self, loc: &Location, result: GetResult) -> Result<Bytes> {
        let size = usize::try_from(result.meta.size).unwrap_or(usize::MAX);
        let expected = self.cfg.bind_object_key.then(|| loc.path.to_string());
        let mut dec = Decryptor::new(self.cfg.limits.clone(), expected).whole_object();
        let mut stored = body_stream(result);
        let mut reserved = false;
        // The header and key are checked as the first chunks arrive, so a bad
        // object fails before the rest is downloaded; the frames are opened
        // in place once all of it is buffered.
        while let Some(chunk) = stored.next().await {
            push(&mut dec, chunk?, self.cfg.key_provider.as_ref())
                .await
                .map_err(|e| read_err(&loc.url, e))?;
            if !reserved && dec.header().is_some() {
                dec.reserve(size.min(MAX_PREALLOC));
                reserved = true;
            }
        }
        let opened = if size <= SLICE {
            dec.finish_whole()
        } else {
            tokio::task::spawn_blocking(move || dec.finish_whole())
                .await
                .map_err(|e| Error::Generic(format!("decryption task failed: {e}")))?
        };
        opened.map_err(|e| read_err(&loc.url, e))
    }

    async fn describe(&self, backend: &dyn Backend, loc: &Location) -> Result<ObjectMetadata> {
        let h = self.read_header(backend, loc).await?;
        let layout = FrameLayout {
            header_len: h.header_len as u64,
            frame_length: h.header.frame_length,
        };
        let size_bytes = layout.plaintext_size(h.meta.size).ok_or_else(|| {
            read_err(
                &loc.url,
                DecryptError::Format(FormatError::Malformed("stored length")),
            )
        })?;
        Ok(ObjectMetadata {
            location: h.meta.location.to_string(),
            last_modified: h.meta.last_modified,
            size_bytes,
            stored_size_bytes: h.meta.size,
            content_type: h
                .attributes
                .get(&Attribute::ContentType)
                .map(|v| v.as_ref().to_owned()),
            e_tag: h.meta.e_tag,
            version: h.meta.version,
            encryption: Some(encryption_info(&h.header)),
        })
    }

    /// Copies or moves an encrypted object (spec §8). The header is never
    /// re-wrapped: a second header tag under the same data key and message ID
    /// would reuse a GCM nonce. A copy is therefore either a byte copy (same
    /// bound key, source already on the active key) or a full decrypt and
    /// re-encrypt with a fresh data key and message ID.
    async fn copy(&self, src: &Resolved, dst: &Resolved, delete_source: bool) -> Result<()> {
        let (read, upload) = if self.cfg.bind_object_key && src.loc.path != dst.loc.path {
            // Another name never takes the byte-copy path, so no probe is
            // needed: the source is decrypted and re-encrypted, and the new
            // header's caller context comes from the source header once its
            // tag and binding verified. A bad source fails the stream before
            // the upload commits.
            self.context(&dst.loc, &CallerContext::new())?;
            let got = src.backend.store().get(&src.loc.path).await?;
            let read = got.meta.clone();
            let slot = Arc::new(OnceLock::new());
            let plain = self.decode_filling(src.loc.clone(), body_stream(got), Arc::clone(&slot));
            (read, self.encode_deferred(dst.loc.clone(), slot, plain))
        } else {
            let header = self
                .read_header(src.backend.as_ref(), &src.loc)
                .await?
                .header;
            // Compare trusted locations, and require the header to bind the source
            // path: an unbound source must fail the binding check on the slow path.
            let src_key = self.cfg.bind_object_key.then(|| src.loc.path.as_ref());
            let same_key = !self.cfg.bind_object_key || src.loc.path == dst.loc.path;
            let byte_copy = same_key
                && header.context.get(KEY_OBJECT) == src_key
                && self.names_active_key(&header);
            if byte_copy {
                // Copying an object onto itself is a no-op (a local copy would
                // leave a hidden hard link behind). With binding on, `same_key`
                // makes the source path the destination path.
                if same_object(&src.loc, &dst.loc) {
                    return Ok(());
                }
                // A server-side copy inside one store is not verified (spec §13).
                if server_side_copy(src, dst, delete_source).await? {
                    return Ok(());
                }
            }
            let got = src.backend.store().get(&src.loc.path).await?;
            let read = got.meta.clone();
            let upload = if byte_copy {
                // Streams the stored bytes unchanged through a decryptor, so a bad
                // source fails the upload before it commits.
                self.decode_expecting(src.loc.clone(), body_stream(got), Some(header), true)
            } else {
                let ctx = caller_context(&header);
                let plain =
                    self.decode_expecting(src.loc.clone(), body_stream(got), Some(header), false);
                self.encode_stream(dst.loc.clone(), ctx, plain)
            };
            (read, upload)
        };
        upload_stream(
            dst.backend.as_ref(),
            &dst.loc.path,
            upload,
            &UploadOptions::copying_from(src.backend.as_ref(), &src.loc.path, read.size),
        )
        .await?;

        // Only after the destination committed, and never the object just
        // written: through another name for the source, the write replaced it.
        if delete_source && !same_object(&src.loc, &dst.loc) {
            delete_unchanged_source(src, &read).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectStoreExt, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    use super::*;
    use crate::backend::TestBackend;
    use crate::encryption::keys::Keyring;
    use crate::encryption::keys::{DataKey, EncryptedDataKey};

    /// An `InMemory` wrapper with optional faults, recording the range of every
    /// `get_opts` call.
    #[derive(Debug, Default)]
    struct TestStore {
        inner: InMemory,
        /// When set, unranged reads come from here: a two-faced source whose
        /// header probe (ranged) and full read disagree.
        full: Option<InMemory>,
        fail_ranged_reads: bool,
        /// A raw OS error code the copy fails with.
        copy_error: Option<i32>,
        ranges: Mutex<Vec<Option<GetRange>>>,
        /// Multipart uploads started.
        multiparts: AtomicUsize,
    }

    impl fmt::Display for TestStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestStore")
        }
    }

    #[async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.multiparts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.ranges
                .lock()
                .expect("lock")
                .push(options.range.clone());
            if options.range.is_some() && self.fail_ranged_reads {
                return Err(object_store::Error::Generic {
                    store: "faulty",
                    source: "ranged read refused".into(),
                });
            }
            match &self.full {
                Some(full) if options.range.is_none() => full.get_opts(location, options).await,
                _ => self.inner.get_opts(location, options).await,
            }
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
            if let Some(code) = self.copy_error {
                return Err(object_store::Error::Generic {
                    store: "faulty",
                    source: std::io::Error::from_raw_os_error(code).into(),
                });
            }
            self.inner.copy_opts(from, to, options).await
        }
    }

    impl TestStore {
        fn requested(&self) -> Vec<Option<GetRange>> {
            self.ranges.lock().expect("lock").clone()
        }
    }

    fn fixture() -> (Envelope, Arc<TestStore>, TestBackend, Location) {
        let provider = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
        let envelope = Envelope::new(EncryptionConfig::new(Arc::new(provider)));
        let store = Arc::new(TestStore::default());
        let backend = TestBackend {
            store: store.clone(),
            multipart: false,
        };
        let loc = Location {
            url: Url::parse("test://bucket/obj").expect("url"),
            path: ObjectPath::from("obj"),
        };
        (envelope, store, backend, loc)
    }

    async fn put(env: &Envelope, store: &TestStore, loc: &Location, ctx: &CallerContext) {
        let stored = env
            .encode(loc, ctx, Bytes::from_static(b"payload"))
            .await
            .expect("encode");
        store
            .inner
            .put(&loc.path, stored.into())
            .await
            .expect("put");
    }

    #[tokio::test]
    async fn header_probe_never_buffers_more_than_it_asked_for() {
        let store = InMemory::new();
        let path = ObjectPath::from("big");
        store
            .put(&path, vec![7u8; 1 << 20].into())
            .await
            .expect("put");
        // An unranged GET stands in for a server that ignores `Range`.
        let whole = store.get(&path).await.expect("get");
        assert_eq!(read_prefix(whole, 4096).await.expect("prefix").len(), 4096);
        let short = store.get(&path).await.expect("get");
        assert_eq!(
            read_prefix(short, 1 << 30).await.expect("prefix").len(),
            1 << 20
        );
    }

    #[tokio::test]
    async fn small_header_takes_one_ranged_read() {
        let (env, store, backend, loc) = fixture();
        put(&env, &store, &loc, &CallerContext::new()).await;
        let meta = env.describe(&backend, &loc).await.expect("describe");
        assert_eq!(meta.size_bytes, 7);
        let max = env.cfg.limits.max_header_len as u64;
        assert_eq!(store.requested(), [Some(GetRange::Bounded(0..max))]);
    }

    #[tokio::test]
    async fn large_header_takes_one_ranged_read() {
        let (env, store, backend, loc) = fixture();
        let ctx = CallerContext::from([("big".to_owned(), "x".repeat(6000))]);
        put(&env, &store, &loc, &ctx).await;
        let meta = env.describe(&backend, &loc).await.expect("describe");
        assert_eq!(meta.size_bytes, 7);
        let max = env.cfg.limits.max_header_len as u64;
        assert_eq!(store.requested(), [Some(GetRange::Bounded(0..max))]);
        let info = meta.encryption.expect("info");
        assert_eq!(info.context["big"].len(), 6000);
    }

    #[tokio::test]
    async fn plain_and_truncated_objects_are_rejected() {
        let (env, store, backend, loc) = fixture();
        for body in [&b""[..], b"x", b"plain text object"] {
            store
                .inner
                .put(&loc.path, body.to_vec().into())
                .await
                .expect("put");
            assert!(
                matches!(
                    env.describe(&backend, &loc).await,
                    Err(Error::NotEncrypted { .. })
                ),
                "{body:?}"
            );
        }
        put(&env, &store, &loc, &CallerContext::new()).await;
        let full = store
            .inner
            .get(&loc.path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        let header_len = env
            .read_header(&backend, &loc)
            .await
            .expect("header")
            .header_len;
        for cut in [3, 40, header_len - 1] {
            store
                .inner
                .put(&loc.path, full.slice(..cut).into())
                .await
                .expect("put");
            assert!(env.describe(&backend, &loc).await.is_err(), "cut {cut}");
        }
        // A header with no room for a final frame has no valid plaintext size.
        store
            .inner
            .put(&loc.path, full.slice(..header_len).into())
            .await
            .expect("put");
        assert!(matches!(
            env.describe(&backend, &loc).await,
            Err(Error::Decryption(DecryptError::Format(
                FormatError::Malformed(_)
            )))
        ));
    }

    fn resolved<S: ObjectStore>(host: &str, store: &Arc<S>) -> Resolved {
        resolved_at(host, "obj", store)
    }

    async fn stored(store: &InMemory) -> Bytes {
        store
            .get(&ObjectPath::from("obj"))
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes")
    }

    #[tokio::test]
    async fn same_key_copy_on_active_key_is_a_byte_copy() {
        let (env, _, _, _) = fixture();
        let (a, b) = (Arc::new(InMemory::new()), Arc::new(InMemory::new()));
        let (src, dst) = (resolved("a", &a), resolved("b", &b));
        let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
        let bytes = env
            .encode(&src.loc, &ctx, Bytes::from_static(b"payload"))
            .await
            .expect("encode");
        a.put(&src.loc.path, bytes.clone().into())
            .await
            .expect("put");

        env.copy(&src, &dst, true).await.expect("move");
        assert_eq!(stored(&b).await, bytes);
        assert!(a.head(&src.loc.path).await.is_err());
    }

    #[tokio::test]
    async fn same_key_copy_from_an_old_key_reencrypts_with_a_fresh_message() {
        let (old_env, _, _, _) = fixture();
        let provider = Keyring::from_material("kek-2", &[("kek-2", &[9; 32]), ("kek-1", &[7; 32])]);
        let env = Envelope::new(EncryptionConfig::new(Arc::new(provider)));

        let (a, b) = (Arc::new(InMemory::new()), Arc::new(InMemory::new()));
        let (src, dst) = (resolved("a", &a), resolved("b", &b));
        let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
        let bytes = old_env
            .encode(&src.loc, &ctx, Bytes::from_static(b"payload"))
            .await
            .expect("encode");
        a.put(&src.loc.path, bytes.into()).await.expect("put");

        env.copy(&src, &dst, false).await.expect("copy");
        let before = env
            .inspect(src.backend.as_ref(), &src.loc)
            .await
            .expect("src");
        let after = env
            .inspect(dst.backend.as_ref(), &dst.loc)
            .await
            .expect("dst");
        assert_eq!(before.key_names, ["kek-1"]);
        assert_eq!(after.key_names, ["kek-2"]);
        assert_ne!(before.message_id, after.message_id);
        assert_eq!(after.context["tenant"], "acme");
        assert_eq!(after.context["osc:key"], "obj");

        let got = env
            .get_bytes(
                &dst.loc,
                dst.backend.store().get(&dst.loc.path).await.expect("get"),
            )
            .await
            .expect("decrypt");
        assert_eq!(got.as_ref(), b"payload");
        // The source is untouched by a copy.
        assert!(a.head(&src.loc.path).await.is_ok());
    }

    fn resolved_at<S: ObjectStore>(host: &str, name: &str, store: &Arc<S>) -> Resolved {
        Resolved {
            backend: Arc::new(TestBackend {
                store: store.clone(),
                multipart: false,
            }),
            loc: Location {
                url: Url::parse(&format!("test://{host}/{name}")).expect("url"),
                path: ObjectPath::from(name),
            },
        }
    }

    async fn put_at(env: &Envelope, at: &Resolved, bound_to: &Resolved, plain: &'static [u8]) {
        let bytes = env
            .encode(
                &bound_to.loc,
                &CallerContext::new(),
                Bytes::from_static(plain),
            )
            .await
            .expect("encode");
        at.backend
            .store()
            .put(&at.loc.path, bytes.into())
            .await
            .expect("put");
    }

    #[tokio::test]
    async fn source_bound_to_another_name_is_not_copied_or_moved() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(InMemory::new());
        let (x, y) = (resolved_at("a", "x", &store), resolved_at("a", "y", &store));
        // The provider placed y's ciphertext at x.
        put_at(&env, &x, &y, b"content of y").await;
        for delete in [false, true] {
            let err = env.copy(&x, &y, delete).await.expect_err("binding");
            assert!(matches!(
                err,
                Error::Decryption(DecryptError::BindingMismatch { .. })
            ));
            assert!(store.head(&y.loc.path).await.is_err());
            assert!(store.head(&x.loc.path).await.is_ok());
        }
    }

    #[tokio::test]
    async fn move_of_a_source_with_a_flipped_binding_keeps_the_source() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(InMemory::new());
        let (x, y) = (resolved_at("a", "x", &store), resolved_at("a", "y", &store));
        put_at(&env, &x, &x, b"data").await;
        let mut raw = stored_at(&store, "x").await.to_vec();
        let at = raw
            .windows(7)
            .position(|w| w == b"osc:key")
            .expect("key name");
        let value = &mut raw[at + 7..];
        let pos = value.iter().position(|&b| b == b'x').expect("value");
        value[pos] = b'y';
        store.put(&x.loc.path, raw.into()).await.expect("put");

        assert!(env.copy(&x, &y, true).await.is_err());
        assert!(store.head(&x.loc.path).await.is_ok());
        assert!(store.head(&y.loc.path).await.is_err());
    }

    fn resolved_mp(name: &str, store: &Arc<TestStore>, multipart: bool) -> Resolved {
        Resolved {
            backend: Arc::new(TestBackend {
                store: store.clone(),
                multipart,
            }),
            loc: Location {
                url: Url::parse(&format!("test://h/{name}")).expect("url"),
                path: ObjectPath::from(name),
            },
        }
    }

    #[tokio::test]
    async fn copy_to_another_name_skips_the_probe_and_keeps_the_caller_context() {
        let (env, _, _, _) = fixture();
        let (a, b) = (
            Arc::new(TestStore::default()),
            Arc::new(TestStore::default()),
        );
        let (x, y) = (resolved_mp("x", &a, false), resolved_mp("y", &b, false));
        let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
        let bytes = env
            .encode(&x.loc, &ctx, Bytes::from_static(b"payload"))
            .await
            .expect("encode");
        a.inner
            .put(&x.loc.path, bytes.clone().into())
            .await
            .expect("put");
        let before = env
            .inspect(x.backend.as_ref(), &x.loc)
            .await
            .expect("inspect");
        a.ranges.lock().expect("lock").clear();

        env.copy(&x, &y, false).await.expect("copy");
        // One whole read of the source, no header probe.
        assert_eq!(a.requested(), [None]);
        let after = env
            .inspect(y.backend.as_ref(), &y.loc)
            .await
            .expect("inspect");
        assert_eq!(after.context["tenant"], "acme");
        assert_eq!(after.context[KEY_OBJECT], "y");
        assert_ne!(after.message_id, before.message_id, "fresh message");
        let got = y.backend.store().get(&y.loc.path).await.expect("get");
        let plain = env.get_bytes(&y.loc, got).await.expect("decrypt");
        assert_eq!(plain.as_ref(), b"payload");

        // An empty object has no frames to wait for, and still copies.
        let empty = env
            .encode(&x.loc, &ctx, Bytes::new())
            .await
            .expect("encode");
        a.inner.put(&x.loc.path, empty.into()).await.expect("put");
        env.copy(&x, &y, true).await.expect("copy empty");
        let got = y.backend.store().get(&y.loc.path).await.expect("get");
        assert!(
            env.get_bytes(&y.loc, got)
                .await
                .expect("decrypt")
                .is_empty()
        );
        assert!(a.inner.head(&x.loc.path).await.is_err(), "moved");
    }

    #[tokio::test]
    async fn copy_to_another_name_of_a_bad_source_commits_nothing() {
        let (env, _, _, _) = fixture();
        let unbound = Envelope::new(env.cfg.as_ref().clone().bind_object_key(false));
        let x_loc = resolved_mp("x", &Arc::new(TestStore::default()), false).loc;
        let y_loc = resolved_mp("y", &Arc::new(TestStore::default()), false).loc;
        let header_len = |b: &Bytes| match HeaderCodec::parse(b, &env.cfg.limits) {
            Ok(HeaderParse::Done { header_len, .. }) => header_len,
            _ => panic!("complete header"),
        };
        let flip = |b: &Bytes, at: usize| {
            let mut v = b.to_vec();
            v[at] ^= 1;
            Bytes::from(v)
        };
        let small = 100_000;
        let big = 12 << 20;
        let cases: Vec<(&str, usize, Bytes)> = {
            let mut cases = Vec::new();
            for len in [small, big] {
                let good = env
                    .encode(&x_loc, &CallerContext::new(), Bytes::from(vec![3u8; len]))
                    .await
                    .expect("encode");
                cases.push(("last tag byte", len, flip(&good, good.len() - 1)));
                cases.push(("truncated", len, good.slice(..good.len() - 100)));
                cases.push(("dropped final frame", len, good.slice(..good.len() - 60)));
                cases.push(("trailing byte", len, [&good[..], &[0]].concat().into()));
                let unbound_msg = unbound
                    .encode(&x_loc, &CallerContext::new(), Bytes::from(vec![3u8; len]))
                    .await
                    .expect("encode");
                cases.push(("unbound", len, unbound_msg));
                let other = env
                    .encode(&y_loc, &CallerContext::new(), Bytes::from(vec![3u8; len]))
                    .await
                    .expect("encode");
                cases.push(("bound to another name", len, other));
                if len == small {
                    let hl = header_len(&good);
                    cases.push(("header tag", len, flip(&good, hl - 1)));
                    cases.push(("first frame", len, flip(&good, hl + 20)));
                    cases.push(("not OSC", len, Bytes::from(vec![0u8; len])));
                }
            }
            cases
        };
        for (what, len, stored) in cases {
            for multipart in [false, true] {
                for delete in [false, true] {
                    let (a, b) = (
                        Arc::new(TestStore::default()),
                        Arc::new(TestStore::default()),
                    );
                    let (x, y) = (
                        resolved_mp("x", &a, multipart),
                        resolved_mp("y", &b, multipart),
                    );
                    a.inner
                        .put(&x.loc.path, stored.clone().into())
                        .await
                        .expect("put");
                    assert!(
                        env.copy(&x, &y, delete).await.is_err(),
                        "{what} {len} multipart {multipart} delete {delete}"
                    );
                    assert!(
                        b.inner.head(&y.loc.path).await.is_err(),
                        "{what}: committed"
                    );
                    assert!(
                        a.inner.head(&x.loc.path).await.is_ok(),
                        "{what}: source gone"
                    );
                    if multipart && len == big && what == "last tag byte" {
                        // The failure came after the upload had started.
                        assert_eq!(b.multiparts.load(Ordering::SeqCst), 1);
                    }
                }
            }
        }
    }

    async fn stored_at(store: &InMemory, name: &str) -> Bytes {
        store
            .get(&ObjectPath::from(name))
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes")
    }

    #[tokio::test]
    async fn decode_fails_when_the_header_differs_from_the_one_probed() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(InMemory::new());
        let x = resolved_at("a", "x", &store);
        put_at(&env, &x, &x, b"data").await;
        let other = CallerContext::from([("tenant".to_owned(), "evil".to_owned())]);
        let forged = env
            .encode(&x.loc, &other, Bytes::from_static(b"data"))
            .await
            .expect("encode");
        let probed = env
            .read_header(x.backend.as_ref(), &x.loc)
            .await
            .expect("header")
            .header;

        let raw = x.backend.store().get(&x.loc.path).await.expect("get");
        let mut ok = env.decode_expecting(
            x.loc.clone(),
            raw.into_stream(),
            Some(probed.clone()),
            false,
        );
        while let Some(frame) = ok.next().await {
            frame.expect("same header");
        }
        let forged_stream = stream::once(async move { Ok(forged) }).boxed();
        let mut bad = env.decode_expecting(x.loc.clone(), forged_stream, Some(probed), false);
        assert!(bad.next().await.expect("item").is_err());
    }

    #[tokio::test]
    async fn expected_header_is_compared_once_whatever_the_chunking() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(InMemory::new());
        let x = resolved_at("a", "x", &store);
        let ctx = CallerContext::from([("big".to_owned(), "x".repeat(300))]);
        let bytes = env
            .encode(&x.loc, &ctx, Bytes::from_static(b"data"))
            .await
            .expect("encode");
        let right = match HeaderCodec::parse(&bytes, &env.cfg.limits).expect("parse") {
            HeaderParse::Done { header, .. } => header,
            HeaderParse::NeedMore => panic!("complete header"),
        };
        let other = env
            .encode(&x.loc, &ctx, Bytes::from_static(b"data"))
            .await
            .expect("encode");

        let one_byte = |b: &Bytes| {
            let chunks: Vec<_> = b.iter().map(|&c| Ok(Bytes::from(vec![c]))).collect();
            stream::iter(chunks).boxed()
        };
        let mut ok =
            env.decode_expecting(x.loc.clone(), one_byte(&bytes), Some(right.clone()), false);
        let mut out = Vec::new();
        while let Some(frame) = ok.next().await {
            out.extend_from_slice(&frame.expect("same header"));
        }
        assert_eq!(out, b"data");

        let mut bad = env.decode_expecting(x.loc.clone(), one_byte(&other), Some(right), false);
        let mut failed = false;
        while let Some(frame) = bad.next().await {
            failed |= frame.is_err();
        }
        assert!(failed);
    }

    #[tokio::test]
    async fn an_oversized_stored_chunk_is_fed_in_slices() {
        let (env, _, _, loc) = fixture();
        let plain = Bytes::from(vec![0x5a; 5 * SLICE / 2]);
        let stored = env
            .encode(&loc, &CallerContext::new(), plain.clone())
            .await
            .expect("encode");
        let one_chunk = |b: &Bytes| stream::iter(vec![Ok(b.clone())]).boxed();

        let mut got = BytesMut::new();
        let mut dec = env.decode_expecting(loc.clone(), one_chunk(&stored), None, false);
        while let Some(frame) = dec.next().await {
            got.extend_from_slice(&frame.expect("decrypt"));
        }
        assert_eq!(got, plain);

        // Echo mode yields the stored bytes unchanged, one slice at a time.
        let mut echoed = BytesMut::new();
        let mut pieces = 0;
        let mut dec = env.decode_expecting(loc.clone(), one_chunk(&stored), None, true);
        while let Some(piece) = dec.next().await {
            let piece = piece.expect("echo");
            assert!(piece.len() <= SLICE);
            echoed.extend_from_slice(&piece);
            pieces += 1;
        }
        assert_eq!(echoed, stored);
        assert!(pieces >= 3, "{pieces}");

        // A tampered tail fails; the earlier slices are the only echo, and the
        // error ends the stream.
        let mut bad = stored.to_vec();
        *bad.last_mut().expect("non-empty") ^= 1;
        let bad = Bytes::from(bad);
        let mut dec = env.decode_expecting(loc, one_chunk(&bad), None, true);
        let mut echoed = Vec::new();
        let mut failed = false;
        while let Some(piece) = dec.next().await {
            match piece {
                Ok(p) => echoed.extend_from_slice(&p),
                Err(_) => failed = true,
            }
        }
        assert!(failed);
        assert!(echoed.len() < bad.len());
        assert!(bad.starts_with(&echoed));
    }

    #[tokio::test]
    async fn out_of_range_frame_length_fails_before_any_wrap() {
        let (env, _, _, loc) = fixture();
        let mut cfg = (*env.cfg).clone();
        cfg.frame_length = 1;
        let env = Envelope::new(cfg);
        let r = env
            .encode(&loc, &CallerContext::new(), Bytes::from_static(b"x"))
            .await;
        assert!(matches!(r, Err(Error::InvalidInput(_))));
        let mut s = env.encode_stream(loc, CallerContext::new(), stream::empty().boxed());
        assert!(matches!(s.next().await, Some(Err(Error::InvalidInput(_)))));
    }

    #[test]
    fn not_encrypted_errors_carry_no_credentials() {
        let url = Url::parse("s3://user:secret@bucket/obj?sig=tok#frag").expect("url");
        let e = read_err(&url, DecryptError::Format(FormatError::NotOsc));
        let Error::NotEncrypted { url } = e else {
            panic!("expected NotEncrypted");
        };
        assert_eq!(url, "s3://bucket/obj");
    }

    #[tokio::test]
    async fn cross_store_byte_copy_of_a_source_that_fails_verification_commits_nothing() {
        let (env, _, _, _) = fixture();
        let dst_store = Arc::new(InMemory::new());
        let dst = resolved("dst", &dst_store);
        let genuine = env
            .encode(
                &resolved("src", &Arc::new(InMemory::new())).loc,
                &CallerContext::new(),
                Bytes::from(vec![5u8; 200_000]),
            )
            .await
            .expect("encode");
        let other = env
            .encode(
                &resolved_at("src", "other", &Arc::new(InMemory::new())).loc,
                &CallerContext::new(),
                Bytes::from(vec![5u8; 200_000]),
            )
            .await
            .expect("encode");
        let fakes = [
            ("bound to another name", other),
            ("not an OSC message", Bytes::from(vec![0u8; 1000])),
            ("truncated", genuine.slice(..genuine.len() - 100)),
        ];
        for (what, fake) in fakes {
            for delete in [false, true] {
                let src_store = Arc::new(TestStore {
                    full: Some(InMemory::new()),
                    ..TestStore::default()
                });
                let path = ObjectPath::from("obj");
                src_store
                    .inner
                    .put(&path, genuine.clone().into())
                    .await
                    .expect("put");
                src_store
                    .full
                    .as_ref()
                    .expect("full")
                    .put(&path, fake.clone().into())
                    .await
                    .expect("put");
                let src = resolved("src", &src_store);

                let result = env.copy(&src, &dst, delete).await;
                assert!(result.is_err(), "{what} (delete {delete})");
                assert!(dst_store.head(&path).await.is_err(), "{what}");
                assert!(src_store.inner.head(&path).await.is_ok(), "{what}");
            }
        }

        // The same layout with a genuine full read still copies.
        let src_store = Arc::new(TestStore {
            full: Some(InMemory::new()),
            ..TestStore::default()
        });
        let path = ObjectPath::from("obj");
        for half in [&src_store.inner, src_store.full.as_ref().expect("full")] {
            half.put(&path, genuine.clone().into()).await.expect("put");
        }
        let src = resolved("src", &src_store);
        env.copy(&src, &dst, true).await.expect("genuine copy");
        assert_eq!(stored(&dst_store).await, genuine);
    }

    #[tokio::test]
    async fn moving_an_old_key_object_onto_itself_keeps_it() {
        let (old_env, _, _, _) = fixture();
        let provider = Keyring::from_material("kek-2", &[("kek-2", &[9; 32]), ("kek-1", &[7; 32])]);
        let env = Envelope::new(EncryptionConfig::new(Arc::new(provider)));
        let store = Arc::new(InMemory::new());
        let x = resolved_at("a", "x", &store);
        put_at(&old_env, &x, &x, b"data").await;

        env.copy(&x, &x, true).await.expect("move onto itself");
        let info = env
            .inspect(x.backend.as_ref(), &x.loc)
            .await
            .expect("inspect");
        assert_eq!(info.key_names, ["kek-2"]);
        let got = env
            .get_bytes(&x.loc, store.get(&x.loc.path).await.expect("get"))
            .await
            .expect("decrypt");
        assert_eq!(got.as_ref(), b"data");
    }

    #[tokio::test]
    async fn describing_a_missing_object_is_not_found() {
        let (env, _, backend, loc) = fixture();
        assert!(matches!(
            env.describe(&backend, &loc).await,
            Err(Error::ObjectStore(object_store::Error::NotFound { .. }))
        ));
    }

    #[tokio::test]
    async fn a_failed_header_read_is_the_stores_error_not_a_plain_object() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(TestStore {
            fail_ranged_reads: true,
            ..TestStore::default()
        });
        let x = resolved_at("same", "x", &store);
        store
            .put(&x.loc.path, Bytes::from_static(b"not empty").into())
            .await
            .expect("put");
        let err = env.describe(x.backend.as_ref(), &x.loc).await;
        assert!(
            matches!(
                &err,
                Err(Error::ObjectStore(object_store::Error::Generic { .. }))
            ),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn move_onto_itself_keeps_an_active_key_object() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(InMemory::new());
        let x = resolved_at("a", "x", &store);
        put_at(&env, &x, &x, b"data").await;
        let before = stored_at(&store, "x").await;

        env.copy(&x, &x, true).await.expect("move onto itself");
        assert_eq!(stored_at(&store, "x").await, before);
    }

    /// A cross-device rename of a `file` object falls back to a verified stream
    /// copy; any other copy error, and any error of another scheme, is the
    /// caller's error and commits nothing.
    #[tokio::test]
    async fn copy_falls_back_to_streaming_only_on_a_cross_device_error() {
        let provider = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
        let env = Envelope::new(EncryptionConfig::new(Arc::new(provider)).bind_object_key(false));
        for (scheme, error, copied) in [
            // EXDEV, EACCES
            ("file", 18, true),
            ("file", 13, false),
            ("test", 18, false),
        ] {
            let store = Arc::new(TestStore {
                copy_error: Some(error),
                ..TestStore::default()
            });
            let backend: Arc<dyn Backend> = Arc::new(TestBackend {
                store: store.clone(),
                multipart: false,
            });
            let at = |name: &str| Resolved {
                backend: Arc::clone(&backend),
                loc: Location {
                    url: Url::parse(&format!("{scheme}:///{name}")).expect("url"),
                    path: ObjectPath::from(name),
                },
            };
            let (x, y) = (at("x"), at("y"));
            let bytes = env
                .encode(&x.loc, &CallerContext::new(), Bytes::from_static(b"data"))
                .await
                .expect("encode");
            store
                .put(&x.loc.path, bytes.clone().into())
                .await
                .expect("put");

            let result = env.copy(&x, &y, true).await;
            assert_eq!(result.is_ok(), copied, "{scheme} {error}: {result:?}");
            if copied {
                let got = store.get(&y.loc.path).await.expect("copied");
                assert_eq!(got.bytes().await.expect("bytes"), bytes);
                assert!(store.head(&x.loc.path).await.is_err());
            } else {
                assert!(store.head(&y.loc.path).await.is_err());
                assert!(store.head(&x.loc.path).await.is_ok());
            }
        }
    }

    /// Copying an active-key object onto itself writes nothing.
    #[tokio::test]
    async fn copy_onto_itself_leaves_an_active_key_object_untouched() {
        let (env, _, _, _) = fixture();
        let store = Arc::new(InMemory::new());
        let x = resolved_at("a", "x", &store);
        put_at(&env, &x, &x, b"data").await;
        let before = store.head(&x.loc.path).await.expect("head");

        env.copy(&x, &x, false).await.expect("copy onto itself");
        assert_eq!(store.head(&x.loc.path).await.expect("head"), before);
    }

    #[derive(Debug)]
    struct BrokenProvider;

    #[async_trait]
    impl KeyProvider for BrokenProvider {
        #[expect(clippy::unnecessary_literal_bound, reason = "trait signature")]
        fn namespace(&self) -> &str {
            "osc-file"
        }

        fn active_key_name(&self) -> String {
            "broken".to_owned()
        }

        async fn wrap(
            &self,
            _dek: &DataKey,
            _ctx: &EncryptionContext,
        ) -> std::result::Result<Vec<EncryptedDataKey>, KeyError> {
            Err(KeyError::Rng)
        }

        async fn unwrap(
            &self,
            _edks: &[EncryptedDataKey],
            _ctx: &EncryptionContext,
        ) -> std::result::Result<DataKey, KeyError> {
            Err(KeyError::Rng)
        }
    }

    #[tokio::test]
    async fn a_provider_that_cannot_wrap_fails_the_encrypting_stream() {
        let env = Envelope::new(EncryptionConfig::new(Arc::new(BrokenProvider)));
        let (_, _, _, loc) = fixture();
        let plain = stream::iter([Ok(Bytes::from_static(b"data"))]).boxed();
        let mut s = env.encode_stream(loc, CallerContext::new(), plain);
        assert!(matches!(
            s.next().await,
            Some(Err(Error::Key(KeyError::Rng)))
        ));
        assert!(s.next().await.is_none(), "nothing follows the error");
    }

    #[tokio::test]
    async fn a_failing_source_stream_fails_the_decrypting_stream() {
        let (env, _, _, loc) = fixture();
        let full = env
            .encode(&loc, &CallerContext::new(), Bytes::from_static(b"payload"))
            .await
            .expect("encode");
        let stored = stream::iter([
            Ok(full.slice(..10)),
            Err(object_store::Error::Generic {
                store: "faulty",
                source: "connection reset".into(),
            }),
        ])
        .boxed();
        let mut s = env.decode(loc, stored);
        assert!(matches!(
            s.next().await,
            Some(Err(Error::ObjectStore(object_store::Error::Generic { .. })))
        ));
        assert!(s.next().await.is_none());
    }

    // --- reencrypt ---

    /// What a [`Meddle`] store does, once, when its trigger is reached.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum When {
        /// Before the first unranged, non-HEAD read (the pinned body read).
        Get,
        /// Before the first HEAD (the commit-time re-check).
        Head,
        /// Before the first conditional put.
        Put,
    }

    /// An `InMemory` wrapper that, once, replaces (or with `newer: None`
    /// deletes) the object it holds at the moment of `when`, as a concurrent
    /// writer would, and counts multipart aborts.
    #[derive(Debug)]
    struct Meddle {
        inner: Arc<InMemory>,
        when: When,
        newer: Option<Bytes>,
        fired: std::sync::atomic::AtomicBool,
        aborts: Arc<AtomicUsize>,
    }

    impl fmt::Display for Meddle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Meddle")
        }
    }

    #[derive(Debug)]
    struct CountAborts(Box<dyn MultipartUpload>, Arc<AtomicUsize>);

    #[async_trait]
    impl MultipartUpload for CountAborts {
        fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
            self.0.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.0.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.1.fetch_add(1, Ordering::SeqCst);
            self.0.abort().await
        }
    }

    impl Meddle {
        async fn at(&self, now: When, location: &ObjectPath) {
            if now == self.when && !self.fired.swap(true, Ordering::SeqCst) {
                match &self.newer {
                    Some(bytes) => {
                        self.inner
                            .put(location, bytes.clone().into())
                            .await
                            .expect("meddle");
                    }
                    None => self.inner.delete(location).await.expect("meddle"),
                }
            }
        }
    }

    #[async_trait]
    impl ObjectStore for Meddle {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if matches!(opts.mode, object_store::PutMode::Update(_)) {
                self.at(When::Put, location).await;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let upload = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(CountAborts(upload, Arc::clone(&self.aborts))))
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.at(When::Head, location).await;
            } else if options.range.is_none() {
                self.at(When::Get, location).await;
            }
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

    /// `kek-1` is the old key, `kek-2` the active one.
    fn rotated() -> (Envelope, Envelope) {
        let old = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
        let new = Keyring::from_material("kek-2", &[("kek-2", &[9; 32]), ("kek-1", &[7; 32])]);
        (
            Envelope::new(EncryptionConfig::new(Arc::new(old))),
            Envelope::new(EncryptionConfig::new(Arc::new(new))),
        )
    }

    fn from_kek1(before: Option<chrono::DateTime<chrono::Utc>>, dry_run: bool) -> ReencryptOptions {
        ReencryptOptions {
            from: "kek-1".to_owned(),
            before,
            dry_run,
        }
    }

    fn resolved_with(store: Arc<dyn ObjectStore>, multipart: bool) -> Resolved {
        Resolved {
            backend: Arc::new(TestBackend { store, multipart }),
            loc: Location {
                url: Url::parse("test://bucket/dir/obj").expect("url"),
                path: ObjectPath::from("dir/obj"),
            },
        }
    }

    async fn seal_with(env: &Envelope, at: &Resolved, plain: &[u8]) -> Bytes {
        let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
        let bytes = env
            .encode(&at.loc, &ctx, Bytes::copy_from_slice(plain))
            .await
            .expect("encode");
        at.backend
            .store()
            .put(&at.loc.path, bytes.clone().into())
            .await
            .expect("put");
        bytes
    }

    async fn read_back(env: &Envelope, at: &Resolved) -> Bytes {
        let got = at.backend.store().get(&at.loc.path).await.expect("get");
        env.get_bytes(&at.loc, got).await.expect("decrypt")
    }

    async fn raw(at: &Resolved) -> Bytes {
        at.backend
            .store()
            .get(&at.loc.path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes")
    }

    #[tokio::test]
    async fn reencrypt_moves_an_object_to_the_active_key_and_is_then_skipped() {
        let (old, env) = rotated();
        let store = Arc::new(InMemory::new());
        let at = resolved_with(store, false);
        let before = seal_with(&old, &at, b"payload").await;

        let opts = from_kek1(None, false);
        assert_eq!(
            env.reencrypt(&at, &opts).await.expect("reencrypt"),
            Reencrypted::Rewritten
        );
        let after = raw(&at).await;
        assert_ne!(before, after);
        let info = env
            .inspect(at.backend.as_ref(), &at.loc)
            .await
            .expect("info");
        assert_eq!(info.key_names, ["kek-2"]);
        assert_eq!(info.context["tenant"], "acme");
        assert_eq!(info.context["osc:key"], "dir/obj");
        assert_eq!(read_back(&env, &at).await.as_ref(), b"payload");

        assert_eq!(
            env.reencrypt(&at, &opts).await.expect("again"),
            Reencrypted::Skipped
        );
        assert_eq!(raw(&at).await, after);
    }

    #[tokio::test]
    async fn reencrypt_skips_other_keys_and_refuses_the_active_one() {
        let (_, env) = rotated();
        let other = Keyring::from_material("kek-3", &[("kek-3", &[3; 32])]);
        let other = Envelope::new(EncryptionConfig::new(Arc::new(other)));
        let store = Arc::new(TestStore::default());
        let at = resolved_with(store.clone(), false);
        let bytes = seal_with(&other, &at, b"payload").await;
        let calls = store.requested().len();

        let skipped = env.reencrypt(&at, &from_kek1(None, false)).await;
        assert_eq!(skipped.expect("skip"), Reencrypted::Skipped);
        assert_eq!(raw(&at).await, bytes);

        let active = ReencryptOptions {
            from: "kek-2".to_owned(),
            ..from_kek1(None, false)
        };
        let before = store.requested().len();
        let err = env.reencrypt(&at, &active).await;
        assert!(matches!(err, Err(Error::InvalidInput(_))), "{err:?}");
        // Refused before any I/O.
        assert_eq!(store.requested().len(), before);
        assert!(calls < before);
    }

    #[tokio::test]
    async fn reencrypt_leaves_plain_and_empty_objects_alone() {
        let (_, env) = rotated();
        let store = Arc::new(InMemory::new());
        let at = resolved_with(store.clone(), false);
        for body in [&b"just some plain text"[..], b""] {
            store
                .put(&at.loc.path, Bytes::copy_from_slice(body).into())
                .await
                .expect("put");
            let got = env.reencrypt(&at, &from_kek1(None, false)).await;
            assert_eq!(got.expect("outcome"), Reencrypted::NotEncrypted);
            assert_eq!(raw(&at).await.as_ref(), body);
        }
    }

    #[tokio::test]
    async fn reencrypt_honours_the_cutoff_and_a_dry_run_writes_nothing() {
        let (old, env) = rotated();
        let store = Arc::new(InMemory::new());
        let at = resolved_with(store, false);
        let bytes = seal_with(&old, &at, b"payload").await;
        let modified = at.backend.store().head(&at.loc.path).await.expect("head");
        let modified = modified.last_modified;
        let hour = chrono::Duration::hours(1);

        // At the cutoff is after it.
        let got = env.reencrypt(&at, &from_kek1(Some(modified), false)).await;
        assert_eq!(got.expect("outcome"), Reencrypted::AfterCutoff);
        let got = env
            .reencrypt(&at, &from_kek1(Some(modified - hour), true))
            .await;
        assert_eq!(got.expect("outcome"), Reencrypted::AfterCutoff);

        let got = env
            .reencrypt(&at, &from_kek1(Some(modified + hour), true))
            .await;
        assert_eq!(got.expect("outcome"), Reencrypted::WouldRewrite);
        assert_eq!(raw(&at).await, bytes);

        let got = env
            .reencrypt(&at, &from_kek1(Some(modified + hour), false))
            .await;
        assert_eq!(got.expect("outcome"), Reencrypted::Rewritten);
    }

    #[tokio::test]
    async fn reencrypt_of_a_tampered_object_fails_and_changes_nothing() {
        let (old, env) = rotated();
        for multipart in [false, true] {
            let store = Arc::new(InMemory::new());
            let at = resolved_with(store, multipart);
            let bytes = seal_with(&old, &at, b"payload").await;
            let mut bad = bytes.to_vec();
            let last = bad.len() - 1;
            bad[last] ^= 1;
            at.backend
                .store()
                .put(&at.loc.path, Bytes::from(bad.clone()).into())
                .await
                .expect("put");

            let got = env.reencrypt(&at, &from_kek1(None, false)).await;
            assert!(matches!(got, Err(Error::Decryption(_))), "{got:?}");
            assert_eq!(raw(&at).await.as_ref(), bad.as_slice());
        }
    }

    #[tokio::test]
    async fn reencrypt_of_a_corrupt_header_is_an_error_not_a_plain_object() {
        let (old, env) = rotated();
        let store = Arc::new(InMemory::new());
        let at = resolved_with(store, false);
        let bytes = seal_with(&old, &at, b"payload").await;
        at.backend
            .store()
            .put(&at.loc.path, bytes.slice(..20).into())
            .await
            .expect("put");
        let got = env.reencrypt(&at, &from_kek1(None, false)).await;
        assert!(
            got.is_err() && !matches!(got, Err(Error::NotEncrypted { .. })),
            "{got:?}"
        );
    }

    /// An `InMemory` wrapper whose reads report neither an e-tag nor a version.
    #[derive(Debug)]
    struct Unversioned(InMemory);

    impl fmt::Display for Unversioned {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Unversioned")
        }
    }

    #[async_trait]
    impl ObjectStore for Unversioned {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.0.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let mut got = self.0.get_opts(location, options).await?;
            got.meta.e_tag = None;
            got.meta.version = None;
            Ok(got)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.0.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn reencrypt_refuses_an_object_without_an_e_tag_or_version() {
        let (old, env) = rotated();
        let at = resolved_with(Arc::new(Unversioned(InMemory::new())), false);
        let bytes = seal_with(&old, &at, b"payload").await;

        // A dry run decides without writing, so it still answers.
        let dry = env.reencrypt(&at, &from_kek1(None, true)).await;
        assert_eq!(dry.expect("dry run"), Reencrypted::WouldRewrite);
        let got = env.reencrypt(&at, &from_kek1(None, false)).await;
        assert!(
            matches!(&got, Err(Error::Generic(m)) if m.contains("neither an e-tag nor a version")),
            "{got:?}"
        );
        assert_eq!(raw(&at).await, bytes);
    }

    /// Runs a reencrypt of an object that `when` replaces with `newer` (or
    /// deletes), and returns the outcome and the meddling store.
    async fn meddled(
        when: When,
        newer: Option<&'static [u8]>,
        len: usize,
        multipart: bool,
    ) -> (Reencrypted, Resolved, Arc<Meddle>, Bytes) {
        let (old, env) = rotated();
        let store = Arc::new(Meddle {
            inner: Arc::new(InMemory::new()),
            when,
            newer: newer.map(Bytes::from_static),
            fired: std::sync::atomic::AtomicBool::new(false),
            aborts: Arc::default(),
        });
        let at = resolved_with(store.clone(), multipart);
        let plain = vec![5u8; len];
        let bytes = seal_with(&old, &at, &plain).await;
        let got = env
            .reencrypt(&at, &from_kek1(None, false))
            .await
            .expect("reencrypt");
        (got, at, store, bytes)
    }

    #[tokio::test]
    async fn an_object_replaced_at_each_step_of_a_single_put_is_changed() {
        for when in [When::Get, When::Head, When::Put] {
            let (got, at, _, _) = meddled(when, Some(b"newer"), 100, false).await;
            assert_eq!(got, Reencrypted::Changed, "{when:?}");
            assert_eq!(raw(&at).await.as_ref(), b"newer", "{when:?}");
        }
    }

    #[tokio::test]
    async fn an_object_deleted_before_the_commit_is_changed_and_stays_deleted() {
        for when in [When::Get, When::Head] {
            let (got, at, _, _) = meddled(when, None, 100, false).await;
            assert_eq!(got, Reencrypted::Changed, "{when:?}");
            let head = at.backend.store().head(&at.loc.path).await;
            assert!(
                matches!(head, Err(object_store::Error::NotFound { .. })),
                "{when:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_multipart_object_replaced_before_completion_is_changed_and_aborted() {
        let (got, at, store, _) = meddled(When::Head, Some(b"newer"), 9 << 20, true).await;
        assert_eq!(got, Reencrypted::Changed);
        assert_eq!(raw(&at).await.as_ref(), b"newer");
        assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_unmolested_multipart_object_is_rewritten() {
        // The trigger never fires: the first HEAD happens at the commit, and
        // `Put` is only reached by a single put.
        let (got, at, store, _) = meddled(When::Put, Some(b"newer"), 9 << 20, true).await;
        assert_eq!(got, Reencrypted::Rewritten);
        assert_eq!(store.aborts.load(Ordering::SeqCst), 0);
        let (_, env) = rotated();
        assert_eq!(read_back(&env, &at).await.len(), 9 << 20);
    }
}
