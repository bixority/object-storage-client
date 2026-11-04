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
use super::header::{CLASSIFY_LEN, DecodeLimits, FormatError, Header, HeaderCodec, HeaderParse};
use super::keys::keyring::NAMESPACE;
use super::keys::raw_aes::key_name_in;
use super::keys::{KeyError, KeyProvider, Keyring};
use super::suite::{DecryptError, fresh_message_material};
use crate::backend::{Backend, body_stream};
use crate::client::{EncryptionInfo, Error, ObjectMetadata, ReencryptOptions, Reencrypted, Result};
use crate::codec::{
    ByteStream, CallerContext, Codec, Location, MAX_PREALLOC, Resolved, delete_unchanged_source,
    redacted, same_object, sealed,
};
use crate::hex;
use crate::upload::{UploadOptions, upload_stream};

/// Default frame length of new messages.
const DEFAULT_FRAME_LENGTH: u32 = 65536;
/// Most plaintext handled between two await points: bounds memory and the
/// time between await points.
const SLICE: usize = 1 << 20;

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

/// `namespace` is the provider ID whose EDKs name the keys listed.
pub(crate) fn encryption_info(h: &Header, namespace: &str) -> EncryptionInfo {
    EncryptionInfo {
        format: FORMAT_NAME.to_owned(),
        message_id: hex::encode(&h.message_id),
        key_names: h
            .edks
            .iter()
            .filter_map(|e| key_name_in(e, namespace).map(str::to_owned))
            .collect(),
        frame_length: h.frame_length,
        context: h.context.0.clone(),
    }
}

/// Settings of the encrypted client.
#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    /// Wraps and unwraps data keys.
    key_provider: Arc<dyn KeyProvider>,
    /// Bind each ciphertext to its object key (`osc:key`); on by default.
    bind_object_key: bool,
    /// Frame length of new messages.
    frame_length: u32,
    /// Limits applied to untrusted headers, and checked on write.
    limits: DecodeLimits,
}

impl EncryptionConfig {
    /// Defaults: object key bound, 64 KiB frames, default limits.
    #[must_use]
    pub fn new(keyring: Arc<Keyring>) -> Self {
        Self::with_provider(keyring)
    }

    /// [`Self::new`] over any key provider (internal test providers).
    pub(crate) fn with_provider(key_provider: Arc<dyn KeyProvider>) -> Self {
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

/// Bytes of the first header read. A typical header is a few hundred bytes.
const FIRST_HEADER_READ: usize = 1024;

/// One ranged GET of the first `end` bytes of an object, with its metadata.
async fn read_head(
    backend: &dyn Backend,
    loc: &Location,
    end: u64,
) -> Result<(ObjectMeta, Attributes, Bytes)> {
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
    Ok((meta, attributes, buf))
}

/// Reads and parses a message header with a ranged GET of the first 1 KiB.
/// A header that does not fit there is read again with one GET of up to
/// `max_header_len` bytes, which covers any header the limits accept. Needs
/// no keys.
async fn read_header(
    backend: &dyn Backend,
    loc: &Location,
    limits: &DecodeLimits,
) -> Result<StoredHeader> {
    let max = limits.max_header_len as u64;
    let mut end = max.min(FIRST_HEADER_READ as u64);
    loop {
        let (meta, attributes, buf) = read_head(backend, loc, end).await?;
        if buf.len() < CLASSIFY_LEN {
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
            } => {
                return Ok(StoredHeader {
                    header,
                    header_len,
                    meta,
                    attributes,
                });
            }
            // The object may hold more than was read: ask for the rest once.
            HeaderParse::NeedMore if end < max && buf.len() as u64 == end => end = max,
            HeaderParse::NeedMore => {
                return Err(read_err(
                    &loc.url,
                    DecryptError::Format(FormatError::Truncated),
                ));
            }
        }
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
    Ok(encryption_info(&h.header, NAMESPACE))
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
            self.cfg.key_provider.namespace(),
        ))
    }

    /// Checks `required` like a write context (reserved keys are rejected
    /// with [`Error::InvalidInput`]), before any I/O.
    pub(crate) fn check_required(required: &CallerContext) -> Result<()> {
        EncryptionContext::new_reserved(None)
            .with_caller(required)
            .map(drop)
            .map_err(|e| write_err(&e))
    }

    /// Like [`CodecImpl::decode`], and the header must carry `required`.
    pub(crate) fn decode_requiring(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
        required: CallerContext,
    ) -> ByteStream {
        let dec = self.decryptor(self.bound_key(&loc), required);
        self.decode_with(dec, loc, stored, DecodeMode::Plaintext { expect: None })
    }

    /// Like [`CodecImpl::get_bytes`], and the header must carry `required`.
    pub(crate) async fn get_bytes_requiring(
        &self,
        loc: &Location,
        result: GetResult,
        required: CallerContext,
    ) -> Result<Bytes> {
        let dec = self.decryptor(self.bound_key(loc), required);
        let size = usize::try_from(result.meta.size).unwrap_or(usize::MAX);
        let mut dec = dec.whole_object();
        let mut stored = body_stream(result);
        let mut reserved = false;
        // The header, key and every frame are checked as they arrive, so a
        // bad or over-long object fails before the rest is downloaded. A
        // chunk longer than SLICE is fed in slices, yielding between them, so
        // one poll never decrypts more than SLICE.
        while let Some(chunk) = stored.next().await {
            let mut chunk = chunk?;
            while !chunk.is_empty() {
                let slice = chunk.split_to(chunk.len().min(SLICE));
                push(&mut dec, slice, self.cfg.key_provider.as_ref())
                    .await
                    .map_err(|e| read_err(&loc.url, e))?;
                if !reserved && dec.header().is_some() {
                    dec.reserve(size.min(MAX_PREALLOC));
                    reserved = true;
                }
                if !chunk.is_empty() {
                    tokio::task::yield_now().await;
                }
            }
        }
        dec.finish_whole().map_err(|e| read_err(&loc.url, e))
    }

    /// A decryptor with this envelope's limits; the header must carry
    /// `expected_key` (if any) as its `osc:key` and all of `required`.
    fn decryptor(&self, expected_key: Option<String>, required: CallerContext) -> Decryptor {
        Decryptor::new(self.cfg.limits.clone(), expected_key).require_context(required)
    }

    /// The `osc:key` a header stored at `loc` must carry, if keys are bound.
    fn bound_key(&self, loc: &Location) -> Option<String> {
        self.cfg.bind_object_key.then(|| loc.bound_key().to_owned())
    }

    /// A decrypting stream; `mode` says what it yields and what it checks or
    /// records once the header authenticated. `expected_key` is the `osc:key`
    /// the header must carry, if any.
    fn decode_stream(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
        expected_key: Option<String>,
        mode: DecodeMode,
    ) -> ByteStream {
        let dec = self.decryptor(expected_key, CallerContext::new());
        self.decode_with(dec, loc, stored, mode)
    }

    /// [`Self::decode_stream`] with the decryptor given.
    fn decode_with(
        &self,
        dec: Decryptor,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
        mode: DecodeMode,
    ) -> ByteStream {
        stream::unfold(
            DecodeState {
                stored,
                cur: Bytes::new(),
                dec: Some(dec),
                ready: VecDeque::new(),
                cfg: Arc::clone(&self.cfg),
                url: loc.url,
                mode,
                header_seen: false,
            },
            DecodeState::step,
        )
        .boxed()
    }

    /// Encrypting stream that writes a header with `context`.
    fn encode_with(&self, context: EncryptionContext, plain: ByteStream) -> ByteStream {
        stream::unfold(
            EncodeState::Start {
                envelope: self.clone(),
                context,
                plain,
            },
            EncodeState::step,
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
        let namespace = self.cfg.key_provider.namespace();
        header
            .edks
            .iter()
            .any(|e| key_name_in(e, namespace) == Some(name))
    }

    /// Refuses, before any I/O, a `from` key that is the active key or that
    /// the key provider does not hold.
    fn check_reencrypt_from(&self, opts: &ReencryptOptions) -> Result<()> {
        let active = self.cfg.key_provider.active_key_name();
        if opts.from == active {
            return Err(Error::InvalidInput(format!(
                "cannot re-encrypt from the active key {active:?}; rotate the keyring first"
            )));
        }
        // Also refused before any I/O: an unknown key would skip every object.
        if !self.cfg.key_provider.holds_key(&opts.from) {
            return Err(Error::Key(KeyError::UnknownId {
                id: opts.from.clone(),
            }));
        }
        Ok(())
    }

    /// Rewrites the object at `r` from key `opts.from` to the active key; see
    /// [`Client::reencrypt`](crate::Client::<Envelope>::reencrypt).
    pub(crate) async fn reencrypt(
        &self,
        r: &Resolved,
        opts: &ReencryptOptions,
    ) -> Result<Reencrypted> {
        self.check_reencrypt_from(opts)?;
        let stored = match self.read_header(r.backend.as_ref(), &r.loc).await {
            Ok(stored) => stored,
            Err(Error::NotEncrypted { .. }) => return Ok(Reencrypted::NotEncrypted),
            Err(e) => return Err(e),
        };
        if !self.names_key(&stored.header, &opts.from) {
            return Ok(Reencrypted::Skipped);
        }
        // A store that reports no modification time yields the epoch; such an
        // object cannot be shown to predate the cutoff.
        let modified = stored.meta.last_modified;
        if opts
            .before
            .is_some_and(|b| modified >= b || modified == chrono::DateTime::UNIX_EPOCH)
        {
            return Ok(Reencrypted::AfterCutoff);
        }
        // The object keeps its own binding (or none), whatever this client's
        // `bind_object_key`; a binding client still refuses an object bound to
        // another name, as its reads would.
        let binding = stored.header.context.get(KEY_OBJECT).map(str::to_owned);
        if self.cfg.bind_object_key
            && let Some(found) = &binding
            && found.as_str() != r.loc.path.as_ref()
        {
            return Err(read_err(
                &r.loc.url,
                DecryptError::BindingMismatch {
                    expected: r.loc.path.to_string(),
                    found: found.clone(),
                },
            ));
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

        // Before any write: a context that cannot be written fails here.
        let context = self.context_bound(binding.as_deref(), &caller_context(&stored.header))?;

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
        let plain = self.decode_stream(
            r.loc.clone(),
            body_stream(got),
            binding,
            DecodeMode::Plaintext {
                expect: Some(stored.header),
            },
        );
        let upload = self.encode_with(context, plain);
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

    /// The context of a message written to `loc`. Fails on reserved caller
    /// keys, before any I/O.
    fn context(&self, loc: &Location, ctx: &CallerContext) -> Result<EncryptionContext> {
        self.context_bound(self.cfg.bind_object_key.then(|| loc.bound_key()), ctx)
    }

    /// Like [`Self::context`] with the `osc:key` binding given explicitly.
    fn context_bound(&self, bound: Option<&str>, ctx: &CallerContext) -> Result<EncryptionContext> {
        // `limits()` set after `frame_length()` can leave the pair inconsistent;
        // fail here, before any I/O.
        self.cfg
            .limits
            .check_frame_length(self.cfg.frame_length)
            .map_err(|e| write_err(&e))?;
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
    mode: DecodeMode,
    /// The authenticated header was handled (compared or recorded).
    header_seen: bool,
}

/// What a decrypting stream yields, and what it checks or records once the
/// header authenticated.
enum DecodeMode {
    /// The plaintext; a `Some` header must equal the authenticated one
    /// (compared once).
    Plaintext { expect: Option<Header> },
    /// The plaintext; `slot` receives the authenticated header's caller
    /// context before any plaintext is yielded.
    PlaintextFilling { slot: Arc<OnceLock<CallerContext>> },
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
                    // store's chunking.
                    if chunk.len() > SLICE {
                        self.cur = chunk.split_off(SLICE);
                    }
                    match push(&mut dec, chunk, self.cfg.key_provider.as_ref()).await {
                        Ok(frames) => {
                            // `push` unwraps the key before returning, so the
                            // header is authenticated (tag and, with binding on,
                            // `osc:key`) as soon as it exists. Handle it once.
                            if !self.header_seen
                                && let Some(got) = dec.header()
                            {
                                self.header_seen = true;
                                let changed = match &self.mode {
                                    DecodeMode::Plaintext { expect } => {
                                        expect.as_ref().is_some_and(|x| x != got)
                                    }
                                    DecodeMode::PlaintextFilling { slot } => {
                                        let _ = slot.set(caller_context(got));
                                        self.mode = DecodeMode::Plaintext { expect: None };
                                        false
                                    }
                                };
                                if changed {
                                    return Some((Err(Error::Changed), self));
                                }
                            }
                            self.ready.extend(frames);
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

    fn check_write(&self, loc: &Location, ctx: &CallerContext) -> Result<()> {
        self.context(loc, ctx).map(drop)
    }

    fn encode_stream(&self, loc: Location, ctx: CallerContext, plain: ByteStream) -> ByteStream {
        match self.context(&loc, &ctx) {
            Ok(context) => self.encode_with(context, plain),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    fn decode(
        &self,
        loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
    ) -> ByteStream {
        self.decode_requiring(loc, stored, CallerContext::new())
    }

    async fn get_bytes(&self, loc: &Location, result: GetResult) -> Result<Bytes> {
        self.get_bytes_requiring(loc, result, CallerContext::new())
            .await
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
            encryption: Some(encryption_info(
                &h.header,
                self.cfg.key_provider.namespace(),
            )),
        })
    }

    /// Copies or moves an encrypted object by decrypting the source and
    /// re-encrypting it with a fresh data key and message ID, for every
    /// destination. The header is never re-wrapped or byte-copied: it binds the
    /// object name, and a second header tag under the same data key and message
    /// ID would reuse a GCM nonce. The new header's caller context comes from
    /// the source header once its tag and binding verified, and a bad source
    /// fails the stream before the upload commits. Onto itself, it does
    /// nothing.
    async fn copy(&self, src: &Resolved, dst: &Resolved, delete_source: bool) -> Result<()> {
        let in_place = same_object(src, dst).await;
        // Copying an object onto itself does nothing, whatever its key.
        if in_place && (!self.cfg.bind_object_key || src.loc.path == dst.loc.path) {
            return Ok(());
        }
        self.context(&dst.loc, &CallerContext::new())?;
        let got = src.backend.store().get(&src.loc.path).await?;
        let read = got.meta.clone();
        let slot = Arc::new(OnceLock::new());
        let plain = self.decode_stream(
            src.loc.clone(),
            body_stream(got),
            self.bound_key(&src.loc),
            DecodeMode::PlaintextFilling {
                slot: Arc::clone(&slot),
            },
        );
        let upload = self.encode_deferred(dst.loc.clone(), slot, plain);
        let mut options =
            UploadOptions::copying_from(src.backend.as_ref(), &src.loc.path, read.size);
        options.move_source = delete_source;
        if in_place {
            // A re-bind through a `file://` alias (a symlinked directory)
            // rewrites the file it reads: commit only if it is still the
            // version that was read.
            options.if_unchanged = Some(UpdateVersion {
                e_tag: read.e_tag.clone(),
                version: read.version.clone(),
            });
        }
        upload_stream(dst.backend.as_ref(), &dst.loc.path, upload, &options).await?;

        // Only after the destination committed, and never the object just
        // written: through another name for the source, the write replaced it.
        if delete_source && !in_place {
            delete_unchanged_source(src, &read).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
