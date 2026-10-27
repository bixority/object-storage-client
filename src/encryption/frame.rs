//! Framed message body: streaming [`Encryptor`] and [`Decryptor`], and
//! [`FrameLayout`] for plaintext-size arithmetic.
//!
//! Frame layout (suite `0x0478`, framed content):
//!
//! ```text
//! regular: seq(4) | iv(12) | ciphertext(frame_length) | tag(16)
//! final:   0xFFFFFFFF | seq(4) | iv(12) | len(4) | ciphertext(len) | tag(16)
//! ```
//!
//! The IV is eight zero bytes followed by the sequence number. The frame AAD
//! is `message_id | frame-type string | seq | content length (u64)`.

use aws_lc_rs::aead::{Aad, Nonce};
use bytes::{Buf, Bytes, BytesMut};

use super::context::EncryptionContext;
use super::header::{
    DecodeLimits, EncryptedDataKey, FormatError, Header, HeaderCodec, HeaderParse,
};
use super::keys::{DataKey, KeyProvider};
use super::suite::{DecryptError, MessageKeys, derive, header_tag, verify_header};

const FINAL_MARKER: u32 = 0xFFFF_FFFF;
const STRING_REGULAR: &[u8] = b"AWSKMSEncryptionClient Frame";
const STRING_FINAL: &[u8] = b"AWSKMSEncryptionClient Final Frame";
const IV_LEN: usize = 12;
const TAG_LEN: usize = 16;
/// Bytes of a regular frame besides its content: `seq`, IV and tag.
const REGULAR_OVERHEAD: usize = 4 + IV_LEN + TAG_LEN;
/// Bytes of a final frame before its content: marker, `seq`, IV and length.
const FINAL_PREFIX: usize = 4 + 4 + IV_LEN + 4;
/// Bytes of a final frame besides its content.
const FINAL_OVERHEAD: u64 = (FINAL_PREFIX + TAG_LEN) as u64;

fn frame_iv(seq: u32) -> [u8; IV_LEN] {
    let mut iv = [0u8; IV_LEN];
    iv[8..].copy_from_slice(&seq.to_be_bytes());
    iv
}

/// Longest frame AAD: message ID, the final-frame string, `seq` and the length.
const AAD_MAX: usize = 32 + STRING_FINAL.len() + 4 + 8;

/// The AAD of a frame in a stack array, with the length in use.
fn frame_aad(
    message_id: &[u8; 32],
    is_final: bool,
    seq: u32,
    content_len: u64,
) -> ([u8; AAD_MAX], usize) {
    let kind = if is_final {
        STRING_FINAL
    } else {
        STRING_REGULAR
    };
    let mut aad = [0; AAD_MAX];
    let mut rest = &mut aad[..];
    for part in [
        &message_id[..],
        kind,
        &seq.to_be_bytes(),
        &content_len.to_be_bytes(),
    ] {
        let (head, tail) = rest.split_at_mut(part.len());
        head.copy_from_slice(part);
        rest = tail;
    }
    let used = AAD_MAX - rest.len();
    (aad, used)
}

/// Appends one sealed frame to `out`.
fn seal_frame(
    keys: &MessageKeys,
    message_id: &[u8; 32],
    seq: u32,
    is_final: bool,
    plain: &[u8],
    out: &mut BytesMut,
) {
    let len32 = u32::try_from(plain.len()).expect("frame content fits in u32");
    let prefix = if is_final { FINAL_PREFIX } else { 4 + IV_LEN };
    out.reserve(prefix + plain.len() + TAG_LEN);
    if is_final {
        out.extend_from_slice(&FINAL_MARKER.to_be_bytes());
    }
    out.extend_from_slice(&seq.to_be_bytes());
    let iv = frame_iv(seq);
    out.extend_from_slice(&iv);
    if is_final {
        out.extend_from_slice(&len32.to_be_bytes());
    }
    let start = out.len();
    out.extend_from_slice(plain);
    let (aad, aad_len) = frame_aad(message_id, is_final, seq, u64::from(len32));
    let tag = keys
        .derived
        .seal_in_place_separate_tag(
            Nonce::assume_unique_for_key(iv),
            Aad::from(&aad[..aad_len]),
            &mut out[start..],
        )
        .expect("AES-GCM seal of a bounded frame");
    out.extend_from_slice(tag.as_ref());
}

fn to_usize(v: u32) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

/// Streaming encryptor for one message. Not `Clone`: it owns the per-message
/// key and the frame counter, which must never be reused.
pub struct Encryptor {
    keys: MessageKeys,
    message_id: [u8; 32],
    frame_length: usize,
    next_seq: u32,
    pending: Vec<u8>,
}

impl Encryptor {
    /// Starts a message and returns its full authenticated header, enforcing
    /// the default [`DecodeLimits`] so that the crate's own reader accepts
    /// what is written.
    ///
    /// # Errors
    /// See [`Self::with_limits`].
    #[cfg(test)]
    pub(crate) fn new(
        dek: &DataKey,
        message_id: [u8; 32],
        context: EncryptionContext,
        edks: Vec<EncryptedDataKey>,
        frame_length: u32,
    ) -> Result<(Bytes, Self), FormatError> {
        Self::with_limits(
            dek,
            message_id,
            context,
            edks,
            frame_length,
            &DecodeLimits::default(),
        )
    }

    /// Starts a message and returns its authenticated header, enforcing the
    /// given `limits` (tests and fuzzing use small frames).
    ///
    /// # Errors
    /// [`FormatError::FrameLengthOutOfRange`] when `frame_length` is zero or
    /// outside the limits, [`FormatError::TooManyEdks`] above `max_edks`,
    /// [`FormatError::HeaderTooLong`] when the header would exceed
    /// `max_header_len`, and [`FormatError::Malformed`] when a field does not
    /// fit its length prefix or there is no EDK.
    pub(crate) fn with_limits(
        dek: &DataKey,
        message_id: [u8; 32],
        context: EncryptionContext,
        edks: Vec<EncryptedDataKey>,
        frame_length: u32,
        limits: &DecodeLimits,
    ) -> Result<(Bytes, Self), FormatError> {
        limits.check_frame_length(frame_length)?;
        if edks.len() > usize::from(limits.max_edks) {
            return Err(FormatError::TooManyEdks(
                u16::try_from(edks.len()).unwrap_or(u16::MAX),
            ));
        }
        let keys = derive(dek, &message_id);
        let header = Header {
            message_id,
            context,
            edks,
            frame_length,
            commitment: keys.commitment,
            auth_tag: [0; 16],
        };
        let mut bytes = HeaderCodec::serialize_unauthenticated(&header)?;
        if bytes.len().saturating_add(TAG_LEN) > limits.max_header_len {
            return Err(FormatError::HeaderTooLong);
        }
        let tag = header_tag(&keys, &bytes);
        bytes.extend_from_slice(&tag);
        let frame_length = to_usize(frame_length);
        Ok((
            Bytes::from(bytes),
            Self {
                keys,
                message_id,
                frame_length,
                next_seq: 1,
                pending: Vec::with_capacity(frame_length.min(1 << 20)),
            },
        ))
    }

    /// Upper bound on the sealed body of a whole message of `plain` bytes
    /// (all `update` output plus `finish`), for sizing a buffer that holds it
    /// all. It does not bound a single `update`, which also emits what earlier
    /// calls left buffered.
    pub(crate) const fn sealed_len_bound(&self, plain: usize) -> usize {
        plain + (plain / self.frame_length + 2) * REGULAR_OVERHEAD
    }

    /// Exact number of bytes the next `update` of `plain` bytes emits: every
    /// frame it completes, and nothing for a partial one.
    pub(crate) const fn emitted_len(&self, plain: usize) -> usize {
        (self.pending.len() + plain) / self.frame_length * (self.frame_length + REGULAR_OVERHEAD)
    }

    fn take_seq(&mut self) -> u32 {
        let seq = self.next_seq;
        self.next_seq = seq
            .checked_add(1)
            .filter(|s| *s != FINAL_MARKER)
            .expect("message exceeds the maximum number of frames");
        seq
    }

    /// Appends plaintext. Every full regular frame is emitted to `out`
    /// immediately.
    ///
    /// # Panics
    /// If the message would exceed 2^32 - 2 frames.
    pub fn update(&mut self, mut data: &[u8], out: &mut BytesMut) {
        let f = self.frame_length;
        if !self.pending.is_empty() {
            let take = (f - self.pending.len()).min(data.len());
            self.pending.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.pending.len() < f {
                return;
            }
            let seq = self.take_seq();
            seal_frame(&self.keys, &self.message_id, seq, false, &self.pending, out);
            self.pending.clear();
        }
        while data.len() >= f {
            let seq = self.take_seq();
            seal_frame(&self.keys, &self.message_id, seq, false, &data[..f], out);
            data = &data[f..];
        }
        self.pending.extend_from_slice(data);
    }

    /// Emits the final frame, which may be empty.
    pub fn finish(self, out: &mut BytesMut) {
        seal_frame(
            &self.keys,
            &self.message_id,
            self.next_seq,
            true,
            &self.pending,
            out,
        );
    }
}

/// Builds a message whose last full frame is marked final with
/// `content_len == frame_length` (the other valid ESDK encoding of an exact
/// multiple). `data.len()` must be a positive multiple of `frame_length`.
#[cfg(test)]
pub(super) fn encrypt_full_final(
    dek: &DataKey,
    message_id: [u8; 32],
    context: EncryptionContext,
    edks: Vec<EncryptedDataKey>,
    frame_length: u32,
    data: &[u8],
    limits: &DecodeLimits,
) -> (Bytes, Vec<u8>) {
    let f = to_usize(frame_length);
    assert!(!data.is_empty() && data.len().is_multiple_of(f));
    let (header, _) = Encryptor::with_limits(dek, message_id, context, edks, frame_length, limits)
        .expect("valid message parameters");
    let keys = derive(dek, &message_id);
    let mut out = BytesMut::from(&header[..]);
    let n = data.len() / f;
    for (i, chunk) in data.chunks(f).enumerate() {
        let seq = u32::try_from(i + 1).expect("seq");
        seal_frame(&keys, &message_id, seq, i + 1 == n, chunk, &mut out);
    }
    (header, out.to_vec())
}

/// Result of [`Decryptor::feed`], [`Decryptor::unwrap_with`] and the
/// key-supplying calls. Dropping it can lose verified plaintext.
#[must_use]
pub enum Progress {
    /// More input is needed.
    NeedMore,
    /// The header is parsed; supply the data key before feeding more.
    NeedKey,
    /// Verified plaintext, one element per non-empty verified frame. Frames
    /// with empty content (such as an empty final frame) are omitted.
    Plaintext(Vec<Bytes>),
}

/// Redacting: shows the number and lengths of the frames, never plaintext.
impl std::fmt::Debug for Progress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeedMore => f.write_str("NeedMore"),
            Self::NeedKey => f.write_str("NeedKey"),
            Self::Plaintext(frames) => f
                .debug_struct("Plaintext")
                .field("frames", &frames.len())
                .field(
                    "lengths",
                    &frames.iter().map(Bytes::len).collect::<Vec<_>>(),
                )
                .finish(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Header,
    AwaitKey,
    Frames,
    Done,
    /// An earlier call returned an error; everything now fails.
    Failed,
}

/// Streaming decryptor: `Header -> (key) -> Frames -> Done`.
///
/// It never yields a byte that has not been authenticated. Buffering is
/// bounded: while the header is incomplete at most `max_header_len` plus the
/// last fed chunk is held; once the header is parsed the decryptor refuses
/// further non-empty input until the data key is supplied
/// ([`DecryptError::KeyRequired`]); afterwards it holds at most one frame
/// (header-declared length plus overhead) beyond the last fed chunk.
///
/// After any call returns an error the decryptor is failed: every later
/// `feed`, `set_data_key`, `unwrap_with` and `finish` returns an error, so a
/// misbehaving adapter cannot continue a poisoned stream. A wrong key is such
/// an error too; pass all candidate EDKs to one [`Self::unwrap_with`] call.
pub struct Decryptor {
    state: State,
    limits: DecodeLimits,
    expected_key: Option<String>,
    buf: BytesMut,
    header: Option<Header>,
    /// Header bytes covered by the auth tag.
    header_raw: Vec<u8>,
    keys: Option<MessageKeys>,
    next_seq: u32,
    /// Buffer length below which re-parsing the incomplete header cannot
    /// progress (avoids quadratic work on tiny chunks).
    header_min_len: usize,
    /// Whole-object mode: frames stay buffered until [`Self::finish_whole`].
    whole: bool,
    #[cfg(test)]
    header_parses: usize,
}

impl Decryptor {
    /// A decryptor enforcing `limits`. With `expected_key`, the `osc:key`
    /// context entry must equal it.
    #[must_use]
    pub fn new(limits: DecodeLimits, expected_key: Option<String>) -> Self {
        Self {
            state: State::Header,
            limits,
            expected_key,
            buf: BytesMut::new(),
            header: None,
            header_raw: Vec::new(),
            keys: None,
            next_seq: 1,
            header_min_len: 0,
            whole: false,
            #[cfg(test)]
            header_parses: 0,
        }
    }

    /// Switches to whole-object mode: after the header and key are accepted,
    /// frames are only buffered (all of the ciphertext, so memory is the
    /// object's size) and opened in place by [`Self::finish_whole`], which is
    /// the only place plaintext is released.
    pub(crate) const fn whole_object(mut self) -> Self {
        self.whole = true;
        self
    }

    /// Makes room for `additional` more buffered bytes.
    pub(crate) fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }

    /// The parsed header, once available.
    ///
    /// The header (context, `osc:key`, EDKs) is unauthenticated until a data
    /// key has been accepted by `set_data_key` or
    /// [`Self::unwrap_with`]: use it only to select a key, never as trusted
    /// data.
    #[must_use]
    pub const fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    /// Marks the decryptor failed when `r` is an error.
    const fn poison<T>(&mut self, r: Result<T, DecryptError>) -> Result<T, DecryptError> {
        if r.is_err() {
            self.state = State::Failed;
        }
        r
    }

    /// Ok only in the state that waits for a key.
    const fn expect_await_key(&self) -> Result<(), DecryptError> {
        match self.state {
            State::AwaitKey => Ok(()),
            State::Failed => Err(DecryptError::Failed),
            State::Header | State::Frames | State::Done => Err(DecryptError::InvalidState(
                "a data key is only accepted once the header is parsed and before frames",
            )),
        }
    }

    /// Supplies the data key: verifies the key commitment and the header tag,
    /// then decrypts any complete frames that were already buffered (so
    /// feed-all, key, finish works). The returned [`Progress`] carries that
    /// plaintext and must not be dropped.
    ///
    /// # Errors
    /// [`DecryptError::CommitmentMismatch`], [`DecryptError::HeaderAuth`],
    /// any frame error from the buffered input, or [`DecryptError::InvalidState`]
    /// when no key is awaited. The decryptor is failed afterwards.
    pub(crate) fn set_data_key(&mut self, dek: &DataKey) -> Result<Progress, DecryptError> {
        let r = self.set_data_key_inner(dek);
        self.poison(r)
    }

    fn set_data_key_inner(&mut self, dek: &DataKey) -> Result<Progress, DecryptError> {
        self.expect_await_key()?;
        let header = self.header.as_ref().ok_or(DecryptError::InvalidState(
            "header missing while awaiting key",
        ))?;
        let keys = derive(dek, &header.message_id);
        verify_header(
            &keys,
            &std::mem::take(&mut self.header_raw),
            &header.auth_tag,
        )?;
        self.keys = Some(keys);
        self.state = State::Frames;
        if self.whole {
            return Ok(Progress::NeedMore);
        }
        self.drain_frames()
    }

    /// Asks `provider` to unwrap the header's EDKs, then supplies the data key
    /// through `set_data_key`. The state is checked before the
    /// provider is called, so a misplaced call costs no provider round trip.
    ///
    /// # Errors
    /// [`DecryptError::Key`] when the provider cannot unwrap, otherwise the
    /// errors of `set_data_key`. The decryptor is failed afterwards.
    pub async fn unwrap_with(
        &mut self,
        provider: &dyn KeyProvider,
    ) -> Result<Progress, DecryptError> {
        let checked = self.expect_await_key();
        self.poison(checked)?;
        let Some(header) = self.header.as_ref() else {
            return self.poison(Err(DecryptError::InvalidState(
                "header missing while awaiting key",
            )));
        };
        let dek = match provider.unwrap(&header.edks, &header.context).await {
            Ok(dek) => dek,
            Err(e) => return self.poison(Err(e.into())),
        };
        self.set_data_key(&dek)
    }

    /// Feeds ciphertext.
    ///
    /// # Errors
    /// Any [`DecryptError`]; the decryptor is then failed. Non-empty input
    /// while a data key is awaited is [`DecryptError::KeyRequired`].
    pub fn feed(&mut self, data: Bytes) -> Result<Progress, DecryptError> {
        let r = self.feed_inner(data);
        self.poison(r)
    }

    /// Buffers `data`, taking over its allocation when nothing is buffered
    /// and it is not shared (the common first chunk), else copying.
    fn append(&mut self, data: Bytes) {
        if self.buf.is_empty() {
            self.buf = data
                .try_into_mut()
                .unwrap_or_else(|shared| BytesMut::from(&shared[..]));
        } else {
            self.buf.extend_from_slice(&data);
        }
    }

    fn feed_inner(&mut self, data: Bytes) -> Result<Progress, DecryptError> {
        match self.state {
            State::Header => {
                self.append(data);
                self.parse_header()
            }
            State::AwaitKey if data.is_empty() => Ok(Progress::NeedKey),
            State::AwaitKey => Err(DecryptError::KeyRequired),
            State::Frames => {
                self.append(data);
                if self.whole {
                    return Ok(Progress::NeedMore);
                }
                self.drain_frames()
            }
            State::Done if data.is_empty() => Ok(Progress::NeedMore),
            State::Done => Err(DecryptError::TrailingBytes),
            State::Failed => Err(DecryptError::Failed),
        }
    }

    /// Ends the stream.
    ///
    /// # Errors
    /// [`DecryptError::MissingFinalFrame`] if the final frame was not seen.
    /// End of input inside the header is a [`DecryptError::Format`] error:
    /// [`FormatError::NotOsc`] when fewer than 3 bytes were ever fed (an
    /// empty or tiny object is not encrypted), else [`FormatError::Truncated`].
    pub fn finish(self) -> Result<(), DecryptError> {
        match self.state {
            State::Done => Ok(()),
            State::Header if self.buf.len() < 3 => Err(FormatError::NotOsc.into()),
            State::Header => Err(FormatError::Truncated.into()),
            State::AwaitKey | State::Frames => Err(DecryptError::MissingFinalFrame),
            State::Failed => Err(DecryptError::Failed),
        }
    }

    fn parse_header(&mut self) -> Result<Progress, DecryptError> {
        if self.buf.len() < self.header_min_len {
            return Ok(Progress::NeedMore);
        }
        #[cfg(test)]
        {
            self.header_parses += 1;
        }
        let (parsed, min_len) = HeaderCodec::parse_hinted(&self.buf, &self.limits)?;
        match parsed {
            HeaderParse::NeedMore => {
                self.header_min_len = min_len;
                Ok(Progress::NeedMore)
            }
            HeaderParse::Done {
                header,
                header_len,
                authenticated_bytes,
            } => {
                if let Some(expected) = &self.expected_key {
                    let found = header.context.get(super::context::KEY_OBJECT);
                    if found != Some(expected.as_str()) {
                        return Err(DecryptError::BindingMismatch {
                            expected: expected.clone(),
                            found: found.unwrap_or_default().to_owned(),
                        });
                    }
                }
                self.header_raw = self.buf[authenticated_bytes].to_vec();
                self.buf.advance(header_len);
                self.header = Some(header);
                self.state = State::AwaitKey;
                Ok(Progress::NeedKey)
            }
        }
    }

    fn drain_frames(&mut self) -> Result<Progress, DecryptError> {
        let mut out = Vec::new();
        while self.state == State::Frames {
            match self.next_frame()? {
                Some(plain) => {
                    if !plain.is_empty() {
                        out.push(plain);
                    }
                }
                None => break,
            }
        }
        if self.state == State::Done && !self.buf.is_empty() {
            return Err(DecryptError::TrailingBytes);
        }
        if out.is_empty() {
            Ok(Progress::NeedMore)
        } else {
            Ok(Progress::Plaintext(out))
        }
    }

    /// Decrypts the next complete frame in the buffer, if any.
    fn next_frame(&mut self) -> Result<Option<Bytes>, DecryptError> {
        let header = self.header.as_ref().ok_or(DecryptError::InvalidState(
            "header missing while decoding frames",
        ))?;
        let keys = self.keys.as_ref().ok_or(DecryptError::InvalidState(
            "data key missing while decoding frames",
        ))?;
        let Some(o) = open_frame(keys, header, self.next_seq, &mut self.buf)? else {
            return Ok(None);
        };
        let mut frame = self.buf.split_to(o.total);
        frame.advance(o.prefix);
        frame.truncate(o.content_len);
        self.commit(&o)?;
        Ok(Some(frame.freeze()))
    }

    /// Records a verified frame: the next sequence number, or the end.
    fn commit(&mut self, o: &Opened) -> Result<(), DecryptError> {
        if o.is_final {
            self.state = State::Done;
        } else {
            self.next_seq = next_seq_after(o)?;
        }
        Ok(())
    }

    /// Ends a [`Self::whole_object`] decryptor: opens every buffered frame in
    /// place, compacting the plaintext to the front of the buffer, and
    /// returns it. Plaintext is released only once the final frame verified
    /// and nothing follows it.
    ///
    /// # Errors
    /// The errors of [`Self::feed`] and [`Self::finish`] for the same input.
    pub(crate) fn finish_whole(mut self) -> Result<Bytes, DecryptError> {
        if self.state != State::Frames {
            return Err(self.finish().err().unwrap_or(DecryptError::InvalidState(
                "a whole-object decryptor ended before its frames",
            )));
        }
        let header = self.header.as_ref().ok_or(DecryptError::InvalidState(
            "header missing while decoding frames",
        ))?;
        let keys = self.keys.as_ref().ok_or(DecryptError::InvalidState(
            "data key missing while decoding frames",
        ))?;
        let mut buf = std::mem::take(&mut self.buf);
        let (mut read, mut write) = (0, 0);
        let mut next_seq = self.next_seq;
        let mut done = false;
        while !done {
            let Some(o) = open_frame(keys, header, next_seq, &mut buf[read..])? else {
                break;
            };
            let content = read + o.prefix..read + o.prefix + o.content_len;
            buf.copy_within(content, write);
            write += o.content_len;
            read += o.total;
            if o.is_final {
                done = true;
            } else {
                next_seq = next_seq_after(&o)?;
            }
        }
        if !done {
            return Err(DecryptError::MissingFinalFrame);
        }
        if read != buf.len() {
            return Err(DecryptError::TrailingBytes);
        }
        buf.truncate(write);
        Ok(buf.freeze())
    }
}

/// A frame validated and opened in place by [`open_frame`].
struct Opened {
    seq: u32,
    /// Bytes before the content, within `total`.
    prefix: usize,
    content_len: usize,
    /// Prefix, content and tag.
    total: usize,
    is_final: bool,
}

/// The sequence number after the verified non-final frame `o`.
fn next_seq_after(o: &Opened) -> Result<u32, DecryptError> {
    o.seq.checked_add(1).ok_or(DecryptError::BadSequence {
        expected: o.seq,
        got: o.seq,
    })
}

/// Validates the frame at the start of `buf` (sequence, final marker and
/// length, IV, tag) and opens it in place; its plaintext is then
/// `buf[prefix..prefix + content_len]`. `None` when `buf` holds less than a
/// whole frame. The one copy of these checks, for the streaming and the
/// whole-object paths alike.
fn open_frame(
    keys: &MessageKeys,
    header: &Header,
    next_seq: u32,
    buf: &mut [u8],
) -> Result<Option<Opened>, DecryptError> {
    let Some(first) = buf.first_chunk::<4>().map(|b| u32::from_be_bytes(*b)) else {
        return Ok(None);
    };
    let frame_length = to_usize(header.frame_length);
    let is_final = first == FINAL_MARKER;

    let (seq, prefix, content_len) = if is_final {
        if buf.len() < FINAL_PREFIX {
            return Ok(None);
        }
        let seq = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let len = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
        (seq, FINAL_PREFIX, to_usize(len))
    } else {
        (first, 4 + IV_LEN, frame_length)
    };
    if seq != next_seq {
        return Err(DecryptError::BadSequence {
            expected: next_seq,
            got: seq,
        });
    }
    if is_final && content_len > frame_length {
        return Err(DecryptError::FinalFrameTooLong);
    }
    let total = prefix
        .checked_add(content_len)
        .and_then(|n| n.checked_add(TAG_LEN))
        .ok_or(FormatError::FrameLengthOutOfRange(header.frame_length))?;
    if buf.len() < total {
        return Ok(None);
    }

    let iv_at = if is_final { 8 } else { 4 };
    let mut iv = [0u8; IV_LEN];
    iv.copy_from_slice(&buf[iv_at..iv_at + IV_LEN]);
    if iv != frame_iv(seq) {
        return Err(DecryptError::BadIv { seq });
    }
    let (aad, aad_len) = frame_aad(&header.message_id, is_final, seq, content_len as u64);
    keys.derived
        .open_in_place(
            Nonce::assume_unique_for_key(iv),
            Aad::from(&aad[..aad_len]),
            &mut buf[prefix..total],
        )
        .map_err(|_| DecryptError::FrameAuth { seq })?;
    Ok(Some(Opened {
        seq,
        prefix,
        content_len,
        total,
        is_final,
    }))
}

/// Maps stored sizes to plaintext sizes for a message with a known header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLayout {
    /// Header length in bytes, including the auth tag.
    pub header_len: u64,
    /// Frame length from the header.
    pub frame_length: u32,
}

impl FrameLayout {
    /// Plaintext size implied by the stored object size. The value is
    /// unauthenticated until the final frame has been verified. `None` if the
    /// size is not a valid framed message.
    #[must_use]
    pub fn plaintext_size(&self, stored_size: u64) -> Option<u64> {
        let body = stored_size.checked_sub(self.header_len)?;
        let rest = body.checked_sub(FINAL_OVERHEAD)?;
        let f = u64::from(self.frame_length);
        let stride = f + REGULAR_OVERHEAD as u64;
        let (n, r) = (rest / stride, rest % stride);
        if r > f {
            return None;
        }
        Some(n * f + r)
    }
}

/// Feeds `msg` to `d` in `piece()`-sized pieces, calling `key` once the header
/// is parsed. Returns every plaintext byte released before the outcome, and
/// the outcome. Test and fuzz harnesses share this loop.
#[cfg(any(test, feature = "fuzzing"))]
pub fn drive(
    mut d: Decryptor,
    msg: &[u8],
    mut piece: impl FnMut() -> usize,
    mut key: impl FnMut(&mut Decryptor) -> Result<Progress, DecryptError>,
) -> (Vec<u8>, Result<(), DecryptError>) {
    let mut out = Vec::new();
    let mut rest = msg;
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(piece().max(1).min(rest.len()));
        rest = tail;
        let step = match d.feed(Bytes::copy_from_slice(chunk)) {
            Ok(Progress::NeedKey) => key(&mut d),
            other => other,
        };
        match step {
            Ok(Progress::Plaintext(frames)) => {
                for f in &frames {
                    out.extend_from_slice(f);
                }
            }
            Ok(_) => {}
            Err(e) => return (out, Err(e)),
        }
    }
    (out, d.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::context::CallerContext;
    use crate::encryption::fixtures::{ESDK, EsdkVector, dek, edk, encrypt, esdk_provider, plain};
    use crate::hex;

    const F: u32 = 4096;
    const FU: usize = F as usize;

    /// Appends the plaintext of `p` to `out`.
    fn take_plaintext(p: Progress, out: &mut Vec<u8>) {
        if let Progress::Plaintext(frames) = p {
            for f in &frames {
                out.extend_from_slice(f);
            }
        }
    }

    /// Feeds `msg` in `piece`-sized chunks (0 = whole) and returns the plaintext.
    fn decrypt(msg: &[u8], piece: usize, key: Option<&str>) -> Result<Vec<u8>, DecryptError> {
        let d = Decryptor::new(DecodeLimits::default(), key.map(str::to_owned));
        let piece = if piece == 0 { usize::MAX } else { piece };
        let (out, r) = drive(d, msg, || piece, |d| d.set_data_key(&dek(0x42)));
        r.map(|()| out)
    }

    fn sizes() -> Vec<usize> {
        vec![0, 1, FU - 1, FU, FU + 1, 3 * FU, 3 * FU + 7]
    }

    #[test]
    fn roundtrip_whole_and_byte_at_a_time() {
        for n in sizes() {
            let data = plain(n, 0);
            let (msg, hl) = encrypt(&data, F);
            assert_eq!(decrypt(&msg, 0, None).expect("whole"), data, "{n}");
            assert_eq!(decrypt(&msg, 1, None).expect("bytes"), data, "{n}");
            let layout = FrameLayout {
                header_len: hl as u64,
                frame_length: F,
            };
            assert_eq!(
                layout.plaintext_size(msg.len() as u64),
                Some(n as u64),
                "{n}"
            );
        }
    }

    #[test]
    fn incomplete_header_is_not_reparsed_on_every_byte() {
        let (msg, hl) = encrypt(&plain(10, 0), F);
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        for b in &msg[..hl] {
            let _ = d.feed(Bytes::copy_from_slice(&[*b])).expect("feed");
        }
        assert!(d.state == State::AwaitKey);
        // One parse per header field boundary at most, not one per byte.
        assert!(
            d.header_parses < hl / 4,
            "{} parses for {hl} bytes",
            d.header_parses
        );
    }

    #[test]
    fn encryptor_output_is_independent_of_update_chunking() {
        let data = plain(3 * FU + 123, 0);
        let (whole, _) = encrypt(&data, F);
        for step in [1usize, 7, 1000, FU, FU + 1, 10 * FU] {
            let ctx = EncryptionContext::new_reserved(Some("obj/key"));
            let (header, mut enc) =
                Encryptor::new(&dek(0x42), [9; 32], ctx, vec![edk()], F).expect("encryptor");
            let mut out = BytesMut::from(&header[..]);
            for c in data.chunks(step) {
                enc.update(c, &mut out);
            }
            enc.finish(&mut out);
            assert_eq!(out.to_vec(), whole, "step {step}");
        }
    }

    #[test]
    fn feed_adopts_an_unshared_chunk_and_copies_a_shared_one() {
        let data = plain(3 * FU + 5, 0);
        let (msg, _) = encrypt(&data, F);
        let leaked: &'static [u8] = Box::leak(msg.clone().into_boxed_slice());
        let shared = Bytes::copy_from_slice(&msg);
        let keep = shared.clone();
        // Unshared, shared and static inputs: each decrypts in place or on a
        // copy, and a chunk the caller still holds is never modified.
        let inputs = [
            Bytes::copy_from_slice(&msg),
            shared,
            Bytes::from_static(leaked),
        ];
        for chunk in inputs {
            let mut d = Decryptor::new(DecodeLimits::default(), None);
            let mut out = Vec::new();
            let step = d.feed(chunk).expect("feed");
            assert!(matches!(step, Progress::NeedKey));
            take_plaintext(d.set_data_key(&dek(0x42)).expect("key"), &mut out);
            d.finish().expect("finish");
            assert_eq!(out, data);
        }
        assert_eq!(
            &keep[..],
            &msg[..],
            "the shared chunk was decrypted in place"
        );
        assert_eq!(leaked, &msg[..]);
    }

    #[test]
    fn emitted_len_is_exactly_what_update_writes() {
        let data = plain(5 * FU + 99, 0);
        for step in [1usize, 7, 1000, FU - 1, FU, FU + 1, 10 * FU] {
            let ctx = EncryptionContext::new_reserved(Some("obj/key"));
            let (_, mut enc) =
                Encryptor::new(&dek(0x42), [9; 32], ctx, vec![edk()], F).expect("encryptor");
            for c in data.chunks(step) {
                let want = enc.emitted_len(c.len());
                let mut out = BytesMut::with_capacity(want);
                let cap = out.capacity();
                enc.update(c, &mut out);
                assert_eq!(out.len(), want, "step {step}");
                // Exactly sized: sealing never had to grow the buffer.
                assert_eq!(out.capacity(), cap, "step {step}");
            }
        }
    }

    #[test]
    fn layout_rejects_impossible_sizes() {
        let l = FrameLayout {
            header_len: 100,
            frame_length: F,
        };
        assert_eq!(l.plaintext_size(99), None);
        assert_eq!(l.plaintext_size(100 + 39), None);
        assert_eq!(l.plaintext_size(100 + 40), Some(0));
        // r = F + 1 is not a valid remainder.
        assert_eq!(l.plaintext_size(100 + 40 + u64::from(F) + 1), None);
        assert_eq!(
            l.plaintext_size(100 + 40 + u64::from(F)),
            Some(u64::from(F))
        );
    }

    #[test]
    fn final_frame_of_full_length_is_accepted() {
        let data = plain(3 * FU, 0);
        let keys = derive(&dek(0x42), &[9; 32]);
        let ctx = EncryptionContext::new_reserved(Some("obj/key"));
        let (header, _) =
            Encryptor::new(&dek(0x42), [9; 32], ctx, vec![edk()], F).expect("encryptor");
        let mut out = BytesMut::from(&header[..]);
        seal_frame(&keys, &[9; 32], 1, false, &data[..FU], &mut out);
        seal_frame(&keys, &[9; 32], 2, false, &data[FU..2 * FU], &mut out);
        seal_frame(&keys, &[9; 32], 3, true, &data[2 * FU..], &mut out);
        let msg = out.to_vec();
        assert_eq!(decrypt(&msg, 0, None).expect("decrypt"), data);
        assert_eq!(decrypt(&msg, 13, None).expect("decrypt"), data);
        let layout = FrameLayout {
            header_len: header.len() as u64,
            frame_length: F,
        };
        assert_eq!(
            layout.plaintext_size(msg.len() as u64),
            Some(data.len() as u64)
        );
    }

    #[test]
    fn missing_final_frame() {
        let (mut msg, _) = encrypt(&plain(FU, 0), F);
        msg.truncate(msg.len() - 40); // drop the empty final frame
        assert!(matches!(
            decrypt(&msg, 0, None),
            Err(DecryptError::MissingFinalFrame)
        ));
        // A frame cut in the middle is also a missing final frame.
        let (mut msg, _) = encrypt(&plain(10, 0), F);
        msg.pop();
        assert!(matches!(
            decrypt(&msg, 0, None),
            Err(DecryptError::MissingFinalFrame)
        ));
    }

    #[test]
    fn truncated_header_is_a_format_error() {
        let (msg, hl) = encrypt(&plain(10, 0), F);
        assert!(matches!(
            decrypt(&msg[..hl - 1], 0, None),
            Err(DecryptError::Format(_))
        ));
    }

    #[test]
    fn trailing_bytes() {
        let (mut msg, _) = encrypt(&plain(10, 0), F);
        msg.push(0);
        assert!(matches!(
            decrypt(&msg, 0, None),
            Err(DecryptError::TrailingBytes)
        ));
        // Arriving in a later feed, one byte at a time.
        assert!(matches!(
            decrypt(&msg, 1, None),
            Err(DecryptError::TrailingBytes)
        ));
    }

    #[test]
    fn swapped_frames_are_a_bad_sequence() {
        let (mut msg, hl) = encrypt(&plain(3 * FU, 0), F);
        let stride = FU + 32;
        let a = msg[hl..hl + stride].to_vec();
        let b = msg[hl + stride..hl + 2 * stride].to_vec();
        msg[hl..hl + stride].copy_from_slice(&b);
        msg[hl + stride..hl + 2 * stride].copy_from_slice(&a);
        assert!(matches!(
            decrypt(&msg, 0, None),
            Err(DecryptError::BadSequence {
                expected: 1,
                got: 2
            })
        ));
    }

    #[test]
    fn binding_mismatch() {
        let (msg, _) = encrypt(&plain(10, 0), F);
        assert_eq!(decrypt(&msg, 0, Some("obj/key")).expect("bound").len(), 10);
        match decrypt(&msg, 0, Some("other")) {
            Err(DecryptError::BindingMismatch { expected, found }) => {
                assert_eq!(expected, "other");
                assert_eq!(found, "obj/key");
            }
            other => panic!("unexpected {other:?}"),
        }
        // A message without osc:key does not satisfy a binding.
        let ctx = EncryptionContext::new_reserved(None);
        let (header, enc) =
            Encryptor::new(&dek(0x42), [9; 32], ctx, vec![edk()], F).expect("encryptor");
        let mut out = BytesMut::from(&header[..]);
        enc.finish(&mut out);
        assert!(matches!(
            decrypt(&out, 0, Some("obj/key")),
            Err(DecryptError::BindingMismatch { .. })
        ));
        assert!(decrypt(&out, 0, None).is_ok());
    }

    #[test]
    fn tampering_is_detected() {
        let data = plain(2 * FU + 5, 0);
        let (msg, hl) = encrypt(&data, F);
        let stride = FU + 32;
        let flip = |at: usize| {
            let mut m = msg.clone();
            m[at] ^= 1;
            m
        };
        // ciphertext and tag of the first frame
        assert!(matches!(
            decrypt(&flip(hl + 16 + 3), 0, None),
            Err(DecryptError::FrameAuth { seq: 1 })
        ));
        assert!(matches!(
            decrypt(&flip(hl + stride - 1), 0, None),
            Err(DecryptError::FrameAuth { seq: 1 })
        ));
        // IV of the second frame
        assert!(matches!(
            decrypt(&flip(hl + stride + 4 + 11), 0, None),
            Err(DecryptError::BadIv { seq: 2 })
        ));
        // final frame: content and length field
        let fin = hl + 2 * stride;
        assert!(matches!(
            decrypt(&flip(fin + 24 + 1), 0, None),
            Err(DecryptError::FrameAuth { seq: 3 })
        ));
        assert!(matches!(
            decrypt(&flip(fin + 23), 0, None),
            Err(DecryptError::FrameAuth { .. } | DecryptError::MissingFinalFrame)
        ));
        // final length above the frame length is rejected from the prefix alone
        let mut m = msg[..fin + FINAL_PREFIX].to_vec();
        m[fin + 20..fin + 24].copy_from_slice(&(F + 1).to_be_bytes());
        assert!(matches!(
            decrypt(&m, 0, None),
            Err(DecryptError::FinalFrameTooLong)
        ));
        // header bytes
        for at in 0..hl {
            let r = decrypt(&flip(at), 0, None);
            assert!(r.is_err(), "header flip at {at} accepted");
        }
    }

    #[test]
    fn wrong_key_fails_the_decryptor() {
        let (msg, _) = encrypt(&plain(10, 0), F);
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        assert!(matches!(
            d.feed(Bytes::from(msg)).expect("feed"),
            Progress::NeedKey
        ));
        assert!(matches!(
            d.set_data_key(&DataKey::from_slice(&[1; 32]).expect("key")),
            Err(DecryptError::CommitmentMismatch)
        ));
        assert_failed(d);
    }

    /// Every operation on a failed decryptor is an error.
    #[track_caller]
    fn assert_failed(mut d: Decryptor) {
        assert!(matches!(d.feed(Bytes::new()), Err(DecryptError::Failed)));
        assert!(matches!(
            d.feed(Bytes::from_static(b"x")),
            Err(DecryptError::Failed)
        ));
        assert!(matches!(
            d.set_data_key(&dek(0x42)),
            Err(DecryptError::Failed)
        ));
        let provider = esdk_provider();
        assert!(matches!(
            futures::executor::block_on(d.unwrap_with(&provider)),
            Err(DecryptError::Failed)
        ));
        assert!(matches!(d.finish(), Err(DecryptError::Failed)));
    }

    #[test]
    fn any_error_fails_the_decryptor() {
        // Trailing byte after the final frame in the same feed: the error must
        // not be followed by a successful finish.
        let (mut msg, _) = encrypt(&plain(10, 0), F);
        msg.push(0);
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        let first = d.feed(Bytes::from(msg)).expect("header");
        assert!(matches!(first, Progress::NeedKey));
        assert!(matches!(
            d.set_data_key(&dek(0x42)),
            Err(DecryptError::TrailingBytes)
        ));
        assert_failed(d);

        // A bad frame, then a retry that must not release anything.
        let (mut msg, hl) = encrypt(&plain(2 * FU, 0), F);
        msg[hl + 20] ^= 1;
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        let _ = d.feed(Bytes::from(msg)).expect("header");
        assert!(matches!(
            d.set_data_key(&dek(0x42)),
            Err(DecryptError::FrameAuth { seq: 1 })
        ));
        assert_failed(d);

        // A format error while parsing the header.
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        assert!(matches!(
            d.feed(Bytes::from_static(b"\x55abc")),
            Err(DecryptError::Format(FormatError::NotOsc))
        ));
        assert_failed(d);

        // Binding mismatch.
        let (msg, _) = encrypt(&plain(10, 0), F);
        let mut d = Decryptor::new(DecodeLimits::default(), Some("other".into()));
        assert!(matches!(
            d.feed(Bytes::from(msg)),
            Err(DecryptError::BindingMismatch { .. })
        ));
        assert_failed(d);
    }

    #[test]
    fn set_data_key_without_header_fails() {
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        assert!(matches!(
            d.set_data_key(&dek(0x42)),
            Err(DecryptError::InvalidState(_))
        ));
        assert_failed(d);
    }

    #[test]
    fn input_while_a_key_is_awaited_is_rejected() {
        let (msg, hl) = encrypt(&plain(10, 0), F);
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        assert!(matches!(
            d.feed(Bytes::copy_from_slice(&msg[..hl])).expect("header"),
            Progress::NeedKey
        ));
        // Empty feeds are fine, more data is not: buffering stays bounded.
        assert!(matches!(
            d.feed(Bytes::new()).expect("empty"),
            Progress::NeedKey
        ));
        assert!(matches!(
            d.feed(Bytes::copy_from_slice(&msg[hl..])),
            Err(DecryptError::KeyRequired)
        ));
        assert_failed(d);
    }

    #[test]
    fn feed_all_then_key_then_finish() {
        for n in [0, 10, FU, 3 * FU + 7] {
            let data = plain(n, 0);
            let (msg, _) = encrypt(&data, F);
            let mut d = Decryptor::new(DecodeLimits::default(), None);
            assert!(matches!(
                d.feed(Bytes::from(msg)).expect("feed"),
                Progress::NeedKey
            ));
            let mut out = Vec::new();
            take_plaintext(d.set_data_key(&dek(0x42)).expect("key"), &mut out);
            d.finish().expect("complete message after set_data_key");
            assert_eq!(out, data, "{n}");
        }
    }

    #[test]
    fn short_objects_are_not_encrypted_objects() {
        for n in 0..3 {
            let mut d = Decryptor::new(DecodeLimits::default(), None);
            if n > 0 {
                // Valid-looking prefix bytes, so only the length decides.
                assert!(matches!(
                    d.feed(Bytes::copy_from_slice(&[2, 4][..n])),
                    Ok(Progress::NeedMore)
                ));
            }
            assert!(
                matches!(d.finish(), Err(DecryptError::Format(FormatError::NotOsc))),
                "{n} bytes"
            );
        }
        let (msg, _) = encrypt(&plain(10, 0), F);
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        let _ = d.feed(Bytes::copy_from_slice(&msg[..3])).expect("prefix");
        assert!(matches!(
            d.finish(),
            Err(DecryptError::Format(FormatError::Truncated))
        ));
    }

    #[test]
    fn progress_debug_hides_plaintext() {
        let p = Progress::Plaintext(vec![
            Bytes::from_static(b"secret-plaintext"),
            Bytes::from_static(b"more"),
        ]);
        let shown = format!("{p:?}");
        assert!(!shown.contains("secret"), "{shown}");
        assert!(!shown.contains("115"), "{shown}"); // 's' as a decimal byte
        assert!(shown.contains("16") && shown.contains('4'), "{shown}");
        assert_eq!(format!("{:?}", Progress::NeedKey), "NeedKey");
    }

    #[test]
    fn progress_debug_names_the_other_states() {
        assert_eq!(format!("{:?}", Progress::NeedMore), "NeedMore");
    }

    #[test]
    fn encryptor_validates_its_parameters() {
        let new = |f: u32, edks: Vec<EncryptedDataKey>, ctx: EncryptionContext| {
            Encryptor::new(&dek(0x42), [9; 32], ctx, edks, f).map(|_| ())
        };
        let ctx = || EncryptionContext::new_reserved(Some("k"));
        for bad in [0, 1, 4095, 4 * 1024 * 1024 + 1, u32::MAX] {
            assert_eq!(
                new(bad, vec![edk()], ctx()),
                Err(FormatError::FrameLengthOutOfRange(bad))
            );
        }
        for ok in [4096, 4 * 1024 * 1024] {
            assert_eq!(new(ok, vec![edk()], ctx()), Ok(()));
        }
        assert!(matches!(
            new(F, vec![edk(); 9], ctx()),
            Err(FormatError::TooManyEdks(9))
        ));
        assert!(matches!(
            new(F, vec![], ctx()),
            Err(FormatError::Malformed(_))
        ));
        let mut huge = edk();
        huge.provider_info = vec![0; 70_000];
        assert!(matches!(
            new(F, vec![huge], ctx()),
            Err(FormatError::Malformed(_))
        ));
        // Fits the 16-bit fields but not the reader's header limit.
        let mut big = edk();
        big.provider_info = vec![0; 20_000];
        assert_eq!(new(F, vec![big], ctx()), Err(FormatError::HeaderTooLong));
        // Not a panic for a key that skipped `with_caller`.
        assert!(matches!(
            new(
                F,
                vec![edk()],
                EncryptionContext::new_reserved(Some(&"k".repeat(70_000)))
            ),
            Err(FormatError::Malformed(_))
        ));
    }

    // --- ESDK vectors ---

    fn decrypt_vector(v: &EsdkVector, pieces: &mut dyn FnMut() -> usize) -> Vec<u8> {
        let provider = esdk_provider();
        let d = Decryptor::new(DecodeLimits::default(), None);
        let (out, r) = drive(d, &v.bytes, pieces, |d| {
            futures::executor::block_on(d.unwrap_with(&provider))
        });
        r.expect(&v.file);
        out
    }

    fn sha256(data: &[u8]) -> Vec<u8> {
        aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data)
            .as_ref()
            .to_vec()
    }

    #[test]
    fn esdk_vectors_decrypt_whole_and_in_random_pieces() {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut random = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            usize::try_from(state % 97).expect("small") + 1
        };
        for v in &ESDK.vectors {
            let whole = decrypt_vector(v, &mut || usize::MAX / 2);
            assert_eq!(whole.len() as u64, v.size, "{}", v.file);
            assert_eq!(sha256(&whole), v.sha256, "{}", v.file);
            let pieces = decrypt_vector(v, &mut random);
            assert_eq!(whole, pieces, "{}", v.file);
        }
    }

    #[test]
    fn esdk_vectors_layout_matches_manifest() {
        for v in &ESDK.vectors {
            let layout = FrameLayout {
                header_len: v.header_len,
                frame_length: v.frame_length,
            };
            assert_eq!(
                layout.plaintext_size(v.bytes.len() as u64),
                Some(v.size),
                "{}",
                v.file
            );
        }
    }

    #[test]
    fn encryptor_reproduces_esdk_vectors_byte_for_byte() {
        // Frame IVs, the header tag and the commitment are deterministic given
        // the DEK and message ID, so the whole message must be identical.
        let provider = esdk_provider();
        for v in &ESDK.vectors {
            let HeaderParse::Done { header, .. } =
                HeaderCodec::parse(&v.bytes, &DecodeLimits::default()).expect("parse")
            else {
                panic!("{}: header", v.file);
            };
            let dek = futures::executor::block_on(provider.unwrap(&header.edks, &header.context))
                .expect(&v.file);
            let (hdr, mut enc) = Encryptor::new(
                &dek,
                header.message_id,
                header.context.clone(),
                header.edks.clone(),
                header.frame_length,
            )
            .expect(&v.file);
            let mut out = BytesMut::from(&hdr[..]);
            let size = usize::try_from(v.size).expect("size");
            enc.update(&plain(size, 0), &mut out);
            enc.finish(&mut out);
            assert_eq!(out.to_vec(), v.bytes, "{}", v.file);
        }
    }

    #[test]
    fn unwrap_with_wrong_kek_reports_key_error_and_fails() {
        use crate::encryption::keys::{KeyError, Keyring};
        let v = &ESDK.vectors[0];
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        assert!(matches!(
            d.feed(Bytes::copy_from_slice(&v.bytes)),
            Ok(Progress::NeedKey)
        ));
        let wrong = Keyring::from_material("kek-test-0001", &[("kek-test-0001", &[9; 32])]);
        assert!(matches!(
            futures::executor::block_on(d.unwrap_with(&wrong)),
            Err(DecryptError::Key(KeyError::UnwrapFailed { .. }))
        ));
        assert_failed(d);

        let mut d = Decryptor::new(DecodeLimits::default(), None);
        let _ = d.feed(Bytes::copy_from_slice(&v.bytes)).expect("feed");
        let other_name = Keyring::from_material("other", &[("other", &[9; 32])]);
        assert!(matches!(
            futures::executor::block_on(d.unwrap_with(&other_name)),
            Err(DecryptError::Key(KeyError::UnknownKey { .. }))
        ));
        assert_failed(d);
    }

    /// Counts `unwrap` calls of the vector provider.
    #[derive(Debug)]
    struct Counting {
        inner: crate::encryption::keys::Keyring,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl KeyProvider for Counting {
        fn namespace(&self) -> &str {
            self.inner.namespace()
        }
        fn active_key_name(&self) -> String {
            self.inner.active_key_name()
        }
        async fn wrap(
            &self,
            dek: &DataKey,
            ctx: &EncryptionContext,
        ) -> Result<Vec<EncryptedDataKey>, crate::encryption::keys::KeyError> {
            self.inner.wrap(dek, ctx).await
        }
        async fn unwrap(
            &self,
            edks: &[EncryptedDataKey],
            ctx: &EncryptionContext,
        ) -> Result<DataKey, crate::encryption::keys::KeyError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.unwrap(edks, ctx).await
        }
    }

    #[test]
    fn unwrap_with_checks_the_state_before_calling_the_provider() {
        use std::sync::atomic::Ordering;
        let v = &ESDK.vectors[0];
        let provider = Counting {
            inner: esdk_provider(),
            calls: 0.into(),
        };
        // No header yet.
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        assert!(matches!(
            futures::executor::block_on(d.unwrap_with(&provider)),
            Err(DecryptError::InvalidState(_))
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

        // Already keyed.
        let mut d = Decryptor::new(DecodeLimits::default(), None);
        let _ = d.feed(Bytes::copy_from_slice(&v.bytes)).expect("feed");
        take_plaintext(
            futures::executor::block_on(d.unwrap_with(&provider)).expect("first"),
            &mut Vec::new(),
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            futures::executor::block_on(d.unwrap_with(&provider)),
            Err(DecryptError::InvalidState(_))
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    // --- interop with the Python oracle ---

    #[test]
    #[ignore = "needs uv and the oracle dependency group (uv run --group oracle)"]
    fn rust_encrypted_messages_decrypt_with_the_python_oracle() {
        use crate::encryption::keys::Keyring;
        let kek: [u8; 32] = std::array::from_fn(|i| u8::try_from(i).expect("small"));
        let key_name = "kek-test-0001";
        let provider = Keyring::from_material(key_name, &[(key_name, &kek)]);
        let mut caller = CallerContext::new();
        caller.insert("tenant".into(), "acme".into());
        let tmp = tempfile::tempdir().expect("tmp");
        let script = concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/esdk_oracle.py");

        for (n, f) in [
            (0usize, 4096u32),
            (1, 4096),
            (4096, 4096),
            (200_000, 4096),
            (70_000, 65536),
        ] {
            let ctx = EncryptionContext::new_reserved(Some("dir/file.bin"))
                .with_caller(&caller)
                .expect("ctx");
            // The production wrap path, not a test-local re-implementation.
            // A fresh DEK and message ID per message: one header per (DEK, ID).
            let (dek, message_id) =
                crate::encryption::suite::fresh_message_material().expect("fresh material");
            let edks = futures::executor::block_on(provider.wrap(&dek, &ctx)).expect("wrap");
            let (header, mut enc) =
                Encryptor::new(&dek, message_id, ctx, edks, f).expect("encryptor");
            let mut out = BytesMut::from(&header[..]);
            let data = plain(n, 0);
            enc.update(&data, &mut out);
            enc.finish(&mut out);

            let input = tmp.path().join("in.bin");
            let output = tmp.path().join("out.bin");
            std::fs::write(&input, &out).expect("write");
            let status = std::process::Command::new("uv")
                .args(["run", "--group", "oracle", "python", script, "decrypt"])
                .args(["--key-hex", &hex::encode(&kek), "--key-name", key_name])
                .args(["--context", "tenant=acme", "--context", "osc:fmt=1"])
                .args(["--context", "osc:key=dir/file.bin"])
                .arg("--in")
                .arg(&input)
                .arg("--out")
                .arg(&output)
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                // Do not rebuild the project into the environment.
                .env("UV_NO_SYNC", "1")
                .status()
                .expect("run uv");
            assert!(status.success(), "oracle rejected n={n} f={f}");
            assert_eq!(std::fs::read(&output).expect("read"), data, "n={n} f={f}");
        }
    }
}
