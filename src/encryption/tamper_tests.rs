//! Tamper suite (spec §10.2) and property-based round-trips.
#![expect(clippy::cast_possible_truncation, reason = "test sizes are small")]

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;

use super::context::{CallerContext, EncryptionContext};
use super::fixtures::{dek, edk, encrypt, encrypt_with, plain, small_limits};
use super::frame::{
    Decryptor, Encryptor, FrameLayout, Progress, drive, drive_whole, encrypt_full_final,
};
use super::header::{DecodeLimits, FormatError};
use super::suite::DecryptError;

const TAG_AND_PREFIX: usize = 32; // 4 + 12 IV + 16 tag for a regular frame

/// Decrypts `msg` in `piece`-byte feeds (0 = whole). Returns every plaintext
/// byte yielded before the outcome, and the outcome.
fn run(
    msg: &[u8],
    piece: usize,
    key: u8,
    limits: DecodeLimits,
) -> (Vec<u8>, Result<(), DecryptError>) {
    let piece = if piece == 0 { usize::MAX } else { piece };
    drive(
        Decryptor::new(limits, None),
        msg,
        || piece,
        |d| d.set_data_key(&dek(key)),
    )
}

/// Asserts the message is rejected and that nothing but a true prefix of the
/// original plaintext was ever yielded.
#[track_caller]
fn assert_rejected(msg: &[u8], original: &[u8], limits: &DecodeLimits, what: &str) {
    for piece in [0, 1] {
        let (out, r) = run(msg, piece, 0x42, limits.clone());
        assert!(r.is_err(), "{what}: accepted (piece {piece})");
        assert!(
            original.starts_with(&out),
            "{what}: yielded unverified bytes (piece {piece})"
        );
    }
    for piece in [0, 1, 5] {
        assert_same_outcome(msg, piece, 0x42, limits, what);
    }
}

/// Offsets of the tail fields of a header of length `hl`.
const fn content_type_at(hl: usize) -> usize {
    hl - 16 - 32 - 4 - 1
}

#[test]
fn every_bit_flip_of_a_three_frame_message_is_rejected() {
    let f = 64u32;
    let data = plain(3 * 64 - 5, 0);
    let (msg, _) = encrypt(&data, f);
    let limits = small_limits();
    assert!(run(&msg, 0, 0x42, limits.clone()).1.is_ok());
    // Whole-message feeds discard a feed's output on error, so the prefix
    // property is only meaningful when streaming one byte at a time.
    for piece in [0, 1] {
        for at in 0..msg.len() {
            for bit in 0..8 {
                let mut m = msg.clone();
                m[at] ^= 1 << bit;
                let (out, r) = run(&m, piece, 0x42, limits.clone());
                assert!(
                    r.is_err(),
                    "flip byte {at} bit {bit} accepted (piece {piece})"
                );
                assert!(
                    data.starts_with(&out),
                    "flip {at}/{bit} yielded bad bytes (piece {piece})"
                );
            }
        }
    }
}

#[test]
fn truncation_at_every_offset_of_a_2kib_message_is_rejected() {
    let data = plain(2048, 0);
    let (msg, _) = encrypt(&data, 256);
    let limits = small_limits();
    assert!(run(&msg, 0, 0x42, limits.clone()).1.is_ok());
    for cut in 0..msg.len() {
        assert_rejected(&msg[..cut], &data, &limits, &format!("cut at {cut}"));
    }
}

#[test]
fn truncation_at_every_frame_boundary_of_a_1mib_message_is_rejected() {
    let f = 65536u32;
    let data = plain(1 << 20, 0);
    let (msg, hl) = encrypt(&data, f);
    let stride = f as usize + TAG_AND_PREFIX;
    let limits = DecodeLimits::default();
    let mut cut = hl;
    while cut < msg.len() {
        for at in [cut - 1, cut, cut + 1] {
            if at < msg.len() {
                let (out, r) = run(&msg[..at], 0, 0x42, limits.clone());
                assert!(r.is_err(), "cut at {at} accepted");
                assert!(data.starts_with(&out));
            }
        }
        cut += stride;
    }
}

#[test]
fn appended_byte_is_rejected() {
    let data = plain(3 * 64 + 1, 0);
    let (mut msg, _) = encrypt(&data, 64);
    msg.push(0);
    let (out, r) = run(&msg, 0, 0x42, small_limits());
    assert!(matches!(r, Err(DecryptError::TrailingBytes)));
    assert!(data.starts_with(&out), "only genuine bytes may be yielded");
    let (_, r) = run(&msg, 1, 0x42, small_limits());
    assert!(matches!(r, Err(DecryptError::TrailingBytes)));
}

#[test]
fn frame_duplication_drop_reorder_and_splice_are_rejected() {
    let f = 64usize;
    let data = plain(4 * f + 10, 0);
    let (msg, hl) = encrypt(&data, f as u32);
    let stride = f + TAG_AND_PREFIX;
    let body = &msg[hl..];
    let frame = |i: usize| body[i * stride..(i + 1) * stride].to_vec();
    let fin = body[4 * stride..].to_vec();
    let build = |parts: &[Vec<u8>]| {
        let mut m = msg[..hl].to_vec();
        for p in parts {
            m.extend_from_slice(p);
        }
        m
    };
    let l = small_limits();
    // duplicate frame 1
    let m = build(&[
        frame(0),
        frame(0),
        frame(1),
        frame(2),
        frame(3),
        fin.clone(),
    ]);
    assert_rejected(&m, &data, &l, "duplicate");
    // drop frame 2
    let m = build(&[frame(0), frame(2), frame(3), fin.clone()]);
    assert_rejected(&m, &data, &l, "drop");
    // drop the final frame
    let m = build(&[frame(0), frame(1), frame(2), frame(3)]);
    assert_rejected(&m, &data, &l, "drop final");
    // reorder
    let m = build(&[frame(0), frame(2), frame(1), frame(3), fin.clone()]);
    assert_rejected(&m, &data, &l, "reorder");
    // splice a frame from another message (same key, other message ID, and
    // other key)
    for (key, id) in [(0x42, 8u8), (0x43, 9u8)] {
        let (other, ohl) = encrypt_with(&plain(4 * f + 10, 99), f as u32, key, id, usize::MAX);
        let ofr = other[ohl + stride..ohl + 2 * stride].to_vec();
        let m = build(&[frame(0), ofr, frame(2), frame(3), fin.clone()]);
        assert_rejected(&m, &data, &l, "splice");
    }
}

#[test]
fn swapping_headers_between_two_messages_is_rejected() {
    let f = 64usize;
    let a = plain(3 * f + 1, 0);
    let b = plain(3 * f + 1, 50);
    for (ka, ida, kb, idb) in [
        (0x42, 1u8, 0x42, 2u8),
        (0x42, 1, 0x43, 1),
        (0x42, 1, 0x43, 2),
    ] {
        let (ma, ha) = encrypt_with(&a, f as u32, ka, ida, usize::MAX);
        let (mb, hb) = encrypt_with(&b, f as u32, kb, idb, usize::MAX);
        let mut swapped = mb[..hb].to_vec();
        swapped.extend_from_slice(&ma[ha..]);
        // Decrypting with A's key: B's header fails the commitment/tag, or
        // (same key, other ID) the frames fail.
        for (key, piece) in [ka, kb].into_iter().flat_map(|k| [(k, 0), (k, 1)]) {
            let (out, r) = run(&swapped, piece, key, small_limits());
            assert!(
                r.is_err(),
                "swapped header accepted ({ka:#x},{ida},{kb:#x},{idb}, piece {piece})"
            );
            assert_eq!(out, Vec::<u8>::new(), "piece {piece}");
            for p in [0, 1, 5] {
                assert_same_outcome(&swapped, p, key, &small_limits(), "swapped header");
            }
        }
    }
}

#[test]
fn header_field_tampering_is_rejected() {
    let data = plain(100, 0);
    let (msg, hl) = encrypt(&data, 4096);
    let l = DecodeLimits::default();

    // suite downgrade to 0x0178
    let mut m = msg.clone();
    m[1..3].copy_from_slice(&0x0178u16.to_be_bytes());
    assert!(matches!(
        run(&m, 0, 0x42, l.clone()).1,
        Err(DecryptError::Format(FormatError::UnsupportedSuite(0x0178)))
    ));

    // non-framed content type
    let mut m = msg.clone();
    m[content_type_at(hl)] = 0x01;
    assert!(matches!(
        run(&m, 0, 0x42, l.clone()).1,
        Err(DecryptError::Format(FormatError::NotFramed))
    ));

    // frame_length = 3 and 2^31
    for bad in [3u32, 1 << 31] {
        let mut m = msg.clone();
        let at = content_type_at(hl) + 1;
        m[at..at + 4].copy_from_slice(&bad.to_be_bytes());
        assert!(
            matches!(
                run(&m, 0, 0x42, l.clone()).1,
                Err(DecryptError::Format(FormatError::FrameLengthOutOfRange(v))) if v == bad
            ),
            "frame length {bad}"
        );
        // Also rejected once the limits allow the value to be parsed: the
        // header tag covers it.
        let relaxed = DecodeLimits {
            min_frame_length: 1,
            max_frame_length: u32::MAX,
            ..DecodeLimits::default()
        };
        let r = run(&m, 0, 0x42, relaxed).1;
        assert!(
            matches!(r, Err(DecryptError::HeaderAuth)),
            "frame length {bad} with relaxed limits: {r:?}"
        );
    }
}

#[test]
fn oversized_contexts_are_rejected() {
    // Writer side: a 70 KiB context never reaches a header.
    let mut caller = CallerContext::new();
    caller.insert("big".into(), "x".repeat(70 * 1024));
    assert!(
        EncryptionContext::new_reserved(None)
            .with_caller(&caller)
            .is_err()
    );
    let mut caller = CallerContext::new();
    for i in 0..10 {
        caller.insert(format!("k{i}"), "x".repeat(4096));
    }
    assert!(
        EncryptionContext::new_reserved(None)
            .with_caller(&caller)
            .is_err()
    );

    // Reader side: a header declaring the largest context (0xFFFF bytes) is
    // refused from the declared length, without waiting for the bytes: only
    // the 37-byte prefix is fed.
    let mut h = vec![2u8, 0x04, 0x78];
    h.extend_from_slice(&[0u8; 32]);
    h.extend_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(h.len(), 37);
    for piece in [0, 1] {
        let (out, r) = run(&h, piece, 0x42, DecodeLimits::default());
        assert_eq!(out, Vec::<u8>::new());
        assert!(
            matches!(r, Err(DecryptError::Format(FormatError::HeaderTooLong))),
            "{r:?}"
        );
    }
}

fn splits(total: usize, sizes: &[usize]) -> Vec<usize> {
    let mut v = Vec::new();
    let mut left = total;
    for s in sizes.iter().cycle() {
        if left == 0 {
            break;
        }
        let s = (*s).clamp(1, left);
        v.push(s);
        left -= s;
    }
    v
}

/// Mostly large chunks, but small ones (1..32) often enough to split every
/// field of the format.
fn chunk_size() -> impl Strategy<Value = usize> {
    prop_oneof![1usize..32, 1usize..70_000]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn roundtrip_with_random_chunking(
        (n, f) in prop::sample::select(vec![4096u32, 65536]).prop_flat_map(|f| {
            // Free sizes, and exact multiples of the frame length used.
            prop_oneof![
                (0usize..300 * 1024),
                (0usize..=4).prop_map(move |k| k * f as usize),
            ]
            .prop_map(move |n| (n, f))
        }),
        full_final in any::<bool>(),
        in_sizes in prop::collection::vec(chunk_size(), 1..8),
        out_sizes in prop::collection::vec(chunk_size(), 1..8),
    ) {
        let data = plain(n, n);
        let ctx = EncryptionContext::new_reserved(Some("obj/key"));
        let (header, msg) = if full_final && n > 0 && n.is_multiple_of(f as usize) {
            encrypt_full_final(
                &dek(1),
                [5; 32],
                ctx,
                vec![edk()],
                f,
                &data,
                &DecodeLimits::default(),
            )
        } else {
            let (header, mut enc) =
                Encryptor::new(&dek(1), [5; 32], ctx, vec![edk()], f).expect("encryptor");
            let mut out = BytesMut::from(&header[..]);
            let mut pos = 0;
            for s in splits(data.len(), &in_sizes) {
                enc.update(&data[pos..pos + s], &mut out);
                pos += s;
            }
            enc.finish(&mut out);
            (header, out.to_vec())
        };

        let layout = FrameLayout { header_len: header.len() as u64, frame_length: f };
        prop_assert_eq!(layout.plaintext_size(msg.len() as u64), Some(n as u64));

        let d = Decryptor::new(DecodeLimits::default(), Some("obj/key".into()));
        let mut sizes = splits(msg.len(), &out_sizes).into_iter();
        let (got, r) = drive(d, &msg, || sizes.next().unwrap_or(usize::MAX), |d| d.set_data_key(&dek(1)));
        prop_assert!(r.is_ok());
        prop_assert_eq!(got, data);
    }
}

/// Decrypts `msg` in whole-object mode, feeding `piece` bytes at a time.
fn run_whole(msg: &[u8], piece: usize, limits: DecodeLimits) -> Result<Vec<u8>, DecryptError> {
    run_whole_keyed(msg, piece, limits, 0x42)
}

fn run_whole_keyed(
    msg: &[u8],
    piece: usize,
    limits: DecodeLimits,
    key: u8,
) -> Result<Vec<u8>, DecryptError> {
    drive_whole(
        Decryptor::new(limits, None),
        msg,
        || piece,
        |d| d.set_data_key(&dek(key)),
    )
    .map(|b| b.to_vec())
}

/// The whole-object path must accept and reject exactly what the streaming
/// one does, with the same error, and release the same plaintext.
#[track_caller]
fn assert_same_outcome(msg: &[u8], piece: usize, key: u8, limits: &DecodeLimits, what: &str) {
    let piece = if piece == 0 { usize::MAX } else { piece };
    let (out, streamed) = run(msg, piece, key, limits.clone());
    let whole = run_whole_keyed(msg, piece, limits.clone(), key);
    match (streamed, whole) {
        (Ok(()), Ok(got)) => assert_eq!(got, out, "{what}"),
        (Err(a), Err(b)) => assert_eq!(format!("{a:?}"), format!("{b:?}"), "{what}"),
        (a, b) => panic!(
            "{what}: streaming {a:?} but whole-object {:?}",
            b.map(|v| v.len())
        ),
    }
}

#[test]
fn whole_object_decrypt_agrees_with_streaming_on_every_bit_flip() {
    let data = plain(3 * 64 - 5, 0);
    let (msg, _) = encrypt(&data, 64);
    let limits = small_limits();
    assert_eq!(
        run_whole(&msg, usize::MAX, limits.clone()).expect("clean"),
        data
    );
    for piece in [0, 1, 5] {
        assert_same_outcome(&msg, piece, 0x42, &limits, "clean");
        for at in 0..msg.len() {
            for bit in 0..8 {
                let mut m = msg.clone();
                m[at] ^= 1 << bit;
                assert_same_outcome(
                    &m,
                    piece,
                    0x42,
                    &limits,
                    &format!("flip {at}/{bit} piece {piece}"),
                );
            }
        }
    }
}

#[test]
fn whole_object_decrypt_rejects_every_truncation() {
    let data = plain(1024, 0);
    let (msg, _) = encrypt(&data, 128);
    let limits = small_limits();
    for cut in 0..msg.len() {
        assert!(
            run_whole(&msg[..cut], usize::MAX, limits.clone()).is_err(),
            "cut at {cut} accepted"
        );
        assert_same_outcome(&msg[..cut], 0, 0x42, &limits, &format!("cut {cut}"));
    }
}

#[test]
fn whole_object_decrypt_rejects_trailing_bytes_and_a_missing_final_frame() {
    let f = 64usize;
    let data = plain(3 * f + 1, 0);
    let (mut msg, hl) = encrypt(&data, f as u32);
    let limits = small_limits();
    let stride = f + TAG_AND_PREFIX;
    // The last full frame without the final one.
    let r = run_whole(&msg[..hl + 3 * stride], usize::MAX, limits.clone());
    assert!(matches!(r, Err(DecryptError::MissingFinalFrame)), "{r:?}");
    msg.push(0);
    for piece in [usize::MAX, 1] {
        let r = run_whole(&msg, piece, limits.clone());
        assert!(matches!(r, Err(DecryptError::TrailingBytes)), "{r:?}");
    }
    // Nor a second whole message appended.
    let (again, _) = encrypt(&data, f as u32);
    let mut twice = again.clone();
    twice.extend_from_slice(&again);
    assert!(run_whole(&twice, usize::MAX, limits).is_err());
}

#[test]
fn whole_object_decrypt_handles_empty_and_exact_multiples() {
    let limits = small_limits();
    for n in [0usize, 1, 63, 64, 65, 128, 192] {
        let data = plain(n, 3);
        let (msg, _) = encrypt(&data, 64);
        assert_eq!(
            run_whole(&msg, usize::MAX, limits.clone()).expect("whole"),
            data,
            "{n}"
        );
        assert_eq!(
            run_whole(&msg, 7, limits.clone()).expect("pieces"),
            data,
            "{n}"
        );
    }
}

/// The first error of a whole-object decryptor fed `msg` in `piece`-byte
/// pieces, with the number of bytes fed when it arrived.
fn first_whole_error(msg: &[u8], piece: usize) -> (DecryptError, usize) {
    let mut d = Decryptor::new(small_limits(), None).whole_object();
    let mut fed = 0;
    for chunk in msg.chunks(piece) {
        fed += chunk.len();
        let step = d.feed(Bytes::copy_from_slice(chunk)).and_then(|p| {
            if matches!(p, Progress::NeedKey) {
                d.set_data_key(&dek(0x42))
            } else {
                Ok(p)
            }
        });
        if let Err(e) = step {
            return (e, fed);
        }
    }
    panic!("the garbage was accepted");
}

#[test]
fn whole_object_rejects_a_bad_frame_before_buffering_the_rest() {
    let f = 64usize;
    let (msg, hl) = encrypt(&plain(3 * f + 1, 0), f as u32);
    let stride = f + TAG_AND_PREFIX;
    // Genuine header, frame 1 with a flipped bit, then 1 MiB of garbage.
    let mut m = msg[..hl + stride].to_vec();
    m[hl + 20] ^= 1;
    m.resize(m.len() + (1 << 20), 0xA5);
    let (e, fed) = first_whole_error(&m, 1024);
    assert!(matches!(e, DecryptError::FrameAuth { seq: 1 }), "{e:?}");
    assert!(
        fed <= hl + stride + 1024,
        "{fed} bytes fed before the error"
    );
}

#[test]
fn whole_object_rejects_bytes_after_the_final_frame_at_once() {
    let (msg, _) = encrypt(&plain(3 * 64 + 1, 0), 64);
    let mut m = msg.clone();
    m.resize(m.len() + (1 << 20), 0xA5);
    let (e, fed) = first_whole_error(&m, 1024);
    assert!(matches!(e, DecryptError::TrailingBytes), "{e:?}");
    assert!(fed <= msg.len() + 1024, "{fed} bytes fed before the error");
}
