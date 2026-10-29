//! Entry points for the `fuzz/` crate. Not part of the public API.

use std::sync::LazyLock;

use bytes::{Bytes, BytesMut};

use super::context::EncryptionContext;
use super::fixtures::{dek, edk, small_limits};
use super::frame::{self, Decryptor, Encryptor, Progress};
use super::header::{DecodeLimits, HeaderCodec, HeaderParse};
use super::keys::DataKey;
use super::suite::DecryptError;

/// The fixed fuzz data key, built once.
static DEK: LazyLock<DataKey> = LazyLock::new(|| dek(0x42));

fn start(frame_length: u32) -> (Bytes, Encryptor) {
    Encryptor::with_limits(
        &DEK,
        [7; 32],
        EncryptionContext::new_reserved(Some("k")),
        vec![edk()],
        frame_length,
        &small_limits(),
    )
    .expect("valid fuzz parameters")
}

/// A genuine header for the fixed key and 64-byte frames, built once.
static KEYED_HEADER: LazyLock<Bytes> = LazyLock::new(|| start(64).0);

/// Parses arbitrary bytes as a header; must never panic.
///
/// A header that parses must re-serialise to the exact bytes the header tag
/// covers: parsing is canonical.
pub fn header_parse(data: &[u8]) {
    for limits in [DecodeLimits::default(), small_limits()] {
        if let Ok(HeaderParse::Done {
            header, header_len, ..
        }) = HeaderCodec::parse(data, &limits)
        {
            assert_eq!(
                HeaderCodec::serialize_unauthenticated(&header)
                    .ok()
                    .as_deref(),
                data.get(..header_len - 16),
                "a parsed header is not canonical"
            );
        }
    }
}

/// Feeds arbitrary bytes to a `Decryptor` in fuzz-chosen pieces.
///
/// The first byte selects the mode and the piece size. In key mode the input
/// follows a genuine header and the decryptor is given the matching key, so
/// the frame parser is exercised; otherwise the input is the raw stream.
/// (Frames cannot authenticate here; see [`encrypt_mutate`] for that.)
pub fn decryptor_feed(data: &[u8]) {
    let Some((&ctl, rest)) = data.split_first() else {
        return;
    };
    let piece = usize::from(ctl >> 1) + 1;
    let keyed = ctl & 1 == 1;

    let mut d = Decryptor::new(small_limits(), None);
    let mut stream: Vec<u8> = Vec::new();
    if keyed {
        stream.extend_from_slice(&KEYED_HEADER);
    }
    stream.extend_from_slice(rest);

    let mut have_key = false;
    for chunk in stream.chunks(piece) {
        let mut input = Bytes::copy_from_slice(chunk);
        loop {
            match d.feed(std::mem::take(&mut input)) {
                Ok(Progress::NeedKey) if keyed && !have_key => {
                    if d.set_data_key(&DEK).is_err() {
                        return;
                    }
                    have_key = true;
                }
                Ok(_) => break,
                Err(_) => return,
            }
        }
    }
    let _ = d.finish();
}

/// Runs `msg` through a keyed [`Decryptor`] in `piece`-byte feeds. Returns
/// every plaintext byte released before the outcome, and the outcome.
fn drive(msg: &[u8], piece: usize) -> (Vec<u8>, Result<(), DecryptError>) {
    frame::drive(
        Decryptor::new(small_limits(), None),
        msg,
        || piece,
        |d| d.set_data_key(&DEK),
    )
}

/// One mutation of a serialized message, decoded from four fuzz bytes.
fn mutate(msg: &mut Vec<u8>, m: &[u8]) {
    let &[kind, hi, lo, val] = m else {
        return;
    };
    if msg.is_empty() {
        return;
    }
    let pos = usize::from(u16::from_be_bytes([hi, lo])) % msg.len();
    let end = (pos + 1 + usize::from(val)).min(msg.len());
    match kind % 7 {
        0 => msg[pos] ^= 1 << (val & 7),
        1 => msg[pos] = val,
        2 => msg.truncate(pos),
        3 => {
            msg.drain(pos..end);
        }
        4 => {
            let copy = msg[pos..end].to_vec();
            msg.splice(end..end, copy);
        }
        5 => msg.insert(pos, val),
        _ => msg.push(val),
    }
}

/// Structure-aware: encrypts fuzzer-chosen plaintext with a fuzzer-chosen
/// frame length and update chunking, applies fuzzer-chosen mutations, and
/// decrypts the result in fuzzer-chosen pieces.
///
/// Input layout: `frame_length | encrypt_chunk | decrypt_piece | plaintext_len/4
/// | plaintext | (kind, pos_hi, pos_lo, value)*` with at most 8 mutations.
/// Unlike [`decryptor_feed`] this reaches frame authentication, sequence
/// advance, the final frame, `Done` and the trailing-bytes check.
///
/// # Panics
/// If any byte is released that is not part of the original plaintext prefix,
/// if an unmodified message does not round-trip, or if a modified message is
/// accepted.
pub fn encrypt_mutate(data: &[u8]) {
    let [f, enc_chunk, dec_piece, len4, rest @ ..] = data else {
        return;
    };
    let frame_length = u32::from(*f % 96) + 1;
    let (plain, muts) = rest.split_at((usize::from(*len4) * 4).min(rest.len()));

    let (header, mut enc) = start(frame_length);
    let mut out = BytesMut::from(&header[..]);
    for chunk in plain.chunks(usize::from(*enc_chunk).max(1)) {
        enc.update(chunk, &mut out);
    }
    enc.finish(&mut out);
    let original = out.to_vec();

    let mut msg = original.clone();
    for m in muts.as_chunks::<4>().0.iter().take(8) {
        mutate(&mut msg, m);
    }

    let piece = if *dec_piece == 0 {
        usize::MAX
    } else {
        usize::from(*dec_piece)
    };
    let (released, result) = drive(&msg, piece);
    let whole = frame::drive_whole(
        Decryptor::new(small_limits(), None),
        &msg,
        || piece,
        |d| d.set_data_key(&DEK),
    );
    match (&result, &whole) {
        (Ok(()), Ok(got)) => assert_eq!(
            &got[..],
            &released[..],
            "whole-object read released other plaintext"
        ),
        (Err(a), Err(b)) => assert_eq!(
            format!("{a:?}"),
            format!("{b:?}"),
            "whole-object read failed differently"
        ),
        _ => panic!("streaming gave {result:?} but whole-object {whole:?}"),
    }
    assert!(
        plain.starts_with(&released),
        "released bytes that are not a verified plaintext prefix"
    );
    if msg == original {
        assert!(result.is_ok(), "unmodified message rejected: {result:?}");
        assert_eq!(released, plain, "unmodified message did not round-trip");
    } else {
        assert!(result.is_err(), "a modified message was accepted");
    }
}

/// Replays the committed seeds in `fuzz/seeds/<target>/`, so a crash found once
/// stays covered by `cargo test` and the harness assertions above run on every
/// build.
#[cfg(test)]
mod tests {
    use std::path::Path;

    /// Runs `entry` on every seed of `target`, naming the seed it is about to
    /// replay (the test harness shows this output for a failing test).
    fn replay(target: &str, entry: fn(&[u8])) {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fuzz/seeds")
            .join(target);
        let mut seeds: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
            .map(|entry| entry.expect("seed entry").path())
            .collect();
        seeds.sort();
        assert!(
            seeds.len() >= 10,
            "{} holds only {} seeds",
            dir.display(),
            seeds.len()
        );
        for seed in seeds {
            println!("replaying {}", seed.display());
            entry(&std::fs::read(&seed).expect("readable seed"));
        }
    }

    #[test]
    fn header_parse_seeds_replay() {
        replay("header_parse", super::header_parse);
    }

    #[test]
    fn decryptor_feed_seeds_replay() {
        replay("decryptor_feed", super::decryptor_feed);
    }

    #[test]
    fn encrypt_mutate_seeds_replay() {
        replay("encrypt_mutate", super::encrypt_mutate);
    }
}
