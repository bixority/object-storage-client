//! Deterministic performance guards: allocation counts and peak heap of the
//! public streaming paths on `file://`, measured with a counting allocator
//! that is installed in this test binary only.
//!
//! Ceilings are the numbers measured at the time of writing (release and
//! debug builds allocate the same amount) plus about 50% headroom, so they
//! catch a change that makes a path copy or allocate per frame, not noise.
//! Run with `-- --nocapture` to see the measured values.
//!
//! Not covered here because the public API cannot reach it: request counts per
//! operation (encrypted get is one GET, a re-encrypting copy at most two) and
//! the multipart part count and buffering of `upload_stream`; `file://` has
//! neither. The `src/upload.rs` and `src/encryption/envelope.rs` tests pin
//! those with instrumented stores.

#[path = "../benches/common/mod.rs"]
mod common;

use alloc_counter::Usage;
use bytes::Bytes;
use common::{MIB, chunked, drain_len, encrypted_pair, measure, payload, plain, url};
use object_storage_client::{EncryptedObjectStorageClient, ObjectStorageClient};
use std::sync::{Mutex, MutexGuard};
use tempfile::TempDir;

#[global_allocator]
static ALLOC: alloc_counter::Counting = alloc_counter::Counting;

/// The counters are process-wide, so one guard runs at a time.
static SERIAL: Mutex<()> = Mutex::new(());

const SIZE: usize = 64 * MIB;
const CHUNK: usize = 64 * 1024;

struct Fixture {
    _dir: TempDir,
    out: std::path::PathBuf,
    plain: ObjectStorageClient,
    enc: EncryptedObjectStorageClient,
    enc_b: EncryptedObjectStorageClient,
    data: Bytes,
    // Last field: the lock is released only after everything else is dropped.
    _serial: MutexGuard<'static, ()>,
}

fn fixture(len: usize) -> Fixture {
    let serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("out");
    std::fs::create_dir(&out).expect("out");
    let (enc, enc_b) = encrypted_pair("guard-a", "guard-b");
    Fixture {
        _serial: serial,
        plain: plain(),
        enc,
        enc_b,
        data: payload(len),
        out,
        _dir: dir,
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

fn show(name: &str, u: &Usage) {
    println!(
        "{name}: allocs={} bytes={} peak_above_base={}",
        u.allocs, u.bytes, u.peak_above_base
    );
}

/// Fails with the measured value if `got > limit`.
fn at_most(what: &str, got: usize, limit: usize) {
    assert!(got <= limit, "{what}: {got} exceeds the ceiling {limit}");
}

async fn seed(f: &Fixture, secure: bool, u: &str) {
    let s = chunked(&f.data, CHUNK);
    if secure {
        f.enc.put_stream(u, s).await.expect("seed");
    } else {
        f.plain.put_stream(u, s).await.expect("seed");
    }
}

#[test]
fn get_stream_allocates_per_frame_and_holds_a_few_mib() {
    let f = fixture(SIZE);
    runtime().block_on(async {
        let (up, ue) = (url(&f.out, "p"), url(&f.out, "e"));
        seed(&f, false, &up).await;
        seed(&f, true, &ue).await;

        let (n, plain_use) =
            measure(async || drain_len(f.plain.get_stream(&up).await.expect("stream")).await).await;
        assert_eq!(n, SIZE);
        let (n, enc_use) =
            measure(async || drain_len(f.enc.get_stream(&ue).await.expect("stream")).await).await;
        assert_eq!(n, SIZE);
        show("plain get_stream 64 MiB", &plain_use);
        show("enc get_stream 64 MiB", &enc_use);

        // Measured: plain 279, enc 565 allocations (about 9 per MiB).
        at_most("plain get_stream allocs", plain_use.allocs, 450);
        at_most("enc get_stream allocs", enc_use.allocs, 850);
        at_most("plain get_stream peak", plain_use.peak_above_base, 4 * MIB);
        at_most("enc get_stream peak", enc_use.peak_above_base, 4 * MIB);
    });
}

#[test]
fn get_holds_the_object_and_a_few_mib() {
    let f = fixture(SIZE);
    runtime().block_on(async {
        let (up, ue) = (url(&f.out, "p"), url(&f.out, "e"));
        seed(&f, false, &up).await;
        seed(&f, true, &ue).await;

        let (b, plain_use) = measure(async || f.plain.get(&up).await.expect("get")).await;
        assert_eq!(b.len(), SIZE);
        drop(b);
        let (b, enc_use) = measure(async || f.enc.get(&ue).await.expect("get")).await;
        assert_eq!(b.len(), SIZE);
        drop(b);
        show("plain get 64 MiB", &plain_use);
        show("enc get 64 MiB", &enc_use);

        // Measured: the result buffer plus about 2.5 MiB (enc).
        at_most("plain get peak", plain_use.peak_above_base, SIZE + 4 * MIB);
        at_most("enc get peak", enc_use.peak_above_base, SIZE + 4 * MIB);
        // Measured: plain 22, enc 308.
        at_most("plain get allocs", plain_use.allocs, 60);
        at_most("enc get allocs", enc_use.allocs, 460);
    });
}

#[test]
fn put_stream_allocates_per_frame_and_holds_a_few_mib() {
    let f = fixture(SIZE);
    runtime().block_on(async {
        let (up, ue) = (url(&f.out, "p"), url(&f.out, "e"));
        let (plain_use, enc_use) = {
            let ((), p) = measure(async || {
                f.plain
                    .put_stream(&up, chunked(&f.data, CHUNK))
                    .await
                    .expect("put");
            })
            .await;
            let ((), e) = measure(async || {
                f.enc
                    .put_stream(&ue, chunked(&f.data, CHUNK))
                    .await
                    .expect("put");
            })
            .await;
            (p, e)
        };
        show("plain put_stream 64 MiB", &plain_use);
        show("enc put_stream 64 MiB", &enc_use);

        // Measured: plain 1075, enc 2130 allocations (the 1024 chunk slices
        // and their stream items are in both).
        at_most("plain put_stream allocs", plain_use.allocs, 1700);
        at_most("enc put_stream allocs", enc_use.allocs, 3200);
        // Local staging writes in bounded chunks; a part is 8 MiB elsewhere,
        // but there is no multipart here, so nothing near that is held.
        at_most("plain put_stream peak", plain_use.peak_above_base, 4 * MIB);
        at_most("enc put_stream peak", enc_use.peak_above_base, 4 * MIB);
    });
}

#[test]
fn small_caller_chunks_do_not_multiply_allocations() {
    let f = fixture(8 * MIB);
    runtime().block_on(async {
        for (chunk, plain_max, enc_max) in [(4 * 1024, 3200, 3800), (MIB, 70, 140)] {
            let (up, ue) = (url(&f.out, "p"), url(&f.out, "e"));
            let ((), p) = measure(async || {
                f.plain
                    .put_stream(&up, chunked(&f.data, chunk))
                    .await
                    .expect("put");
            })
            .await;
            let ((), e) = measure(async || {
                f.enc
                    .put_stream(&ue, chunked(&f.data, chunk))
                    .await
                    .expect("put");
            })
            .await;
            show(&format!("plain put_stream 8 MiB, {chunk} B chunks"), &p);
            show(&format!("enc put_stream 8 MiB, {chunk} B chunks"), &e);
            at_most("plain allocs", p.allocs, plain_max);
            at_most("enc allocs", e.allocs, enc_max);
            at_most("plain peak", p.peak_above_base, 4 * MIB);
            at_most("enc peak", e.peak_above_base, 4 * MIB);
        }
    });
}

#[test]
fn reencrypting_copy_streams_in_bounded_memory() {
    let f = fixture(SIZE);
    runtime().block_on(async {
        let (src, dst) = (url(&f.out, "src"), url(&f.out, "dst"));
        seed(&f, true, &src).await;

        // `enc_b` has a different active key, so this is a re-encryption.
        let ((), usage) = measure(async || f.enc_b.copy(&src, &dst).await.expect("copy")).await;
        show("enc copy (re-encrypt) 64 MiB", &usage);
        // Measured: 2715 allocations.
        at_most("copy allocs", usage.allocs, 4100);
        at_most("copy peak", usage.peak_above_base, 4 * MIB);

        let n = drain_len(f.enc_b.get_stream(&dst).await.expect("stream")).await;
        assert_eq!(n, SIZE, "the copy must hold the whole object");
    });
}

#[test]
fn many_small_objects_cost_a_bounded_number_of_allocations() {
    let f = fixture(4096);
    runtime().block_on(async {
        let n = 1000;
        let ((), plain_use) = measure(async || {
            for i in 0..n {
                let u = url(&f.out, &format!("p{i}"));
                f.plain.put(&u, f.data.clone()).await.expect("put");
                assert_eq!(f.plain.get(&u).await.expect("get").len(), 4096);
            }
        })
        .await;
        let ((), enc_use) = measure(async || {
            for i in 0..n {
                let u = url(&f.out, &format!("e{i}"));
                f.enc.put(&u, f.data.clone()).await.expect("put");
                assert_eq!(f.enc.get(&u).await.expect("get").len(), 4096);
            }
        })
        .await;
        show("plain 1000 x 4 KiB put+get", &plain_use);
        show("enc 1000 x 4 KiB put+get", &enc_use);
        // Measured: 60 (plain) and 115 (enc) allocations per object.
        at_most("plain small objects allocs", plain_use.allocs, 90 * n);
        at_most("enc small objects allocs", enc_use.allocs, 175 * n);
        at_most("small objects peak", enc_use.peak_above_base, 4 * MIB);
    });
}
