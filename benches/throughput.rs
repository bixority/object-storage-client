//! Throughput and allocation benchmark over the public API on `file://`
//! (the crate has no in-memory URL scheme). It prints one table: the median
//! of `BENCH_RUNS` runs (default 3), MB/s, allocation count and the peak heap
//! above the heap at the start of the run.
//!
//! ```text
//! cargo bench --bench throughput
//! BENCH_MIB=64 BENCH_RUNS=5 BENCH_DIR=/data/scratch cargo bench --bench throughput
//! ```
//!
//! Data goes under `BENCH_DIR`, default `target/bench-data`, which is on the
//! real disk rather than a tmpfs `/tmp`; it is deleted at exit.
//!
//! Only a real benchmark run (`cargo bench`, which passes `--bench`) uses
//! `BENCH_MIB` and `BENCH_RUNS`. `cargo test --benches`, `--all-targets` and
//! `cargo bench -- --test` run one 8 MiB pass instead.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

mod common;

use common::{MIB, chunked, drain_len, encrypted_pair, measure, payload, plain, url};
use std::future::Future;
use std::path::Path;
use std::time::Instant;

#[global_allocator]
static ALLOC: alloc_counter::Counting = alloc_counter::Counting;

struct Row {
    name: String,
    secs: f64,
    mb_s: f64,
    allocs: usize,
    peak: usize,
}

fn env_num(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn median<T: Copy + PartialOrd>(mut v: Vec<T>) -> T {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

/// Runs `f` `runs` times; reports the median wall time and allocations for
/// `bytes` of payload.
async fn bench<F, Fut>(rows: &mut Vec<Row>, name: &str, bytes: usize, runs: usize, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let (mut secs, mut allocs, mut peaks) = (vec![], vec![], vec![]);
    for _ in 0..runs {
        let t = Instant::now();
        let ((), usage) = measure(async || f().await).await;
        secs.push(t.elapsed().as_secs_f64());
        allocs.push(usage.allocs);
        peaks.push(usage.peak_above_base);
    }
    let secs = median(secs);
    rows.push(Row {
        name: name.to_owned(),
        secs,
        mb_s: bytes as f64 / secs / 1e6,
        allocs: median(allocs),
        peak: median(peaks),
    });
}

async fn scenarios(dir: &Path, mib: usize, runs: usize) -> Vec<Row> {
    let mut rows = vec![];
    let plain = plain();
    // The second client's active key differs (and it holds the first's key),
    // to force re-encryption.
    let (enc, enc_b) = encrypted_pair("bench-a", "bench-b");

    let size = mib * MIB;
    let data = payload(size);
    let out = dir.join("out");
    std::fs::create_dir(&out).expect("out dir");
    let label = format!("{mib} MiB");

    for (kind, secure) in [("plain", false), ("enc", true)] {
        let u = url(&out, &format!("{kind}.bin"));
        // Seeds the object for the reads below.
        let seed = chunked(&data, 64 * 1024);
        if secure {
            enc.put_stream(&u, seed).await.expect("seed");
        } else {
            plain.put_stream(&u, seed).await.expect("seed");
        }

        bench(
            &mut rows,
            &format!("{kind} get {label}"),
            size,
            runs,
            async || {
                let b = if secure {
                    enc.get(&u).await
                } else {
                    plain.get(&u).await
                };
                assert_eq!(b.expect("get").len(), size);
            },
        )
        .await;
        bench(
            &mut rows,
            &format!("{kind} get_stream {label}"),
            size,
            runs,
            async || {
                let n = if secure {
                    drain_len(enc.get_stream(&u).await.expect("stream")).await
                } else {
                    drain_len(plain.get_stream(&u).await.expect("stream")).await
                };
                assert_eq!(n, size);
            },
        )
        .await;
        bench(
            &mut rows,
            &format!("{kind} put_stream {label} 64KiB chunks"),
            size,
            runs,
            async || {
                let s = chunked(&data, 64 * 1024);
                if secure {
                    enc.put_stream(&u, s).await.expect("put");
                } else {
                    plain.put_stream(&u, s).await.expect("put");
                }
            },
        )
        .await;
    }

    // Chunk sizes of the caller's stream; the encoder re-frames them.
    let small = (size / 8).max(MIB);
    let part = data.slice(..small);
    for (kind, secure) in [("plain", false), ("enc", true)] {
        let u = url(&out, &format!("{kind}-chunks.bin"));
        for chunk in [4 * 1024, 64 * 1024, MIB] {
            let name = format!(
                "{kind} put_stream {} MiB {} chunks",
                small / MIB,
                human(chunk)
            );
            bench(&mut rows, &name, small, runs, async || {
                let s = chunked(&part, chunk);
                if secure {
                    enc.put_stream(&u, s).await.expect("put");
                } else {
                    plain.put_stream(&u, s).await.expect("put");
                }
            })
            .await;
        }
    }

    // Reads with enc, writes with enc_b: a new data key and message id.
    let src = url(&out, "enc.bin");
    let dst = url(&out, "recrypt.bin");
    bench(
        &mut rows,
        &format!("enc copy (re-encrypt) {label}"),
        size,
        runs,
        async || {
            enc_b.copy(&src, &dst).await.expect("copy");
        },
    )
    .await;
    // The same client, which may byte-copy where the key and path allow.
    let dst = url(&out, "bytecopy.bin");
    bench(
        &mut rows,
        &format!("enc copy (same client) {label}"),
        size,
        runs,
        async || {
            enc.copy(&src, &dst).await.expect("copy");
        },
    )
    .await;

    // Per-object overhead: 1000 x 4 KiB, each a put then a get.
    let many = out.join("many");
    std::fs::create_dir(&many).expect("many dir");
    let tiny = payload(4096);
    for (kind, secure) in [("plain", false), ("enc", true)] {
        let n = 1000;
        bench(
            &mut rows,
            &format!("{kind} 1000 x 4 KiB put+get"),
            n * 4096,
            runs,
            async || {
                for i in 0..n {
                    let u = url(&many, &format!("{kind}-{i}"));
                    if secure {
                        enc.put(&u, tiny.clone()).await.expect("put");
                        assert_eq!(enc.get(&u).await.expect("get").len(), 4096);
                    } else {
                        plain.put(&u, tiny.clone()).await.expect("put");
                        assert_eq!(plain.get(&u).await.expect("get").len(), 4096);
                    }
                }
            },
        )
        .await;
    }
    rows
}

fn human(n: usize) -> String {
    if n >= MIB {
        format!("{}MiB", n / MIB)
    } else {
        format!("{}KiB", n / 1024)
    }
}

fn main() {
    // `cargo bench` passes `--bench`, while `cargo test --bench(es)` and
    // `--all-targets` pass nothing. So anything but a benchmark run, or
    // `cargo bench -- --test`, is one cheap 8 MiB pass.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let has = |flag: &str| args.iter().any(|a| a == flag);
    let smoke = has("--test") || !has("--bench");
    let mib = if smoke { 8 } else { env_num("BENCH_MIB", 256) };
    let runs = if smoke { 1 } else { env_num("BENCH_RUNS", 3) };
    let base = std::env::var_os("BENCH_DIR").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("target/bench-data"),
        Into::into,
    );
    std::fs::create_dir_all(&base).expect("bench dir");
    let tmp = tempfile::Builder::new()
        .prefix("osc-bench-")
        .tempdir_in(&base)
        .expect("tempdir");

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let rows = rt.block_on(scenarios(tmp.path(), mib, runs));

    println!(
        "{:<46} {:>9} {:>10} {:>10} {:>10}",
        "scenario", "median s", "MB/s", "allocs", "peak MiB"
    );
    for r in rows {
        println!(
            "{:<46} {:>9.3} {:>10.0} {:>10} {:>10.1}",
            r.name,
            r.secs,
            r.mb_s,
            r.allocs,
            r.peak as f64 / MIB as f64
        );
    }
}
