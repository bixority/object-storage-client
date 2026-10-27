#!/usr/bin/env python3
"""Smoke test of the release `osc` binary on file:// objects.

Runs put, get and get-stream, plain and encrypted, on one large file and
records wall time, CPU time and peak RSS of each run as JSON (median of
--runs). Peak RSS over budget fails the run (exit 1); throughput and the
encrypted/plain CPU ratio only warn, because shared runners are noisy.

    python3 scripts/perf_smoke.py --osc target/release/osc --out perf.json
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

MIB = 1 << 20
# Peak RSS budgets in MB, from the performance plan: a get or get-stream stays
# near the baseline whatever the size, a single upload holds about
# (4 + 1) x 8 MiB of parts plus slack.
RSS_BUDGET_MB = {"get": 16.0, "get-stream": 16.0, "put": 40.0}
CPU_RATIO_WARN = 2.5
THROUGHPUT_REGRESSION_WARN = 0.25


# Runs argv in a child of a small interpreter and prints its wall time, CPU
# time and peak RSS. A child's peak RSS includes its parent's size at exec, so
# a small parent (about 10 MB, which is also the floor of every figure here)
# keeps the numbers close to the binary's own.
WRAPPER = """
import json, resource, subprocess, sys, time
argv, out = json.loads(sys.argv[1]), sys.argv[2]
start = time.perf_counter()
proc = subprocess.run(argv, stdout=open(out, "wb"), stderr=subprocess.PIPE)
wall = time.perf_counter() - start
ru = resource.getrusage(resource.RUSAGE_CHILDREN)
print(json.dumps([proc.returncode, wall, ru.ru_utime + ru.ru_stime,
                  ru.ru_maxrss / 1024, proc.stderr.decode(errors="replace")[-500:]]))
"""


def run_once(argv, stdout_path=None):
    """Runs argv; returns (wall s, cpu s, max rss MB)."""
    done = subprocess.run(
        [sys.executable, "-I", "-S", "-c", WRAPPER, json.dumps(argv), stdout_path or os.devnull],
        capture_output=True,
        text=True,
        check=True,
    )
    code, wall, cpu, rss, stderr = json.loads(done.stdout)
    if code != 0:
        sys.exit(f"FAIL {argv}: exit {code}\n{stderr}")
    return wall, cpu, rss  # ru_maxrss is KiB on Linux, converted by the wrapper


def scenario(name, op, kind, size_mb, argv, runs, setup=None, stdout_path=None):
    samples = []
    for _ in range(runs):
        if setup:
            setup()
        samples.append(run_once(argv, stdout_path))
    wall = statistics.median(s[0] for s in samples)
    return {
        "scenario": name,
        "op": op,
        "kind": kind,
        "size_mb": size_mb,
        "wall_s": round(wall, 3),
        "cpu_s": round(statistics.median(s[1] for s in samples), 3),
        "max_rss_mb": round(statistics.median(s[2] for s in samples), 1),
        "max_rss_mb_worst": round(max(s[2] for s in samples), 1),
        "throughput_mb_s": round(size_mb * MIB / 1e6 / wall, 1),
    }


def rm(path):
    p = Path(path)
    if p.exists():
        p.unlink()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--osc", default="target/release/osc")
    ap.add_argument("--size-mib", type=int, default=512)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--out", default="perf-smoke.json")
    ap.add_argument("--baseline", help="earlier --out file; throughput is compared to it")
    ap.add_argument("--dir", help="scratch directory on a real disk (default: a temp dir in cwd)")
    args = ap.parse_args()

    osc = str(Path(args.osc).resolve())
    work = Path(tempfile.mkdtemp(prefix="osc-perf-", dir=args.dir or "."))
    try:
        results = measure(osc, work, args)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    Path(args.out).write_text(json.dumps(results, indent=2) + "\n")
    sys.exit(report(results, args))


def measure(osc, work, args):
    keyring = work / "keyring"
    # Created owner-only: the keyring file must not be accessible to others.
    fd = os.open(keyring, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(
            subprocess.run(
                [osc, "keyring", "new", "--prefix", "perf"],
                check=True,
                capture_output=True,
            ).stdout
        )
    enc = ["--keyring", str(keyring)]

    src = work / "big.bin"
    block = os.urandom(MIB)
    with open(src, "wb") as f:
        for _ in range(args.size_mib):
            f.write(block)
    store, out = work / "store", work / "out"
    store.mkdir()
    out.mkdir()
    size = args.size_mib

    def u(name):
        return f"file://{store / name}"

    results = []
    for kind, extra in (("plain", []), ("enc", enc)):
        obj = f"{kind}.bin"
        dst = out / f"{kind}.bin"
        results.append(
            scenario(
                f"{kind} put", "put", kind, size,
                [osc, *extra, "put", str(src), u(obj)], args.runs,
                setup=lambda obj=obj: rm(store / obj),
            )
        )
        results.append(
            scenario(
                f"{kind} get", "get", kind, size,
                [osc, *extra, "get", u(obj), str(dst)], args.runs,
                setup=lambda dst=dst: rm(dst),
            )
        )
        results.append(
            scenario(
                f"{kind} get-stream to /dev/null", "get-stream", kind, size,
                [osc, *extra, "get-stream", u(obj)], args.runs,
                stdout_path="/dev/null",
            )
        )
    for r in results:
        print(json.dumps(r), flush=True)
    return results


def report(results, args):
    failed = False
    for r in results:
        budget = RSS_BUDGET_MB[r["op"]]
        if r["max_rss_mb_worst"] > budget:
            failed = True
            print(
                f"::error::{r['scenario']}: peak RSS {r['max_rss_mb_worst']} MB "
                f"exceeds the budget of {budget} MB"
            )
    by = {(r["op"], r["kind"]): r for r in results}
    for op in RSS_BUDGET_MB:
        plain, enc = by[(op, "plain")], by[(op, "enc")]
        if plain["cpu_s"] > 0 and enc["cpu_s"] / plain["cpu_s"] > CPU_RATIO_WARN:
            print(
                f"::warning::{op}: encrypted CPU is {enc['cpu_s'] / plain['cpu_s']:.1f}x "
                f"plain ({enc['cpu_s']} s vs {plain['cpu_s']} s)"
            )
    if args.baseline and Path(args.baseline).exists():
        old = {r["scenario"]: r for r in json.loads(Path(args.baseline).read_text())}
        for r in results:
            was = old.get(r["scenario"])
            if was and r["throughput_mb_s"] < was["throughput_mb_s"] * (1 - THROUGHPUT_REGRESSION_WARN):
                print(
                    f"::warning::{r['scenario']}: {r['throughput_mb_s']} MB/s is more than "
                    f"{THROUGHPUT_REGRESSION_WARN:.0%} below the baseline {was['throughput_mb_s']} MB/s"
                )
    return 1 if failed else 0


if __name__ == "__main__":
    main()
