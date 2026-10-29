"""The async runtime is capped at a few worker threads, overridable by env.

The policy itself (the cap of 4 and the override rules) is pinned by the Rust
test `worker_threads_policy`; these tests check that it is wired to the
environment, on a child pinned to one CPU so the host and its cgroup quota
cannot change the answer.
"""

import os
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    not os.path.isdir("/proc/self/task"), reason="needs /proc"
)

# A pure-async call (it fails before any file access), so only the runtime's
# workers exist when the threads are counted. The child pins itself to one CPU
# first, so the default is 1 whatever the host or its CPU quota.
_PROBE = textwrap.dedent(
    """
    import asyncio, os
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    from object_storage_client import ObjectStorageClient

    async def main():
        c = ObjectStorageClient()
        try:
            await c.get_pre_signed_url("file:///x")
        except Exception:
            pass
        n = 0
        for t in os.listdir("/proc/self/task"):
            with open(f"/proc/self/task/{t}/comm") as f:
                n += f.read().startswith("tokio-rt-worker")
        print(n)

    asyncio.run(main())
    """
)


@pytest.fixture
def workers(run_isolated):
    def count(value: str | None) -> int:
        env = {} if value is None else {"OSC_WORKER_THREADS": value}
        out = run_isolated(_PROBE, env=env, timeout=60)
        assert out.returncode == 0, out.stderr
        return int(out.stdout.strip())

    return count


# One extra thread of the same name may exist: a task on the blocking pool.
def test_default_is_one_worker_on_one_cpu(workers):
    assert 1 <= workers(None) <= 2


@pytest.mark.parametrize("n", [1, 3])
def test_env_overrides_worker_count(workers, n):
    # Pinned to one CPU, so 3 workers can only come from the override.
    assert n <= workers(str(n)) <= n + 1


@pytest.mark.parametrize("bad", ["0", "-1", "many", ""])
def test_invalid_override_falls_back_to_default(workers, bad):
    assert 1 <= workers(bad) <= 2


_BURST = textwrap.dedent(
    """
    import asyncio, os, sys, time
    from object_storage_client import ObjectStorageClient

    async def main():
        c = ObjectStorageClient()
        urls = []
        for i in range(400):
            path = os.path.join(sys.argv[1], f"f{i}")
            open(path, "wb").close()
            urls.append("file://" + path)
        # Not awaited: all 400 results are delivered while the GIL is held.
        calls = [asyncio.ensure_future(c.object_exists(u)) for u in urls]
        sys.setswitchinterval(5)
        end = time.monotonic() + 1.0
        while time.monotonic() < end:
            pass
        n = 0
        for t in os.listdir("/proc/self/task"):
            with open(f"/proc/self/task/{t}/comm") as f:
                n += f.read().startswith("tokio-rt-worker")
        print(n)
        sys.setswitchinterval(0.005)
        assert all(await asyncio.gather(*calls))

    asyncio.run(main())
    """
)


def test_blocking_pool_is_capped(run_isolated, tmp_path):
    out = run_isolated(
        _BURST,
        str(tmp_path),
        env={"OSC_WORKER_THREADS": "2"},
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    count = int(out.stdout.strip())
    # The lower bound proves the burst piled up threads (not vacuous); the
    # upper bound is the 2 workers plus the 64 blocking threads.
    assert 10 < count <= 2 + 64
