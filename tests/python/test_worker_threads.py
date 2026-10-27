"""The async runtime is capped at a few worker threads, overridable by env."""

import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    not os.path.isdir("/proc/self/task"), reason="needs /proc"
)

# A pure-async call (it fails before any file access), so only the runtime's
# workers exist when the threads are counted.
_PROBE = textwrap.dedent(
    """
    import asyncio, os
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


def _workers(value: str | None) -> int:
    env = {k: v for k, v in os.environ.items() if k != "OSC_WORKER_THREADS"}
    if value is not None:
        env["OSC_WORKER_THREADS"] = value
    out = subprocess.run(
        [sys.executable, "-c", _PROBE],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return int(out.stdout.strip())


def _expected_default() -> int:
    return min(len(os.sched_getaffinity(0)), 4)


# One extra thread of the same name may exist: a task on the blocking pool.
def test_default_is_capped_at_four():
    want = _expected_default()
    assert want <= _workers(None) <= want + 1


@pytest.mark.parametrize("n", [1, 3])
def test_env_overrides_worker_count(n):
    assert n <= _workers(str(n)) <= n + 1


@pytest.mark.parametrize("bad", ["0", "-1", "many", ""])
def test_invalid_override_falls_back_to_default(bad):
    want = _expected_default()
    assert want <= _workers(bad) <= want + 1
