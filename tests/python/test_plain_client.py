import array
import asyncio
import gc
import sys
import time
import weakref
from datetime import datetime

import pytest

from object_storage_client import (
    ByteStream,
    EncryptedObjectStorageClient,
    ObjectStorageClient,
)


async def test_metadata_keys(plain, tmp_url):
    url = f"{tmp_url}/m.bin"
    await plain.put_object(url, b"12345")
    meta = await plain.get_object_metadata(url)
    for key in ("location", "last_modified", "size_bytes", "content_type", "e_tag", "version"):
        assert key in meta
    assert meta["size_bytes"] == 5
    assert meta["location"].endswith("m.bin")
    datetime.fromisoformat(meta["last_modified"])


def test_profile_argument_is_accepted():
    ObjectStorageClient()
    ObjectStorageClient(profile=None)
    ObjectStorageClient(profile="x")
    with pytest.raises(TypeError):
        ObjectStorageClient(profile=1)


@pytest.mark.parametrize("profile", ["", "  "])
def test_an_empty_profile_name_is_a_value_error(profile):
    with pytest.raises(ValueError, match="profile name is empty"):
        ObjectStorageClient(profile=profile)


async def test_profile_does_not_affect_file_urls(tmp_url):
    client = ObjectStorageClient(profile="x")
    await client.put_object(f"{tmp_url}/p.bin", b"abc")
    assert await client.get_object(f"{tmp_url}/p.bin") == b"abc"


async def test_put_object_refuses_a_url_ending_in_a_slash(plain, tmp_url):
    # The slash used to be dropped, so this wrote an object called "dir".
    with pytest.raises(ValueError):
        await plain.put_object(f"{tmp_url}/dir/", b"x")


async def test_presigned_url_unsupported_on_file(plain, tmp_url):
    with pytest.raises(RuntimeError, match="not supported"):
        await plain.get_pre_signed_url(f"{tmp_url}/a.bin")


@pytest.mark.parametrize("expires_in_secs", [0, 604801, 10**13])
@pytest.mark.parametrize("url", ["s3://b/k", "gs://b/k", "az://c/b"])
async def test_presign_expiry_out_of_range_is_value_error(plain, url, expires_in_secs):
    with pytest.raises(ValueError, match="7 days"):
        await plain.get_pre_signed_url(url, expires_in_secs=expires_in_secs)
    with pytest.raises(ValueError, match="7 days"):
        await plain.get_pre_signed_urls([url], expires_in_secs=expires_in_secs)


_ARGUMENT_ERRORS = {
    "unknown-method": lambda c, u: c.get_pre_signed_url("s3://b/k", method="FETCH"),
    "unsupported-scheme": lambda c, u: c.get_object("ftp://h/x"),
    "invalid-url": lambda c, u: c.get_object("http://[::1/x"),
    "no-urls": lambda c, u: c.get_pre_signed_urls([]),
    "mixed-origins": lambda c, u: c.get_pre_signed_urls([f"{u}/a", "s3://b/k"]),
    "negative-expiry": lambda c, u: c.get_pre_signed_url("s3://b/k", expires_in_secs=-1),
    "huge-expiry": lambda c, u: c.get_pre_signed_url("s3://b/k", expires_in_secs=2**64),
    "negative-content-length": lambda c, u: c.get_pre_signed_url(
        "s3://b/k", content_length=-1
    ),
}


@pytest.mark.parametrize("case", list(_ARGUMENT_ERRORS))
async def test_argument_errors_are_value_errors(plain, tmp_url, case):
    with pytest.raises(ValueError):
        await _ARGUMENT_ERRORS[case](plain, tmp_url)


_FORBIDDEN_SCRIPT = """
import asyncio, sys
from object_storage_client import ObjectStorageClient

url = f"http://127.0.0.1:{sys.argv[1]}/x"

async def main():
    client = ObjectStorageClient()
    for call in (client.get_object, client.get_object_metadata):
        try:
            await call(url)
        except PermissionError:
            print("PermissionError")
        except BaseException as e:
            print(f"{type(e).__name__}: {e}")
        else:
            print("no error")

asyncio.run(main())
"""


@pytest.mark.parametrize("status", [401, 403])
def test_forbidden_is_permission_error(run_isolated, status):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def _deny(self):
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_GET = do_HEAD = _deny

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        proc = run_isolated(_FORBIDDEN_SCRIPT, str(server.server_address[1]), timeout=60)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["PermissionError", "PermissionError"], proc.stdout


async def _collect(stream) -> list:
    return [chunk async for chunk in stream]


async def test_stream_async_for_terminates(plain, tmp_url):
    url = f"{tmp_url}/big.bin"
    data = bytes(i % 251 for i in range(200_000))
    await plain.put_object(url, data)
    stream = await plain.get_object_stream(url)
    chunks = await asyncio.wait_for(_collect(stream), 10)
    assert all(c is not None for c in chunks)
    assert b"".join(chunks) == data


async def test_stream_next_returns_empty_at_eof(plain, tmp_url):
    url = f"{tmp_url}/small.bin"
    await plain.put_object(url, b"abc")
    stream = await plain.get_object_stream(url)
    got = b""
    while chunk := await stream.next():
        got += chunk
    assert got == b"abc"
    assert await stream.next() == b""
    assert await stream.next() == b""


async def test_stream_empty_object_yields_no_chunks(plain, tmp_url):
    url = f"{tmp_url}/empty.bin"
    await plain.put_object(url, b"")
    stream = await plain.get_object_stream(url)
    assert await asyncio.wait_for(_collect(stream), 10) == []


async def test_stream_anext_after_end_keeps_raising(plain, tmp_url):
    url = f"{tmp_url}/s.bin"
    await plain.put_object(url, b"abc")
    stream = await plain.get_object_stream(url)
    await asyncio.wait_for(_collect(stream), 10)
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()


async def test_stream_missing_object_raises_at_get_object_stream(plain, tmp_url):
    with pytest.raises(FileNotFoundError):
        await plain.get_object_stream(f"{tmp_url}/missing.bin")


async def _big_stream(plain, tmp_url):
    url = f"{tmp_url}/big.bin"
    data = bytes(i % 251 for i in range(2_000_000))
    await plain.put_object(url, data)
    return data, await plain.get_object_stream(url)


async def _drain(stream):
    return b"".join([c async for c in stream])


async def test_concurrent_read_rejected(plain, tmp_url):
    # (g) overlapping awaited reads: exactly one "concurrent" error and the
    # reader that then continues loses nothing.
    data, stream = await _big_stream(plain, tmp_url)
    first = await stream.next()
    r1, r2 = await asyncio.gather(
        stream.next(), stream.next(), return_exceptions=True
    )
    errors = [r for r in (r1, r2) if isinstance(r, BaseException)]
    chunks = [r for r in (r1, r2) if isinstance(r, bytes)]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "concurrent" in str(errors[0])
    assert len(chunks) == 1
    assert first + chunks[0] + await _drain(stream) == data


async def test_cancelled_read_loses_nothing(plain, tmp_url):
    data, stream = await _big_stream(plain, tmp_url)
    t = asyncio.ensure_future(stream.__anext__())
    await asyncio.sleep(0)
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    del t
    assert await _drain(stream) == data


@pytest.mark.parametrize("method", ["next", "__anext__"])
async def test_timeout_retry_with_loop_stall_loses_nothing(plain, tmp_url, method):
    # (a) asyncio.timeout + loop stall + retry with fresh reads.
    data, stream = await _big_stream(plain, tmp_url)
    got = b""
    timeouts = 0

    async def staller():
        await asyncio.sleep(0)
        time.sleep(0.3)
        await asyncio.sleep(0)

    st = asyncio.ensure_future(staller())
    for _ in range(10000):
        try:
            async with asyncio.timeout(0.05):
                chunk = await getattr(stream, method)()
        except TimeoutError:
            timeouts += 1
            continue
        except StopAsyncIteration:
            break
        if not chunk:
            break
        got += chunk
    await st
    # The stall blocks the loop while the first read's fill is pending, so the
    # first read must have timed out; without it this test proves nothing.
    assert timeouts > 0
    assert got == data


async def test_cancel_after_fill_resolved_before_resume_loses_nothing(
    plain, tmp_url
):
    # (b) the fill finished while the loop was blocked; the task is cancelled
    # before it resumes.
    data, stream = await _big_stream(plain, tmp_url)
    t = asyncio.ensure_future(stream.next())
    await asyncio.sleep(0)  # task is now awaiting fill()
    time.sleep(0.3)  # fill completes in the background, loop is blocked
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert await _drain(stream) == data


async def test_reawaiting_timed_out_read_is_refused_and_loses_nothing(
    plain, tmp_url
):
    # (c) a read is a plain coroutine: re-awaiting it after a timeout raises
    # Python's own error, and a fresh read loses nothing.
    data, stream = await _big_stream(plain, tmp_url)
    read = stream.next()
    got = b""
    try:
        got = await asyncio.wait_for(read, 1e-9)
    except TimeoutError:
        with pytest.raises(RuntimeError, match="cannot reuse already awaited"):
            await read
    assert got + await _drain(stream) == data


async def test_task_shield_idiom_with_loop_stall_makes_progress(plain, tmp_url):
    # A finished Task is returned as-is by shield(), so a stalled loop cannot
    # starve the documented idiom.
    data, stream = await _big_stream(plain, tmp_url)
    got = b""
    deadline = time.monotonic() + 60
    done = False
    while not done and time.monotonic() < deadline:
        task = asyncio.ensure_future(stream.next())
        while time.monotonic() < deadline:
            try:
                time.sleep(0.002)
                chunk = await asyncio.wait_for(asyncio.shield(task), 5e-4)
            except TimeoutError:
                continue
            done = not chunk
            got += chunk
            break
    assert done
    assert got == data


async def test_task_shield_idiom_with_retries_reassembles(plain, tmp_url):
    # (d) documented idiom: wrap the read in a Task, shield it, re-await it.
    data, stream = await _big_stream(plain, tmp_url)
    got = b""
    while True:
        task = asyncio.ensure_future(stream.next())
        while True:
            try:
                chunk = await asyncio.wait_for(asyncio.shield(task), 1e-4)
                break
            except TimeoutError:
                continue
        if not chunk:
            break
        got += chunk
    assert got == data


async def test_prefetch_with_cpu_work_has_no_spurious_concurrency_error(
    plain, tmp_url
):
    # (e)
    data, stream = await _big_stream(plain, tmp_url)
    got = b""
    pending = stream.next()
    while True:
        time.sleep(0.001)
        chunk = await pending
        if not chunk:
            break
        pending = stream.next()
        time.sleep(0.001)
        got += chunk
        await asyncio.sleep(0)
    assert got == data


@pytest.mark.filterwarnings("ignore:coroutine .* was never awaited:RuntimeWarning")
async def test_unawaited_read_releases_stream(plain, tmp_url):
    # (f)
    data, stream = await _big_stream(plain, tmp_url)
    stream.next()
    stream.__anext__()
    assert await _drain(stream) == data


async def test_sequential_reads_after_completed_read_work(plain, tmp_url):
    data, stream = await _big_stream(plain, tmp_url)
    first = await stream.__anext__()
    second = await stream.next()
    rest = b"".join([c async for c in stream])
    assert first + second + rest == data


async def test_tiny_timeout_retry_makes_progress(plain, tmp_url):
    # (h) parked-chunk fast path: no livelock with tiny timeouts.
    data, stream = await _big_stream(plain, tmp_url)
    got = b""
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            chunk = await asyncio.wait_for(stream.next(), 1e-5)
        except TimeoutError:
            continue
        if not chunk:
            break
        got += chunk
    else:
        pytest.fail("no progress within time budget")
    assert got == data


def _start_pending_read(loop, stream):
    task = asyncio.ensure_future(stream.next(), loop=loop)
    loop.run_until_complete(asyncio.sleep(0))
    if task.done():
        raise AssertionError("read finished too early")
    return task


def _forgive(task):
    """Mark an intentionally abandoned task so its destruction is not logged
    as "Task was destroyed but it is pending"; it changes no behaviour."""
    task._log_destroy_pending = False


def test_loop_closed_with_read_pending_then_drain_on_new_loop(tmp_path):
    # (i)
    data = bytes(i % 251 for i in range(2_000_000))
    url = (tmp_path / "big.bin").as_uri()
    plain = ObjectStorageClient()
    loop1 = asyncio.new_event_loop()
    try:

        async def setup():
            await plain.put_object(url, data)
            return await plain.get_object_stream(url)

        stream = loop1.run_until_complete(setup())
        # Keep a reference: the abandoned task must not need GC to release
        # the stream.
        abandoned = _start_pending_read(loop1, stream)
    finally:
        loop1.close()
    assert not abandoned.done()
    _forgive(abandoned)
    # Reusable immediately: no gc, no clearing of sys.last_*.

    async def drain():
        return await asyncio.wait_for(_drain(stream), 30)

    loop2 = asyncio.new_event_loop()
    try:
        assert loop2.run_until_complete(drain()) == data
    finally:
        loop2.close()


def test_superseded_read_on_resumed_loop_raises_and_loses_nothing(tmp_path):
    data = bytes(i % 251 for i in range(2_000_000))
    url = (tmp_path / "big.bin").as_uri()
    plain = ObjectStorageClient()
    loop_a = asyncio.new_event_loop()
    loop_b = asyncio.new_event_loop()
    try:

        async def setup():
            await plain.put_object(url, data)
            return await plain.get_object_stream(url)

        stream = loop_a.run_until_complete(setup())
        # A read waits in fill() on loop A, which then stops running.
        stale = _start_pending_read(loop_a, stream)

        # Loop B takes over and reads the first chunk.
        first = loop_b.run_until_complete(stream.next())

        async def park_a_chunk():
            t = asyncio.ensure_future(stream.next())
            await asyncio.sleep(0)  # awaiting fill()
            time.sleep(0.3)  # the fill completes while the loop is blocked
            t.cancel()  # the chunk stays parked for the next read
            await asyncio.gather(t, return_exceptions=True)

        loop_b.run_until_complete(park_a_chunk())

        # Loop A runs again: the stale read must raise, not take the chunk.
        (outcome,) = loop_a.run_until_complete(
            asyncio.gather(stale, return_exceptions=True)
        )
        assert isinstance(outcome, RuntimeError)
        assert "superseded" in str(outcome)

        rest = loop_b.run_until_complete(_drain(stream))
        assert first + rest == data
    finally:
        loop_a.close()
        loop_b.close()


class _FakeRaw:
    """Raw-stream stand-in: yields scripted items, then EOF."""

    def __init__(self, items):
        self._items = list(items)
        self._slot = None

    def ready(self):
        return self._slot is not None

    async def fill(self):
        if self._slot is None:
            self._slot = self._items.pop(0) if self._items else None
            if self._slot is None:
                self._slot = b""

    def take(self):
        item, self._slot = self._slot, None
        if isinstance(item, BaseException):
            raise item
        return item or None


async def test_midstream_error_surfaces_once_and_maps():
    # (j) wrapper-level: an error taken from the slot is raised exactly once.
    stream = ByteStream(
        _FakeRaw([b"a", FileNotFoundError("gone"), RuntimeError("boom"), b"z"])
    )
    assert await stream.next() == b"a"
    with pytest.raises(FileNotFoundError):
        await stream.next()
    with pytest.raises(RuntimeError, match="boom"):
        await stream.next()
    assert await stream.next() == b"z"
    assert await stream.next() == b""


async def test_take_without_fill_is_runtime_error(plain, tmp_url):
    _, stream = await _big_stream(plain, tmp_url)
    with pytest.raises(RuntimeError, match="no item is ready"):
        stream._raw.take()


async def _until(predicate, what):
    """Poll until `predicate()` holds, failing after a 10 s deadline."""
    deadline = time.monotonic() + 10
    while not predicate():
        assert time.monotonic() < deadline, what
        await asyncio.sleep(0.01)


def _live(*names):
    gc.collect()
    return {
        n: sum(1 for o in gc.get_objects() if type(o).__name__ == n) for n in names
    }


async def test_many_sequential_reads_do_not_leak(plain, tmp_url):
    # (k)
    url = f"{tmp_url}/small.bin"
    await plain.put_object(url, b"x" * 100)
    names = ("ByteStream", "_RawByteStream", "Task")
    before = _live(*names)
    for _ in range(100):
        stream = await plain.get_object_stream(url)
        for _ in range(200):
            await stream.next()
    del stream
    after = _live(*names)
    for n in names:
        assert after[n] <= before[n] + 2, (n, before, after)


def test_unclosed_abandoned_loop_does_not_wedge_stream(tmp_path):
    data = bytes(i % 251 for i in range(2_000_000))
    url = (tmp_path / "big.bin").as_uri()
    plain = ObjectStorageClient()

    async def setup():
        await plain.put_object(url, data)
        return await plain.get_object_stream(url)

    loop = asyncio.new_event_loop()
    stream = loop.run_until_complete(setup())
    task = asyncio.ensure_future(stream.next(), loop=loop)
    loop.run_until_complete(asyncio.sleep(0))
    _forgive(task)
    del task
    ref = weakref.ref(loop)
    del loop  # dropped without close()
    for _ in range(100):  # the native fill task may still hold the loop briefly
        gc.collect()
        if ref() is None:
            break
        time.sleep(0.05)
    assert ref() is None  # the stream must not pin the abandoned loop

    async def drain():
        out = b""
        async for chunk in stream:
            out += chunk
        return out

    got = asyncio.run(drain())
    assert data.endswith(got) and len(got) >= len(data) - 1_000_000


def test_abandoned_loop_still_referenced_does_not_wedge_stream(tmp_path):
    # A loop that is dropped mid-read but kept alive (not closed, not running)
    # must not need garbage collection before the stream works elsewhere.
    data = bytes(i % 251 for i in range(2_000_000))
    url = (tmp_path / "big.bin").as_uri()
    plain = ObjectStorageClient()

    async def setup():
        await plain.put_object(url, data)
        return await plain.get_object_stream(url)

    loop = asyncio.new_event_loop()
    try:
        stream = loop.run_until_complete(setup())
        pending = _start_pending_read(loop, stream)

        async def drain():
            return await asyncio.wait_for(_drain(stream), 30)

        got = asyncio.run(drain())
        assert data.endswith(got) and len(got) >= len(data) - 1_000_000
        assert not pending.done()
        # Finalize the abandoned read on its own loop before closing it.
        pending.cancel()
        loop.run_until_complete(asyncio.gather(pending, return_exceptions=True))
    finally:
        loop.close()


async def test_put_object_stream_roundtrip(plain, tmp_url):
    import os
    import random

    rng = random.Random(1)
    chunks = [os.urandom(rng.randint(0, 200 * 1024)) for _ in range(300)]

    async def gen():
        for c in chunks:
            yield c

    url = f"{tmp_url}/s.bin"
    await plain.put_object_stream(url, gen())
    assert await plain.get_object(url) == b"".join(chunks)


async def test_put_object_stream_accepts_bytearray_and_memoryview(plain, tmp_url):
    async def gen():
        yield bytearray(b"ab")
        yield memoryview(b"cd")
        yield b"ef"

    url = f"{tmp_url}/bm.bin"
    await plain.put_object_stream(url, gen())
    assert await plain.get_object(url) == b"abcdef"


async def test_put_object_stream_empty(plain, tmp_url):
    async def gen():
        return
        yield

    url = f"{tmp_url}/e.bin"
    await plain.put_object_stream(url, gen())
    assert await plain.get_object(url) == b""
    assert (await plain.get_object_metadata(url))["size_bytes"] == 0


async def test_put_object_stream_pipe_from_get_stream(plain, tmp_url):
    src, dst = f"{tmp_url}/src.bin", f"{tmp_url}/dst.bin"
    data = bytes(range(256)) * 40000
    await plain.put_object(src, data)
    await plain.put_object_stream(dst, await plain.get_object_stream(src))
    assert await plain.get_object(dst) == data


async def test_put_object_stream_iterator_raises_no_object(plain, tmp_url, tmp_path):
    async def gen():
        for _ in range(3):
            yield b"x" * 1024
        raise ValueError("boom")

    url = f"{tmp_url}/r.bin"
    with pytest.raises(ValueError, match="boom"):
        await plain.put_object_stream(url, gen())
    assert not await plain.object_exists(url)
    await _until(lambda: list(tmp_path.iterdir()) == [], "staging file left behind")


async def test_put_object_stream_error_after_multipart_started_leaves_nothing(
    plain, tmp_url, tmp_path
):
    async def gen():
        for _ in range(10):
            yield b"x" * (1024 * 1024)
        raise ValueError("late boom")

    with pytest.raises(ValueError, match="late boom"):
        await plain.put_object_stream(f"{tmp_url}/m.bin", gen())
    await _until(lambda: list(tmp_path.iterdir()) == [], "staging file left behind")


@pytest.mark.parametrize("size", [0, 1])
async def test_put_object_stream_pipe_tiny_object(plain, tmp_url, size):
    src, dst = f"{tmp_url}/src.bin", f"{tmp_url}/dst.bin"
    await plain.put_object(src, b"z" * size)
    await plain.put_object_stream(dst, await plain.get_object_stream(src))
    assert await plain.get_object(dst) == b"z" * size


async def test_put_object_stream_aiter_raises(plain, tmp_url):
    class Bad:
        def __aiter__(self):
            raise ValueError("no iterator")

    url = f"{tmp_url}/ai.bin"
    with pytest.raises(ValueError, match="no iterator"):
        await plain.put_object_stream(url, Bad())
    assert not await plain.object_exists(url)


async def test_put_object_stream_cancelled_error_from_iterator(plain, tmp_url):
    async def gen():
        yield b"x" * 1024
        raise asyncio.CancelledError

    url = f"{tmp_url}/ce.bin"
    with pytest.raises(BaseException) as info:
        await asyncio.wait_for(plain.put_object_stream(url, gen()), 5)
    assert not isinstance(info.value, TimeoutError)
    assert not await plain.object_exists(url)


async def test_put_object_stream_piped_stream_error_keeps_type(plain, tmp_url):
    stream = ByteStream(_FakeRaw([b"a", FileNotFoundError("gone")]))
    url = f"{tmp_url}/out.bin"
    with pytest.raises(FileNotFoundError, match="gone"):
        await plain.put_object_stream(url, stream)
    assert not await plain.object_exists(url)


@pytest.mark.parametrize("bad", ["str", 1])
async def test_put_object_stream_wrong_chunk_type(plain, tmp_url, bad):
    async def gen():
        yield b"ok"
        yield bad

    url = f"{tmp_url}/t.bin"
    with pytest.raises(TypeError, match=f"got {type(bad).__name__}") as info:
        await plain.put_object_stream(url, gen())
    assert repr(bad) not in str(info.value).replace(f"got {type(bad).__name__}", "")
    assert not await plain.object_exists(url)


async def _refcount_gaps(plain, url, size, count):
    """Upload `count` chunks of `size` bytes; return the stored size and, for
    every request after the first, how much more the source's previous chunk
    is referenced than an identical chunk that never left this function."""
    held = []  # the chunks handed to the uploader
    control = []  # same-size chunks that nothing else ever sees
    gaps = []

    class Source:
        def __aiter__(self):
            return self

        async def __anext__(self):
            if held:
                # Same access form for both, so the counts are comparable.
                gaps.append(sys.getrefcount(held[-1]) - sys.getrefcount(control[-1]))
            await asyncio.sleep(0)
            if len(held) == count:
                raise StopAsyncIteration
            # A runtime size, so these are fresh objects, never constants.
            held.append(b"x" * size)
            control.append(b"x" * size)
            return held[-1]

    await plain.put_object_stream(url, Source())
    return (await plain.get_object_metadata(url))["size_bytes"], gaps


async def test_put_object_stream_no_prefetch_beyond_batch(plain, tmp_url):
    # Read-ahead is bounded by the batch size (256 KiB): chunks that fill a
    # batch on their own are requested only after the previous one is consumed
    # and released by the uploader.
    size = 300 * 1024
    stored, gaps = await _refcount_gaps(plain, f"{tmp_url}/p.bin", size, 6)
    assert stored == 6 * size
    assert len(gaps) == 6
    # At most the previous chunk may still be referenced when the next one is
    # requested: the uploader releases it asynchronously, and on Windows that
    # takes a moment longer. Two chunks held would be read-ahead.
    assert all(g <= 1 for g in gaps), gaps


async def test_put_object_stream_refcount_probe_detects_a_holder(plain, tmp_url):
    # Negative control for the test above: with several chunks per batch the
    # batch still holds the previous chunk at the next request, and the probe
    # must see that.
    size = 100 * 1024
    stored, gaps = await _refcount_gaps(plain, f"{tmp_url}/q.bin", size, 12)
    assert stored == 12 * size
    assert max(gaps) > 0


async def test_put_object_stream_reused_buffer_is_snapshotted(plain, tmp_url):
    # A source may refill one buffer for its next item; the batch must have
    # copied the previous contents by then.
    buf = bytearray(100)

    async def gen():
        for i in range(50):
            buf[:] = bytes([i]) * 100
            yield buf
            await asyncio.sleep(0)

    url = f"{tmp_url}/reuse.bin"
    await plain.put_object_stream(url, gen())
    assert await plain.get_object(url) == b"".join(bytes([i]) * 100 for i in range(50))


async def test_put_object_stream_many_small_chunks(plain, tmp_url):
    async def gen():
        for i in range(5000):
            yield bytes([i % 256])

    url = f"{tmp_url}/tiny.bin"
    await plain.put_object_stream(url, gen())
    assert await plain.get_object(url) == bytes(i % 256 for i in range(5000))


async def test_put_object_stream_error_after_batched_items_leaves_nothing(
    plain, tmp_url
):
    async def gen():
        for _ in range(10):
            yield b"x" * 100
        raise ValueError("late")

    url = f"{tmp_url}/late.bin"
    with pytest.raises(ValueError, match="late"):
        await plain.put_object_stream(url, gen())
    assert not await plain.object_exists(url)


async def test_cancel_put_object_stream_leaves_no_object(plain, tmp_url, tmp_path):
    async def gen():
        while True:
            yield b"x" * (1024 * 1024)
            await asyncio.sleep(0.01)

    url = f"{tmp_url}/c.bin"
    t = asyncio.ensure_future(plain.put_object_stream(url, gen()))
    # Cancel only once the upload has really started writing a staging file.
    await _until(lambda: list(tmp_path.iterdir()) != [], "no staging file appeared")
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert not await plain.object_exists(url)
    await _until(lambda: list(tmp_path.iterdir()) == [], "staging file left behind")


async def test_put_object_stream_cancel_cancels_pending_anext(plain, tmp_url):
    log = []

    async def gen():
        try:
            yield b"x" * 1024
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            log.append("cancelled")
            raise
        finally:
            log.append("finally")

    g = gen()
    t = asyncio.ensure_future(plain.put_object_stream(f"{tmp_url}/g.bin", g))
    await asyncio.sleep(0.3)
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    await asyncio.sleep(0.1)
    assert log == ["cancelled", "finally"]
    await g.aclose()


async def test_put_object_stream_non_awaitable_anext(plain, tmp_url):
    class Bad:
        def __aiter__(self):
            return self

        def __anext__(self):
            return b"x"

    url = f"{tmp_url}/na.bin"
    with pytest.raises(TypeError):
        await asyncio.wait_for(plain.put_object_stream(url, Bad()), 5)
    assert not await plain.object_exists(url)


def test_byte_stream_cannot_be_copied_or_pickled():
    import copy
    import pickle

    stream = ByteStream(_FakeRaw([b"a"]))
    for op in (
        copy.copy,
        copy.deepcopy,
        pickle.dumps,
        lambda s: pickle.dumps(s, protocol=0),
        lambda s: s.__reduce_ex__(2),
        lambda s: s.__reduce__(),
    ):
        with pytest.raises(TypeError):
            op(stream)


@pytest.mark.parametrize(
    "make",
    [
        bytes,
        bytearray,
        memoryview,
        lambda b: memoryview(b)[::1],
        lambda b: array.array("B", b),
        lambda b: list(b),
    ],
    ids=["bytes", "bytearray", "memoryview", "slice", "array_B", "list"],
)
async def test_put_object_accepts_buffers(plain, tmp_url, make):
    data = bytes(range(256)) * 4
    url = f"{tmp_url}/buf.bin"
    await plain.put_object(url, make(data))
    assert await plain.get_object(url) == data


async def test_put_object_non_byte_array_uses_raw_bytes(plain, tmp_url):
    arr = array.array("I", [1, 2, 3])
    url = f"{tmp_url}/arr.bin"
    await plain.put_object(url, arr)
    assert await plain.get_object(url) == arr.tobytes()


async def test_put_object_memoryview_is_snapshotted(plain, tmp_url):
    buf = bytearray(b"a" * 1000)
    url = f"{tmp_url}/snap.bin"
    task = asyncio.ensure_future(plain.put_object(url, memoryview(buf)))
    buf[:] = b"b" * 1000
    await task
    assert await plain.get_object(url) == b"a" * 1000


@pytest.mark.parametrize("bad", ["str", 5, None, [256], [1.5]])
async def test_put_object_rejects_non_buffers(plain, tmp_url, bad):
    with pytest.raises(TypeError, match="bytes-like"):
        await plain.put_object(f"{tmp_url}/bad.bin", bad)
    assert not await plain.object_exists(f"{tmp_url}/bad.bin")


async def test_put_object_stream_accepts_array(plain, tmp_url):
    async def gen():
        yield array.array("B", b"ab")
        yield b"cd"

    url = f"{tmp_url}/arr2.bin"
    await plain.put_object_stream(url, gen())
    assert await plain.get_object(url) == b"abcd"


@pytest.mark.parametrize("size_hint", [0, 3, 10**12, None])
async def test_put_object_stream_size_hint_roundtrip(any_client, tmp_url, size_hint):
    # A hint (right, wrong or huge) never changes what is stored.
    async def gen():
        yield b"abc"
        yield b"def"

    url = f"{tmp_url}/h.bin"
    await any_client.put_object_stream(url, gen(), size_hint=size_hint)
    assert await any_client.get_object(url) == b"abcdef"


async def test_put_object_stream_size_hint_is_validated(any_client, tmp_url):
    async def gen():
        yield b"abc"

    url = f"{tmp_url}/v.bin"
    with pytest.raises(ValueError, match="size_hint"):
        await any_client.put_object_stream(url, gen(), size_hint=-1)
    # Above 5 TiB: rejected up front, never sized into a buffer.
    for big in (5 * 2**40 + 1, 2**62, 2**64):
        with pytest.raises(ValueError, match="size_hint"):
            await any_client.put_object_stream(url, gen(), size_hint=big)
    with pytest.raises(TypeError):
        await any_client.put_object_stream(url, gen(), size_hint="big")
    with pytest.raises(TypeError):
        await any_client.put_object_stream(url, gen(), size_hint=1.5)
    # Keyword-only: the encrypted client takes `context` before it, so give
    # that a value and put the hint in the next positional slot.
    if isinstance(any_client, EncryptedObjectStorageClient):
        positional = (None, 3)
    else:
        positional = (3,)
    with pytest.raises(TypeError):
        await any_client.put_object_stream(url, gen(), *positional)
    assert not await any_client.object_exists(url)
