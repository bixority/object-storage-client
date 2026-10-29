"""A failed ByteStream read must not leak the stream or its descriptors."""

import asyncio
import contextlib
import gc
import os
import traceback

import pytest

from object_storage_client import DecryptionError


def _fds() -> int:
    return len(os.listdir("/proc/self/fd"))


def _live_streams() -> int:
    return sum(type(o).__name__ == "ByteStream" for o in gc.get_objects())


async def _tampered(secure, tmp_path, store) -> str:
    url = f"{store}/t.bin"
    await secure.put_object(url, b"payload" * 100)
    path = tmp_path / "store" / "t.bin"
    raw = bytearray(path.read_bytes())
    raw[-20] ^= 0x01
    path.write_bytes(bytes(raw))
    return url


async def _fail_once(secure, url):
    stream = await secure.get_object_stream(url)
    try:
        while await stream.next():
            pass
    except DecryptionError as e:
        return str(e)
    raise AssertionError("expected DecryptionError")


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
async def test_failed_reads_do_not_leak(secure, tmp_path, store):
    url = await _tampered(secure, tmp_path, store)
    await _fail_once(secure, url)  # warm up
    gc.collect()
    before = _fds()
    streams_before = _live_streams()
    for _ in range(50):
        await _fail_once(secure, url)
    gc.collect()
    assert _live_streams() == streams_before
    assert _fds() - before <= 3


async def test_retry_raises_fresh_equal_error_with_bounded_traceback(
    secure, tmp_path, store
):
    url = await _tampered(secure, tmp_path, store)
    stream = await secure.get_object_stream(url)
    with pytest.raises(DecryptionError) as first:
        while await stream.next():
            pass
    depths = []
    for _ in range(5):
        with pytest.raises(DecryptionError) as again:
            await stream.next()
        assert str(again.value) == str(first.value)
        assert again.value is not first.value
        depths.append(len(traceback.extract_tb(again.value.__traceback__)))
    assert len(set(depths)) == 1


@pytest.fixture
def big_url(tmp_path, store):
    (tmp_path / "store" / "big.bin").write_bytes(os.urandom(2_000_000))
    return f"{store}/big.bin"


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
async def test_aclose_releases_the_file_descriptor(plain, big_url):
    gc.collect()
    before = _fds()
    stream = await plain.get_object_stream(big_url)
    assert await stream.next()
    assert _fds() > before  # the open file is really held
    await stream.aclose()
    assert _fds() == before  # no gc.collect(): the release is deterministic


async def test_read_after_aclose_raises(plain, big_url):
    stream = await plain.get_object_stream(big_url)
    await stream.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await stream.next()
    with pytest.raises(RuntimeError, match="closed"):
        await stream.__anext__()
    await stream.aclose()  # idempotent


async def test_aclose_with_a_pending_read_raises(plain, big_url):
    stream = await plain.get_object_stream(big_url)
    pending = asyncio.ensure_future(stream.next())
    await asyncio.sleep(0)  # let the read start; it cannot finish before a yield
    assert not pending.done()
    with pytest.raises(RuntimeError, match="concurrent"):
        await stream.aclose()
    total = len(await pending)
    while chunk := await stream.next():
        total += len(chunk)
    assert total == 2_000_000


async def test_aclosing_context_manager(plain, big_url):
    async with contextlib.aclosing(await plain.get_object_stream(big_url)) as stream:
        assert await stream.next()
    with pytest.raises(RuntimeError, match="closed"):
        await stream.next()
