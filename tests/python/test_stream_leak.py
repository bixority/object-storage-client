"""A failed ByteStream read must not leak the stream or its descriptors."""

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
    for _ in range(50):
        await _fail_once(secure, url)
    gc.collect()
    assert _live_streams() == 0
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
