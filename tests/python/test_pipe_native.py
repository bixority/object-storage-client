"""A ByteStream passed to put_object_stream is piped natively and consumed."""

import asyncio
import os
import time

import pytest
from object_storage_client import DecryptionError

SIZE = 64 * 1024 * 1024


async def test_pipe_moves_bytes_while_the_loop_is_blocked(plain, tmp_path, store):
    # The event loop is blocked outright (no await), so anything that needs a
    # Python call per batch cannot move a byte; the native pipe needs none.
    data = os.urandom(1 << 20) * (SIZE >> 20)
    (tmp_path / "store" / "src.bin").write_bytes(data)
    stream = await plain.get_object_stream(f"{store}/src.bin")
    put = plain.put_object_stream(f"{store}/dst.bin", stream)

    def staged() -> int:
        files = list((tmp_path / "store").glob("dst.bin*"))
        return max((f.stat().st_size for f in files), default=0)

    deadline = time.monotonic() + 20
    while staged() < SIZE and time.monotonic() < deadline:
        # Blocks the loop on purpose: no callback runs meanwhile.
        time.sleep(0.01)  # noqa: ASYNC251
    assert staged() == SIZE, f"only {staged()} of {SIZE} bytes moved"
    await put
    assert (tmp_path / "store" / "dst.bin").read_bytes() == data


async def test_stream_is_closed_after_put(plain, tmp_path, store):
    (tmp_path / "store" / "src.bin").write_bytes(b"abc" * 1000)
    stream = await plain.get_object_stream(f"{store}/src.bin")
    await plain.put_object_stream(f"{store}/dst.bin", stream)
    with pytest.raises(RuntimeError, match="closed"):
        await stream.next()
    with pytest.raises(RuntimeError, match="closed"):
        await plain.put_object_stream(f"{store}/dst2.bin", stream)
    assert not (tmp_path / "store" / "dst2.bin").exists()


async def test_pending_read_is_refused_and_nothing_is_written(plain, tmp_path, store):
    (tmp_path / "store" / "src.bin").write_bytes(b"abc" * 1000)
    stream = await plain.get_object_stream(f"{store}/src.bin")
    reader = asyncio.ensure_future(stream.next())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="concurrent"):
        await plain.put_object_stream(f"{store}/dst.bin", stream)
    assert await reader == b"abc" * 1000
    assert not (tmp_path / "store" / "dst.bin").exists()


async def test_partly_read_stream_uploads_the_rest(plain, tmp_path, store):
    data = os.urandom(3_000_000)
    (tmp_path / "store" / "src.bin").write_bytes(data)
    stream = await plain.get_object_stream(f"{store}/src.bin")
    first = await stream.next()
    # A cancelled read leaves its chunks parked; the upload sends them too.
    task = asyncio.ensure_future(stream.next())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await plain.put_object_stream(f"{store}/dst.bin", stream)
    got = (tmp_path / "store" / "dst.bin").read_bytes()
    assert data.startswith(first)
    assert got == data[len(first) :]


async def test_source_error_fails_the_put_and_leaves_nothing(
    secure, plain, tmp_path, store
):
    await secure.put_object(f"{store}/src.bin", os.urandom(1_000_000))
    path = tmp_path / "store" / "src.bin"
    path.write_bytes(path.read_bytes()[:-100_000])
    stream = await secure.get_object_stream(f"{store}/src.bin")
    with pytest.raises(DecryptionError):
        await plain.put_object_stream(f"{store}/dst.bin", stream)
    assert sorted(p.name for p in (tmp_path / "store").iterdir()) == ["src.bin"]


async def test_a_stream_that_already_failed_never_pipes_a_clean_end(
    secure, plain, tmp_path, store
):
    # The Python read raised first; the pipe must raise again, not upload a
    # truncated object.
    await secure.put_object(f"{store}/src.bin", os.urandom(1_000_000))
    path = tmp_path / "store" / "src.bin"
    path.write_bytes(path.read_bytes()[:-100_000])
    stream = await secure.get_object_stream(f"{store}/src.bin")
    with pytest.raises(DecryptionError):
        while await stream.next():
            pass
    with pytest.raises(DecryptionError):
        await plain.put_object_stream(f"{store}/dst.bin", stream)
    assert sorted(p.name for p in (tmp_path / "store").iterdir()) == ["src.bin"]


async def test_encrypted_client_pipe_roundtrips(secure, store):
    data = os.urandom(2_000_000)
    await secure.put_object(f"{store}/a", data)
    stream = await secure.get_object_stream(f"{store}/a")
    await secure.put_object_stream(f"{store}/b", stream)
    assert await secure.get_object(f"{store}/b") == data
    with pytest.raises(RuntimeError):
        await stream.next()
