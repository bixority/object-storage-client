"""A failed raw stream must never later report a clean end of stream."""

import asyncio
import random

import pytest

from object_storage_client import DecryptionError

from test_stream_leak import _tampered


async def test_fill_racing_take_never_yields_eof_after_failure(
    secure, tmp_path, store
):
    url = await _tampered(secure, tmp_path, store)
    for _ in range(200):
        raw = (await secure.get_object_stream(url))._raw
        racers: list[asyncio.Future] = []
        failed = False
        for _ in range(100):
            if not raw.ready():
                await raw.fill()
            # Fills started right before and after take() run on tokio
            # workers while take() has the slot popped.
            racers.append(asyncio.ensure_future(raw.fill()))
            try:
                chunk = raw.take()
            except DecryptionError:
                failed = True
                racers.append(asyncio.ensure_future(raw.fill()))
                break
            assert chunk is not None, "clean EOF before the failure"
        assert failed
        await asyncio.gather(*racers)
        for _ in range(5):
            if not raw.ready():
                await raw.fill()
            try:
                result = raw.take()
            except DecryptionError:
                continue
            raise AssertionError(f"read after failure returned {result!r}")


async def test_error_after_buffered_chunks_delivers_chunks_then_raises(
    secure, tmp_path, store
):
    # The last frame is corrupted, so every earlier frame is verified and
    # delivered first (possibly joined into one read); the error then stays.
    data = bytes(i % 251 for i in range(1_000_000))
    url = f"{store}/late.bin"
    await secure.put_object(url, data)
    path = tmp_path / "store" / "late.bin"
    raw = bytearray(path.read_bytes())
    raw[-20] ^= 0x01
    path.write_bytes(bytes(raw))

    stream = await secure.get_object_stream(url)
    got = bytearray()
    with pytest.raises(DecryptionError) as first:
        while chunk := await stream.next():
            got += chunk
    assert got
    assert data.startswith(got)
    assert len(got) < len(data)
    for _ in range(3):
        with pytest.raises(DecryptionError) as again:
            await stream.next()
        assert str(again.value) == str(first.value)


async def test_cancel_mid_batch_keeps_bytes_contiguous(plain, tmp_url):
    data = bytes(i % 251 for i in range(3_000_000))
    url = f"{tmp_url}/batch.bin"
    await plain.put_object(url, data)
    rng = random.Random(7)
    for _ in range(40):
        stream = await plain.get_object_stream(url)
        got = bytearray()
        while True:
            # Cancel the read at a random point; whatever the fill had pulled
            # by then stays parked, in order, for the next read.
            task = asyncio.ensure_future(stream.next())
            await asyncio.sleep(rng.choice((0, 0, 0.0005, 0.002)))
            if rng.random() < 0.5:
                task.cancel()
            try:
                chunk = await task
            except asyncio.CancelledError:
                chunk = await stream.next()
            if not chunk:
                break
            got += chunk
        assert bytes(got) == data
