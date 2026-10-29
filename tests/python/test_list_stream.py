"""list_objects_stream: bounded batches over a file:// listing, on both clients."""

import asyncio
import sys

import pytest

from object_storage_client import ListStream

BATCH_NAMES = 1024
BATCH_BYTES = 256 * 1024


async def _put_all(client, prefix: str, names: list[str]) -> None:
    gate = asyncio.Semaphore(64)

    async def put(name: str) -> None:
        async with gate:
            await client.put_object(f"{prefix}/{name}", b"x")

    await asyncio.gather(*(put(n) for n in names))


@pytest.fixture
async def listing(any_client, store):
    """A prefix holding 5,000 objects in ten subdirectories."""
    prefix = f"{store}/data"
    names = [f"dir{i % 10}/obj-{i:05d}" for i in range(5000)]
    await _put_all(any_client, prefix, names)
    return any_client, f"{prefix}/", set(names)


def _within_limits(batch: list[str]) -> bool:
    return (
        bool(batch)
        and len(batch) <= BATCH_NAMES
        and sum(len(n.encode()) for n in batch) <= BATCH_BYTES
    )


async def test_every_name_comes_out_once_in_bounded_batches(listing):
    client, prefix, names = listing
    stream = await client.list_objects_stream(prefix)
    assert isinstance(stream, ListStream)
    batches = [batch async for batch in stream]
    assert all(type(b) is list for b in batches)
    assert all(_within_limits(b) for b in batches)
    assert len(batches) > 1
    got = [n for b in batches for n in b]
    assert len(got) == len(names)
    assert set(got) == names
    # The same names as the unchanged list_objects.
    assert sorted(got) == sorted(await client.list_objects(prefix))


async def test_long_names_are_bounded_by_bytes(any_client, store):
    # About 330 bytes each, so 256 KiB is reached long before 1024 names.
    names = [f"{'d' * 120}/{'f' * 200}{i:04d}" for i in range(1500)]
    await _put_all(any_client, f"{store}/long", names)
    batches = [b async for b in await any_client.list_objects_stream(f"{store}/long/")]
    assert all(_within_limits(b) for b in batches)
    assert max(len(b) for b in batches) < BATCH_NAMES
    assert sorted(n for b in batches for n in b) == sorted(names)


async def test_an_empty_prefix_ends_at_once(any_client, store):
    stream = await any_client.list_objects_stream(f"{store}/nothing/")
    assert [b async for b in stream] == []


async def test_a_cancelled_read_loses_no_names(listing):
    client, prefix, names = listing
    stream = await client.list_objects_stream(prefix)
    got: list[str] = []
    cancelled = 0
    while True:
        # One yield lets the read start and wait for the listing; it is
        # cancelled before the loop can deliver the result.
        read = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0)
        read.cancel()
        try:
            got += await read
        except asyncio.CancelledError:
            cancelled += 1
        except StopAsyncIteration:
            break
        try:
            got += await stream.__anext__()
        except StopAsyncIteration:
            break
    assert cancelled >= 1
    assert len(got) == len(names)
    assert set(got) == names


async def test_overlapping_reads_raise(listing):
    client, prefix, _ = listing
    stream = await client.list_objects_stream(prefix)
    first = asyncio.ensure_future(stream.__anext__())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="concurrent reads"):
        await stream.__anext__()
    assert _within_limits(await first)


@pytest.mark.skipif(
    sys.platform != "linux", reason="needs a file name that is not valid UTF-8"
)
async def test_an_error_is_raised_on_every_read_that_reaches_it(
    any_client, store, tmp_path
):
    names = [f"obj-{i:04d}" for i in range(3000)]
    await _put_all(any_client, f"{store}/bad", names)
    # object_store refuses a file name that is not UTF-8, wherever the
    # directory order puts it.
    bad = b"bad-\xff".decode("utf-8", "surrogateescape")
    (tmp_path / "store" / "bad" / bad).write_bytes(b"x")
    stream = await any_client.list_objects_stream(f"{store}/bad/")
    got: list[str] = []
    with pytest.raises(RuntimeError, match="non-unicode") as first:
        async for batch in stream:
            got += batch
    assert len(got) == len(set(got))
    assert set(got) <= set(names)
    for _ in range(3):
        with pytest.raises(RuntimeError, match="non-unicode") as again:
            await stream.__anext__()
        assert str(again.value) == str(first.value)


async def test_aclose_stops_the_listing(listing):
    client, prefix, names = listing
    stream = await client.list_objects_stream(prefix)
    first = await stream.__anext__()
    await stream.aclose()
    await stream.aclose()
    for _ in range(2):
        with pytest.raises(RuntimeError, match="ListStream is closed"):
            await stream.__anext__()
    assert 0 < len(first) < len(names)


async def test_aclose_before_any_read(listing):
    client, prefix, _ = listing
    stream = await client.list_objects_stream(prefix)
    await stream.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await stream.__anext__()


async def test_aclose_during_a_pending_read_raises(listing):
    client, prefix, names = listing
    stream = await client.list_objects_stream(prefix)
    read = asyncio.ensure_future(stream.__anext__())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="concurrent reads"):
        await stream.aclose()
    assert _within_limits(await read)
