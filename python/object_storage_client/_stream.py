"""Python-side wrapper for the native byte stream."""

from __future__ import annotations

import asyncio
import weakref
from typing import Any


# Cap on the items one batch pulls, for sources that yield tiny chunks.
_BATCH_ITEMS = 1024


async def _anext_batch(it: Any, limit: int) -> tuple[list[Any], bool]:
    """Pull items from the async iterator ``it`` until about ``limit`` bytes.

    Returns ``(items, done)``; ``done`` is true once the iterator is exhausted.
    This is the bounded read-ahead of ``put_object_stream``: at most ``limit``
    bytes (plus one item) and ``_BATCH_ITEMS`` items are pulled past what the
    uploader has consumed. Each item is converted to an immutable ``bytes`` as
    soon as it is yielded, because a source may reuse and refill one buffer
    for its next item. An item that is not bytes-like is passed through as is
    (the caller rejects it) and ends the batch. An error from the iterator
    propagates and drops the batch; the upload is aborted anyway.
    """
    items: list[Any] = []
    size = 0
    while size < limit and len(items) < _BATCH_ITEMS:
        aw = it.__anext__()
        if not hasattr(aw, "__await__"):
            raise TypeError(
                "put_object_stream: __anext__ returned non-awaitable "
                f"{type(aw).__name__}"
            )
        try:
            item = await aw
        except StopAsyncIteration:
            return items, True
        if type(item) is not bytes:
            try:
                item = bytes(memoryview(item))
            except TypeError:
                items.append(item)
                break
        items.append(item)
        size += len(item)
    return items, False


class _Stream:
    """Reading rules shared by ``ByteStream`` and ``ListStream``.

    Reads follow the same rules as ``asyncio.Queue.get()``:

    * Reads must not overlap: starting a read while another is pending raises
      ``RuntimeError``.
    * A cancelled or timed-out read never loses data: the item stays parked
      and is returned by the next read.
    * Each call returns a fresh coroutine that can be awaited once. To time out
      a read without cancelling it, wrap it in a Task first
      (``task = asyncio.ensure_future(stream.next())``;
      ``await asyncio.wait_for(asyncio.shield(task), t)``) and re-await the task.
    * After a read raises an error other than cancellation, every later read
      raises the same error; the stream never reports end-of-stream after a
      failure.
    """

    __slots__ = ("_raw", "_owner")

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        # Token of the pending read: a one-element list holding a weak
        # reference to the loop it runs on (a fresh list per read, because
        # CPython reuses weakref objects).
        self._owner: list[Any] | None = None

    # A copy would share the native stream under its own owner token and so
    # defeat the overlapping-read check.
    def __copy__(self) -> Any:
        raise TypeError(f"{type(self).__name__} cannot be copied")

    def __deepcopy__(self, memo: Any) -> Any:
        raise TypeError(f"{type(self).__name__} cannot be copied")

    def __reduce__(self) -> Any:
        raise TypeError(f"{type(self).__name__} cannot be copied or pickled")

    def __reduce_ex__(self, protocol: Any) -> Any:
        raise TypeError(f"{type(self).__name__} cannot be copied or pickled")

    def __aiter__(self) -> Any:
        return self

    async def _read(self) -> Any:
        token = self._acquire()
        try:
            # The fill future only signals that a chunk is parked; the chunk
            # is popped synchronously here, so cancellation cannot lose it.
            if not self._raw.ready():
                await self._raw.fill()
                if self._owner is not token:
                    raise RuntimeError(
                        f"{type(self).__name__}: read superseded by a read on "
                        "another event loop"
                    )
            return self._raw.take()
        finally:
            # An abandoned read that finalizes late must not release a newer
            # read's ownership.
            if self._owner is token:
                self._owner = None

    async def aclose(self) -> None:
        """Release the connection, file or listing now; later reads raise
        ``RuntimeError``.

        Idempotent. Raises ``RuntimeError`` while a read is pending.
        """
        self._check_idle()
        self._owner = None
        self._raw = None

    def _check_idle(self) -> None:
        if self._owner is not None:
            loop = self._owner[0]()
            # A pending read whose loop is gone, closed or not running is
            # treated as abandoned, and the new read takes over. If that loop
            # runs again, the superseded read raises instead of taking data.
            if loop is not None and loop.is_running() and not loop.is_closed():
                raise RuntimeError(
                    f"{type(self).__name__}: concurrent reads are not supported; "
                    "await the previous read first"
                )

    def _acquire(self) -> list[Any]:
        # A closed stream must raise, never report end-of-stream: a clean end
        # tells an encrypted reader that the object was complete.
        if self._raw is None:
            raise RuntimeError(f"{type(self).__name__} is closed")
        self._check_idle()
        token = [weakref.ref(asyncio.get_running_loop())]
        self._owner = token
        return token


class ByteStream(_Stream):
    """Async byte-stream returned by ``ObjectStorageClient.get_object_stream``.

    Use it directly in ``async for``::

        stream = await client.get_object_stream("s3://bucket/key")
        async for chunk in stream:
            process(chunk)

    Reads follow the rules of ``_Stream`` (see the type stub).
    """

    __slots__ = ()

    async def __anext__(self) -> bytes:
        chunk = await self._read()
        if chunk is None:
            raise StopAsyncIteration
        return chunk

    async def next(self) -> bytes:
        """Fetch the next chunk manually; returns empty ``bytes`` on EOF."""
        chunk = await self._read()
        return b"" if chunk is None else chunk


class ListStream(_Stream):
    """Async iterator of ``list[str]`` batches of names, returned by
    ``list_objects_stream``. Each batch holds at most 1024 names or 256 KiB of
    names, so memory stays bounded; it follows the same reading rules as
    ``ByteStream``::

        async for names in await client.list_objects_stream("s3://bucket/p/"):
            process(names)
    """

    __slots__ = ()

    async def __anext__(self) -> list[str]:
        names = await self._read()
        if names is None:
            raise StopAsyncIteration
        return names
