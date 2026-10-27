"""Unified object storage client.

Multiprocessing: use spawn/forkserver, or fork before the first use of any
client (the async runtime does not survive a fork). A client used in a child
forked after the runtime started raises ``RuntimeError`` instead of hanging.
"""

import os
from datetime import datetime
from collections.abc import Buffer
from typing import AsyncIterable, AsyncIterator, TypedDict

class EncryptionError(RuntimeError): ...
class DecryptionError(EncryptionError): ...
class UnknownKeyError(EncryptionError):
    """No key for the object is available. Derived from unauthenticated header
    data, so it may also indicate tampering."""

class NotEncryptedError(EncryptionError): ...

class EncryptionInfo(TypedDict):
    format: str
    message_id: str
    key_names: list[str]
    frame_length: int
    context: dict[str, str]

class ObjectMetadata(TypedDict):
    location: str
    last_modified: str
    size_bytes: int
    content_type: str | None
    e_tag: str | None
    version: str | None

class EncryptedObjectMetadata(ObjectMetadata):
    stored_size_bytes: int
    encrypted: bool
    encryption: EncryptionInfo

class KeyInfo(TypedDict):
    id: str
    created: str
    """Creation date, ``YYYY-MM-DD``."""

class KeyringStatus(TypedDict):
    active: str
    keys: list[KeyInfo]
    """Newest first."""
    active_age_days: int
    last_reload_error: str | None
    """Why the file on disk was not loaded; ``None`` while it is valid."""

class Keyring:
    """A reloadable set of key-encryption keys read from a file; key bytes never
    reach Python. The file is re-read on use when it changes."""
    @staticmethod
    def from_file(path: str | os.PathLike[str]) -> Keyring:
        """Load a keyring file. Raises ``EncryptionError`` if it is unreadable,
        accessible to others, or invalid."""
    def status(self) -> KeyringStatus: ...
    @property
    def wraps(self) -> int:
        """Wrap operations (one per object written) done with the active key by
        this keyring instance in this process. Not persisted; use it to rotate
        keys long before the random-IV bound of about 2**32 wraps per key."""
    def __repr__(self) -> str: ...

class ByteStream(AsyncIterator[bytes]):
    """Async byte-stream returned by :py:meth:`ObjectStorageClient.get_object_stream`.

    Reads follow the same rules as ``asyncio.Queue.get()``: they must not
    overlap (``RuntimeError``); a cancelled or timed-out read never loses data
    (the chunk is returned by the next read); each call returns a fresh
    coroutine that can be awaited once. To time out a read without cancelling
    it, wrap it in a Task (``task = asyncio.ensure_future(stream.next())``) and
    retry ``await asyncio.wait_for(asyncio.shield(task), t)``. After a read
    raises an error other than cancellation, every later read raises the same
    error; the stream never reports end-of-stream after a failure.
    """
    def __aiter__(self) -> AsyncIterator[bytes]: ...
    async def __anext__(self) -> bytes: ...
    async def next(self) -> bytes: ...

class ObjectStorageClient:
    def __init__(self) -> None: ...
    def with_encryption(
        self,
        keyring: Keyring,
        bind_object_key: bool = True,
        frame_length: int = 65536,
    ) -> EncryptedObjectStorageClient:
        """An encrypting view over the same stores and connections."""
    async def get_object(self, url: str) -> bytes:
        """Return the whole object as ``bytes``.

        Peak memory is about twice the object size while the result is copied
        into a Python ``bytes``. For large objects use :py:meth:`get_object_stream`,
        which keeps memory flat.
        """
    async def get_object_stream(self, url: str) -> ByteStream: ...
    async def put_object_stream(
        self,
        url: str,
        chunks: AsyncIterable[Buffer],
        *,
        size_hint: int | None = None,
    ) -> None:
        """Upload the bytes-like chunks of an async iterable.

        Chunks are pulled in small batches: the iterable is read ahead by at
        most 256 KiB (or 1024 chunks) past what the upload has consumed. Each
        chunk is copied when it is yielded, so a source may reuse one buffer.
        The object is created only if the iterable finishes cleanly; on any
        error or cancellation the upload is aborted and nothing is left behind.

        ``size_hint`` is the expected total size in bytes, if known (a negative
        value or one above 5 TiB raises ``ValueError``). A large hint makes the upload use bigger
        parts so that it stays within the store's part limit (about 78 GiB
        otherwise). It is a hint, not a limit.
        """
    async def put_object(self, url: str, data: Buffer | list[int]) -> None:
        """Upload ``data``: any bytes-like object (``bytes``, ``bytearray``,
        ``memoryview``, ``array.array``, ...) or a list of ints in 0..255. ``bytes``
        is shared without a copy; other buffers are copied once up front."""
    async def delete_object(self, url: str) -> None: ...
    async def delete_objects_by_prefix(self, url: str) -> None: ...
    async def list_objects(self, url: str) -> list[str]: ...
    async def get_object_metadata(self, url: str) -> ObjectMetadata: ...
    async def object_exists(self, url: str) -> bool: ...
    async def create_bucket(self, url: str) -> None: ...
    async def bucket_exists(self, url: str) -> bool: ...
    async def copy_object(self, from_url: str, to_url: str) -> None: ...
    async def move_object(self, from_url: str, to_url: str) -> None: ...
    async def get_pre_signed_url(
        self,
        url: str,
        method: str = "GET",
        expires_in_secs: int = 3600,
        content_length: int | None = None,
        content_type: str | None = None,
    ) -> str: ...
    async def get_pre_signed_urls(
        self,
        urls: list[str],
        method: str = "GET",
        expires_in_secs: int = 3600,
        content_length: int | None = None,
        content_type: str | None = None,
    ) -> list[str]: ...

class EncryptedObjectStorageClient:
    """Encrypts every object it writes and decrypts every object it reads.

    Data from ``get_object_stream`` is provisional until the iterator ends
    cleanly: truncation or tampering raises ``DecryptionError`` at the point it
    is detected. There are no pre-signed URL methods.
    """
    def __init__(
        self,
        keyring: Keyring,
        bind_object_key: bool = True,
        frame_length: int = 65536,
    ) -> None: ...
    async def reencrypt(
        self,
        url: str,
        from_key: str,
        *,
        before: datetime | None = None,
        dry_run: bool = False,
    ) -> str:
        """Rewrite the object under the active key if it is wrapped by
        ``from_key``. Returns ``"rewritten"``, ``"would-rewrite"``,
        ``"skipped"``, ``"not-encrypted"``, ``"after-cutoff"`` or ``"changed"``.
        ``before`` must be timezone-aware (``ValueError`` otherwise);
        ``from_key`` equal to the active key raises ``ValueError``."""
    def without_encryption(self) -> ObjectStorageClient: ...
    async def get_object(self, url: str) -> bytes:
        """Return the whole object as ``bytes``.

        Peak memory is about twice the object size while the result is copied
        into a Python ``bytes``. For large objects use :py:meth:`get_object_stream`,
        which keeps memory flat.
        """
    async def get_object_stream(self, url: str) -> ByteStream: ...
    async def put_object(
        self, url: str, data: Buffer | list[int], context: dict[str, str] | None = None
    ) -> None: ...
    async def put_object_stream(
        self,
        url: str,
        chunks: AsyncIterable[Buffer],
        context: dict[str, str] | None = None,
        *,
        size_hint: int | None = None,
    ) -> None:
        """Encrypt and upload the chunks of an async iterable, with the same
        bounded read-ahead and abort semantics as the plain client.
        ``size_hint`` is the expected plaintext size in bytes, if known (``ValueError`` if negative or above 5 TiB)."""
    async def inspect_encryption(self, url: str) -> EncryptionInfo: ...
    async def delete_object(self, url: str) -> None: ...
    async def delete_objects_by_prefix(self, url: str) -> None: ...
    async def list_objects(self, url: str) -> list[str]: ...
    async def get_object_metadata(self, url: str) -> EncryptedObjectMetadata: ...
    async def object_exists(self, url: str) -> bool: ...
    async def create_bucket(self, url: str) -> None: ...
    async def bucket_exists(self, url: str) -> bool: ...
    async def copy_object(self, from_url: str, to_url: str) -> None: ...
    async def move_object(self, from_url: str, to_url: str) -> None: ...
