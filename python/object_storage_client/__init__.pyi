"""Unified object storage client.

Multiprocessing: use spawn/forkserver, or fork before the first client call
(the async runtime does not survive a fork). A client used in a child forked
after the runtime started raises ``RuntimeError`` instead of hanging.

Set environment variables before the first client call; do not modify
``os.environ`` while client calls are in flight.
"""

import asyncio
import os
from collections.abc import AsyncIterable, AsyncIterator, Buffer
from datetime import datetime
from typing import Self

from ._types import EncryptedObjectMetadata as EncryptedObjectMetadata
from ._types import EncryptionInfo as EncryptionInfo
from ._types import KeyInfo as KeyInfo
from ._types import KeyringStatus as KeyringStatus
from ._types import ObjectMetadata as ObjectMetadata

class EncryptionError(RuntimeError): ...
class DecryptionError(EncryptionError): ...
class UnknownKeyError(EncryptionError):
    """No key for the object is available. Derived from unauthenticated header
    data, so it may also indicate tampering."""

class NotEncryptedError(EncryptionError): ...

class Keyring:
    """A reloadable set of key-encryption keys read from a file; key bytes never
    reach Python. The file is re-read on use when it changes."""
    @staticmethod
    def from_file(path: str | os.PathLike[str]) -> Keyring:
        """Load a keyring file. Raises ``EncryptionError`` for an unreadable or
        invalid file and, on Unix, for one its group can write, others can
        access, or another user owns; other platforms do not check the file's
        permissions."""
    def status(self) -> KeyringStatus: ...
    @property
    def wraps(self) -> int:
        """Wrap operations (one per object written) done with the active key by
        this keyring instance in this process. Not persisted; use it to rotate
        keys long before the random-IV bound of about 2**32 wraps per key."""

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

    ``await stream.aclose()`` releases the connection or file at once (use
    ``async with contextlib.aclosing(stream)``). It is idempotent and raises
    ``RuntimeError`` while a read is pending. A read after ``aclose`` raises
    ``RuntimeError``; it never reports end-of-stream.
    """
    def __aiter__(self) -> Self: ...
    async def __anext__(self) -> bytes: ...
    async def next(self) -> bytes: ...
    async def aclose(self) -> None: ...

class ListStream(AsyncIterator[list[str]]):
    """Async iterator of batches of names returned by
    :py:meth:`ObjectStorageClient.list_objects_stream`.

    Each batch is a non-empty ``list[str]`` of at most 1024 names or 256 KiB of
    names. Reads follow the rules of :py:class:`ByteStream`: they must not
    overlap (``RuntimeError``); a cancelled read loses no names; after a read
    raises an error other than cancellation, every later read raises it again;
    ``await stream.aclose()`` stops the listing (idempotent, ``RuntimeError``
    while a read is pending), and a read after ``aclose`` raises
    ``RuntimeError``.
    """
    def __aiter__(self) -> Self: ...
    async def __anext__(self) -> list[str]: ...
    async def aclose(self) -> None: ...

class ObjectStorageClient:
    """Unified client for S3, GCS, Azure and ``file://`` URLs.

    Each method starts its operation when called and returns an
    ``asyncio.Future``. Await it, pass it to ``asyncio.gather`` or
    ``asyncio.ensure_future``, or cancel it to stop the operation.
    ``asyncio.create_task`` and ``TaskGroup.create_task`` reject it, so wrap the
    call in a coroutine for them.
    """
    # Error types, for every method: ``ValueError`` for a caller-argument error
    # (unsupported scheme, invalid URL, unknown pre-sign method, empty URL
    # list, URLs from different origins, content binding on a non-S3 store,
    # an out-of-range ``expires_in_secs``, ``content_length`` or
    # ``frame_length``; ints beyond +-2**127 still raise ``OverflowError``);
    # ``FileNotFoundError`` for a missing object; ``PermissionError`` for an
    # HTTP 401 or 403 answer and for an archived object that must be restored
    # (S3) or rehydrated (Azure) first; other ``OSError`` subclasses for local
    # file failures; ``RuntimeError`` for the rest.
    def __init__(self, profile: str | None = None) -> None:
        """``profile`` names an AWS profile for ``s3://`` credentials. It is the
        only credential source: static keys in ``S3_ACCESS_KEY_ID`` /
        ``AWS_ACCESS_KEY_ID`` are not used, and ``AWS_PROFILE`` is not read. The
        profile is resolved on the first signed request; a profile that cannot
        be resolved raises a ``RuntimeError`` naming it and never falls back to
        instance metadata. A ``credential_process`` entry in the profile makes
        this process run that command, so the profile files must be trusted.
        An empty or blank ``profile`` raises ``ValueError``.
        ``with_encryption`` keeps the profile:
        ``ObjectStorageClient(profile="dev").with_encryption(keyring)``."""
    def with_encryption(
        self,
        keyring: Keyring,
        bind_object_key: bool = True,
        frame_length: int = 65536,
    ) -> EncryptedObjectStorageClient:
        """An encrypting view over the same stores, connections and AWS profile."""
    def get_object(self, url: str) -> asyncio.Future[bytes]:
        """Return the whole object as ``bytes``.

        Peak memory is about twice the object size while the result is copied
        into a Python ``bytes``. For large objects use :py:meth:`get_object_stream`,
        which keeps memory flat.
        """
    def get_object_stream(self, url: str) -> asyncio.Future[ByteStream]: ...
    def put_object_stream(
        self,
        url: str,
        chunks: AsyncIterable[Buffer],
        *,
        size_hint: int | None = None,
    ) -> asyncio.Future[None]:
        """Upload the bytes-like chunks of an async iterable.

        Chunks are pulled in small batches: the iterable is read ahead by at
        most 256 KiB (or 1024 chunks) past what the upload has consumed. Each
        chunk is copied when it is yielded, so a source may reuse one buffer.
        The object is created only if the iterable finishes cleanly; on any
        error or cancellation the upload is aborted and nothing is left behind.
        On cancellation the abort runs in the background and the interpreter
        waits up to 10 s for it at exit. Azure has no abort (uncommitted blocks
        expire after 7 days). SIGKILL or an unhandled SIGTERM can still leave an
        incomplete multipart upload: configure an AbortIncompleteMultipartUpload
        lifecycle rule.

        A ``ByteStream`` from ``get_object_stream`` is piped natively: no Python
        code runs per chunk, so the bytes move even while the event loop is
        busy (a busy loop still delays the start and end of the call). It is
        consumed and closed by the call, also when the upload then fails;
        later reads raise ``RuntimeError``. A closed ``ByteStream``, one with a
        read pending, or a call refused before it starts (no running event
        loop) raises ``RuntimeError``, leaves the stream as it was and writes
        nothing.

        ``size_hint`` is the expected total size in bytes, if known (a negative
        value or one above 5 TiB raises ``ValueError``). A large hint makes the upload use bigger
        parts so that it stays within the store's part limit (about 78 GiB
        otherwise). It is a hint, not a limit. A multipart upload holds about
        40 MiB of parts; up to 32 MiB of parts are in flight (always at least
        one). Above about 278 GiB a part exceeds 32 MiB, so memory is about one
        part plus about one input chunk (roughly 590 MiB at 5 TiB).
        """
    def put_object(self, url: str, data: Buffer | list[int]) -> asyncio.Future[None]:
        """Upload ``data``: any bytes-like object (``bytes``, ``bytearray``,
        ``memoryview``, ``array.array``, ...) or a list of ints in 0..255. ``bytes``
        is shared without a copy; other buffers are copied once up front."""
    def delete_object(self, url: str) -> asyncio.Future[None]:
        """Delete an object. An ``http(s)://`` URL that is a ``WebDAV`` collection
        raises ``ValueError``, because deleting it would remove everything in
        it; use ``delete_objects_by_prefix``."""
    def delete_objects_by_prefix(self, url: str) -> asyncio.Future[None]:
        """Delete every object under the prefix. A URL that names only a bucket
        or container deletes everything in it. An empty URL, or the ``file://``
        root, raises ``ValueError``. Symlinks below a ``file://`` prefix are
        removed as links and never followed."""
    def list_objects(self, url: str) -> asyncio.Future[list[str]]:
        """List the objects under the prefix. Names are relative to the prefix
        and come in store order. On ``file://``, symlinks below the prefix are
        not listed. The whole listing is held in memory (twice
        while it is converted); for a large prefix use
        :py:meth:`list_objects_stream`."""
    def list_objects_stream(self, url: str) -> asyncio.Future[ListStream]:
        """Like :py:meth:`list_objects`, as an async iterator of ``list[str]``
        batches (at most 1024 names or 256 KiB of names each), so memory stays
        bounded::

            async for names in await client.list_objects_stream(url):
                ...
        """
    def get_object_metadata(self, url: str) -> asyncio.Future[ObjectMetadata]: ...
    def object_exists(self, url: str) -> asyncio.Future[bool]: ...
    def create_bucket(self, url: str) -> asyncio.Future[None]:
        """Create the bucket or container. A bucket that already exists and is
        yours (S3 ``BucketAlreadyOwnedByYou``, GCS visible to the caller, Azure
        409) is success; a name owned by another account or project raises
        ``RuntimeError``."""
    def bucket_exists(self, url: str) -> asyncio.Future[bool]: ...
    def copy_object(self, from_url: str, to_url: str) -> asyncio.Future[None]:
        """Copy an object; copying it onto itself does nothing. The plain client
        first reads the start of the source (one extra request) and raises
        ``ValueError`` for an encrypted object bound to its name when the
        destination name differs; copy it with the encrypted client. The
        encrypted client always decrypts and re-encrypts, never copying
        server-side."""
    def move_object(self, from_url: str, to_url: str) -> asyncio.Future[None]:
        """Move an object. The source is deleted only after the destination
        committed and only if it is unchanged. The checks and costs are those of
        ``copy_object``. An ``http(s)://`` source that is a ``WebDAV`` collection
        raises ``ValueError``."""
    # expires_in_secs must be 1 to 604800 (7 days), otherwise ValueError. An
    # s3:// URL signed with temporary credentials stops working when that
    # session ends, whatever expires_in_secs says.
    def get_pre_signed_url(
        self,
        url: str,
        method: str = "GET",
        expires_in_secs: int = 3600,
        content_length: int | None = None,
        content_type: str | None = None,
    ) -> asyncio.Future[str]: ...
    # expires_in_secs must be 1 to 604800 (7 days), otherwise ValueError. An
    # s3:// URL signed with temporary credentials stops working when that
    # session ends, whatever expires_in_secs says.
    def get_pre_signed_urls(
        self,
        urls: list[str],
        method: str = "GET",
        expires_in_secs: int = 3600,
        content_length: int | None = None,
        content_type: str | None = None,
    ) -> asyncio.Future[list[str]]: ...

class EncryptedObjectStorageClient:
    """Encrypts every object it writes and decrypts every object it reads.

    Data from ``get_object_stream`` is provisional until the iterator ends
    cleanly: truncation or tampering raises ``DecryptionError`` at the point it
    is detected. There are no pre-signed URL methods.

    Obtain it with :py:meth:`ObjectStorageClient.with_encryption`, which shares
    that client's stores and AWS profile, or construct it directly with
    ``EncryptedObjectStorageClient(keyring, profile=...)``.

    Each method starts its operation when called and returns an
    ``asyncio.Future``. Await it, pass it to ``asyncio.gather`` or
    ``asyncio.ensure_future``, or cancel it to stop the operation.
    ``asyncio.create_task`` and ``TaskGroup.create_task`` reject it, so wrap the
    call in a coroutine for them.
    """
    def __init__(
        self,
        keyring: Keyring,
        bind_object_key: bool = True,
        frame_length: int = 65536,
        *,
        profile: str | None = None,
    ) -> None:
        """``profile`` (keyword-only) names an AWS profile for ``s3://``
        credentials, exactly as for ``ObjectStorageClient`` (an empty or blank
        name raises ``ValueError``)."""
    def reencrypt(
        self,
        url: str,
        from_key: str,
        *,
        before: datetime | None = None,
        dry_run: bool = False,
    ) -> asyncio.Future[str]:
        """Rewrite the object under the active key if it is wrapped by
        ``from_key``. Returns ``"rewritten"``, ``"would-rewrite"``,
        ``"skipped"``, ``"not-encrypted"``, ``"after-cutoff"`` or ``"changed"``.
        ``before`` must be timezone-aware (``ValueError`` otherwise; a
        non-datetime raises ``TypeError``);
        ``from_key`` equal to the active key raises ``ValueError``; a
        ``from_key`` the keyring does not hold raises ``EncryptionError``.
        Both are raised before any object is read.

        ``before`` compares the modification time the store reports, which is
        not authenticated: it only screens out forgeries by holders of leaked
        storage credentials on stores that set the time themselves at commit.
        The provider, anyone who can set file times on ``file://`` or NFS, and
        S3 multipart uploads started before the cutoff are exceptions; an
        object whose store reports no time is ``"after-cutoff"``. If a party
        that may hold the leaked key can influence modification times, check
        candidates against an independent record (application records,
        provider audit logs or inventory) before re-encrypting."""
    def without_encryption(self) -> ObjectStorageClient: ...
    def get_object(
        self, url: str, *, context: dict[str, str] | None = None
    ) -> asyncio.Future[bytes]:
        """Return the whole object as ``bytes``.

        Peak memory is about twice the object size while the result is copied
        into a Python ``bytes``. For large objects use :py:meth:`get_object_stream`,
        which keeps memory flat.

        With ``context``, the object must carry every given pair in its
        authenticated encryption context (extra stored pairs are allowed);
        otherwise ``DecryptionError`` is raised before any key is used. Without
        it the stored context is not checked. Keys starting with ``osc:`` or
        ``aws-crypto-`` raise ``ValueError``.
        """
    def get_object_stream(
        self, url: str, *, context: dict[str, str] | None = None
    ) -> asyncio.Future[ByteStream]:
        """Like :py:meth:`get_object` for a stream; ``context`` is checked
        before the first chunk."""
    def put_object(
        self, url: str, data: Buffer | list[int], context: dict[str, str] | None = None
    ) -> asyncio.Future[None]: ...
    def put_object_stream(
        self,
        url: str,
        chunks: AsyncIterable[Buffer],
        context: dict[str, str] | None = None,
        *,
        size_hint: int | None = None,
    ) -> asyncio.Future[None]:
        """Encrypt and upload the chunks of an async iterable, with the same
        bounded read-ahead and abort semantics as the plain client, including
        the background abort that the interpreter waits for at exit.
        A ``ByteStream`` is piped natively and is consumed and closed by the
        call (see the plain client).
        ``size_hint`` is the expected plaintext size in bytes, if known (``ValueError`` if negative or above 5 TiB)."""
    def inspect_encryption(self, url: str) -> asyncio.Future[EncryptionInfo]:
        """Read the encryption header. Needs no keys. The result is
        unauthenticated: anyone who can modify the stored object can change it.
        To enforce a context, read with ``get_object(url, context=...)``."""
    def delete_object(self, url: str) -> asyncio.Future[None]:
        """Delete an object. An ``http(s)://`` URL that is a ``WebDAV`` collection
        raises ``ValueError``, because deleting it would remove everything in
        it; use ``delete_objects_by_prefix``."""
    def delete_objects_by_prefix(self, url: str) -> asyncio.Future[None]:
        """Delete every object under the prefix. A URL that names only a bucket
        or container deletes everything in it. An empty URL, or the ``file://``
        root, raises ``ValueError``. Symlinks below a ``file://`` prefix are
        removed as links and never followed."""
    def list_objects(self, url: str) -> asyncio.Future[list[str]]:
        """List the objects under the prefix. Names are relative to the prefix
        and come in store order. The whole listing is held in memory (twice
        while it is converted); for a large prefix use
        :py:meth:`list_objects_stream`."""
    def list_objects_stream(self, url: str) -> asyncio.Future[ListStream]:
        """Like :py:meth:`list_objects`, as an async iterator of ``list[str]``
        batches (at most 1024 names or 256 KiB of names each), so memory stays
        bounded::

            async for names in await client.list_objects_stream(url):
                ...
        """
    def get_object_metadata(self, url: str) -> asyncio.Future[EncryptedObjectMetadata]:
        """Size, type and version of the object. The ``encryption`` field is
        read from the unauthenticated header: anyone who can modify the stored
        object can change it. To enforce a context, read with
        ``get_object(url, context=...)``."""
    def object_exists(self, url: str) -> asyncio.Future[bool]: ...
    def create_bucket(self, url: str) -> asyncio.Future[None]:
        """Create the bucket or container. A bucket that already exists and is
        yours (S3 ``BucketAlreadyOwnedByYou``, GCS visible to the caller, Azure
        409) is success; a name owned by another account or project raises
        ``RuntimeError``."""
    def bucket_exists(self, url: str) -> asyncio.Future[bool]: ...
    def copy_object(self, from_url: str, to_url: str) -> asyncio.Future[None]:
        """Copy an object; copying it onto itself does nothing. The plain client
        first reads the start of the source (one extra request) and raises
        ``ValueError`` for an encrypted object bound to its name when the
        destination name differs; copy it with the encrypted client. The
        encrypted client always decrypts and re-encrypts, never copying
        server-side."""
    def move_object(self, from_url: str, to_url: str) -> asyncio.Future[None]:
        """Move an object. The source is deleted only after the destination
        committed and only if it is unchanged. The checks and costs are those of
        ``copy_object``. An ``http(s)://`` source that is a ``WebDAV`` collection
        raises ``ValueError``."""
