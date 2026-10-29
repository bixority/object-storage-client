"""Unified object storage client.

Multiprocessing: use spawn/forkserver, or fork before the first client call
(the async runtime does not survive a fork). A client used in a child forked
after the runtime started raises ``RuntimeError`` instead of hanging.
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
    """Why the last check of the file failed; ``None`` while the file is valid.
    The file is checked on use at most every 5 s, including by ``status()``."""

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

    ``await stream.aclose()`` releases the connection or file at once (use
    ``async with contextlib.aclosing(stream)``). It is idempotent and raises
    ``RuntimeError`` while a read is pending. A read after ``aclose`` raises
    ``RuntimeError``; it never reports end-of-stream.
    """
    def __aiter__(self) -> AsyncIterator[bytes]: ...
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
    def __aiter__(self) -> AsyncIterator[list[str]]: ...
    async def __anext__(self) -> list[str]: ...
    async def aclose(self) -> None: ...

class ObjectStorageClient:
    # Error types, for every method: ``ValueError`` for a caller-argument error
    # (unsupported scheme, invalid URL, unknown pre-sign method, empty URL
    # list, URLs from different origins, content binding on a non-S3 store,
    # an out-of-range ``expires_in_secs``, ``content_length`` or
    # ``frame_length``; ints beyond +-2**127 still raise ``OverflowError``);
    # ``FileNotFoundError`` for a missing object; ``PermissionError`` for an
    # HTTP 401 or 403 answer; other ``OSError`` subclasses for local file
    # failures; ``RuntimeError`` for the rest.
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
        On cancellation the abort runs in the background and the interpreter
        waits up to 10 s for it at exit. Azure has no abort (uncommitted blocks
        expire after 7 days). SIGKILL or an unhandled SIGTERM can still leave an
        incomplete multipart upload: configure an AbortIncompleteMultipartUpload
        lifecycle rule.

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
    async def delete_objects_by_prefix(self, url: str) -> None:
        """Delete every object under the prefix. A URL that names only a bucket
        or container deletes everything in it. An empty URL, or the ``file://``
        root, raises ``ValueError``. Symlinks below a ``file://`` prefix are
        removed as links and never followed."""
    async def list_objects(self, url: str) -> list[str]:
        """List the objects under the prefix. Names are relative to the prefix
        and come in store order. The whole listing is held in memory (twice
        while it is converted); for a large prefix use
        :py:meth:`list_objects_stream`."""
    async def list_objects_stream(self, url: str) -> ListStream:
        """Like :py:meth:`list_objects`, as an async iterator of ``list[str]``
        batches (at most 1024 names or 256 KiB of names each), so memory stays
        bounded::

            async for names in await client.list_objects_stream(url):
                ...
        """
    async def get_object_metadata(self, url: str) -> ObjectMetadata: ...
    async def object_exists(self, url: str) -> bool: ...
    async def create_bucket(self, url: str) -> None: ...
    async def bucket_exists(self, url: str) -> bool: ...
    async def copy_object(self, from_url: str, to_url: str) -> None: ...
    async def move_object(self, from_url: str, to_url: str) -> None: ...
    # expires_in_secs must be 1 to 604800 (7 days), otherwise ValueError. An
    # s3:// URL signed with temporary credentials stops working when that
    # session ends, whatever expires_in_secs says.
    async def get_pre_signed_url(
        self,
        url: str,
        method: str = "GET",
        expires_in_secs: int = 3600,
        content_length: int | None = None,
        content_type: str | None = None,
    ) -> str: ...
    # expires_in_secs must be 1 to 604800 (7 days), otherwise ValueError. An
    # s3:// URL signed with temporary credentials stops working when that
    # session ends, whatever expires_in_secs says.
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

    Obtain it with :py:meth:`ObjectStorageClient.with_encryption`, which shares
    that client's stores and AWS profile, or construct it directly with
    ``EncryptedObjectStorageClient(keyring, profile=...)``.
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
        ``before`` must be timezone-aware (``ValueError`` otherwise; a
        non-datetime raises ``TypeError``);
        ``from_key`` equal to the active key raises ``ValueError``.

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
    async def get_object(
        self, url: str, *, context: dict[str, str] | None = None
    ) -> bytes:
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
    async def get_object_stream(
        self, url: str, *, context: dict[str, str] | None = None
    ) -> ByteStream:
        """Like :py:meth:`get_object` for a stream; ``context`` is checked
        before the first chunk."""
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
        bounded read-ahead and abort semantics as the plain client, including
        the background abort that the interpreter waits for at exit.
        ``size_hint`` is the expected plaintext size in bytes, if known (``ValueError`` if negative or above 5 TiB)."""
    async def inspect_encryption(self, url: str) -> EncryptionInfo: ...
    async def delete_object(self, url: str) -> None: ...
    async def delete_objects_by_prefix(self, url: str) -> None:
        """Delete every object under the prefix. A URL that names only a bucket
        or container deletes everything in it. An empty URL, or the ``file://``
        root, raises ``ValueError``. Symlinks below a ``file://`` prefix are
        removed as links and never followed."""
    async def list_objects(self, url: str) -> list[str]:
        """List the objects under the prefix. Names are relative to the prefix
        and come in store order. The whole listing is held in memory (twice
        while it is converted); for a large prefix use
        :py:meth:`list_objects_stream`."""
    async def list_objects_stream(self, url: str) -> ListStream:
        """Like :py:meth:`list_objects`, as an async iterator of ``list[str]``
        batches (at most 1024 names or 256 KiB of names each), so memory stays
        bounded::

            async for names in await client.list_objects_stream(url):
                ...
        """
    async def get_object_metadata(self, url: str) -> EncryptedObjectMetadata: ...
    async def object_exists(self, url: str) -> bool: ...
    async def create_bucket(self, url: str) -> None: ...
    async def bucket_exists(self, url: str) -> bool: ...
    async def copy_object(self, from_url: str, to_url: str) -> None: ...
    async def move_object(self, from_url: str, to_url: str) -> None: ...
