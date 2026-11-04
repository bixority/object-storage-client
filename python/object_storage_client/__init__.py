"""Unified object storage client.

Multiprocessing: use spawn/forkserver, or fork before the first client call
(the async runtime does not survive a fork). A client used in a child forked
after the runtime started raises ``RuntimeError`` instead of hanging.
"""

from ._object_storage_client import (
    DecryptionError,
    EncryptedObjectStorageClient,
    EncryptionError,
    Keyring,
    NotEncryptedError,
    ObjectStorageClient,
    UnknownKeyError,
)
from ._stream import ByteStream, ListStream
from ._types import (
    EncryptedObjectMetadata,
    EncryptionInfo,
    KeyInfo,
    KeyringStatus,
    ObjectMetadata,
)

__all__ = [
    "ByteStream",
    "DecryptionError",
    "EncryptedObjectMetadata",
    "EncryptedObjectStorageClient",
    "EncryptionError",
    "EncryptionInfo",
    "KeyInfo",
    "Keyring",
    "KeyringStatus",
    "ListStream",
    "NotEncryptedError",
    "ObjectMetadata",
    "ObjectStorageClient",
    "UnknownKeyError",
]
