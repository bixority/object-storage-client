"""Unified object storage client.

Multiprocessing: use spawn/forkserver, or fork before the first use of any
client (the async runtime does not survive a fork). A client used in a child
forked after the runtime started raises ``RuntimeError`` instead of hanging.
"""

from typing import TypedDict
from ._object_storage_client import (
    DecryptionError,
    EncryptedObjectStorageClient,
    EncryptionError,
    Keyring,
    NotEncryptedError,
    ObjectStorageClient,
    UnknownKeyError,
)
from ._stream import ByteStream

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

__all__ = [
    "ObjectStorageClient",
    "EncryptedObjectStorageClient",
    "Keyring",
    "ByteStream",
    "ObjectMetadata",
    "EncryptedObjectMetadata",
    "EncryptionInfo",
    "EncryptionError",
    "DecryptionError",
    "UnknownKeyError",
    "NotEncryptedError",
]
