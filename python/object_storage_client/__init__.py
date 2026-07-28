from typing import TypedDict
from ._object_storage_client import ObjectStorageClient, ByteStream

class ObjectMetadata(TypedDict):
    location: str
    last_modified: str
    size_bytes: int
    content_type: str | None
    e_tag: str | None
    version: str | None

__all__ = ["ObjectStorageClient", "ByteStream", "ObjectMetadata"]
