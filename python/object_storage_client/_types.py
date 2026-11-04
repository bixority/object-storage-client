"""The dictionaries the client returns; the single definition of their types."""

from typing import Literal, TypedDict


class EncryptionInfo(TypedDict):
    """Read from the unauthenticated header: anyone who can modify the stored
    object can change these fields. To enforce a context, read with
    ``get_object(url, context=...)``."""

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
    """The ``encryption`` field is read from the unauthenticated header: anyone
    who can modify the stored object can change it. To enforce a context, read
    with ``get_object(url, context=...)``."""

    stored_size_bytes: int
    # Present, and always true, for an encrypted object; the plain client's
    # ``ObjectMetadata`` has no such key.
    encrypted: Literal[True]
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
