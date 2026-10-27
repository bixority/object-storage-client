"""Upload a file, download it back and compare SHA-256 checksums.

Runs against the Garage (S3) and Azurite (Azure) emulators, for the plain
and the encrypted client. Needs a container runtime:
`pytest -m "integration and not large"`. The 4 GiB tier is local only:
`pytest -m large`.
"""

import hashlib
import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

CHUNK = 1 << 20
TMP_DIR = Path(__file__).resolve().parents[2] / "target" / "tmp"
SIZES = [
    pytest.param(4 << 10, id="4KiB"),
    pytest.param(4 << 20, id="4MiB"),
    pytest.param(4 << 30, id="4GiB", marks=pytest.mark.large),
]


@pytest.fixture
def provider(request):
    """Start the emulator (sync, outside the event loop) and name it."""
    env = {"garage": "garage_env", "azure": "azurite_env"}[request.param]
    request.getfixturevalue(env)
    return request.param


@pytest.fixture
async def location(provider, plain):
    """Object URL in a fresh prefix (Garage) or container (Azure)."""
    if provider == "garage":
        return f"s3://default/py-roundtrip-{uuid.uuid4()}/obj"
    container = f"az://it-{uuid.uuid4().hex}/"
    await plain.create_bucket(container)
    return f"{container}obj"


@pytest.fixture
def source(request):
    """A random on-disk file of the requested size and its SHA-256."""
    size = request.param
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    path = TMP_DIR / f"roundtrip-{uuid.uuid4().hex}.bin"
    digest = hashlib.sha256()
    try:
        with path.open("wb") as f:
            for off in range(0, size, CHUNK):
                block = os.urandom(min(CHUNK, size - off))
                digest.update(block)
                f.write(block)
        yield path, digest.hexdigest()
    finally:
        path.unlink(missing_ok=True)


async def _chunks(path: Path):
    with path.open("rb") as f:
        while block := f.read(CHUNK):
            yield block


@pytest.mark.parametrize("source", SIZES, indirect=True)
@pytest.mark.parametrize("mode", ["plain", "encrypted"])
@pytest.mark.parametrize("provider", ["garage", "azure"], indirect=True)
async def test_roundtrip(location, mode, source, plain, secure):
    """Uploaded bytes come back with the same checksum."""
    path, expected = source
    size = path.stat().st_size
    client = plain if mode == "plain" else secure
    try:
        await client.put_object_stream(location, _chunks(path), size_hint=size)

        digest, count = hashlib.sha256(), 0
        async for chunk in await client.get_object_stream(location):
            digest.update(chunk)
            count += len(chunk)
        assert count == size
        assert digest.hexdigest() == expected

        stored = (await plain.get_object_metadata(location))["size_bytes"]
        if mode == "plain":
            assert stored == size
        else:
            assert stored > size  # ciphertext is what was stored
    finally:
        await client.delete_object(location)
