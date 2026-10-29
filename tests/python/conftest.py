import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from object_storage_client import (
    EncryptedObjectStorageClient,
    Keyring,
    ObjectStorageClient,
)


@pytest.fixture
def tmp_url(tmp_path) -> str:
    """A file:// URL of a fresh temporary directory."""
    return tmp_path.as_uri()


_KEYRINGS = Path(__file__).resolve().parents[1] / "vectors" / "keyrings"


def _private_copy(name: str, directory: Path) -> Path:
    """Copy a TEST-ONLY fixture keyring to `directory` with mode 0600."""
    directory.mkdir(exist_ok=True)
    target = directory / name
    target.write_bytes((_KEYRINGS / name).read_bytes())
    target.chmod(0o600)
    return target


@pytest.fixture
def keyring_path(tmp_path) -> Path:
    """A private copy of one.keyring (one key, active)."""
    return _private_copy("one.keyring", tmp_path / "keyring")


@pytest.fixture
def two_keyring_path(tmp_path) -> Path:
    """A private copy of two.keyring (the key of one.keyring plus a newer, active one)."""
    return _private_copy("two.keyring", tmp_path / "keyring2")


@pytest.fixture
def other_keyring_path(tmp_path) -> Path:
    """A private copy of other.keyring (a key unrelated to the others)."""
    return _private_copy("other.keyring", tmp_path / "keyring3")


@pytest.fixture
def keyring(keyring_path) -> Keyring:
    return Keyring.from_file(keyring_path)


@pytest.fixture
def plain() -> ObjectStorageClient:
    return ObjectStorageClient()


@pytest.fixture
def secure(plain, keyring) -> EncryptedObjectStorageClient:
    return plain.with_encryption(keyring)


@pytest.fixture
def store(tmp_path) -> str:
    """A file:// URL of a fresh object directory."""
    d = tmp_path / "store"
    d.mkdir()
    return d.as_uri()


@pytest.fixture(params=["plain", "secure"])
def any_client(request):
    """Each client kind in turn. Built on `store`, so it never sees the keyring."""
    return request.getfixturevalue(request.param)


# Container-backed fixtures (integration tests only; nothing starts otherwise).

_ROOT = Path(__file__).resolve().parents[2]
_LABEL = "org.bixority.object-storage-client"
_PROBE_TIMEOUT = 120
# Azurite's well-known development key (public, not a secret).
_AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


def _log_line(text: str) -> LogMessageWaitStrategy:
    return LogMessageWaitStrategy(text).with_startup_timeout(_PROBE_TIMEOUT)


def _loopback(container: DockerContainer, port: int) -> DockerContainer:
    """Publish `port` on a random 127.0.0.1 port only (keys are well known)."""
    container.ports[f"{port}/tcp"] = ("127.0.0.1", None)
    return container


def _wait_until_ready(probe) -> None:
    """Retry `await probe(client)` with a throwaway client until it works."""

    async def attempt():
        await probe(ObjectStorageClient())

    deadline = time.monotonic() + _PROBE_TIMEOUT
    while True:
        try:
            asyncio.run(attempt())
            return
        except Exception as e:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"emulator not ready after {_PROBE_TIMEOUT}s: {e!r}"
                ) from e
            time.sleep(1)


@pytest.fixture(scope="session")
def garage_env():
    """Start Garage, point the S3_* settings at it, stop it afterwards."""
    container = (
        DockerContainer("docker.io/dxflrs/garage:v2.5.0")
        .with_command("/garage server --single-node --default-bucket")
        .with_env("GARAGE_DEFAULT_ACCESS_KEY", "rootroot")
        .with_env("GARAGE_DEFAULT_SECRET_KEY", "rootrootrootroot")
        .with_env("GARAGE_DEFAULT_BUCKET", "default")
        .waiting_for(_log_line("Creating default bucket"))
        .with_kwargs(labels={_LABEL: "py-it-garage"})
        .with_copy_into_container(
            _ROOT / ".docker" / "garage.toml", "/etc/garage.toml"
        )
    )
    _loopback(container, 3900)
    with container, pytest.MonkeyPatch.context() as mp:
        port = container.get_exposed_port(3900)
        mp.setenv("S3_ENDPOINT", f"http://127.0.0.1:{port}")
        mp.setenv("S3_REGION", "eu-north-1")
        mp.setenv("S3_ACCESS_KEY_ID", "rootroot")
        mp.setenv("S3_SECRET_ACCESS_KEY", "rootrootrootroot")
        mp.setenv("S3_ALLOW_HTTP", "true")

        async def probe(client):
            if not await client.bucket_exists("s3://default/"):
                raise RuntimeError("default bucket missing")

        _wait_until_ready(probe)
        yield


@pytest.fixture(scope="session")
def azurite_env():
    """Start Azurite, point the AZURE_* settings at it, stop it afterwards."""
    container = (
        DockerContainer("mcr.microsoft.com/azure-storage/azurite:latest")
        .with_command("azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck")
        .waiting_for(_log_line("Azurite Blob service successfully listens"))
        .with_kwargs(labels={_LABEL: "py-it-azurite"})
    )
    _loopback(container, 10000)
    with container, pytest.MonkeyPatch.context() as mp:
        port = container.get_exposed_port(10000)
        mp.setenv("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1")
        mp.setenv("AZURE_STORAGE_ACCOUNT_KEY", _AZURITE_KEY)
        mp.setenv(
            "AZURE_STORAGE_ENDPOINT", f"http://127.0.0.1:{port}/devstoreaccount1"
        )
        mp.setenv("AZURE_ALLOW_HTTP", "true")

        async def probe(client):
            await client.bucket_exists("az://readiness-probe/")

        _wait_until_ready(probe)
        yield


@pytest.fixture
def run_isolated(tmp_path):
    """Run Python code in a child with a minimal, explicit environment.

    The child sees only PATH, HOME (an empty directory below `tmp_path`),
    PYTHONPATH (this process's sys.path) and the caller's `env`, so nothing
    from the developer's environment (AWS_*, proxies, OSC_*) reaches it. On a
    timeout the whole process group is killed.
    """
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)

    def run(code: str, *args: str, env=None, timeout: float = 120):
        child_env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(home),
            "PYTHONPATH": os.pathsep.join(sys.path),
            **(env or {}),
        }
        proc = subprocess.Popen(
            [sys.executable, "-c", code, *args],
            env=child_env,
            cwd=tmp_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate()
            raise
        return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)

    return run
