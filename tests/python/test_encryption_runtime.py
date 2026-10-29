import asyncio
import json
import os
import sys
import textwrap
from pathlib import Path

import pytest

from object_storage_client import (
    EncryptedObjectStorageClient,
    Keyring,
)


async def _drain(stream) -> bytes:
    out = []
    async for chunk in stream:
        out.append(chunk)
    return b"".join(out)


async def test_cancel_get_object_stream_midway(secure, store):
    url = f"{store}/big.bin"
    data = os.urandom(1_000_000)
    await secure.put_object(url, data)
    stream = await secure.get_object_stream(url)
    first = await asyncio.wait_for(stream.next(), 10)
    task = asyncio.ensure_future(stream.next())
    await asyncio.sleep(0)  # the read is now awaiting the native fill
    task.cancel()
    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
    assert task.cancelled()
    rest = await asyncio.wait_for(_drain(stream), 10)
    assert first + rest == data
    assert await asyncio.wait_for(secure.get_object(url), 10) == data


async def test_break_out_of_async_for(secure, store):
    url = f"{store}/big.bin"
    data = os.urandom(1_000_000)
    await secure.put_object(url, data)
    stream = await secure.get_object_stream(url)
    async for chunk in stream:
        assert chunk
        break
    del stream
    assert await asyncio.wait_for(secure.get_object(url), 10) == data
    stream = await secure.get_object_stream(url)
    assert b"".join([c async for c in stream]) == data


FORK_SCRIPT = textwrap.dedent(
    """
    import asyncio, json, os, sys, traceback
    from object_storage_client import Keyring, ObjectStorageClient

    keyring_path, store, out_dir = sys.argv[1:4]
    N = 1000

    async def run(tag):
        secure = ObjectStorageClient().with_encryption(Keyring.from_file(keyring_path))
        ids = []
        for i in range(N):
            url = f"{store}/{tag}-{i}"
            await secure.put_object(url, b"x")
            ids.append((await secure.inspect_encryption(url))["message_id"])
        with open(os.path.join(out_dir, tag + ".json"), "w") as f:
            json.dump(ids, f)

    # fork BEFORE any client or runtime use
    pid = os.fork()
    if pid == 0:
        try:
            asyncio.run(run("child"))
        except BaseException:
            traceback.print_exc()
            os._exit(1)
        os._exit(0)
    asyncio.run(run("parent"))
    _, status = os.waitpid(pid, 0)
    sys.exit(0 if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0 else 1)
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_fork_no_repeated_message_ids(keyring_path, tmp_path, store, run_isolated):
    out = tmp_path / "out"
    out.mkdir()
    proc = run_isolated(FORK_SCRIPT, str(keyring_path), store, str(out))
    assert proc.returncode == 0, proc.stderr
    parent = json.loads((out / "parent.json").read_text())
    child = json.loads((out / "child.json").read_text())
    assert len(parent) == len(child) == 1000
    assert len(set(parent) | set(child)) == 2000


FORK_AFTER_SCRIPT = textwrap.dedent(
    """
    import asyncio, json, os, signal, sys
    from object_storage_client import Keyring, ObjectStorageClient

    keyring_path, store, out_dir = sys.argv[1:4]
    secure = ObjectStorageClient().with_encryption(Keyring.from_file(keyring_path))

    async def put(tag):
        url = f"{store}/{tag}"
        await secure.put_object(url, b"same plaintext")
        info = await secure.inspect_encryption(url)
        assert await secure.get_object(url) == b"same plaintext"
        return info["message_id"]

    # Start the runtime in the parent first, then fork.
    before = asyncio.run(put("before"))
    pid = os.fork()
    if pid == 0:
        signal.alarm(20)  # guard: a hang kills the child without a result
        result = {}
        for name, call in (
            ("put", lambda: secure.put_object(f"{store}/c", b"x")),
            ("get", lambda: secure.get_object(f"{store}/before")),
            ("inspect", lambda: secure.inspect_encryption(f"{store}/before")),
            ("stream", lambda: secure.get_object_stream(f"{store}/before")),
        ):
            async def attempt():
                await call()  # inside the loop, so the fork check is reached

            try:
                asyncio.run(attempt())
                result[name] = "ok"
            except BaseException as e:
                result[name] = f"{type(e).__name__}: {e}"
        with open(os.path.join(out_dir, "child.json"), "w") as f:
            json.dump(result, f)
        os._exit(0)
    parent = asyncio.run(put("parent"))
    _, status = os.waitpid(pid, 0)
    with open(os.path.join(out_dir, "parent.json"), "w") as f:
        json.dump({"before": before, "after": parent}, f)
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_fork_after_runtime_started(keyring_path, tmp_path, store, run_isolated):
    out = tmp_path / "out"
    out.mkdir()
    proc = run_isolated(
        FORK_AFTER_SCRIPT, str(keyring_path), store, str(out), timeout=90
    )
    assert proc.returncode == 0, proc.stderr
    # The parent keeps working after the fork, with fresh message IDs.
    parent = json.loads((out / "parent.json").read_text())
    assert parent["before"] != parent["after"]
    # The child fails fast with a clear RuntimeError at every entry point
    # (a hang would be killed by the alarm and leave no child.json).
    child = json.loads((out / "child.json").read_text())
    assert set(child) == {"put", "get", "inspect", "stream"}
    for name, outcome in child.items():
        assert outcome.startswith("RuntimeError: ") and "forked" in outcome, (name, outcome)


FORK_AFTER_FAILED_CALL_SCRIPT = textwrap.dedent(
    """
    import asyncio, os, sys
    from object_storage_client import ObjectStorageClient

    url = sys.argv[1]
    client = ObjectStorageClient()
    # A call outside a running loop fails before any runtime thread exists.
    try:
        client.object_exists(url)
    except RuntimeError as e:
        assert "no running event loop" in str(e), str(e)
    else:
        raise SystemExit("expected RuntimeError")

    pid = os.fork()
    if pid == 0:
        async def check():
            return await client.object_exists(url)
        try:
            ok = asyncio.run(check()) is True
        except BaseException:
            ok = False
        os._exit(0 if ok else 1)
    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_failed_call_outside_a_loop_does_not_block_a_later_fork(store, run_isolated):
    (Path(store.removeprefix("file://")) / "present").write_bytes(b"x")
    proc = run_isolated(FORK_AFTER_FAILED_CALL_SCRIPT, f"{store}/present", timeout=60)
    assert proc.returncode == 0, proc.stderr


# Interop with the Python AWS Encryption SDK (oracle dependency group).

PLAINTEXTS = [b"", b"hello esdk", os.urandom(200_000)]


def _esdk_master_key(keyring_path, keyring):
    """An ESDK master key for the active key of a TEST-ONLY fixture keyring.

    This is the independent oracle: the material is read straight from the
    fixture file, never from the library.
    """
    pytest.importorskip("aws_encryption_sdk")
    from aws_encryption_sdk.identifiers import EncryptionKeyType, WrappingAlgorithm
    from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
    from aws_encryption_sdk.key_providers.raw import RawMasterKey

    key_id = keyring.status()["active"]
    line = next(
        line
        for line in Path(keyring_path).read_text().splitlines()
        if line.startswith(f"key={key_id} ")
    )
    material = next(f for f in line.split() if f.startswith("material=")).split("=", 1)[1]
    wrapping_key = WrappingKey(
        wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
        wrapping_key=bytes.fromhex(material),
        wrapping_key_type=EncryptionKeyType.SYMMETRIC,
    )
    return RawMasterKey(
        provider_id="osc-file", key_id=key_id.encode(), wrapping_key=wrapping_key
    )


def _esdk_client():
    import aws_encryption_sdk
    from aws_encryption_sdk import CommitmentPolicy

    return aws_encryption_sdk.EncryptionSDKClient(
        commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    )


@pytest.mark.parametrize("data", PLAINTEXTS, ids=["empty", "small", "large"])
async def test_python_esdk_decrypts_osc(keyring_path, keyring, secure, store, tmp_path, data):
    master = _esdk_master_key(keyring_path, keyring)
    await secure.put_object(f"{store}/o.bin", data, context={"tenant": "acme"})
    blob = (tmp_path / "store" / "o.bin").read_bytes()
    plain, header = _esdk_client().decrypt(source=blob, key_provider=master)
    assert plain == data
    context = header.encryption_context
    assert context["tenant"] == "acme"
    assert context["osc:fmt"] == "1"
    assert "osc:key" in context


async def test_python_esdk_decrypts_osc_stream(keyring_path, keyring, secure, store, tmp_path):
    master = _esdk_master_key(keyring_path, keyring)
    part = 8 * 1024 * 1024
    for name, size in (("odd", 2 * part + 12345), ("exact", 2 * part)):
        data = os.urandom(size)

        async def chunks(data=data):
            for i in range(0, len(data), 1024 * 1024):
                yield data[i : i + 1024 * 1024]

        await secure.put_object_stream(
            f"{store}/{name}.bin", chunks(), context={"tenant": "acme"}
        )
        blob = (tmp_path / "store" / f"{name}.bin").read_bytes()
        plain, header = _esdk_client().decrypt(source=blob, key_provider=master)
        assert plain == data
        assert header.encryption_context["tenant"] == "acme"
        assert header.encryption_context["osc:fmt"] == "1"
        assert header.encryption_context["osc:key"]
        del blob, plain, data


def _esdk_encrypt(master, context, data=b"payload"):
    from aws_encryption_sdk.identifiers import Algorithm

    blob, _ = _esdk_client().encrypt(
        source=data,
        key_provider=master,
        encryption_context=context,
        frame_length=4096,
        algorithm=Algorithm.AES_256_GCM_HKDF_SHA512_COMMIT_KEY,
    )
    return blob


@pytest.mark.parametrize("data", PLAINTEXTS, ids=["empty", "small", "large"])
async def test_osc_decrypts_python_esdk(keyring_path, keyring, store, tmp_path, data):
    master = _esdk_master_key(keyring_path, keyring)
    secure = EncryptedObjectStorageClient(keyring)
    # bound path: the osc:key an object written next to it would carry
    key = str(tmp_path / "store" / "p.bin").lstrip("/")
    ctx = {"osc:fmt": "1", "osc:key": key, "tenant": "acme"}
    (tmp_path / "store" / "p.bin").write_bytes(_esdk_encrypt(master, ctx, data))
    assert await secure.get_object(f"{store}/p.bin") == data
    assert b"".join([c async for c in await secure.get_object_stream(f"{store}/p.bin")]) == data
    # binding off: osc:key not required
    unbound = EncryptedObjectStorageClient(keyring, bind_object_key=False)
    (tmp_path / "store" / "q.bin").write_bytes(
        _esdk_encrypt(master, {"osc:fmt": "1", "tenant": "acme"}, data)
    )
    assert await unbound.get_object(f"{store}/q.bin") == data


async def test_osc_rejects_bad_esdk_context(keyring_path, keyring, store, tmp_path):
    from object_storage_client import DecryptionError

    master = _esdk_master_key(keyring_path, keyring)
    secure = EncryptedObjectStorageClient(keyring)
    good = str(tmp_path / "store" / "bad.bin").lstrip("/")
    cases = {
        "wrongkey": {"osc:fmt": "1", "osc:key": good + "x"},
        "nokey": {"osc:fmt": "1"},
        "nofmt": {"osc:key": good},
        "fmt2": {"osc:fmt": "2", "osc:key": good},
    }
    for label, ctx in cases.items():
        (tmp_path / "store" / "bad.bin").write_bytes(_esdk_encrypt(master, ctx))
        with pytest.raises(DecryptionError):
            await secure.get_object(f"{store}/bad.bin")
