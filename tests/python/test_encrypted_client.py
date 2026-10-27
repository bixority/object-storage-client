import asyncio
import datetime
import os
import re
import shutil
from urllib.parse import quote

import pytest

from object_storage_client import (
    ByteStream,
    DecryptionError,
    EncryptedObjectStorageClient,
    EncryptionError,
    Keyring,
    NotEncryptedError,
    ObjectStorageClient,
    UnknownKeyError,
)


async def chunks(*items):
    for item in items:
        yield item


async def collect(stream) -> bytes:
    return b"".join([c async for c in stream])


def test_construction_routes(plain, keyring):
    assert isinstance(plain.with_encryption(keyring), EncryptedObjectStorageClient)
    direct = EncryptedObjectStorageClient(keyring, bind_object_key=False, frame_length=4096)
    assert isinstance(direct.without_encryption(), ObjectStorageClient)
    assert isinstance(plain.with_encryption(keyring).without_encryption(), ObjectStorageClient)


def test_bad_frame_length_raises(plain, keyring):
    with pytest.raises(ValueError):
        plain.with_encryption(keyring, frame_length=1)


def test_with_encryption_absent_on_encrypted(secure):
    assert not hasattr(secure, "with_encryption")


def test_no_presign_on_encrypted(secure):
    assert not hasattr(secure, "get_pre_signed_url")
    assert not hasattr(secure, "get_pre_signed_urls")


def test_exception_hierarchy():
    assert issubclass(EncryptionError, RuntimeError)
    for exc in (DecryptionError, UnknownKeyError, NotEncryptedError):
        assert issubclass(exc, EncryptionError)
        assert issubclass(exc, RuntimeError)


def test_from_file_errors(tmp_path):
    with pytest.raises(EncryptionError):
        Keyring.from_file(tmp_path / "nope")
    with pytest.raises(EncryptionError):
        Keyring.from_file(str(tmp_path))  # a directory


def test_from_file_accepts_str_and_path_like(keyring_path):
    assert isinstance(Keyring.from_file(keyring_path), Keyring)
    assert isinstance(Keyring.from_file(os.fspath(keyring_path)), Keyring)


def test_keyring_has_no_parse():
    assert not hasattr(Keyring, "parse")


@pytest.mark.skipif(os.name != "posix", reason="Unix permissions")
def test_keyring_readable_by_others_is_rejected(keyring_path):
    keyring_path.chmod(0o644)
    with pytest.raises(EncryptionError):
        Keyring.from_file(keyring_path)


def test_corrupted_keyring_is_rejected(keyring_path):
    text = keyring_path.read_text()
    head, _, tail = text.partition("check=")
    flipped = "0" if tail[0] != "0" else "1"
    keyring_path.write_text(head + "check=" + flipped + tail[1:])
    with pytest.raises(EncryptionError):
        Keyring.from_file(keyring_path)


def test_status(keyring, two_keyring_path):
    st = keyring.status()
    assert set(st) == {"active", "keys", "active_age_days", "last_reload_error"}
    assert isinstance(st["active"], str)
    assert isinstance(st["active_age_days"], int) and st["active_age_days"] >= 0
    assert st["last_reload_error"] is None
    assert [k["id"] for k in st["keys"]] == [st["active"]]
    assert set(st["keys"][0]) == {"id", "created"}
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", st["keys"][0]["created"])
    two = Keyring.from_file(two_keyring_path).status()
    assert len(two["keys"]) == 2
    assert st["active"] in [k["id"] for k in two["keys"]]
    assert two["active"] != st["active"]


def test_repr_shows_path_and_active_id_only(keyring, keyring_path):
    r = repr(keyring)
    assert str(keyring_path) in r and keyring.status()["active"] in r
    # nothing of the file body (material, check values) is shown
    assert "material" not in r and "check" not in r


async def test_put_get_roundtrip_and_stored_bytes(secure, plain, store):
    url = f"{store}/a.bin"
    await secure.put_object(url, b"hello world")
    assert await secure.get_object(url) == b"hello world"
    raw = await plain.get_object(url)
    assert raw[:3] == b"\x02\x04\x78"
    assert b"hello world" not in raw


async def test_stream_roundtrip(secure, store):
    url = f"{store}/s.bin"
    data = os.urandom(200_000)
    await secure.put_object_stream(url, chunks(data[:70_000], data[70_000:]))
    stream = await secure.get_object_stream(url)
    assert isinstance(stream, ByteStream)
    assert await collect(stream) == data
    assert await secure.get_object(url) == data


async def test_pipes_both_ways(secure, plain, store):
    src, dst, dst2 = f"{store}/src", f"{store}/dst", f"{store}/dst2"
    data = os.urandom(150_000)
    await secure.put_object(src, data)
    await plain.put_object_stream(dst, await secure.get_object_stream(src))
    assert await plain.get_object(dst) == data
    await secure.put_object_stream(dst2, await plain.get_object_stream(dst))
    assert await secure.get_object(dst2) == data


async def test_metadata_keys(secure, plain, store):
    url = f"{store}/m.bin"
    await secure.put_object(url, b"12345")
    meta = await secure.get_object_metadata(url)
    assert meta["size_bytes"] == 5
    assert meta["stored_size_bytes"] > 5
    assert meta["encrypted"] is True
    enc = meta["encryption"]
    assert enc["format"] == "osc-esdk2-0478-v1"
    assert enc["frame_length"] == 65536
    assert set(enc) == {"format", "message_id", "key_names", "frame_length", "context"}
    assert set(meta) == {
        "location",
        "last_modified",
        "size_bytes",
        "content_type",
        "e_tag",
        "version",
        "stored_size_bytes",
        "encrypted",
        "encryption",
    }
    pmeta = await plain.get_object_metadata(url)
    assert set(pmeta) == {
        "location",
        "last_modified",
        "size_bytes",
        "content_type",
        "e_tag",
        "version",
    }


def test_encryption_methods_absent_on_plain(plain):
    assert not hasattr(plain, "inspect_encryption")
    assert not hasattr(plain, "without_encryption")


async def test_inspect_and_context(secure, plain, keyring, store):
    url = f"{store}/c.bin"
    await secure.put_object(url, b"x", context={"tenant": "acme"})
    info = await secure.inspect_encryption(url)
    assert info["context"]["tenant"] == "acme"
    assert "osc:key" in info["context"]
    assert info["key_names"] == [keyring.status()["active"]]
    assert len(info["message_id"]) == 64


async def test_inspect_needs_no_keys(secure, plain, other_keyring_path, store):
    url = f"{store}/k.bin"
    await secure.put_object(url, b"x")
    stranger = plain.with_encryption(Keyring.from_file(other_keyring_path))
    assert (await stranger.inspect_encryption(url))["format"] == "osc-esdk2-0478-v1"
    with pytest.raises(UnknownKeyError):
        await stranger.get_object(url)


async def test_reserved_context_key_raises_before_io(secure, plain, store):
    for bad in ("osc:key", "osc:x", "aws-crypto-public-key", "OSC:key", "Aws-Crypto-x"):
        with pytest.raises(ValueError, match="reserved context key"):
            await secure.put_object(f"{store}/r.bin", b"x", context={bad: "v"})
        with pytest.raises(ValueError, match="reserved context key"):
            await secure.put_object_stream(f"{store}/r.bin", chunks(b"x"), context={bad: "v"})
    assert not await plain.object_exists(f"{store}/r.bin")


async def test_oversized_context_raises_value_error(secure, plain, store):
    with pytest.raises(ValueError):
        await secure.put_object(f"{store}/big.bin", b"x", context={"k": "v" * 70_000})
    assert not await plain.object_exists(f"{store}/big.bin")


async def test_not_encrypted_error(secure, plain, store):
    url = f"{store}/plain.bin"
    await plain.put_object(url, b"plain data, not encrypted")
    with pytest.raises(NotEncryptedError):
        await secure.get_object(url)
    with pytest.raises(NotEncryptedError):
        await collect(await secure.get_object_stream(url))
    with pytest.raises(NotEncryptedError):
        await secure.inspect_encryption(url)


async def test_tampered_object_raises_decryption_error(secure, tmp_path, store):
    url = f"{store}/t.bin"
    await secure.put_object(url, b"payload" * 100)
    path = tmp_path / "store" / "t.bin"
    raw = bytearray(path.read_bytes())
    raw[-20] ^= 0x01
    path.write_bytes(bytes(raw))
    with pytest.raises(DecryptionError):
        await secure.get_object(url)
    with pytest.raises(DecryptionError):
        await collect(await secure.get_object_stream(url))


async def test_truncated_stream_raises(secure, tmp_path, store):
    url = f"{store}/trunc.bin"
    await secure.put_object(url, os.urandom(200_000))
    path = tmp_path / "store" / "trunc.bin"
    path.write_bytes(path.read_bytes()[:-100])
    with pytest.raises(DecryptionError):
        await collect(await secure.get_object_stream(url))


async def test_error_is_sticky_and_never_becomes_eof(secure, tmp_path, store):
    url = f"{store}/sticky.bin"
    await secure.put_object(url, os.urandom(300_000))
    path = tmp_path / "store" / "sticky.bin"
    path.write_bytes(path.read_bytes()[:-100_000])
    stream = await secure.get_object_stream(url)
    with pytest.raises(DecryptionError):
        while True:
            if not await stream.next():
                pytest.fail("clean end of a truncated object")
    with pytest.raises(DecryptionError):
        await stream.next()
    with pytest.raises(DecryptionError):
        await stream.__anext__()


async def test_missing_object_is_file_not_found(secure, store):
    with pytest.raises(FileNotFoundError):
        await secure.get_object(f"{store}/missing")


async def test_binding_mismatch_after_rename(secure, plain, store):
    await secure.put_object(f"{store}/one", b"data")
    await plain.copy_object(f"{store}/one", f"{store}/two")
    with pytest.raises(DecryptionError):
        await secure.get_object(f"{store}/two")


# Review focus 1: empty objects and exact frame multiples.


@pytest.mark.parametrize("size", [0, 1, 4096, 8192, 65536, 131072])
async def test_sizes_roundtrip(plain, keyring, store, size):
    secure = plain.with_encryption(keyring, frame_length=4096)
    data = os.urandom(size)
    await secure.put_object(f"{store}/p", data)
    assert await secure.get_object(f"{store}/p") == data
    assert (await secure.get_object_metadata(f"{store}/p"))["size_bytes"] == size
    await secure.put_object_stream(f"{store}/q", chunks(data))
    assert await collect(await secure.get_object_stream(f"{store}/q")) == data
    assert (await secure.get_object_metadata(f"{store}/q"))["size_bytes"] == size


async def test_empty_and_zero_chunk_objects(secure, plain, store):
    await secure.put_object(f"{store}/e1", b"")
    assert await secure.get_object(f"{store}/e1") == b""
    await secure.put_object_stream(f"{store}/e2", chunks())
    assert await secure.get_object(f"{store}/e2") == b""
    assert (await secure.get_object_metadata(f"{store}/e2"))["size_bytes"] == 0
    await plain.put_object_stream(f"{store}/e3", chunks())
    assert await plain.get_object(f"{store}/e3") == b""


# Review focus 2: unusual object keys.


@pytest.mark.parametrize("name", ["with space.bin", "100%.bin", "pct%20enc.bin", "ключ-鍵.bin"])
async def test_unusual_keys(secure, store, name):
    url = f"{store}/{quote(name)}"
    await secure.put_object(url, b"payload")
    assert await secure.get_object(url) == b"payload"
    assert await collect(await secure.get_object_stream(url)) == b"payload"
    dst = f"{store}/copy {name}"
    await secure.copy_object(url, quote(dst, safe=":/"))
    assert await secure.get_object(quote(dst, safe=":/")) == b"payload"


# Review focus 3: caller context on copy and move.


async def test_copy_move_keep_caller_context(secure, store):
    src, dst, dst2 = f"{store}/a", f"{store}/b", f"{store}/c"
    await secure.put_object(src, b"data", context={"tenant": "acme"})
    src_ctx = (await secure.inspect_encryption(src))["context"]
    await secure.copy_object(src, dst)
    dst_ctx = (await secure.inspect_encryption(dst))["context"]
    assert dst_ctx["tenant"] == "acme"
    assert dst_ctx["osc:key"] != src_ctx["osc:key"]
    assert {k: v for k, v in dst_ctx.items() if k != "osc:key"} == {
        k: v for k, v in src_ctx.items() if k != "osc:key"
    }
    assert await secure.get_object(dst) == b"data"
    await secure.move_object(dst, dst2)
    assert (await secure.inspect_encryption(dst2))["context"]["tenant"] == "acme"
    assert await secure.get_object(dst2) == b"data"
    assert not await secure.object_exists(dst)


# Review focus 4: wrong chunk types.


@pytest.mark.parametrize("bad", ["a str", 5])
async def test_wrong_chunk_type(secure, plain, store, bad):
    for client in (secure, plain):
        url = f"{store}/bad.bin"
        with pytest.raises(TypeError):
            await client.put_object_stream(url, chunks(b"ok", bad))
        assert not await plain.object_exists(url)


async def test_bind_object_key_false_allows_rename(plain, keyring, store):
    secure = plain.with_encryption(keyring, bind_object_key=False)
    await secure.put_object(f"{store}/one", b"data")
    await plain.copy_object(f"{store}/one", f"{store}/two")
    assert await secure.get_object(f"{store}/two") == b"data"


async def test_reserved_prefix_is_case_insensitive_in_context_only_for_callers(secure, store):
    # Library entries keep their exact lowercase names; only caller keys are checked.
    url = f"{store}/ci.bin"
    await secure.put_object(url, b"x", context={"tenant": "acme", "oscar": "ok"})
    ctx = (await secure.inspect_encryption(url))["context"]
    assert ctx["osc:fmt"] == "1" and ctx["oscar"] == "ok"


def test_wraps_counts_wraps_of_this_keyring(keyring, keyring_path, store):
    assert keyring.wraps == 0
    secure = EncryptedObjectStorageClient(keyring)

    async def go():
        await secure.put_object(f"{store}/w1", b"a")
        await secure.put_object(f"{store}/w2", b"b")
        assert await secure.get_object(f"{store}/w1") == b"a"  # unwrap is not a wrap

    asyncio.run(go())
    assert keyring.wraps == 2
    assert Keyring.from_file(keyring_path).wraps == 0  # per instance, not persisted
    with pytest.raises(AttributeError):
        keyring.wraps = 5


async def test_reload_after_atomic_replace(plain, keyring_path, two_keyring_path, store):
    a = Keyring.from_file(keyring_path)
    client_a = plain.with_encryption(a)
    client_b = plain.with_encryption(Keyring.from_file(two_keyring_path))
    first = a.status()["active"]
    await client_b.put_object(f"{store}/new", b"wrapped by the second key")
    # A does not know the second key yet.
    assert a.status()["active"] == first
    replacement = keyring_path.with_name("replacement")
    shutil.copyfile(two_keyring_path, replacement)
    replacement.chmod(0o600)
    os.replace(replacement, keyring_path)
    # The unknown key forces a re-check at once; no waiting.
    assert await client_a.get_object(f"{store}/new") == b"wrapped by the second key"
    st = a.status()
    assert st["active"] != first
    assert len(st["keys"]) == 2
    assert st["last_reload_error"] is None


async def test_reencrypt(plain, keyring_path, two_keyring_path, store, tmp_path):
    old = Keyring.from_file(keyring_path)
    new = Keyring.from_file(two_keyring_path)
    first = old.status()["active"]
    url = f"{store}/r.bin"
    await plain.with_encryption(old).put_object(url, b"rotate me")
    secure = plain.with_encryption(new)
    path = tmp_path / "store" / "r.bin"
    before = path.read_bytes()

    assert await secure.reencrypt(url, first, dry_run=True) == "would-rewrite"
    assert path.read_bytes() == before
    assert await secure.reencrypt(url, first) == "rewritten"
    assert path.read_bytes() != before
    assert await secure.get_object(url) == b"rotate me"
    assert (await secure.inspect_encryption(url))["key_names"] == [new.status()["active"]]
    assert await secure.reencrypt(url, first) == "skipped"


async def test_reencrypt_cutoff_and_plain(plain, keyring_path, two_keyring_path, store):
    first = Keyring.from_file(keyring_path).status()["active"]
    secure = plain.with_encryption(Keyring.from_file(two_keyring_path))
    await plain.with_encryption(Keyring.from_file(keyring_path)).put_object(f"{store}/c", b"x")
    past = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    assert await secure.reencrypt(f"{store}/c", first, before=past) == "after-cutoff"
    await plain.put_object(f"{store}/p", b"plain")
    assert await secure.reencrypt(f"{store}/p", first) == "not-encrypted"


async def test_reencrypt_argument_errors(plain, two_keyring_path, store):
    keyring = Keyring.from_file(two_keyring_path)
    secure = plain.with_encryption(keyring)
    with pytest.raises(ValueError):
        await secure.reencrypt(f"{store}/x", keyring.status()["active"])
    with pytest.raises(ValueError):
        await secure.reencrypt(f"{store}/x", "other", before=datetime.datetime(2000, 1, 1))
