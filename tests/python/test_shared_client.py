import pytest


async def test_put_get_roundtrip(any_client, store):
    url = f"{store}/a.bin"
    await any_client.put_object(url, b"hello world")
    assert (await any_client.get_object(url)) == b"hello world"


async def test_put_accepts_list_of_ints(any_client, store):
    url = f"{store}/l.bin"
    await any_client.put_object(url, [1, 2, 3])
    assert (await any_client.get_object(url)) == b"\x01\x02\x03"


async def test_object_exists(any_client, store):
    url = f"{store}/e.bin"
    assert await any_client.object_exists(url) is False
    await any_client.put_object(url, b"x")
    assert await any_client.object_exists(url) is True


async def test_list_objects_relative_keys(any_client, store):
    await any_client.put_object(f"{store}/dir/one.bin", b"1")
    await any_client.put_object(f"{store}/dir/two.bin", b"2")
    keys = await any_client.list_objects(f"{store}/dir")
    assert sorted(keys) == ["one.bin", "two.bin"]


async def test_copy_object(any_client, store):
    src, dst = f"{store}/src.bin", f"{store}/dst.bin"
    await any_client.put_object(src, b"data")
    await any_client.copy_object(src, dst)
    assert (await any_client.get_object(dst)) == b"data"
    assert await any_client.object_exists(src)


async def test_move_object(any_client, store):
    src, dst = f"{store}/src.bin", f"{store}/dst.bin"
    await any_client.put_object(src, b"data")
    await any_client.move_object(src, dst)
    assert (await any_client.get_object(dst)) == b"data"
    assert not await any_client.object_exists(src)


async def test_delete_object(any_client, store):
    url = f"{store}/d.bin"
    await any_client.put_object(url, b"x")
    assert await any_client.object_exists(url)
    await any_client.delete_object(url)
    assert not await any_client.object_exists(url)


async def test_delete_objects_by_prefix(any_client, store):
    await any_client.put_object(f"{store}/p/a.bin", b"a")
    await any_client.put_object(f"{store}/p/b.bin", b"b")
    await any_client.put_object(f"{store}/keep.bin", b"k")
    assert sorted(await any_client.list_objects(f"{store}/p")) == ["a.bin", "b.bin"]
    await any_client.delete_objects_by_prefix(f"{store}/p")
    assert await any_client.list_objects(f"{store}/p") == []
    assert await any_client.object_exists(f"{store}/keep.bin")


async def test_get_missing_object_raises_file_not_found(any_client, store):
    with pytest.raises(FileNotFoundError):
        await any_client.get_object(f"{store}/missing.bin")


async def test_bucket_calls_on_file(any_client, store):
    bucket = f"{store}/newbucket"
    assert await any_client.bucket_exists(bucket) is False
    await any_client.create_bucket(bucket)
    assert await any_client.bucket_exists(bucket) is True
