"""Native result conversion and upload batching must not park runtime workers.

Each child runs with ONE runtime worker and holds the GIL in a busy loop
(`sys.setswitchinterval(5)` keeps the interpreter from handing it over).
While the GIL is held, a worker that waits for it cannot run anything else.
After the hold, the child starts a download fill, which needs no GIL, and
checks that it completes while the GIL is still held. It only can if the one
worker is free.
"""

import os
import sys
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Linux only"
)

_ENV = {"OSC_WORKER_THREADS": "1"}

# Shared by both children: hold the GIL for `seconds`, then fill a stream and
# busy-poll (still holding the GIL) for whether the fill completed.
_HELPERS = """
import asyncio, os, sys, time
from object_storage_client import ObjectStorageClient

def busy(seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        pass

def fill_completes(stream, seconds):
    stream._raw.fill()
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if stream._raw.ready():
            return True
    return False
"""

_METADATA_CHILD = _HELPERS + textwrap.dedent(
    """
    async def main():
        client = ObjectStorageClient()
        path = os.path.join(sys.argv[1], "f.bin")
        with open(path, "wb") as f:
            f.write(b"x" * 1000)
        stream = await client.get_object_stream("file://" + path)
        # Not awaited: the HEAD completes while the GIL is held below.
        pending = asyncio.ensure_future(
            client.get_object_metadata(f"http://127.0.0.1:{sys.argv[2]}/x")
        )
        sys.setswitchinterval(5)
        busy(0.6)
        print(fill_completes(stream, 0.5))
        sys.setswitchinterval(0.005)
        print((await pending)["size_bytes"])

    asyncio.run(main())
    """
)

_UPLOAD_CHILD = _HELPERS + textwrap.dedent(
    """
    result = []

    async def main():
        client = ObjectStorageClient()
        path = os.path.join(sys.argv[1], "f.bin")
        with open(path, "wb") as f:
            f.write(b"x" * 1000)
        stream = await client.get_object_stream("file://" + path)
        loop = asyncio.get_running_loop()
        release = asyncio.Event()

        def hold():
            sys.setswitchinterval(5)
            busy(0.6)
            result.append(fill_completes(stream, 0.5))
            sys.setswitchinterval(0.005)
            release.set()

        async def source():
            # One batch (300 KiB is over the 256 KiB limit). Two loop turns
            # later `hold` runs right after the batch was delivered and before
            # the uploader has scheduled the next one.
            loop.call_soon(lambda: loop.call_soon(hold))
            yield b"y" * (300 * 1024)
            await release.wait()

        await client.put_object_stream("file://" + sys.argv[1] + "/out.bin", source())
        print(result[0])

    asyncio.run(main())
    """
)


def _head_server():
    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            # Answer after 300 ms, in the middle of the child's GIL hold.
            threading.Event().wait(0.3)
            self.send_response(200)
            self.send_header("Content-Length", "7")
            self.send_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_result_conversion_does_not_block_the_worker(run_isolated, tmp_path):
    server, thread = _head_server()
    try:
        proc = run_isolated(
            _METADATA_CHILD,
            str(tmp_path),
            str(server.server_address[1]),
            env=_ENV,
            timeout=60,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["True", "7"], proc.stdout


def test_upload_batching_does_not_block_the_worker(run_isolated, tmp_path):
    proc = run_isolated(_UPLOAD_CHILD, str(tmp_path), env=_ENV, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["True"], proc.stdout
    assert os.path.getsize(tmp_path / "out.bin") == 300 * 1024
