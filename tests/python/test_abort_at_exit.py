"""The interpreter waits at exit for the aborts of cancelled multipart uploads."""

import sys
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Linux only"
)

_INITIATE = (
    b'<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult>'
    b"<Bucket>bkt</Bucket><Key>obj</Key><UploadId>u1</UploadId>"
    b"</InitiateMultipartUploadResult>"
)

_CHILD = textwrap.dedent(
    """
    import asyncio, os, sys, time
    from object_storage_client import ObjectStorageClient

    async def source():
        yield b"x" * (8 * 1024 * 1024)
        await asyncio.Event().wait()

    async def main():
        client = ObjectStorageClient()
        task = asyncio.ensure_future(
            client.put_object_stream("s3://bkt/obj", source())
        )
        deadline = time.monotonic() + 30
        while not os.path.exists(sys.argv[1]):
            assert time.monotonic() < deadline, "no part was uploaded"
            await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(main())
    """
)

_RUNTIME_THREADS = """
import os
def workers():
    n = 0
    for t in os.listdir("/proc/self/task"):
        with open(f"/proc/self/task/{t}/comm") as f:
            n += f.read().startswith("tokio-rt-worker")
    return n
"""


def _fake_s3(marker, aborted):
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, status, body=b"", headers=()):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            for k, v in headers:
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            self._reply(200, _INITIATE)

        def do_PUT(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            marker.write_text("part")
            self._reply(200, headers=[("ETag", '"e1"')])

        def do_DELETE(self):
            # A slow abort: the process must outlive it.
            threading.Event().wait(1)
            aborted.set()
            self._reply(204)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_exit_waits_for_a_cancelled_multipart_upload_to_abort(run_isolated, tmp_path):
    marker = tmp_path / "part-uploaded"
    aborted = threading.Event()
    server, thread = _fake_s3(marker, aborted)
    try:
        proc = run_isolated(
            _CHILD,
            str(marker),
            env={
                "S3_ENDPOINT": f"http://127.0.0.1:{server.server_address[1]}",
                "S3_ALLOW_HTTP": "true",
                "S3_REGION": "us-east-1",
                "S3_ACCESS_KEY_ID": "test-only-key-id",
                "S3_SECRET_ACCESS_KEY": "test-only-secret",
            },
            timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert aborted.is_set(), "the child exited before the abort finished"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_exit_is_immediate_without_pending_aborts(run_isolated, tmp_path):
    code = textwrap.dedent(
        """
        import asyncio, sys
        from object_storage_client import ObjectStorageClient

        async def main():
            c = ObjectStorageClient()
            await c.put_object("file://" + sys.argv[1] + "/o", b"x")

        asyncio.run(main())
        """
    )
    proc = run_isolated(code, str(tmp_path), timeout=5)
    assert proc.returncode == 0, proc.stderr


def test_exit_builds_no_runtime_when_no_call_was_made(run_isolated):
    # Registered before the import, so it runs after the module's exit hook.
    code = (
        _RUNTIME_THREADS
        + textwrap.dedent(
            """
            import atexit
            atexit.register(lambda: print("workers", workers()))
            import object_storage_client
            """
        )
    )
    proc = run_isolated(code, timeout=30)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["workers", "0"], proc.stdout
