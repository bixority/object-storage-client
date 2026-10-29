"""`profile=` reaches S3 signing from Python.

Only the wiring is proven here: the named profile's key ID and region end up
in a pre-signed URL. Pre-signing is local, so nothing is contacted.
"""

import json
import textwrap

import pytest

_CONFIG = textwrap.dedent(
    """
    [profile dev]
    aws_access_key_id = AKIDPYTEST
    aws_secret_access_key = test-only
    region = eu-west-3

    [profile decoy]
    aws_access_key_id = AKIDDECOY
    aws_secret_access_key = test-only-decoy
    """
)

# Prints a JSON dict of case name -> pre-signed URL or error, for the cases
# `names` selects. `keyring` is a static test keyring file.
_CHILD = textwrap.dedent(
    """
    import asyncio, json, sys
    from object_storage_client import (
        EncryptedObjectStorageClient, Keyring, ObjectStorageClient,
    )

    URL = "s3://bkt/k"
    keyring = Keyring.from_file(sys.argv[1])
    cases = {
        "plain": lambda: ObjectStorageClient(profile="dev"),
        "view": lambda: ObjectStorageClient(profile="dev")
            .with_encryption(keyring).without_encryption(),
        "encrypted": lambda: EncryptedObjectStorageClient(keyring, profile="dev")
            .without_encryption(),
        "missing": lambda: ObjectStorageClient(profile="missing"),
    }

    async def main():
        out = {}
        for name in sys.argv[2:]:
            try:
                out[name] = await cases[name]().get_pre_signed_url(URL)
            except Exception as e:
                out[name] = {"error": type(e).__name__, "message": str(e)}
        print(json.dumps(out))

    asyncio.run(main())
    """
)


@pytest.fixture
def presign(run_isolated, tmp_path, keyring_path):
    config = tmp_path / "aws-config"
    config.write_text(_CONFIG)

    def run(*names: str, extra_env=None) -> dict:
        env = {
            "AWS_CONFIG_FILE": str(config),
            "AWS_SHARED_CREDENTIALS_FILE": str(tmp_path / "no-credentials"),
            # Never contacted: pre-signing is local.
            "S3_ENDPOINT": "http://127.0.0.1:9",
            "AWS_PROFILE": "decoy",
            **(extra_env or {}),
        }
        proc = run_isolated(_CHILD, str(keyring_path), *names, env=env, timeout=60)
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout)

    return run


def _assert_signed_by_dev(url: str) -> None:
    assert "X-Amz-Credential=AKIDPYTEST%2F" in url
    assert "%2Feu-west-3%2Fs3%2F" in url
    assert "AKIDDECOY" not in url


def test_profile_signs_presigned_urls(presign):
    _assert_signed_by_dev(presign("plain")["plain"])


def test_encrypted_views_keep_the_profile(presign):
    got = presign("view", "encrypted")
    assert set(got) == {"view", "encrypted"}
    for url in got.values():
        _assert_signed_by_dev(url)


def test_explicit_profile_beats_static_keys(presign):
    # Decision D2: an explicit profile is the only S3 credential source.
    got = presign(
        "plain",
        extra_env={
            "S3_ACCESS_KEY_ID": "AKIDSTATIC",
            "S3_SECRET_ACCESS_KEY": "static-test-only",
        },
    )["plain"]
    _assert_signed_by_dev(got)
    assert "AKIDSTATIC" not in got


def test_missing_profile_is_an_error_not_a_fallback(presign):
    got = presign("missing")["missing"]
    assert got["error"] == "RuntimeError"
    assert "`missing`" in got["message"]
    assert "169.254" not in got["message"]
