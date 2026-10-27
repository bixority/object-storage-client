#!/usr/bin/env python3
"""AWS Encryption SDK oracle for the OSC encryption profile.

Subcommands:
  encrypt  --key-hex H --key-name N [--namespace osc-file] --frame-length F
           [--context k=v ...] --in P --out C
  decrypt  --key-hex H --key-name N [--namespace osc-file] [--context k=v ...]
           --in C --out P
           Strict: the message must be suite 0x0478, framed, wrapped by
           provider_id == namespace, and end exactly at the final frame.
           Every --context pair must be present in the message.
  inspect  FILE [--key-hex H --key-name N] [--namespace osc-file]
           Print the parsed structure as JSON. The DEK is never printed. With
           --key-name, also require an EDK for exactly that key name.
  vectors  --out-dir D [--append]
           Regenerate tests/vectors/esdk (TEST-ONLY key, see README.md there).
           --append only generates the EXTRA vectors and merges them into the
           existing manifest, leaving existing ciphertexts untouched.

Run with: uv run --group oracle python scripts/esdk_oracle.py ...
"""

import argparse
import hashlib
import io
import json
import struct
import sys
from pathlib import Path

import aws_encryption_sdk
from aws_encryption_sdk import CommitmentPolicy
from aws_encryption_sdk.identifiers import Algorithm, EncryptionKeyType, WrappingAlgorithm
from aws_encryption_sdk.internal.formatting import deserialize
from aws_encryption_sdk.internal.formatting.encryption_context import serialize_encryption_context
from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
from aws_encryption_sdk.key_providers.raw import RawMasterKey

ALGORITHM = Algorithm.AES_256_GCM_HKDF_SHA512_COMMIT_KEY
WRAPPING = WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING
DEFAULT_NAMESPACE = "osc-file"

# TEST-ONLY key: never use outside tests/vectors.
TEST_KEK_HEX = bytes(range(32)).hex()
TEST_KEY_NAME = "kek-test-0001"
TEST_CONTEXT = {"osc:fmt": "1", "osc:key": "dir/file.bin", "tenant": "acme"}
SIZES = [0, 1, 4095, 4096, 4097, 65535, 65536, 65537, 200000]
FRAME_LENGTHS = [4096, 65536]


def plaintext(n):
    return bytes((i * 7 + 3) & 0xFF for i in range(n))


def client():
    return aws_encryption_sdk.EncryptionSDKClient(
        commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    )


def master_key(key_hex, key_name, namespace):
    wrapping_key = WrappingKey(
        wrapping_algorithm=WRAPPING,
        wrapping_key=bytes.fromhex(key_hex),
        wrapping_key_type=EncryptionKeyType.SYMMETRIC,
    )
    return RawMasterKey(
        provider_id=namespace, key_id=key_name.encode(), wrapping_key=wrapping_key
    )


def parse_context(pairs):
    ctx = {}
    for p in pairs or []:
        k, sep, v = p.partition("=")
        if not sep:
            raise SystemExit(f"bad --context {p!r}, expected k=v")
        ctx[k] = v
    return ctx


def do_encrypt(key_hex, key_name, namespace, frame_length, context, data):
    ct, _ = client().encrypt(
        source=data,
        key_provider=master_key(key_hex, key_name, namespace),
        encryption_context=context,
        frame_length=frame_length,
        algorithm=ALGORITHM,
    )
    return ct


def fail(msg):
    raise SystemExit(f"esdk_oracle: {msg}")


def do_decrypt(key_hex, key_name, namespace, data):
    # Strict structure checks first (trailing bytes, suite, content type,
    # provider); the SDK itself ignores data after the final frame.
    info = inspect(data, key_name=key_name, namespace=namespace)
    try:
        pt, header = client().decrypt(
            source=data, key_provider=master_key(key_hex, key_name, namespace)
        )
    except Exception as exc:  # noqa: BLE001 - surface any SDK failure cleanly
        fail(f"decrypt failed: {type(exc).__name__}: {exc}")
    return pt, header, info


def need(body, pos, n, what):
    if pos + n > len(body):
        fail(f"truncated body: {what} needs {n} bytes at offset {pos}")


def scan_frames(body, frame_length):
    """Describe the body frames: list of {seq, final, content_len}.

    Stops at the final frame, checks sequence numbers, and requires the body
    to end exactly there.
    """
    if frame_length == 0:
        fail("frame_length 0 in a framed message")
    frames, pos, expect = [], 0, 1
    while True:
        need(body, pos, 4, "sequence number")
        (first,) = struct.unpack_from(">I", body, pos)
        if first == 0xFFFFFFFF:
            need(body, pos, 4 + 4 + 12 + 4, "final frame header")
            (seq,) = struct.unpack_from(">I", body, pos + 4)
            (clen,) = struct.unpack_from(">I", body, pos + 4 + 4 + 12)
            if clen > frame_length:
                fail(f"final frame content {clen} exceeds frame length")
            need(body, pos, 4 + 4 + 12 + 4 + clen + 16, "final frame")
            if seq != expect:
                fail(f"final frame seq {seq}, expected {expect}")
            frames.append({"seq": seq, "final": True, "content_len": clen})
            pos += 4 + 4 + 12 + 4 + clen + 16
            break
        need(body, pos, 4 + 12 + frame_length + 16, "frame")
        if first != expect:
            fail(f"frame seq {first}, expected {expect}")
        frames.append({"seq": first, "final": False, "content_len": frame_length})
        pos += 4 + 12 + frame_length + 16
        expect += 1
    if pos != len(body):
        fail(f"{len(body) - pos} trailing bytes after the final frame")
    return frames


def check_edk(edk, key_name, namespace):
    """Exact Raw AES provider_info: name || tag_bits(4)=128 || iv_len(4)=12 || iv."""
    if edk.key_provider.provider_id != namespace:
        return False
    info = edk.key_provider.key_info
    name = key_name.encode()
    n = len(name)
    if not info.startswith(name) or len(info) != n + 8 + 12:
        return False
    tag_bits, iv_len = struct.unpack_from(">II", info, n)
    if tag_bits != 128:
        fail(f"EDK tag_bits {tag_bits}, expected 128")
    return iv_len == 12


def inspect(data, key_name=None, namespace=DEFAULT_NAMESPACE):
    stream = io.BytesIO(data)
    try:
        header, _raw = deserialize.deserialize_header(stream)
        auth = deserialize.deserialize_header_auth(
            header.version, stream, header.algorithm
        )
    except Exception as exc:  # noqa: BLE001
        fail(f"cannot parse header: {type(exc).__name__}: {exc}")
    header_len = stream.tell()
    algorithm_id = header.algorithm.algorithm_id
    if algorithm_id != 0x0478:
        fail(f"algorithm 0x{algorithm_id:04x}, expected 0x0478")
    if header.content_type.value != 2:
        fail(f"content type {header.content_type.value}, expected framed (2)")
    ser_ctx = serialize_encryption_context(header.encryption_context)
    edks = []
    for edk in header.encrypted_data_keys:
        if edk.key_provider.provider_id != namespace:
            fail(f"provider_id {edk.key_provider.provider_id!r}, expected {namespace!r}")
        edks.append(
            {
                "provider_id": edk.key_provider.provider_id,
                "provider_info_hex": edk.key_provider.key_info.hex(),
                "ciphertext_hex": edk.encrypted_data_key.hex(),
            }
        )
    if key_name is not None and not any(
        check_edk(e, key_name, namespace) for e in header.encrypted_data_keys
    ):
        fail(f"no EDK for key name {key_name!r}")
    return {
        "version": header.version.value,
        "algorithm_id": algorithm_id,
        "message_id_hex": header.message_id.hex(),
        "encryption_context": dict(header.encryption_context),
        "serialized_context_hex": ser_ctx.hex(),
        "edks": edks,
        "content_type": header.content_type.value,
        "frame_length": header.frame_length,
        # header_length includes the 16-byte header auth tag; header_body_length
        # is the authenticated part before the tag.
        "header_body_length": header_len - len(auth.tag),
        "header_length": header_len,
        "suite_data_hex": header.commitment_key.hex(),
        "header_auth_tag_hex": auth.tag.hex(),
        "frames": scan_frames(data[header_len:], header.frame_length),
    }


def vector_specs(extra_only):
    specs = []
    if not extra_only:
        for fl in FRAME_LENGTHS:
            for n in SIZES:
                specs.append((f"p{n}_f{fl}.bin", n, fl, TEST_CONTEXT))
        specs.append(("ctx_min.bin", 5000, 4096, {"osc:fmt": "1"}))
        long_ctx = dict(TEST_CONTEXT, tenant="x" * 300)
        specs.append(("ctx_long.bin", 5000, 4096, long_ctx))
    # EXTRA vectors: added after the first generation, never regenerated alone.
    utf8_ctx = {
        "osc:fmt": "1",
        "osc:key": "dir/\u00fc %20 \u00e9.bin",
        "tenant": "acme",
        "z\u00e9": "na\u00efve \u5024",
    }
    specs.append(("ctx_utf8.bin", 5000, 4096, utf8_ctx))
    return specs


def make_vectors(out_dir, append=False):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.json"
    entries = []
    if append:
        manifest = json.loads(manifest_path.read_text())
        entries = manifest["vectors"]
        for e in entries:  # backfill, ciphertexts are untouched
            ins = e["inspect"]
            ins.setdefault("header_body_length", ins["header_length"] - 16)
    for name, n, fl, ctx in vector_specs(append):
        pt = plaintext(n)
        ct = do_encrypt(TEST_KEK_HEX, TEST_KEY_NAME, DEFAULT_NAMESPACE, fl, ctx, pt)
        (out / name).write_bytes(ct)
        got, _, info = do_decrypt(TEST_KEK_HEX, TEST_KEY_NAME, DEFAULT_NAMESPACE, ct)
        if got != pt:
            fail(f"round trip mismatch for {name}")
        entry = {
            "file": name,
            "plaintext_sha256": hashlib.sha256(pt).hexdigest(),
            "plaintext_size": n,
            "frame_length": fl,
            "context": ctx,
            "inspect": info,
        }
        entries = [e for e in entries if e["file"] != name] + [entry]
    if append:
        manifest["vectors"] = entries
    else:
        keep = {e["file"] for e in entries}
        for stale in out.glob("*.bin"):
            if stale.name not in keep:
                stale.unlink()
        manifest = {
            "note": "TEST-ONLY key material. Plaintext(n) = bytes((i*7+3)&0xff for i in range(n)).",
            "sdk_version": aws_encryption_sdk.__version__,
            "kek_hex": TEST_KEK_HEX,
            "key_name": TEST_KEY_NAME,
            "provider_id": DEFAULT_NAMESPACE,
            "vectors": entries,
        }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--key-hex", required=True)
        p.add_argument("--key-name", required=True)
        p.add_argument("--namespace", default=DEFAULT_NAMESPACE)

    e = sub.add_parser("encrypt")
    common(e)
    e.add_argument("--frame-length", type=int, required=True)
    e.add_argument("--context", action="append")
    e.add_argument("--in", dest="inp", required=True)
    e.add_argument("--out", required=True)

    d = sub.add_parser("decrypt")
    common(d)
    d.add_argument("--context", action="append")
    d.add_argument("--in", dest="inp", required=True)
    d.add_argument("--out", required=True)

    i = sub.add_parser("inspect")
    i.add_argument("file")
    i.add_argument("--key-hex")  # accepted for symmetry; never used
    i.add_argument("--key-name")
    i.add_argument("--namespace", default=DEFAULT_NAMESPACE)

    v = sub.add_parser("vectors")
    v.add_argument("--out-dir", required=True)
    v.add_argument("--append", action="store_true")

    a = ap.parse_args()
    if a.cmd == "encrypt":
        ct = do_encrypt(
            a.key_hex, a.key_name, a.namespace, a.frame_length,
            parse_context(a.context), Path(a.inp).read_bytes(),
        )
        Path(a.out).write_bytes(ct)
    elif a.cmd == "decrypt":
        pt, header, _ = do_decrypt(
            a.key_hex, a.key_name, a.namespace, Path(a.inp).read_bytes()
        )
        for k, val in parse_context(a.context).items():
            if header.encryption_context.get(k) != val:
                fail(f"context mismatch for {k!r}")
        Path(a.out).write_bytes(pt)
    elif a.cmd == "inspect":
        json.dump(
            inspect(Path(a.file).read_bytes(), a.key_name, a.namespace),
            sys.stdout, indent=2, sort_keys=True, ensure_ascii=False,
        )
        print()
    else:
        make_vectors(a.out_dir, a.append)


if __name__ == "__main__":
    main()
