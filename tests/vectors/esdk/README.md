# ESDK reference vectors

Ciphertexts produced by the AWS Encryption SDK for Python (the oracle), used to
check that our header codec, key derivation and framing are byte-compatible.

**All keys here are TEST-ONLY.** The KEK is `000102...1f` (hex), key name
`kek-test-0001`, provider ID `osc-file`. Rust tests unwrap each message's EDK
with this KEK through the real Raw AES code path. Never use these values
outside tests.

Plaintext of size `n` is `bytes((i*7+3) & 0xff for i in range(n))`.
Files are named `p<size>_f<frame_length>.bin`, plus `ctx_min.bin` (context
`{"osc:fmt":"1"}` only) and `ctx_long.bin` (a 300-byte caller value); both use plaintext size 5000 and
frame length F=4096. `ctx_utf8.bin` (also 5000 / 4096) has `osc:key` =
`dir/ü %20 é.bin` plus a non-ASCII caller key and value, which pins the UTF-8
encoding and the length prefixes of non-ASCII context strings. It does not
pin the sort order of keys (byte order versus UTF-16 order).

## Regenerate

    uv run --group oracle python scripts/esdk_oracle.py vectors --out-dir tests/vectors/esdk

Ciphertexts differ on every run (random message ID, DEK and IV); the parsed
structure (sizes, frame layout, header length) is stable. Commit one generation
and do not regenerate casually. A full run also removes `*.bin` files that are
not in the new manifest. To add only the vectors marked EXTRA in the script,
without touching existing ciphertexts, pass `--append`.

`inspect.header_length` includes the 16-byte header auth tag;
`inspect.header_body_length` is the part before the tag.

## Other oracle commands

Run each as `uv run --group oracle python scripts/esdk_oracle.py ...`:

    encrypt --key-hex H --key-name N [--namespace osc-file] --frame-length F [--context k=v ...] --in P --out C
    decrypt --key-hex H --key-name N [--namespace osc-file] [--context k=v ...] --in C --out P
    inspect FILE [--key-hex H --key-name N] [--namespace osc-file]

`--context` is repeated once per pair. `decrypt` is strict: it rejects bytes after
the final frame, any suite other than 0x0478, non-framed content and a foreign
provider ID, and requires every given `--context` pair to be present.
