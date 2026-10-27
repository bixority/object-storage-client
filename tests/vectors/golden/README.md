# Golden ciphertexts

`v1/` holds objects written by the v1 encryptor (ESDK v2, suite 0x0478, framed
64 KiB, no object-key binding) plus their keyring in `v1/keyring` (both keys, the newer one active).

- The keys are TEST-ONLY material. They protect nothing; never use them.
- `manifest.json` records the plaintext length, SHA-256, key ID and context of
  every file. `size_*.bin` and `context.bin` use the newer (active) key;
  `old_key.bin` was written under the older key while the newer one was active.
- Git does not keep file modes; the test `golden_v1_decrypts` reads the text
  and uses `Keyring::parse`, which has no file checks.
- CI never regenerates these. To regenerate (replaces the keyring too):
  `cargo test --test functional regenerate_golden_v1 -- --ignored`
