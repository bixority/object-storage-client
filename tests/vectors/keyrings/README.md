# Test keyrings

`one.keyring` and `two.keyring` are TEST-ONLY keyring files. Their keys are
committed to the repository, so they protect nothing. Never use them for real
data.

- `one.keyring`: one key, active.
- `two.keyring`: the key of `one.keyring` plus a second key, which is active.

- `other.keyring`: an unrelated single key (a keyring that cannot read the
  others' objects).

They were made with the Rust CLI:

```sh
osc keyring new --prefix test > one.keyring
osc keyring rotate --prefix test < one.keyring > two.keyring
osc keyring new --prefix other > other.keyring
```

The Python tests copy them to a private temporary file (mode 0600) before use,
because a keyring file must not be accessible to others.
