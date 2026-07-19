# Object Storage Client

A unified object storage client for Rust and Python, supporting S3, GCS, Azure Blob Storage, HTTP/HTTPS, and Local Filesystem. It provides a simple, URL-based API for object operations, including cross-provider copy and move.

## Features

- **Unified API**: Single interface for various storage backends.
- **Cross-Provider**: Copy or move objects between different storage providers (e.g., S3 to Local FS).
- **Listing**: List a prefix, or the whole bucket from a bucket-root URL (e.g. `s3://bucket`). Listing is flat and recursive — every key under the prefix is returned, not just the immediate level.
- **Existence checks**: Test whether an object or bucket exists without raising on a miss.
- **Bucket creation**: Create buckets/containers on S3, GCS and Azure (or directories for local paths).
- **Pre-signed URLs**: Generate time-limited, credential-free URLs for S3, GCS and Azure.
- **Multi-Language**: Native Rust library with Python 3.13+ bindings.
- **Streaming**: Async streaming support for both Rust and Python.
- **CLI**: `osc` command-line tool for quick operations.

## Supported Schemes

- `s3://bucket/path` (AWS S3)
- `gs://bucket/path` or `gcs://bucket/path` (Google Cloud Storage)
- `az://`, `wasb://`, `wasbs://`, `abfs://`, or `abfss://` (Azure Blob Storage)
- `http://host/path` or `https://host/path` (HTTP/HTTPS)
- `file:///absolute/path` or `local_path` (Local Filesystem)

For the cloud schemes the host is the bucket / container and the path is the
object key. The path is optional: a bucket-root URL such as `s3://bucket` (or
`s3://bucket/`) is valid and addresses the bucket itself — use it to list the
whole bucket, create it, or check that it exists.

---

## Environment Variables

Credentials are read from the environment the first time a backend is used — the
client never takes them as constructor arguments. The bucket / container always
comes from the URL host, so a single process can talk to several buckets across
several providers at once. That is exactly what makes the cross-provider copy
and move shown in the walk-throughs below work: export the variables for every
provider you touch, and a single `ObjectStorageClient` can shuttle objects
between them. The local filesystem needs no variables.

### AWS S3 (`s3://`)

Also covers S3-compatible stores such as MinIO and SeaweedFS.

```bash
export S3_ACCESS_KEY_ID="AKIA..."
export S3_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-east-1"
# Optional: temporary credentials
export AWS_SESSION_TOKEN="..."
# Optional: custom endpoint for S3-compatible stores (e.g. MinIO)
export AWS_ENDPOINT_URL_S3="http://localhost:9000"

# Convenience overrides honoured by this client (take precedence when set):
#   S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
# Allow plain HTTP (e.g. a local MinIO):
#   export S3_ALLOW_HTTP=true
```

### Google Cloud Storage (`gs://` / `gcs://`)

```bash
# Path to a service-account JSON key file...
export GOOGLE_SERVICE_ACCOUNT="/path/to/service-account.json"
# (GOOGLE_SERVICE_ACCOUNT_PATH and the standard
#  GOOGLE_APPLICATION_CREDENTIALS are also recognised.)

# ...or the service-account JSON supplied inline instead of a path:
#   export GOOGLE_SERVICE_ACCOUNT_KEY='{"type":"service_account", ...}'
```

### Azure Blob Storage (`az://`, `wasb(s)://`, `abfs(s)://`)

```bash
export AZURE_STORAGE_ACCOUNT_NAME="mystorageaccount"

# Pick ONE authentication method:
# 1. Shared account key
export AZURE_STORAGE_ACCOUNT_KEY="..."
# 2. Shared Access Signature (SAS) token
#   export AZURE_STORAGE_SAS_KEY="?sv=..."
# 3. Service principal (Azure AD)
#   export AZURE_STORAGE_CLIENT_ID="..."
#   export AZURE_STORAGE_CLIENT_SECRET="..."
#   export AZURE_STORAGE_TENANT_ID="..."
```

### Local Filesystem (`file://`)

No environment variables — no credentials are required.

---

## CLI Usage (`osc`)

The `osc` tool allows you to interact with object storage directly from your terminal.

### Installation

Install the `osc` binary directly from the [Bixority Codeberg crate registry](https://codeberg.org/bixority/object-storage-client) with Cargo. Point Cargo at the registry with an environment variable, then install:

```bash
export CARGO_REGISTRIES_BIXORITY_INDEX="sparse+https://codeberg.org/api/packages/bixority/cargo/"
cargo install object-storage-client --registry bixority
```

Alternatively, install straight from Git:

```bash
cargo install --git https://codeberg.org/bixority/object-storage-client
```

Or, if you have the source code, install it from the local checkout:

```bash
cargo install --path .
```

### Examples

- **Upload a local file**:
  ```bash
  osc put my_file.txt s3://my-bucket/remote_file.txt
  ```

- **Upload a directory recursively**:
  ```bash
  osc put --recursive my_dir s3://my-bucket/remote_dir/
  ```

- **Download an object**:
  ```bash
  osc get gs://my-bucket/data.json ./local_data.json
  ```

- **Copy between providers**:
  ```bash
  osc cp s3://source-bucket/image.png az://dest-container/image.png
  ```

- **Move an object**:
  ```bash
  osc mv s3://my-bucket/old_name.txt s3://my-bucket/new_name.txt
  ```

- **List objects**:
  ```bash
  osc ls s3://my-bucket/logs/
  ```

- **Delete an object**:
  ```bash
  osc rm s3://my-bucket/temp_file.tmp
  ```

- **Check whether an object exists** (prints `true`/`false`):
  ```bash
  osc exists s3://my-bucket/report.pdf
  ```

- **Create a bucket** (S3, GCS, Azure, or a directory for local paths):
  ```bash
  osc mb s3://my-new-bucket
  ```

- **Check whether a bucket exists** (prints `true`/`false`):
  ```bash
  osc bucket-exists s3://my-bucket
  ```

- **Stream an object**:
  ```bash
  osc get-stream gs://my-bucket/large_file.bin
  ```

- **Generate a pre-signed URL** (S3, GCS, Azure):
  ```bash
  # Pre-signed download URL, valid for the default 1 hour
  osc sign s3://my-bucket/report.pdf

  # Pre-signed upload URL (PUT), valid for 15 minutes
  osc sign --method PUT --expires-in 900 s3://my-bucket/upload.bin

  # Pre-signed upload URL binding the exact size and type the client must send
  # (S3 only): the upload is rejected unless Content-Length and Content-Type
  # match, so the object store enforces size/type up front.
  osc sign --method PUT --content-length 1048576 \
      --content-type application/pdf s3://my-bucket/upload.pdf
  ```

---

## Rust Usage

### Installation

The crate is published to the [Bixority Codeberg crate registry](https://codeberg.org/bixority/object-storage-client). Point Cargo at the registry with an environment variable:

```bash
export CARGO_REGISTRIES_BIXORITY_INDEX="sparse+https://codeberg.org/api/packages/bixority/cargo/"
```

Then add `object-storage-client` to your `Cargo.toml`:

```toml
[dependencies]
object-storage-client = { version = "0.0.36", registry = "bixority" }
tokio = { version = "1.0", features = ["full"] }
```

Alternatively, you can depend on it directly from Git:

```toml
[dependencies]
object-storage-client = { git = "https://codeberg.org/bixority/object-storage-client" }
tokio = { version = "1.0", features = ["full"] }
```

### Walk-through

A single client works across every provider — the scheme in each URL selects the
backend, so you can upload to S3, then copy or move the object straight to GCS,
Azure or the local disk with no intermediate download on your side.

```rust
use object_storage_client::{ObjectStorageClient, SignMethod, SignOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();

    // Create a bucket (S3/GCS/Azure, or a directory for file:// URLs); idempotent
    client.create_bucket("s3://my-bucket").await?;

    // Upload data to S3
    client.put("s3://my-bucket/hello.txt", &b"Hello from Rust!"[..]).await?;

    // Download it back
    let retrieved = client.get("s3://my-bucket/hello.txt").await?;
    println!("Retrieved: {}", String::from_utf8_lossy(&retrieved));

    // List the whole bucket from a bucket-root URL (flat, recursive: every key
    // is returned). Pass a prefix such as "s3://my-bucket/logs/" to narrow it.
    let keys = client.list("s3://my-bucket").await?;
    println!("Bucket keys: {keys:?}");

    // Existence checks (missing -> Ok(false), never an error)
    if client.bucket_exists("s3://my-bucket").await? {
        println!("my-bucket is present");
    }
    if client.exists("s3://my-bucket/hello.txt").await? {
        println!("hello.txt is present");
    }

    // --- Move data across providers with one client ---

    // Copy S3 -> Google Cloud Storage (source is left in place)
    client
        .copy("s3://my-bucket/hello.txt", "gs://my-gcs-bucket/hello.txt")
        .await?;

    // Move GCS -> Azure Blob Storage (source is deleted afterwards)
    client
        .move_object("gs://my-gcs-bucket/hello.txt", "az://my-container/hello.txt")
        .await?;

    // Copy Azure -> local disk for a working copy
    client
        .copy("az://my-container/hello.txt", "file:///tmp/hello_local.txt")
        .await?;

    // --- Pre-signed URLs: time-limited, credential-free access (S3/GCS/Azure) ---

    // Pre-signed download (GET) link, valid for one hour
    let download_url = client
        .get_pre_signed_url(
            "s3://my-bucket/hello.txt",
            SignMethod::Get,
            Duration::from_secs(3600),
            &SignOptions::default(),
        )
        .await?;
    println!("Share this download link: {download_url}");

    // Pre-signed upload (PUT) link binding the exact size and type the client
    // must send (S3 only); the store rejects mismatched uploads up front.
    let upload_url = client
        .get_pre_signed_url(
            "s3://my-bucket/upload.bin",
            SignMethod::Put,
            Duration::from_secs(900),
            &SignOptions {
                content_length: Some(1_048_576),
                content_type: Some("application/octet-stream".to_string()),
            },
        )
        .await?;
    println!("Upload directly to: {upload_url}");

    Ok(())
}
```

---

## Python 3.13+ Usage

### Installation

The package is published on [PyPI](https://pypi.org/project/object-storage-client/). Note that it requires Python 3.13+.

```bash
pip install object-storage-client
```

Or if you are developing locally, you can use `maturin`:

```bash
maturin develop
```

### Walk-through

The same client handles every provider; the scheme in each URL picks the
backend, so copying or moving an object between S3, GCS, Azure and local disk is
a single call.

```python
import asyncio
from object_storage_client import ObjectStorageClient

async def main():
    client = ObjectStorageClient()

    # Create a bucket (S3/GCS/Azure, or a directory for file:// URLs); idempotent
    await client.create_bucket("s3://my-bucket")

    # Check whether a bucket exists (returns a bool; never raises for a miss).
    if await client.bucket_exists("s3://my-bucket"):
        print("my-bucket is present")

    # Upload data to S3
    await client.put_object("s3://my-bucket/hello.txt", b"Hello from Python!")

    # Check whether an object exists (returns a bool; never raises for a miss).
    # If you prefer the missing case to raise FileNotFoundError, use
    # get_object_metadata() or get_object() instead.
    if await client.object_exists("s3://my-bucket/hello.txt"):
        print("hello.txt is present")

    # Fetch full metadata (raises FileNotFoundError if the object is missing)
    meta = await client.get_object_metadata("s3://my-bucket/hello.txt")
    print(f"Size: {meta['size_bytes']}, type: {meta['content_type']}")

    # Download data
    data = await client.get_object("s3://my-bucket/hello.txt")
    print(f"Retrieved: {data.decode()}")

    # List objects
    items = await client.list_objects("s3://my-bucket/")
    print(f"Bucket items: {items}")

    # Stream data
    stream = await client.get_object_stream("s3://my-bucket/hello.txt")
    async for chunk in stream:
        print(f"Chunk size: {len(chunk)}")

    # --- Move data across providers with one client ---

    # Copy S3 -> Google Cloud Storage (source is left in place)
    await client.copy_object("s3://my-bucket/hello.txt", "gs://my-gcs-bucket/hello.txt")

    # Move GCS -> Azure Blob Storage (source is deleted afterwards)
    await client.move_object("gs://my-gcs-bucket/hello.txt", "az://my-container/hello.txt")

    # Copy Azure -> local disk for a working copy
    await client.copy_object("az://my-container/hello.txt", "file:///tmp/hello_local.txt")

    # --- Pre-signed URLs (S3/GCS/Azure): credential-free, time-limited access ---

    download_url = await client.get_pre_signed_url("s3://my-bucket/hello.txt")
    # Bind the exact Content-Length and Content-Type the client must send (S3
    # only); the store rejects uploads that don't match.
    upload_url = await client.get_pre_signed_url(
        "s3://my-bucket/upload.bin",
        method="PUT",
        expires_in_secs=900,
        content_length=1_048_576,
        content_type="application/octet-stream",
    )
    print(f"Download: {download_url}\nUpload: {upload_url}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Developer Instructions

### Prerequisites

- Rust 1.85+ (or latest stable)
- Python 3.13+
- `maturin` (for Python bindings)

### Building

- **Rust**: `cargo build --release`
- **Python**: `maturin build --release`
- **CLI**: `cargo build --bin osc`

### Testing

```bash
cargo test
```