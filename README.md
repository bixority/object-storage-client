# Object Storage Client

A unified object storage client for Rust and Python, supporting S3, GCS, Azure Blob Storage, HTTP/HTTPS, and Local Filesystem. It provides a simple, URL-based API for object operations, including cross-provider copy and move.

## Features

- **Unified API**: Single interface for various storage backends.
- **Cross-Provider**: Copy or move objects between different storage providers (e.g., S3 to Local FS).
- **Multi-Language**: Native Rust library with Python 3.14 bindings.
- **CLI**: `osc` command-line tool for quick operations.

## Supported Schemes

- `s3://bucket/path` (AWS S3)
- `gs://bucket/path` or `gcs://bucket/path` (Google Cloud Storage)
- `az://container/path` (Azure Blob Storage)
- `http://host/path` or `https://host/path` (HTTP/HTTPS)
- `file:///absolute/path` or `local_path` (Local Filesystem)

---

## CLI Usage (`osc`)

The `osc` tool allows you to interact with object storage directly from your terminal.

### Installation

If you have the source code, you can install it using Cargo:

```bash
cargo install --path .
```

### Examples

- **Upload a local file**:
  ```bash
  osc put my_file.txt s3://my-bucket/remote_file.txt
  ```

- **Download an object**:
  ```bash
  osc get gs://my-bucket/data.json ./local_data.json
  ```

- **Copy between providers**:
  ```bash
  osc cp s3://source-bucket/image.png az://dest-container/image.png
  ```

- **List objects**:
  ```bash
  osc ls s3://my-bucket/logs/
  ```

- **Delete an object**:
  ```bash
  osc rm s3://my-bucket/temp_file.tmp
  ```

---

## Rust Usage

### Installation

Add `object-storage-client` to your `Cargo.toml`:

```toml
[dependencies]
object-storage-client = { git = "https://github.com/bixority/object-storage-client" }
tokio = { version = "1.0", features = ["full"] }
```

### Example

```rust
use object_storage_client::ObjectStorageClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ObjectStorageClient::new();

    // Upload data
    let data = b"Hello from Rust!";
    client.put("s3://my-bucket/hello.txt", data.to_vec()).await?;

    // Download data
    let retrieved = client.get("s3://my-bucket/hello.txt").await?;
    println!("Retrieved: {:?}", String::from_utf8(retrieved.to_vec())?);

    // Cross-provider copy (S3 to Local)
    client.copy("s3://my-bucket/hello.txt", "file:///tmp/hello_local.txt").await?;

    Ok(())
}
```

---

## Python 3.14 Usage

### Installation

You can install the package using `pip`. Note that it requires Python 3.14.

```bash
pip install git+https://github.com/bixority/object-storage-client
```

Or if you are developing locally, you can use `maturin`:

```bash
maturin develop
```

### Example

```python
import asyncio
from object_storage_client import ObjectStorageClient

async def main():
    client = ObjectStorageClient()

    # Upload data
    await client.put("s3://my-bucket/python_test.txt", b"Hello from Python 3.14!")

    # Download data
    data = await client.get("s3://my-bucket/python_test.txt")
    print(f"Retrieved: {data.decode()}")

    # List objects
    items = await client.list("s3://my-bucket/")
    print(f"Bucket items: {items}")

    # Cross-provider move (GCS to S3)
    await client.move_object("gs://my-gcs-bucket/data.csv", "s3://my-s3-bucket/data.csv")

if __name__ == "__main__":
    asyncio.run(main())
```

## Developer Instructions

### Prerequisites

- Rust 1.85+ (or latest stable)
- Python 3.14+
- `maturin` (for Python bindings)

### Building

- **Rust**: `cargo build --release`
- **Python**: `maturin build --release`
- **CLI**: `cargo build --bin osc`

### Testing

```bash
cargo test
```