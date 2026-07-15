//! Command-line interface for the object storage client.
//!
//! Parses arguments with `clap` and dispatches each subcommand against an
//! [`ObjectStorageClient`]. The longer subcommands (`get-stream`, `sign`) are
//! delegated to helpers so the dispatch stays small and readable.

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use object_storage_client::client::Error as StorageError;
use object_storage_client::{ObjectStorageClient, SignMethod, SignOptions};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{self, AsyncWrite, AsyncWriteExt, BufWriter};

/// Errors surfaced by the `osc` command-line interface.
///
/// Storage failures are delegated to the library's [`StorageError`]; the
/// remaining variants attach the offending local path to an I/O failure so the
/// user can see which file could not be read, written or created.
#[derive(Debug, Error)]
pub enum CliError {
    /// A failure from the underlying storage operation.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// The local source file for an upload could not be read.
    #[error("failed to read {path}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// A downloaded object could not be written to its local destination.
    #[error("failed to write {path}")]
    WriteFile {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// A directory was provided for upload without the recursive flag.
    #[error("directory {path} requires --recursive to upload")]
    DirectoryRecursiveRequired { path: String },

    /// The local destination file for a stream could not be created.
    #[error("failed to create {path}")]
    CreateFile {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Writing streamed bytes to the output sink failed.
    #[error("failed to write output")]
    Output(#[source] std::io::Error),
}

#[derive(Parser)]
#[command(name = "osc")]
#[command(about = "Unified object storage client CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Cp {
        src: String,
        dst: String,
    },
    Mv {
        src: String,
        dst: String,
    },
    Put {
        src: PathBuf,
        dst: String,
        /// Recursively upload a directory.
        #[arg(short, long)]
        recursive: bool,
    },
    Get {
        src: String,
        dst: PathBuf,
    },
    GetStream {
        src: String,
        dst: Option<PathBuf>,
    },
    Ls {
        url: String,
    },
    Rm {
        url: String,
    },
    /// Check whether an object exists, printing `true` or `false`.
    Exists {
        url: String,
    },
    /// Create a bucket / container (or, for local paths, a directory).
    Mb {
        url: String,
    },
    /// Check whether a bucket / container exists, printing `true` or `false`.
    BucketExists {
        url: String,
    },
    /// Generate a pre-signed URL (S3, GCS and Azure only).
    Sign {
        url: String,
        /// HTTP method the URL authorizes: GET, PUT, POST, DELETE or HEAD.
        #[arg(short, long, default_value = "GET")]
        method: String,
        /// Validity duration in seconds.
        #[arg(short, long, default_value_t = 3600)]
        expires_in: u64,
        /// Bind a required Content-Length (bytes) into the signature (S3 only).
        #[arg(long)]
        content_length: Option<u64>,
        /// Bind a required Content-Type into the signature (S3 only).
        #[arg(long)]
        content_type: Option<String>,
    },
}

fn to_url(s: &str) -> String {
    if s.contains("://") {
        s.to_string()
    } else {
        let path = fs::canonicalize(s).unwrap_or_else(|_| {
            std::env::current_dir().map_or_else(|_| PathBuf::from(s), |p| p.join(s))
        });

        let mut path_str = path.to_string_lossy().into_owned();
        if !path_str.starts_with('/') {
            path_str = format!("/{path_str}");
        }

        format!("file://{path_str}")
    }
}

enum Output {
    Stdout(io::Stdout),

    /// Buffered writer reduces syscall overhead (portable optimization)
    BufferedFile(BufWriter<tokio::fs::File>),
}

impl Output {
    fn new_file(file: tokio::fs::File) -> Self {
        Self::BufferedFile(BufWriter::with_capacity(64 * 1024, file))
    }
}

impl AsyncWrite for Output {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            Output::Stdout(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Output::BufferedFile(f) => std::pin::Pin::new(f).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Output::Stdout(s) => std::pin::Pin::new(s).poll_flush(cx),
            Output::BufferedFile(f) => std::pin::Pin::new(f).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Output::Stdout(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Output::BufferedFile(f) => std::pin::Pin::new(f).poll_shutdown(cx),
        }
    }
}

impl Cli {
    /// Dispatch the parsed command against a fresh client.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying storage operation.
    pub async fn run(self) -> Result<(), CliError> {
        let client = ObjectStorageClient::new();

        match self.command {
            Commands::Cp { src, dst } => cp(&client, &src, &dst).await?,

            Commands::Mv { src, dst } => mv(&client, &src, &dst).await?,

            Commands::Put {
                src,
                dst,
                recursive,
            } => put(&client, src, dst, recursive).await?,

            Commands::Get { src, dst } => get(&client, &src, &dst).await?,

            Commands::GetStream { src, dst } => get_stream(&client, &src, dst.as_deref()).await?,

            Commands::Ls { url } => ls(&client, &url).await?,

            Commands::Rm { url } => rm(&client, &url).await?,

            Commands::Exists { url } => exists(&client, &url).await?,

            Commands::Mb { url } => mb(&client, &url).await?,

            Commands::BucketExists { url } => bucket_exists(&client, &url).await?,

            Commands::Sign {
                url,
                method,
                expires_in,
                content_length,
                content_type,
            } => {
                sign(
                    &client,
                    &url,
                    &method,
                    expires_in,
                    content_length,
                    content_type,
                )
                .await?;
            }
        }

        Ok(())
    }
}

async fn cp(client: &ObjectStorageClient, src: &str, dst: &str) -> Result<(), CliError> {
    let src_url = to_url(src);
    let dst_url = to_url(dst);

    println!("Copy {src_url} -> {dst_url}");
    client.copy(&src_url, &dst_url).await?;
    Ok(())
}

async fn mv(client: &ObjectStorageClient, src: &str, dst: &str) -> Result<(), CliError> {
    let src_url = to_url(src);
    let dst_url = to_url(dst);

    println!("Move {src_url} -> {dst_url}");
    client.move_object(&src_url, &dst_url).await?;
    Ok(())
}

async fn put(
    client: &ObjectStorageClient,
    src: PathBuf,
    dst: String,
    recursive: bool,
) -> Result<(), CliError> {
    let src_metadata = fs::metadata(&src).map_err(|source| CliError::ReadFile {
        path: src.display().to_string(),
        source,
    })?;

    if src_metadata.is_dir() {
        if !recursive {
            return Err(CliError::DirectoryRecursiveRequired {
                path: src.display().to_string(),
            });
        }

        let mut dst_root = to_url(&dst);
        if !dst_root.ends_with('/') {
            dst_root.push('/');
        }

        println!("Recursive upload {} -> {dst_root}", src.display());
        upload_dir_recursive(client, &src, &dst_root).await?;
    } else {
        let data = fs::read(&src).map_err(|source| CliError::ReadFile {
            path: src.display().to_string(),
            source,
        })?;

        let dst_url = to_url(&dst);

        println!("Upload {} -> {dst_url}", src.display());
        client.put(&dst_url, data).await?;
    }

    Ok(())
}

async fn get(client: &ObjectStorageClient, src: &str, dst: &Path) -> Result<(), CliError> {
    let src_url = to_url(src);

    println!("Download {src_url} -> {}", dst.display());

    let data = client.get(&src_url).await?;

    fs::write(dst, data).map_err(|source| CliError::WriteFile {
        path: dst.display().to_string(),
        source,
    })?;
    Ok(())
}

async fn ls(client: &ObjectStorageClient, url: &str) -> Result<(), CliError> {
    let target = to_url(url);

    let list = client.list(&target).await?;

    for item in list {
        println!("{item}");
    }
    Ok(())
}

async fn rm(client: &ObjectStorageClient, url: &str) -> Result<(), CliError> {
    let target = to_url(url);

    println!("Delete {target}");
    client.delete(&target).await?;
    Ok(())
}

async fn exists(client: &ObjectStorageClient, url: &str) -> Result<(), CliError> {
    let target = to_url(url);

    let exists = client.exists(&target).await?;
    println!("{exists}");
    Ok(())
}

async fn mb(client: &ObjectStorageClient, url: &str) -> Result<(), CliError> {
    let target = to_url(url);

    println!("Create bucket {target}");
    client.create_bucket(&target).await?;
    Ok(())
}

async fn bucket_exists(client: &ObjectStorageClient, url: &str) -> Result<(), CliError> {
    let target = to_url(url);

    let exists = client.bucket_exists(&target).await?;
    println!("{exists}");
    Ok(())
}

/// Stream an object to a file (when `dst` is given) or to standard output.
async fn get_stream(
    client: &ObjectStorageClient,
    src: &str,
    dst: Option<&Path>,
) -> Result<(), CliError> {
    let src_url = to_url(src);

    println!("Streaming {src_url}");

    let mut stream = client.get_stream(&src_url).await?;

    let mut output: Output = match dst {
        Some(path) => {
            println!("Writing to file {}", path.display());

            let file =
                tokio::fs::File::create(path)
                    .await
                    .map_err(|source| CliError::CreateFile {
                        path: path.display().to_string(),
                        source,
                    })?;

            Output::new_file(file)
        }
        None => Output::Stdout(tokio::io::stdout()),
    };

    // streaming loop (zero-copy from Bytes -> &[u8])
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.map_err(StorageError::from)?;

        output.write_all(&bytes).await.map_err(CliError::Output)?;
    }

    output.flush().await.map_err(CliError::Output)?;
    Ok(())
}

/// Generate and print a pre-signed URL for `url`.
async fn sign(
    client: &ObjectStorageClient,
    url: &str,
    method: &str,
    expires_in: u64,
    content_length: Option<u64>,
    content_type: Option<String>,
) -> Result<(), CliError> {
    let target = to_url(url);
    let signed_method: SignMethod = method.parse()?;
    let options = SignOptions {
        content_length,
        content_type,
    };

    let signed = client
        .get_pre_signed_url(
            &target,
            signed_method,
            Duration::from_secs(expires_in),
            &options,
        )
        .await?;

    println!("{signed}");
    Ok(())
}

/// Recursively upload the contents of `src_dir` to the destination prefix.
async fn upload_dir_recursive(
    client: &ObjectStorageClient,
    src_dir: &Path,
    dst_root_url: &str,
) -> Result<(), CliError> {
    let mut files = Vec::new();
    let mut dir_stack = vec![src_dir.to_path_buf()];

    while let Some(current_dir) = dir_stack.pop() {
        let mut entries =
            tokio::fs::read_dir(&current_dir)
                .await
                .map_err(|source| CliError::ReadFile {
                    path: current_dir.display().to_string(),
                    source,
                })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| CliError::ReadFile {
                path: current_dir.display().to_string(),
                source,
            })?
        {
            let path = entry.path();
            let metadata = entry
                .metadata()
                .await
                .map_err(|source| CliError::ReadFile {
                    path: path.display().to_string(),
                    source,
                })?;

            if metadata.is_dir() {
                dir_stack.push(path);
            } else if metadata.is_file() {
                files.push(path);
            }
        }
    }

    futures_util::stream::iter(files)
        .map(|path| {
            let client = client.clone();
            let src_dir = src_dir.to_path_buf();
            let dst_root_url = dst_root_url.to_string();
            async move {
                let rel_path = path.strip_prefix(&src_dir).unwrap();
                let rel_path_str = rel_path.to_string_lossy().replace('\\', "/");
                let dst_url = format!("{dst_root_url}{rel_path_str}");

                let data = tokio::fs::read(&path)
                    .await
                    .map_err(|source| CliError::ReadFile {
                        path: path.display().to_string(),
                        source,
                    })?;

                println!("Upload {} -> {dst_url}", path.display());
                client.put(&dst_url, data).await?;
                Ok::<(), CliError>(())
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<Result<(), CliError>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<()>, CliError>>()?;

    Ok(())
}
