use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use object_storage_client::{ObjectStorageClient, SignMethod, SignOptions};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{self, AsyncWrite, AsyncWriteExt, BufWriter};

#[derive(Parser)]
#[command(name = "osc")]
#[command(about = "Unified object storage client CLI", long_about = None)]
struct Cli {
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = ObjectStorageClient::new();

    match cli.command {
        Commands::Cp { src, dst } => {
            let src_url = to_url(&src);
            let dst_url = to_url(&dst);

            println!("Copy {src_url} -> {dst_url}");
            client.copy(&src_url, &dst_url).await?;
        }

        Commands::Mv { src, dst } => {
            let src_url = to_url(&src);
            let dst_url = to_url(&dst);

            println!("Move {src_url} -> {dst_url}");
            client.move_object(&src_url, &dst_url).await?;
        }

        Commands::Put { src, dst } => {
            let data =
                fs::read(&src).with_context(|| format!("failed to read {}", src.display()))?;

            let dst_url = to_url(&dst);

            println!("Upload {} -> {dst_url}", src.display());
            client.put(&dst_url, data).await?;
        }

        Commands::Get { src, dst } => {
            let src_url = to_url(&src);

            println!("Download {src_url} -> {}", dst.display());

            let data = client.get(&src_url).await?;

            fs::write(&dst, data).with_context(|| format!("failed to write {}", dst.display()))?;
        }

        Commands::GetStream { src, dst } => {
            let src_url = to_url(&src);

            println!("Streaming {src_url}");

            let mut stream = client
                .get_stream(&src_url)
                .await
                .context("failed to start stream")?;

            let mut output: Output = match dst {
                Some(path) => {
                    println!("Writing to file {}", path.display());

                    let file = tokio::fs::File::create(&path)
                        .await
                        .with_context(|| format!("failed to create {}", path.display()))?;

                    Output::new_file(file)
                }
                None => Output::Stdout(tokio::io::stdout()),
            };

            // streaming loop (zero-copy from Bytes -> &[u8])
            while let Some(chunk) = stream.next().await {
                let bytes = chunk.context("stream error")?;

                output.write_all(&bytes).await?;
            }

            output.flush().await?;
        }

        Commands::Ls { url } => {
            let target = to_url(&url);

            let list = client.list(&target).await?;

            for item in list {
                println!("{item}");
            }
        }

        Commands::Rm { url } => {
            let target = to_url(&url);

            println!("Delete {target}");
            client.delete(&target).await?;
        }

        Commands::Sign {
            url,
            method,
            expires_in,
            content_length,
            content_type,
        } => {
            let target = to_url(&url);
            let signed_method: SignMethod = method
                .parse()
                .with_context(|| format!("invalid HTTP method: {method}"))?;
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
                .await
                .context("failed to generate pre-signed URL")?;

            println!("{signed}");
        }
    }

    Ok(())
}
