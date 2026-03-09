use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use object_storage_client::ObjectStorageClient;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "osc")]
#[command(about = "Unified object storage client CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Copy an object from source to destination
    Cp {
        /// Source URL or path
        src: String,
        /// Destination URL or path
        dst: String,
    },
    /// Move an object from source to destination
    Mv {
        /// Source URL or path
        src: String,
        /// Destination URL or path
        dst: String,
    },
    /// Upload a local file to object storage
    Put {
        /// Local file path
        src: PathBuf,
        /// Destination URL
        dst: String,
    },
    /// Download an object to local filesystem
    Get {
        /// Source URL
        src: String,
        /// Local file path
        dst: PathBuf,
    },
    /// List objects under a prefix
    Ls {
        /// URL prefix
        url: String,
    },
    /// Delete an object
    Rm {
        /// URL to delete
        url: String,
    },
}

fn to_url(s: &str) -> String {
    if s.contains("://") {
        s.to_string()
    } else {
        // Convert local path to file:// URL
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = ObjectStorageClient::new();

    match cli.command {
        Commands::Cp { src, dst } => {
            let src_url = to_url(&src);
            let dst_url = to_url(&dst);
            println!("Copying {src_url} to {dst_url}");
            client.copy(&src_url, &dst_url).await?;
        }
        Commands::Mv { src, dst } => {
            let src_url = to_url(&src);
            let dst_url = to_url(&dst);
            println!("Moving {src_url} to {dst_url}");
            client.move_object(&src_url, &dst_url).await?;
        }
        Commands::Put { src, dst } => {
            let data = fs::read(&src)
                .with_context(|| format!("Failed to read source file {}", src.display()))?;
            let dst_url = to_url(&dst);
            println!("Uploading {} to {dst_url}", src.display());
            client.put(&dst_url, &data).await?;
        }
        Commands::Get { src, dst } => {
            let src_url = to_url(&src);
            println!("Downloading {src_url} to {}", dst.display());
            let data = client.get(&src_url).await?;
            fs::write(&dst, data)
                .with_context(|| format!("Failed to write destination file {}", dst.display()))?;
        }
        Commands::Ls { url } => {
            let target_url = to_url(&url);
            let list = client.list(&target_url).await?;
            for item in list {
                println!("{item}");
            }
        }
        Commands::Rm { url } => {
            let target_url = to_url(&url);
            println!("Deleting {target_url}");
            client.delete(&target_url).await?;
        }
    }

    Ok(())
}
