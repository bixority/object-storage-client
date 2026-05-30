use clap::Parser;
use std::error::Error;
use std::process::ExitCode;

mod cli;

#[tokio::main]
async fn main() -> ExitCode {
    let Err(err) = cli::Cli::parse().run().await else {
        return ExitCode::SUCCESS;
    };

    eprintln!("Error: {err}");

    // Print the full source chain so wrapped I/O / storage causes are visible.
    let mut source = err.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }

    ExitCode::FAILURE
}
