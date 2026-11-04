#![cfg_attr(windows, windows_subsystem = "windows")]

use clap::Parser;
use object_storage_client::gui::{self, args::Args};
use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    #[cfg(unix)]
    object_storage_client::cli::harden_process();
    gui::run(&Args::parse().with_keyring_command(env::var_os("OSC_KEYRING_COMMAND")))
}
