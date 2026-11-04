//! The command-line grammar of `osc-gui`.

use clap::Parser;
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "osc-gui", version)]
#[command(about = "Two-pane object storage browser", long_about = None)]
pub struct Args {
    /// Keyring file; enables encryption. The environment variable
    /// `OSC_KEYRING_COMMAND` (a shell command that prints a keyring) is the
    /// alternative source; setting both is an error.
    #[arg(long, env = "OSC_KEYRING_FILE", value_name = "FILE")]
    pub(crate) keyring: Option<PathBuf>,

    /// The shell command of `OSC_KEYRING_COMMAND`, set by the binary only
    /// ([`Args::with_keyring_command`]), so that tests never read the
    /// environment.
    #[arg(skip)]
    pub(crate) keyring_command: Option<OsString>,

    /// Refuse to start unless a key source is configured, instead of sending
    /// or reading plaintext. Also set by `OSC_REQUIRE_ENCRYPTION` (`1`,
    /// `true`, `yes`, `on`; `0` and the like leave it off).
    #[arg(
        long,
        env = "OSC_REQUIRE_ENCRYPTION",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub(crate) require_encryption: bool,
}

impl Args {
    /// Sets the key source command (the value of `OSC_KEYRING_COMMAND`). Only
    /// the binary reads that variable.
    #[must_use]
    pub fn with_keyring_command(mut self, command: Option<OsString>) -> Self {
        self.keyring_command = command;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, FromArgMatches};

    /// Parses without the env bindings, so an exported `OSC_KEYRING_FILE` or
    /// `OSC_REQUIRE_ENCRYPTION` cannot leak into the test.
    fn parse(argv: &[&str]) -> Result<Args, clap::Error> {
        let m = Args::command()
            .mut_arg("keyring", |a| a.env(None))
            .mut_arg("require_encryption", |a| a.env(None))
            .try_get_matches_from(argv)?;
        Args::from_arg_matches(&m)
    }

    #[test]
    fn panels_start_on_local_so_there_are_no_locations_or_profile() {
        assert!(parse(&["osc-gui"]).is_ok());
        assert!(parse(&["osc-gui", "--profile", "p"]).is_err());
        assert!(parse(&["osc-gui", "s3://b/"]).is_err());
        assert!(parse(&["osc-gui", "/tmp/a", "/tmp/b"]).is_err());
    }
}
