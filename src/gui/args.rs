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

    /// AWS profile for `s3://` credentials (static keys, SSO, ...). It is the
    /// only credential source: `S3_*` / `AWS_*` keys are not used, `AWS_PROFILE`
    /// is not read and `AWS_ENDPOINT_URL` is refused. A `credential_process`
    /// entry in the profile makes osc run that command.
    #[arg(long, value_name = "NAME")]
    pub(crate) profile: Option<String>,

    /// Refuse to start unless a key source is configured, instead of sending
    /// or reading plaintext. Also set by `OSC_REQUIRE_ENCRYPTION` (`1`,
    /// `true`, `yes`, `on`; `0` and the like leave it off).
    #[arg(
        long,
        env = "OSC_REQUIRE_ENCRYPTION",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub(crate) require_encryption: bool,

    /// The URL or local path shown in the left pane.
    #[arg(value_name = "LEFT")]
    pub(crate) left: Option<String>,

    /// The URL or local path shown in the right pane.
    #[arg(value_name = "RIGHT")]
    pub(crate) right: Option<String>,
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
    fn parse(argv: &[&str]) -> Args {
        let m = Args::command()
            .mut_arg("keyring", |a| a.env(None))
            .mut_arg("require_encryption", |a| a.env(None))
            .try_get_matches_from(argv)
            .expect("parses");
        Args::from_arg_matches(&m).expect("args")
    }

    #[test]
    fn args_take_two_optional_urls() {
        let args = parse(&["osc-gui", "s3://b/p/", "/tmp/x"]);
        assert_eq!(args.left.as_deref(), Some("s3://b/p/"));
        assert_eq!(args.right.as_deref(), Some("/tmp/x"));
        let args = parse(&["osc-gui"]);
        assert!(args.left.is_none() && args.right.is_none());
    }
}
