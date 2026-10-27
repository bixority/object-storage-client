//! Key sources (`--keyring`, `OSC_KEYRING_COMMAND`) and the `osc keyring`
//! helpers.

use std::ffi::{OsStr, OsString};
use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use clap::Subcommand;
use zeroize::Zeroizing;

use super::{CliError, sanitize};
use crate::encryption::keys::keyring::MAX_KEYRING_LEN;
use crate::encryption::{Keyring, new_keyring, retire_key, rotate_keyring};

/// Environment variable naming the command that prints a keyring.
const COMMAND_ENV: &str = "OSC_KEYRING_COMMAND";

/// How long `OSC_KEYRING_COMMAND` may run.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Exit code of `keyring status` when the active key is too old.
pub(super) const EXIT_KEY_OVERDUE: u8 = 3;

#[derive(Subcommand)]
pub(super) enum KeyringCommand {
    /// Print a new keyring with one fresh active key.
    New {
        /// Prefix of the key ID.
        #[arg(long, default_value = "kek")]
        prefix: String,
        /// Write to a terminal too.
        #[arg(long)]
        force: bool,
    },
    /// Read a keyring from standard input and print it with a fresh key
    /// added and made active.
    Rotate {
        /// Prefix of the key ID.
        #[arg(long, default_value = "kek")]
        prefix: String,
        /// Write to a terminal too.
        #[arg(long)]
        force: bool,
    },
    /// Read a keyring from standard input and print it without the key ID.
    Retire {
        id: String,
        /// Write to a terminal too.
        #[arg(long)]
        force: bool,
    },
    /// Validate a keyring and list its keys: the configured one (`--keyring`
    /// or `OSC_KEYRING_COMMAND`), otherwise standard input. Exits with 3 when
    /// the active key is too old.
    Status {
        /// Age in days from which the active key counts as overdue.
        #[arg(long, default_value_t = 365)]
        max_age_days: i64,
    },
}

enum Source {
    File(PathBuf),
    Command(OsString),
}

/// The one configured key source, if any.
fn source(file: Option<PathBuf>) -> Result<Option<Source>, CliError> {
    let command = std::env::var_os(COMMAND_ENV).filter(|c| !c.is_empty());
    match (file, command) {
        (Some(_), Some(_)) => Err(CliError::KeyringSourcesConflict),
        (Some(file), None) => Ok(Some(Source::File(file))),
        (None, Some(command)) => Ok(Some(Source::Command(command))),
        (None, None) => Ok(None),
    }
}

/// Whether a key source is configured (which enables encryption).
pub(super) fn is_configured(file: Option<PathBuf>) -> Result<bool, CliError> {
    source(file).map(|s| s.is_some())
}

/// The configured keyring, or `None` for plain operation.
pub(super) async fn open(file: Option<PathBuf>) -> Result<Option<Arc<Keyring>>, CliError> {
    let ring = match source(file)? {
        None => return Ok(None),
        Some(Source::File(path)) => Keyring::from_file(path)?,
        Some(Source::Command(command)) => {
            // Blocking, so off the thread that waits for signals.
            tokio::task::spawn_blocking(move || run_command(&command, COMMAND_TIMEOUT))
                .await
                .map_err(|e| CliError::Output(std::io::Error::other(e)))??
        }
    };
    Ok(Some(Arc::new(ring)))
}

/// Runs `command` through the shell and parses its standard output.
///
/// Standard input is the null device and standard error is inherited. The
/// output is captured into a zeroizing buffer of at most 64 KiB; a command
/// that is still running after `timeout` is killed.
pub(super) fn run_command(command: &OsStr, timeout: Duration) -> Result<Keyring, CliError> {
    let deadline = Instant::now() + timeout;
    let mut shell = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.arg("/C");
        c
    } else {
        let mut c = Command::new("sh");
        c.arg("-c");
        c
    };
    let mut child = shell
        .arg(command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(CliError::KeyringCommandStart)?;
    let kill = |child: &mut std::process::Child| {
        let _ = child.kill();
        let _ = child.wait();
    };

    let mut pipe = child
        .stdout
        .take()
        .ok_or_else(|| CliError::KeyringCommandStart(std::io::Error::other("no stdout pipe")))?;
    let (tx, rx) = channel();
    // Not joined: a grandchild holding the pipe open would block a join past
    // the timeout. The buffer is zeroized wherever it ends up.
    std::thread::spawn(move || {
        let cap = MAX_KEYRING_LEN as u64;
        let mut out = Zeroizing::new(Vec::with_capacity(MAX_KEYRING_LEN + 1));
        let read = (&mut pipe).take(cap + 1).read_to_end(&mut out);
        let _ = tx.send(read.map(|_| out));
    });
    let output = match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            kill(&mut child);
            return Err(CliError::KeyringCommandStart(e));
        }
        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
            kill(&mut child);
            return Err(CliError::KeyringCommandTimeout(timeout));
        }
    };
    if output.len() > MAX_KEYRING_LEN {
        kill(&mut child);
        return Err(CliError::KeyringCommandTooLarge);
    }
    // The output is complete; the exit status may still be pending.
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => {
                kill(&mut child);
                return Err(CliError::KeyringCommandTimeout(timeout));
            }
            Err(e) => {
                kill(&mut child);
                return Err(CliError::KeyringCommandStart(e));
            }
        }
    };
    if !status.success() {
        return Err(CliError::KeyringCommandStatus(status.to_string()));
    }
    let text = std::str::from_utf8(&output).map_err(|_| CliError::KeyringCommandNotUtf8)?;
    Ok(Keyring::parse(text)?)
}

/// Standard input, at most 64 KiB, in a zeroizing buffer.
async fn read_stdin() -> Result<Zeroizing<Vec<u8>>, CliError> {
    tokio::task::spawn_blocking(|| {
        let cap = MAX_KEYRING_LEN as u64;
        let mut raw = Zeroizing::new(Vec::with_capacity(MAX_KEYRING_LEN + 1));
        std::io::stdin()
            .lock()
            .take(cap + 1)
            .read_to_end(&mut raw)
            .map_err(CliError::KeyringStdin)?;
        if raw.len() > MAX_KEYRING_LEN {
            return Err(CliError::KeyringInputTooLarge);
        }
        Ok(raw)
    })
    .await
    .map_err(|e| CliError::Output(std::io::Error::other(e)))?
}

async fn read_stdin_text() -> Result<Zeroizing<String>, CliError> {
    let mut raw = read_stdin().await?;
    // Moves the buffer into the string; the error keeps it, still zeroized.
    String::from_utf8(std::mem::take(&mut *raw))
        .map(Zeroizing::new)
        .map_err(|e| {
            drop(Zeroizing::new(e.into_bytes()));
            CliError::KeyringInputNotUtf8
        })
}

fn check_terminal(force: bool) -> Result<(), CliError> {
    if !force && std::io::stdout().is_terminal() {
        return Err(CliError::KeyringToTerminal);
    }
    Ok(())
}

fn write_stdout(text: &str) -> Result<(), CliError> {
    let mut out = std::io::stdout().lock();
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .map_err(CliError::Output)
}

fn today() -> chrono::NaiveDate {
    chrono::Utc::now().date_naive()
}

/// `osc keyring ...`.
pub(super) async fn run(cmd: KeyringCommand, file: Option<PathBuf>) -> Result<(), CliError> {
    match cmd {
        KeyringCommand::New { prefix, force } => {
            check_terminal(force)?;
            write_stdout(&new_keyring(&prefix, today())?)
        }
        KeyringCommand::Rotate { prefix, force } => {
            check_terminal(force)?;
            let input = read_stdin_text().await?;
            write_stdout(&rotate_keyring(&input, &prefix, today())?)
        }
        KeyringCommand::Retire { id, force } => {
            check_terminal(force)?;
            let input = read_stdin_text().await?;
            write_stdout(&retire_key(&input, &id)?)
        }
        KeyringCommand::Status { max_age_days } => status(file, max_age_days).await,
    }
}

async fn status(file: Option<PathBuf>, max_age_days: i64) -> Result<(), CliError> {
    let ring = if let Some(ring) = open(file).await? {
        ring
    } else {
        let input = read_stdin_text().await?;
        Arc::new(Keyring::parse(&input)?)
    };
    let today = today();
    let status = ring.status_on(today);
    let width = status
        .keys
        .iter()
        .map(|k| sanitize(&k.id).len())
        .max()
        .unwrap_or(0)
        .max("ID".len());
    let mut table = format!(
        "{:<width$}  {:<10}  {:>8}  ACTIVE\n",
        "ID", "CREATED", "AGE_DAYS"
    );
    for k in &status.keys {
        let mark = if k.id == status.active { "*" } else { "" };
        let line = format!(
            "{:<width$}  {}  {:>8}  {mark}",
            sanitize(&k.id),
            k.created,
            (today - k.created).num_days(),
        );
        table.push_str(line.trim_end());
        table.push('\n');
    }
    print!("{table}");
    if status.active_age_days > max_age_days {
        return Err(CliError::KeyOverdue {
            id: sanitize(&status.active),
            age_days: status.active_age_days,
            max_age_days,
        });
    }
    Ok(())
}
