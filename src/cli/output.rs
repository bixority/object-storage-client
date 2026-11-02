//! What the CLI prints: standard output and error lines, and escaping and
//! redaction of the names and URLs they show.

use super::CliError;
use crate::text;
use std::path::Path;

/// Writes one line to standard output. A closed pipe is an error, not a
/// panic as with `println!`, so the command stops and drops its in-flight work
/// (which cleans up) instead of aborting.
pub(super) fn out(args: std::fmt::Arguments<'_>) -> Result<(), CliError> {
    use std::io::Write;
    writeln!(std::io::stdout().lock(), "{args}").map_err(CliError::Output)
}

/// [`out`] with `println!` syntax.
macro_rules! outln {
    ($($arg:tt)*) => {
        $crate::cli::output::out(format_args!($($arg)*))
    };
}

/// Writes one line to standard error and ignores a failure: a status or error
/// line must never abort the command.
macro_rules! errln {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

/// Writes `text` to standard output exactly, without a trailing newline.
#[cfg(feature = "encryption")]
pub(super) fn write_stdout(text: &str) -> Result<(), CliError> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .map_err(CliError::Output)
}

/// Escapes control characters (C0, DEL, C1) and bidirectional controls as
/// `\u{..}`, so a name taken from a file system or a store cannot inject
/// terminal escape sequences or reorder the text around it.
///
/// For text printed to a terminal only; never for object bytes.
#[doc(hidden)]
#[must_use]
pub fn sanitize(text: &str) -> String {
    text::escape(text, usize::MAX)
}

/// A URL made safe for a status line: redacted, then escaped.
pub(super) fn show_url(text: &str) -> String {
    sanitize(&crate::codec::redact_url(text))
}

/// A local path made safe for a status line.
pub(super) fn show_path(path: &Path) -> String {
    sanitize(&path.display().to_string())
}
