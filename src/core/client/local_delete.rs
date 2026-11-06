//! Deleting everything under a `file://` prefix without following symlinks.
//!
//! This walk never follows a symlink below the prefix: a link is removed as a
//! link. A listing-based delete could not do that, since the crate's local
//! listing skips links and `object_store`'s follows them.
//!
//! It deletes regular files and symlinks, minus names `object_store` treats
//! as in-flight upload staging files (`name#<digits>`). Directories are left
//! in place, as `LocalFileSystem` without automatic cleanup does.

use crate::backend::fs_path;
use crate::client::{ERROR_PATH_CAP, ERROR_SOURCE_CAP, Error, Result};
use object_store::path::Path as ObjectPath;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Deepest directory nesting the walk enters; deeper trees are an error, so a
/// hostile tree cannot exhaust file descriptors or the stack.
const MAX_DEPTH: usize = 128;

/// Sets the flag when dropped, so a cancelled `delete_by_prefix` future stops
/// the blocking walk at its next entry.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn check_walk(cancel: &AtomicBool, depth: usize) -> Result<()> {
    if cancel.load(Ordering::Relaxed) {
        return Err(Error::Generic("delete_by_prefix was cancelled".into()));
    }
    if depth > MAX_DEPTH {
        return Err(Error::InvalidInput(format!(
            "Directory tree below the prefix is nested deeper than {MAX_DEPTH} levels"
        )));
    }
    Ok(())
}

/// Whether `object_store` would list a file with this name: it hides the
/// `name#<digits>` staging files of in-progress uploads.
pub fn is_listed_name(name: &str) -> bool {
    match name.split_once('#') {
        Some((_, suffix)) if !suffix.is_empty() => !suffix.bytes().all(|b| b.is_ascii_digit()),
        _ => true,
    }
}

/// Whether a file at `fs_path` is one `object_store` would list. A name it
/// cannot map to an object path makes its list fail, so this errors too.
fn is_listed(fs_path: &Path) -> Result<bool> {
    let location = ObjectPath::from_absolute_path(fs_path).map_err(|e| {
        // The file name may hold control characters.
        let path = crate::text::escape_display(&fs_path.display(), ERROR_PATH_CAP);
        let e = crate::text::escape_display(&e, ERROR_SOURCE_CAP);
        Error::InvalidInput(format!("Invalid path {path}: {e}"))
    })?;
    Ok(location.filename().is_none_or(is_listed_name))
}

/// [`is_listed`] for the entry `name` of the directory `dir`, without turning
/// the whole path into an object path for every file. Only the name is checked
/// per entry; `dir_valid` caches whether `dir` itself maps to an object path,
/// computed on the first file that needs it. A name or directory that does not
/// map takes the full [`is_listed`] check, for its error.
#[cfg(unix)]
fn is_listed_in(dir: &Path, dir_valid: &mut Option<bool>, name: &std::ffi::CStr) -> Result<bool> {
    use std::os::unix::ffi::OsStrExt;

    if let Ok(leaf) = std::str::from_utf8(name.to_bytes())
        && object_store::path::PathPart::parse(leaf).is_ok()
        && *dir_valid.get_or_insert_with(|| ObjectPath::from_absolute_path(dir).is_ok())
    {
        return Ok(is_listed_name(leaf));
    }
    is_listed(&dir.join(std::ffi::OsStr::from_bytes(name.to_bytes())))
}

/// Deletes every listed file and symlink below the directory `prefix`, never
/// following a symlink. A missing prefix, or one that is not a directory, is
/// not an error.
pub async fn delete_by_prefix(prefix: ObjectPath) -> Result<()> {
    let root = fs_path(&prefix)?;
    let cancel = Arc::new(AtomicBool::new(false));
    let _guard = CancelOnDrop(Arc::clone(&cancel));
    tokio::task::spawn_blocking(move || walk_root(&root, &cancel))
        .await
        .map_err(|e| Error::Generic(format!("delete_by_prefix task failed: {e}")))?
}

#[cfg(unix)]
fn walk_root(root: &Path, cancel: &AtomicBool) -> Result<()> {
    use rustix::fs::{Mode, OFlags, open};
    use rustix::io::Errno;

    let fd = match open(
        root,
        OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT | Errno::NOTDIR) => return Ok(()),
        Err(e) => return Err(std::io::Error::from(e).into()),
    };
    walk_dir(&fd, root, cancel, 0)
}

/// Deletes the files below the directory `dir` (named `path`, for messages and
/// the name check only). Every operation goes through `dir`, so a directory
/// swapped for a symlink mid-walk is never entered.
#[cfg(unix)]
fn walk_dir(
    dir: &rustix::fd::OwnedFd,
    path: &Path,
    cancel: &AtomicBool,
    depth: usize,
) -> Result<()> {
    use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, openat, statat, unlinkat};
    use rustix::io::Errno;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    check_walk(cancel, depth)?;
    // Read the whole directory first, so entries are not removed while it is
    // being iterated.
    let mut entries = Vec::new();
    for entry in Dir::read_from(dir).map_err(std::io::Error::from)? {
        let entry = entry.map_err(std::io::Error::from)?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        let mut file_type = entry.file_type();
        if file_type == FileType::Unknown {
            // Some file systems do not report the type in the listing.
            file_type = match statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(st) => FileType::from_raw_mode(st.st_mode),
                Err(Errno::NOENT) => continue,
                Err(e) => return Err(std::io::Error::from(e).into()),
            };
        }
        entries.push((name.to_owned(), file_type));
    }

    let mut dir_valid = None;
    for (name, file_type) in entries {
        check_walk(cancel, depth)?;
        match file_type {
            FileType::Directory => {
                let child = path.join(OsStr::from_bytes(name.to_bytes()));
                // `O_NOFOLLOW` fails on a symlink: a directory swapped for one
                // since the listing is skipped, not entered.
                match openat(
                    dir,
                    &name,
                    OFlags::DIRECTORY | OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                ) {
                    Ok(sub) => walk_dir(&sub, &child, cancel, depth + 1)?,
                    Err(Errno::NOENT | Errno::LOOP | Errno::NOTDIR) => {}
                    Err(e) => return Err(std::io::Error::from(e).into()),
                }
            }
            // `unlinkat` removes a symlink itself, never its target.
            FileType::RegularFile | FileType::Symlink => {
                if !is_listed_in(path, &mut dir_valid, &name)? {
                    continue;
                }
                match unlinkat(dir, &name, AtFlags::empty()) {
                    Ok(()) | Err(Errno::NOENT) => {}
                    Err(e) => return Err(std::io::Error::from(e).into()),
                }
            }
            // Sockets, FIFOs and devices are not objects.
            _ => {}
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn walk_root(root: &Path, cancel: &AtomicBool) -> Result<()> {
    match std::fs::symlink_metadata(root) {
        Ok(meta) if meta.is_dir() => walk_dir(root, cancel, 0),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Path-based walk for platforms without `openat`. It checks each entry with
/// `symlink_metadata`, so a link is never followed.
#[cfg(not(unix))]
fn walk_dir(dir: &Path, cancel: &AtomicBool, depth: usize) -> Result<()> {
    check_walk(cancel, depth)?;
    for entry in std::fs::read_dir(dir)? {
        check_walk(cancel, depth)?;
        let path = entry?.path();
        let file_type = match std::fs::symlink_metadata(&path) {
            Ok(meta) => meta.file_type(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if file_type.is_symlink() {
            if is_listed(&path)? {
                // A directory symlink on Windows is removed with `remove_dir`.
                std::fs::remove_file(&path).or_else(|_| std::fs::remove_dir(&path))?;
            }
        } else if file_type.is_dir() {
            walk_dir(&path, cancel, depth + 1)?;
        } else if file_type.is_file() && is_listed(&path)? {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_tree_is_an_error() {
        let cancel = AtomicBool::new(false);
        assert!(check_walk(&cancel, MAX_DEPTH).is_ok());
        assert!(check_walk(&cancel, MAX_DEPTH + 1).is_err());
        cancel.store(true, Ordering::Relaxed);
        assert!(check_walk(&cancel, 0).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_control_character_in_a_file_name_is_escaped_in_the_error() {
        let Err(Error::InvalidInput(message)) = is_listed(Path::new("/tmp/\x1b]52;c;x\x07")) else {
            panic!("a control character in the name is invalid");
        };
        assert!(message.contains("\\u{1b}"), "{message}");
        assert!(
            !message.chars().any(crate::text::is_unsafe_char),
            "{message}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_name_check_agrees_with_the_full_path_check() {
        use std::ffi::CString;

        let cases = [
            ("/tmp/d", "a"),
            ("/tmp/d", "a#12"),
            ("/tmp/d", "a#x"),
            ("/tmp/d", "a%41 b"),
            ("/tmp/d", "a\x07"),
            ("/tmp/\x07d", "a"),
        ];
        for (dir, name) in cases {
            let full = is_listed(&Path::new(dir).join(name)).map_err(|e| e.to_string());
            let name = CString::new(name).expect("name");
            let fast = is_listed_in(Path::new(dir), &mut None, &name).map_err(|e| e.to_string());
            assert_eq!(fast, full, "{dir:?} {name:?}");
        }
    }
}
