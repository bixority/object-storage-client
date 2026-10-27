//! Deleting everything under a `file://` prefix without following symlinks.
//!
//! `object_store`'s local `list` walks with `follow_links(true)`, so listing
//! and then deleting by logical path would delete files outside the prefix
//! through a symlinked directory. This walk never follows a symlink below the
//! prefix: a link is removed as a link.
//!
//! The entries it deletes mirror what `LocalFileSystem::list` reports: regular
//! files and symlinks, minus names `object_store` treats as in-flight upload
//! staging files (`name#<digits>`). Directories are left in place, as
//! `LocalFileSystem` without automatic cleanup does.

use crate::client::{Error, Result};
use object_store::path::Path as ObjectPath;
use std::path::{Path, PathBuf};
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
fn is_listed_name(name: &str) -> bool {
    match name.split_once('#') {
        Some((_, suffix)) if !suffix.is_empty() => !suffix.bytes().all(|b| b.is_ascii_digit()),
        _ => true,
    }
}

/// The file system path `object_store`'s local store maps `prefix` to.
fn prefix_to_fs_path(prefix: &ObjectPath) -> PathBuf {
    // The same mapping as `object_store`: through a `file:///` URL, which on
    // Windows yields `C:\data` for a first part `C:` (a plain `push` would
    // give the drive-relative `C:data`).
    let mut url = url::Url::parse("file:///").expect("static URL is valid");
    if let Ok(mut segments) = url.path_segments_mut() {
        segments.pop_if_empty().extend(prefix.parts());
    }
    url.to_file_path()
        .unwrap_or_else(|()| PathBuf::from(std::path::MAIN_SEPARATOR_STR))
}

/// Whether a file at `fs_path` is one `object_store` would list. A name it
/// cannot map to an object path makes its list fail, so this errors too.
fn is_listed(fs_path: &Path) -> Result<bool> {
    let location = ObjectPath::from_absolute_path(fs_path)
        .map_err(|e| Error::InvalidInput(format!("Invalid path {}: {e}", fs_path.display())))?;
    Ok(location.filename().is_none_or(is_listed_name))
}

/// Deletes every listed file and symlink below the directory `prefix`, never
/// following a symlink. A missing prefix, or one that is not a directory, is
/// not an error.
pub async fn delete_by_prefix(prefix: ObjectPath) -> Result<()> {
    let root = prefix_to_fs_path(&prefix);
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

    for (name, file_type) in entries {
        check_walk(cancel, depth)?;
        let child = path.join(OsStr::from_bytes(name.to_bytes()));
        match file_type {
            FileType::Directory => {
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
                if !is_listed(&child)? {
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
    use object_store::local::LocalFileSystem;

    #[test]
    fn prefix_mapping_matches_object_store() {
        let fs = LocalFileSystem::new();
        for p in ["a", "a/b/c", "with space/x%20y"] {
            let prefix = ObjectPath::from(p);
            assert_eq!(
                prefix_to_fs_path(&prefix),
                fs.path_to_filesystem(&prefix).expect("maps")
            );
        }
    }

    #[test]
    fn deep_tree_is_an_error() {
        let cancel = AtomicBool::new(false);
        assert!(check_walk(&cancel, MAX_DEPTH).is_ok());
        assert!(check_walk(&cancel, MAX_DEPTH + 1).is_err());
        cancel.store(true, Ordering::Relaxed);
        assert!(check_walk(&cancel, 0).is_err());
    }
}
