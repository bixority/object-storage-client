//! The source tree of a recursive upload, walked through directory file
//! descriptors on Unix, and the mapping of its files to destination URLs.

use super::CliError;
#[cfg(not(unix))]
use super::transfer::read_err;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::fs;
use std::ops::ControlFlow;
use std::path::{Component, Path, PathBuf};

/// What stays unescaped when a relative file path is appended to a URL.
///
/// Applied to one path component at a time, so `/`, `\` and `%` are always
/// escaped.
pub(super) const URL_PATH_SEGMENTS: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.');

/// Device and inode of a file or directory found by the directory walk.
#[cfg(unix)]
pub(super) type FileId = (u64, u64);

/// A regular file found by [`SourceTree::walk`].
#[derive(Debug, Clone)]
pub(super) struct TreeFile {
    /// Path relative to the scanned root.
    pub rel: PathBuf,
    /// Identity of the file itself (Unix; other platforms open by path).
    #[cfg(unix)]
    id: FileId,
    /// Identity of each directory between the root and the file, outermost
    /// first.
    #[cfg(unix)]
    dirs: Vec<FileId>,
}

/// A source directory tree walked for a recursive upload.
///
/// On Unix the walk goes through directory file descriptors: the root is
/// opened once and held, every subdirectory is opened with `openat` relative to
/// its parent with `O_NOFOLLOW` and checked against the identity the listing
/// reported. [`Self::open`] later walks from the same root descriptor and
/// re-checks every directory on the chain, so swapping a directory for a
/// symlink at any point cannot redirect the upload outside the tree. Symlinks
/// inside the tree are skipped, as before.
#[derive(Debug)]
pub(super) struct SourceTree {
    root_path: PathBuf,
    #[cfg(unix)]
    root: std::os::fd::OwnedFd,
}

/// Deepest directory nesting a scan follows; each level holds a descriptor.
#[cfg(unix)]
const MAX_TREE_DEPTH: usize = 128;

#[cfg(unix)]
const fn stat_id(st: &rustix::fs::Stat) -> FileId {
    #[allow(clippy::unnecessary_cast, clippy::cast_sign_loss)]
    (st.st_dev as u64, st.st_ino as u64)
}

#[cfg(unix)]
fn io_err(path: &Path, e: rustix::io::Errno) -> CliError {
    CliError::ReadFile {
        path: path.display().to_string(),
        source: e.into(),
    }
}

/// Maps the errors `O_NOFOLLOW | O_DIRECTORY` gives for a swapped-in symlink
/// or non-directory to a refusal, anything else to a read error.
#[cfg(unix)]
fn swapped_or_io(path: &Path, e: rustix::io::Errno) -> CliError {
    use rustix::io::Errno;
    if e == Errno::LOOP || e == Errno::NOTDIR {
        CliError::UnsafePath {
            path: path.display().to_string(),
            reason: "path changed after the directory scan",
        }
    } else {
        io_err(path, e)
    }
}

impl SourceTree {
    /// Lists the tree below `root` into a vector, calling `hook` with a
    /// subdirectory's path after the listing reported it and before it is
    /// opened (lets tests swap it deterministically).
    ///
    /// # Errors
    ///
    /// As [`Self::open_root`] and [`Self::walk`].
    #[cfg(test)]
    pub fn scan_with_hook(
        root: &Path,
        hook: &mut dyn FnMut(&Path),
    ) -> Result<(Self, Vec<TreeFile>), CliError> {
        let tree = Self::open_root(root)?;
        let mut files = Vec::new();
        tree.walk(hook, &mut |file| {
            files.push(file);
            ControlFlow::Continue(())
        })?;
        Ok((tree, files))
    }

    /// [`Self::scan_with_hook`] without a hook.
    ///
    /// # Errors
    ///
    /// As [`Self::scan_with_hook`].
    #[cfg(test)]
    pub fn scan(root: &Path) -> Result<(Self, Vec<TreeFile>), CliError> {
        Self::scan_with_hook(root, &mut |_| {})
    }
}

#[cfg(unix)]
impl SourceTree {
    /// Opens `root` (as named, so a symlink given by the user is followed).
    ///
    /// # Errors
    ///
    /// [`CliError::ReadFile`] when it cannot be opened as a directory.
    pub fn open_root(root: &Path) -> Result<Self, CliError> {
        use rustix::fs::{Mode, OFlags};
        let fd = rustix::fs::open(
            root,
            OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| io_err(root, e))?;
        Ok(Self {
            root_path: root.to_path_buf(),
            root: fd,
        })
    }

    /// Lists the tree below the root, handing each regular file to `emit`
    /// as it is found, and stops when `emit` breaks. `hook` is called with a
    /// subdirectory's path after the listing reported it and before it is
    /// opened.
    ///
    /// # Errors
    ///
    /// [`CliError::ReadFile`] when a directory cannot be read,
    /// [`CliError::UnsafePath`] when one is swapped during the walk or the tree
    /// is nested too deep.
    pub fn walk(
        &self,
        hook: &mut dyn FnMut(&Path),
        emit: &mut dyn FnMut(TreeFile) -> ControlFlow<()>,
    ) -> Result<(), CliError> {
        let mut rel = PathBuf::new();
        let mut dirs = Vec::new();
        // Whether the walk ended early or ran out is the same to the caller.
        scan_dir(&self.root_path, &self.root, &mut rel, &mut dirs, emit, hook).map(|_| ())
    }

    /// The path shown for `file` in messages.
    #[must_use]
    pub fn display_path(&self, file: &TreeFile) -> PathBuf {
        self.root_path.join(&file.rel)
    }

    /// Opens `file` for reading, refusing anything that is no longer what the
    /// scan saw: a directory on the way replaced (by a symlink or anything
    /// else), the file replaced, or the file no longer a regular file.
    ///
    /// # Errors
    ///
    /// [`CliError::UnsafePath`] when the tree changed since the scan,
    /// [`CliError::ReadFile`] when it cannot be opened.
    pub fn open(&self, file: &TreeFile) -> Result<tokio::fs::File, CliError> {
        use rustix::fs::{Mode, OFlags, openat};
        use std::os::fd::{AsFd, OwnedFd};
        let path = self.display_path(file);
        let changed = |reason| CliError::UnsafePath {
            path: path.display().to_string(),
            reason,
        };
        let dir_flags = OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::RDONLY;

        let comps: Vec<_> = file.rel.components().collect();
        let (name, parents) = comps.split_last().ok_or_else(|| changed("path is empty"))?;
        if parents.len() != file.dirs.len() {
            return Err(changed("file changed after the directory scan"));
        }

        let mut held: Option<OwnedFd> = None;
        for (comp, id) in parents.iter().zip(&file.dirs) {
            let cur = held.as_ref().map_or_else(|| self.root.as_fd(), AsFd::as_fd);
            let next = openat(cur, comp.as_os_str(), dir_flags, Mode::empty())
                .map_err(|e| swapped_or_io(&path, e))?;
            let st = rustix::fs::fstat(&next).map_err(|e| io_err(&path, e))?;
            if stat_id(&st) != *id {
                return Err(changed("directory changed after the directory scan"));
            }
            held = Some(next);
        }

        let cur = held.as_ref().map_or_else(|| self.root.as_fd(), AsFd::as_fd);
        let fd = openat(
            cur,
            name.as_os_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| swapped_or_io(&path, e))?;
        let st = rustix::fs::fstat(&fd).map_err(|e| io_err(&path, e))?;
        if rustix::fs::FileType::from_raw_mode(st.st_mode) != rustix::fs::FileType::RegularFile {
            return Err(changed("path is no longer a regular file"));
        }
        if stat_id(&st) != file.id {
            return Err(changed("file changed after the directory scan"));
        }
        Ok(tokio::fs::File::from_std(fs::File::from(fd)))
    }
}

/// Lists the directory held in `dir_fd` (shown as `dir_path`) and recurses into
/// its subdirectories, each opened relative to `dir_fd` without following
/// symlinks.
#[cfg(unix)]
fn scan_dir(
    dir_path: &Path,
    dir_fd: &std::os::fd::OwnedFd,
    rel: &mut PathBuf,
    dirs: &mut Vec<FileId>,
    emit: &mut dyn FnMut(TreeFile) -> ControlFlow<()>,
    hook: &mut dyn FnMut(&Path),
) -> Result<ControlFlow<()>, CliError> {
    use rustix::fs::{AtFlags, FileType, Mode, OFlags, openat, statat};
    use std::os::unix::ffi::OsStrExt;

    if dirs.len() >= MAX_TREE_DEPTH {
        return Err(CliError::UnsafePath {
            path: dir_path.display().to_string(),
            reason: "directory nesting is too deep",
        });
    }

    let mut subdirs = Vec::new();
    {
        let listing = rustix::fs::Dir::read_from(dir_fd).map_err(|e| io_err(dir_path, e))?;
        for entry in listing {
            let entry = entry.map_err(|e| io_err(dir_path, e))?;
            let raw = entry.file_name().to_bytes();
            if raw == b"." || raw == b".." {
                continue;
            }
            let name = std::ffi::OsStr::from_bytes(raw).to_os_string();
            let path = dir_path.join(&name);
            // Like `DirEntry::metadata`: a symlink is neither a file nor a
            // directory here and is skipped.
            let st = statat(dir_fd, name.as_os_str(), AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|e| io_err(&path, e))?;
            match FileType::from_raw_mode(st.st_mode) {
                FileType::Directory => subdirs.push((name, stat_id(&st))),
                FileType::RegularFile => {
                    let file = TreeFile {
                        rel: rel.join(&name),
                        id: stat_id(&st),
                        dirs: dirs.clone(),
                    };
                    if emit(file).is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                _ => {}
            }
        }
    }

    for (name, id) in subdirs {
        let path = dir_path.join(&name);
        hook(&path);
        let fd = openat(
            dir_fd,
            name.as_os_str(),
            OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::RDONLY,
            Mode::empty(),
        )
        .map_err(|e| swapped_or_io(&path, e))?;
        let opened = stat_id(&rustix::fs::fstat(&fd).map_err(|e| io_err(&path, e))?);
        if opened != id {
            return Err(CliError::UnsafePath {
                path: path.display().to_string(),
                reason: "directory changed during the scan",
            });
        }
        rel.push(&name);
        dirs.push(id);
        let res = scan_dir(&path, &fd, rel, dirs, emit, hook);
        dirs.pop();
        rel.pop();
        if res?.is_break() {
            return Ok(ControlFlow::Break(()));
        }
    }
    Ok(ControlFlow::Continue(()))
}

#[cfg(not(unix))]
impl SourceTree {
    /// Names `root`. Without directory descriptors there is no identity check
    /// on directories; files are opened by path.
    ///
    /// # Errors
    ///
    /// Never fails here; an unreadable root fails the walk.
    #[expect(clippy::unnecessary_wraps, reason = "as on Unix, where it can fail")]
    pub fn open_root(root: &Path) -> Result<Self, CliError> {
        Ok(Self {
            root_path: root.to_path_buf(),
        })
    }

    /// Lists the tree below the root, handing each regular file to `emit`
    /// as it is found, and stops when `emit` breaks. `hook` is not used on
    /// this platform.
    ///
    /// # Errors
    ///
    /// [`CliError::ReadFile`] when a directory cannot be read.
    pub fn walk(
        &self,
        _hook: &mut dyn FnMut(&Path),
        emit: &mut dyn FnMut(TreeFile) -> ControlFlow<()>,
    ) -> Result<(), CliError> {
        let read_err = |path: &Path, source| CliError::ReadFile {
            path: path.display().to_string(),
            source,
        };
        let mut stack = vec![self.root_path.clone()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).map_err(|e| read_err(&dir, e))? {
                let entry = entry.map_err(|e| read_err(&dir, e))?;
                let path = entry.path();
                let meta = entry.metadata().map_err(|e| read_err(&path, e))?;
                if meta.is_dir() {
                    stack.push(path);
                } else if meta.is_file() {
                    let rel = path
                        .strip_prefix(&self.root_path)
                        .expect("path discovered via root must start with root")
                        .to_path_buf();
                    if emit(TreeFile { rel }).is_break() {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    /// The path shown for `file` in messages.
    #[must_use]
    pub fn display_path(&self, file: &TreeFile) -> PathBuf {
        self.root_path.join(&file.rel)
    }

    /// Opens `file` for reading, refusing a path that is no longer a regular
    /// file. There is no identity or symlink check on this platform.
    ///
    /// # Errors
    ///
    /// [`CliError::UnsafePath`] when the path is no longer a regular file,
    /// [`CliError::ReadFile`] when it cannot be opened.
    pub fn open(&self, file: &TreeFile) -> Result<tokio::fs::File, CliError> {
        let path = self.display_path(file);
        let read_err = read_err(&path);
        let opened = fs::File::open(&path).map_err(&read_err)?;
        if !opened.metadata().map_err(read_err)?.is_file() {
            return Err(CliError::UnsafePath {
                path: path.display().to_string(),
                reason: "path is no longer a regular file",
            });
        }
        Ok(tokio::fs::File::from_std(opened))
    }
}

/// Appends `rel_path` to the destination prefix `dst_root_url` (ending in `/`).
///
/// Only plain UTF-8 components are accepted and each is percent-encoded on its
/// own, so no file name can add, remove or reinterpret a path segment. The
/// parsed result must still lie under the prefix.
///
/// # Errors
///
/// [`CliError::UnsafePath`] for a non-normal or non-UTF-8 component, or a URL
/// that leaves the destination prefix.
pub(super) fn destination_url(dst_root_url: &str, rel_path: &Path) -> Result<String, CliError> {
    let unsafe_path = |reason| CliError::UnsafePath {
        path: rel_path.display().to_string(),
        reason,
    };

    let mut encoded = Vec::new();
    for component in rel_path.components() {
        let Component::Normal(name) = component else {
            return Err(unsafe_path("path has a non-normal component"));
        };
        let name = name
            .to_str()
            .ok_or_else(|| unsafe_path("file name is not valid UTF-8"))?;
        encoded.push(utf8_percent_encode(name, URL_PATH_SEGMENTS).to_string());
    }
    if encoded.is_empty() {
        return Err(unsafe_path("path is empty"));
    }
    let dst_url = format!("{dst_root_url}{}", encoded.join("/"));

    let root = url::Url::parse(dst_root_url).map_err(|_| unsafe_path("invalid destination"))?;
    let parsed = url::Url::parse(&dst_url).map_err(|_| unsafe_path("invalid destination"))?;
    let root_path = if root.path().ends_with('/') {
        root.path().to_string()
    } else {
        format!("{}/", root.path())
    };
    let inside = parsed.scheme() == root.scheme()
        && parsed.host_str() == root.host_str()
        && parsed.port() == root.port()
        && parsed.path().starts_with(&root_path)
        && parsed.path().len() > root_path.len();
    if !inside {
        return Err(unsafe_path("destination leaves the target prefix"));
    }
    Ok(dst_url)
}

#[cfg(test)]
mod tests;
