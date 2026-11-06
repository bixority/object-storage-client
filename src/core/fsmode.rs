//! File-mode rules shared by the `file://` writer and the CLI download.
#![cfg_attr(not(unix), allow(dead_code))]

/// The process umask, or `None` when it cannot be read. It is read from
/// `/proc/self/status`, which avoids the racy set-and-restore of `umask(2)`,
/// so it is available on Linux only: on other platforms, and where `/proc` is
/// not mounted, this is `None`. [`umask_near`] then learns it from a probe.
fn current_umask() -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status
            .lines()
            .find_map(|l| l.strip_prefix("Umask:"))
            .and_then(|v| u32::from_str_radix(v.trim(), 8).ok())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Destinations whose umask is learned from the probe even where `/proc`
/// would answer, for tests.
#[cfg(all(test, unix))]
pub static PROBE_ONLY: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

/// Destinations whose probe fails, for tests.
#[cfg(all(test, unix))]
pub static PROBE_FAULTS: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

/// The umask for a file about to be created at `path`, or `None` when it
/// cannot be learned.
///
/// It is read from `/proc` where that exists, unless the directory has a
/// default ACL (Linux), which the umask does not govern. Elsewhere (macOS, other Unix)
/// it is learned from a probe: an empty file is created with mode `0o666` in
/// the nearest existing directory of `path`, its mode is read, and it is
/// removed again. `umask(2)` is never called, because it is process-wide and
/// races with other threads.
pub fn umask_near(path: &std::path::Path) -> Option<u32> {
    #[cfg(all(test, unix))]
    let skip_proc = PROBE_ONLY
        .lock()
        .is_ok_and(|set| set.iter().any(|p| p == path));
    #[cfg(not(all(test, unix)))]
    let skip_proc = false;
    if !skip_proc
        && !has_default_acl(path)
        && let Some(umask) = current_umask()
    {
        return Some(umask);
    }
    probe_umask(path)
}

/// The nearest existing directory above `path`; the empty path stands for
/// the current directory.
#[cfg(unix)]
fn nearest_dir(path: &std::path::Path) -> Option<&std::path::Path> {
    let dir = path
        .ancestors()
        .skip(1)
        .find(|p| p.as_os_str().is_empty() || p.is_dir())?;
    Some(if dir.as_os_str().is_empty() {
        std::path::Path::new(".")
    } else {
        dir
    })
}

/// Whether the directory a file at `path` will be created in has a default
/// POSIX ACL. The kernel then ignores the umask, so `/proc` cannot say what
/// mode a new file gets and the probe must.
fn has_default_acl(path: &std::path::Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        let Some(dir) = nearest_dir(path) else {
            return false;
        };
        // A one-byte buffer: a short ACL fits (`Ok`) and a longer one gives `ERANGE`;
        // either way it is present.
        match rustix::fs::getxattr(dir, "system.posix_acl_default", &mut [0_u8; 1]) {
            Ok(_) => true,
            Err(e) => e == rustix::io::Errno::RANGE,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        false
    }
}

#[cfg(unix)]
fn probe_umask(path: &std::path::Path) -> Option<u32> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    #[cfg(test)]
    if PROBE_FAULTS
        .lock()
        .is_ok_and(|set| set.iter().any(|p| p == path))
    {
        return None;
    }
    let dir = nearest_dir(path)?;
    for _ in 0..4 {
        let mut suffix = [0_u8; 8];
        aws_lc_rs::rand::fill(&mut suffix).ok()?;
        let probe = dir.join(format!(".osc-umask-{}", crate::hex::encode(&suffix)));
        let created = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits().cast_signed())
            .open(&probe);
        match created {
            Ok(file) => {
                let mode = file.metadata().map(|m| m.permissions().mode() & 0o777);
                drop(file);
                let _ = std::fs::remove_file(&probe);
                return mode.ok().map(|mode| !mode & 0o777);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return None,
        }
    }
    None
}

#[cfg(not(unix))]
fn probe_umask(_path: &std::path::Path) -> Option<u32> {
    None
}

/// The permission bits (`0o777`, never setuid, setgid or sticky) of the
/// `(mode, owner)` of an existing file, if the effective user `euid` owns it.
/// A file owned by someone else (planted in a shared directory) must not
/// decide who can read what is written over it.
pub fn owned_mode(existing: Option<(u32, u32)>, euid: u32) -> Option<u32> {
    existing
        .filter(|&(_, owner)| owner == euid)
        .map(|(mode, _)| mode & 0o777)
}

/// Decides `(creation mode, final mode)` of a written file.
///
/// A move (`moving`) with a known `source` takes the source's permission bits
/// (never setuid, setgid or sticky), whatever it replaces and whatever the
/// umask, as a rename would keep them.
///
/// `existing` is the `(mode, owner)` of the regular file being replaced; its
/// mode is adopted only when `euid` owns it (see [`owned_mode`]). Otherwise a
/// copy takes `source` and anything else `0o666`, both less the umask. The
/// file is created 0600 and gets the final mode once complete. Without a
/// known umask the file is created with the source's permission bits (never
/// setuid, setgid or sticky) and the kernel applies the real umask, so a copy
/// is never more permissive than its source and a private source is staged
/// privately; with no source nothing changes (`None, None`).
pub fn modes(
    existing: Option<(u32, u32)>,
    euid: u32,
    source: Option<u32>,
    moving: bool,
    umask: Option<u32>,
) -> (Option<u32>, Option<u32>) {
    if moving && let Some(s) = source {
        return (Some(0o600), Some(s & 0o777));
    }
    if let Some(mode) = owned_mode(existing, euid) {
        return (Some(0o600), Some(mode));
    }
    umask.map_or_else(
        || (source.map(|s| s & 0o777), None),
        |m| {
            let base = source.map_or(0o666, |s| s & 0o777);
            (Some(0o600), Some(base & !m & 0o777))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_file_gets_default_less_umask_after_private_staging() {
        assert_eq!(
            modes(None, 1000, None, false, Some(0o022)),
            (Some(0o600), Some(0o644))
        );
        assert_eq!(
            modes(None, 1000, None, false, Some(0o077)),
            (Some(0o600), Some(0o600))
        );
    }

    /// The default creation mode of a new file in `dir`, learned without the
    /// crate's probe.
    #[cfg(unix)]
    fn default_mode_in(dir: &std::path::Path) -> u32 {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let path = dir.join("independent-probe");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&path)
            .expect("probe");
        let mode = file.metadata().expect("meta").permissions().mode() & 0o777;
        std::fs::remove_file(path).expect("remove");
        mode
    }

    #[cfg(unix)]
    #[test]
    fn the_probe_learns_the_umask_and_leaves_no_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        // The parent directory does not exist yet: the probe uses the nearest
        // one that does.
        let path = dir.path().join("missing/new");
        PROBE_ONLY.lock().expect("lock").push(path.clone());
        let umask = umask_near(&path);
        PROBE_ONLY.lock().expect("lock").retain(|p| *p != path);

        assert_eq!(umask, Some(!default_mode_in(dir.path()) & 0o777));
        assert_eq!(std::fs::read_dir(dir.path()).expect("dir").count(), 0);
    }

    #[test]
    fn a_move_takes_the_source_bits_over_an_owned_destination() {
        // A cross-device move gives what a rename gives: the source's bits,
        // without setuid, setgid and sticky, whatever it replaces.
        assert_eq!(
            modes(
                Some((0o100_644, 1000)),
                1000,
                Some(0o104_600),
                true,
                Some(0o022)
            ),
            (Some(0o600), Some(0o600))
        );
        assert_eq!(
            modes(None, 1000, Some(0o100_666), true, Some(0o077)),
            (Some(0o600), Some(0o666))
        );
        // The same copy keeps the destination's bits.
        assert_eq!(
            modes(
                Some((0o100_644, 1000)),
                1000,
                Some(0o600),
                false,
                Some(0o022)
            ),
            (Some(0o600), Some(0o644))
        );
        // Without a known source a move is not special.
        assert_eq!(
            modes(Some((0o100_644, 1000)), 1000, None, true, Some(0o022)),
            (Some(0o600), Some(0o644))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_default_acl_makes_the_probe_decide_the_umask() {
        let dir = tempfile::tempdir().expect("tempdir");
        let set = std::process::Command::new("setfacl")
            .args(["-d", "-m", "o::---"])
            .arg(dir.path())
            .output();
        if !set.is_ok_and(|o| o.status.success()) {
            eprintln!("skipped: setfacl or ACL support is missing");
            return;
        }
        let path = dir.path().join("new");
        assert!(has_default_acl(&path));
        let umask = umask_near(&path).expect("umask");
        assert_eq!(umask, !default_mode_in(dir.path()) & 0o777);
        assert_eq!(umask & 0o007, 0o007, "others stay locked out");
        assert!(!has_default_acl(&std::env::temp_dir().join("x")) || umask != 0);
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_probe_falls_back_to_the_core_umask_rule() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("new");
        PROBE_ONLY.lock().expect("lock").push(path.clone());
        PROBE_FAULTS.lock().expect("lock").push(path.clone());
        let umask = umask_near(&path);
        PROBE_ONLY.lock().expect("lock").retain(|p| *p != path);
        PROBE_FAULTS.lock().expect("lock").retain(|p| *p != path);

        assert_eq!(umask, None);
        assert_eq!(
            modes(None, 1000, Some(0o600), false, umask),
            (Some(0o600), None)
        );
        assert_eq!(std::fs::read_dir(dir.path()).expect("dir").count(), 0);
    }

    #[test]
    fn an_unknown_umask_creates_with_the_source_bits() {
        assert_eq!(
            modes(None, 1000, Some(0o600), false, None),
            (Some(0o600), None)
        );
        assert_eq!(
            modes(None, 1000, Some(0o104_755), false, None),
            (Some(0o755), None)
        );
        assert_eq!(modes(None, 1000, None, false, None), (None, None));
    }

    #[test]
    fn own_destination_keeps_its_mode_without_special_bits() {
        assert_eq!(
            modes(Some((0o104_600, 1000)), 1000, None, false, None),
            (Some(0o600), Some(0o600))
        );
        assert_eq!(
            modes(
                Some((0o100_640, 1000)),
                1000,
                Some(0o600),
                false,
                Some(0o022)
            ),
            (Some(0o600), Some(0o640))
        );
    }

    #[test]
    fn foreign_destination_is_treated_as_new() {
        assert_eq!(
            modes(Some((0o100_666, 0)), 1000, None, false, Some(0o022)),
            (Some(0o600), Some(0o644))
        );
    }

    #[test]
    fn copy_takes_source_mode_less_umask() {
        assert_eq!(
            modes(None, 1000, Some(0o104_755), false, Some(0o022)),
            (Some(0o600), Some(0o755))
        );
        assert_eq!(
            modes(None, 1000, Some(0o600), false, Some(0o022)),
            (Some(0o600), Some(0o600))
        );
    }
}
