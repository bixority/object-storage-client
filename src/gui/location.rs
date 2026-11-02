//! The GUI's address model: always a folder URL.

use super::display;
use crate::registry::{child_url, parse_url};
use url::Url;

/// Whether `scheme` names a bucket or container store.
fn is_bucket_scheme(scheme: &str) -> bool {
    matches!(
        scheme,
        "s3" | "gs" | "gcs" | "az" | "wasb" | "wasbs" | "abfs" | "abfss"
    )
}

/// Whether `segment` names an entry of its own: not empty, `.` or `..`.
fn plain_segment(segment: &str) -> bool {
    !matches!(segment, "" | "." | "..")
}

/// A folder in some store. The URL path always ends in `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    url: Url,
}

impl Location {
    /// Parses user text into a folder location, adding a trailing `/`.
    ///
    /// # Errors
    /// Returns display-safe text when the URL or path does not parse.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut url = parse_url(text.trim()).map_err(|err| display::text(&err.to_string()))?;
        if !matches!(url.scheme(), "file" | "http" | "https") && !is_bucket_scheme(url.scheme()) {
            return Err(display::text(&format!(
                "unsupported URL scheme `{}`",
                url.scheme()
            )));
        }
        if !url.path().ends_with('/') {
            let path = format!("{}/", url.path());
            url.set_path(&path);
        }
        Ok(Self { url })
    }

    /// The URL, for client calls.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.url.as_str()
    }

    /// The URL for display, with credentials redacted.
    pub(crate) fn display(&self) -> String {
        display::url(self.as_str())
    }

    /// Whether this is the top of its store.
    pub(crate) fn is_root(&self) -> bool {
        let path = self.url.path();
        if path == "/" {
            return true;
        }
        if self.url.scheme() != "file" || !cfg!(windows) {
            return false;
        }
        let bytes = path.as_bytes();
        bytes.len() == 4
            && bytes[0] == b'/'
            && bytes[1].is_ascii_alphabetic()
            && &bytes[2..] == b":/"
    }

    /// The enclosing folder, or `None` at the root.
    pub(crate) fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        let mut url = self.url.clone();
        let path = url.path().trim_end_matches('/');
        let cut = path.rfind('/').map_or(0, |i| i + 1);
        let parent = path[..cut].to_owned();
        url.set_path(&parent);
        Some(Self { url })
    }

    /// The bucket or container name, for schemes that have one.
    pub(crate) fn bucket(&self) -> Option<String> {
        match self.url.scheme() {
            scheme if is_bucket_scheme(scheme) => {
                let user = self.url.username();
                if user.is_empty() {
                    self.url.host_str().map(str::to_owned)
                } else {
                    Some(user.to_owned())
                }
            }
            _ => None,
        }
    }

    /// The subfolder `name`, or `None` when `name` is empty, `.` or `..`:
    /// such a name would resolve to this folder or its parent.
    #[must_use]
    pub fn child_dir(&self, name: &str) -> Option<Self> {
        if !plain_segment(name) {
            return None;
        }
        // Built on the path, so it cannot fail: the segment is percent-encoded.
        let mut url = self.url.clone();
        url.set_path(&format!("{}/", child_url(self.url.path(), [name])));
        Some(Self { url })
    }

    /// The URL of the object `name` (which may contain `/`) in this folder, or
    /// `None` when any `/`-separated segment is empty, `.` or `..`.
    #[must_use]
    pub fn child_object(&self, name: &str) -> Option<String> {
        name.split('/')
            .all(plain_segment)
            .then(|| child_url(self.as_str(), name.split('/')))
    }

    /// Whether `other` is this folder or lies below it.
    pub(crate) fn contains(&self, other: &Self) -> bool {
        other.as_str().starts_with(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(text: &str) -> Location {
        Location::parse(text).expect("parses")
    }

    #[test]
    fn parse_adds_trailing_slash() {
        assert_eq!(loc("s3://b/p").as_str(), "s3://b/p/");
    }

    #[test]
    fn parse_error_is_text() {
        let err = Location::parse("nope://x").expect_err("must fail");
        assert_ne!(err, "");
    }

    #[test]
    fn parent_stops_at_bucket_root() {
        let a = loc("s3://b/p/q/");
        let b = a.parent().expect("parent");
        assert_eq!(b.as_str(), "s3://b/p/");
        let c = b.parent().expect("parent");
        assert_eq!(c.as_str(), "s3://b/");
        assert!(c.is_root());
        assert_eq!(c.parent(), None);
    }

    #[test]
    fn file_root() {
        assert_eq!(loc("file:///").parent(), None);
    }

    #[test]
    fn bucket_names() {
        assert_eq!(loc("s3://media/").bucket().as_deref(), Some("media"));
        assert_eq!(loc("file:///tmp/").bucket(), None);
        assert_eq!(loc("https://h/x/").bucket(), None);
        assert_eq!(
            loc("abfss://c@acct.dfs.core.windows.net/")
                .bucket()
                .as_deref(),
            Some("c")
        );
    }

    #[test]
    fn child_round_trip() {
        let root = loc("s3://b/");
        let dir = root.child_dir("a b").expect("plain name");
        assert_eq!(dir.as_str(), "s3://b/a%20b/");
        assert_eq!(
            root.child_object("x/y z").as_deref(),
            Some("s3://b/x/y%20z")
        );
    }

    #[test]
    fn child_rejects_dot_segments() {
        let p = loc("s3://b/p/");
        assert_eq!(p.child_dir(".."), None);
        assert_eq!(p.child_dir("."), None);
        assert_eq!(p.child_dir(""), None);
        assert_eq!(p.child_object("a/../b"), None);
        assert_eq!(p.child_object("./a"), None);
        assert_eq!(p.child_object(".."), None);
        assert_eq!(p.child_object("a//b"), None);
        assert_eq!(p.child_object(""), None);
        assert_eq!(
            p.child_object("a.b/..c").as_deref(),
            Some("s3://b/p/a.b/..c")
        );
    }

    #[test]
    fn contains_self_and_below() {
        let p = loc("s3://b/p/");
        assert!(p.contains(&p));
        assert!(p.contains(&loc("s3://b/p/q/")));
        assert!(!p.contains(&loc("s3://b/pq/")));
    }

    #[test]
    fn display_hides_credentials() {
        assert!(!loc("https://u:pw@h/x/").display().contains("pw"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_drive_path() {
        assert!(loc(r"C:\data").as_str().starts_with("file:///C:/data/"));
    }
}
