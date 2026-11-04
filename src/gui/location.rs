//! The GUI's address model: always a folder URL.

use super::display;
use crate::registry::{child_url, is_bucket_scheme, origin, parse_url, provider_root};
use percent_encoding::percent_decode_str;
use url::Url;

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

    /// The URL scheme, lowercase.
    pub(crate) fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Whether this is a provider root (`s3:///`), which lists the buckets or
    /// containers of a connection.
    pub(crate) fn is_provider_root(&self) -> bool {
        provider_root(&self.url).is_some()
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

    /// The enclosing folder, or `None` at the root. A bucket root's parent is
    /// its provider root.
    pub(crate) fn parent(&self) -> Option<Self> {
        if self.is_provider_root() {
            return None;
        }
        if self.is_root() {
            return is_bucket_scheme(self.url.scheme())
                .then(|| Url::parse(&format!("{}:///", self.url.scheme())).ok())
                .flatten()
                .map(|url| Self { url });
        }
        let mut url = self.url.clone();
        let path = url.path().trim_end_matches('/');
        let cut = path.rfind('/').map_or(0, |i| i + 1);
        // An empty path would drop the trailing slash and the root check.
        let parent = if cut == 0 { "/" } else { &path[..cut] }.to_owned();
        url.set_path(&parent);
        Some(Self { url })
    }

    /// The bucket or container name, for schemes that have one.
    pub(crate) fn bucket(&self) -> Option<String> {
        // A URL with user information is refused by the library, so the host
        // is always the bucket.
        if is_bucket_scheme(self.url.scheme()) {
            self.url.host_str().map(str::to_owned)
        } else {
            None
        }
    }

    /// The subfolder `name`, or `None` when `name` is empty, `.` or `..`:
    /// such a name would resolve to this folder or its parent.
    #[must_use]
    pub fn child_dir(&self, name: &str) -> Option<Self> {
        if !plain_segment(name) {
            return None;
        }
        if self.is_provider_root() {
            // A bucket name is raw, as the listing gives it.
            let url = Url::parse(&format!("{}://{name}/", self.url.scheme())).ok()?;
            return (url.host_str() == Some(name)).then_some(Self { url });
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

    /// The origin and percent-decoded path, so `(` and `%28` compare equal.
    fn key(&self) -> (&str, Vec<u8>) {
        (
            origin(&self.url),
            percent_decode_str(self.url.path()).collect(),
        )
    }

    /// Whether `other` is this folder or lies below it.
    pub(crate) fn contains(&self, other: &Self) -> bool {
        let (origin, path) = self.key();
        let (other_origin, other_path) = other.key();
        // Both paths end in `/`, so the prefix stays on segment boundaries.
        origin == other_origin && other_path.starts_with(&path)
    }

    /// Whether `other` is the same folder, however its path is encoded.
    pub(crate) fn same_folder(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(text: &str) -> Location {
        Location::parse(text).expect("parses")
    }

    #[test]
    fn contains_ignores_how_a_path_is_encoded() {
        // `parse` keeps `(` and `+`; `child_dir` encodes them.
        for name in ["a (1)", "a+b"] {
            let child = loc("file:///u/").child_dir(name).expect("valid name");
            let below = loc(&format!("file:///u/{name}/b/"));
            assert!(child.contains(&below), "{name}: {below:?}");
            assert!(child.same_folder(&loc(&format!("file:///u/{name}/"))));
        }
        assert!(!loc("file:///u/a/").contains(&loc("file:///u/ab/")));
        assert!(!loc("file:///u/a/").same_folder(&loc("file:///u/a/b/")));
        assert!(!loc("s3://b/a/").contains(&loc("gs://b/a/")));
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
        assert!(!c.is_provider_root());
        let root = c.parent().expect("provider root");
        assert_eq!(root.as_str(), "s3:///");
        assert_eq!(root.parent(), None);
    }

    #[test]
    fn a_provider_root_is_a_location_that_opens_buckets() {
        for (text, root) in [
            ("s3://", "s3:///"),
            ("gs:///", "gs:///"),
            ("az://", "az:///"),
        ] {
            let at = loc(text);
            assert_eq!(at.as_str(), root);
            assert!(at.is_root() && at.is_provider_root());
            assert_eq!(at.bucket(), None);
            assert_eq!(at.parent(), None);
        }
        assert_eq!(loc("s3://").scheme(), "s3");
        let bucket = loc("s3://").child_dir("My_bucket.1").expect("bucket");
        assert_eq!(bucket.as_str(), "s3://My_bucket.1/");
        assert_eq!(bucket.bucket().as_deref(), Some("My_bucket.1"));
        assert_eq!(bucket.parent(), Some(loc("s3://")));
        assert_eq!(
            loc("gs://").child_dir("b").expect("bucket").as_str(),
            "gs://b/"
        );
        assert_eq!(loc("s3://").child_dir("a/b"), None);
        assert_eq!(loc("s3://").child_dir(".."), None);
        assert!(!loc("file:///").is_provider_root());
        assert_eq!(loc("file:///").parent(), None);
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

    #[test]
    fn parent_chain_of_empty_segments_ends_at_the_bucket_root() {
        let mut at = loc("s3://b//p/");
        let mut chain = vec![at.as_str().to_owned()];
        while let Some(up) = at.parent() {
            chain.push(up.as_str().to_owned());
            at = up;
        }
        assert_eq!(chain, ["s3://b//p/", "s3://b//", "s3://b/", "s3:///"]);
        assert!(at.is_provider_root());
    }
}
