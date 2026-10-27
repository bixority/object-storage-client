//! Shared backend cache and HTTP client used by every [`Client`](crate::Client).

use crate::backend::{self, Backend, Vars};
use crate::client::{Error, Result};
use object_store::path::Path as ObjectPath;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;
use url::{Position, Url};

/// Everything that changes the store built for a URL: scheme, credentials
/// (user name and password), host and port. Path, query and fragment do not.
pub fn origin(url: &Url) -> &str {
    &url[..Position::AfterPort]
}

/// The object a URL names: its percent-decoded path. A bucket-only URL (e.g.
/// `s3://bucket`) has an empty path, which yields the root path.
pub fn object_path(url: &Url) -> Result<ObjectPath> {
    ObjectPath::from_url_path(url.path())
        .map_err(|e| Error::InvalidInput(format!("Invalid object path in URL: {e}")))
}

/// A resolved backend and the object path within it.
type Located = (Arc<dyn Backend>, ObjectPath);

/// How many backends a registry keeps. An application that forwards user URLs
/// would otherwise grow the cache without bound; past the cap a backend is
/// built per call and not kept.
const MAX_CACHED_STORES: usize = 256;

/// Cache of resolved backends keyed on (scheme, userinfo, host, port), so each provider's
/// `object_store` is built once and reused across calls, plus the shared HTTP
/// client for bucket operations and other direct requests. The cache holds at
/// most [`MAX_CACHED_STORES`] backends.
pub struct StoreRegistry {
    stores: Mutex<HashMap<String, Arc<dyn Backend>>>,
    capacity: usize,
    http: OnceLock<reqwest::Client>,
    vars: Vars,
}

impl Default for StoreRegistry {
    fn default() -> Self {
        Self::with_capacity(MAX_CACHED_STORES, Vars::default())
    }
}

impl StoreRegistry {
    /// Resolves the provider-specific [`Backend`] for a given URL, building it
    /// on first use and caching it by (scheme, userinfo, host, port) so
    /// subsequent calls reuse the same store and credentials.
    ///
    /// Concurrent first uses of a key may each build a backend, but all of
    /// them get the one that was stored first. Building is synchronous but not
    /// free of I/O (GCS reads its service-account file, the HTTP backend
    /// builds a TLS client), so it runs outside the lock.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is unsupported.
    /// - The URL is missing required components (e.g., bucket name for S3/GCS).
    /// - There is an error building the underlying `ObjectStore`.
    pub fn resolve(&self, url: &Url) -> Result<Located> {
        let object_path = object_path(url)?;
        let key = origin(url);
        let cached = self.lock().get(key).cloned();
        if let Some(backend) = cached {
            return Ok((backend, object_path));
        }
        let built = backend::build(url, &self.vars)?;
        Ok((self.keep(key, built), object_path))
    }

    /// Resolves the two ends of a copy or move. URLs with the same origin get
    /// the one backend even when the cache is full and would build one per
    /// call, so a copy inside one store can still be done server-side (the
    /// caller tells that by `Arc::ptr_eq`).
    ///
    /// # Errors
    ///
    /// As [`Self::resolve`].
    pub fn resolve_pair(&self, from: &Url, to: &Url) -> Result<(Located, Located)> {
        let first = self.resolve(from)?;
        if origin(from) == origin(to) {
            return Ok((first.clone(), (first.0, object_path(to)?)));
        }
        Ok((first, self.resolve(to)?))
    }

    /// Stores `built` under `key` unless the key is already cached (that
    /// backend wins) or the cache is full, and returns the backend to use.
    fn keep(&self, key: &str, built: Arc<dyn Backend>) -> Arc<dyn Backend> {
        let mut cache = self.lock();
        // Past the cap, only already cached keys are served from the cache.
        if cache.len() >= self.capacity && !cache.contains_key(key) {
            return built;
        }
        Arc::clone(cache.entry(key.to_owned()).or_insert(built))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Arc<dyn Backend>>> {
        self.stores.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A registry whose backends read configuration from `vars` instead of the
    /// process environment.
    pub fn with_vars(vars: HashMap<String, String>) -> Self {
        Self::with_capacity(MAX_CACHED_STORES, Vars::Map(Arc::new(vars)))
    }

    fn with_capacity(capacity: usize, vars: Vars) -> Self {
        Self {
            stores: Mutex::default(),
            capacity,
            http: OnceLock::new(),
            vars,
        }
    }

    /// Number of distinct backends built so far.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// The shared HTTP client for bucket operations, built on first use. A
    /// server that does not answer cannot hang a call: connecting is given 5
    /// seconds and a whole request 30.
    pub fn http(&self) -> &reqwest::Client {
        self.http.get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default()
        })
    }
}

/// Parses a URL string into a [`Url`], treating absolute or relative paths
/// without a scheme as `file://` URLs.
pub fn parse_url(url: &str) -> Result<Url> {
    match Url::parse(url) {
        Ok(parsed) => {
            // On Windows, an absolute path like `C:\path` might be parsed
            // as having scheme `c`. If the scheme is a single character,
            // we treat it as a local filesystem path instead.
            if parsed.scheme().len() == 1 {
                path_to_file_url(url)
            } else {
                Ok(parsed)
            }
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => path_to_file_url(url),
        Err(e) => Err(e.into()),
    }
}

/// `~` or `~/rest` under `home`; `None` for any other path.
fn expand_tilde(path: &str, home: Option<PathBuf>) -> Result<Option<PathBuf>> {
    let Some(rest) = (if path == "~" {
        Some("")
    } else {
        path.strip_prefix("~/")
    }) else {
        return Ok(None);
    };
    let home = home.ok_or_else(|| Error::Generic("Failed to determine home directory".into()))?;
    Ok(Some(if rest.is_empty() {
        home
    } else {
        home.join(rest)
    }))
}

/// Converts a local filesystem path (absolute or relative) into a `file://`
/// [`Url`].
fn path_to_file_url(path_str: &str) -> Result<Url> {
    let abs_path = if let Some(path) = expand_tilde(path_str, std::env::home_dir())? {
        path
    } else {
        let path = std::path::Path::new(path_str);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        }
    };
    Url::from_file_path(&abs_path).map_err(|()| {
        Error::Generic(format!(
            "Failed to convert path to URL: {}",
            abs_path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(url: &str) -> Result<Vec<String>> {
        let url = Url::parse(url).expect("url");
        let (_, path) = StoreRegistry::default().resolve(&url)?;
        Ok(path.parts().map(|p| p.as_ref().to_owned()).collect())
    }

    #[test]
    fn url_paths_are_percent_decoded_once() {
        let got = parts("file:///tmp/dir%20a/x%20%25%C3%BC.bin").expect("resolve");
        assert_eq!(got, ["tmp", "dir a", "x %\u{fc}.bin"]);
    }

    #[test]
    fn backends_are_keyed_on_scheme_userinfo_host_and_port() {
        let registry = StoreRegistry::default();
        let build = |url: &str| {
            registry
                .resolve(&Url::parse(url).expect("url"))
                .expect("resolve");
            registry.len()
        };
        assert_eq!(build("http://h/a?x=1"), 1);
        // Path and query do not matter.
        assert_eq!(build("http://h/b"), 1);
        assert_eq!(build("https://h/a"), 2);
        assert_eq!(build("http://h:8080/a"), 3);
        assert_eq!(build("http://u@h/a"), 4);
        assert_eq!(build("http://u:p@h/a"), 5);
        assert_eq!(build("http://other/a"), 6);
    }

    #[test]
    fn a_pair_of_one_origin_shares_a_backend_past_the_cap() {
        let registry = StoreRegistry::with_capacity(1, Vars::default());
        let url = |u: &str| Url::parse(u).expect("url");
        registry.resolve(&url("http://full/x")).expect("fill");

        // Past the cap a single resolve builds a backend per call...
        let (a, _) = registry.resolve(&url("http://c/x")).expect("a");
        let (b, _) = registry.resolve(&url("http://c/y")).expect("b");
        assert!(!Arc::ptr_eq(&a, &b));

        // ...but a pair shares one, and keeps its own paths.
        let ((a, pa), (b, pb)) = registry
            .resolve_pair(&url("http://c/x"), &url("http://c/y%20z"))
            .expect("pair");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!((pa.as_ref(), pb.as_ref()), ("x", "y z"));

        // Other origins stay separate: no normalisation beyond the cache key.
        let ((a, _), (b, _)) = registry
            .resolve_pair(&url("http://c/x"), &url("http://d/x"))
            .expect("pair");
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn the_http_client_is_built_once_on_first_use() {
        let registry = StoreRegistry::default();
        assert!(registry.http.get().is_none());
        assert!(std::ptr::eq(registry.http(), registry.http()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_server_cannot_hang_a_bucket_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        // Accepts and then says nothing.
        let silent = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.expect("accept");
            std::future::pending::<()>().await;
            drop(conn);
        });
        let registry = StoreRegistry::default();
        let err = registry
            .http()
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect_err("timed out");
        assert!(err.is_timeout(), "{err}");
        silent.abort();
    }

    #[test]
    fn the_cache_is_capped_and_uncached_backends_are_still_served() {
        let registry = StoreRegistry::with_capacity(2, Vars::default());
        let resolve = |url: &str| {
            registry
                .resolve(&Url::parse(url).expect("url"))
                .expect("resolve")
                .0
        };
        let (a, b) = (resolve("http://a/x"), resolve("http://b/x"));
        assert_eq!(registry.len(), 2);

        // Beyond the cap each call builds its own backend, kept nowhere.
        let (c1, c2) = (resolve("http://c/x"), resolve("http://c/y"));
        assert_eq!(registry.len(), 2);
        assert!(!Arc::ptr_eq(&c1, &c2));

        // Cached keys are still shared.
        assert!(Arc::ptr_eq(&a, &resolve("http://a/y")));
        assert!(Arc::ptr_eq(&b, &resolve("http://b/y")));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn a_leading_tilde_is_the_home_directory() {
        let home = || Some(PathBuf::from("/home/u"));
        let expand = |p| expand_tilde(p, home()).expect("expand");
        assert_eq!(expand("~"), Some(PathBuf::from("/home/u")));
        assert_eq!(expand("~/x/y"), Some(PathBuf::from("/home/u/x/y")));
        assert_eq!(expand("~u/x"), None);
        assert_eq!(expand("a/~/x"), None);
        assert!(expand_tilde("~/x", None).is_err());
        assert_eq!(expand_tilde("/x", None).expect("expand"), None);
    }

    #[test]
    fn plus_is_literal_and_invalid_utf8_is_rejected() {
        let got = parts("file:///tmp/a+b").expect("resolve");
        assert_eq!(got, ["tmp", "a+b"]);
        assert!(matches!(
            parts("file:///tmp/%FF"),
            Err(Error::InvalidInput(_))
        ));
    }
}
