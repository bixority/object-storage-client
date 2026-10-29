//! Shared backend cache and HTTP client used by every [`Client`](crate::Client).

use crate::backend::{self, AwsProfile, Backend, Vars};
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
/// would otherwise grow the cache without bound; at the cap the least recently
/// used backend is dropped to make room (calls already holding it keep it).
const MAX_CACHED_STORES: usize = 256;

/// The cached backends, each with the tick of its last use, so the least
/// recently used one can be evicted.
#[derive(Default)]
struct Cache {
    entries: HashMap<String, (u64, Arc<dyn Backend>)>,
    tick: u64,
}

impl Cache {
    /// The backend cached under `key`, marked as just used.
    fn get(&mut self, key: &str) -> Option<Arc<dyn Backend>> {
        self.tick += 1;
        let (used, backend) = self.entries.get_mut(key)?;
        *used = self.tick;
        Some(Arc::clone(backend))
    }
}

/// Cache of resolved backends keyed on [`origin`], so each provider's
/// `object_store` is built once and reused across calls, plus the shared HTTP
/// client for bucket operations and other direct requests. The cache holds at
/// most [`MAX_CACHED_STORES`] backends and evicts the least recently used one.
pub struct StoreRegistry {
    stores: Mutex<Cache>,
    capacity: usize,
    http: OnceLock<reqwest::Client>,
    vars: Vars,
    /// The named AWS profile for S3 credentials, shared by every S3 backend.
    /// It is fixed for the registry's lifetime: [`Self::with_profile`] starts
    /// a new, empty cache, which is what isolates profiles, so the profile is
    /// not part of the cache key.
    profile: Option<Arc<AwsProfile>>,
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
        let cached = self.lock().get(key);
        if let Some(backend) = cached {
            return Ok((backend, object_path));
        }
        let built = backend::build(url, &self.vars, self.profile.as_deref())?;
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
    /// backend wins), evicting the least recently used backend when the cache
    /// is full, and returns the backend to use.
    fn keep(&self, key: &str, built: Arc<dyn Backend>) -> Arc<dyn Backend> {
        let mut cache = self.lock();
        if let Some(existing) = cache.get(key) {
            return existing;
        }
        if cache.entries.len() >= self.capacity {
            let oldest = cache
                .entries
                .iter()
                .min_by_key(|(_, (used, _))| *used)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                cache.entries.remove(&oldest);
            }
        }
        let tick = cache.tick;
        cache
            .entries
            .insert(key.to_owned(), (tick, Arc::clone(&built)));
        built
    }

    fn lock(&self) -> MutexGuard<'_, Cache> {
        self.stores.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A registry whose backends read configuration from `vars` instead of the
    /// process environment.
    pub fn with_vars(vars: HashMap<String, String>) -> Self {
        Self::with_capacity(MAX_CACHED_STORES, Vars::Map(Arc::new(vars)))
    }

    /// A new, empty registry with this registry's configuration whose S3
    /// backends take their credentials from the AWS profile `profile`.
    pub fn with_profile(&self, profile: String) -> Self {
        Self {
            profile: Some(Arc::new(AwsProfile::new(profile, &self.vars))),
            ..Self::with_capacity(self.capacity, self.vars.clone())
        }
    }

    fn with_capacity(capacity: usize, vars: Vars) -> Self {
        Self {
            stores: Mutex::default(),
            capacity,
            http: OnceLock::new(),
            vars,
            profile: None,
        }
    }

    /// Number of distinct backends built so far.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// The shared HTTP client for bucket operations, built on first use. A
    /// server that does not answer cannot hang a call: connecting is given 5
    /// seconds and a whole request 30.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Generic`] if the client cannot be built (for example
    /// TLS or resolver initialisation fails).
    pub fn http(&self) -> Result<&reqwest::Client> {
        if let Some(client) = self.http.get() {
            return Ok(client);
        }
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));
        // Injected variables never read the process environment, and neither
        // do their proxy settings. Production keeps honouring the proxies.
        if matches!(self.vars, Vars::Map(_)) {
            builder = builder.no_proxy();
        }
        let client = builder
            .build()
            .map_err(|e| Error::Generic(format!("HTTP client: {e}")))?;
        // A racing builder simply loses; `OnceLock::get_or_try_init` is unstable.
        Ok(self.http.get_or_init(|| client))
    }
}

/// Parses a URL string into a [`Url`], treating absolute or relative paths
/// without a scheme as `file://` URLs. A local path is treated exactly like
/// the `file://` URL of its absolute path, so `..` is resolved lexically.
pub fn parse_url(url: &str) -> Result<Url> {
    parse_url_with(url, std::env::home_dir, std::env::current_dir)
}

/// [`parse_url`] with the home and working directories supplied by the caller;
/// each is asked for only when a local path needs it.
fn parse_url_with(
    url: &str,
    home: impl FnOnce() -> Option<PathBuf>,
    cwd: impl FnOnce() -> std::io::Result<PathBuf>,
) -> Result<Url> {
    match Url::parse(url) {
        Ok(parsed) => {
            // On Windows, an absolute path like `C:\path` might be parsed
            // as having scheme `c`. If the scheme is a single character,
            // we treat it as a local filesystem path instead.
            if parsed.scheme().len() == 1 {
                path_to_file_url(url, home, cwd)
            } else {
                Ok(parsed)
            }
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => path_to_file_url(url, home, cwd),
        Err(e) => Err(e.into()),
    }
}

/// `~` or `~/rest` under `home`; `None` for any other path.
fn expand_tilde(path: &str, home: impl FnOnce() -> Option<PathBuf>) -> Result<Option<PathBuf>> {
    let Some(rest) = (if path == "~" {
        Some("")
    } else {
        path.strip_prefix("~/")
    }) else {
        return Ok(None);
    };
    let home = home().ok_or_else(|| Error::Generic("Failed to determine home directory".into()))?;
    Ok(Some(if rest.is_empty() {
        home
    } else {
        home.join(rest)
    }))
}

/// Converts a local filesystem path (absolute or relative) into a `file://`
/// [`Url`].
fn path_to_file_url(
    path_str: &str,
    home: impl FnOnce() -> Option<PathBuf>,
    cwd: impl FnOnce() -> std::io::Result<PathBuf>,
) -> Result<Url> {
    let abs_path = if let Some(path) = expand_tilde(path_str, home)? {
        path
    } else {
        let path = std::path::Path::new(path_str);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd()?.join(path)
        }
    };
    // A trailing separator names a directory and must survive, or `./out/`
    // would address a file called `out`.
    let converted = if path_str.ends_with(std::path::is_separator) {
        Url::from_directory_path(&abs_path)
    } else {
        Url::from_file_path(&abs_path)
    };
    let url = converted.map_err(|()| {
        Error::Generic(format!(
            "Failed to convert path to URL: {}",
            abs_path.display()
        ))
    })?;
    // `from_file_path` keeps `..` as a literal segment, which `object_path`
    // would reject. Re-parsing resolves it lexically (symlinks are not
    // followed), exactly as for a `file://` URL typed by the user.
    Ok(Url::parse(url.as_str())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(url: &str) -> Result<Vec<String>> {
        let url = Url::parse(url).expect("url");
        let (_, path) = StoreRegistry::default().resolve(&url)?;
        Ok(path.parts().map(|p| p.as_ref().to_owned()).collect())
    }

    #[cfg(unix)]
    #[test]
    fn local_dot_segments_resolve_lexically() {
        let url = parse_url("/a/b/../c").expect("parse");
        assert_eq!(url.as_str(), "file:///a/c");
        assert_eq!(parts(url.as_str()).expect("parts"), ["a", "c"]);
        assert_eq!(parse_url("/a/./b").expect("parse").as_str(), "file:///a/b");
        // `from_file_path` escapes the percent sign, so this is a file name.
        let url = parse_url("/a/%2e%2e").expect("parse");
        assert_eq!(parts(url.as_str()).expect("parts"), ["a", "%2e%2e"]);
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

    /// Variables naming a config file in `dir` that holds profile `p` with
    /// static keys and a region.
    fn profile_vars(dir: &tempfile::TempDir) -> (std::path::PathBuf, HashMap<String, String>) {
        let path = dir.path().join("config");
        std::fs::write(
            &path,
            "[profile p]\nregion = eu-west-3\naws_access_key_id = AKIDP\n\
             aws_secret_access_key = s\n",
        )
        .expect("write");
        let vars = HashMap::from([("AWS_CONFIG_FILE".to_owned(), path.display().to_string())]);
        (path, vars)
    }

    fn s3(registry: &StoreRegistry, bucket: &str) -> Arc<dyn Backend> {
        let url = Url::parse(&format!("s3://{bucket}/x")).expect("url");
        registry.resolve(&url).expect("resolve").0
    }

    #[test]
    fn one_registry_shares_profile_credentials_across_buckets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, vars) = profile_vars(&dir);
        let registry = StoreRegistry::with_vars(vars).with_profile("p".into());
        let (a, b) = (s3(&registry, "a"), s3(&registry, "b"));
        let (id, region) = a.s3_signing().expect("s3 backend");
        assert_eq!(region, "eu-west-3");
        assert_eq!(Some((id, region)), b.s3_signing());
        let shared = registry.profile.as_ref().expect("profile").credentials();
        assert_eq!(id, Arc::as_ptr(&shared).cast());
    }

    #[test]
    fn profile_files_are_read_once_per_registry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (path, vars) = profile_vars(&dir);
        let registry =
            StoreRegistry::with_capacity(1, Vars::Map(Arc::new(vars))).with_profile("p".into());
        let first = s3(&registry, "a").s3_signing().expect("s3 backend");
        assert_eq!(registry.len(), 1);
        std::fs::remove_file(&path).expect("remove");
        // Each of these evicts the previous backend; the deleted file is not
        // read again because the profile state belongs to the registry.
        let (b, c) = (s3(&registry, "b"), s3(&registry, "c"));
        assert_eq!(registry.len(), 1);
        assert!(!Arc::ptr_eq(&b, &c));
        assert_eq!(first.1, "eu-west-3");
        assert_eq!(b.s3_signing(), Some(first.clone()));
        assert_eq!(c.s3_signing(), Some(first));
    }

    #[test]
    fn each_profile_registry_has_its_own_stores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, vars) = profile_vars(&dir);
        let plain = StoreRegistry::with_vars(vars);
        let (a, b) = (
            plain.with_profile("p".into()),
            plain.with_profile("p".into()),
        );
        let (a1, b1) = (s3(&a, "bkt"), s3(&b, "bkt"));
        assert!(!Arc::ptr_eq(&a1, &b1));
        assert!(Arc::ptr_eq(&a1, &s3(&a, "bkt")));
        assert!(Arc::ptr_eq(&b1, &s3(&b, "bkt")));
        assert_eq!((a.len(), b.len(), plain.len()), (1, 1, 0));
    }

    #[test]
    fn a_pair_of_one_origin_shares_a_backend_in_a_full_cache() {
        let registry = StoreRegistry::with_capacity(1, Vars::Map(Arc::new(HashMap::new())));
        let url = |u: &str| Url::parse(u).expect("url");
        registry.resolve(&url("http://full/x")).expect("fill");

        // A pair shares one backend, and keeps its own paths.
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
        assert!(std::ptr::eq(
            registry.http().expect("client"),
            registry.http().expect("client")
        ));
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
        let registry = StoreRegistry::with_vars(HashMap::new());
        let err = registry
            .http()
            .expect("client")
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect_err("timed out");
        assert!(err.is_timeout(), "{err}");
        silent.abort();
    }

    #[test]
    fn lru_evicts_the_least_recently_used_backend() {
        let registry = StoreRegistry::with_capacity(2, Vars::Map(Arc::new(HashMap::new())));
        let resolve = |url: &str| {
            registry
                .resolve(&Url::parse(url).expect("url"))
                .expect("resolve")
                .0
        };
        let (a, b) = (resolve("http://a/x"), resolve("http://b/x"));
        // Using `a` again makes `b` the oldest.
        assert!(Arc::ptr_eq(&a, &resolve("http://a/y")));
        let c = resolve("http://c/x");
        assert_eq!(registry.len(), 2);

        assert!(Arc::ptr_eq(&a, &resolve("http://a/z")));
        assert!(Arc::ptr_eq(&c, &resolve("http://c/y")));
        assert_eq!(registry.len(), 2);
        // `b` was evicted: it is rebuilt, and the old Arc stays usable.
        let b2 = resolve("http://b/y");
        assert!(!Arc::ptr_eq(&b, &b2));
        assert_eq!(b.scheme(), "http");
        assert_eq!(registry.len(), 2);
    }

    fn mapped() -> StoreRegistry {
        StoreRegistry::with_vars(HashMap::from([(
            "AWS_REGION".to_owned(),
            "us-east-1".to_owned(),
        )]))
    }

    #[test]
    fn s3_bucket_only_urls_have_an_empty_path() {
        // This used to panic on `&url.path()[1..]`.
        let (backend, path) = mapped()
            .resolve(&Url::parse("s3://bucket").expect("url"))
            .expect("resolve");
        assert_eq!(backend.scheme(), "s3");
        assert_eq!(path.as_ref(), "");
    }

    #[test]
    fn s3_keys_drop_the_leading_slash() {
        let (backend, path) = mapped()
            .resolve(&Url::parse("s3://bucket/path").expect("url"))
            .expect("resolve");
        assert_eq!(backend.scheme(), "s3");
        assert_eq!(path.as_ref(), "path");
    }

    #[test]
    fn s3_backends_are_cached_by_bucket() {
        let registry = mapped();
        let resolve = |url: &str| {
            registry
                .resolve(&Url::parse(url).expect("url"))
                .expect("resolve")
                .0
        };
        let (a, b) = (resolve("s3://bucket"), resolve("s3://bucket/path"));
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &resolve("s3://other/path")));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn file_urls_ignore_the_host() {
        // The authority ("tmp") is the host and "/path" the path; the file
        // backend ignores the host.
        let (backend, path) = mapped()
            .resolve(&Url::parse("file://tmp/path").expect("url"))
            .expect("resolve");
        assert_eq!(backend.scheme(), "file");
        assert_eq!(path.as_ref(), "path");
    }

    fn no_cwd() -> std::io::Result<PathBuf> {
        panic!("the working directory was read")
    }

    #[cfg(unix)]
    #[test]
    fn tilde_paths_use_the_given_home() {
        let home = || Some(PathBuf::from("/home/u"));
        let parse = |p| parse_url_with(p, home, no_cwd).expect("parse");
        assert_eq!(parse("~").as_str(), "file:///home/u");
        assert_eq!(parse("~/t.txt").as_str(), "file:///home/u/t.txt");
        assert!(matches!(
            parse_url_with("~", || None, no_cwd),
            Err(Error::Generic(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn relative_paths_use_the_given_cwd() {
        let cwd = || Ok(PathBuf::from("/w"));
        let no_home = || -> Option<PathBuf> { panic!("the home directory was read") };
        let parse = |p| parse_url_with(p, no_home, cwd).expect("parse");
        assert_eq!(parse("rel/x").as_str(), "file:///w/rel/x");
        assert_eq!(parse("../x").as_str(), "file:///x");
        assert_eq!(parse("out/").as_str(), "file:///w/out/");
    }

    #[cfg(unix)]
    #[test]
    fn a_single_letter_scheme_is_a_local_path() {
        // `c:/x` is not the URL scheme `c`: below the working directory on Unix.
        let url = parse_url_with("c:/x", || None, || Ok(PathBuf::from("/w"))).expect("parse");
        assert_eq!(url.as_str(), "file:///w/c:/x");
    }

    #[cfg(windows)]
    #[test]
    fn a_single_letter_scheme_is_a_local_path() {
        let url = parse_url_with("c:/x", || None, || Ok(PathBuf::from("c:/w"))).expect("parse");
        assert_eq!(url.as_str(), "file:///C:/x");
    }

    #[test]
    fn urls_never_read_home_or_cwd() {
        let no_home = || -> Option<PathBuf> { panic!("the home directory was read") };
        let url = parse_url_with("s3://b/k", no_home, no_cwd).expect("parse");
        assert_eq!(url.as_str(), "s3://b/k");
    }

    #[test]
    fn a_leading_tilde_is_the_home_directory() {
        let home = || Some(PathBuf::from("/home/u"));
        let expand = |p| expand_tilde(p, home).expect("expand");
        assert_eq!(expand("~"), Some(PathBuf::from("/home/u")));
        assert_eq!(expand("~/x/y"), Some(PathBuf::from("/home/u/x/y")));
        assert_eq!(expand("~u/x"), None);
        assert_eq!(expand("a/~/x"), None);
        assert!(expand_tilde("~/x", || None).is_err());
        assert_eq!(expand_tilde("/x", || None).expect("expand"), None);
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
