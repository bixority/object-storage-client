//! Shared backend cache and HTTP client used by every [`Client`](crate::Client).

use crate::backend::{self, AwsProfile, Backend, Shared, Vars};
use crate::client::{ERROR_SOURCE_CAP, Error, Result};
use object_store::path::Path as ObjectPath;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, PoisonError};
use url::{Position, Url};

/// Everything that changes the store built for a URL: scheme, credentials
/// (user name and password), host and port. Path, query and fragment do not.
pub fn origin(url: &Url) -> &str {
    &url[..Position::AfterPort]
}

/// Whether `scheme` names a bucket or container store.
#[cfg(feature = "gui")]
pub fn is_bucket_scheme(scheme: &str) -> bool {
    matches!(
        scheme,
        "s3" | "gs" | "gcs" | "az" | "wasb" | "wasbs" | "abfs" | "abfss"
    )
}

/// The scheme of `url` if it is a provider root: a bucket scheme with no
/// bucket and no path (`s3://`, `s3:///`). Only a bucket listing accepts it.
#[cfg(feature = "gui")]
pub fn provider_root(url: &Url) -> Option<&str> {
    (is_bucket_scheme(url.scheme())
        && url.host_str().is_none_or(str::is_empty)
        && matches!(url.path(), "" | "/"))
    .then(|| url.scheme())
}

/// The object a URL names: its percent-decoded path. A bucket-only URL (e.g.
/// `s3://bucket`) has an empty path, which yields the root path.
pub fn object_path(url: &Url) -> Result<ObjectPath> {
    ObjectPath::from_url_path(url.path()).map_err(|e| {
        // The decoded path may hold control characters.
        let e = crate::text::escape_display(&e, ERROR_SOURCE_CAP);
        Error::InvalidInput(format!("Invalid object path in URL: {e}"))
    })
}

/// What stays unescaped when a relative file path is appended to a URL.
///
/// Applied to one path component at a time, so `/`, `\` and `%` are always
/// escaped.
pub const URL_PATH_SEGMENTS: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.');

/// `dir` (ending in `/`) followed by each segment percent-encoded with
/// [`URL_PATH_SEGMENTS`] and joined by `/`.
#[cfg_attr(not(feature = "cli"), allow(dead_code))]
pub fn child_url<'a>(dir: &str, segments: impl IntoIterator<Item = &'a str>) -> String {
    debug_assert!(dir.ends_with('/'), "directory URL must end in `/`: {dir}");
    let encoded: Vec<String> = segments
        .into_iter()
        .map(|s| utf8_percent_encode(s, URL_PATH_SEGMENTS).to_string())
        .collect();
    format!("{dir}{}", encoded.join("/"))
}

/// A resolved backend and the object path within it.
type Located = (Arc<dyn Backend>, ObjectPath);

/// How many backends a registry keeps. An application that addresses many
/// origins would otherwise grow the cache without bound; at the cap the least recently
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
/// `object_store` is built once and reused across calls. The cache holds at
/// most [`MAX_CACHED_STORES`] backends and evicts the least recently used one.
pub struct StoreRegistry {
    stores: Mutex<Cache>,
    capacity: usize,
    vars: Vars,
    /// The named AWS profile for S3 credentials, shared by every S3 backend.
    /// It is fixed for the registry's lifetime: [`Self::with_profile`] starts
    /// a new, empty cache, which is what isolates profiles, so the profile is
    /// not part of the cache key.
    profile: Option<Arc<AwsProfile>>,
    /// The HTTP clients and credential providers the backends share. Outside
    /// the cache, so evicting a backend does not drop them; a new registry
    /// starts with its own, like the profile.
    shared: Shared,
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
    /// free of I/O (GCS reads its service-account file, and a first client of
    /// each kind loads the system CA store), so it runs outside the lock. The
    /// registry's backends share their HTTP clients and credential providers.
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
        let built = backend::build(url, &self.vars, self.profile.as_deref(), &self.shared)?;
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

    /// The names of all buckets or containers that this registry's credentials
    /// for `scheme` can list, sorted. Nothing is cached.
    ///
    /// # Errors
    ///
    /// As [`backend::list_buckets`].
    #[cfg(feature = "gui")]
    pub async fn list_buckets(&self, scheme: &str) -> Result<Vec<String>> {
        backend::list_buckets(scheme, &self.vars, self.profile.as_deref(), &self.shared).await
    }

    /// For an S3 `url`: drops the cached backend when this registry has learned
    /// a region for its bucket that differs from the backend's, so the next
    /// [`Self::resolve`] builds one that signs with it. Returns whether it did.
    #[cfg(feature = "gui")]
    pub(crate) fn evict_for_learned_region(&self, url: &Url) -> bool {
        if url.scheme() != "s3" {
            return false;
        }
        let Some(bucket) = url.host_str() else {
            return false;
        };
        let Some(learned) = self.shared.learned_region(bucket) else {
            return false;
        };
        let mut cache = self.lock();
        let key = origin(url);
        let stale = cache.entries.get(key).is_some_and(|(_, backend)| {
            backend
                .as_any()
                .and_then(|any| any.downcast_ref::<backend::S3Backend>())
                .is_some_and(|s3| s3.region() != learned)
        });
        if stale {
            cache.entries.remove(key);
        }
        stale
    }

    /// Caches `backend` as the one for the origin of `url`.
    #[cfg(all(test, any(feature = "cli", feature = "encryption", feature = "gui")))]
    pub(crate) fn preload(&self, url: &Url, backend: Arc<dyn Backend>) {
        self.keep(origin(url), backend);
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
        Self::with_capacity(MAX_CACHED_STORES, Vars::map(vars))
    }

    /// Like [`Self::with_vars`], but the backends honour the system proxy
    /// setting.
    #[cfg(feature = "gui")]
    pub(crate) fn isolated(vars: HashMap<String, String>) -> Self {
        Self::with_capacity(
            MAX_CACHED_STORES,
            Vars::Map {
                vars: Arc::new(vars),
                system_proxy: true,
            },
        )
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
            vars,
            profile: None,
            shared: Shared::default(),
        }
    }

    /// Number of distinct backends built so far.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }
}

/// Parses a URL string into a [`Url`], treating absolute or relative paths
/// without a scheme as `file://` URLs. A local path is treated exactly like
/// the `file://` URL of its absolute path, so `..` is resolved lexically.
pub fn parse_url(url: &str) -> Result<Url> {
    parse_url_with(url, std::env::home_dir, std::env::current_dir)
}

/// The parsed `raw` if [`parse_url`] takes it as a URL: it parses and its
/// scheme has more than one character (`C:\x` is a Windows path). Anything
/// else is a local path.
pub fn url_form(raw: &str) -> Option<Url> {
    Url::parse(raw).ok().filter(|u| u.scheme().len() > 1)
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

    #[test]
    fn child_url_encodes_each_segment() {
        assert_eq!(
            child_url("s3://b/p/", ["a b", "c%d", "é#?"]),
            "s3://b/p/a%20b/c%25d/%C3%A9%23%3F"
        );
        assert_eq!(child_url("file:///t/", ["x"]), "file:///t/x");
        assert_eq!(child_url("s3://b/", ["d", "e.txt"]), "s3://b/d/e.txt");
    }

    #[cfg(feature = "gui")]
    #[test]
    fn an_isolated_registry_honours_the_system_proxy() {
        let proxy = |r: &StoreRegistry| {
            matches!(
                r.vars,
                Vars::Map {
                    system_proxy: true,
                    ..
                }
            )
        };
        let isolated = StoreRegistry::isolated(HashMap::new());
        assert!(proxy(&isolated));
        assert!(proxy(&isolated.with_profile("p".into())));
        assert!(!proxy(&StoreRegistry::with_vars(HashMap::new())));
    }

    #[cfg(feature = "gui")]
    #[test]
    fn provider_roots_are_bucket_schemes_without_a_bucket() {
        let root = |u: &str| provider_root(&Url::parse(u).expect("url")).map(str::to_owned);
        for (url, scheme) in [
            ("s3://", "s3"),
            ("s3:///", "s3"),
            ("gs:///", "gs"),
            ("gcs://", "gcs"),
            ("az:///", "az"),
            ("abfss:///", "abfss"),
        ] {
            assert_eq!(root(url).as_deref(), Some(scheme), "{url}");
        }
        for url in [
            "s3://b",
            "s3://b/",
            "s3:///x",
            "file:///",
            "http://h/",
            "ftp://h/",
        ] {
            assert_eq!(root(url), None, "{url}");
        }
    }

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
    fn a_control_character_in_a_path_is_escaped_in_the_error() {
        let url = Url::parse("s3://b/%1b]52;c;x%07").expect("url");
        let Err(Error::InvalidInput(message)) = object_path(&url) else {
            panic!("a control character in the path is invalid");
        };
        assert!(message.contains("\\u{1b}"), "{message}");
        assert!(
            !message.chars().any(crate::text::is_unsafe_char),
            "{message}"
        );
    }

    /// A stub that answers bucket calls with 302 to a second stub: they fail
    /// and the second stub receives nothing.
    #[tokio::test]
    async fn bucket_calls_do_not_follow_redirects() {
        use crate::backend::stub::{Reply, serve};
        let target = serve(vec![Reply::new(200, "")]).await;
        let location: &'static str = Box::leak(format!("{}/bkt", target.base).into_boxed_str());
        let first = serve(vec![
            Reply::new(302, "").header("Location", location),
            Reply::new(302, "").header("Location", location),
        ])
        .await;
        let vars = HashMap::from(
            [
                ("S3_ENDPOINT", first.base.as_str()),
                ("S3_ALLOW_HTTP", "true"),
                ("S3_REGION", "us-east-1"),
                ("S3_ACCESS_KEY_ID", "k"),
                ("S3_SECRET_ACCESS_KEY", "s"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );
        let registry = StoreRegistry::with_vars(vars);
        let url = Url::parse("s3://bkt").expect("url");
        let (backend, _) = registry.resolve(&url).expect("resolve");
        let err = backend.bucket_exists(&url).await.expect_err("302");
        assert!(err.to_string().contains("302"), "{err}");
        let err = backend.create_bucket(&url).await.expect_err("302");
        assert!(err.to_string().contains("302"), "{err}");
        assert_eq!(first.requests().len(), 2);
        assert!(target.requests().is_empty());
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
    fn credentials_survive_eviction_and_belong_to_one_registry() {
        let vars = || {
            Vars::map(HashMap::from([
                ("AWS_REGION".to_owned(), "eu-west-1".to_owned()),
                ("AWS_ACCESS_KEY_ID".to_owned(), "k".to_owned()),
                ("AWS_SECRET_ACCESS_KEY".to_owned(), "s".to_owned()),
            ]))
        };
        let registry = StoreRegistry::with_capacity(1, vars());
        let first = s3(&registry, "a").s3_signing().expect("s3 backend");
        // Each of these evicts the previous backend.
        let (b, c) = (s3(&registry, "b"), s3(&registry, "c"));
        assert_eq!(registry.len(), 1);
        assert_eq!(b.s3_signing(), Some(first.clone()));
        assert_eq!(c.s3_signing(), Some(first.clone()));
        assert_eq!(s3(&registry, "a").s3_signing(), Some(first.clone()));
        let other = StoreRegistry::with_capacity(1, vars());
        assert_ne!(s3(&other, "a").s3_signing().expect("s3 backend").0, first.0);
    }

    #[cfg(feature = "gui")]
    #[test]
    fn a_learned_bucket_region_replaces_the_backend_and_the_resolved_chain() {
        let registry = StoreRegistry::with_vars(HashMap::from([
            ("AWS_REGION".to_owned(), "eu-west-1".to_owned()),
            ("AWS_ACCESS_KEY_ID".to_owned(), "k".to_owned()),
            ("AWS_SECRET_ACCESS_KEY".to_owned(), "s".to_owned()),
        ]));
        let url = Url::parse("s3://a/x").expect("url");
        let region = |b: &Arc<dyn Backend>| b.s3_signing().expect("s3 backend").1;
        let first = s3(&registry, "a");
        assert_eq!(region(&first), "eu-west-1");
        // Nothing learned: nothing is evicted.
        assert!(!registry.evict_for_learned_region(&url));
        registry.shared.learn_region("a", "eu-central-1");
        assert!(registry.evict_for_learned_region(&url));
        assert_eq!(registry.len(), 0);
        let second = s3(&registry, "a");
        assert_eq!(region(&second), "eu-central-1");
        // The rebuilt backend already agrees: no further eviction.
        assert!(!registry.evict_for_learned_region(&url));
        assert!(Arc::ptr_eq(&second, &s3(&registry, "a")));
        // Other buckets and other schemes are untouched.
        assert_eq!(region(&s3(&registry, "b")), "eu-west-1");
        let gs = Url::parse("gs://a/x").expect("url");
        assert!(!registry.evict_for_learned_region(&gs));
    }

    #[test]
    fn profile_files_are_read_once_per_registry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (path, vars) = profile_vars(&dir);
        let registry = StoreRegistry::with_capacity(1, Vars::map(vars)).with_profile("p".into());
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
        let registry = StoreRegistry::with_capacity(1, Vars::map(HashMap::new()));
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
    fn lru_evicts_the_least_recently_used_backend() {
        let registry = StoreRegistry::with_capacity(2, Vars::map(HashMap::new()));
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
