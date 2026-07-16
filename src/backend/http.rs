//! Plain HTTP / `WebDAV` backend.
//!
//! Supports only generic object operations; bucket management and pre-signing
//! fall back to the [`Backend`] trait's "unsupported" defaults.

use super::Backend;
use crate::client::Result;
use object_store::ObjectStore;
use object_store::http::HttpBuilder;
use std::sync::Arc;
use url::Url;

pub struct HttpBackend {
    store: Arc<dyn ObjectStore>,
    scheme: String,
}

impl HttpBackend {
    /// Build an HTTP store pointed at `url`.
    ///
    /// # Errors
    ///
    /// Returns an error if the store fails to build.
    pub fn new(scheme: &str, url: &Url) -> Result<Self> {
        let store = HttpBuilder::new().with_url(url.as_str()).build()?;
        Ok(Self {
            store: Arc::new(store),
            scheme: scheme.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl Backend for HttpBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }
}
