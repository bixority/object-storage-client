//! Python bindings for client-side encryption: the encrypted client,
//! `Keyring` and the exception hierarchy.

use super::client::{
    ObjectStorageClient, bytes_from_py, client_pyclass, encryption_to_dict, upload_options,
};
use super::stream::py_async_iter_to_stream;
use super::{call, client_to_py_err, spawn};
use crate::ReencryptOptions;
use crate::client::EncryptedObjectStorageClient as InternalClient;
use crate::codec::CallerContext;
use crate::encryption::EncryptionConfig;
use crate::encryption::keys::{KeyError, Keyring as RustKeyring};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use std::path::PathBuf;
use std::sync::Arc;

pyo3::create_exception!(
    object_storage_client,
    EncryptionError,
    PyRuntimeError,
    "Base class of all client-side encryption errors."
);
pyo3::create_exception!(
    object_storage_client,
    DecryptionError,
    EncryptionError,
    "Tampering, authentication failure, truncation or a binding mismatch."
);
pyo3::create_exception!(
    object_storage_client,
    UnknownKeyError,
    EncryptionError,
    "No key for the object is available; names key IDs only. Derived from unauthenticated header data, so it may also indicate tampering."
);
pyo3::create_exception!(
    object_storage_client,
    NotEncryptedError,
    EncryptionError,
    "The encrypted client read an object that is not encrypted."
);

/// A reloadable set of key-encryption keys read from a file. Key bytes never
/// leave Rust.
#[pyclass(frozen, module = "object_storage_client")]
pub struct Keyring {
    inner: Arc<RustKeyring>,
    path: PathBuf,
}

impl Keyring {
    pub(super) fn config(
        &self,
        bind_object_key: bool,
        frame_length: u32,
    ) -> PyResult<EncryptionConfig> {
        EncryptionConfig::new(self.inner.clone())
            .bind_object_key(bind_object_key)
            .frame_length(frame_length)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

/// A timezone-aware ``datetime`` as UTC, through its RFC 3339 ``isoformat()``.
/// A naive one has no offset and is rejected.
fn aware_datetime(dt: &Bound<'_, PyAny>) -> PyResult<chrono::DateTime<chrono::Utc>> {
    let text: String = dt.call_method0("isoformat")?.extract()?;
    chrono::DateTime::parse_from_rfc3339(&text)
        .map(|d| d.with_timezone(&chrono::Utc))
        .map_err(|_| PyValueError::new_err("`before` must be a timezone-aware datetime"))
}

fn key_err(e: KeyError) -> PyErr {
    client_to_py_err(e.into())
}

#[pymethods]
impl Keyring {
    /// Loads the keyring file at ``path`` (a ``str`` or path-like). Later
    /// changes of the file are picked up on use. Raises ``EncryptionError`` for
    /// an unreadable, insecure (accessible to others) or invalid file.
    #[staticmethod]
    fn from_file(path: PathBuf) -> PyResult<Self> {
        let inner = RustKeyring::from_file(&path).map_err(key_err)?;
        Ok(Self {
            inner: Arc::new(inner),
            path,
        })
    }

    /// The keys and health of this keyring: ``{"active": str, "keys":
    /// [{"id": str, "created": "YYYY-MM-DD"}], "active_age_days": int,
    /// "last_reload_error": str | None}``.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let st = self.inner.status();
        let d = PyDict::new(py);
        let keys = PyList::empty(py);
        for k in st.keys {
            let item = PyDict::new(py);
            item.set_item("id", k.id)?;
            item.set_item("created", k.created.to_string())?;
            keys.append(item)?;
        }
        d.set_item("active", st.active)?;
        d.set_item("keys", keys)?;
        d.set_item("active_age_days", st.active_age_days)?;
        d.set_item("last_reload_error", st.last_reload_error)?;
        Ok(d)
    }

    /// Number of key wrap operations (one per object written) this process has
    /// done with the active key through this keyring. Not persisted: it
    /// restarts at 0 with every process and keyring instance.
    #[getter]
    fn wraps(&self) -> u64 {
        self.inner.wraps()
    }

    fn __repr__(&self) -> String {
        format!(
            "Keyring(path={:?}, active={:?})",
            self.path.to_string_lossy(),
            self.inner.status().active
        )
    }
}

/// Client that encrypts every object it writes and decrypts every object it
/// reads. Obtain it with ``ObjectStorageClient.with_encryption`` or construct
/// it directly.
#[pyclass(module = "object_storage_client")]
pub struct EncryptedObjectStorageClient {
    pub(super) inner: Arc<InternalClient>,
}

client_pyclass!(EncryptedObjectStorageClient, {
    #[new]
    #[pyo3(signature = (keyring, bind_object_key=true, frame_length=65536))]
    fn new(keyring: &Keyring, bind_object_key: bool, frame_length: u32) -> PyResult<Self> {
        let cfg = keyring.config(bind_object_key, frame_length)?;
        Ok(Self {
            inner: Arc::new(InternalClient::new(cfg)),
        })
    }

    /// A plain view sharing this client's stores and connections.
    fn without_encryption(&self) -> ObjectStorageClient {
        ObjectStorageClient {
            inner: Arc::new(self.inner.without_encryption()),
        }
    }

    /// Encrypt and upload ``data``: any bytes-like object or a list of ints
    /// (see the plain client's ``put_object``). ``context`` pairs are bound into the
    /// ciphertext; keys starting with ``osc:`` or ``aws-crypto-`` (in any ASCII case) are
    /// rejected.
    #[pyo3(signature = (url, data, context=None))]
    fn put_object<'py>(
        &self,
        py: Python<'py>,
        url: String,
        data: &Bound<'py, PyAny>,
        context: Option<CallerContext>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let bytes = bytes_from_py(data)?;
        let ctx = context.unwrap_or_default();

        call(py, async move {
            inner.put_with_context(&url, bytes, &ctx).await
        })
    }

    /// Encrypt and upload the bytes-like chunks of an async iterable. Same
    /// bounded read-ahead, back-pressure and abort semantics as the plain client.
    /// ``size_hint`` is the expected plaintext size in bytes, if known (see the
    /// plain client).
    #[pyo3(signature = (url, chunks, context=None, *, size_hint=None))]
    fn put_object_stream<'py>(
        &self,
        py: Python<'py>,
        url: String,
        chunks: &Bound<'py, PyAny>,
        context: Option<CallerContext>,
        size_hint: Option<i128>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let opts = upload_options(size_hint)?;
        let stream = py_async_iter_to_stream(chunks)?;
        let ctx = context.unwrap_or_default();

        call(py, async move {
            inner.put_stream_inner(&url, stream, ctx, &opts).await
        })
    }

    /// Rewrite the object so its data key is wrapped by the active key, only
    /// if it is wrapped by ``from_key``. Returns ``"rewritten"``,
    /// ``"would-rewrite"`` (``dry_run``), ``"skipped"``, ``"not-encrypted"``,
    /// ``"after-cutoff"`` (modified at or after the timezone-aware ``before``)
    /// or ``"changed"`` (modified meanwhile; the newer content is kept).
    #[pyo3(signature = (url, from_key, *, before=None, dry_run=false))]
    fn reencrypt<'py>(
        &self,
        py: Python<'py>,
        url: String,
        from_key: String,
        before: Option<&Bound<'py, PyAny>>,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let before = before.map(aware_datetime).transpose()?;
        let opts = ReencryptOptions {
            from: from_key,
            before,
            dry_run,
        };

        call(py, async move {
            inner
                .reencrypt(&url, &opts)
                .await
                .map(|r| r.as_str().to_owned())
        })
    }

    /// Read the envelope details from the object's header. Needs no keys and
    /// is unauthenticated.
    fn inspect_encryption<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        spawn(py, async move {
            let info = inner
                .inspect_encryption(&url)
                .await
                .map_err(client_to_py_err)?;
            Python::attach(|py| Ok(encryption_to_dict(py, info)?.into_any().unbind()))
        })
    }
});
