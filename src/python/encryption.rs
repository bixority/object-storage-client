//! Python bindings for client-side encryption: the encrypted client,
//! `Keyring` and the exception hierarchy.

use super::client::{
    ObjectStorageClient, bytes_from_py, client_pyclass, stream_result, upload_options,
};
use super::stream::py_async_iter_to_stream;
use super::{call, client_to_py_err, int_in};
use crate::ReencryptOptions;
use crate::client::EncryptedObjectStorageClient as InternalClient;
use crate::codec::CallerContext;
use crate::encryption::EncryptionConfig;
use crate::encryption::keys::{KeyError, KeyProvider as _, Keyring as RustKeyring};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyDict, PyList, PyTzInfo};
use pyo3_async_runtimes::tokio::get_current_locals;

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
        frame_length: i128,
    ) -> PyResult<EncryptionConfig> {
        EncryptionConfig::new(self.inner.clone())
            .bind_object_key(bind_object_key)
            .frame_length(int_in("frame_length", frame_length)?)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

/// A timezone-aware ``datetime`` as UTC. A non-datetime is a ``TypeError``; a
/// naive one has no offset and is a ``ValueError``.
fn aware_datetime(dt: &Bound<'_, PyAny>) -> PyResult<chrono::DateTime<chrono::Utc>> {
    let dt = dt.cast::<PyDateTime>()?;
    // Before `astimezone`, which reads a naive value as local time.
    if dt.call_method0("utcoffset")?.is_none() {
        return Err(PyValueError::new_err(
            "`before` must be a timezone-aware datetime",
        ));
    }
    let utc = dt.call_method1("astimezone", (PyTzInfo::utc(dt.py())?,))?;
    let text: String = utc.call_method0("isoformat")?.extract()?;
    chrono::DateTime::parse_from_rfc3339(&text)
        .map(|d| d.with_timezone(&chrono::Utc))
        .map_err(|e| PyValueError::new_err(format!("`before` is out of range: {e}")))
}

fn key_err(e: KeyError) -> PyErr {
    client_to_py_err(e.into())
}

#[pymethods]
impl Keyring {
    /// Loads the keyring file at ``path`` (a ``str`` or path-like). Later
    /// changes of the file are picked up on use. Raises ``EncryptionError`` for
    /// an unreadable or invalid file and, on Unix, for one its group can
    /// write, others can access, or another user owns; other platforms do not
    /// check the file's permissions.
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
        // Passive: repr never checks the file.
        format!(
            "Keyring(path={:?}, active={:?})",
            self.path.to_string_lossy(),
            self.inner.active_key_name()
        )
    }
}

/// Client that encrypts every object it writes and decrypts every object it
/// reads. Obtain it with ``ObjectStorageClient.with_encryption``, which shares
/// that client's stores and AWS profile, or construct it directly with
/// ``EncryptedObjectStorageClient(keyring, profile=...)``.
#[pyclass(frozen, module = "object_storage_client")]
pub struct EncryptedObjectStorageClient {
    pub(super) inner: Arc<InternalClient>,
}

client_pyclass!(EncryptedObjectStorageClient, {
    /// ``profile`` (keyword-only) names an AWS profile for ``s3://``
    /// credentials, exactly as for ``ObjectStorageClient`` (an empty or blank
    /// name raises ``ValueError``).
    #[new]
    #[pyo3(signature = (keyring, bind_object_key=true, frame_length=65536, *, profile=None))]
    fn new(
        keyring: &Keyring,
        bind_object_key: bool,
        frame_length: i128,
        profile: Option<String>,
    ) -> PyResult<Self> {
        let cfg = keyring.config(bind_object_key, frame_length)?;
        let plain = match profile {
            Some(p) => crate::ObjectStorageClient::new()
                .with_s3_profile(p)
                .map_err(client_to_py_err)?,
            None => crate::ObjectStorageClient::new(),
        };
        Ok(Self {
            inner: Arc::new(plain.with_encryption(cfg)),
        })
    }

    /// A plain view sharing this client's stores and connections.
    fn without_encryption(&self) -> ObjectStorageClient {
        ObjectStorageClient {
            inner: Arc::new(self.inner.without_encryption()),
        }
    }

    /// Return the whole object as ``bytes``, decrypted and authenticated.
    ///
    /// The object is held in memory, and peaks at about twice its size while
    /// the result is copied into a Python ``bytes``. For large objects use
    /// :py:meth:`get_object_stream`, which keeps memory flat. With
    /// ``context``, the object must carry every given pair in its encryption
    /// context (extra stored pairs are allowed); otherwise
    /// ``DecryptionError`` is raised before any key is used. Keys starting
    /// with ``osc:`` or ``aws-crypto-`` raise ``ValueError``.
    #[pyo3(signature = (url, *, context=None))]
    fn get_object<'py>(
        &self,
        py: Python<'py>,
        url: String,
        context: Option<CallerContext>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let required = context.unwrap_or_default();

        call(
            py,
            async move { inner.get_with_context(&url, &required).await },
        )
    }

    /// Return an async iterator over the decrypted object's bytes. The data
    /// is provisional until the iterator ends cleanly: truncation or
    /// tampering raises ``DecryptionError`` at the point it is detected.
    /// ``context`` is required as for :py:meth:`get_object`.
    #[pyo3(signature = (url, *, context=None))]
    fn get_object_stream<'py>(
        &self,
        py: Python<'py>,
        url: String,
        context: Option<CallerContext>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let required = context.unwrap_or_default();

        call(py, async move {
            Ok(stream_result(
                inner.get_stream_with_context(&url, &required).await?,
            ))
        })
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
    /// bounded read-ahead, back-pressure and abort semantics as the plain client,
    /// including the background abort that the interpreter waits for at exit.
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
        let stream = py_async_iter_to_stream(chunks, get_current_locals(py)?)?;
        let ctx = context.unwrap_or_default();

        call(py, async move {
            inner.put_stream_inner(&url, stream, ctx, &opts).await
        })
    }

    /// Rewrite the object so its data key is wrapped by the active key, only
    /// if it is wrapped by ``from_key``. Returns ``"rewritten"``,
    /// ``"would-rewrite"`` (``dry_run``), ``"skipped"``, ``"not-encrypted"``,
    /// ``"after-cutoff"`` (modified at or after the timezone-aware ``before``,
    /// or the store reports no modification time) or ``"changed"`` (modified
    /// meanwhile; the newer content is kept). ``before`` compares the
    /// modification time the store reports, which is not authenticated: it only
    /// screens out forgeries by holders of leaked storage credentials on stores
    /// that set the time themselves at commit. The provider, anyone who can set
    /// file times on ``file://`` or NFS, and S3 multipart uploads started
    /// before the cutoff are exceptions. If a party that may hold the leaked
    /// key can influence modification times, check candidates against an
    /// independent record (application records, provider audit logs or
    /// inventory) before re-encrypting.
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

        call(py, async move { inner.inspect_encryption(&url).await })
    }
});
