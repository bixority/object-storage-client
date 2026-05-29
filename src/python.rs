use crate::client::ObjectStorageClient as InternalClient;
use crate::sign::{SignMethod, SignOptions};
use bytes::Bytes;
use futures::StreamExt;
use object_store::Error as ObjectStoreError;
use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

fn object_store_to_py_err(e: ObjectStoreError) -> PyErr {
    match e {
        ObjectStoreError::NotFound { path, .. } => {
            PyFileNotFoundError::new_err(format!("Object not found: {path}"))
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn client_to_py_err(e: crate::client::Error) -> PyErr {
    match e {
        crate::client::Error::ObjectStore(object_store::Error::NotFound { path, .. }) => {
            PyFileNotFoundError::new_err(format!("Object not found: {path}"))
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

type PinnedByteStream = Arc<
    Mutex<
        std::pin::Pin<
            Box<dyn futures::Stream<Item = object_store::Result<bytes::Bytes>> + Send + 'static>,
        >,
    >,
>;

/// Async byte-stream returned by :py:meth:`ObjectStorageClient.get_object_stream`.
///
/// Implements the async iterator protocol, so you can use it directly in
/// ``async for``:
///
/// ```text
/// stream = await client.get_object_stream("s3://bucket/key")
/// async for chunk in stream:
///     process(chunk)
/// ```
#[pyclass]
pub struct ByteStream {
    inner: PinnedByteStream,
}

#[pymethods]
impl ByteStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut locked = inner.lock().await;

            match locked.next().await {
                Some(Ok(bytes)) => Ok(Some(bytes.to_vec())),
                Some(Err(e)) => Err(object_store_to_py_err(e)),
                None => Ok(None::<Vec<u8>>),
            }
        })
    }

    /// Fetch the next chunk manually.
    ///
    /// Returns empty ``bytes`` on EOF.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut locked = inner.lock().await;

            match locked.next().await {
                Some(Ok(bytes)) => Ok(bytes.to_vec()),
                Some(Err(e)) => Err(object_store_to_py_err(e)),
                None => Ok(Vec::<u8>::new()),
            }
        })
    }
}

#[pyclass]
pub struct ObjectStorageClient {
    inner: Arc<InternalClient>,
}

#[pymethods]
impl ObjectStorageClient {
    #[new]
    #[must_use]
    fn new() -> Self {
        Self {
            inner: Arc::new(InternalClient::new()),
        }
    }

    fn get_object<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let bytes = inner.get(&url).await.map_err(client_to_py_err)?;
            let result = pyo3::Python::attach(|py| PyBytes::new(py, &bytes).into_any().unbind());
            Ok(result)
        })
    }

    fn get_object_stream<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let stream = inner.get_stream(&url).await.map_err(client_to_py_err)?;
            Ok(ByteStream {
                inner: Arc::new(Mutex::new(Box::pin(stream))),
            })
        })
    }

    fn put_object<'py>(
        &self,
        py: Python<'py>,
        url: String,
        data: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let bytes = if let Ok(py_bytes) = data.cast::<PyBytes>() {
            Bytes::copy_from_slice(py_bytes.as_bytes())
        } else if let Ok(vec) = data.extract::<Vec<u8>>() {
            Bytes::from(vec)
        } else {
            return Err(PyTypeError::new_err("Expected bytes or list of integers"));
        };

        future_into_py(py, async move {
            inner.put(&url, bytes).await.map_err(client_to_py_err)?;
            Ok(())
        })
    }

    fn delete_object<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            inner.delete(&url).await.map_err(client_to_py_err)?;
            Ok(())
        })
    }

    fn list_objects<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let results = inner.list(&url).await.map_err(client_to_py_err)?;
            Ok(results)
        })
    }

    /// Return metadata for the object at ``url``.
    ///
    /// On success returns a dict with the keys ``location`` (str),
    /// ``last_modified`` (RFC 3339 str), ``size_bytes`` (int), ``content_type``
    /// (str or ``None``), ``e_tag`` (str or ``None``) and ``version`` (str or
    /// ``None``). A missing object raises ``FileNotFoundError``; use
    /// :py:meth:`object_exists` for a non-raising presence check.
    fn get_object_metadata<'py>(
        &self,
        py: Python<'py>,
        url: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let stored = inner
                .get_object_metadata(&url)
                .await
                .map_err(client_to_py_err)?;

            pyo3::Python::attach(|py| {
                let dict = PyDict::new(py);
                dict.set_item("location", stored.location)?;
                dict.set_item("last_modified", stored.last_modified.to_rfc3339())?;
                dict.set_item("size_bytes", stored.size_bytes)?;
                dict.set_item("content_type", stored.content_type)?;
                dict.set_item("e_tag", stored.e_tag)?;
                dict.set_item("version", stored.version)?;
                Ok(dict.into_any().unbind())
            })
        })
    }

    /// Return ``True`` if an object exists at ``url``, otherwise ``False``.
    ///
    /// A missing object yields ``False`` rather than raising. Use
    /// :py:meth:`get_object_metadata` or :py:meth:`get_object` if you prefer
    /// the missing case to raise ``FileNotFoundError``.
    fn object_exists<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let exists = inner.exists(&url).await.map_err(client_to_py_err)?;
            Ok(exists)
        })
    }

    /// Create the bucket / container identified by ``url``.
    ///
    /// The bucket is identified by the URL's scheme and host; for ``file://``
    /// URLs the path is the directory to create. Supported for ``file://`` and
    /// ``s3://`` (AWS S3, MinIO and SeaweedFS). Creating a bucket that already
    /// exists is treated as success.
    fn create_bucket<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            inner.create_bucket(&url).await.map_err(client_to_py_err)?;
            Ok(())
        })
    }

    fn copy_object<'py>(
        &self,
        py: Python<'py>,
        from_url: String,
        to_url: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            inner
                .copy(&from_url, &to_url)
                .await
                .map_err(client_to_py_err)?;
            Ok(())
        })
    }

    fn move_object<'py>(
        &self,
        py: Python<'py>,
        from_url: String,
        to_url: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            inner
                .move_object(&from_url, &to_url)
                .await
                .map_err(client_to_py_err)?;
            Ok(())
        })
    }

    /// Generate a pre-signed URL for ``url``.
    ///
    /// ``method`` is one of ``"GET"``, ``"PUT"``, ``"POST"``, ``"DELETE"`` or
    /// ``"HEAD"`` (case-insensitive). ``expires_in_secs`` is how long the URL
    /// remains valid, in seconds (default one hour).
    ///
    /// ``content_length`` and ``content_type`` bind a required ``Content-Length``
    /// and ``Content-Type`` into the signature (S3 only): a client using the URL
    /// MUST send exactly those header values or the request is rejected with a
    /// 403 signature mismatch. Leave them as ``None`` for plain signing.
    ///
    /// Supported for ``s3://``, ``gs://``/``gcs://`` and Azure schemes; binding
    /// content headers is only supported for ``s3://``.
    #[pyo3(signature = (url, method="GET", expires_in_secs=3600, content_length=None, content_type=None))]
    fn get_pre_signed_url<'py>(
        &self,
        py: Python<'py>,
        url: String,
        method: &str,
        expires_in_secs: u64,
        content_length: Option<u64>,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let method: SignMethod = method.parse().map_err(client_to_py_err)?;
        let expires_in = Duration::from_secs(expires_in_secs);
        let options = SignOptions {
            content_length,
            content_type,
        };

        future_into_py(py, async move {
            let signed = inner
                .get_pre_signed_url(&url, method, expires_in, &options)
                .await
                .map_err(client_to_py_err)?;
            Ok(signed)
        })
    }

    /// Generate pre-signed URLs for multiple objects sharing the same backend.
    ///
    /// All ``urls`` must resolve to the same backend (scheme and host). See
    /// :py:meth:`get_pre_signed_url` for the meaning of ``method``,
    /// ``expires_in_secs``, ``content_length`` and ``content_type``; the same
    /// options are applied to every URL.
    #[pyo3(signature = (urls, method="GET", expires_in_secs=3600, content_length=None, content_type=None))]
    fn get_pre_signed_urls<'py>(
        &self,
        py: Python<'py>,
        urls: Vec<String>,
        method: &str,
        expires_in_secs: u64,
        content_length: Option<u64>,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let method: SignMethod = method.parse().map_err(client_to_py_err)?;
        let expires_in = Duration::from_secs(expires_in_secs);
        let options = SignOptions {
            content_length,
            content_type,
        };

        future_into_py(py, async move {
            let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
            let signed = inner
                .get_pre_signed_urls(&refs, method, expires_in, &options)
                .await
                .map_err(client_to_py_err)?;
            Ok(signed)
        })
    }
}

#[pymodule]
fn _object_storage_client(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ObjectStorageClient>()?;
    m.add_class::<ByteStream>()?;
    Ok(())
}
