use super::encryption::{EncryptedObjectStorageClient, Keyring};
use super::stream::py_async_iter_to_stream;
use super::{call, client_to_py_err, int_in};
use crate::client::{EncryptionInfo, ObjectMetadata, ObjectStorageClient as InternalClient};
use crate::codec::CallerContext;
use crate::sign::{SignMethod, SignOptions};
use crate::upload::UploadOptions;
use bytes::Bytes;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyMemoryView};
use pyo3_async_runtimes::tokio::get_current_locals;
use std::sync::Arc;
use std::time::Duration;

/// Largest accepted `size_hint`: 5 TiB, the biggest S3 object. The hint sets
/// the part size, so an absurd value must not inflate it.
pub(super) const MAX_SIZE_HINT: u64 = 5 << 40;

/// Upload options carrying the caller's optional `size_hint`, which must be in
/// `0..=MAX_SIZE_HINT`.
pub(super) fn upload_options(size_hint: Option<i128>) -> PyResult<UploadOptions> {
    let size_hint = size_hint
        .map(|n| {
            let n = u64::try_from(n)
                .map_err(|_| PyValueError::new_err("size_hint must not be negative"))?;
            if n > MAX_SIZE_HINT {
                return Err(PyValueError::new_err(format!(
                    "size_hint must not exceed {MAX_SIZE_HINT} bytes (5 TiB)"
                )));
            }
            Ok(n)
        })
        .transpose()?;
    Ok(UploadOptions {
        size_hint,
        ..UploadOptions::default()
    })
}

/// Converts a bytes-like object into `Bytes`, or `None` if it is not one.
/// `bytes` is shared without a copy and `bytearray` snapshotted once. Any other
/// buffer-protocol object (`memoryview`, `array.array`, ...) is copied once
/// through `bytes(obj)`, whatever its item format. (`PyBuffer<u8>` is not used:
/// it rejects non-`B` formats.)
pub(super) fn buffer_to_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Option<Bytes>> {
    if obj.is_instance_of::<PyBytes>() || obj.is_instance_of::<PyByteArray>() {
        return Ok(Some(Bytes::from_owner(obj.extract::<PyBackedBytes>()?)));
    }
    let py = obj.py();
    // `memoryview(obj)` succeeds exactly for buffer-protocol objects, which
    // keeps `bytes(obj)` from reading an int as a length or an iterable.
    let Ok(view) = py.get_type::<PyMemoryView>().call1((obj,)) else {
        return Ok(None);
    };
    let copy = py.get_type::<PyBytes>().call1((view,))?;
    Ok(Some(Bytes::from_owner(copy.extract::<PyBackedBytes>()?)))
}

/// Converts a bytes-like object or a list of ints into `Bytes`.
pub(super) fn bytes_from_py(data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Some(bytes) = buffer_to_bytes(data)? {
        return Ok(bytes);
    }
    data.extract::<Vec<u8>>().map(Bytes::from).map_err(|_| {
        PyTypeError::new_err(
            "Expected a bytes-like object (bytes, bytearray, memoryview, ...) or a list of integers",
        )
    })
}

/// The envelope details as a dict: `format`, `message_id`, `key_names`,
/// `frame_length` and `context`.
pub(super) fn encryption_to_dict(
    py: Python<'_>,
    info: EncryptionInfo,
) -> PyResult<Bound<'_, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("format", info.format)?;
    dict.set_item("message_id", info.message_id)?;
    dict.set_item("key_names", info.key_names)?;
    dict.set_item("frame_length", info.frame_length)?;
    dict.set_item("context", info.context)?;
    Ok(dict)
}

impl<'py> IntoPyObject<'py> for EncryptionInfo {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        encryption_to_dict(py, self)
    }
}

impl<'py> IntoPyObject<'py> for ObjectMetadata {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        metadata_to_dict(py, self)
    }
}

/// The metadata dict; the encryption keys appear only for encrypted objects.
fn metadata_to_dict(py: Python<'_>, stored: ObjectMetadata) -> PyResult<Bound<'_, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("location", stored.location)?;
    dict.set_item("last_modified", stored.last_modified.to_rfc3339())?;
    dict.set_item("size_bytes", stored.size_bytes)?;
    dict.set_item("content_type", stored.content_type)?;
    dict.set_item("e_tag", stored.e_tag)?;
    dict.set_item("version", stored.version)?;
    if let Some(info) = stored.encryption {
        dict.set_item("stored_size_bytes", stored.stored_size_bytes)?;
        dict.set_item("encrypted", true)?;
        dict.set_item("encryption", encryption_to_dict(py, info)?)?;
    }
    Ok(dict)
}

/// The signing arguments shared by both pre-sign methods.
fn sign_args(
    method: &str,
    expires_in_secs: i128,
    content_length: Option<i128>,
    content_type: Option<String>,
) -> PyResult<(SignMethod, Duration, SignOptions)> {
    Ok((
        method.parse().map_err(client_to_py_err)?,
        Duration::from_secs(int_in("expires_in_secs", expires_in_secs)?),
        SignOptions {
            content_length: content_length
                .map(|n| int_in("content_length", n))
                .transpose()?,
            content_type,
        },
    ))
}

/// A download on its way to Python as a `ByteStream`.
pub(super) fn stream_result(stream: crate::codec::ByteStream) -> super::stream::StreamResult {
    super::stream::StreamResult(super::stream::RawByteStream::new(Box::pin(
        futures::StreamExt::fuse(stream),
    )))
}

/// Emits the whole `#[pymethods]` block of a client class: the methods shared
/// by both clients followed by the class's own `extra` methods. (pyo3 does not
/// process methods produced by a macro call inside the block.)
macro_rules! client_pyclass {
    ($ty:ident, { $($extra:tt)* }) => {
        #[pymethods]
        impl $ty {
            fn delete_object<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.delete(&url).await })
            }

            /// Delete every object under the prefix. A URL that names only a
            /// bucket or container deletes everything in it. An empty URL, or
            /// the ``file://`` root, raises ``ValueError``. Symlinks below a
            /// ``file://`` prefix are removed as links and never followed.
            fn delete_objects_by_prefix<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.delete_by_prefix(&url).await })
            }

            /// List the objects under the prefix. Names are relative to the
            /// prefix and come in store order. The whole listing is held in
            /// memory (twice while it is converted); for a large prefix use
            /// :py:meth:`list_objects_stream`.
            fn list_objects<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.list(&url).await })
            }

            /// List the objects under the prefix as an async iterator of
            /// ``list[str]`` batches (at most 1024 names or 256 KiB of names
            /// each), so memory stays bounded. Names are relative to the
            /// prefix. The iterator follows the rules of ``ByteStream``; use
            /// ``aclose()`` to stop the listing early.
            fn list_objects_stream<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move {
                    Ok($crate::python::stream::ListStreamResult(
                        $crate::python::stream::RawListStream::new(inner.list_stream(&url)?),
                    ))
                })
            }

            /// Return metadata for the object at ``url``.
            ///
            /// On success returns a dict with the keys ``location`` (str),
            /// ``last_modified`` (RFC 3339 str), ``size_bytes`` (int), ``content_type``
            /// (str or ``None``), ``e_tag`` (str or ``None``) and ``version`` (str or
            /// ``None``). The encrypted client adds ``stored_size_bytes`` (int),
            /// ``encrypted`` (``True``) and ``encryption`` (a dict with ``format``,
            /// ``message_id``, ``key_names``, ``frame_length`` and ``context``), read
            /// from the unauthenticated header. A missing object raises ``FileNotFoundError``; use
            /// :py:meth:`object_exists` for a non-raising presence check.
            fn get_object_metadata<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.get_object_metadata(&url).await })
            }

            /// Return ``True`` if an object exists at ``url``, otherwise ``False``.
            ///
            /// A missing object yields ``False`` rather than raising. Use
            /// :py:meth:`get_object_metadata` or :py:meth:`get_object` if you prefer
            /// the missing case to raise ``FileNotFoundError``.
            fn object_exists<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.exists(&url).await })
            }

            /// Create the bucket / container identified by ``url``.
            ///
            /// The bucket is identified by the URL's scheme and host; for ``file://``
            /// URLs the path is the directory to create. Supported for ``file://``,
            /// ``s3://`` (AWS S3, `MinIO` and `SeaweedFS`), ``gs://``/``gcs://`` (Google
            /// Cloud Storage) and the Azure schemes (``az://``, ``wasb(s)://``,
            /// ``abfs(s)://``). Creating a bucket that already exists is treated as
            /// success.
            fn create_bucket<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.create_bucket(&url).await })
            }

            /// Return ``True`` if the bucket / container identified by ``url`` exists,
            /// otherwise ``False``.
            ///
            /// The bucket is identified by the URL's scheme and host; for ``file://``
            /// URLs the path is the directory to probe. Supported for ``file://``,
            /// ``s3://`` (AWS S3, `MinIO` and `SeaweedFS`), ``gs://``/``gcs://`` (Google
            /// Cloud Storage) and the Azure schemes (``az://``, ``wasb(s)://``,
            /// ``abfs(s)://``). A missing bucket yields ``False`` rather than raising.
            fn bucket_exists<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.bucket_exists(&url).await })
            }

            fn copy_object<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                from_url: String,
                to_url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.copy(&from_url, &to_url).await })
            }

            fn move_object<'py>(
                &self,
                py: ::pyo3::Python<'py>,
                from_url: String,
                to_url: String,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::PyAny>> {
                let inner = ::std::sync::Arc::clone(&self.inner);

                $crate::python::call(py, async move { inner.move_object(&from_url, &to_url).await })
            }

            $($extra)*
        }
    };
}
pub(super) use client_pyclass;

#[pyclass(frozen, module = "object_storage_client")]
pub struct ObjectStorageClient {
    pub(super) inner: Arc<InternalClient>,
}

client_pyclass!(ObjectStorageClient, {
    /// ``profile`` names an AWS profile for ``s3://`` credentials. It is the
    /// only credential source: static keys in ``S3_ACCESS_KEY_ID`` /
    /// ``AWS_ACCESS_KEY_ID`` are not used, and ``AWS_PROFILE`` is not read. The
    /// profile is resolved on the first signed
    /// request; a profile that cannot be resolved raises a ``RuntimeError``
    /// naming it and never falls back to instance metadata.
    /// A ``credential_process`` entry in the profile makes this process run
    /// that command, so the profile files must be trusted.
    /// An empty or blank ``profile`` raises ``ValueError``.
    /// ``with_encryption`` keeps the profile:
    /// ``ObjectStorageClient(profile="dev").with_encryption(keyring)``.
    #[new]
    #[pyo3(signature = (profile=None))]
    fn new(profile: Option<String>) -> PyResult<Self> {
        let inner = match profile {
            Some(profile) => InternalClient::new()
                .with_s3_profile(profile)
                .map_err(client_to_py_err)?,
            None => InternalClient::new(),
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// An encrypting view sharing this client's stores and connections.
    ///
    /// Multiprocessing note: use spawn/forkserver, or fork before the first
    /// client call (the async runtime does not survive a fork). A client
    /// used in a child forked after first use raises ``RuntimeError``.
    #[pyo3(signature = (keyring, bind_object_key=true, frame_length=65536))]
    fn with_encryption(
        &self,
        keyring: &Keyring,
        bind_object_key: bool,
        frame_length: i128,
    ) -> PyResult<EncryptedObjectStorageClient> {
        let cfg = keyring.config(bind_object_key, frame_length)?;
        Ok(EncryptedObjectStorageClient {
            inner: Arc::new(self.inner.with_encryption(cfg)),
        })
    }

    /// Return the whole object as ``bytes``.
    ///
    /// The object is held in memory, and peaks at about twice its size while
    /// the result is copied into a Python ``bytes``. For large objects use
    /// :py:meth:`get_object_stream`, which keeps memory flat.
    fn get_object<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        call(py, async move { inner.get(&url).await })
    }

    /// Return an async iterator over the object's bytes.
    fn get_object_stream<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        call(py, async move {
            Ok(stream_result(inner.get_stream(&url).await?))
        })
    }

    /// Upload ``data``: any bytes-like object (``bytes``, ``bytearray``,
    /// ``memoryview``, ``array.array``, ...) or a list of ints in 0..255.
    /// ``bytes`` is shared without a copy; other buffers are copied once up
    /// front.
    fn put_object<'py>(
        &self,
        py: Python<'py>,
        url: String,
        data: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let bytes = bytes_from_py(data)?;

        call(py, async move { inner.put(&url, bytes).await })
    }

    /// Upload the bytes-like chunks of an async iterable to url.
    ///
    /// Chunks are pulled in small batches (back-pressure): the iterable is
    /// read ahead by at most 256 KiB, or 1024 chunks, past what the upload has
    /// consumed. Each chunk is copied when it is yielded, so a source may reuse
    /// one buffer. The object is created only if the iterable finishes cleanly;
    /// on any error or cancellation the upload is aborted and no object is left
    /// behind. On cancellation the abort runs in the background and the
    /// interpreter waits up to 10 s for it at exit. Azure has no abort
    /// (uncommitted blocks expire after 7 days). ``SIGKILL`` or an unhandled
    /// ``SIGTERM`` can still leave an incomplete multipart upload: configure an
    /// ``AbortIncompleteMultipartUpload`` lifecycle rule.
    ///
    /// ``size_hint`` is the expected total size in bytes, if known. A large
    /// hint makes the upload use bigger parts so that it stays within the
    /// store's part limit (about 78 GiB otherwise). It is a hint, not a limit, but
    /// it must be between 0 and 5 TiB (``ValueError`` otherwise). A
    /// multipart upload holds about 40 MiB of parts in memory (up to five
    /// 8 MiB parts); with a hint beyond about 70 GiB the parts grow, and at
    /// most 32 MiB of them are in flight plus the one being filled. A store
    /// without multipart upload (``http(s)://``) buffers the whole object and
    /// refuses more than 1 GiB.
    #[pyo3(signature = (url, chunks, *, size_hint=None))]
    fn put_object_stream<'py>(
        &self,
        py: Python<'py>,
        url: String,
        chunks: &Bound<'py, PyAny>,
        size_hint: Option<i128>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let opts = upload_options(size_hint)?;
        let stream = py_async_iter_to_stream(chunks, get_current_locals(py)?)?;

        call(py, async move {
            inner
                .put_stream_inner(&url, stream, CallerContext::new(), &opts)
                .await
        })
    }

    /// Generate a pre-signed URL for ``url``.
    ///
    /// ``method`` is one of ``"GET"``, ``"PUT"``, ``"POST"``, ``"DELETE"`` or
    /// ``"HEAD"`` (case-insensitive). ``expires_in_secs`` is how long the URL
    /// remains valid, in seconds (default one hour); it must be between 1 and
    /// 604800 (7 days), otherwise ``ValueError`` is raised. An ``s3://`` URL
    /// signed with temporary credentials stops working when that session ends,
    /// whatever ``expires_in_secs`` says.
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
        expires_in_secs: i128,
        content_length: Option<i128>,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (method, expires_in, options) =
            sign_args(method, expires_in_secs, content_length, content_type)?;

        call(py, async move {
            inner
                .get_pre_signed_url(&url, method, expires_in, &options)
                .await
        })
    }

    /// Generate pre-signed URLs for multiple objects sharing the same backend.
    ///
    /// All ``urls`` must resolve to the same backend (scheme and host). See
    /// :py:meth:`get_pre_signed_url` for the meaning of ``method``,
    /// ``expires_in_secs``, ``content_length`` and ``content_type``; the same
    /// options are applied to every URL, and ``expires_in_secs`` has the same
    /// 1 to 604800 second range. An ``s3://`` URL signed with temporary
    /// credentials stops working when that session ends, whatever
    /// ``expires_in_secs`` says.
    #[pyo3(signature = (urls, method="GET", expires_in_secs=3600, content_length=None, content_type=None))]
    fn get_pre_signed_urls<'py>(
        &self,
        py: Python<'py>,
        urls: Vec<String>,
        method: &str,
        expires_in_secs: i128,
        content_length: Option<i128>,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (method, expires_in, options) =
            sign_args(method, expires_in_secs, content_length, content_type)?;

        call(py, async move {
            let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
            inner
                .get_pre_signed_urls(&refs, method, expires_in, &options)
                .await
        })
    }
});
