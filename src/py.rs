// Python bindings
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyclass::PyClass;
use pyo3::types::{PyBytes, PyDateTime, PyList};

#[pyclass]
#[derive(Clone, Debug)]
struct PyConfig {
    #[pyo3(get, set)]
    s3_region: Option<String>,
    #[pyo3(get, set)]
    azure_account: Option<String>,
}

impl Default for PyConfig {
    fn default() -> Self {
        Self {
            s3_region: None,
            azure_account: None,
        }
    }
}

impl From<PyConfig> for Config {
    fn from(value: PyConfig) -> Self {
        Self {
            s3_region: value.s3_region,
            azure_account: value.azure_account,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
struct PyObjectInfo {
    key: String,
    size: Option<u64>,
    last_modified: Option<String>,
    etag: Option<String>,
}

impl From<ObjectInfo> for PyObjectInfo {
    fn from(value: ObjectInfo) -> Self {
        Self {
            key: value.key,
            size: value.size,
            last_modified: value.last_modified.map(|lm| lm.to_rfc3339()),
            etag: value.etag,
        }
    }
}

#[pyclass]
struct PyObjectStorage {
    inner: Arc<ObjectStorage>,
}

#[pymethods]
impl PyObjectStorage {
    #[classmethod]
    fn new(cls: &PyType, py: Python, config: PyRef<PyConfig>) -> PyResult<Py<PyAny>> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let inner = ObjectStorage::new(config.into())
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Py::new(
                py,
                PyObjectStorage {
                    inner: Arc::new(inner),
                },
            )
            .map_err(Into::into)
        })
    }

    fn list(&self, py: Python, prefix: String) -> PyResult<Py<PyAny>> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = inner
                .list(&prefix)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let py_infos: Vec<PyObjectInfo> = res.into_iter().map(Into::into).collect();
            Ok(py_infos)
        })
    }

    fn get(&self, py: Python, key: String) -> PyResult<Py<PyAny>> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut stream = inner
                .get_stream(&key)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let mut data = Vec::new();
            while let Some(item) = stream.next().await {
                data.extend_from_slice(&item.map_err(|e| PyRuntimeError::new_err(e.to_string()))?);
            }
            Ok(data)
        })
    }

    fn put(&self, py: Python, key: String, data: Vec<u8>) -> PyResult<Py<PyAny>> {
        let inner = self.inner.clone();
        let content_length = Some(data.len() as u64);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let stream = futures::stream::once(futures::future::ok(Bytes::from(data)));
            inner
                .put_stream(&key, stream, content_length)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    fn delete(&self, py: Python, key: String) -> PyResult<Py<PyAny>> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .delete(&key)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    fn copy(&self, py: Python, src: String, dest: String) -> PyResult<Py<PyAny>> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .copy(&src, &dest)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

// Implement IntoPy for Vec<PyObjectInfo>
impl ToPyObject for Vec<PyObjectInfo> {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        let list = PyList::new(py);
        for item in self {
            list.append(Py::new(py, item.clone()).unwrap()).unwrap();
        }
        list.into_py(py)
    }
}

impl IntoPy<Py<PyAny>> for Vec<PyObjectInfo> {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.to_object(py)
    }
}

// Implement IntoPy for Vec<u8> to PyBytes
impl IntoPy<Py<PyAny>> for Vec<u8> {
    fn into_py(self, py: Python) -> Py<PyAny> {
        PyBytes::new(py, &self).into_py(py)
    }
}

// Implement IntoPy for ()
impl IntoPy<Py<PyAny>> for () {
    fn into_py(self, py: Python) -> Py<PyAny> {
        py.None()
    }
}

#[pymodule]
fn object_storage(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_asyncio::tokio::init_multi_thread_once();
    m.add_class::<PyConfig>()?;
    m.add_class::<PyObjectInfo>()?;
    m.add_class::<PyObjectStorage>()?;
    Ok(())
}
