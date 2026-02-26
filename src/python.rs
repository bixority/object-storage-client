use crate::client::ObjectStorageClient as InternalClient;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

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

    fn get<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let bytes = inner
                .get(&url)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let result = Python::try_attach(|py| {
                let py_bytes = PyBytes::new(py, &bytes);
                py_bytes.into_any().unbind()
            });
            match result {
                Some(res) => Ok(res),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Failed to attach to Python",
                )),
            }
        })
    }

    fn put<'py>(&self, py: Python<'py>, url: String, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner
                .put(&url, data)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    fn delete<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner
                .delete(&url)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    fn list<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let results = inner
                .list(&url)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(results)
        })
    }

    fn head<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let meta = inner
                .head(&url)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let result = Python::try_attach(|py| {
                let dict = PyDict::new(py);
                dict.set_item("location", meta.location.to_string())?;
                dict.set_item("last_modified", meta.last_modified.to_rfc3339())?;
                dict.set_item("size", meta.size)?;
                if let Some(e_tag) = meta.e_tag {
                    dict.set_item("e_tag", e_tag)?;
                }
                if let Some(version) = meta.version {
                    dict.set_item("version", version)?;
                }
                Ok(dict.into_any().unbind())
            });
            match result {
                Some(res) => {
                    res.map_err(|e: PyErr| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                }
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Failed to attach to Python",
                )),
            }
        })
    }

    fn copy<'py>(
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
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
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
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ObjectStorageClient>()?;
    Ok(())
}
