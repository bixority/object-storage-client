mod client;
mod encryption;
mod stream;

use crate::client::Error;
use crate::encryption::keys::KeyError;
use encryption::{DecryptionError, EncryptionError, NotEncryptedError, UnknownKeyError};
use pyo3::exceptions::{PyFileNotFoundError, PyImportError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// Process that first started async work on the runtime.
static RUNTIME_PID: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// Variable that overrides the number of runtime worker threads.
const WORKER_THREADS_ENV: &str = "OSC_WORKER_THREADS";

/// The runtime is built lazily by the first `future_into_py`, so its builder
/// is configured once, right before the first use.
static RUNTIME_CONFIG: std::sync::Once = std::sync::Once::new();

/// Worker threads: `OSC_WORKER_THREADS` if it is a positive integer, else the
/// available parallelism capped at 4 (an I/O-bound client needs no more, and
/// every idle worker costs a thread and an allocator arena).
fn worker_threads() -> usize {
    std::env::var(WORKER_THREADS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(4)
        })
}

fn configure_runtime() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().worker_threads(worker_threads());
    pyo3_async_runtimes::tokio::init(builder);
}

/// Starts `fut` on the shared runtime and returns the awaitable. Every entry
/// point goes through here. A process forked after the runtime started would
/// hang (the runtime's threads do not exist in the child), so it gets a
/// `RuntimeError` instead. A fork before first use leaves the pid unset and
/// the child claims it.
fn spawn<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    let pid = std::process::id();
    if *RUNTIME_PID.get_or_init(|| pid) != pid {
        return Err(PyRuntimeError::new_err(
            "object_storage_client: this process was forked after the async runtime started; \
             the runtime does not survive fork. Create clients after forking, or use the \
             'spawn' or 'forkserver' start method.",
        ));
    }
    RUNTIME_CONFIG.call_once(configure_runtime);
    pyo3_async_runtimes::tokio::future_into_py(py, fut)
}

/// [`spawn`] for a client call: its error becomes the matching Python
/// exception and its value converts to Python as is.
fn call<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = Result<T, Error>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    spawn(py, async move { fut.await.map_err(client_to_py_err) })
}

fn client_to_py_err(e: Error) -> PyErr {
    match e {
        Error::ObjectStore(object_store::Error::NotFound { path, .. }) => {
            PyFileNotFoundError::new_err(format!("Object not found: {path}"))
        }
        Error::Python(e) => e,
        Error::Decryption(_) => DecryptionError::new_err(e.to_string()),
        Error::Key(KeyError::UnknownKey { .. }) => UnknownKeyError::new_err(e.to_string()),
        Error::Key(_) => EncryptionError::new_err(e.to_string()),
        Error::NotEncrypted { .. } => NotEncryptedError::new_err(e.to_string()),
        Error::InvalidInput(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// Refuses to load anywhere but the main interpreter. Module-level state (the
/// runtime pid, the cached `ByteStream` class, the asyncio objects cached by
/// `pyo3-async-runtimes`) is shared by the whole process, so a second
/// interpreter that shares the GIL (a "legacy" subinterpreter, as `mod_wsgi`
/// creates) would mix objects between interpreters and hang. pyo3 itself only
/// blocks isolated ones. Fails closed when the interpreter cannot be
/// identified. Uses `_interpreters` (present on every supported Python)
/// because the crate forbids `unsafe` and so cannot call
/// `PyInterpreterState_Main` directly.
fn ensure_main_interpreter(py: Python<'_>) -> PyResult<()> {
    let id = |name: &str| -> PyResult<usize> {
        py.import("_interpreters")?
            .call_method0(name)?
            .get_item(0)?
            .extract()
    };
    if id("get_current")? == id("get_main")? {
        Ok(())
    } else {
        Err(PyImportError::new_err(
            "object_storage_client can only be imported in the main interpreter",
        ))
    }
}

#[pymodule]
fn _object_storage_client(m: &Bound<'_, PyModule>) -> PyResult<()> {
    ensure_main_interpreter(m.py())?;
    m.add_class::<client::ObjectStorageClient>()?;
    m.add_class::<encryption::EncryptedObjectStorageClient>()?;
    m.add_class::<encryption::Keyring>()?;
    m.add_class::<stream::RawByteStream>()?;
    let py = m.py();
    m.add("EncryptionError", py.get_type::<EncryptionError>())?;
    m.add("DecryptionError", py.get_type::<DecryptionError>())?;
    m.add("UnknownKeyError", py.get_type::<UnknownKeyError>())?;
    m.add("NotEncryptedError", py.get_type::<NotEncryptedError>())?;
    Ok(())
}
