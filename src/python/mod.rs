#[cfg(panic = "abort")]
compile_error!("the Python extension must unwind on panic: do not build it with panic = \"abort\"");

mod client;
mod encryption;
mod stream;

use crate::client::Error;
use crate::encryption::keys::KeyError;
use encryption::{DecryptionError, EncryptionError, NotEncryptedError, UnknownKeyError};
use pyo3::exceptions::{
    PyFileNotFoundError, PyImportError, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;

/// Process that first started async work on the runtime.
static RUNTIME_PID: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// Variable that overrides the number of runtime worker threads.
const WORKER_THREADS_ENV: &str = "OSC_WORKER_THREADS";

/// The runtime is built lazily by the first `future_into_py`, so its builder
/// is configured once, right before the first use.
static RUNTIME_CONFIG: std::sync::Once = std::sync::Once::new();

/// Worker threads: `var` (the value of `OSC_WORKER_THREADS`) if it is a
/// positive integer, else `parallelism` capped at 4 (an I/O-bound client needs
/// no more, and every idle worker costs a thread and an allocator arena).
fn worker_threads_for(var: Option<&str>, parallelism: usize) -> usize {
    var.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| parallelism.min(4))
}

fn worker_threads() -> usize {
    worker_threads_for(
        std::env::var(WORKER_THREADS_ENV).ok().as_deref(),
        std::thread::available_parallelism().map_or(1, usize::from),
    )
}

/// Cap on the runtime's blocking thread pool. Result delivery, `getaddrinfo`,
/// local file I/O and large-object encryption all share this pool. Deliveries
/// that wait for the GIL gain nothing from more threads, and 64 leaves
/// headroom, so slow DNS or disk work cannot hold up every delivery. Nothing
/// in the binding makes a GIL holder wait on this pool, so a full pool only
/// queues work and cannot deadlock.
const MAX_BLOCKING_THREADS: usize = 64;

fn configure_runtime() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .worker_threads(worker_threads())
        .max_blocking_threads(MAX_BLOCKING_THREADS);
    pyo3_async_runtimes::tokio::init(builder);
}

/// Starts `fut` on the shared runtime and returns the awaitable. Every entry
/// point goes through here. A process forked after the runtime started would
/// hang (the runtime's threads do not exist in the child), so it gets a
/// `RuntimeError` instead. The pid is claimed by the first call that actually
/// starts work: a call made outside a running event loop fails first and
/// claims nothing, and a fork before the first successful call leaves the pid
/// unset, so the child claims it.
fn spawn<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    // Fails without a running event loop, before anything is claimed.
    let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
    let pid = std::process::id();
    if *RUNTIME_PID.get_or_init(|| pid) != pid {
        return Err(PyRuntimeError::new_err(
            "object_storage_client: this process was forked after the async runtime started, \
             and the runtime does not survive a fork, so no client can be used in this \
             process. Fork before the first client call, or use the 'spawn' or \
             'forkserver' start method.",
        ));
    }
    RUNTIME_CONFIG.call_once(configure_runtime);
    pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, fut)
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

/// Converts an integer argument taken as `i128` (so that pyo3 never raises
/// `OverflowError` for ints up to ±2**127) to its Rust type, or `ValueError`.
fn int_in<T: TryFrom<i128>>(name: &str, v: i128) -> PyResult<T> {
    T::try_from(v).map_err(|_| PyValueError::new_err(format!("{name} is out of range")))
}

fn client_to_py_err(e: Error) -> PyErr {
    match e {
        Error::ObjectStore(object_store::Error::NotFound { path, .. }) => {
            PyFileNotFoundError::new_err(format!("Object not found: {path}"))
        }
        Error::Python(e) => e,
        // The matching `OSError` subclass (`FileNotFoundError`, ...).
        Error::Io(e) => e.into(),
        Error::ObjectStore(
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. },
        ) => PyPermissionError::new_err(e.to_string()),
        Error::InvalidUrl(_) | Error::UnsupportedScheme(_) => PyValueError::new_err(e.to_string()),
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

/// Interpreter-exit hook: waits for the aborts of cancelled uploads. A dropped
/// multipart upload aborts in a detached task, and the runtime is a static that
/// is never dropped, so a process exiting right after a cancellation (for
/// example `asyncio.run` cancelling the remaining tasks on SIGTERM) would lose
/// the abort. Bounded like the CLI's wait. Does nothing in a forked child, and
/// builds no runtime when no call ever started one.
#[pyfunction]
fn _drain_aborts_at_exit(py: Python<'_>) {
    if RUNTIME_PID.get() != Some(&std::process::id()) {
        return;
    }
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(crate::upload::drain_pending_aborts(
            std::time::Duration::from_secs(10),
        ));
    });
}

// Built and tested only with the GIL: ByteStream's overlap check is not atomic
// across threads.
#[pymodule(gil_used = true)]
fn _object_storage_client(m: &Bound<'_, PyModule>) -> PyResult<()> {
    ensure_main_interpreter(m.py())?;
    m.add_class::<client::ObjectStorageClient>()?;
    m.add_class::<encryption::EncryptedObjectStorageClient>()?;
    m.add_class::<encryption::Keyring>()?;
    m.add_class::<stream::RawByteStream>()?;
    m.add_class::<stream::RawListStream>()?;
    let py = m.py();
    py.import("atexit")?
        .call_method1("register", (wrap_pyfunction!(_drain_aborts_at_exit, m)?,))?;
    m.add("EncryptionError", py.get_type::<EncryptionError>())?;
    m.add("DecryptionError", py.get_type::<DecryptionError>())?;
    m.add("UnknownKeyError", py.get_type::<UnknownKeyError>())?;
    m.add("NotEncryptedError", py.get_type::<NotEncryptedError>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::worker_threads_for;

    #[test]
    fn worker_threads_policy() {
        let cases: [(Option<&str>, usize, usize); 10] = [
            (None, 16, 4),
            (None, 2, 2),
            (None, 1, 1),
            (Some("3"), 16, 3),
            (Some(" 2 "), 8, 2),
            (Some("64"), 2, 64),
            (Some("0"), 16, 4),
            (Some("-1"), 1, 1),
            (Some("many"), 8, 4),
            (Some(""), 8, 4),
        ];
        for (var, parallelism, want) in cases {
            assert_eq!(
                worker_threads_for(var, parallelism),
                want,
                "{var:?} on {parallelism}"
            );
        }
    }
}
