use super::client::buffer_to_bytes;
use super::client_to_py_err;
use crate::codec::ByteStream;
use bytes::Bytes;
use futures::StreamExt;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use pyo3::types::PyCFunction;
use pyo3::types::{PyTuple, PyType};
use pyo3_async_runtimes::tokio::get_current_locals;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

/// How much a fill gathers before it signals the consumer. Fewer, larger
/// chunks cut the per-read cost of crossing into Python.
const BATCH_BYTES: usize = 256 * 1024;

/// How long a fill keeps waiting for more items once it holds one. Bounds the
/// extra latency a slow source can add to the first bytes.
const BATCH_WAIT: Duration = Duration::from_millis(1);

/// The end of the stream, parked behind the chunks buffered before it.
enum Terminal {
    Err(crate::Error),
    /// A failure already raised once. Parked for good, so a retried read
    /// raises it again instead of reporting a clean end of stream. Holds the
    /// exception class and args, never the exception object: its traceback
    /// would reference the reader's frames and form a cycle through Rust that
    /// the Python GC cannot see.
    Failed(Py<PyType>, Py<PyTuple>),
    Eof,
}

/// Items pulled by a background fill and not yet handed to the consumer.
#[derive(Default)]
struct Slot {
    chunks: VecDeque<Bytes>,
    /// Total length of `chunks`.
    buffered: usize,
    terminal: Option<Terminal>,
}

impl Slot {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.terminal.is_none()
    }
}

struct Shared {
    stream: Mutex<ByteStream>,
    slot: std::sync::Mutex<Slot>,
    /// Set (under the stream mutex) once an error was pulled. A later fill
    /// never touches the replaced, empty stream, so it cannot store an `Eof`
    /// after a failure.
    failed: AtomicBool,
}

impl Shared {
    fn lock_slot(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pulls items into the slot unless it already holds something: waits for
    /// the first, then keeps taking whatever arrives within [`BATCH_WAIT`]
    /// until [`BATCH_BYTES`] are buffered or the stream ends. Each item is
    /// stored under the lock right after `next()` returns, with no `.await`
    /// in between, so dropping this future (a cancelled read) never loses a
    /// pulled item. An error or the end is parked behind the chunks already
    /// buffered.
    async fn fill(&self) {
        let mut stream = self.stream.lock().await;
        if self.failed.load(Ordering::Acquire) || !self.lock_slot().is_empty() {
            return;
        }
        let mut deadline = None;
        loop {
            let next = match deadline {
                None => stream.next().await,
                Some(at) => match tokio::time::timeout_at(at, stream.next()).await {
                    Ok(item) => item,
                    Err(_) => break,
                },
            };
            let terminal = match next {
                Some(Ok(bytes)) => {
                    let mut slot = self.lock_slot();
                    slot.buffered += bytes.len();
                    slot.chunks.push_back(bytes);
                    if slot.buffered >= BATCH_BYTES {
                        break;
                    }
                    drop(slot);
                    deadline.get_or_insert_with(|| tokio::time::Instant::now() + BATCH_WAIT);
                    continue;
                }
                Some(Err(e)) => Terminal::Err(e),
                None => Terminal::Eof,
            };
            // Terminal: release the file descriptor, connection and key
            // config now instead of when the stream object is dropped.
            *stream = futures::stream::empty().boxed();
            if matches!(terminal, Terminal::Err(_)) {
                self.failed.store(true, Ordering::Release);
            }
            self.lock_slot().terminal = Some(terminal);
            break;
        }
        drop(stream);
    }
}

/// Native half of `object_storage_client.ByteStream` (private).
///
/// `fill()` returns an asyncio future that only signals that something is
/// parked; `take()` pops the parked chunks synchronously in the caller's own
/// frame, so a cancel at any point leaves the data in the slot.
#[pyclass(name = "_RawByteStream", frozen, module = "object_storage_client")]
pub struct RawByteStream {
    shared: Arc<Shared>,
}

impl RawByteStream {
    pub fn new(inner: ByteStream) -> Self {
        Self {
            shared: Arc::new(Shared {
                stream: Mutex::new(inner),
                slot: std::sync::Mutex::new(Slot::default()),
                failed: AtomicBool::new(false),
            }),
        }
    }
}

#[pymethods]
impl RawByteStream {
    /// Pull items into the slot (no-op if it already holds something).
    fn fill<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let shared = Arc::clone(&self.shared);
        super::spawn(py, async move {
            shared.fill().await;
            Ok(())
        })
    }

    /// Whether anything is parked in the slot.
    fn ready(&self) -> bool {
        !self.shared.lock_slot().is_empty()
    }

    /// Pop the parked data: the buffered chunks joined into one ``bytes``,
    /// ``None`` at EOF, or raise. Buffered chunks come first; the error or EOF
    /// behind them is reported by the next call. After an error every later
    /// call raises it again; EOF is never reported.
    #[allow(clippy::significant_drop_tightening)] // the guard is held on purpose
    fn take<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        // The guard is held to the end, so the slot is never observably empty
        // between an error being popped and `Failed` being parked. Fill never
        // needs the GIL and never holds the slot lock across an await, so
        // there is no lock-order issue with the stream mutex.
        let mut slot = self.shared.lock_slot();
        if !slot.chunks.is_empty() {
            let total = std::mem::take(&mut slot.buffered);
            let chunks = std::mem::take(&mut slot.chunks);
            return PyBytes::new_with(py, total, |out| {
                let mut at = 0;
                for c in &chunks {
                    out[at..at + c.len()].copy_from_slice(c);
                    at += c.len();
                }
                Ok(())
            })
            .map(Some);
        }
        match slot.terminal.take() {
            Some(Terminal::Eof) => Ok(None),
            Some(Terminal::Err(e)) => {
                let err = client_to_py_err(e);
                let ty = err.get_type(py).unbind();
                let args = err
                    .value(py)
                    .getattr(intern!(py, "args"))
                    .and_then(|a| Ok(a.cast_into::<PyTuple>()?.unbind()))
                    .unwrap_or_else(|_| PyTuple::empty(py).unbind());
                slot.terminal = Some(Terminal::Failed(ty, args));
                Err(err)
            }
            Some(Terminal::Failed(ty, args)) => {
                let fresh = ty.bind(py).call1(args.bind(py)).map_or_else(
                    |_| PyRuntimeError::new_err(format!("{}: {}", ty.bind(py), args.bind(py))),
                    PyErr::from_value,
                );
                slot.terminal = Some(Terminal::Failed(ty, args));
                Err(fresh)
            }
            None => Err(PyRuntimeError::new_err(
                "ByteStream: no item is ready; call fill() first",
            )),
        }
    }
}

static BYTE_STREAM_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

pub fn wrap_stream(py: Python<'_>, raw: RawByteStream) -> PyResult<Py<PyAny>> {
    let cls = BYTE_STREAM_CLS.get_or_try_init(py, || {
        PyResult::Ok(
            py.import("object_storage_client._stream")?
                .getattr("ByteStream")?
                .unbind(),
        )
    })?;
    Ok(cls.bind(py).call1((Py::new(py, raw)?,))?.unbind())
}

/// Converts one item yielded by a Python async iterator into bytes. `bytes`
/// is shared without a copy; mutable buffers are snapshotted once. The error
/// never includes the chunk's contents.
fn chunk_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Some(bytes) = buffer_to_bytes(obj)? {
        Ok(bytes)
    } else {
        let ty = obj.get_type().name()?;
        Err(PyTypeError::new_err(format!(
            "put_object_stream: expected bytes-like chunk, got {ty}"
        )))
    }
}

/// Cancels the scheduled Python task (via its `concurrent.futures.Future`)
/// when dropped, so cancelling the upload cancels the in-flight `__anext__`.
///
/// Cleared once the task has finished, so a completed chunk costs no GIL
/// acquisition to cancel.
struct CancelOnDrop(Option<Py<PyAny>>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cf) = self.0.take() {
            Python::try_attach(|py| {
                // Best effort: a no-op if the task already finished.
                let _ = cf.bind(py).call_method0(intern!(py, "cancel"));
            });
        }
    }
}

/// Runs `_anext_batch(it, BATCH_BYTES)` as a task on the caller's event loop
/// and waits for its result: the converted chunks and whether the iterator is
/// exhausted. Dropping the returned future cancels that task.
async fn anext_batch_cancellable(it: &Py<PyAny>) -> PyResult<(Vec<Bytes>, bool)> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut guard = Python::attach(|py| -> PyResult<CancelOnDrop> {
        let locals = get_current_locals(py)?;
        let coro = py
            .import("object_storage_client._stream")?
            .getattr("_anext_batch")?
            .call1((it.bind(py), BATCH_BYTES))?;
        let asyncio = py.import("asyncio")?;
        let cf = locals.context(py).call_method1(
            intern!(py, "run"),
            (
                asyncio.getattr("run_coroutine_threadsafe")?,
                coro,
                locals.event_loop(py),
            ),
        )?;
        let tx = std::sync::Mutex::new(Some(tx));
        let done = PyCFunction::new_closure(py, None, None, move |args, _| {
            let fut = args.get_item(0)?;
            // Runs with the GIL held: convert here, not on a tokio worker.
            let res = fut.call_method0("result").and_then(|obj| {
                let (items, done): (Vec<Bound<'_, PyAny>>, bool) = obj.extract()?;
                let chunks = items
                    .iter()
                    .map(chunk_from_py)
                    .collect::<PyResult<Vec<_>>>()?;
                Ok((chunks, done))
            });
            if let Some(tx) = tx.lock().ok().and_then(|mut t| t.take()) {
                let _ = tx.send(res);
            }
            PyResult::Ok(())
        })?;
        cf.call_method1(intern!(py, "add_done_callback"), (done,))?;
        Ok(CancelOnDrop(Some(cf.unbind())))
    })?;
    let res = rx.await;
    guard.0 = None; // the task has finished; do not take the GIL just to cancel it
    res.unwrap_or_else(|_| Err(PyRuntimeError::new_err("event loop closed")))
}

/// State of the stream over a Python async iterator.
struct PyChunks {
    iter: Py<PyAny>,
    /// Converted chunks not yet handed to the consumer.
    ready: VecDeque<Bytes>,
    /// The iterator raised `StopAsyncIteration`.
    done: bool,
}

/// Adapts a Python async iterable of bytes-like chunks into a Rust stream.
///
/// Items are pulled in batches, only when the consumer has used up the last
/// one, so the read-ahead is bounded by [`BATCH_BYTES`] (plus one item). The
/// stream ends after the first error.
pub fn py_async_iter_to_stream(iterable: &Bound<'_, PyAny>) -> PyResult<ByteStream> {
    let iter = iterable.call_method0("__aiter__")?.unbind();
    let state = PyChunks {
        iter,
        ready: VecDeque::new(),
        done: false,
    };
    Ok(futures::stream::unfold(Some(state), |state| async move {
        let mut st = state?;
        loop {
            if let Some(b) = st.ready.pop_front() {
                return Some((Ok(b), Some(st)));
            }
            if st.done {
                return None;
            }
            match anext_batch_cancellable(&st.iter).await {
                Ok((chunks, done)) => {
                    st.ready.extend(chunks);
                    st.done = done;
                }
                Err(e) => return Some((Err(crate::Error::Python(e)), None)),
            }
        }
    })
    .boxed())
}
