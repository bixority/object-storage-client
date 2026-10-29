use super::client::buffer_to_bytes;
use super::client_to_py_err;
use crate::codec::ByteStream;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use pyo3::PyTypeInfo;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyCFunction;
use pyo3::types::{PyBytes, PyList};
use pyo3::types::{PyTuple, PyType};
use pyo3_async_runtimes::TaskLocals;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

/// How much a fill gathers before it signals the consumer. Fewer, larger
/// chunks cut the per-read cost of crossing into Python.
const BATCH_BYTES: usize = 256 * 1024;

/// Most names in one listing batch.
const BATCH_NAMES: usize = 1024;

/// An item a fill can batch: its size counts against [`BATCH_BYTES`], and
/// `MAX_ITEMS` caps how many go into one batch.
trait Item: Send + 'static {
    const MAX_ITEMS: usize;
    fn size(&self) -> usize;
}

impl Item for Bytes {
    const MAX_ITEMS: usize = usize::MAX;
    fn size(&self) -> usize {
        self.len()
    }
}

impl Item for String {
    const MAX_ITEMS: usize = BATCH_NAMES;
    fn size(&self) -> usize {
        self.len()
    }
}

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
struct Slot<T> {
    chunks: VecDeque<T>,
    /// Total size of `chunks`.
    buffered: usize,
    /// An item pulled but not fitting the full batch; it starts the next one.
    carry: Option<T>,
    terminal: Option<Terminal>,
}

impl<T: Item> Slot<T> {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.terminal.is_none()
    }

    /// Moves the carry, if any, into the batch it opens; says whether the
    /// batch is full.
    fn seed(&mut self) -> Option<bool> {
        self.carry.take().map(|item| self.add(item))
    }

    /// Adds a pulled item and says whether the batch is now full. An item
    /// that would overflow a non-empty batch becomes the carry instead; a
    /// lone oversized item is still delivered, alone.
    fn add(&mut self, item: T) -> bool {
        if !self.chunks.is_empty() && self.buffered + item.size() > BATCH_BYTES {
            self.carry = Some(item);
            return true;
        }
        self.buffered += item.size();
        self.chunks.push_back(item);
        self.buffered >= BATCH_BYTES || self.chunks.len() >= T::MAX_ITEMS
    }
}

struct Shared<T> {
    stream: Mutex<BoxStream<'static, crate::Result<T>>>,
    slot: std::sync::Mutex<Slot<T>>,
    /// Set (under the stream mutex) once an error was pulled. A later fill
    /// never touches the replaced, empty stream, so it cannot store an `Eof`
    /// after a failure.
    failed: AtomicBool,
}

impl<T: Item> Shared<T> {
    fn new(stream: BoxStream<'static, crate::Result<T>>) -> Self {
        Self {
            stream: Mutex::new(stream),
            slot: std::sync::Mutex::new(Slot {
                chunks: VecDeque::new(),
                buffered: 0,
                carry: None,
                terminal: None,
            }),
            failed: AtomicBool::new(false),
        }
    }

    fn lock_slot(&self) -> std::sync::MutexGuard<'_, Slot<T>> {
        self.slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pulls items into the slot unless it already holds something: waits for
    /// the first, then keeps taking whatever arrives within [`BATCH_WAIT`]
    /// until a batch is full or the stream ends. Each item is stored under
    /// the lock right after `next()` returns, with no `.await` in between, so
    /// dropping this future (a cancelled read) never loses a pulled item. An
    /// item that does not fit the batch is parked as the carry and opens the
    /// next one. An error or the end is parked behind the items already
    /// buffered.
    async fn fill(&self) {
        let mut stream = self.stream.lock().await;
        if self.failed.load(Ordering::Acquire) || !self.lock_slot().is_empty() {
            return;
        }
        let mut deadline = None;
        let seeded = self.lock_slot().seed();
        match seeded {
            Some(true) => return,
            Some(false) => deadline = Some(tokio::time::Instant::now() + BATCH_WAIT),
            None => {}
        }
        loop {
            let next = match deadline {
                None => stream.next().await,
                Some(at) => match tokio::time::timeout_at(at, stream.next()).await {
                    Ok(item) => item,
                    Err(_) => break,
                },
            };
            let terminal = match next {
                Some(Ok(item)) => {
                    if self.lock_slot().add(item) {
                        break;
                    }
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

    /// Pops the parked batch and converts it with `convert` (given the items
    /// and their total size): `None` at EOF, or the error. Buffered items come
    /// first; the error or EOF behind them is reported by the next call.
    /// After an error every later call raises it again; EOF is never
    /// reported.
    #[allow(clippy::significant_drop_tightening)] // the guard is held on purpose
    fn take<'py, R>(
        &self,
        py: Python<'py>,
        what: &str,
        convert: impl FnOnce(Python<'py>, VecDeque<T>, usize) -> PyResult<R>,
    ) -> PyResult<Option<R>> {
        // The guard is held to the end, so the slot is never observably empty
        // between an error being popped and `Failed` being parked. Fill never
        // needs the GIL and never holds the slot lock across an await, so
        // there is no lock-order issue with the stream mutex.
        let mut slot = self.lock_slot();
        if !slot.chunks.is_empty() {
            let total = std::mem::take(&mut slot.buffered);
            let items = std::mem::take(&mut slot.chunks);
            return convert(py, items, total).map(Some);
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
            None => Err(PyRuntimeError::new_err(format!(
                "{what}: no item is ready; call fill() first"
            ))),
        }
    }
}

/// Native half of `object_storage_client.ByteStream` (private).
///
/// `fill()` returns an asyncio future that only signals that something is
/// parked; `take()` pops the parked chunks synchronously in the caller's own
/// frame, so a cancel at any point leaves the data in the slot.
#[pyclass(name = "_RawByteStream", frozen, module = "object_storage_client")]
pub struct RawByteStream {
    shared: Arc<Shared<Bytes>>,
}

impl RawByteStream {
    pub fn new(inner: ByteStream) -> Self {
        Self {
            shared: Arc::new(Shared::new(inner)),
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
    fn take<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        self.shared.take(py, "ByteStream", |py, chunks, total| {
            PyBytes::new_with(py, total, |out| {
                let mut at = 0;
                for c in &chunks {
                    out[at..at + c.len()].copy_from_slice(c);
                    at += c.len();
                }
                Ok(())
            })
        })
    }
}

/// Native half of `object_storage_client.ListStream` (private): the same
/// fill/take slot over listed names, taken as one `list[str]` per batch.
#[pyclass(name = "_RawListStream", frozen, module = "object_storage_client")]
pub struct RawListStream {
    shared: Arc<Shared<String>>,
}

impl RawListStream {
    pub fn new(inner: BoxStream<'static, crate::Result<String>>) -> Self {
        Self {
            shared: Arc::new(Shared::new(inner)),
        }
    }
}

#[pymethods]
impl RawListStream {
    /// Pull names into the slot (no-op if it already holds something).
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

    /// Pop the parked batch as a ``list[str]``, ``None`` at the end of the
    /// listing, or raise (see ``_RawByteStream.take``).
    fn take<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyList>>> {
        self.shared
            .take(py, "ListStream", |py, names, _| PyList::new(py, names))
    }
}

static BYTE_STREAM_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static LIST_STREAM_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// A download stream on its way to Python: converted to the `ByteStream`
/// object by the blocking pool, not on a runtime worker.
pub struct StreamResult(pub RawByteStream);

impl<'py> IntoPyObject<'py> for StreamResult {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        wrap_stream(py, &BYTE_STREAM_CLS, "ByteStream", Py::new(py, self.0)?)
    }
}

/// A listing on its way to Python as a `ListStream`, converted like
/// [`StreamResult`].
pub struct ListStreamResult(pub RawListStream);

impl<'py> IntoPyObject<'py> for ListStreamResult {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        wrap_stream(py, &LIST_STREAM_CLS, "ListStream", Py::new(py, self.0)?)
    }
}

/// Wraps a native stream in the Python class `name` of `_stream`.
fn wrap_stream<'py, T: PyTypeInfo>(
    py: Python<'py>,
    cache: &'static PyOnceLock<Py<PyAny>>,
    name: &str,
    raw: Py<T>,
) -> PyResult<Bound<'py, PyAny>> {
    let cls = cache.get_or_try_init(py, || {
        PyResult::Ok(
            py.import("object_storage_client._stream")?
                .getattr(name)?
                .unbind(),
        )
    })?;
    cls.bind(py).call1((raw,))
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
        let Some(cf) = self.0.take() else { return };
        let cancel = move || {
            Python::try_attach(|py| {
                // Best effort: a no-op if the task already finished.
                let _ = cf.bind(py).call_method0(intern!(py, "cancel"));
            });
        };
        // On a runtime thread, never wait for the GIL there: the blocking
        // pool does it.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            drop(handle.spawn_blocking(cancel));
        } else {
            cancel();
        }
    }
}

// Process-wide: the module loads only in the main interpreter.
static ANEXT_BATCH: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static RUN_COROUTINE_THREADSAFE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Schedules `_anext_batch(it, BATCH_BYTES)` as a task on the loop of
/// `locals`, with the GIL held (call it from a blocking thread). The result
/// is sent through `tx`, converted by the done callback on the event loop.
fn schedule_batch(
    py: Python<'_>,
    it: &Py<PyAny>,
    locals: &TaskLocals,
    tx: tokio::sync::oneshot::Sender<PyResult<(Vec<Bytes>, bool)>>,
) -> PyResult<CancelOnDrop> {
    let anext_batch = ANEXT_BATCH.get_or_try_init(py, || {
        PyResult::Ok(
            py.import("object_storage_client._stream")?
                .getattr("_anext_batch")?
                .unbind(),
        )
    })?;
    let run_threadsafe = RUN_COROUTINE_THREADSAFE.get_or_try_init(py, || {
        PyResult::Ok(
            py.import("asyncio")?
                .getattr("run_coroutine_threadsafe")?
                .unbind(),
        )
    })?;
    let coro = anext_batch.bind(py).call1((it.bind(py), BATCH_BYTES))?;
    let cf = locals.context(py).call_method1(
        intern!(py, "run"),
        (run_threadsafe.bind(py), coro, locals.event_loop(py)),
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
}

/// Runs `_anext_batch(it, BATCH_BYTES)` as a task on the caller's event loop
/// and waits for its result: the converted chunks and whether the iterator is
/// exhausted. Dropping the returned future cancels that task, also when it is
/// dropped while the task is still being scheduled: the guard comes back
/// through the join handle and cancels on drop.
async fn anext_batch_cancellable(
    it: Arc<Py<PyAny>>,
    locals: TaskLocals,
) -> PyResult<(Vec<Bytes>, bool)> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    // Waiting for the GIL happens on the blocking pool, never on a worker.
    let mut guard = tokio::task::spawn_blocking(move || {
        Python::attach(|py| schedule_batch(py, &it, &locals, tx))
    })
    .await
    .map_err(|e| PyRuntimeError::new_err(format!("scheduling the next batch failed: {e}")))??;
    let res = rx.await;
    guard.0 = None; // the task has finished; do not take the GIL just to cancel it
    res.unwrap_or_else(|_| Err(PyRuntimeError::new_err("event loop closed")))
}

/// State of the stream over a Python async iterator.
struct PyChunks {
    iter: Arc<Py<PyAny>>,
    /// The caller's event loop and context, captured once.
    locals: TaskLocals,
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
pub fn py_async_iter_to_stream(
    iterable: &Bound<'_, PyAny>,
    locals: TaskLocals,
) -> PyResult<ByteStream> {
    let iter = Arc::new(iterable.call_method0("__aiter__")?.unbind());
    let state = PyChunks {
        iter,
        locals,
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
            match anext_batch_cancellable(Arc::clone(&st.iter), st.locals.clone()).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
    }

    /// Fills `shared` until the end, draining the slot after every fill.
    /// Returns the batches, and checks that a fill never holds more than one
    /// batch and never pulls more than one item past it (the carry).
    fn drain(shared: &Shared<String>, pulled: &AtomicUsize) -> Vec<Vec<String>> {
        let rt = runtime();
        let mut batches = Vec::new();
        loop {
            rt.block_on(shared.fill());
            let mut slot = shared.lock_slot();
            let held = slot.chunks.len();
            assert!(held <= BATCH_NAMES, "{held} names in one batch");
            assert!(slot.buffered <= BATCH_BYTES, "{} bytes", slot.buffered);
            let carried = usize::from(slot.carry.is_some());
            assert_eq!(
                pulled.load(Ordering::SeqCst),
                batches.iter().map(Vec::len).sum::<usize>() + held + carried,
                "the fill pulled more than it stored"
            );
            if held == 0 {
                assert!(matches!(slot.terminal, Some(Terminal::Eof)));
                return batches;
            }
            slot.buffered = 0;
            batches.push(std::mem::take(&mut slot.chunks).into());
        }
    }

    fn names_of(len: usize, count: usize, pulled: &Arc<AtomicUsize>) -> Shared<String> {
        let pulled = Arc::clone(pulled);
        Shared::new(
            futures::stream::iter((0..count).map(move |i| {
                pulled.fetch_add(1, Ordering::SeqCst);
                Ok(format!("{i:0len$}"))
            }))
            .boxed(),
        )
    }

    #[test]
    fn fill_holds_at_most_one_batch_of_names() {
        // Short names: the item cap binds.
        let pulled = Arc::new(AtomicUsize::new(0));
        let batches = drain(&names_of(8, 5_000, &pulled), &pulled);
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 5_000);
        assert_eq!(batches.iter().map(Vec::len).max(), Some(BATCH_NAMES));
        let flat: Vec<_> = batches.concat();
        assert!(
            flat.iter()
                .enumerate()
                .all(|(i, n)| *n == format!("{i:08}"))
        );
    }

    #[test]
    fn fill_holds_at_most_one_batch_of_name_bytes() {
        // 1000-byte names: the byte cap binds (262 of them fit in 256 KiB).
        let pulled = Arc::new(AtomicUsize::new(0));
        let batches = drain(&names_of(1000, 2_000, &pulled), &pulled);
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 2_000);
        assert_eq!(batches[0].len(), BATCH_BYTES / 1000);
        assert!(batches.iter().all(|b| b.len() <= BATCH_BYTES / 1000));
    }

    #[test]
    fn an_error_is_parked_behind_the_names_pulled_before_it() {
        let shared = Shared::new(
            futures::stream::iter([
                Ok("a".to_owned()),
                Ok("b".to_owned()),
                Err(crate::Error::InvalidInput("boom".into())),
            ])
            .boxed(),
        );
        runtime().block_on(shared.fill());
        let (names, failed) = {
            let slot = shared.lock_slot();
            (
                slot.chunks.clone(),
                matches!(slot.terminal, Some(Terminal::Err(_))),
            )
        };
        assert_eq!(names, ["a", "b"]);
        assert!(failed);
    }
}
