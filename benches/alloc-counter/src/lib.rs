//! A pass-through global allocator that counts calls, bytes and the peak of the
//! live heap. A binary installs it with `#[global_allocator]`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// The counting allocator; counters are process-wide.
pub struct Counting;

/// What a measured section allocated.
#[derive(Clone, Copy, Debug)]
pub struct Usage {
    pub allocs: usize,
    pub bytes: usize,
    /// Highest live heap during the section, above the heap at its start.
    pub peak_above_base: usize,
}

fn grow(by: usize) {
    let now = CURRENT.fetch_add(by, Relaxed) + by;
    PEAK.fetch_max(now, Relaxed);
}

/// Zeroes the counters and starts a new peak from the current live heap.
/// Returns the live heap, the base for [`finish`].
#[must_use]
pub fn start() -> usize {
    let base = CURRENT.load(Relaxed);
    ALLOCS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    PEAK.store(base, Relaxed);
    base
}

/// The usage since [`start`], which returned `base`.
#[must_use]
pub fn finish(base: usize) -> Usage {
    Usage {
        allocs: ALLOCS.load(Relaxed),
        bytes: BYTES.load(Relaxed),
        peak_above_base: PEAK.load(Relaxed).saturating_sub(base),
    }
}

// SAFETY: every method forwards to `System` with the caller's arguments and
// only updates atomic counters around the call.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size(), Relaxed);
        grow(l.size());
        unsafe { System.alloc(l) }
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size(), Relaxed);
        grow(l.size());
        unsafe { System.alloc_zeroed(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        CURRENT.fetch_sub(l.size(), Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(n, Relaxed);
        if n > l.size() {
            grow(n - l.size());
        } else {
            CURRENT.fetch_sub(l.size() - n, Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
}
