//! Memory allocation tracking and debugging utilities
//!
//! This module uses eprintln! for debugging output, which is intentional
#![allow(clippy::print_stderr)]

use peak_alloc::PeakAlloc;
use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

const MAX_TRACKED_ALLOCS: usize = 100;
const TRACK_THRESHOLD_MB: usize = 50;

static LARGE_ALLOCS: [AtomicUsize; MAX_TRACKED_ALLOCS * 2] =
    [const { AtomicUsize::new(0) }; MAX_TRACKED_ALLOCS * 2];
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct PanicOnLargeAlloc {
    inner: PeakAlloc,
    max_size: usize,
}

impl PanicOnLargeAlloc {
    pub const fn new(max_mb: usize) -> Self {
        Self {
            inner: PeakAlloc,
            max_size: max_mb * 1024 * 1024,
        }
    }

    pub fn peak_usage_as_mb(&self) -> f32 {
        self.inner.peak_usage_as_mb()
    }

    pub fn print_large_allocs(&self) {
        let count = ALLOC_COUNT.load(Ordering::Relaxed);
        if count == 0 {
            return;
        }

        eprintln!("\n=== Large Allocations (>{}MB) ===", TRACK_THRESHOLD_MB);
        let limit = count.min(MAX_TRACKED_ALLOCS);
        for i in 0..limit {
            let size = LARGE_ALLOCS[i * 2].load(Ordering::Relaxed);
            let ptr = LARGE_ALLOCS[i * 2 + 1].load(Ordering::Relaxed);
            if size > 0 {
                eprintln!(
                    "  #{}: {:.2} MB @ 0x{:x}",
                    i + 1,
                    size as f64 / 1_048_576.0,
                    ptr
                );
            }
        }
        if count > MAX_TRACKED_ALLOCS {
            eprintln!(
                "  ... and {} more allocations not shown",
                count - MAX_TRACKED_ALLOCS
            );
        }
        eprintln!("=== Total: {} large allocations ===\n", count);
    }
}

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for PanicOnLargeAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() > self.max_size {
            panic!(
                "Allocation of {} bytes exceeds limit of {} bytes",
                layout.size(),
                self.max_size
            );
        }
        let ptr = unsafe { self.inner.alloc(layout) };

        // Track large allocations
        if layout.size() >= TRACK_THRESHOLD_MB * 1024 * 1024 {
            let idx = ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            if idx < MAX_TRACKED_ALLOCS {
                LARGE_ALLOCS[idx * 2].store(layout.size(), Ordering::Relaxed);
                LARGE_ALLOCS[idx * 2 + 1].store(ptr as usize, Ordering::Relaxed);
            }
        }

        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() > self.max_size {
            panic!(
                "Zeroed allocation of {} bytes exceeds limit of {} bytes",
                layout.size(),
                self.max_size
            );
        }
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };

        // Track large allocations
        if layout.size() >= TRACK_THRESHOLD_MB * 1024 * 1024 {
            let idx = ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            if idx < MAX_TRACKED_ALLOCS {
                LARGE_ALLOCS[idx * 2].store(layout.size(), Ordering::Relaxed);
                LARGE_ALLOCS[idx * 2 + 1].store(ptr as usize, Ordering::Relaxed);
            }
        }

        ptr
    }
}
