use peak_alloc::PeakAlloc;
use std::alloc::{GlobalAlloc, Layout};

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
}

unsafe impl GlobalAlloc for PanicOnLargeAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() > self.max_size {
            panic!(
                "Allocation of {} bytes exceeds limit of {} bytes",
                layout.size(),
                self.max_size
            );
        }
        unsafe { self.inner.alloc(layout) }
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
        unsafe { self.inner.alloc_zeroed(layout) }
    }
}
