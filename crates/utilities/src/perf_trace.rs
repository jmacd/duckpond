// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Performance tracing utilities for analyzing call patterns and performance
//!
//! This module provides tools to track function call patterns and performance metrics.
//! Particularly useful for analyzing database query patterns and optimizing hot paths.
//!
//! # Example
//!
//! ```no_run
//! use utilities::perf_trace::PerfTrace;
//!
//! async fn my_function(id: u64) -> Result<(), Box<dyn std::error::Error>> {
//!     let trace = PerfTrace::start("my_function");
//!     
//!     // Add context parameters
//!     trace.param("id", id);
//!     
//!     // Your code here
//!     
//!     // Metrics are automatically logged on drop
//!     Ok(())
//! }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Performance trace for a single function call
///
/// Automatically logs performance metrics when dropped.
/// Call patterns are tracked via PERF_TRACE output to stderr.
pub struct PerfTrace {
    call_num: u64,
    function_name: String,
    params: Vec<String>,
    start_time: Instant,
    metrics: Vec<(String, u64)>,
}

static CALL_COUNTER: AtomicU64 = AtomicU64::new(0);

impl PerfTrace {
    /// Start a new performance trace for the given function
    ///
    /// # Arguments
    /// * `function_name` - Name of the function being traced
    ///
    /// # Returns
    /// A new PerfTrace instance that will log metrics on drop
    #[must_use]
    pub fn start(function_name: impl Into<String>) -> Self {
        let call_num = CALL_COUNTER.fetch_add(1, Ordering::Relaxed);

        Self {
            call_num,
            function_name: function_name.into(),
            params: Vec::new(),
            start_time: Instant::now(),
            metrics: Vec::new(),
        }
    }

    /// Add a parameter to the trace
    ///
    /// Parameters are logged in the trace output for correlation analysis.
    ///
    /// # Arguments
    /// * `name` - Parameter name
    /// * `value` - Parameter value (any Display type)
    pub fn param(&mut self, name: impl Into<String>, value: impl std::fmt::Display) {
        self.params.push(format!("{}={}", name.into(), value));
    }

    /// Record a performance metric
    ///
    /// # Arguments
    /// * `name` - Metric name (e.g., "query_ms", "records_count")
    /// * `value` - Metric value
    pub fn metric(&mut self, name: impl Into<String>, value: u64) {
        self.metrics.push((name.into(), value));
    }

    /// Get the call number for this trace
    #[must_use]
    pub fn call_num(&self) -> u64 {
        self.call_num
    }

    /// Get the elapsed time in milliseconds since trace start
    #[must_use]
    pub fn elapsed_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// Get the elapsed time in microseconds since trace start
    #[must_use]
    pub fn elapsed_us(&self) -> u64 {
        self.start_time.elapsed().as_micros() as u64
    }
}

impl Drop for PerfTrace {
    fn drop(&mut self) {
        let elapsed_ms = self.elapsed_ms();

        // Format: PERF_TRACE|call_num|function|param1=val1|param2=val2|...|elapsed_ms|metric1=val1|metric2=val2|...
        let params_str = if self.params.is_empty() {
            String::new()
        } else {
            format!("|{}", self.params.join("|"))
        };

        let metrics_str = if self.metrics.is_empty() {
            String::new()
        } else {
            let metric_strs: Vec<String> = self
                .metrics
                .iter()
                .map(|(name, value)| format!("{}={}", name, value))
                .collect();
            format!("|{}", metric_strs.join("|"))
        };

        log::debug!(
            "PERF_TRACE|{}|{}{}|elapsed_ms={}{}",
            self.call_num,
            self.function_name,
            params_str,
            elapsed_ms,
            metrics_str
        );
    }
}

/// Extract caller function name from backtrace
///
/// Attempts to find the calling function by analyzing the backtrace.
/// This is useful for automatic caller identification in performance traces.
///
/// # Arguments
/// * `module_prefix` - Module prefix to search for (e.g., "tlogfs::persistence")
/// * `exclude_function` - Function name to skip (usually the tracing function itself)
///
/// # Returns
/// Function name if found, "unknown" otherwise
#[must_use]
pub fn extract_caller(module_prefix: &str, exclude_function: &str) -> String {
    let backtrace = std::backtrace::Backtrace::capture();
    let backtrace_str = backtrace.to_string();

    // Find the first frame that matches the module but isn't the excluded function
    backtrace_str
        .lines()
        .find(|line| line.contains(module_prefix) && !line.contains(exclude_function))
        .and_then(|line| {
            // Extract function name - look for pattern like "module::prefix::function_name"
            if let Some(pos) = line.find(module_prefix) {
                let rest = &line[pos + module_prefix.len()..];
                // Take function name up to :: or {{
                let end = rest
                    .find("::")
                    .or_else(|| rest.find("{{"))
                    .unwrap_or(rest.len());
                Some(rest[..end].to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perf_trace_basic() {
        let mut trace = PerfTrace::start("test_function");
        trace.param("id", 123);
        trace.param("name", "test");
        trace.metric("records", 42);

        // Trace will log on drop
        drop(trace);
    }

    #[test]
    fn test_perf_trace_timing() {
        let trace = PerfTrace::start("timing_test");
        std::thread::sleep(std::time::Duration::from_millis(10));

        let elapsed = trace.elapsed_ms();
        assert!(elapsed >= 10, "Expected at least 10ms, got {}", elapsed);
    }
}
