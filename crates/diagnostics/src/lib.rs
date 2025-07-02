//! Simple diagnostics library for DuckPond project
//! 
//! Provides lightweight, configurable logging across all crates in the project.
//! 
//! Usage:
//! - Set DUCKPOND_LOG=off (default) - no logs
//! - Set DUCKPOND_LOG=info - basic operation logs  
//! - Set DUCKPOND_LOG=debug - detailed diagnostic logs
//!
//! Example:
//! ```rust
//! use diagnostics::{log_info, log_debug, init_diagnostics};
//! 
//! // Call once at startup
//! init_diagnostics();
//! 
//! // Use throughout your code
//! log_info!("Starting operation with {count} items", count = items.len());
//! log_debug!("Processing item: {item:?}", item = item);
//! ```

use std::sync::Once;

// Re-export emit so macros can use it
pub use emit;

static INIT: Once = Once::new();

/// Initialize diagnostics based on DUCKPOND_LOG environment variable
/// 
/// This should be called once at application startup. It's safe to call
/// multiple times - subsequent calls will be ignored.
pub fn init_diagnostics() {
    INIT.call_once(|| {
        let log_level = std::env::var("DUCKPOND_LOG").unwrap_or_else(|_| "off".to_string());
        
        match log_level.as_str() {
            "off" => {
                // No logging setup - emit will be silent
            }
            "info" => {
                let _rt = emit::setup()
                    .emit_to(emit_term::stdout())
                    .emit_when(emit::level::min_filter(emit::Level::Info))
                    .init();
                std::mem::forget(_rt); // Keep runtime alive for application lifetime
            }
            "debug" => {
                let _rt = emit::setup()
                    .emit_to(emit_term::stdout())
                    .emit_when(emit::level::min_filter(emit::Level::Debug))
                    .init();
                std::mem::forget(_rt); // Keep runtime alive for application lifetime
            }
            _ => {
                eprintln!("Warning: Unknown DUCKPOND_LOG value '{}', using 'off'", log_level);
            }
        }
    });
}

/// Log basic operations (queries, commits, filesystem operations, etc.)
/// 
/// Use this for operations that users might want to see in normal usage.
/// Examples: "Started transaction", "Committed 5 records", "Opened file"
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::emit::info!($($arg)*)
    };
}

/// Log detailed diagnostics (record counts, processing steps, internal state, etc.)
/// 
/// Use this for detailed information useful for debugging and performance analysis.
/// Examples: "Processing batch with 1024 records", "Cache hit for key", "SQL: SELECT ..."
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::emit::debug!($($arg)*)
    };
}

/// Re-export the init function for convenience
pub use init_diagnostics as init;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_is_safe_to_call_multiple_times() {
        // Should not panic when called multiple times
        init_diagnostics();
        init_diagnostics();
        init_diagnostics();
    }

    #[test]
    fn test_macros_compile() {
        // These should compile without errors
        log_info!("Test message");
        log_debug!("Debug message with {value}", value: 42);
    }
}
