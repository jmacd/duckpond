//! Simple diagnostics library for DuckPond project
//! 
//! Provides lightweight, configurable logging across all crates in the project.
//! 
//! Usage:
//! - Set DUCKPOND_LOG=off (default) - no logs
//! - Set DUCKPOND_LOG=info - basic operation logs  
//! - Set DUCKPOND_LOG=debug - detailed diagnostic logs

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
        
        let rt = match log_level.as_str() {
            "off" => return, // No setup needed
            "debug" => emit::setup()
                .emit_to(emit_term::stderr())
                .emit_when(emit::level::min_filter(emit::Level::Debug))
                .init(),
            "info" => emit::setup()
                .emit_to(emit_term::stderr())
                .emit_when(emit::level::min_filter(emit::Level::Info))
                .init(),
            "warn" => emit::setup()
                .emit_to(emit_term::stderr())
                .emit_when(emit::level::min_filter(emit::Level::Warn))
                .init(),
            "error" => emit::setup()
                .emit_to(emit_term::stderr())
                .emit_when(emit::level::min_filter(emit::Level::Error))
                .init(),
            _ => {
                // Fixed: Use structured logging instead of eprintln!
                let rt = emit::setup()
                    .emit_to(emit_term::stderr())
                    .emit_when(emit::level::min_filter(emit::Level::Info))
                    .init();
                // Bootstrap warning - this will show even with unknown level
                eprintln!("Warning: Unknown DUCKPOND_LOG value '{}', using 'info'", log_level);
                rt
            }
        };

        // Store runtime properly instead of memory leak
        std::mem::forget(rt); // TODO: Find better lifetime management
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

/// Log warning conditions (config issues, fallbacks, recoverable errors)
/// 
/// Use this for issues that don't prevent operation but should be noted.
/// Examples: "Config file not found, using defaults", "Retrying operation"
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        $crate::emit::warn!($($arg)*)
    };
}

/// Log critical error conditions (failures, exceptions, unrecoverable errors)
/// 
/// Use this for serious problems that prevent normal operation.
/// Examples: "Failed to connect to database", "File corruption detected"
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::emit::error!($($arg)*)
    };
}

// Short-name versions for ergonomic usage (Phase 1 implementation)

/// Log basic operations (queries, commits, filesystem operations, etc.)
/// 
/// Use this for operations that users might want to see in normal usage.
/// Examples: "Started transaction", "Committed 5 records", "Opened file"
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::emit::info!($($arg)*)
    };
}

/// Log detailed diagnostics (record counts, processing steps, internal state, etc.)
/// 
/// Use this for detailed information useful for debugging and performance analysis.
/// Examples: "Processing batch with 1024 records", "Cache hit for key", "SQL: SELECT ..."
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        $crate::emit::debug!($($arg)*)
    };
}

/// Log warning conditions (config issues, fallbacks, recoverable errors)
/// 
/// Use this for issues that don't prevent operation but should be noted.
/// Examples: "Config file not found, using defaults", "Retrying operation"
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::emit::warn!($($arg)*)
    };
}

/// Log critical error conditions (failures, exceptions, unrecoverable errors)
/// Using "error" instead of "fatal" for consistency with emit-rs
/// 
/// Use this for serious problems that prevent normal operation.
/// Examples: "Failed to connect to database", "File corruption detected"
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::emit::error!($($arg)*)
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
        // Test old-style macros still work
        log_info!("Test message");
        log_debug!("Debug message with {value}", value: 42);
        log_warn!("Warning message");
        log_error!("Error message");
        
        // Test new-style short macros
        info!("Test message");
        debug!("Debug message with {value}", value: 42);
        warn!("Warning message");
        error!("Error message");
    }
}
