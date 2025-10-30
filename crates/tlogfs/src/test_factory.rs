//! Test executable factory for unit testing
//!
//! This is a simple factory that doesn't require external dependencies,
//! used to test the executable factory system.

use crate::{FactoryContext, TLogFSError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::Result as TinyFSResult;

#[derive(Debug, Serialize, Deserialize)]
pub struct TestConfig {
    pub message: String,
    #[serde(default = "default_repeat")]
    pub repeat_count: usize,
    #[serde(default)]
    pub fail: bool,
    /// Optional: Path to file to read and verify
    #[serde(default)]
    pub file_to_read: Option<String>,
    /// Optional: Expected content of file (for verification)
    #[serde(default)]
    pub expected_content: Option<String>,
}

fn default_repeat() -> usize {
    1
}

/// Validate test configuration from YAML bytes
fn validate_test_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config: TestConfig = serde_yaml::from_slice(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid test config: {}", e)))?;
    
    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

/// Initialize test factory - creates a simple output directory
async fn initialize_test(
    config: Value,
    _context: FactoryContext,
) -> Result<(), TLogFSError> {
    let parsed_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    log::info!("Initializing test factory with message: {}", parsed_config.message);
    
    // The parent_node_id in context is the parent directory (e.g., /configs)
    // We don't have the config file's node ID here, so we can't create subdirectories
    // This is a limitation of the initialize hook - it runs after creation but doesn't get the new node ID
    // For now, skip directory creation in initialize and do it in execute
    log::info!("Test factory initialized (directory creation deferred to execution)");
    Ok(())
}

/// Execute test factory - prints message multiple times
async fn execute_test(
    config: Value,
    context: FactoryContext,
    ctx: crate::factory::ExecutionContext,
) -> Result<(), TLogFSError> {
    let parsed_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    // Check if this execution should fail
    if parsed_config.fail {
        return Err(TLogFSError::TinyFS(tinyfs::Error::Other(
            format!("Test factory intentionally failed: {}", parsed_config.message)
        )));
    }
    
    log::debug!("=== Test Factory Execution ===");
    log::debug!("Message: {}", parsed_config.message);
    log::debug!("Repeat count: {}", parsed_config.repeat_count);
    log::debug!("Context: {:?}", ctx);
    log::debug!("==============================");
    
    for i in 1..=parsed_config.repeat_count {
        log::debug!("[{}] {}", i, parsed_config.message);
    }
    
    // If file_to_read is specified, read and verify it
    if let Some(file_path) = &parsed_config.file_to_read {
        log::info!("Test factory reading file: {}", file_path);
        
        // Create FS from context state to read the file
        let fs = tinyfs::FS::new(context.state.clone()).await
            .map_err(|e| TLogFSError::TinyFS(e))?;
        let root = fs.root().await
            .map_err(|e| TLogFSError::TinyFS(e))?;
        
        // Read the file
        let content_bytes = root.read_file_path_to_vec(file_path).await
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(
                format!("Failed to read file {}: {}", file_path, e)
            )))?;
        
        let content = String::from_utf8_lossy(&content_bytes).to_string();
        log::info!("Test factory read content from {}: {:?}", file_path, content);
        
        // Verify content if expected_content is specified
        if let Some(expected) = &parsed_config.expected_content {
            if content != *expected {
                return Err(TLogFSError::TinyFS(tinyfs::Error::Other(
                    format!("Content mismatch! Expected: {:?}, Got: {:?}", expected, content)
                )));
            }
            log::info!("Test factory verified content matches expected value");
        }
    }
    
    // For testing purposes, create a result in /tmp/test-executor-results
    // (We can't easily create directories under the config node without knowing its ID)
    let result_path = format!("/tmp/test-executor-result-{}.txt", context.parent_node_id);
    let result_content = format!(
        "Executed {} times\nMessage: {}\nContext: {:?}\n",
        parsed_config.repeat_count,
        parsed_config.message,
        ctx
    );
    
    std::fs::write(&result_path, result_content)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to write result: {}", e))))?;
    
    log::info!("Test factory execution completed, result written to {}", result_path);
    Ok(())
}

// Register the test executable factory
// Note: No file creation function - config bytes ARE the file content for executable factories
crate::register_executable_factory!(
    name: "test-executor",
    description: "Test executable factory for unit testing",
    validate: validate_test_config,
    initialize: initialize_test,
    execute: execute_test
);
