//! Test executable factory for unit testing
//!
//! This is a simple factory that doesn't require external dependencies,
//! used to test the executable factory system.

use crate::{ConfigFile, FactoryContext, TLogFSError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::{FileHandle, Result as TinyFSResult};

#[derive(Debug, Serialize, Deserialize)]
pub struct TestConfig {
    pub message: String,
    #[serde(default = "default_repeat")]
    pub repeat_count: usize,
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

/// Create a simple config file (no FS operations, runs inside lock)
async fn create_test_file(
    _config: Value,
    _context: FactoryContext,
) -> TinyFSResult<FileHandle> {
    // The config is stored by the caller - we just need to return a handle
    // In a real implementation, we'd read from the state, but for simplicity:
    let placeholder_config = b"message: test\nrepeat_count: 1\n".to_vec();
    Ok(ConfigFile::new(placeholder_config).create_handle())
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
    mode: crate::factory::ExecutionMode,
) -> Result<(), TLogFSError> {
    let parsed_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    println!("=== Test Factory Execution ===");
    println!("Message: {}", parsed_config.message);
    println!("Repeat count: {}", parsed_config.repeat_count);
    println!("Mode: {:?}", mode);
    println!("==============================");
    
    for i in 1..=parsed_config.repeat_count {
        println!("[{}] {}", i, parsed_config.message);
    }
    
    // For testing purposes, create a result in /tmp/test-executor-results
    // (We can't easily create directories under the config node without knowing its ID)
    let result_path = format!("/tmp/test-executor-result-{}.txt", context.parent_node_id);
    let result_content = format!(
        "Executed {} times\nMessage: {}\nMode: {:?}\n",
        parsed_config.repeat_count,
        parsed_config.message,
        mode
    );
    
    std::fs::write(&result_path, result_content)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to write result: {}", e))))?;
    
    log::info!("Test factory execution completed, result written to {}", result_path);
    Ok(())
}

// Register the test executable factory
crate::register_executable_factory!(
    name: "test-executor",
    description: "Test executable factory for unit testing",
    file: create_test_file,
    validate: validate_test_config,
    initialize: initialize_test,
    execute: execute_test
);
