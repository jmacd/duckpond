//! Test executable factory for unit testing
//!
//! This is a simple factory that doesn't require external dependencies,
//! used to test the executable factory system.

use crate::TLogFSError;
use provider::FactoryContext;
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
async fn initialize_test(config: Value, _context: provider::FactoryContext) -> Result<(), TLogFSError> {
    let parsed_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    log::info!(
        "Initializing test factory with message: {}",
        parsed_config.message
    );

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
    context: provider::FactoryContext,
    ctx: crate::factory::ExecutionContext,
) -> Result<(), TLogFSError> {
    let parsed_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    // Check if this execution should fail
    if parsed_config.fail {
        return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
            "Test factory intentionally failed: {}",
            parsed_config.message
        ))));
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
    if parsed_config.file_to_read.is_some() {
        log::warn!("File reading temporarily disabled during Step 7 migration");
    }

    // For testing purposes, create a result file on the host filesystem
    let result_path = "/tmp/test-executor-result.txt";
    let result_content = format!(
        "Executed {} times\nMessage: {}\nContext: {:?}\n",
        parsed_config.repeat_count, parsed_config.message, ctx
    );

    std::fs::write(result_path, result_content).map_err(|e| {
        TLogFSError::TinyFS(tinyfs::Error::Other(format!(
            "Failed to write result: {}",
            e
        )))
    })?;

    log::info!(
        "Test factory execution completed, result written to {}",
        result_path
    );
    Ok(())
}

// Register the test executable factory
// Note: No file creation function - config bytes ARE the file content for executable factories
// Note: Using provider::register_executable_factory! directly because this test factory
// already uses provider::FactoryContext (it doesn't need the legacy conversion wrapper)
provider::register_executable_factory!(
    name: "test-executor",
    description: "Test executable factory for unit testing",
    validate: validate_test_config,
    initialize: |config, context| async move {
        initialize_test(config, context).await.map_err(|e: TLogFSError| tinyfs::Error::Other(e.to_string()))
    },
    execute: |config, context, ctx| async move {
        execute_test(config, context, ctx).await.map_err(|e: TLogFSError| tinyfs::Error::Other(e.to_string()))
    }
);

// ============================================================================
// Test Directory Factory
// ============================================================================

/// Validate test directory configuration from YAML bytes
fn validate_test_dir_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config = tinyfs::testing::TestDirectoryConfig::from_yaml_bytes(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid test-dir config: {}", e)))?;

    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

/// Create a test directory handle
fn create_test_dir(
    _config: Value,
    _context: provider::FactoryContext,
) -> TinyFSResult<tinyfs::DirHandle> {
    // TODO: Implement test directory factory properly
    // For now, this test factory is not used by any critical tests
    Err(tinyfs::Error::Other("test-dir factory not yet implemented with provider::FactoryContext".to_string()))
}

crate::register_dynamic_factory!(
    name: "test-dir",
    description: "Test directory factory with required YAML config for unit testing",
    directory: create_test_dir,
    validate: validate_test_dir_config
);

// ============================================================================
// Test File Factory
// ============================================================================

/// Validate test file configuration from YAML bytes
fn validate_test_file_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config = tinyfs::testing::TestFileConfig::from_yaml_bytes(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid test-file config: {}", e)))?;

    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

/// Create a test file handle
fn create_test_file(config: Value, _context: provider::FactoryContext) -> TinyFSResult<tinyfs::FileHandle> {
    let parsed: tinyfs::testing::TestFileConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Return file handle with the configured content
    Ok(crate::factory::ConfigFile::new(parsed.content.into_bytes()).create_handle())
}

crate::register_dynamic_factory!(
    name: "test-file",
    description: "Test file factory with required YAML config for unit testing",
    file: create_test_file,
    validate: validate_test_file_config
);
