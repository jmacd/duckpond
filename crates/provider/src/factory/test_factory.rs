//! Test factories for unit testing the factory system
//!
//! Simple factories that don't require external dependencies,
//! used to test the factory infrastructure.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::Result as TinyFSResult;

/// Test factory configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct TestConfig {
    /// Message to print during execution
    pub message: String,
    /// Number of times to repeat the message
    #[serde(default = "default_repeat")]
    pub repeat_count: usize,
    /// Whether the test should intentionally fail
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
    _context: crate::FactoryContext,
) -> Result<(), tinyfs::Error> {
    let test_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    log::info!(
        "Initializing test factory with message: {}",
        test_config.message
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
    _context: crate::FactoryContext,
    ctx: crate::ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let parsed_config: TestConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Check if this execution should fail
    if parsed_config.fail {
        return Err(tinyfs::Error::Other(format!(
            "Test factory intentionally failed: {}",
            parsed_config.message
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
    if parsed_config.file_to_read.is_some() {
        log::warn!("File reading temporarily disabled during Step 7 migration");
    }

    // For testing purposes, create a result file on the host filesystem
    let result_path = "/tmp/test-executor-result.txt";
    let result_content = format!(
        "Executed {} times\nMessage: {}\nContext: {:?}\n",
        parsed_config.repeat_count, parsed_config.message, ctx
    );

    std::fs::write(result_path, result_content)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to write result: {}", e)))?;

    log::info!(
        "Test factory execution completed, result written to {}",
        result_path
    );
    Ok(())
}

// Register the test executable factory
crate::register_executable_factory!(
    name: "test-executor",
    description: "Test executable factory for unit testing",
    validate: validate_test_config,
    initialize: |config, context| async move {
        initialize_test(config, context).await
    },
    execute: |config, context, ctx| async move {
        execute_test(config, context, ctx).await
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
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<tinyfs::DirHandle> {
    let _test_config: tinyfs::testing::TestDirectoryConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Create an empty dynamic directory for testing
    // TestDirectoryConfig only has name/metadata, not actual file specs
    let dynamic_config = crate::DynamicDirConfig { entries: vec![] };
    let dynamic_dir = crate::DynamicDirDirectory::new(dynamic_config, context);
    Ok(dynamic_dir.create_handle())
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
fn create_test_file(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<tinyfs::FileHandle> {
    let parsed: tinyfs::testing::TestFileConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Return file handle with the configured content
    Ok(crate::ConfigFile::new(parsed.content.into_bytes()).create_handle())
}

crate::register_dynamic_factory!(
    name: "test-file",
    description: "Test file factory with required YAML config for unit testing",
    file: create_test_file,
    validate: validate_test_file_config
);
