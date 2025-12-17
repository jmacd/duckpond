//! Integration tests for executable factory system
//!
//! These tests verify the complete workflow:
//! 1. Create config with factory-based dynamic file creation
//! 2. Execute config with factory execution
//! 3. Verify output files and behavior

use cmd::common::ShipContext;
use log::debug;
use provider::registry::ExecutionContext;
use std::collections::HashMap;
use steward::PondUserMetadata;
use tempfile::TempDir;
use tinyfs::FS;
use tlogfs::FactoryRegistry;

/// Helper to create a test ship and workspace
async fn setup_test_ship() -> (ShipContext, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let pond_path = temp_dir.path().join("test_pond");

    let ship_context = ShipContext {
        pond_path: Some(pond_path.clone()),
        original_args: vec!["pond".to_string(), "init".to_string()],
        template_variables: HashMap::new(),
    };

    // Initialize the pond
    cmd::commands::init::init_command(&ship_context, None, None)
        .await
        .expect("Failed to initialize pond");

    (ship_context, temp_dir)
}

/// Helper to create a test config file using the factory within a single transaction
async fn create_test_config(
    ship_context: &ShipContext,
    path: &str,
    config_yaml: &str,
) -> anyhow::Result<()> {
    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "mknod".to_string(),
            "test-executor".to_string(),
            path.to_string(),
        ]))
        .await?;

    let state = tx.state()?;
    let fs = FS::new(state.clone()).await?;
    let root = fs.root().await?;

    // Create parent directories
    let parts: Vec<&str> = path.rsplitn(2, '/').collect();
    let parent_path = if parts.len() == 2 && !parts[1].is_empty() {
        parts[1]
    } else {
        "/"
    };

    // Create parent directory first
    if parent_path != "/" {
        _ = root.create_dir_path(parent_path).await?;
    }

    // Resolve parent after creation
    let (parent_wd, _) = root.resolve_path(parent_path).await?;
    let parent_node_id = parent_wd.node_path().id();

    // Create dynamic node using path-based API
    let _node_path = root
        .create_dynamic_path(
            path,
            tinyfs::EntryType::FileDataDynamic,
            "test-executor",
            config_yaml.as_bytes().to_vec(),
        )
        .await?;

    // Initialize the factory
    let provider_context = state.as_provider_context();
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "test-executor",
        config_yaml.as_bytes(),
        context,
    )
    .await?;

    _ = tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_create_and_read_test_config() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    // Create a test config
    let config_yaml = r#"
message: "Hello from test!"
repeat_count: 3
"#;

    create_test_config(&ship_context, "/configs/test1", config_yaml)
        .await
        .expect("Failed to create test config");

    // Verify the config was created - simple check that it doesn't error
    _ = create_test_config(&ship_context, "/configs/test1", config_yaml)
        .await
        .expect_err("Should fail on duplicate path");
}

#[tokio::test]
async fn test_execute_test_factory() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    let config_yaml = r#"
message: "Execute test"
repeat_count: 5
"#;

    debug!("Creating and executing config in single transaction...");

    // Do BOTH create and execute in a SINGLE transaction (following system patterns)
    let mut ship = ship_context.open_pond().await.expect("Failed to open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "create-and-execute".to_string(),
        ]))
        .await
        .expect("Failed to begin transaction");

    let state = tx.state().expect("Failed to get state");
    let fs = FS::new(state.clone()).await.expect("Failed to create FS");
    let root = fs.root().await.expect("Failed to get root");

    // Create parent directory and config node
    _ = root
        .create_dir_path("/configs")
        .await
        .expect("Failed to create configs dir");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("Failed to resolve configs");
    let parent_node_id = parent_wd.node_path().id();

    // Create dynamic node using path-based API
    let _node_path = root
        .create_dynamic_path(
            "/configs/test3",
            tinyfs::EntryType::FileDataDynamic,
            "test-executor",
            config_yaml.as_bytes().to_vec(),
        )
        .await
        .expect("Failed to create config node");

    // Initialize and execute in the SAME transaction
    let provider_context = state.as_provider_context();
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "test-executor",
        config_yaml.as_bytes(),
        context.clone(),
    )
    .await
    .expect("Failed to initialize factory");

    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "test-executor",
        config_yaml.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await
    .expect("Failed to execute factory");

    _ = tx.commit().await.expect("Failed to commit transaction");

    debug!("Successfully created and executed in single transaction!");
}

#[tokio::test]
async fn test_invalid_config_rejected() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    let invalid_yaml = r#"
this is not valid yaml: [[[
"#;

    let result = create_test_config(&ship_context, "/configs/invalid", invalid_yaml).await;

    assert!(result.is_err(), "Invalid config should be rejected");
}
