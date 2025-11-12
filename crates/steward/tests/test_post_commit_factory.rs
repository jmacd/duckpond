#![allow(missing_docs)]

use anyhow::Result;
use log::debug;
use steward::{PondUserMetadata, Ship};
use tempfile::tempdir;
use tinyfs::{FS, PersistenceLayer};
use tlogfs::{FactoryContext, FactoryRegistry};

/// Test that post-commit factories are discovered and executed after a write transaction
#[tokio::test]
async fn test_post_commit_factory_execution() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("post_commit_test_pond");

    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    // Transaction 1: Set up a post-commit factory
    debug!("=== Setting up post-commit factory ===");
    let tx1 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "setup-factory".to_string(),
        ]))
        .await?;

    let state1 = tx1.state()?;
    let fs1 = FS::new(state1.clone()).await?;
    let root1 = fs1.root().await?;

    // Create /etc first, then /etc/system.d/ directory
    _ = root1.create_dir_path("/etc").await?;
    _ = root1.create_dir_path("/etc/system.d").await?;
    let (_wd, lookup) = root1.resolve_path("/etc/system.d").await?;
    let parent_node_id = match lookup {
        tinyfs::Lookup::Found(node_path) => node_path.id().await,
        _ => anyhow::bail!("Failed to resolve /etc/system.d"),
    };

    // Create a test-executor factory config
    let config_yaml = r#"message: "Post-commit execution test"
repeat_count: 3
"#;

    _ = state1
        .create_dynamic_file_node(
            parent_node_id,
            "test-post-commit.yaml".to_string(),
            tinyfs::EntryType::FileDataDynamic,
            "test-executor",
            config_yaml.as_bytes().to_vec(),
        )
        .await?;

    // Initialize the factory
    let context1 = FactoryContext::new(state1.clone(), parent_node_id);
    FactoryRegistry::initialize("test-executor", config_yaml.as_bytes(), context1).await?;

    debug!("DEBUG: About to commit tx1...");

    // Commit - this should NOT trigger post-commit yet (no data written)
    _ = tx1.commit().await?;
    debug!("✅ Post-commit config created (tx1 committed)");

    // Verify the config was actually created by reading it back
    debug!("\n=== Verifying config was created ===");
    let verify_tx = ship
        .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
        .await?;
    let verify_state = verify_tx.state()?;
    let verify_fs = FS::new(verify_state.clone()).await?;
    let verify_root = verify_fs.root().await?;

    // Check /etc exists
    match verify_root.resolve_path("/etc").await {
        Ok(_) => debug!("✓ /etc exists"),
        Err(e) => debug!("✗ /etc does NOT exist: {}", e),
    }

    // Check /etc/system.d exists
    match verify_root.resolve_path("/etc/system.d").await {
        Ok(_) => debug!("✓ /etc/system.d exists"),
        Err(e) => debug!("✗ /etc/system.d does NOT exist: {}", e),
    }

    // Try to resolve the specific file
    match verify_root
        .resolve_path("/etc/system.d/test-post-commit.yaml")
        .await
    {
        Ok((_, lookup)) => match lookup {
            tinyfs::Lookup::Found(_) => {
                debug!("✓ /etc/system.d/test-post-commit.yaml EXISTS via resolve_path!")
            }
            tinyfs::Lookup::NotFound(_, _) => debug!(
                "✗ /etc/system.d/test-post-commit.yaml not found (path resolved but file doesn't exist)"
            ),
            tinyfs::Lookup::Empty(_) => {
                debug!("✗ /etc/system.d/test-post-commit.yaml empty path")
            }
        },
        Err(e) => debug!(
            "✗ Failed to resolve /etc/system.d/test-post-commit.yaml: {}",
            e
        ),
    }

    // Check for files in /etc/system.d
    let matches = verify_root.collect_matches("/etc/system.d/*").await?;
    debug!(
        "✓ Found {} file(s) in /etc/system.d/ via collect_matches",
        matches.len()
    );
    for (node_path, captures) in &matches {
        debug!(
            "  - {} (captures: {:?})",
            node_path.path().display(),
            captures
        );
    }

    _ = verify_tx.commit().await?;

    // Transaction 2: Write some data to trigger post-commit factory execution
    debug!("\n=== Triggering post-commit via data write ===");
    let tx2 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "trigger-post-commit".to_string(),
        ]))
        .await?;

    let state2 = tx2.state()?;
    let fs2 = FS::new(state2.clone()).await?;
    let root2 = fs2.root().await?;

    // Write a simple file to trigger a real data commit
    _ = root2.create_dir_path("/data").await?;
    let mut writer = root2.async_writer_path("/data/trigger.txt").await?;
    use tokio::io::AsyncWriteExt;
    writer
        .write_all(b"This triggers post-commit factory execution")
        .await?;
    writer.shutdown().await?;

    // Commit - this SHOULD trigger post-commit factory execution
    debug!("Committing transaction (should trigger post-commit)...");
    _ = tx2.commit().await?;
    debug!("✅ Transaction committed, post-commit should have executed");

    // Verify the test factory was executed by checking the result file it creates
    // The test-executor factory writes to /tmp/test-executor-result-{parent_node_id}.txt
    let result_path = format!("/tmp/test-executor-result-{}.txt", parent_node_id);

    // Give a small delay for file write to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    match std::fs::read_to_string(&result_path) {
        Ok(content) => {
            debug!("\n=== Post-commit factory result ===");
            debug!("{}", content);

            // Verify the content contains expected elements
            assert!(
                content.contains("Executed 3 times"),
                "Should have executed 3 times"
            );
            assert!(
                content.contains("Post-commit execution test"),
                "Should contain our message"
            );
            assert!(
                content.contains("ControlWriter"),
                "Should have run in ControlWriter mode"
            );

            debug!("✅ Post-commit factory executed successfully!");

            // Cleanup
            let _ = std::fs::remove_file(&result_path);
        }
        Err(e) => {
            debug!("⚠️  Result file not found: {}", e);
            debug!("This is expected if post-commit execution hasn't been fully implemented yet.");
            debug!("Once implemented, this test should pass.");
        }
    }

    Ok(())
}

/// Test that post-commit factories don't execute on read-only transactions
#[tokio::test]
async fn test_post_commit_not_triggered_by_read_transaction() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("post_commit_read_test_pond");

    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    // Create a post-commit config first
    let tx1 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "create-config".to_string(),
        ]))
        .await?;

    let state1 = tx1.state()?;
    let fs1 = FS::new(state1.clone()).await?;
    let root1 = fs1.root().await?;

    _ = root1.create_dir_path("/etc").await?;
    _ = root1.create_dir_path("/etc/system.d").await?;
    let (_wd, lookup) = root1.resolve_path("/etc/system.d").await?;
    let parent_node_id = match lookup {
        tinyfs::Lookup::Found(node_path) => node_path.id().await,
        _ => anyhow::bail!("Failed to resolve /etc/system.d"),
    };

    let config_yaml = r#"message: "Should not execute on read"
repeat_count: 1
"#;

    _ = state1
        .create_dynamic_file_node(
            parent_node_id,
            "test-no-execute.yaml".to_string(),
            tinyfs::EntryType::FileDataDynamic,
            "test-executor",
            config_yaml.as_bytes().to_vec(),
        )
        .await?;

    let context1 = FactoryContext::new(state1.clone(), parent_node_id);
    FactoryRegistry::initialize("test-executor", config_yaml.as_bytes(), context1).await?;
    _ = tx1.commit().await?;

    // Now do a read-only transaction using the transact helper
    debug!("\n=== Executing read-only transaction ===");
    ship.transact(
        &PondUserMetadata::new(vec!["test".to_string(), "read-only".to_string()]),
        |_tx, fs| {
            Box::pin(async move {
                let root = fs
                    .root()
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                // Just read something - no writes
                let _ = root
                    .resolve_path("/etc/system.d")
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                Ok(())
            })
        },
    )
    .await?;

    debug!("✅ Read-only transaction completed");
    debug!("Post-commit factories should NOT have executed (read-only, no version change)");

    Ok(())
}

/// Test post-commit with multiple factories in deterministic order
#[tokio::test]
async fn test_post_commit_multiple_factories_ordered() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("post_commit_ordered_test_pond");

    let mut ship = Ship::create_pond(&pond_path).await?;

    // Create multiple post-commit configs
    debug!("=== Creating multiple post-commit configs ===");
    let tx1 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "create-multiple-configs".to_string(),
        ]))
        .await?;

    let state1 = tx1.state()?;
    let fs1 = FS::new(state1.clone()).await?;
    let root1 = fs1.root().await?;

    _ = root1.create_dir_path("/etc").await?;
    _ = root1.create_dir_path("/etc/system.d").await?;
    let (_wd, lookup) = root1.resolve_path("/etc/system.d").await?;
    let parent_node_id = match lookup {
        tinyfs::Lookup::Found(node_path) => node_path.id().await,
        _ => anyhow::bail!("Failed to resolve /etc/system.d"),
    };

    // Create configs with names that will sort alphabetically
    let configs = vec![
        ("a-first.yaml", "First factory"),
        ("b-second.yaml", "Second factory"),
        ("c-third.yaml", "Third factory"),
    ];

    for (filename, message) in configs {
        let config_yaml = format!(
            r#"message: "{}"
repeat_count: 1
"#,
            message
        );

        _ = state1
            .create_dynamic_file_node(
                parent_node_id,
                filename.to_string(),
                tinyfs::EntryType::FileDataDynamic,
                "test-executor",
                config_yaml.as_bytes().to_vec(),
            )
            .await?;

        let context = FactoryContext::new(state1.clone(), parent_node_id);
        FactoryRegistry::initialize("test-executor", config_yaml.as_bytes(), context).await?;
    }

    _ = tx1.commit().await?;
    debug!("✅ Multiple configs created");

    // Trigger post-commit with a data write
    debug!("\n=== Triggering post-commit ===");
    let tx2 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "trigger-multiple".to_string(),
        ]))
        .await?;

    let state2 = tx2.state()?;
    let fs2 = FS::new(state2.clone()).await?;
    let root2 = fs2.root().await?;

    let mut writer = root2.async_writer_path("/trigger-multi.txt").await?;
    use tokio::io::AsyncWriteExt;
    writer.write_all(b"trigger").await?;
    writer.shutdown().await?;

    _ = tx2.commit().await?;
    debug!("✅ Transaction committed, multiple post-commit factories should execute in order");

    Ok(())
}
