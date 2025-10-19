//! Run command - executes run configurations stored as pond nodes
//!
//! This command reads a configuration file from the pond, extracts the factory name
//! from the file's metadata, and executes it within a single transaction.
//!
//! Example:
//!   pond run /configs/hydrovu-collector
//!
//! The configuration file must have been created with a factory that supports execution
//! (registered with register_executable_factory!).

use crate::common::ShipContext;
use anyhow::{Context, Result, anyhow};
use tokio::io::AsyncReadExt;

/// Execute a run configuration
pub async fn run_command(ship_context: &ShipContext, config_path: &str) -> Result<()> {
    log::debug!("Running configuration: {}", config_path);

    // Open pond and begin single transaction
    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_transaction(
            steward::TransactionOptions::write(vec!["run".to_string(), config_path.to_string()])
        )
        .await?;

    // Get filesystem root
    let fs = tinyfs::FS::new(tx.state()?).await?;
    let root = fs.root().await?;

    // Get the node ID for the config file
    let (parent_wd, lookup_result) = root
        .resolve_path(config_path)
        .await
        .with_context(|| format!("Failed to resolve path: {}", config_path))?;

    let config_node = match lookup_result {
        tinyfs::Lookup::Found(node) => node,
        tinyfs::Lookup::NotFound(_, _) => {
            return Err(anyhow!("Configuration file not found: {}", config_path));
        }
        tinyfs::Lookup::Empty(_) => {
            return Err(anyhow!("Invalid path: {}", config_path));
        }
    };

    // Get node and parent IDs for querying the factory
    let node_id = config_node.borrow().await.id();
    let part_id = parent_wd.node_path().id().await;

    // Get the factory name from the oplog
    let factory_name = tx
        .state()?
        .get_factory_for_node(node_id, part_id)
        .await
        .with_context(|| format!("Failed to get factory for: {}", config_path))?
        .ok_or_else(|| anyhow!("Configuration file has no associated factory: {}", config_path))?;

    // Read the configuration file contents
    let config_bytes = {
        let mut reader = root
            .async_reader_path(config_path)
            .await
            .with_context(|| format!("Failed to open file: {}", config_path))?;

        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .await
            .with_context(|| format!("Failed to read file: {}", config_path))?;
        buffer
    };

    log::debug!(
        "Executing configuration with factory '{}' ({} bytes)",
        factory_name,
        config_bytes.len()
    );

    // Create factory context with state and parent node ID
    let factory_context = tlogfs::factory::FactoryContext::new(tx.state()?, node_id);

    // Execute the configuration using the factory registry in write mode
    tlogfs::factory::FactoryRegistry::execute(
        &factory_name,
        &config_bytes,
        factory_context,
        tlogfs::factory::ExecutionMode::InTransactionWriter,
    )
        .await
        .with_context(|| format!("Execution failed for factory '{}'", factory_name))?;

    // Commit the transaction
    tx.commit().await?;

    log::debug!("Configuration executed successfully");
    println!("✓ Execution complete");

    Ok(())
}
