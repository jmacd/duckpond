//! Run command - executes run configurations stored as pond nodes

use crate::common::ShipContext;
use anyhow::{Context, Result, anyhow};
use tlogfs::factory::ExecutionContext;
use tokio::io::AsyncReadExt;
use log::{debug, error};

/// Execute a run configuration
pub async fn run_command(
    ship_context: &ShipContext,
    config_path: &str,
    extra_args: Vec<String>,
) -> Result<()> {
    log::debug!(
        "Running configuration: {} with args: {:?}",
        config_path,
        extra_args
    );

    // Open pond
    let mut ship = ship_context.open_pond().await?;

    // Pre-load all factory modes and pond metadata before starting transaction
    // This is a small amount of data, so we just load it upfront
    let all_factory_modes = ship.control_table().factory_modes().clone();

    let pond_metadata = ship.control_table().get_pond_metadata().clone();

    log::debug!("Loaded factory modes: {:?}", all_factory_modes);

    // Start write transaction for the entire operation
    let mut tx = ship
        .begin_write(&steward::PondUserMetadata::new(vec![
            "run".to_string(),
            config_path.to_string(),
        ]))
        .await?;

    match run_command_impl(
        &mut tx,
        config_path,
        extra_args,
        all_factory_modes,
        pond_metadata,
    )
    .await
    {
        Ok(()) => {
            _ = tx.commit().await?;
            log::debug!("Configuration executed successfully");
            debug!("✓ Execution complete");
            Ok(())
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of run command
async fn run_command_impl(
    tx: &mut steward::StewardTransactionGuard<'_>,
    config_path: &str,
    extra_args: Vec<String>,
    all_factory_modes: std::collections::HashMap<String, String>,
    pond_metadata: tlogfs::PondMetadata,
) -> Result<()> {
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
        .ok_or_else(|| {
            anyhow!(
                "Configuration file has no associated factory: {}",
                config_path
            )
        })?;

    // Read the configuration file contents
    let config_bytes = {
        let mut reader = root
            .async_reader_path(config_path)
            .await
            .with_context(|| format!("Failed to open file: {}", config_path))?;

        let mut buffer = Vec::new();
        _ = reader
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

    // Build args: if extra_args provided, use those; otherwise use factory mode from control table
    let args = if !extra_args.is_empty() {
        log::debug!(
            "Factory '{}' using explicit args: {:?}",
            factory_name,
            extra_args
        );
        extra_args
    } else {
        all_factory_modes
            .get(&factory_name)
            .map(|mode| {
                log::debug!("Factory '{}' has mode: {}", factory_name, mode);
                vec![mode.clone()]
            })
            .unwrap_or_else(|| {
                log::debug!(
                    "Factory '{}' has no mode set, using empty args (will default to 'push')",
                    factory_name
                );
                vec![]
            })
    };

    // Create factory context with pond metadata (pre-loaded above)
    let factory_context =
        tlogfs::factory::FactoryContext::with_metadata(tx.state()?, node_id, pond_metadata);

    // Execute the configuration using the factory registry in write mode
    tlogfs::factory::FactoryRegistry::execute(
        &factory_name,
        &config_bytes,
        factory_context,
        ExecutionContext::pond_readwriter(args),
    )
    .await
    .map_err(|e| {
        // Print the underlying error details before wrapping
        error!("Factory '{}' execution error: {}", factory_name, e);
        // Print the full error chain if available
        use std::error::Error as StdError;
        if let Some(source) = StdError::source(&e) {
            error!("Caused by: {}", source);
        }
        e
    })
    .with_context(|| format!("Execution failed for factory '{}'", factory_name))?;

    Ok(())
}
