// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Run command - executes factory configurations from pond nodes or host files

use crate::common::{ShipContext, TargetContext, classify_target};
use anyhow::{Context, Result, anyhow};
use log::{debug, error};
use provider::FactoryRegistry;
use provider::registry::ExecutionContext;
use tokio::io::AsyncReadExt;

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

    // Classify the config path to determine pond vs host context
    let target = classify_target(config_path);

    match target {
        TargetContext::Host(_) => run_host_command(ship_context, config_path, extra_args).await,
        TargetContext::Pond(_) => run_pond_command(ship_context, config_path, extra_args).await,
    }
}

/// Execute a factory from a host filesystem config file.
///
/// The factory name comes from the URL scheme (e.g., `host+sitegen://site.yaml`
/// means factory=`sitegen`). The config bytes come from reading the host file.
/// No pond is required.
async fn run_host_command(
    ship_context: &ShipContext,
    config_path: &str,
    extra_args: Vec<String>,
) -> Result<()> {
    // Parse the URL to extract factory name from the scheme
    let url = provider::Url::parse(config_path)
        .map_err(|e| anyhow!("Failed to parse URL '{}': {}", config_path, e))?;

    let factory_name = url.scheme().to_string();

    // Validate that the scheme is actually a factory (not a format provider or builtin)
    match provider::SchemeRegistry::classify(&factory_name) {
        Some(provider::SchemeKind::Factory) => {}
        Some(provider::SchemeKind::Format) => {
            return Err(anyhow!(
                "'{}' is a format provider, not a factory. Use 'pond cat' for format providers.",
                factory_name
            ));
        }
        Some(provider::SchemeKind::Builtin) => {
            return Err(anyhow!(
                "'{}' is a builtin scheme, not a factory.",
                factory_name
            ));
        }
        None => {
            return Err(anyhow!(
                "Unknown scheme '{}'. Not a registered factory or format provider.",
                factory_name
            ));
        }
    }

    // Open host steward
    let mut ship = ship_context.open_host()?;
    let tx = ship
        .begin_write(&steward::PondUserMetadata::new(vec![
            "run".to_string(),
            config_path.to_string(),
        ]))
        .await?;

    // Read config bytes from host filesystem
    let host_path = url.path();
    let config_bytes = {
        let mut reader = tx
            .root()
            .await?
            .async_reader_path(host_path)
            .await
            .with_context(|| format!("Failed to open host file: {}", host_path))?;

        let mut buffer = Vec::new();
        _ = reader
            .read_to_end(&mut buffer)
            .await
            .with_context(|| format!("Failed to read host file: {}", host_path))?;
        buffer
    };

    log::debug!(
        "Executing host factory '{}' with {} bytes from '{}'",
        factory_name,
        config_bytes.len(),
        host_path
    );

    // Resolve the config node's FileID for the factory context.
    // On the host filesystem the FileID is deterministic from the path.
    let root = tx.root().await?;
    let (_parent_wd, lookup_result) = root
        .resolve_path(host_path)
        .await
        .with_context(|| format!("Failed to resolve host path: {}", host_path))?;

    let node_id = match lookup_result {
        tinyfs::Lookup::Found(node) => node.id(),
        _ => return Err(anyhow!("Host file not found: {}", host_path)),
    };

    // Build the factory context -- no pond metadata for host execution
    let provider_context = tx.provider_context()?;
    let factory_context = provider::FactoryContext::new(provider_context, node_id);

    // Execute the factory
    let args = if extra_args.is_empty() {
        vec![]
    } else {
        extra_args
    };

    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        &factory_name,
        &config_bytes,
        factory_context,
        ExecutionContext::pond_readwriter(args),
    )
    .await
    .map_err(|e| {
        error!("Factory '{}' execution error: {}", factory_name, e);
        use std::error::Error as StdError;
        if let Some(source) = StdError::source(&e) {
            error!("Caused by: {}", source);
        }
        e
    })
    .with_context(|| format!("Execution failed for factory '{}'", factory_name))?;

    _ = tx.commit().await?;
    debug!("[OK] Host factory execution complete");
    Ok(())
}

/// Execute a factory from a pond node (existing behavior).
///
/// The factory name comes from the oplog (set by `pond mknod`).
/// The config bytes come from the pond file contents.
async fn run_pond_command(
    ship_context: &ShipContext,
    config_path: &str,
    extra_args: Vec<String>,
) -> Result<()> {
    // Open pond
    let mut ship = ship_context.open_pond().await?;

    // Pre-load all factory modes and pond metadata before starting transaction
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

    match run_pond_command_impl(
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
            debug!("[OK] Execution complete");
            Ok(())
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of pond run command
async fn run_pond_command_impl(
    tx: &mut steward::Transaction<'_>,
    config_path: &str,
    extra_args: Vec<String>,
    all_factory_modes: std::collections::HashMap<String, String>,
    pond_metadata: tlogfs::PondMetadata,
) -> Result<()> {
    // Get filesystem root (guard derefs to FS)
    let root = tx.root().await?;

    // Get the node ID for the config file
    let (_parent_wd, lookup_result) = root
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

    // Get node ID for querying the factory
    let node_id = config_node.id();

    // Get the factory name from the oplog
    let factory_name = tx
        .get_factory_for_node(node_id)
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
    let provider_context = tx.provider_context()?;
    let factory_context =
        provider::FactoryContext::with_metadata(provider_context, node_id, pond_metadata);

    // Execute the configuration using the factory registry in write mode
    FactoryRegistry::execute::<tlogfs::TLogFSError>(
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

#[cfg(test)]
mod tests {
    use provider::{SchemeKind, SchemeRegistry};

    #[test]
    fn test_no_scheme_conflicts() {
        let conflicts = SchemeRegistry::find_conflicts();
        assert!(
            conflicts.is_empty(),
            "Scheme name conflicts detected: {:?}",
            conflicts
        );
    }

    #[test]
    fn test_executable_factories_classified() {
        // External executable factories (linked via cmd)
        assert_eq!(
            SchemeRegistry::classify("sitegen"),
            Some(SchemeKind::Factory)
        );
        assert_eq!(
            SchemeRegistry::classify("hydrovu"),
            Some(SchemeKind::Factory)
        );
        assert_eq!(
            SchemeRegistry::classify("remote"),
            Some(SchemeKind::Factory)
        );
    }

    #[test]
    fn test_factory_scheme_url_parsing() {
        // Verify that host+factory URLs parse correctly and the
        // scheme is the factory name
        let url = provider::Url::parse("host+sitegen:///site.yaml").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "sitegen");
        assert_eq!(url.path(), "/site.yaml");

        // Factory name is recognized in the unified registry
        assert_eq!(
            SchemeRegistry::classify(url.scheme()),
            Some(SchemeKind::Factory)
        );
    }

    #[test]
    fn test_format_not_accepted_as_factory() {
        // csv is a format provider, not a factory
        assert_eq!(SchemeRegistry::classify("csv"), Some(SchemeKind::Format));
        assert_ne!(SchemeRegistry::classify("csv"), Some(SchemeKind::Factory));
    }
}
