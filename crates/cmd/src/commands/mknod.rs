// CLI command for creating dynamic nodes
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use log::debug;
use std::fs;
use tlogfs::factory::FactoryRegistry;

/// Create a dynamic node in the pond using transaction guard pattern
pub async fn mknod_command(
    ship_context: &ShipContext,
    factory_type: &str,
    path: &str,
    config_path: &str,
    overwrite: bool,
) -> Result<()> {
    debug!("Creating dynamic node in pond: {path} with factory: {factory_type}");

    // Read config file early to validate it exists and is readable
    let config_bytes = fs::read(config_path)
        .map_err(|e| anyhow!("Failed to read config file '{}': {}", config_path, e))?;

    // Validate the factory and configuration early, get processed config
    let validated_config =
        FactoryRegistry::validate_config(factory_type, &config_bytes).map_err(|e| {
            anyhow!(
                "Invalid configuration for factory '{}': {}",
                factory_type,
                e
            )
        })?;

    // Convert validated config back to bytes for storage
    let processed_config_bytes = serde_yaml::to_string(&validated_config)
        .map_err(|e| anyhow!("Failed to serialize processed config: {}", e))?
        .into_bytes();

    // Create ship and use scoped transaction for mknod operation
    let mut ship = ship_context.open_pond().await?;
    let path_clone = path.to_string();
    let factory_type_clone = factory_type.to_string();

    ship.transact(
        vec![
            "mknod".to_string(),
            factory_type_clone.clone(),
            path_clone.clone(),
        ],
        |tx, fs| {
            Box::pin(async move {
                mknod_impl(
                    tx,
                    fs,
                    &path_clone,
                    &factory_type_clone,
                    processed_config_bytes.clone(),
                    overwrite,
                )
                .await
                .map_err(|e| {
                    steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                        tinyfs::Error::Other(e.to_string()),
                    ))
                })
            })
        },
    )
    .await
    .map_err(|e| anyhow!("mknod operation failed: {}", e))
}

async fn mknod_impl(
    tx: &steward::StewardTransactionGuard<'_>,
    fs: &tinyfs::FS,
    path: &str,
    factory_type: &str,
    config_bytes: Vec<u8>,
    overwrite: bool,
) -> Result<()> {
    let root = fs.root().await?;

    // Check what the factory supports and use the appropriate creation method
    let factory = tlogfs::factory::FactoryRegistry::get_factory(factory_type)
        .ok_or_else(|| anyhow!("Unknown factory type: {}", factory_type))?;

    // Determine what type of node to create based on factory capabilities
    let _node_path = if factory.create_directory.is_some() {
        // Factory supports directories
        if overwrite {
            // Use overwrite method directly to bypass parsing existing config
            root
                .create_dynamic_directory_path_with_overwrite(
                    path,
                    factory_type,
                    config_bytes.clone(),
                    overwrite,
                )
                .await?
        } else {
            // Normal creation path
            root.create_dynamic_directory_path(
            path,
            factory_type,
            config_bytes.clone(),
        ).await.map_err(|e| match e {
            tinyfs::Error::AlreadyExists(_) => {
                anyhow!("Dynamic node already exists at path '{}'. Use --overwrite to replace it.", path)
            },
            e => anyhow!("Failed to create dynamic directory: {}", e),
        })?
        }
    } else if factory.create_file.is_some() {
        // Factory supports files - use FileDataDynamic for executable configs
        if overwrite {
            // Use overwrite method directly to bypass parsing existing config
            root
                .create_dynamic_file_path_with_overwrite(
                    path,
                    tinyfs::EntryType::FileDataDynamic, // Executable configs are data files
                    factory_type,
                    config_bytes.clone(),
                    overwrite,
                )
                .await?
        } else {
            // Normal creation path
            root.create_dynamic_file_path(
            path,
            tinyfs::EntryType::FileDataDynamic, // Executable configs are data files
            factory_type,
            config_bytes.clone(),
        ).await.map_err(|e| match e {
            tinyfs::Error::AlreadyExists(_) => {
                anyhow!("Dynamic node already exists at path '{}'. Use --overwrite to replace it.", path)
            },
            e => anyhow!("Failed to create dynamic file: {}", e),
        })?
        }
    } else {
        return Err(anyhow!(
            "Factory '{}' does not support creating directories or files",
            factory_type
        ));
    };

    // Node is created, lock is released - now run factory initialization
    // Get the parent node ID for factory context
    let parent_path = std::path::Path::new(path).parent().unwrap_or(std::path::Path::new("/"));
    let parent_node_path = root.resolve_path(parent_path).await?;
    let parent_node_id = match parent_node_path.1 {
        tinyfs::Lookup::Found(node) => node.id().await,
        _ => return Err(anyhow!("Parent directory not found: {}", parent_path.display())),
    };

    // Create factory context with state from transaction guard
    let state = tx.state().map_err(|e| anyhow!("Failed to get state: {}", e))?;
    let context = tlogfs::factory::FactoryContext::new(state, parent_node_id);

    // Run factory initialization if it exists (e.g., create directories)
    tlogfs::factory::FactoryRegistry::initialize(factory_type, &config_bytes, context)
        .await
        .map_err(|e| anyhow!("Factory initialization failed: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    struct TestSetup {
        temp_dir: TempDir,
        ship_context: ShipContext,
        #[allow(dead_code)]
        pond_path: PathBuf,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(pond_path.clone()), init_args.clone());

            // Initialize pond
            init_command(&ship_context).await?;

            Ok(Self {
                temp_dir: temp_dir,
                ship_context,
                pond_path,
            })
        }

        /// Create template configuration file for testing
        fn create_template_config(&self) -> Result<PathBuf> {
            let config_path = self.temp_dir.path().join("template_config.yaml");
            let config_content = r#"in_pattern: "/base/*.tmpl"
out_pattern: "$0.txt"
template: |
  Test template content
  Generated file: {{ filename }}
"#;
            fs::write(&config_path, config_content)?;
            Ok(config_path)
        }

        /// Verify that the dynamic node exists in the pond
        async fn verify_node_exists(&self, pond_path: &str) -> Result<bool> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_transaction(steward::TransactionOptions::read(vec![
                    "verify_node".to_string(),
                ]))
                .await?;

            let result = {
                let fs = &*tx;
                let root = fs
                    .root()
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                root.exists(pond_path).await
            };

            tx.commit().await?;
            Ok(result)
        }
    }

    #[tokio::test]
    async fn test_mknod_nonexistent_factory() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a dummy config file
        let config_path = setup.temp_dir.path().join("dummy_config.json");
        fs::write(&config_path, "{}")?;

        // Try to create node with unknown factory
        let result = mknod_command(
            &setup.ship_context,
            "unknown_factory",
            "/test_node",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Unknown factory type")
                || error_msg.contains("Invalid configuration")
        );

        // Verify node was not created
        assert!(!setup.verify_node_exists("/test_node").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_invalid_config_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Try with nonexistent config file
        let result = mknod_command(
            &setup.ship_context,
            "hostmount",
            "/test_node",
            "/nonexistent/config.json",
            false,
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Failed to read config file"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_nested_path() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create parent directories first
        crate::commands::mkdir::mkdir_command(&setup.ship_context, "/deep/nested/path", true)
            .await?;

        // Create template config
        let config_path = setup.create_template_config()?;

        // Create node at nested path
        let result = mknod_command(
            &setup.ship_context,
            "template",
            "/deep/nested/path/templates",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(
            result.is_ok(),
            "mknod should succeed for nested paths: {:?}",
            result.err()
        );

        // Verify the nested node was created
        assert!(
            setup
                .verify_node_exists("/deep/nested/path/templates")
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_duplicate_path() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create template config
        let config_path = setup.create_template_config()?;

        // Create first node
        let result1 = mknod_command(
            &setup.ship_context,
            "template",
            "/duplicate_node",
            &config_path.to_string_lossy(),
            false,
        )
        .await;
        assert!(result1.is_ok());

        // Try to create second node at same path
        let result2 = mknod_command(
            &setup.ship_context,
            "template",
            "/duplicate_node",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        // This should fail since the path already exists
        assert!(result2.is_err());
        let error_msg = result2.unwrap_err().to_string();
        assert!(error_msg.contains("already exists") || error_msg.contains("conflict"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_transaction_rollback_on_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create invalid config that will fail during node creation
        let config_path = setup.temp_dir.path().join("malformed_config.yaml");
        fs::write(&config_path, "{ malformed yaml")?; // Invalid YAML

        // Try to create node
        let result = mknod_command(
            &setup.ship_context,
            "hostmount",
            "/test_rollback",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result.is_err());

        // Verify that no partial node was created (transaction rolled back)
        assert!(!setup.verify_node_exists("/test_rollback").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_overwrite_flag_error_message() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create template config
        let config_path = setup.create_template_config()?;

        // Create first node
        let result1 = mknod_command(
            &setup.ship_context,
            "template",
            "/test_overwrite",
            &config_path.to_string_lossy(),
            false,
        )
        .await;
        assert!(result1.is_ok());

        // Try to create second node at same path without --overwrite - should show helpful error
        let result2 = mknod_command(
            &setup.ship_context,
            "template",
            "/test_overwrite",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result2.is_err());
        let error_msg = result2.unwrap_err().to_string();
        assert!(error_msg.contains("Dynamic node already exists"));
        assert!(error_msg.contains("Use --overwrite to replace it"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_overwrite_success() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create first template config
        let config_path1 = setup.create_template_config()?;

        // Create first node
        let result1 = mknod_command(
            &setup.ship_context,
            "template",
            "/test_overwrite_success",
            &config_path1.to_string_lossy(),
            false,
        )
        .await;
        assert!(
            result1.is_ok(),
            "First mknod should succeed: {:?}",
            result1.err()
        );

        // Verify the node was created
        assert!(setup.verify_node_exists("/test_overwrite_success").await?);

        // Create second template config with different content
        let config_path2 = setup.temp_dir.path().join("template_config2.yaml");
        let config_content2 = r#"in_pattern: "/base/*.tmpl"
out_pattern: "$0.html"
template: |
  Updated template content
  New file: {{ filename }}
"#;
        fs::write(&config_path2, config_content2)?;

        // Overwrite the node with new configuration
        let result2 = mknod_command(
            &setup.ship_context,
            "template",
            "/test_overwrite_success",
            &config_path2.to_string_lossy(),
            true,
        )
        .await;
        assert!(
            result2.is_ok(),
            "Overwrite mknod should succeed: {:?}",
            result2.err()
        );

        // Verify the node still exists (configuration should be updated)
        assert!(setup.verify_node_exists("/test_overwrite_success").await?);

        Ok(())
    }
}
