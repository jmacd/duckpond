// CLI command for creating dynamic nodes
use std::fs;
use tlogfs::factory::FactoryRegistry;
use anyhow::{Result, anyhow};
use log::debug;
use crate::common::ShipContext;

/// Create a dynamic node in the pond using transaction guard pattern
pub async fn mknod_command(ship_context: &ShipContext, factory_type: &str, path: &str, config_path: &str) -> Result<()> {
    debug!("Creating dynamic node in pond: {path} with factory: {factory_type}");

    // Read config file early to validate it exists and is readable
    let config_bytes = fs::read(config_path)
        .map_err(|e| anyhow!("Failed to read config file '{}': {}", config_path, e))?;
    
    // Validate the factory and configuration early
    FactoryRegistry::validate_config(factory_type, &config_bytes)
        .map_err(|e| anyhow!("Invalid configuration for factory '{}': {}", factory_type, e))?;

    // Create ship and use scoped transaction for mknod operation
    let mut ship = ship_context.open_pond().await?;
    let path_clone = path.to_string();
    let factory_type_clone = factory_type.to_string();
    
    ship.transact(
        vec!["mknod".to_string(), factory_type_clone.clone(), path_clone.clone()],
        |_tx, fs| Box::pin(async move {
            mknod_impl(fs, &path_clone, &factory_type_clone, config_bytes.clone()).await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string()))))
        })
    ).await
        .map_err(|e| anyhow!("mknod operation failed: {}", e))
}

async fn mknod_impl(fs: &tinyfs::FS, path: &str, factory_type: &str, config_bytes: Vec<u8>) -> Result<()> {
    let root = fs.root().await?;
    
    // Check what the factory supports and use the appropriate creation method
    let factory = tlogfs::factory::FactoryRegistry::get_factory(factory_type)
        .ok_or_else(|| anyhow!("Unknown factory type: {}", factory_type))?;
    
    if factory.create_directory_with_context.is_some() && factory.create_file_with_context.is_none() {
        // Factory only supports directories
        let _node_path = root.create_dynamic_directory_path(
            path,
            factory_type,
            config_bytes,
        ).await?;
    } else if factory.create_file_with_context.is_some() && factory.create_directory_with_context.is_none() {
        // Factory only supports files
        let _node_path = root.create_dynamic_file_path(
            path,
            tinyfs::EntryType::FileTable, // SQL-derived files are table-like
            factory_type,
            config_bytes,
        ).await?;
    } else if factory.create_directory_with_context.is_some() && factory.create_file_with_context.is_some() {
        // Factory supports both - default to directory for backward compatibility
        let _node_path = root.create_dynamic_directory_path(
            path,
            factory_type,
            config_bytes,
        ).await?;
    } else {
        return Err(anyhow!("Factory '{}' does not support creating directories or files", factory_type));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;
    use std::path::PathBuf;
    use crate::common::ShipContext;
    use crate::commands::init::init_command;

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

        /// Create a host directory for hostmount testing
        fn create_host_dir(&self, dir_name: &str) -> Result<PathBuf> {
            let host_dir = self.temp_dir.path().join(dir_name);
            fs::create_dir_all(&host_dir)?;
            Ok(host_dir)
        }

        /// Create a host file for hostmount testing
        #[allow(dead_code)]
        fn create_host_file(&self, file_name: &str, content: &str) -> Result<PathBuf> {
            let host_file = self.temp_dir.path().join(file_name);
            fs::write(&host_file, content)?;
            Ok(host_file)
        }

        /// Create hostmount configuration file
        fn create_hostmount_config(&self, host_path: &PathBuf) -> Result<PathBuf> {
            let config_path = self.temp_dir.path().join("hostmount_config.yaml");
            let config_content = format!("directory: \"{}\"", host_path.to_string_lossy());
            fs::write(&config_path, config_content)?;
            Ok(config_path)
        }

        /// Verify that the dynamic node exists in the pond
        async fn verify_node_exists(&self, pond_path: &str) -> Result<bool> {
            let mut ship = self.ship_context.open_pond().await?;
            let path_for_closure = pond_path.to_string();
            ship.transact(
                vec!["verify_node".to_string()],
                |_tx, fs| Box::pin(async move {
                    let root = fs.root().await
                        .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                    Ok(root.exists(&path_for_closure).await)
                })
            ).await
                .map_err(|e| anyhow!("Failed to verify node existence: {}", e))
        }
    }

    #[tokio::test]
    async fn test_mknod_hostmount_directory_success() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create a host directory to mount
        let host_dir = setup.create_host_dir("test_host_dir")?;
        let config_path = setup.create_hostmount_config(&host_dir)?;
        
        // Create hostmount node in pond
        let result = mknod_command(&setup.ship_context, "hostmount", "/mounted_dir", &config_path.to_string_lossy()).await;
        
        assert!(result.is_ok(), "mknod should succeed for valid hostmount config: {:?}", result.err());
        
        // Verify the node was created
        assert!(setup.verify_node_exists("/mounted_dir").await?);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_nonexistent_factory() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create a dummy config file
        let config_path = setup.temp_dir.path().join("dummy_config.json");
        fs::write(&config_path, "{}")?;
        
        // Try to create node with unknown factory
        let result = mknod_command(&setup.ship_context, "unknown_factory", "/test_node", &config_path.to_string_lossy()).await;
        
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Unknown factory type") || error_msg.contains("Invalid configuration"));
        
        // Verify node was not created
        assert!(!setup.verify_node_exists("/test_node").await?);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_invalid_config_file() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Try with nonexistent config file
        let result = mknod_command(&setup.ship_context, "hostmount", "/test_node", "/nonexistent/config.json").await;
        
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Failed to read config file"));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_invalid_hostmount_config() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create invalid config file
        let config_path = setup.temp_dir.path().join("invalid_config.yaml");
        fs::write(&config_path, "invalid: config")?;
        
        // Try to create hostmount node with invalid config
        let result = mknod_command(&setup.ship_context, "hostmount", "/test_node", &config_path.to_string_lossy()).await;
        
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid configuration"));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_hostmount_nonexistent_host_path() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create config pointing to nonexistent host path
        let nonexistent_path = PathBuf::from("/this/path/does/not/exist");
        let config_path = setup.create_hostmount_config(&nonexistent_path)?;
        
        // Try to create hostmount node
        let result = mknod_command(&setup.ship_context, "hostmount", "/test_node", &config_path.to_string_lossy()).await;
        
        // This might succeed (creating the node) but fail when accessing the host path
        // The exact behavior depends on how hostmount factory validates paths
        // We primarily test that we don't panic or crash
        match result {
            Ok(_) => {
                // Node created, but accessing it might fail later
                assert!(setup.verify_node_exists("/test_node").await?);
            }
            Err(err) => {
                // Factory validation caught the issue
                let error_msg = err.to_string();
                assert!(error_msg.contains("Invalid configuration") || error_msg.contains("host_path"));
            }
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_nested_path() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create parent directories first
        crate::commands::mkdir::mkdir_command(&setup.ship_context, "/deep/nested/path", true).await?;
        
        // Create host directory
        let host_dir = setup.create_host_dir("nested_test")?;
        let config_path = setup.create_hostmount_config(&host_dir)?;
        
        // Create node at nested path
        let result = mknod_command(&setup.ship_context, "hostmount", "/deep/nested/path/mounted", &config_path.to_string_lossy()).await;
        
        assert!(result.is_ok(), "mknod should succeed for nested paths: {:?}", result.err());
        
        // Verify the nested node was created
        assert!(setup.verify_node_exists("/deep/nested/path/mounted").await?);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_duplicate_path() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create host directory
        let host_dir = setup.create_host_dir("duplicate_test")?;
        let config_path = setup.create_hostmount_config(&host_dir)?;
        
        // Create first node
        let result1 = mknod_command(&setup.ship_context, "hostmount", "/duplicate_node", &config_path.to_string_lossy()).await;
        assert!(result1.is_ok());
        
        // Try to create second node at same path
        let result2 = mknod_command(&setup.ship_context, "hostmount", "/duplicate_node", &config_path.to_string_lossy()).await;
        
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
        let result = mknod_command(&setup.ship_context, "hostmount", "/test_rollback", &config_path.to_string_lossy()).await;
        
        assert!(result.is_err());
        
        // Verify that no partial node was created (transaction rolled back)
        assert!(!setup.verify_node_exists("/test_rollback").await?);
        
        Ok(())
    }
}
