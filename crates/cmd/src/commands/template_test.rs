#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::TempDir;
    use std::fs;
    use std::path::PathBuf;
    use crate::common::{ShipContext, FilesystemChoice};
    use crate::commands::init::init_command;
    use crate::commands::mknod::mknod_command;
    use crate::commands::cat::cat_command;

    struct TestSetup {
        temp_dir: TempDir,
        ship_context: ShipContext,
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
                temp_dir,
                ship_context,
                pond_path,
            })
        }

        /// Create template configuration file
        fn create_template_config(&self, template_content: &str) -> Result<PathBuf> {
            let config_path = self.temp_dir.path().join("template_config.yaml");
            let config_content = format!(
                "source: \"/base/*.template\"\npatterns:\n  - \"*.template\"\ntemplate_content: |\n  {}",
                template_content.replace('\n', "\n  ")
            );
            fs::write(&config_path, config_content)?;
            Ok(config_path)
        }

        /// Create empty files to establish pattern matches
        async fn create_pattern_files(&self, filenames: &[&str]) -> Result<()> {
            use crate::commands::mkdir_command;
            use tokio::io::AsyncWriteExt;
            
            // First create the /base directory
            mkdir_command(&self.ship_context, "/base", true).await?;
            
            let mut ship = steward::Ship::open_pond(&self.pond_path).await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;
            
            for filename in filenames {
                let filename_str = filename.to_string();
                let tx = ship.begin_transaction(vec!["test".to_string(), "create".to_string()]).await?;
                
                let result = {
                    let fs = &*tx;
                    let root = fs.root().await?;
                    let mut writer = root.async_writer_path_with_type(&filename_str, tinyfs::EntryType::FileData).await?;
                    writer.write_all(b"template file content").await?;
                    writer.flush().await
                };
                
                tx.commit().await?;
                result.map_err(|e| anyhow::anyhow!("Failed to write template file content: {}", e))?;
            }
            
            Ok(())
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
                .map_err(|e| anyhow::anyhow!("Failed to verify node existence: {}", e))
        }
    }

    #[tokio::test]
    async fn test_template_factory_with_date_function() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create template config with Tera date function
        let template_content = "Generated on: {{ now() | date(format=\"%Y-%m-%d %H:%M:%S\") }}\nTemplate test successful!";
        let config_path = setup.create_template_config(template_content)?;
        
        // Create template dynamic directory in pond
        let result = mknod_command(
            &setup.ship_context, 
            "template", 
            "/templates", 
            &config_path.to_string_lossy()
        ).await;
        
        assert!(result.is_ok(), "mknod should succeed for valid template config: {:?}", result.err());
        
        // Verify the node was created
        assert!(setup.verify_node_exists("/templates").await?);
        
        // Create some empty files that match the pattern to establish filenames
        setup.create_pattern_files(&[
            "/base/test.template",
            "/base/hello.template"
        ]).await?;
        
        // Try to cat a template file - this should trigger template rendering
        let mut output = String::new();
        let cat_result = cat_command(
            &setup.ship_context, 
            "/templates/test.template",
            FilesystemChoice::Data,
            "text",
            Some(&mut output),
            None, // time_start
            None, // time_end
            None  // sql_query
        ).await;
        
        assert!(cat_result.is_ok(), "cat should succeed for template file: {:?}", cat_result.err());
        
        // The output should contain the template content with date function rendered
        // We can't check exact date, but we can check that template rendering occurred
        println!("Template output content: {}", output);
        println!("Template cat result: {:?}", cat_result);
        
        // Check that the output contains expected template structure
        assert!(output.contains("Generated on:"), "Template should contain 'Generated on:' text");
        assert!(output.contains("Template test successful!"), "Template should contain success message");
        
        Ok(())
    }

    #[tokio::test]
    async fn test_template_factory_simple_content() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create template config with simple static content
        let template_content = "Hello from template factory!\nThis is a test template.";
        let config_path = setup.create_template_config(template_content)?;
        
        // Create template dynamic directory in pond
        let result = mknod_command(
            &setup.ship_context, 
            "template", 
            "/simple_templates", 
            &config_path.to_string_lossy()
        ).await;
        
        assert!(result.is_ok(), "mknod should succeed for valid template config: {:?}", result.err());
        
        // Verify the node was created
        assert!(setup.verify_node_exists("/simple_templates").await?);
        
        // Create source template files that the factory can discover
        setup.create_pattern_files(&[
            "/base/hello.template",
            "/base/world.template"
        ]).await?;
        
        // Try to cat a template file - this should trigger template rendering
        let mut output = String::new();
        let cat_result = cat_command(
            &setup.ship_context, 
            "/simple_templates/hello.template",
            FilesystemChoice::Data,
            "text", 
            Some(&mut output),
            None, // time_start
            None, // time_end
            None  // sql_query
        ).await;
        
        assert!(cat_result.is_ok(), "cat should succeed for simple template file: {:?}", cat_result.err());
        
        // Verify the output contains the rendered template content
        assert!(output.contains("Hello from template factory!"), 
                "Output should contain rendered template content. Got: {}", output);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_template_factory_invalid_config() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create invalid template config (missing required fields)
        let config_path = setup.temp_dir.path().join("invalid_config.yaml");
        fs::write(&config_path, "invalid: yaml content")?;
        
        // Try to create template node with invalid config
        let result = mknod_command(
            &setup.ship_context, 
            "template", 
            "/invalid_templates", 
            &config_path.to_string_lossy()
        ).await;
        
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid template spec") || error_msg.contains("missing field"));
        
        // Verify node was not created
        assert!(!setup.verify_node_exists("/invalid_templates").await?);
        
        Ok(())
    }
}