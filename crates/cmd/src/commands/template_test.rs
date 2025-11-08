#[cfg(test)]
mod tests {
    use crate::commands::cat::cat_command;
    use crate::commands::init::init_command;
    use crate::commands::mknod::mknod_command;
    use crate::common::ShipContext;
    use anyhow::Result;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

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
            init_command(&ship_context, None, None).await?;

            Ok(Self {
                temp_dir,
                ship_context,
                pond_path,
            })
        }

        /// Create template configuration file that references a pond path
        /// Also copies the template file into the pond at the specified path
        async fn create_template_config(
            &self,
            template_content: &str,
            pond_template_path: &str,
        ) -> Result<PathBuf> {
            use crate::commands::mkdir_command;

            // First, create the directory structure in the pond for the template file
            let template_dir = std::path::Path::new(pond_template_path)
                .parent()
                .and_then(|p| p.to_str())
                .unwrap_or("/");

            if template_dir != "/" {
                mkdir_command(&self.ship_context, template_dir, true).await?;
            }

            // Copy the template file into the pond
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test".to_string(),
                    "copy_template".to_string(),
                ]))
                .await?;

            {
                let fs = &*tx;
                let root = fs.root().await?;
                let mut writer = root
                    .async_writer_path_with_type(
                        pond_template_path,
                        tinyfs::EntryType::FileDataPhysical,
                    )
                    .await?;
                writer
                    .write_all(template_content.as_bytes())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to write template content: {}", e))?;
                writer
                    .flush()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to flush template file: {}", e))?;
                writer
                    .shutdown()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to shutdown template writer: {}", e))?;
            }

            tx.commit().await?;

            // Create config that references the pond path
            let config_path = self.temp_dir.path().join("template_config.yaml");
            let config_content = format!(
                "in_pattern: \"/base/*.template\"\nout_pattern: \"$0.template\"\ntemplate_file: \"{}\"",
                pond_template_path
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

            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            for filename in filenames {
                let filename_str = filename.to_string();
                let tx = ship
                    .begin_write(&steward::PondUserMetadata::new(vec![
                        "test".to_string(),
                        "create".to_string(),
                    ]))
                    .await?;

                let result = {
                    let fs = &*tx;
                    let root = fs.root().await?;
                    let mut writer = root
                        .async_writer_path_with_type(
                            &filename_str,
                            tinyfs::EntryType::FileDataPhysical,
                        )
                        .await?;
                    writer.write_all(b"template file content").await?;
                    writer.flush().await?;
                    writer.shutdown().await
                };

                tx.commit().await?;
                result
                    .map_err(|e| anyhow::anyhow!("Failed to write template file content: {}", e))?;
            }

            Ok(())
        }

        /// Verify that the dynamic node exists in the pond
        async fn verify_node_exists(&self, pond_path: &str) -> Result<bool> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
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
    async fn test_template_factory_with_date_function() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create template config with Tera date function
        let template_content = "Generated on: {{ now() | date(format=\"%Y-%m-%d %H:%M:%S\") }}\nTemplate test successful!";
        let config_path = setup
            .create_template_config(template_content, "/tmpl/date.tmpl")
            .await?;

        // Create template dynamic directory in pond
        let result = mknod_command(
            &setup.ship_context,
            "template",
            "/templates",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(
            result.is_ok(),
            "mknod should succeed for valid template config: {:?}",
            result.err()
        );

        // Verify the node was created
        assert!(setup.verify_node_exists("/templates").await?);

        // Create some empty files that match the pattern to establish filenames
        setup
            .create_pattern_files(&["/base/test.template", "/base/hello.template"])
            .await?;

        // Try to cat a template file - this should trigger template rendering
        let mut output = String::new();
        let cat_result = cat_command(
            &setup.ship_context,
            "/templates/test.template",
            "text",
            Some(&mut output),
            None, // time_start
            None, // time_end
            None, // sql_query
        )
        .await;

        assert!(
            cat_result.is_ok(),
            "cat should succeed for template file: {:?}",
            cat_result.err()
        );

        // The output should contain the template content with date function rendered
        // We can't check exact date, but we can check that template rendering occurred
        println!("Template output content: {}", output);
        println!("Template cat result: {:?}", cat_result);

        // Check that the output contains expected template structure
        assert!(
            output.contains("Generated on:"),
            "Template should contain 'Generated on:' text"
        );
        assert!(
            output.contains("Template test successful!"),
            "Template should contain success message"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_template_factory_simple_content() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create template config with simple static content
        let template_content = "Hello from template factory!\nThis is a test template.";
        let config_path = setup
            .create_template_config(template_content, "/tmpl/simple.tmpl")
            .await?;

        // Create template dynamic directory in pond
        let result = mknod_command(
            &setup.ship_context,
            "template",
            "/simple_templates",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(
            result.is_ok(),
            "mknod should succeed for valid template config: {:?}",
            result.err()
        );

        // Verify the node was created
        assert!(setup.verify_node_exists("/simple_templates").await?);

        // Create source template files that the factory can discover
        setup
            .create_pattern_files(&["/base/hello.template", "/base/world.template"])
            .await?;

        // Try to cat a template file - this should trigger template rendering
        let mut output = String::new();
        let cat_result = cat_command(
            &setup.ship_context,
            "/simple_templates/hello.template",
            "text",
            Some(&mut output),
            None, // time_start
            None, // time_end
            None, // sql_query
        )
        .await;

        assert!(
            cat_result.is_ok(),
            "cat should succeed for simple template file: {:?}",
            cat_result.err()
        );

        // Verify the output contains the rendered template content
        assert!(
            output.contains("Hello from template factory!"),
            "Output should contain rendered template content. Got: {}",
            output
        );

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
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid template spec") || error_msg.contains("missing field"));

        // Verify node was not created
        assert!(!setup.verify_node_exists("/invalid_templates").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_template_factory_with_cli_variables() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create template config that uses CLI variables
        let template_content = "Hello {{ vars.name }}!\nYour message: {{ vars.message }}";
        let config_path = setup
            .create_template_config(template_content, "/tmpl/vars.tmpl")
            .await?;

        // Create template dynamic directory in pond
        let result = mknod_command(
            &setup.ship_context,
            "template",
            "/var_templates",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(
            result.is_ok(),
            "mknod should succeed for template config with variables: {:?}",
            result.err()
        );

        // Verify the node was created
        assert!(setup.verify_node_exists("/var_templates").await?);

        // Create source template files
        setup
            .create_pattern_files(&["/base/greeting.template"])
            .await?;

        // Write template content to the greeting.template file
        {
            let mut ship = steward::Ship::open_pond(&setup.pond_path).await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test".to_string(),
                    "write_template".to_string(),
                ]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            // Write template content
            let template_path = "/base/greeting.template";
            let mut writer = root
                .async_writer_path_with_type(&template_path, tinyfs::EntryType::FileDataPhysical)
                .await?;
            writer.write_all(template_content.as_bytes()).await?;
            writer.flush().await?;
            writer.shutdown().await?;

            tx.commit().await?;
        }

        // Create a ShipContext with template variables
        let mut variables = std::collections::HashMap::new();
        variables.insert("name".to_string(), "DuckPond".to_string());
        variables.insert(
            "message".to_string(),
            "Template variables work!".to_string(),
        );

        let var_ship_context = ShipContext::with_variables(
            setup.ship_context.pond_path.clone(),
            setup.ship_context.original_args.clone(),
            variables,
        );

        // Try to cat a template file - this should trigger template rendering with variables
        let mut output = String::new();
        let cat_result = cat_command(
            &var_ship_context,
            "/var_templates/greeting.template",
            "text",
            Some(&mut output),
            None, // time_start
            None, // time_end
            None, // sql_query
        )
        .await;

        assert!(
            cat_result.is_ok(),
            "cat should succeed for template file with variables: {:?}",
            cat_result.err()
        );

        // The output should contain the template content with variables expanded
        println!("Template output with variables: {}", output);
        assert!(
            output.contains("Hello DuckPond!"),
            "Template should expand name variable"
        );
        assert!(
            output.contains("Your message: Template variables work!"),
            "Template should expand message variable"
        );

        Ok(())
    }
}
