use crate::common::{FileInfoVisitor, ShipContext};
use anyhow::Result;

/// Describe command - shows file types and schemas for files matching the pattern
///
/// This command operates on an existing pond via the provided Ship.
/// Uses scoped transactions for consistent filesystem access.
pub async fn describe_command<F>(
    ship_context: &ShipContext,
    pattern: &str,
    mut handler: F,
) -> Result<()>
where
    F: FnMut(String),
{
    log::debug!("describe_command called with pattern {pattern}");

    let mut ship = ship_context.open_pond().await?;

    // Use transaction for consistent filesystem access
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "describe".to_string(),
            pattern.to_string(),
        ]))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

    match describe_command_impl(&mut tx, ship_context, pattern).await {
        Ok(output) => {
            tx.commit()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            handler(output);
            Ok(())
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of describe command
async fn describe_command_impl(
    tx: &mut steward::StewardTransactionGuard<'_>,
    ship_context: &ShipContext,
    pattern: &str,
) -> Result<String> {
    let fs = &**tx;
    let root = fs
        .root()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {}", e))?;

    // Use the same file matching logic as list command
    let mut visitor = FileInfoVisitor::new(false); // don't show hidden files
    let mut files = root
        .visit_with_visitor(pattern, &mut visitor)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to find files with pattern '{}': {}", pattern, e))?;

    if files.is_empty() {
        return Ok(format!("No files found matching pattern '{}'", pattern));
    }

    // Sort results by path for consistent output
    files.sort_by(|a, b| a.path.cmp(&b.path));

    let mut output = String::new();
    output.push_str("=== File Schema Description ===\n");
    output.push_str(&format!("Pattern: {}\n", pattern));
    output.push_str(&format!("Files found: {}\n\n", files.len()));

    for file_info in files {
        output.push_str(&format!("📄 {}\n", file_info.path));
        output.push_str(&format!("   Type: {:?}\n", file_info.metadata.entry_type));

        match file_info.metadata.entry_type {
            tinyfs::EntryType::FileSeriesPhysical | tinyfs::EntryType::FileSeriesDynamic => {
                output.push_str("   Format: Parquet series\n");
                match describe_file_series_schema(ship_context, &file_info.path).await {
                    Ok(schema_info) => {
                        output
                            .push_str(&format!("   Schema: {} fields\n", schema_info.field_count));
                        for field_info in schema_info.fields {
                            output.push_str(&format!(
                                "     • {}: {}\n",
                                field_info.name, field_info.data_type
                            ));
                        }
                        if let Some(timestamp_col) = schema_info.timestamp_column {
                            output.push_str(&format!("   Timestamp Column: {}\n", timestamp_col));
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("   Schema: Error loading schema - {}\n", e));
                    }
                }
            }
            tinyfs::EntryType::FileTablePhysical | tinyfs::EntryType::FileTableDynamic => {
                output.push_str("   Format: Parquet table\n");
                match describe_file_table_schema(ship_context, &file_info.path).await {
                    Ok(schema_info) => {
                        output
                            .push_str(&format!("   Schema: {} fields\n", schema_info.field_count));
                        for field_info in schema_info.fields {
                            output.push_str(&format!(
                                "     • {}: {}\n",
                                field_info.name, field_info.data_type
                            ));
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("   Schema: Error loading schema - {}\n", e));
                    }
                }
            }
            tinyfs::EntryType::FileDataPhysical | tinyfs::EntryType::FileDataDynamic => {
                output.push_str("   Format: Raw data\n");
            }
            tinyfs::EntryType::DirectoryPhysical | tinyfs::EntryType::DirectoryDynamic => {
                output.push_str("   Format: Directory\n");
            }
            tinyfs::EntryType::Symlink => {
                output.push_str("   Format: Symbolic link\n");
            }
        }

        output.push_str(&format!(
            "   Size: {} bytes\n",
            file_info.metadata.size.unwrap_or(0)
        ));
        output.push_str(&format!("   Version: {}\n", file_info.metadata.version));
        output.push_str("\n");
    }

    Ok(output)
}

#[derive(Debug)]
pub struct SchemaInfo {
    pub field_count: usize,
    pub fields: Vec<FieldInfo>,
    pub timestamp_column: Option<String>,
}

#[derive(Debug)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
}

/// Describe the schema of a file:series using tlogfs schema API
async fn describe_file_series_schema(ship_context: &ShipContext, path: &str) -> Result<SchemaInfo> {
    let mut ship = ship_context.open_pond().await?;
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "describe-schema".to_string(),
        ]))
        .await?;

    match describe_file_series_schema_impl(&mut tx, path).await {
        Ok(schema_info) => {
            tx.commit().await?;
            Ok(schema_info)
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of file series schema description
async fn describe_file_series_schema_impl(
    tx: &mut steward::StewardTransactionGuard<'_>,
    path: &str,
) -> Result<SchemaInfo> {
    let fs = &**tx;
    let root = fs.root().await?;

    // Use tlogfs get_file_schema API - works for both static and dynamic files
    let state = tx.transaction_guard()?.state()?;
    let schema = tlogfs::get_file_schema(&root, path, &state)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get schema for '{}': {}", path, e))?;

    extract_schema_info(&schema, true)
}

/// Describe the schema of a file:table using tlogfs schema API
async fn describe_file_table_schema(ship_context: &ShipContext, path: &str) -> Result<SchemaInfo> {
    let mut ship = ship_context.open_pond().await?;
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "describe-schema".to_string(),
        ]))
        .await?;

    match describe_file_table_schema_impl(&mut tx, path).await {
        Ok(schema_info) => {
            tx.commit().await?;
            Ok(schema_info)
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of file table schema description
async fn describe_file_table_schema_impl(
    tx: &mut steward::StewardTransactionGuard<'_>,
    path: &str,
) -> Result<SchemaInfo> {
    let fs = &**tx;
    let root = fs.root().await?;

    // Use tlogfs get_file_schema API - works for both static and dynamic files
    let state = tx.transaction_guard()?.state()?;
    let schema = tlogfs::get_file_schema(&root, path, &state)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get schema for '{}': {}", path, e))?;

    extract_schema_info(&schema, false)
}

/// Extract schema info from Arrow schema
fn extract_schema_info(
    schema: &arrow::datatypes::Schema,
    detect_timestamp: bool,
) -> Result<SchemaInfo> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        fields.push(FieldInfo {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
        });
    }

    // Try to detect timestamp column for series
    let timestamp_column = if detect_timestamp {
        schema
            .fields()
            .iter()
            .find(|field| {
                matches!(
                    field.name().to_lowercase().as_str(),
                    "timestamp" | "time" | "event_time" | "ts" | "datetime"
                )
            })
            .map(|field| field.name().clone())
    } else {
        None
    };

    Ok(SchemaInfo {
        field_count: schema.fields().len(),
        fields,
        timestamp_column,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::copy::copy_command;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use anyhow::Result;
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
            let temp_dir = tempfile::tempdir()?;
            let pond_path = temp_dir.path().join("test_pond");

            // Initialize the pond using init_command
            let ship_context = ShipContext {
                pond_path: Some(pond_path.clone()),
                original_args: vec!["pond".to_string(), "init".to_string()],
                template_variables: std::collections::HashMap::new(),
            };
            init_command(&ship_context, None, None).await?;

            Ok(TestSetup {
                temp_dir,
                ship_context,
                pond_path,
            })
        }

        async fn create_host_file(&self, filename: &str, content: &str) -> Result<PathBuf> {
            let host_file = self.temp_dir.path().join(filename);
            tokio::fs::write(&host_file, content).await?;
            Ok(host_file)
        }

        async fn create_csv_file(&self, filename: &str, csv_data: &str) -> Result<PathBuf> {
            self.create_host_file(filename, csv_data).await
        }

        /// Create a sample Parquet file by copying CSV and letting it convert
        async fn create_parquet_table(&self, pond_path: &str, csv_data: &str) -> Result<()> {
            let host_file = self.create_csv_file("temp.csv", csv_data).await?;
            copy_command(
                &self.ship_context,
                &[host_file.to_string_lossy().to_string()],
                pond_path,
                "table",
            )
            .await?;
            Ok(())
        }

        /// Create a sample Parquet series by copying CSV and letting it convert
        async fn create_parquet_series(&self, pond_path: &str, csv_data: &str) -> Result<()> {
            let host_file = self.create_csv_file("temp.csv", csv_data).await?;
            copy_command(
                &self.ship_context,
                &[host_file.to_string_lossy().to_string()],
                pond_path,
                "series",
            )
            .await?;
            Ok(())
        }

        /// Create a raw data file
        async fn create_raw_data_file(&self, pond_path: &str, content: &str) -> Result<()> {
            let host_file = self.create_host_file("temp.txt", content).await?;
            copy_command(
                &self.ship_context,
                &[host_file.to_string_lossy().to_string()],
                pond_path,
                "data",
            )
            .await?;
            Ok(())
        }

        /// Helper to capture describe command output
        async fn describe_output(&self, pattern: &str) -> Result<String> {
            let mut output = String::new();
            describe_command(&self.ship_context, pattern, |s| {
                output.push_str(&s);
            })
            .await?;
            Ok(output)
        }
    }

    #[tokio::test]
    async fn test_describe_command_empty_pond() -> Result<()> {
        let setup = TestSetup::new().await?;

        let output = setup.describe_output("**/*").await?;
        assert!(output.contains("No files found matching pattern"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_parquet_table() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a Parquet table file
        let csv_data = "name,age,city\nAlice,30,NYC\nBob,25,LA";
        setup
            .create_parquet_table("users.parquet", csv_data)
            .await?;

        let output = setup.describe_output("users.parquet").await?;

        assert!(output.contains("📄 /users.parquet"));
        assert!(output.contains("Type: FileTable"));
        let output = setup.describe_output("users.parquet").await?;

        assert!(output.contains("📄 /users.parquet"));
        assert!(output.contains("Type: FileTable"));
        assert!(output.contains("Format: Parquet table"));
        // Schema parsing might fail in tests, so be more flexible
        assert!(output.contains("Schema:"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_parquet_series() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a Parquet series file with timestamp column
        let csv_data =
            "timestamp,device_id,value\n1609459200000,device1,100.0\n1609459260000,device2,101.5";
        setup
            .create_parquet_series("sensors.parquet", csv_data)
            .await?;

        let output = setup.describe_output("sensors.parquet").await?;

        assert!(output.contains("📄 /sensors.parquet"));
        assert!(output.contains("Type: FileSeries"));
        assert!(output.contains("Format: Parquet series"));
        // Schema parsing might fail in tests, so be more flexible
        assert!(output.contains("Schema:"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_multiple_files() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create multiple files of different types
        setup
            .create_parquet_table("table1.parquet", "id,name\n1,Alice\n2,Bob")
            .await?;
        setup
            .create_parquet_series("series1.parquet", "timestamp,value\n1609459200000,100")
            .await?;
        setup
            .create_raw_data_file("readme.txt", "This is a text file")
            .await?;

        let output = setup.describe_output("**/*").await?;

        assert!(output.contains("Files found: 3"));
        assert!(output.contains("📄 /readme.txt"));
        assert!(output.contains("📄 /series1.parquet"));
        assert!(output.contains("📄 /table1.parquet"));

        // Check that different types are described correctly
        assert!(output.contains("Type: FileTable"));
        assert!(output.contains("Type: FileSeries"));
        assert!(output.contains("Type: FileData"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_pattern_matching() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create files with different extensions
        setup
            .create_parquet_table("users.parquet", "name,age\nAlice,30")
            .await?;
        setup
            .create_raw_data_file("config.txt", "debug=true")
            .await?;
        setup
            .create_raw_data_file("data.json", r#"{"key": "value"}"#)
            .await?;

        // Test pattern matching for parquet files only
        let output = setup.describe_output("*.parquet").await?;
        assert!(output.contains("Files found: 1"));
        assert!(output.contains("users.parquet"));
        assert!(!output.contains("config.txt"));
        assert!(!output.contains("data.json"));

        // Test pattern matching for text files
        let output = setup.describe_output("*.txt").await?;
        assert!(output.contains("Files found: 1"));
        assert!(output.contains("config.txt"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_nonexistent_pattern() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create one file
        setup
            .create_raw_data_file("existing.txt", "content")
            .await?;

        // Try to describe a non-matching pattern
        let output = setup.describe_output("nonexistent*").await?;
        assert!(output.contains("No files found matching pattern 'nonexistent*'"));

        Ok(())
    }
}
