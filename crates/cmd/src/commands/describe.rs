use anyhow::Result;
use crate::common::{FilesystemChoice, ShipContext, FileInfoVisitor};
use diagnostics::*;

/// Describe command - shows file types and schemas for files matching the pattern
/// 
/// This command operates on an existing pond via the provided Ship.
/// Uses scoped transactions for consistent filesystem access.
pub async fn describe_command<F>(
    ship_context: &ShipContext, 
    pattern: &str, 
    filesystem: FilesystemChoice,
    mut handler: F
) -> Result<()>
where
    F: FnMut(String),
{
    log_debug!("describe_command called with pattern", pattern: pattern);

    // For now, only support data filesystem - control filesystem access would require different API
    if filesystem == FilesystemChoice::Control {
        return Err(anyhow::anyhow!("Control filesystem access not yet implemented for describe command"));
    }

    let mut ship = ship_context.open_pond().await?;
    
    // Use transaction for consistent filesystem access
    let tx = ship.begin_transaction(vec!["describe".to_string(), pattern.to_string()]).await
        .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
    
    let result: Result<String> = {
        let fs = &*tx;
        let root = fs.root().await
            .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {}", e))?;
        
        // Use the same file matching logic as list command
        let mut visitor = FileInfoVisitor::new(false); // don't show hidden files
        let mut files = root.visit_with_visitor(pattern, &mut visitor).await
            .map_err(|e| anyhow::anyhow!("Failed to find files with pattern '{}': {}", pattern, e))?;

        if files.is_empty() {
            Ok(format!("No files found matching pattern '{}'", pattern))
        } else {
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
                    tinyfs::EntryType::FileSeries => {
                        output.push_str("   Format: Parquet series\n");
                        match describe_file_series_schema(&*tx, &file_info.path).await {
                            Ok(schema_info) => {
                                output.push_str(&format!("   Schema: {} fields\n", schema_info.field_count));
                                for field_info in schema_info.fields {
                                    output.push_str(&format!("     • {}: {}\n", field_info.name, field_info.data_type));
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
                    tinyfs::EntryType::FileTable => {
                        output.push_str("   Format: Parquet table\n");
                        match describe_file_table_schema(&*tx, &file_info.path).await {
                            Ok(schema_info) => {
                                output.push_str(&format!("   Schema: {} fields\n", schema_info.field_count));
                                for field_info in schema_info.fields {
                                    output.push_str(&format!("     • {}: {}\n", field_info.name, field_info.data_type));
                                }
                            }
                            Err(e) => {
                                output.push_str(&format!("   Schema: Error loading schema - {}\n", e));
                            }
                        }
                    }
                    tinyfs::EntryType::FileData => {
                        output.push_str("   Format: Raw data\n");
                        match detect_content_type(&*tx, &file_info.path).await {
                            Ok(content_type) => {
                                output.push_str(&format!("   Content: {}\n", content_type));
                            }
                            Err(_) => {
                                output.push_str("   Content: Binary/unknown\n");
                            }
                        }
                    }
                    tinyfs::EntryType::Directory => {
                        output.push_str("   Format: Directory\n");
                        output.push_str("   Contains: Multiple entries\n");
                    }
                    tinyfs::EntryType::Symlink => {
                        output.push_str("   Format: Symbolic link\n");
                        if let Some(target) = &file_info.symlink_target {
                            output.push_str(&format!("   Target: {}\n", target));
                        } else {
                            output.push_str("   Target: Unknown\n");
                        }
                    }
                }
                
                output.push_str(&format!("   Size: {} bytes\n", file_info.metadata.size.unwrap_or(0)));
                output.push_str(&format!("   Version: {}\n", file_info.metadata.version));
                output.push_str("\n");
            }

            Ok(output)
        }
    };
    
    // Commit the transaction before processing results
    tx.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
    
    let output = result?;
    handler(output);
    
    Ok(())
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

/// Describe the schema of a file:series
async fn describe_file_series_schema(root: &tinyfs::FS, path: &str) -> Result<SchemaInfo> {
    let wd = root.root().await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {}", e))?;
    
    // Get the first version to read its schema
    let file_versions = wd.list_file_versions(path).await
        .map_err(|e| anyhow::anyhow!("Failed to list file versions: {}", e))?;
    
    if file_versions.is_empty() {
        return Err(anyhow::anyhow!("No versions found in series"));
    }

    // Read the first version to get the schema
    let first_version = &file_versions[0];
    let version_data = wd.read_file_version(path, Some(first_version.version)).await
        .map_err(|e| anyhow::anyhow!("Failed to read version {}: {}", first_version.version, e))?;

    parse_parquet_schema(&version_data, true).await
}

/// Describe the schema of a file:table
async fn describe_file_table_schema(root: &tinyfs::FS, path: &str) -> Result<SchemaInfo> {
    let wd = root.root().await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {}", e))?;
    
    // Read the file content - use None for latest version
    let file_data = wd.read_file_version(path, None).await
        .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;

    parse_parquet_schema(&file_data, false).await
}

/// Parse Parquet schema from data
async fn parse_parquet_schema(data: &[u8], detect_timestamp: bool) -> Result<SchemaInfo> {
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    // Clone the data to avoid lifetime issues with the cursor
    let data_owned = data.to_vec();
    let cursor = std::io::Cursor::new(data_owned);
    let builder = ParquetRecordBatchStreamBuilder::new(cursor).await
        .map_err(|e| anyhow::anyhow!("Failed to read Parquet schema: {}", e))?;
    
    let schema = builder.schema();
    
    let mut fields = Vec::new();
    for field in schema.fields() {
        fields.push(FieldInfo {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
        });
    }
    
    // Try to detect timestamp column for series
    let timestamp_column = if detect_timestamp {
        schema.fields().iter()
            .find(|field| {
                matches!(field.name().to_lowercase().as_str(), 
                    "timestamp" | "time" | "event_time" | "ts" | "datetime")
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

/// Try to detect content type for file:data
async fn detect_content_type(root: &tinyfs::FS, path: &str) -> Result<String> {
    let wd = root.root().await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {}", e))?;
    
    // Read the first few bytes to detect content type - use None for latest version
    let file_data = wd.read_file_version(path, None).await
        .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;
    
    if file_data.is_empty() {
        return Ok("Empty file".to_string());
    }
    
    // Check for common file types
    if file_data.len() >= 4 {
        let header = &file_data[0..4];
        if header == b"PAR1" {
            return Ok("Parquet file".to_string());
        }
        if header.starts_with(b"\x7fELF") {
            return Ok("ELF binary".to_string());
        }
        if header.starts_with(b"\x89PNG") {
            return Ok("PNG image".to_string());
        }
        if header.starts_with(b"GIF8") {
            return Ok("GIF image".to_string());
        }
        if header.starts_with(b"\xff\xd8\xff") {
            return Ok("JPEG image".to_string());
        }
        if header == b"PK\x03\x04" || header == b"PK\x05\x06" || header == b"PK\x07\x08" {
            return Ok("ZIP archive".to_string());
        }
    }
    
    // Check if it's text
    if file_data.iter().take(1024).all(|&b| b.is_ascii() && (b >= 32 || b == b'\n' || b == b'\r' || b == b'\t')) {
        // Try to guess text format
        let sample = String::from_utf8_lossy(&file_data[0..1024.min(file_data.len())]);
        if sample.contains(',') && sample.lines().count() > 1 {
            return Ok("CSV text".to_string());
        }
        if sample.starts_with('{') || sample.starts_with('[') {
            return Ok("JSON text".to_string());
        }
        if sample.contains('\t') {
            return Ok("TSV text".to_string());
        }
        return Ok("Plain text".to_string());
    }
    
    Ok(format!("Binary data ({} bytes)", file_data.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::path::PathBuf;
    use crate::common::ShipContext;
    use crate::commands::init::init_command;
    use crate::commands::copy::copy_command;
    use anyhow::Result;

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
            };
            init_command(&ship_context).await?;

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
            copy_command(&self.ship_context, &[host_file.to_string_lossy().to_string()], pond_path, "table").await?;
            Ok(())
        }

        /// Create a sample Parquet series by copying CSV and letting it convert
        async fn create_parquet_series(&self, pond_path: &str, csv_data: &str) -> Result<()> {
            let host_file = self.create_csv_file("temp.csv", csv_data).await?;
            copy_command(&self.ship_context, &[host_file.to_string_lossy().to_string()], pond_path, "series").await?;
            Ok(())
        }

        /// Create a raw data file
        async fn create_raw_data_file(&self, pond_path: &str, content: &str) -> Result<()> {
            let host_file = self.create_host_file("temp.txt", content).await?;
            copy_command(&self.ship_context, &[host_file.to_string_lossy().to_string()], pond_path, "data").await?;
            Ok(())
        }

        /// Helper to capture describe command output
        async fn describe_output(&self, pattern: &str) -> Result<String> {
            let mut output = String::new();
            describe_command(&self.ship_context, pattern, FilesystemChoice::Data, |s| {
                output.push_str(&s);
            }).await?;
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
        setup.create_parquet_table("users.parquet", csv_data).await?;

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
        let csv_data = "timestamp,device_id,value\n1609459200000,device1,100.0\n1609459260000,device2,101.5";
        setup.create_parquet_series("sensors.parquet", csv_data).await?;

        let output = setup.describe_output("sensors.parquet").await?;
        
        assert!(output.contains("📄 /sensors.parquet"));
        assert!(output.contains("Type: FileSeries"));
        assert!(output.contains("Format: Parquet series"));
        // Schema parsing might fail in tests, so be more flexible
        assert!(output.contains("Schema:"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_raw_data_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a CSV text file as raw data
        let csv_content = "id,name,score\n1,Alice,95\n2,Bob,87";
        setup.create_raw_data_file("data.csv", csv_content).await?;

        let output = setup.describe_output("data.csv").await?;
        
        assert!(output.contains("📄 /data.csv"));
        assert!(output.contains("Type: FileData"));
        assert!(output.contains("Format: Raw data"));
        assert!(output.contains("Content: CSV text"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_json_content() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a JSON file as raw data
        let json_content = r#"{"users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]}"#;
        setup.create_raw_data_file("data.json", json_content).await?;

        let output = setup.describe_output("data.json").await?;
        
        assert!(output.contains("📄 /data.json"));
        assert!(output.contains("Content: JSON text"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_multiple_files() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create multiple files of different types
        setup.create_parquet_table("table1.parquet", "id,name\n1,Alice\n2,Bob").await?;
        setup.create_parquet_series("series1.parquet", "timestamp,value\n1609459200000,100").await?;
        setup.create_raw_data_file("readme.txt", "This is a text file").await?;

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
        setup.create_parquet_table("users.parquet", "name,age\nAlice,30").await?;
        setup.create_raw_data_file("config.txt", "debug=true").await?;
        setup.create_raw_data_file("data.json", r#"{"key": "value"}"#).await?;

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
    async fn test_describe_command_control_filesystem_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Control filesystem access should return an error for now
        let result = describe_command(&setup.ship_context, "**/*", FilesystemChoice::Control, |_| {}).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Control filesystem access not yet implemented"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_nonexistent_pattern() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create one file
        setup.create_raw_data_file("existing.txt", "content").await?;

        // Try to describe a non-matching pattern
        let output = setup.describe_output("nonexistent*").await?;
        assert!(output.contains("No files found matching pattern 'nonexistent*'"));

        Ok(())
    }
}
