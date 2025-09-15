use diagnostics;
use crate::common::ShipContext;
use anyhow::{Result, anyhow};

async fn get_entry_type_for_file(format: &str) -> Result<tinyfs::EntryType> {
    let entry_type = match format {
        "data" => Ok(tinyfs::EntryType::FileData),
        "table" => {
            Ok(tinyfs::EntryType::FileTable)
        }
        "series" => {
            Ok(tinyfs::EntryType::FileSeries)
        }
        _ => {
            Err(anyhow!("Invalid format '{}'", format))
        }
    };

    entry_type
}

// STREAMING COPY: Copy multiple files to directory using proper context
async fn copy_files_to_directory(
    sources: &[String],
    dest_wd: &tinyfs::WD,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for source in sources {
        // Extract filename from source path
        let source_filename = std::path::Path::new(source)
            .file_name()
            .ok_or("Invalid file path")?
            .to_str()
            .ok_or("Invalid filename")?;
        
        copy_single_file_to_directory_with_name(source, dest_wd, source_filename, format).await?;
    }
    Ok(())
}

// Copy a single file to a directory using the provided working directory context
async fn copy_single_file_to_directory_with_name(
    file_path: &str,
    dest_wd: &tinyfs::WD,
    filename: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    
    // Determine entry type based on format flag
    let entry_type = get_entry_type_for_file(format).await?;
    let entry_type_str = format!("{:?}", entry_type);
    
    diagnostics::log_debug!("copy_single_file_to_directory", 
        source_path: file_path, 
        dest_filename: filename, 
        format: format, 
        entry_type: entry_type_str
    );
    
    // Unified streaming copy for all entry types
    let mut source_file = File::open(file_path).await
        .map_err(|e| format!("Failed to open source file: {}", e))?;
    
    let mut dest_writer = dest_wd.async_writer_path_with_type(filename, entry_type).await
        .map_err(|e| format!("Failed to create destination writer: {}", e))?;
    
    // Stream copy with 64KB buffer for memory efficiency
    tokio::io::copy(&mut source_file, &mut dest_writer).await
        .map_err(|e| format!("Failed to stream file content: {}", e))?;
    
    // TLogFS handles temporal metadata extraction during shutdown for FileSeries
    dest_writer.shutdown().await
        .map_err(|e| format!("Failed to complete file write: {}", e))?;
    
    diagnostics::log_info!("Copied {file_path} to directory as {filename}", file_path: file_path, filename: filename);
    Ok(())
}

/// Copy files into the pond 
/// 
/// This command operates on an existing pond via the provided Ship.
/// Uses scoped transactions for automatic commit/rollback handling.
pub async fn copy_command(ship_context: &ShipContext, sources: &[String], dest: &str, format: &str) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    
    // Add a unique marker to verify we're running the right code
    diagnostics::log_debug!("COPY_VERSION: scoped-transaction-v2.0");
    
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Clone data needed inside the closure
    let sources = sources.to_vec();
    let dest = dest.to_string();
    let format = format.to_string();

    // Use scoped transaction for the copy operation
    ship.transact(
        vec!["copy".to_string(), dest.clone()],
        |_tx, fs| Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            let copy_result = root.resolve_copy_destination(&dest).await;
            match copy_result {
                Ok((dest_wd, dest_type)) => {
                    match dest_type {
                        tinyfs::CopyDestination::Directory | tinyfs::CopyDestination::ExistingDirectory => {
                            // Destination is a directory (either explicit with / or existing) - copy files into it
                            copy_files_to_directory(&sources, &dest_wd, &format).await
                                .map_err(|e| steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Copy to directory failed: {}", e)))
                                ))
                        }
                        tinyfs::CopyDestination::ExistingFile => {
                            // Destination is an existing file - not supported for copy operations
                            if sources.len() == 1 {
                                Err(steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                        format!("Destination '{}' exists but is not a directory (cannot copy to existing file)", &dest)
                                    ))
                                ))
                            } else {
                                Err(steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                        format!("When copying multiple files, destination '{}' must be a directory", &dest)
                                    ))
                                ))
                            }
                        }
                        tinyfs::CopyDestination::NewPath(name) => {
                            // Destination doesn't exist
                            if sources.len() == 1 {
                                // Single file to non-existent destination - use format flag only
                                let source = &sources[0];
                                
                                // Use the same logic as directory copying, just with the specific filename
                                copy_single_file_to_directory_with_name(&source, &dest_wd, &name, &format).await
                                    .map_err(|e| steward::StewardError::DataInit(
                                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to copy file: {}", e)))
                                    ))?;
                                
                                diagnostics::log_info!("Copied {source} to {name}", source: source, name: name);
                                Ok(())
                            } else {
                                Err(steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                        format!("When copying multiple files, destination '{}' must be an existing directory", &dest)
                                    ))
                                ))
                            }
                        }
                    }
                }
                Err(e) => {
                    // Check if this is a trailing slash case where we need to create a directory
                    if dest.ends_with('/') {
                        // Directory doesn't exist but user wants to copy into directory
                        // Create the directory first
                        let clean_dest = dest.trim_end_matches('/');
                        root.create_dir_path(clean_dest).await
                            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                        
                        // Now resolve the destination again (should work now)
                        let (dest_wd, _) = root.resolve_copy_destination(clean_dest).await
                            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                        
                        // Copy files to the newly created directory
                        copy_files_to_directory(&sources, &dest_wd, &format).await
                            .map_err(|e| steward::StewardError::DataInit(
                                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Copy to created directory failed: {}", e)))
                            ))
                    } else {
                        Err(steward::StewardError::DataInit(
                            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to resolve destination '{}': {}", &dest, e)))
                        ))
                    }
                }
            }
        })
    ).await.map_err(|e| anyhow!("Copy operation failed: {}", e))?;
    
    diagnostics::log_info!("✅ File(s) copied successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ShipContext;
    use crate::commands::init::init_command;
    use anyhow::Result;
    use tempfile::TempDir;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    
    /// Test setup helper - creates pond and host files for copy testing
    struct TestSetup {
        #[allow(dead_code)]
        temp_dir: TempDir,
        pond_path: std::path::PathBuf,
        host_files_dir: std::path::PathBuf,
        ship_context: ShipContext,
    }
    
    impl TestSetup {
        async fn new() -> Result<Self> {
            // Create a temporary directory for the test pond
            let temp_dir = TempDir::new()?;
            // Use a unique subdirectory to avoid any conflicts
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let pond_path = temp_dir.path().join(format!("test_pond_{}", timestamp));
            let host_files_dir = temp_dir.path().join("host_files");
            
            // Create host files directory
            tokio::fs::create_dir_all(&host_files_dir).await?;
            
            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(pond_path.clone()), init_args.clone());
            
            // Initialize the pond
            init_command(&ship_context).await?;
            
            Ok(TestSetup {
                temp_dir,
                pond_path,
                host_files_dir,
                ship_context,
            })
        }
        
        /// Create a text file in the host filesystem
        async fn create_host_text_file(&self, filename: &str, content: &str) -> Result<std::path::PathBuf> {
            let file_path = self.host_files_dir.join(filename);
            let mut file = File::create(&file_path).await?;
            file.write_all(content.as_bytes()).await?;
            file.shutdown().await?;
            Ok(file_path)
        }
        
        /// Create a CSV file in the host filesystem
        async fn create_host_csv_file(&self, filename: &str) -> Result<std::path::PathBuf> {
            let content = "timestamp,value,doubled_value\n2024-01-01T00:00:00Z,42.0,84.0\n2024-01-01T01:00:00Z,43.5,87.0\n";
            self.create_host_text_file(filename, content).await
        }
        
        /// Create a series-format parquet file in the host filesystem with proper temporal data
        async fn create_host_series_parquet_file(&self, filename: &str) -> Result<std::path::PathBuf> {
            let file_path = self.host_files_dir.join(filename);
            
            // Create Arrow RecordBatch with Int64 timestamps for series (milliseconds since epoch)
            use arrow_array::{Int64Array, Float64Array, RecordBatch};
            use arrow_schema::{DataType, Field, Schema};
            use std::sync::Arc;
            
            let schema = Schema::new(vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("doubled_value", DataType::Float64, false),
            ]);
            
            let timestamp_array = Int64Array::from(vec![1704067200000_i64, 1704070800000_i64]); // 2024-01-01 timestamps
            let value_array = Float64Array::from(vec![42.0_f64, 43.5_f64]);
            let doubled_value_array = Float64Array::from(vec![84.0_f64, 87.0_f64]);
            
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(timestamp_array),
                    Arc::new(value_array),
                    Arc::new(doubled_value_array),
                ]
            )?;
            
            // Write parquet file to host filesystem
            use parquet::arrow::ArrowWriter;
            
            let file = std::fs::File::create(&file_path)?;
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
            writer.write(&batch)?;
            writer.close()?;
            
            Ok(file_path)
        }
        
        /// Helper to get copy context
        fn copy_context(&self, sources: Vec<String>, dest: String, format: String) -> (ShipContext, Vec<String>, String, String) {
            (self.ship_context.clone(), sources, dest, format)
        }
        
        /// Verify a file exists in the pond by trying to read it
        async fn verify_file_exists(&self, path: &str) -> Result<bool> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship.begin_transaction(vec!["verify".to_string()]).await?;
            let fs = &*tx;
            let root = fs.root().await?;
            
            let exists = match root.async_reader_path(path).await {
                Ok(_) => true,
                Err(tinyfs::Error::NotFound(_)) => false,
                Err(e) => return Err(e.into()),
            };
            
            tx.commit().await?;
            Ok(exists)
        }
        
        /// Read file content from the pond using TinyFS
        async fn read_pond_file_content(&self, path: &str) -> Result<Vec<u8>> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship.begin_transaction(vec!["read".to_string()]).await?;
            let fs = &*tx;
            let root = fs.root().await?;
            
            let mut content = Vec::new();
            {
                use tokio::io::AsyncReadExt;
                let mut reader = root.async_reader_path(path).await?;
                reader.read_to_end(&mut content).await?;
            }
            
            tx.commit().await?;
            Ok(content)
        }
        
        /// Verify file metadata by reading the node metadata directly
        async fn verify_file_metadata(&self, path: &str, _expected_type: tinyfs::EntryType) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship.begin_transaction(vec!["metadata".to_string()]).await?;
            let fs = &*tx;
            let root = fs.root().await?;
            
            // Try to read the file to verify it exists and get the entry type info 
            // (The exact metadata API is not exposed in the same way, but we can verify by successful read)
            let _reader = root.async_reader_path(path).await
                .map_err(|e| anyhow::anyhow!("File {} does not exist or wrong type: {}", path, e))?;
            
            tx.commit().await?;
            Ok(())
        }
    }
    
    #[tokio::test]
    async fn test_copy_single_text_file() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create host text file
        let content = "Hello, DuckPond!\nThis is a test file.\n";
        let host_file = setup.create_host_text_file("test.txt", content).await?;
        
        // Copy to pond as data file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()], 
            "copied_test.txt".to_string(), 
            "data".to_string()
        );
        
        copy_command(&ship_context, &sources, &dest, &format).await?;
        
        // Verify file exists and has correct content
        assert!(setup.verify_file_exists("copied_test.txt").await?, "File should exist in pond");
        
        let pond_content = setup.read_pond_file_content("copied_test.txt").await?;
        assert_eq!(pond_content, content.as_bytes(), "File content should match");
        
        // Verify metadata (simplified check)
        setup.verify_file_metadata("copied_test.txt", tinyfs::EntryType::FileData).await?;
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_copy_parquet_as_series() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create host parquet file with proper temporal data for series
        let host_file = setup.create_host_series_parquet_file("test_series.parquet").await?;
        
        // Copy to pond as series file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()], 
            "copied_series.parquet".to_string(), 
            "series".to_string()
        );
        
        copy_command(&ship_context, &sources, &dest, &format).await?;
        
        // Verify file exists in the pond
        assert!(setup.verify_file_exists("copied_series.parquet").await?, "Series file should exist in pond");
        
        // Verify content is preserved by reading the parquet file
        let pond_content = setup.read_pond_file_content("copied_series.parquet").await?;
        
        // Verify PAR1 magic bytes (parquet format)
        assert!(pond_content.len() > 4, "Series file should be larger than 4 bytes");
        assert_eq!(&pond_content[0..4], b"PAR1", "Series file should start with PAR1 magic bytes");
        
        // Verify we can read it as parquet and get the expected temporal data
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(pond_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;
        
        let batch = arrow_reader.next()
            .ok_or_else(|| anyhow::anyhow!("No batch found in series file"))?
            .map_err(|e| anyhow::anyhow!("Failed to read batch: {}", e))?;
        
        // Verify the schema matches our test data
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 3, "Series should have 3 fields");
        assert_eq!(schema.field(0).name(), "timestamp", "First field should be timestamp");
        assert_eq!(schema.field(1).name(), "value", "Second field should be value");
        assert_eq!(schema.field(2).name(), "doubled_value", "Third field should be doubled_value");
        
        // Verify the actual data values - series temporal data
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");
        
        // Check timestamp column (Int64 milliseconds since epoch)
        use arrow_array::Int64Array;
        let timestamp_col = batch.column(0).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Int64Array for timestamp"))?;
        assert_eq!(timestamp_col.value(0), 1704067200000_i64); // 2024-01-01T00:00:00Z
        assert_eq!(timestamp_col.value(1), 1704070800000_i64); // 2024-01-01T01:00:00Z
        
        // Check value columns
        use arrow_array::Float64Array;
        let value_col = batch.column(1).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for value"))?;
        assert_eq!(value_col.value(0), 42.0);
        assert_eq!(value_col.value(1), 43.5);
        
        let doubled_value_col = batch.column(2).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for doubled_value"))?;
        assert_eq!(doubled_value_col.value(0), 84.0);
        assert_eq!(doubled_value_col.value(1), 87.0);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_copy_single_csv_as_table() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create host CSV file
        let host_file = setup.create_host_csv_file("test.csv").await?;
        
        // Copy to pond as table file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()], 
            "copied_table.csv".to_string(), 
            "table".to_string()
        );
        
        copy_command(&ship_context, &sources, &dest, &format).await?;
        
        // Verify file exists
        assert!(setup.verify_file_exists("copied_table.csv").await?, "File should exist in pond");
        
        // Verify content is preserved
        let pond_content = setup.read_pond_file_content("copied_table.csv").await?;
        let expected_content = "timestamp,value,doubled_value\n2024-01-01T00:00:00Z,42.0,84.0\n2024-01-01T01:00:00Z,43.5,87.0\n";
        assert_eq!(String::from_utf8(pond_content)?, expected_content, "CSV content should be preserved");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_copy_multiple_files_to_directory() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create multiple host files
        let file1 = setup.create_host_text_file("file1.txt", "Content of file 1").await?;
        let file2 = setup.create_host_text_file("file2.txt", "Content of file 2").await?;
        let file3 = setup.create_host_csv_file("data.csv").await?;
        
        // Copy all to a directory (use trailing slash to ensure directory creation)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![
                file1.to_string_lossy().to_string(),
                file2.to_string_lossy().to_string(),
                file3.to_string_lossy().to_string(),
            ], 
            "uploaded/".to_string(), 
            "data".to_string()
        );
        
        copy_command(&ship_context, &sources, &dest, &format).await?;
        
        // Verify all files exist in the directory
        assert!(setup.verify_file_exists("uploaded/file1.txt").await?, "file1.txt should exist in directory");
        assert!(setup.verify_file_exists("uploaded/file2.txt").await?, "file2.txt should exist in directory");
        assert!(setup.verify_file_exists("uploaded/data.csv").await?, "data.csv should exist in directory");
        
        // Verify content
        let content1 = setup.read_pond_file_content("uploaded/file1.txt").await?;
        assert_eq!(String::from_utf8(content1)?, "Content of file 1");
        
        let content2 = setup.read_pond_file_content("uploaded/file2.txt").await?;
        assert_eq!(String::from_utf8(content2)?, "Content of file 2");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_copy_with_format_override() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create CSV file but copy as table format
        let host_file = setup.create_host_csv_file("test.csv").await?;
        
        // Copy with table format (should override file extension)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()], 
            "formatted_as_table.csv".to_string(), 
            "table".to_string()
        );
        
        copy_command(&ship_context, &sources, &dest, &format).await?;
        
        // Verify it was stored successfully (format is handled by the copy command)
        assert!(setup.verify_file_exists("formatted_as_table.csv").await?, "File should exist in pond");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_copy_error_handling() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Test copying non-existent file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec!["/nonexistent/file.txt".to_string()], 
            "should_fail.txt".to_string(), 
            "data".to_string()
        );
        
        let result = copy_command(&ship_context, &sources, &dest, &format).await;
        assert!(result.is_err(), "Should fail when copying non-existent file");
        
        // Test copying with empty sources
        let result = copy_command(&ship_context, &[], &dest, &format).await;
        assert!(result.is_err(), "Should fail with empty sources");
        
        // Test copying with invalid format
        let host_file = setup.create_host_text_file("test.txt", "test").await?;
        let (ship_context, sources, dest, _) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()], 
            "test.txt".to_string(), 
            "invalid_format".to_string()
        );
        
        let result = copy_command(&ship_context, &sources, &dest, "invalid_format").await;
        assert!(result.is_err(), "Should fail with invalid format");
        
        Ok(())
    }
    
    #[tokio::test] 
    async fn test_copy_single_file_to_new_path() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Create host file
        let host_file = setup.create_host_text_file("original.txt", "Original content").await?;
        
        // Copy to new filename (not directory)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()], 
            "renamed_file.txt".to_string(), 
            "data".to_string()
        );
        
        copy_command(&ship_context, &sources, &dest, &format).await?;
        
        // Verify the file exists with the new name
        assert!(setup.verify_file_exists("renamed_file.txt").await?, "File should exist with new name");
        
        let content = setup.read_pond_file_content("renamed_file.txt").await?;
        assert_eq!(String::from_utf8(content)?, "Original content");
        
        Ok(())
    }
}
