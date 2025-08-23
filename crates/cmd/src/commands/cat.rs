use anyhow::Result;
use std::io::{self, Write};
use std::sync::Arc;

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::*;

/// Cat file with optional SQL query
pub async fn cat_command_with_sql(
    ship_context: &ShipContext,
    path: &str,
    filesystem: FilesystemChoice,
    display: &str,
    output: Option<&mut String>,
    time_start: Option<i64>,
    time_end: Option<i64>,
    sql_query: Option<&str>,
) -> Result<()> {
    debug!("cat_command_with_sql called with path: {path}, sql_query: {sql_query}", path: path, sql_query: sql_query.unwrap_or("None"));
    
    // For now, only support data filesystem - control filesystem access would require different API
    if filesystem == FilesystemChoice::Control {
        return Err(anyhow::anyhow!("Control filesystem access not yet implemented for cat command"));
    }
    
    let mut ship = ship_context.open_pond().await?;
    
    // Use manual transaction pattern for complex DataFusion setup
    let tx = ship.begin_transaction(ship_context.original_args.clone()).await?;
    let fs = &*tx; // StewardTransactionGuard derefs to FS
    
    let root = fs.root().await?;
    
    // Check if this should use DataFusion SQL interface based on entry type
    let metadata_result = root.metadata_for_path(path).await;
    
    let should_use_datafusion = match &metadata_result {
        Ok(metadata) => {
            let entry_type_str = format!("{:?}", metadata.entry_type);
            debug!("File entry type: {entry_type_str}", entry_type_str: entry_type_str);
            // Use DataFusion for both file:series and file:table
            matches!(metadata.entry_type, tinyfs::EntryType::FileSeries | tinyfs::EntryType::FileTable)
        },
        Err(e) => {
            let error_str = format!("{}", e);
            debug!("Failed to get metadata: {error_str}", error_str: error_str);
            false // If we can't get metadata, proceed with normal behavior
        }
    };
    
    if should_use_datafusion {
        // Use DataFusion SQL interface for file:series and file:table
        let effective_sql_query = sql_query.unwrap_or("SELECT * FROM series");
        debug!("Using DataFusion SQL interface for: {path}", path: path);
        
        // Get the node_id from the path for proper table creation
        let node_path = root.get_node_path(path).await
            .map_err(|e| anyhow::anyhow!("Failed to resolve path to node_id: {}", e))?;
        let node_id = node_path.node.id().await;
        let node_id_str = node_id.to_hex_string();
        
        // Pass the entry type we already determined
        let entry_type = metadata_result.unwrap().entry_type;
        
        // Execute DataFusion query within the transaction
        display_file_with_sql_and_node_id(&tx, fs, path, &node_id_str, entry_type, time_start, time_end, Some(effective_sql_query), output).await?;
        
        // Commit transaction and return
        tx.commit().await?;
        return Ok(());
    }
    
    // Check if we should use table display for regular files
    if display == "table" {
        if output.is_some() {
            return Err(anyhow::anyhow!("Output capture not supported for table display mode"));
        }
        // Display as table (for FileTable entries)
        display_regular_file_as_table(&root, path).await?;
        tx.commit().await?;
        return Ok(());
    }
    
    // Default/raw display behavior - use streaming for better memory efficiency
    stream_file_to_stdout(&root, path, output).await?;
    
    // Commit transaction
    tx.commit().await?;
    Ok(())
}

// DataFusion SQL interface for file:series and file:table queries with node_id (more efficient)
async fn display_file_with_sql_and_node_id(
    tx: &steward::StewardTransactionGuard<'_>, 
    fs: &tinyfs::FS,
    path: &str, 
    node_id: &str, 
    entry_type: tinyfs::EntryType, 
    time_start: Option<i64>, 
    time_end: Option<i64>, 
    sql_query: Option<&str>,
    output: Option<&mut String>,
) -> Result<()> {
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    
    // Create DataFusion session context
    let ctx = SessionContext::new();
    
    // Get TinyFS root from the provided FS (already in transaction)
    let tinyfs_root = fs.root().await?;
    
    // Get the data persistence layer from the transaction guard for queries
    let data_persistence = tx.data_persistence()
        .map_err(|e| anyhow::anyhow!("Failed to access data persistence: {}", e))?;
    
    // Create MetadataTable using the DeltaTable from persistence
    let metadata_table = tlogfs::query::MetadataTable::new(data_persistence.table().clone());
    
    // Use the entry type passed from the caller
    let is_series = entry_type == tinyfs::EntryType::FileSeries;
    
    // Create provider using unified architecture
    let mut provider = if is_series {
        tlogfs::query::UnifiedTableProvider::create_series_table_with_tinyfs_and_node_id(
            path.to_string(), // Use actual file path instead of placeholder
            node_id.to_string(), 
            metadata_table, 
            Arc::new(tinyfs_root)
        )
    } else {
        tlogfs::query::UnifiedTableProvider::create_table_table_with_tinyfs_and_node_id(
            path.to_string(),
            node_id.to_string(),
            metadata_table,
            Arc::new(tinyfs_root)
        )
    };
    
    // Load the schema from the actual Parquet files before registering
    // This is required for DataFusion to validate column references and enable predicate pushdown
    provider.load_schema_from_data().await
        .map_err(|e| anyhow::anyhow!("Failed to load schema from data: {}", e))?;
    
    // Register the unified provider with DataFusion
    ctx.register_table(TableReference::bare("series"), Arc::new(provider))
        .map_err(|e| anyhow::anyhow!("Failed to register UnifiedTableProvider: {}", e))?;
    
    // Build SQL query with time filtering
    let base_query = sql_query.unwrap_or("SELECT * FROM series");
    let final_query = if time_start.is_some() || time_end.is_some() {
        // Add time range predicates to the WHERE clause
        let mut conditions = Vec::new();
        
        if let Some(start) = time_start {
            conditions.push(format!("timestamp >= {}", start));
        }
        if let Some(end) = time_end {
            conditions.push(format!("timestamp <= {}", end));
        }
        
        let time_filter = conditions.join(" AND ");
        
        // If the user query already has a WHERE clause, append with AND
        if base_query.to_lowercase().contains("where") {
            format!("{} AND {}", base_query, time_filter)
        } else {
            format!("{} WHERE {}", base_query, time_filter)
        }
    } else {
        base_query.to_string()
    };
    
    debug!("Executing SQL query with node_id {node_id}: {final_query}", node_id: node_id, final_query: final_query);
    
    // Execute the SQL query with automatic predicate pushdown
    let dataframe = ctx.sql(&final_query).await
        .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?;
    
    // Collect and display results
    let batches = dataframe.collect().await
        .map_err(|e| anyhow::anyhow!("Failed to collect query results: {}", e))?;
    
    if batches.is_empty() {
        let msg = "No data found after filtering";
        if let Some(out) = output {
            out.push_str(msg);
            out.push('\n');
        } else {
            println!("{}", msg);
        }
    } else {
        let pretty_output = arrow_cast::pretty::pretty_format_batches(&batches)
            .map_err(|e| anyhow::anyhow!("Failed to format results: {}", e))?;
        
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let summary = format!("\nSummary: {} total rows", total_rows);
        
        if let Some(out) = output {
            out.push_str(&pretty_output.to_string());
            out.push_str(&summary);
        } else {
            println!("{}", pretty_output);
            println!("{}", summary);
        }
    }
    
    Ok(())
}

// Streaming table display functionality for regular (non-series) files
async fn display_regular_file_as_table(root: &tinyfs::WD, path: &str) -> Result<()> {
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use futures::stream::TryStreamExt;
    use std::io::{self, Write};
    
    // Use the async_reader_path which now returns AsyncRead + AsyncSeek
    let seek_reader = root.async_reader_path(path).await
        .map_err(|e| anyhow::anyhow!("Failed to get seekable reader: {}", e))?;
    
    // Build streaming Parquet reader
    let builder = ParquetRecordBatchStreamBuilder::new(seek_reader)
        .await
        .map_err(|e| anyhow::anyhow!("Not a valid Parquet file: {}", e))?;
    
    let mut stream = builder.build()
        .map_err(|e| anyhow::anyhow!("Failed to create Parquet stream: {}", e))?;
    
    // Track if we've seen any batches
    let mut batch_count = 0;
    let mut schema_printed = false;
    
    // Stream and display each batch individually for memory efficiency
    while let Some(batch_result) = stream.try_next().await
        .map_err(|e| anyhow::anyhow!("Failed to read Parquet batch: {}", e))? {
        
        batch_count += 1;
        
        // Print schema header only once
        if !schema_printed {
            println!("Parquet Schema:");
            for (i, field) in batch_result.schema().fields().iter().enumerate() {
                println!("  {}: {} ({})", i, field.name(), field.data_type());
            }
            println!();
            schema_printed = true;
        }
        
        println!("Batch {} ({} rows):", batch_count, batch_result.num_rows());
        
        // Display each batch individually - this is memory efficient
        let table_str = arrow_cast::pretty::pretty_format_batches(&[batch_result])
            .map_err(|e| anyhow::anyhow!("Failed to format batch: {}", e))?;
        
        print!("{}", table_str);
        io::stdout().flush()?;
        println!(); // Extra line between batches
    }
    
    if batch_count == 0 {
        println!("Empty Parquet file");
    } else {
        println!("Total batches: {}", batch_count);
    }
    
    Ok(())
}

// Stream copy function for non-display mode
async fn stream_file_to_stdout(root: &tinyfs::WD, path: &str, mut output: Option<&mut String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    use std::pin::Pin;
    
    let mut reader: Pin<Box<dyn tinyfs::AsyncReadSeek>> = root.async_reader_path(path).await
        .map_err(|e| anyhow::anyhow!("Failed to open file for reading: {}", e))?;
    
    // Stream in chunks rather than loading entire file
    let mut buffer = vec![0u8; 8192]; // 8KB chunks
    loop {
        let bytes_read = reader.read(&mut buffer).await
            .map_err(|e| anyhow::anyhow!("Failed to read from file: {}", e))?;
        if bytes_read == 0 {
            break; // EOF
        }
        if let Some(out) = output.as_deref_mut() {
            out.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));
        } else {
            io::stdout().write_all(&buffer[..bytes_read])
                .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use tempfile::TempDir;
    use arrow_array::record_batch;
    use tinyfs::arrow::SimpleParquetExt;
    
    /// Test setup helper - creates pond and returns context for testing
    struct TestSetup {
        temp_dir: TempDir,
        pond_path: std::path::PathBuf,
        ship_context: ShipContext,
        test_content: String,
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
            
            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(pond_path.clone()), init_args.clone());
            
            // Initialize the pond
            init_command(&ship_context).await?;
            
            let test_content = "timestamp,value\n2024-01-01T00:00:00Z,42.0\n2024-01-01T01:00:00Z,43.5\n";
            
            Ok(TestSetup {
                temp_dir,
                pond_path,
                ship_context,
                test_content: test_content.to_string(),
            })
        }
        
        /// Write test data as raw text file (file:data type)
        async fn write_text_file(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;
            
            let test_content = self.test_content.clone();
            let filename = filename.to_string();
            
            ship.transact(vec!["test".to_string(), "write".to_string()], |_tx, fs| Box::pin(async move {
                let root = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                root.write_file_path_from_slice(&filename, test_content.as_bytes()).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                Ok(())
            })).await.map_err(|e| anyhow::anyhow!("Failed to write test data: {}", e))?;
            
            Ok(())
        }
        
        /// Write test data as parquet table (file:table type)
        async fn write_parquet_table(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;
            
            let filename = filename.to_string();
            
            ship.transact(vec!["test".to_string(), "write_table".to_string()], |_tx, fs| Box::pin(async move {
                let root = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create Arrow RecordBatch from our test data
                let batch = record_batch!(
                    ("timestamp", Utf8, ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                    ("value", Float64, [42.0_f64, 43.5_f64])
                ).map_err(|e| steward::StewardError::DataInit(
                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Arrow error: {}", e)))
                ))?;
                
                // Write as parquet table using SimpleParquetExt
                root.write_parquet(&filename, &batch, tinyfs::EntryType::FileTable).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                Ok(())
            })).await.map_err(|e| anyhow::anyhow!("Failed to write parquet table: {}", e))?;
            
            Ok(())
        }
        
        /// Create ShipContext for cat command
        fn cat_context(&self, filename: &str) -> ShipContext {
            let cat_args = vec!["pond".to_string(), "cat".to_string(), filename.to_string()];
            ShipContext::new(Some(self.pond_path.clone()), cat_args)
        }
        
        /// Create expected DataFusion output for our test data using the same formatting path
        fn expected_datafusion_output(&self) -> Result<String> {
            // Create the same RecordBatch we use in write_parquet_table
            let batch = record_batch!(
                ("timestamp", Utf8, ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                ("value", Float64, [42.0_f64, 43.5_f64])
            ).map_err(|e| anyhow::anyhow!("Arrow error: {}", e))?;
            
            // Use the same formatting logic as the cat command
            let pretty_output = arrow_cast::pretty::pretty_format_batches(&[batch])
                .map_err(|e| anyhow::anyhow!("Failed to format results: {}", e))?;
            let summary = format!("\nSummary: 2 total rows");
            
            Ok(format!("{}{}", pretty_output.to_string(), summary))
        }
    }
    
    #[tokio::test]
    async fn test_cat_command_text_file() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Write text file
        setup.write_text_file("test_data.csv").await?;
        
        // Test cat command with output capture
        let cat_context = setup.cat_context("test_data.csv");
        let mut output_buffer = String::new();
        
        cat_command_with_sql(
            &cat_context,
            "test_data.csv",
            FilesystemChoice::Data,
            "raw",
            Some(&mut output_buffer),
            None, None, None,
        ).await?;
        
        assert_eq!(output_buffer, setup.test_content, 
                  "Cat command output should exactly match file content");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_cat_command_parquet_table() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Write parquet table file
        setup.write_parquet_table("test_table.parquet").await?;
        
        // Get expected output from our baseline formatter
        let expected_output = setup.expected_datafusion_output()?;
        
        // Test cat command - this should use DataFusion SQL interface
        let cat_context = setup.cat_context("test_table.parquet");
        let mut output_buffer = String::new();
        
        cat_command_with_sql(
            &cat_context,
            "test_table.parquet",
            FilesystemChoice::Data,
            "raw",
            Some(&mut output_buffer),
            None, None, None,
        ).await?;
        
        // Test for exact equality with baseline
        assert_eq!(output_buffer.trim(), expected_output.trim(), 
                  "Cat command DataFusion output should exactly match baseline formatting");
        
        println!("✅ DataFusion SQL output matches baseline!");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_cat_command_parquet_table_with_sql() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Write parquet table file
        setup.write_parquet_table("test_table.parquet").await?;
        
        // Test cat command with custom SQL query
        let cat_args = vec![
            "pond".to_string(), 
            "cat".to_string(), 
            "test_table.parquet".to_string(),
            "--sql".to_string(),
            "SELECT timestamp, value * 2 as doubled_value FROM series WHERE value > 42".to_string()
        ];
        let cat_context = ShipContext::new(Some(setup.pond_path.clone()), cat_args);
        let mut output_buffer = String::new();
        
        cat_command_with_sql(
            &cat_context,
            "test_table.parquet",
            FilesystemChoice::Data,
            "raw",
            Some(&mut output_buffer),
            None, None, 
            Some("SELECT timestamp, value * 2 as doubled_value FROM series WHERE value > 42"),
        ).await?;
        
        // Verify the output contains expected elements from the SQL query
        assert!(output_buffer.contains("doubled_value"), 
               "Output should contain computed column name");
        assert!(output_buffer.contains("87"), 
               "Output should contain doubled value: 43.5 * 2 = 87");
        assert!(!output_buffer.contains("42"), 
               "Output should not contain filtered value (42)");
        assert!(output_buffer.contains("Summary: 1 total rows"), 
               "Output should contain filtered row count");
        
        println!("✅ SQL query filtering and computation works!");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_cat_command_nonexistent_file() -> Result<()> {
        let setup = TestSetup::new().await?;
        
        // Try to cat a non-existent file
        let cat_context = setup.cat_context("nonexistent.txt");
        let mut output_buffer = String::new();
        
        let result = cat_command_with_sql(
            &cat_context,
            "nonexistent.txt", 
            FilesystemChoice::Data,
            "raw",
            Some(&mut output_buffer),
            None, None, None,
        ).await;
        
        // Should fail with appropriate error
        assert!(result.is_err(), "Cat command should fail for non-existent file");
        
        Ok(())
    }
}

