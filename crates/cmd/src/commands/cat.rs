use anyhow::Result;
use std::io::{self, Write};
use std::sync::Arc;

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::log_debug;


// DataFusion SQL interface for file:series and file:table queries with node_id (more efficient)
async fn display_file_with_sql_and_node_id(ship: &steward::Ship, path: &str, node_id: &str, time_start: Option<i64>, time_end: Option<i64>, sql_query: Option<&str>) -> Result<()> {
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    
    // Create DataFusion session context
    let ctx = SessionContext::new();
    
    // Get TinyFS root for file access
    let tinyfs_root = ship.data_fs().root().await?;
    
    // Create MetadataTable for metadata queries (no IPC deserialization)
    // We need to access the Delta Lake table for the data filesystem
    let data_path = ship.data_path();
    let delta_manager = tlogfs::DeltaTableManager::new();
    let metadata_table = tlogfs::query::MetadataTable::new(data_path.clone(), delta_manager);
    
    // Determine the entry type to choose the right table provider
    let metadata_result = tinyfs_root.metadata_for_path(path).await;
    let is_series = match &metadata_result {
        Ok(metadata) => metadata.entry_type == tinyfs::EntryType::FileSeries,
        Err(_) => true, // Default to series for backward compatibility
    };
    
    // UNIFIED APPROACH: Create provider using unified architecture
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
    
    log_debug!("Executing SQL query with node_id {node_id}: {final_query}", node_id: node_id, final_query: final_query);
    
    // Execute the SQL query with automatic predicate pushdown
    let dataframe = ctx.sql(&final_query).await
        .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?;
    
    // Collect and display results
    let batches = dataframe.collect().await
        .map_err(|e| anyhow::anyhow!("Failed to collect query results: {}", e))?;
    
    if batches.is_empty() {
        println!("No data found after filtering");
    } else {
        println!("=== SQL Query Results ===");
        let pretty_output = arrow_cast::pretty::pretty_format_batches(&batches)
            .map_err(|e| anyhow::anyhow!("Failed to format results: {}", e))?;
        println!("{}", pretty_output);
        
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("\nSummary: {} total rows", total_rows);
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
async fn stream_file_to_stdout(root: &tinyfs::WD, path: &str) -> Result<()> {
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
        
        io::stdout().write_all(&buffer[..bytes_read])
            .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
    }
    
    Ok(())
}

/// Cat file with optional SQL query
pub async fn cat_command_with_sql(ship_context: &ShipContext, path: &str, filesystem: FilesystemChoice, display: &str, time_start: Option<i64>, time_end: Option<i64>, sql_query: Option<&str>) -> Result<()> {
    log_debug!("cat_command_with_sql called with path: {path}, sql_query: {sql_query}", path: path, sql_query: sql_query.unwrap_or("None"));
    
    let ship = ship_context.create_ship().await?;
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    let root = fs.root().await?;    // Check if this is a file:series that should use DataFusion SQL interface
    let metadata_result = root.metadata_for_path(path).await;
    
    let should_use_datafusion = match &metadata_result {
        Ok(metadata) => {
            let entry_type_str = format!("{:?}", metadata.entry_type);
            log_debug!("File entry type: {entry_type_str}", entry_type_str: entry_type_str);
            // Use DataFusion for both file:series and file:table
            metadata.entry_type == tinyfs::EntryType::FileSeries || metadata.entry_type == tinyfs::EntryType::FileTable
        },
        Err(e) => {
            let error_str = format!("{}", e);
            log_debug!("Failed to get metadata: {error_str}", error_str: error_str);
            false // If we can't get metadata, proceed with normal behavior
        }
    };
    
    log_debug!("Should use DataFusion: {should_use_datafusion}", should_use_datafusion: should_use_datafusion);
    
    if should_use_datafusion {
        // Always use DataFusion SQL interface for file:series and file:table
        // If no query specified, use "SELECT * FROM series"
        let effective_sql_query = sql_query.unwrap_or("SELECT * FROM series");
        
        log_debug!("Using DataFusion SQL interface for file:series/file:table: {path}", path: path);
        
        // Get the node_id from the path for proper SeriesTable creation
        match root.get_node_path(path).await {
            Ok(node_path) => {
                let node_id = node_path.node.id().await;
                let node_id_str = node_id.to_hex_string();
                log_debug!("Resolved node_id for SQL query: {node_id_str}", node_id_str: node_id_str);
                return display_file_with_sql_and_node_id(&ship, path, &node_id_str, time_start, time_end, Some(effective_sql_query)).await;
            },
            Err(e) => {
                log_debug!("Failed to get node_path for {path}: {e}", path: path, e: e);
                return Err(anyhow::anyhow!("Failed to resolve path to node_id: {}", e));
            }
        }
    }
    
    // Check if we should use table display for regular files
    if display == "table" {
        // Try to read as table first (for FileTable entries)
        match display_regular_file_as_table(&root, path).await {
            Ok(()) => return Ok(()), // Successfully displayed as table
            Err(_) => {
                // Fall back to raw display
                log_debug!("Failed to display as table, falling back to raw display");
            }
        }
    }
    
    // Default/raw display behavior - use streaming for better memory efficiency
    stream_file_to_stdout(&root, path).await
}
