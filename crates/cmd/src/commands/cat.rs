use anyhow::Result;
use std::io::{self, Write};

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::log_debug;

// DataFusion streaming with SeriesTable integration (production-ready)
async fn display_file_series_as_table(root: &tinyfs::WD, path: &str, time_start: Option<i64>, time_end: Option<i64>) -> Result<()> {
    use tlogfs::query::{SeriesTable, OperationsTable};
    use tlogfs::delta::DeltaTableManager;
    use datafusion::execution::context::SessionContext;
    use futures::stream::TryStreamExt;
    use arrow_cast::pretty;
    use std::sync::Arc;
    
    // Create DataFusion session context
    let ctx = SessionContext::new();
    
    // Create SeriesTable for time-based filtering with TinyFS root access
    let delta_manager = DeltaTableManager::new();
    let ops_table = OperationsTable::new(path.to_string(), delta_manager);
    let series_table = SeriesTable::new_with_tinyfs(path.to_string(), ops_table, Arc::new(root.clone()));
    
    // Register the SeriesTable as a DataFusion table
    ctx.register_table("series_data", Arc::new(series_table))
        .map_err(|e| anyhow::anyhow!("Failed to register SeriesTable: {}", e))?;
    
    // Build the SQL query with optional time filtering
    let sql = match (time_start, time_end) {
        (Some(start), Some(end)) => {
            format!("SELECT * FROM series_data WHERE timestamp >= {} AND timestamp <= {}", start, end)
        },
        (Some(start), None) => {
            format!("SELECT * FROM series_data WHERE timestamp >= {}", start)
        },
        (None, Some(end)) => {
            format!("SELECT * FROM series_data WHERE timestamp <= {}", end)
        },
        (None, None) => {
            // No time filtering - return all data
            "SELECT * FROM series_data".to_string()
        }
    };
    
    log_debug!("Executing DataFusion query: {sql}", sql: sql);
    
    // Execute the query to get a streaming result
    let df = ctx.sql(&sql).await
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))?;
    
    // Get the execution plan and create a stream
    let stream = df.execute_stream().await
        .map_err(|e| anyhow::anyhow!("Failed to execute stream: {}", e))?;
    
    // Stream through record batches without memory accumulation
    let mut batch_count = 0;
    let mut schema_printed = false;
    let mut total_rows = 0;
    
    let mut stream = stream;
    while let Some(batch_result) = stream.try_next().await
        .map_err(|e| anyhow::anyhow!("Failed to read batch from DataFusion stream: {}", e))? {
        
        batch_count += 1;
        total_rows += batch_result.num_rows();
        
        // Print header only once
        if !schema_printed {
            let filter_desc = if time_start.is_some() || time_end.is_some() {
                "Time-Filtered "
            } else {
                ""
            };
            println!("{}Series Data (streaming):", filter_desc);
            println!();
            schema_printed = true;
        }
        
        // Stream each batch individually - the issue is that each batch gets its own table header
        // This is where we need the SeriesExecutionPlan to combine Parquet files logically
        let batch_str = pretty::pretty_format_batches(&[batch_result])
            .map_err(|e| anyhow::anyhow!("Failed to format batch: {}", e))?;
        
        print!("{}", batch_str);
        io::stdout().flush()?;
    }
    
    if batch_count == 0 {
        println!("No data found");
    } else {
        println!();
        let summary = match (time_start, time_end) {
            (Some(start), Some(end)) => format!("Summary: {} batches, {} total rows, time range [{}, {}]", batch_count, total_rows, start, end),
            (Some(start), None) => format!("Summary: {} batches, {} total rows, time >= {}", batch_count, total_rows, start),
            (None, Some(end)) => format!("Summary: {} batches, {} total rows, time <= {}", batch_count, total_rows, end),
            (None, None) => format!("Summary: {} batches, {} total rows", batch_count, total_rows),
        };
        println!("{}", summary);
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

/// Cat file
pub async fn cat_command(ship_context: &ShipContext, path: &str, filesystem: FilesystemChoice, display: &str, time_start: Option<i64>, time_end: Option<i64>) -> Result<()> {
    log_debug!("Reading file from pond: {path}", path: path);
    
    let ship = ship_context.create_ship().await?;
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    let root = fs.root().await?;
    
    // Check if this is a file:series that should show all versions
    let metadata_result = root.metadata_for_path(path).await;
    
    let should_show_all_versions = match &metadata_result {
        Ok(metadata) => {
            let entry_type_str = format!("{:?}", metadata.entry_type);
            log_debug!("File entry type: {entry_type_str}", entry_type_str: entry_type_str);
            metadata.entry_type == tinyfs::EntryType::FileSeries
        },
        Err(e) => {
            let error_str = format!("{}", e);
            log_debug!("Failed to get metadata: {error_str}", error_str: error_str);
            false // If we can't get metadata, proceed with normal behavior
        }
    };
    
    log_debug!("Should show all versions: {should_show_all_versions}", should_show_all_versions: should_show_all_versions);
    
    if should_show_all_versions {
        // For file:series, use unified DataFusion table display
        if display == "table" {
            log_debug!("Attempting to display file:series as table for: {path}", path: path);
            return display_file_series_as_table(&root, path, time_start, time_end).await;
        }
        
        // Raw display for file:series
        log_debug!("Attempting to read all file versions for: {path}", path: path);
        match root.read_all_file_versions(path).await {
            Ok(all_content) => {
                let size = all_content.len();
                log_debug!("Successfully read all versions, total size: {size} bytes", size: size);
                io::stdout().write_all(&all_content)
                    .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
                return Ok(());
            },
            Err(e) => {
                log_debug!("Failed to read all versions for file:series: {e}");
                return Err(anyhow::anyhow!("Failed to read file:series: {}", e));
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
