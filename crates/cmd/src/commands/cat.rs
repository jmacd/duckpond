use anyhow::Result;
use std::io::{self, Write};
use std::sync::Arc;

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::log_debug;

// DataFusion streaming with SeriesTable integration (production-ready)
async fn display_file_series_as_table(root: &tinyfs::WD, path: &str, _data_path: &str, time_start: Option<i64>, time_end: Option<i64>) -> Result<()> {
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use futures::stream::TryStreamExt;
    use arrow_cast::pretty;
    
    // Get the file versions first
    let file_versions = root.list_file_versions(path).await
        .map_err(|e| anyhow::anyhow!("Failed to list file versions: {}", e))?;
    
    println!("Found {} versions in series", file_versions.len());
    
    let mut all_batches = Vec::new();
    let mut unified_schema: Option<Arc<arrow::datatypes::Schema>> = None;
    
    // Process each version individually
    for version_info in file_versions.iter() {
        println!("\n=== Version {} (size: {} bytes) ===", version_info.version, version_info.size);
        
        // Read this specific version
        let version_data = root.read_file_version(path, Some(version_info.version)).await
            .map_err(|e| anyhow::anyhow!("Failed to read version {}: {}", version_info.version, e))?;
        
        // Create a cursor for the Parquet data
        let cursor = std::io::Cursor::new(version_data);
        
        // Parse Parquet file
        match ParquetRecordBatchStreamBuilder::new(cursor).await {
            Ok(builder) => {
                let schema = builder.schema();
                
                // Ensure schema consistency
                if let Some(ref existing_schema) = unified_schema {
                    if schema != existing_schema {
                        println!("Warning: Schema mismatch in version {}", version_info.version);
                        continue;
                    }
                } else {
                    unified_schema = Some(schema.clone());
                }
                
                let mut stream = builder.build().map_err(|e| anyhow::anyhow!("Failed to build stream: {}", e))?;
                
                while let Some(batch) = stream.try_next().await.map_err(|e| anyhow::anyhow!("Stream error: {}", e))? {
                    // Apply time filtering if specified
                    let filtered_batch = if time_start.is_some() || time_end.is_some() {
                        apply_time_filter(&batch, time_start, time_end)?
                    } else {
                        batch
                    };
                    
                    if filtered_batch.num_rows() > 0 {
                        all_batches.push(filtered_batch);
                    }
                }
            },
            Err(e) => {
                println!("Warning: Failed to parse version {}: {}", version_info.version, e);
            }
        }
    }
    
    // Display all batches together
    if all_batches.is_empty() {
        println!("No data found after filtering");
    } else {
        println!("\n=== Combined Data ===");
        let pretty_output = pretty::pretty_format_batches(&all_batches)
            .map_err(|e| anyhow::anyhow!("Failed to format batches: {}", e))?;
        println!("{}", pretty_output);
        
        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        println!("\nSummary: {} versions, {} total rows", file_versions.len(), total_rows);
    }
    
    Ok(())
}

fn find_bytes_in_slice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn apply_time_filter(batch: &arrow::record_batch::RecordBatch, time_start: Option<i64>, time_end: Option<i64>) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{Int64Array, TimestampMillisecondArray, Array};
    
    // Look for timestamp column
    let schema = batch.schema();
    let timestamp_col_idx = schema.fields().iter().position(|field| {
        matches!(field.name().to_lowercase().as_str(), "timestamp" | "time" | "event_time" | "ts" | "datetime")
    });
    
    if let Some(col_idx) = timestamp_col_idx {
        use arrow::compute::filter;
        use arrow::array::BooleanArray;
        
        let column = batch.column(col_idx);
        
        // Handle different timestamp types
        let filter_mask = if let Some(timestamp_array) = column.as_any().downcast_ref::<Int64Array>() {
            let mut mask_values = vec![true; batch.num_rows()];
            
            for i in 0..batch.num_rows() {
                if timestamp_array.is_valid(i) {
                    let ts = timestamp_array.value(i);
                    let pass_start = time_start.map_or(true, |start| ts >= start);
                    let pass_end = time_end.map_or(true, |end| ts <= end);
                    mask_values[i] = pass_start && pass_end;
                } else {
                    mask_values[i] = false;
                }
            }
            
            BooleanArray::from(mask_values)
        } else if let Some(timestamp_array) = column.as_any().downcast_ref::<TimestampMillisecondArray>() {
            let mut mask_values = vec![true; batch.num_rows()];
            
            for i in 0..batch.num_rows() {
                if timestamp_array.is_valid(i) {
                    let ts = timestamp_array.value(i);
                    let pass_start = time_start.map_or(true, |start| ts >= start);
                    let pass_end = time_end.map_or(true, |end| ts <= end);
                    mask_values[i] = pass_start && pass_end;
                } else {
                    mask_values[i] = false;
                }
            }
            
            BooleanArray::from(mask_values)
        } else {
            // Unknown timestamp type, don't filter
            return Ok(batch.clone());
        };
        
        // Apply filter to all columns
        let filtered_columns: Result<Vec<_>, _> = batch.columns().iter()
            .map(|col| filter(col, &filter_mask))
            .collect();
        
        let filtered_columns = filtered_columns.map_err(|e| anyhow::anyhow!("Filter error: {}", e))?;
        
        let filtered_batch = arrow::record_batch::RecordBatch::try_new(batch.schema(), filtered_columns)
            .map_err(|e| anyhow::anyhow!("Failed to create filtered batch: {}", e))?;
        
        Ok(filtered_batch)
    } else {
        // No timestamp column found, return original batch
        Ok(batch.clone())
    }
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
            let data_path = ship.data_path();
            return display_file_series_as_table(&root, path, &data_path, time_start, time_end).await;
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
