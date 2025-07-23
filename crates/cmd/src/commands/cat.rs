use anyhow::Result;
use std::io::{self, Write};

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::log_debug;

// EXPERIMENTAL PARQUET: Streaming table display functionality for file:series
async fn try_display_file_series_as_table(root: &tinyfs::WD, path: &str) -> Result<()> {
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use futures::stream::TryStreamExt;
    use std::io::Cursor;
    
    // Get list of all versions first
    let versions = root.list_file_versions(path).await
        .map_err(|e| anyhow::anyhow!("Failed to list file versions: {}", e))?;
    
    if versions.is_empty() {
        println!("Empty file:series");
        return Ok(());
    }
    
    let version_count = versions.len();
    log_debug!("Found {version_count} versions to display as table", version_count: version_count);
    
    // Track accumulated batches for final display
    let mut all_batches = Vec::new();
    let mut schema_opt = None;
    
    // Read each version individually and collect all batches
    // Note: versions are returned in timestamp DESC order (newest first), 
    // but we want to display chronologically (oldest first)
    for (version_idx, version_info) in versions.iter().rev().enumerate() {
        let version_num = version_idx + 1; // Count from 1, oldest first
        let version_timestamp = version_info.timestamp;
        let actual_version = version_info.version;
        log_debug!("Reading version {version_num} (actual_version={actual_version}, timestamp {version_timestamp})", version_num: version_num, actual_version: actual_version, version_timestamp: version_timestamp);
        
        // Read the actual version number from the version info
        let version_content = root.read_file_version(path, Some(actual_version)).await
            .map_err(|e| anyhow::anyhow!("Failed to read version {}: {}", actual_version, e))?;
        
        if version_content.is_empty() {
            let empty_version_id = version_info.version;
            log_debug!("Version {empty_version_id} is empty, skipping", empty_version_id: empty_version_id);
            continue;
        }
        
        // Create a cursor from this version's content
        let cursor = Cursor::new(version_content);
        
        // Build streaming Parquet reader for this version
        let builder = ParquetRecordBatchStreamBuilder::new(cursor)
            .await
            .map_err(|e| anyhow::anyhow!("Version {} is not a valid Parquet file: {}", version_info.version, e))?;
        
        let mut stream = builder.build()
            .map_err(|e| anyhow::anyhow!("Failed to create Parquet stream for version {}: {}", version_info.version, e))?;
        
        // Collect all batches from this version
        while let Some(batch_result) = stream.try_next().await
            .map_err(|e| anyhow::anyhow!("Failed to read Parquet batch from version {}: {}", version_info.version, e))? {
            
            // Store schema from first batch
            if schema_opt.is_none() {
                schema_opt = Some(batch_result.schema());
            }
            
            all_batches.push(batch_result);
        }
    }
    
    if all_batches.is_empty() {
        println!("No data found in file:series versions");
        return Ok(());
    }
    
    // Print schema header
    if let Some(schema) = &schema_opt {
        println!("File:Series Parquet Schema ({} versions):", versions.len());
        for (i, field) in schema.fields().iter().enumerate() {
            println!("  {}: {} ({})", i, field.name(), field.data_type());
        }
        println!();
    }
    
    // Calculate total rows
    let total_rows: usize = all_batches.iter().map(|batch| batch.num_rows()).sum();
    println!("Total rows across all versions: {}", total_rows);
    println!();
    
    // Display all batches as a single unified table
    let table_str = arrow_cast::pretty::pretty_format_batches(&all_batches)
        .map_err(|e| anyhow::anyhow!("Failed to format combined batches: {}", e))?;
    
    print!("{}", table_str);
    io::stdout().flush()?;
    
    println!();
    println!("File:Series Summary: {} versions, {} total rows", versions.len(), total_rows);
    
    Ok(())
}

// EXPERIMENTAL PARQUET: Streaming table display functionality
async fn try_display_as_table_streaming(root: &tinyfs::WD, path: &str) -> Result<()> {
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

// EXPERIMENTAL PARQUET: Stream copy function for non-display mode
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
pub async fn cat_command(ship_context: &ShipContext, path: &str, filesystem: FilesystemChoice, display: &str) -> Result<()> {
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
        // For file:series, handle table display specially
        if display == "table" {
            log_debug!("Attempting to display file:series as table for: {path}", path: path);
            match try_display_file_series_as_table(&root, path).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    log_debug!("Failed to display file:series as table: {e}, falling back to raw");
                    // Fall through to raw display
                }
            }
        }
        
        // Raw display for file:series (or fallback from failed table display)
        log_debug!("Attempting to read all file versions for: {path}", path: path);
        match root.read_all_file_versions(path).await {
            Ok(all_content) => {
                let size = all_content.len();
                log_debug!("Successfully read all versions, total size: {size} bytes", size: size);
                // Output all versions as raw data
                io::stdout().write_all(&all_content)
                    .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
                return Ok(());
            },
            Err(e) => {
                log_debug!("Failed to read all versions for file:series, falling back to latest version: {e}");
                // Fall through to normal single-version behavior
            }
        }
    }
    
    // EXPERIMENTAL PARQUET: Check if we should use table display for regular files
    if display == "table" {
        // Try to read as table first (for FileTable entries)
        match try_display_as_table_streaming(&root, path).await {
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
