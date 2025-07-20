use anyhow::Result;
use std::io::{self, Write};

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::log_debug;

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
    
    // EXPERIMENTAL PARQUET: Check if we should use table display
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
