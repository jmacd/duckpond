use anyhow::{Result, anyhow};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::common::{FilesystemChoice, ShipContext, FileInfoVisitor};
use diagnostics::*;

/// Data structure representing a temporal overlap between two files
#[derive(Debug, Clone)]
pub struct TemporalOverlap {
    pub file_a: String,
    pub file_b: String,
    pub node_a: String,
    pub node_b: String,
    pub overlap_start: i64,
    pub overlap_end: i64,
    pub overlap_duration_ms: i64,
}

/// Check for temporal overlaps between file:series in the pond
pub async fn check_overlaps_command(
    ship_context: &ShipContext,
    filesystem: &FilesystemChoice,
    pattern: Option<&str>,
    verbose: bool,
) -> Result<()> {
    debug!("check_overlaps_command called with pattern and verbose flag");
    
    if *filesystem == FilesystemChoice::Control {
        return Err(anyhow!("Control filesystem access not supported for overlap detection"));
    }

    let mut ship = ship_context.open_pond().await?;
    let tx = ship.begin_transaction(ship_context.original_args.clone()).await?;
    
    let persistence = tx.data_persistence()
        .map_err(|e| anyhow!("Failed to get data persistence: {}", e))?;
    let delta_table = persistence.table().clone();
    
    let session_context = datafusion::execution::context::SessionContext::new();
    
    // Register the oplog_entries table
    session_context.register_table("oplog_entries", Arc::new(delta_table.clone()))
        .map_err(|e| anyhow!("Failed to register oplog_entries table: {}", e))?;
    
    // Create the nodes view
    let create_nodes_view = "
        CREATE VIEW nodes AS
        SELECT 
            part_id, node_id, file_type, timestamp, version,
            sha256, size, min_event_time, max_event_time, min_override, max_override
        FROM oplog_entries
    ";
    session_context.sql(create_nodes_view).await
        .map_err(|e| anyhow!("Failed to create nodes view: {}", e))?
        .collect().await
        .map_err(|e| anyhow!("Failed to execute CREATE VIEW for nodes: {}", e))?;
    
    // Register DirectoryTable for file name resolution
    let directory_table = Arc::new(tlogfs::query::DirectoryTable::new(delta_table.clone()));
    session_context.register_table("directory_entries", directory_table)
        .map_err(|e| anyhow!("Failed to register directory_entries table: {}", e))?;

    info!("Detecting temporal overlaps in file:series data");

    // Resolve pattern to specific node IDs if pattern is provided
    let node_filter = if let Some(pattern_str) = pattern {
        info!("Resolving pattern {pattern_str}");
        
        // Use the transaction as FS to resolve the pattern to specific files
        let fs = &*tx; // StewardTransactionGuard derefs to FS
        let root = fs.root().await
            .map_err(|e| anyhow!("Failed to get filesystem root: {}", e))?;
            
        // Use FileInfoVisitor to collect file information
        let mut visitor = FileInfoVisitor::new(true);
        let resolved_files = root.visit_with_visitor(pattern_str, &mut visitor).await
            .map_err(|e| anyhow!("Failed to resolve pattern '{}': {}", pattern_str, e))?;
        
        if resolved_files.is_empty() {
            println!("No files found matching pattern: {}", pattern_str);
            return Ok(());
        }
        
        // Extract node IDs from the resolved files
        let mut node_ids = Vec::new();
        for file_info in resolved_files {
            // Get the node ID from the file info (it's a field, not a method)
            node_ids.push(format!("'{}'", file_info.node_id));
        }
        
        if node_ids.is_empty() {
            println!("No file:series nodes found in pattern: {}", pattern_str);
            return Ok(());
        }
        
        let file_count = node_ids.len();
        info!("Found {file_count} files matching pattern");
        Some(format!("AND n.node_id IN ({})", node_ids.join(", ")))
    } else {
        None
    };

    // SQL query to find overlapping time series
    let node_filter_clause = node_filter.as_deref().unwrap_or("");
    let overlap_query = format!("
        WITH series_with_times AS (
            SELECT 
                n.node_id,
                n.min_event_time,
                n.max_event_time,
                n.timestamp as created_at,
                n.version
            FROM nodes n
            WHERE n.file_type = 'file:series'
              AND n.min_event_time IS NOT NULL 
              AND n.max_event_time IS NOT NULL
              AND n.min_event_time < n.max_event_time
              {}
        ),
        overlapping_pairs AS (
            SELECT 
                a.node_id as node_a,
                b.node_id as node_b,
                a.min_event_time as a_start,
                a.max_event_time as a_end,
                b.min_event_time as b_start,
                b.max_event_time as b_end,
                GREATEST(a.min_event_time, b.min_event_time) as overlap_start,
                LEAST(a.max_event_time, b.max_event_time) as overlap_end
            FROM series_with_times a
            JOIN series_with_times b ON a.node_id < b.node_id
            WHERE a.max_event_time > b.min_event_time 
              AND a.min_event_time < b.max_event_time
        )
        SELECT 
            node_a,
            node_b,
            a_start,
            a_end,
            b_start,
            b_end,
            overlap_start,
            overlap_end,
            (overlap_end - overlap_start) as overlap_duration_ms
        FROM overlapping_pairs
        ORDER BY overlap_duration_ms DESC
        LIMIT 10
    ", node_filter_clause);

    let df = session_context.sql(&overlap_query).await
        .map_err(|e| anyhow!("Failed to execute overlap detection query: {}", e))?;
    
    let mut stream = df.execute_stream().await
        .map_err(|e| anyhow!("Failed to execute overlap query stream: {}", e))?;
    
    let mut overlaps = Vec::new();
    let mut total_overlap_count = 0;

    // Collect overlap results
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| anyhow!("Error in overlap query stream: {}", e))?;
        
        // Extract overlap data from the batch
        for row_idx in 0..batch.num_rows() {
            let node_a = extract_string_from_batch(&batch, "node_a", row_idx)?;
            let node_b = extract_string_from_batch(&batch, "node_b", row_idx)?;
            let overlap_start = extract_i64_from_batch(&batch, "overlap_start", row_idx)?;
            let overlap_end = extract_i64_from_batch(&batch, "overlap_end", row_idx)?;
            let overlap_duration = extract_i64_from_batch(&batch, "overlap_duration_ms", row_idx)?;

            overlaps.push(TemporalOverlap {
                file_a: "unknown".to_string(), // Will resolve with directory entries
                file_b: "unknown".to_string(),
                node_a: node_a.clone(),
                node_b: node_b.clone(),
                overlap_start,
                overlap_end,
                overlap_duration_ms: overlap_duration,
            });
            
            total_overlap_count += 1;
        }
    }

    if overlaps.is_empty() {
        println!("✅ No temporal overlaps detected in file:series data");
        return Ok(());
    }

    // Resolve file names using directory entries
    let resolved_overlaps = resolve_file_names(&session_context, overlaps).await?;

    // Display results
    println!("⚠️  Found {} temporal overlap{}", 
        total_overlap_count, 
        if total_overlap_count == 1 { "" } else { "s" }
    );
    println!();

    for (idx, overlap) in resolved_overlaps.iter().enumerate() {
        println!("Overlap #{}", idx + 1);
        println!("  Files: {} ↔ {}", overlap.file_a, overlap.file_b);
        if verbose {
            println!("  Node IDs: {} ↔ {}", overlap.node_a, overlap.node_b);
        }
        println!("  Overlap: {} ms to {} ms", overlap.overlap_start, overlap.overlap_end);
        println!("  Overlap: {} to {}", 
            format_timestamp(overlap.overlap_start), 
            format_timestamp(overlap.overlap_end)
        );
        println!("  Duration: {} ms ({:.2} hours)", 
            overlap.overlap_duration_ms,
            overlap.overlap_duration_ms as f64 / (1000.0 * 60.0 * 60.0)
        );
        println!();
    }

    println!("Use 'pond set-temporal-bounds' to configure overrides for problematic files.");
    
    if total_overlap_count > 0 {
        std::process::exit(1); // Exit with error code to indicate overlaps found
    }
    
    Ok(())
}

/// Convert Unix timestamp in milliseconds to human-readable date string
fn format_timestamp(timestamp_ms: i64) -> String {
    // Convert milliseconds to seconds for DateTime
    let timestamp_secs = timestamp_ms / 1000;
    let naive_datetime = chrono::DateTime::from_timestamp(timestamp_secs, 0);
    
    match naive_datetime {
        Some(dt) => {
            let utc_dt: DateTime<Utc> = dt.into();
            utc_dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
        },
        None => format!("Invalid timestamp: {}", timestamp_ms)
    }
}

/// Resolve node IDs to file names using directory entries
async fn resolve_file_names(
    session_context: &datafusion::execution::context::SessionContext,
    overlaps: Vec<TemporalOverlap>,
) -> Result<Vec<TemporalOverlap>> {
    // Build a map of node_id -> file_name
    let name_resolution_query = "
        SELECT child_node_id, name
        FROM directory_entries
        WHERE node_type = 'file:series'
    ";
    
    let df = session_context.sql(name_resolution_query).await
        .map_err(|e| anyhow!("Failed to execute name resolution query: {}", e))?;
    
    let mut stream = df.execute_stream().await
        .map_err(|e| anyhow!("Failed to execute name resolution stream: {}", e))?;
    
    let mut node_to_name = HashMap::new();
    
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| anyhow!("Error in name resolution stream: {}", e))?;
        
        for row_idx in 0..batch.num_rows() {
            let node_id = extract_string_from_batch(&batch, "child_node_id", row_idx)?;
            let name = extract_string_from_batch(&batch, "name", row_idx)?;
            node_to_name.insert(node_id, name);
        }
    }
    
    // Apply name resolution to overlaps
    let resolved_overlaps = overlaps.into_iter().map(|mut overlap| {
        overlap.file_a = node_to_name.get(&overlap.node_a)
            .cloned()
            .unwrap_or_else(|| format!("node:{}", overlap.node_a));
        overlap.file_b = node_to_name.get(&overlap.node_b)
            .cloned()
            .unwrap_or_else(|| format!("node:{}", overlap.node_b));
        overlap
    }).collect();
    
    Ok(resolved_overlaps)
}

/// Placeholder command for setting temporal bounds (implementation needed)
pub async fn set_temporal_bounds_command(
    _ship_context: &ShipContext,
    filesystem: &FilesystemChoice,
    file_pattern: &str,
    min_time: Option<i64>,
    max_time: Option<i64>,
) -> Result<()> {
    warn!("set-temporal-bounds command not yet implemented");
    println!("Command: set-temporal-bounds");
    println!("  Pattern: {}", file_pattern);
    println!("  Min time: {:?}", min_time);
    println!("  Max time: {:?}", max_time);
    println!("  Filesystem: {:?}", filesystem);
    println!();
    println!("This command would set temporal override bounds for files matching the pattern.");
    println!("Implementation needed in future version.");
    
    Err(anyhow!("set-temporal-bounds command not yet implemented"))
}

/// Helper function to extract string values from Arrow batch
fn extract_string_from_batch(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column_name: &str,
    row_idx: usize,
) -> Result<String> {
    use datafusion::arrow::array::Array;
    
    let column = batch.column_by_name(column_name)
        .ok_or_else(|| anyhow!("Column '{}' not found in batch", column_name))?;
    
    if let Some(string_array) = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>() {
        if !string_array.is_null(row_idx) {
            return Ok(string_array.value(row_idx).to_string());
        }
    }
    
    Err(anyhow!("Failed to extract string from column '{}' at row {}", column_name, row_idx))
}

/// Helper function to extract i64 values from Arrow batch
fn extract_i64_from_batch(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column_name: &str,
    row_idx: usize,
) -> Result<i64> {
    use datafusion::arrow::array::Array;
    
    let column = batch.column_by_name(column_name)
        .ok_or_else(|| anyhow!("Column '{}' not found in batch", column_name))?;
    
    if let Some(i64_array) = column.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
        if !i64_array.is_null(row_idx) {
            return Ok(i64_array.value(row_idx));
        }
    }
    
    Err(anyhow!("Failed to extract i64 from column '{}' at row {}", column_name, row_idx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TestShipEnvironment;

    #[tokio::test]
    async fn test_check_overlaps_empty_pond() -> Result<()> {
        let setup = TestShipEnvironment::new().await?;
        
        // Test overlap detection on empty pond
        let result = check_overlaps_command(
            &setup.ship_context,
            &FilesystemChoice::Data,
            None,
            false,
        ).await;
        
        // Should succeed and find no overlaps
        assert!(result.is_ok());
        
        Ok(())
    }

    #[tokio::test]
    async fn test_set_temporal_bounds_unimplemented() -> Result<()> {
        let setup = TestShipEnvironment::new().await?;
        
        // Test that set-temporal-bounds returns unimplemented error
        let result = set_temporal_bounds_command(
            &setup.ship_context,
            &FilesystemChoice::Data,
            "test.csv",
            Some(1000),
            Some(2000),
        ).await;
        
        // Should fail with unimplemented message
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not yet implemented"));
        
        Ok(())
    }
}
