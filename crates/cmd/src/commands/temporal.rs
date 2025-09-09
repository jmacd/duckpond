use anyhow::{Result, anyhow};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::common::{FilesystemChoice, ShipContext, FileInfoVisitor};
use diagnostics::*;

#[derive(Debug)]
pub struct TemporalOverlap {
    pub file_a: String,
    pub file_b: String, 
    pub node_a: String,
    pub node_b: String,
    pub overlap_start: i64,
    pub overlap_end: i64,
    pub overlap_duration_ms: i64,
    pub overlap_points: u64,
}

/// Simple overlap detection using direct time series data analysis
pub async fn detect_overlaps_command(
    ship_context: &ShipContext,
    filesystem: &FilesystemChoice,
    patterns: &[String],
    verbose: bool,
    format: &str,
) -> Result<()> {
    debug!("detect_overlaps_command called with patterns and verbose flag");
    
    if *filesystem == FilesystemChoice::Control {
        return Err(anyhow!("Control filesystem access not supported for overlap detection"));
    }

    if patterns.is_empty() {
        return Err(anyhow!("At least one file pattern must be specified"));
    }

    let mut ship = ship_context.open_pond().await?;
    let tx = ship.begin_transaction(ship_context.original_args.clone()).await?;

    let pattern_count = patterns.len();
    info!("Starting simplified temporal overlap detection for {pattern_count} patterns");

    // SIMPLIFIED APPROACH: Use TLogFS factory directly to get time series data
    
    // Get TinyFS root for file access
    let fs = tinyfs::FS::new(tx.state()?).await
        .map_err(|e| anyhow!("Failed to get TinyFS: {}", e))?;
    let tinyfs_root = Arc::new(fs.root().await
        .map_err(|e| anyhow!("Failed to get TinyFS root: {}", e))?);
    
    // Create DataFusion context
    let ctx = datafusion::execution::context::SessionContext::new();
    
    // Collect all matching file paths and their metadata
    let mut file_info = Vec::new();
    
    for pattern in patterns {
        info!("Resolving pattern: {pattern}");
        
        // Resolve pattern to files using TinyFS pattern matching
        let matches = tinyfs_root.collect_matches(pattern).await
            .map_err(|e| anyhow!("Failed to resolve pattern '{}': {}", pattern, e))?;
        
        for (node_path, _captured) in matches {
            let node_ref = node_path.borrow().await;
            
            if let Ok(file_node) = node_ref.as_file() {
                if let Ok(metadata) = file_node.metadata().await {
                    if metadata.entry_type == tinyfs::EntryType::FileSeries {
                        let path_str = node_path.path().to_string_lossy().to_string();
                        let node_id = node_path.id().await.to_hex_string();
                        
                        file_info.push((path_str, node_id));
                    }
                }
            }
        }
    }
    
    if file_info.is_empty() {
        println!("No FileSeries files found matching the specified patterns");
        return Ok(());
    }

    let file_count = file_info.len();
    info!("Found {file_count} files for overlap analysis", file_count: file_count);
    
    // Create a simple UNION query to get all data sorted by timestamp
    let mut union_parts = Vec::new();
    for (idx, (path_str, node_id)) in file_info.iter().enumerate() {
        // Create NodeVersionTable for the latest version of each file
        let table_provider = Arc::new(tlogfs::query::NodeVersionTable::new(
            node_id.clone(),
            None, // Use latest version
            path_str.clone(),
            tinyfs_root.clone(),
        ).await.map_err(|e| anyhow!("Failed to create NodeVersionTable for {}: {}", path_str, e))?);
        
        // Register table with simple name
        let table_name = format!("file_{}", idx);
        ctx.register_table(&table_name, table_provider)
            .map_err(|e| anyhow!("Failed to register table '{}': {}", table_name, e))?;
        
        // Add to UNION query with specific columns only
        union_parts.push(format!("(SELECT timestamp, {} as _node_id, {} as _version, '{}' as _file_path FROM {})", 
                                idx, 1, path_str, table_name));
    }
    
    // Create simple query: just timestamp and metadata, sorted by timestamp
    let union_query = format!(
        "SELECT timestamp, _node_id, _version, _file_path FROM ({}) ORDER BY timestamp", 
        union_parts.join(" UNION ALL ")
    );
    debug!("Generated simple UNION query: {union_query}", union_query: union_query);
    
    // Execute the query to get all data sorted by timestamp
    let dataframe = ctx.sql(&union_query).await
        .map_err(|e| anyhow!("Failed to execute UNION query: {}", e))?;
    
    let all_batches = dataframe.collect().await
        .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;
    
    if all_batches.is_empty() {
        println!("No data found in specified patterns");
        return Ok(());
    }

    // Analyze the combined batches for overlaps
    let overlap_analysis = analyze_temporal_overlaps(&all_batches, verbose)?;

    // Output results based on format
    match format {
        "summary" => print_overlap_summary(&overlap_analysis),
        "full" => print_overlap_details(&overlap_analysis),
        "json" => print_overlap_json(&overlap_analysis)?,
        _ => return Err(anyhow!("Invalid format '{}'. Use summary, full, or json", format)),
    }

    Ok(())
}

/// Data structure for overlap analysis results
#[derive(Debug)]
struct OverlapAnalysis {
    total_points: usize,
    overlapping_points: usize,
    overlap_runs: Vec<OverlapRun>,
    origin_statistics: HashMap<i64, OriginStats>,
}

#[derive(Debug)]
struct OverlapRun {
    start_timestamp: i64,
    end_timestamp: i64,
    duration_ms: i64,
    point_count: usize,
    origins_involved: Vec<i64>,
}

#[derive(Debug)]
struct OriginStats {
    point_count: usize,
    time_range: Option<(i64, i64)>,
}

/// Analyze temporal overlaps in the interleaved data
fn analyze_temporal_overlaps(batches: &[arrow::record_batch::RecordBatch], _verbose: bool) -> Result<OverlapAnalysis> {
    let mut total_points = 0;
    let mut overlapping_points = 0;
    let mut overlap_runs = Vec::new();
    let mut origin_statistics = HashMap::new();
    let mut current_run: Option<OverlapRun> = None;

    for batch in batches {
        total_points += batch.num_rows();

        // Extract columns
        let timestamp_col = batch.column_by_name("timestamp")
            .ok_or_else(|| anyhow!("No timestamp column found"))?;
        let origin_col = batch.column_by_name("_node_id")
            .ok_or_else(|| anyhow!("No _node_id column found"))?;

        // Handle timestamp as either millisecond or second timestamp types
        let timestamps = match timestamp_col.data_type() {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                timestamp_col.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| anyhow!("Failed to cast timestamp column"))?
                    .values().to_vec()
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                // Convert seconds to milliseconds for consistent internal representation
                timestamp_col.as_any().downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| anyhow!("Failed to cast timestamp column"))?
                    .values().iter().map(|&ts| ts * 1000).collect() // Convert seconds to milliseconds
            }
            _ => return Err(anyhow!("Unsupported timestamp column type: {:?}", timestamp_col.data_type())),
        };

        let origin_ids = origin_col.as_any().downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow!("Failed to cast _node_id column"))?
            .values().to_vec();

        // Process each row
        for (i, (&timestamp, &origin_id)) in timestamps.iter().zip(origin_ids.iter()).enumerate() {
            // Update origin statistics
            let stats = origin_statistics.entry(origin_id).or_insert(OriginStats {
                point_count: 0,
                time_range: None,
            });
            stats.point_count += 1;
            stats.time_range = match stats.time_range {
                Some((min, max)) => Some((min.min(timestamp), max.max(timestamp))),
                None => Some((timestamp, timestamp)),
            };

            // Check for overlaps by looking at neighboring points
            let has_overlap = if i > 0 {
                origin_ids[i - 1] != origin_id
            } else if i + 1 < origin_ids.len() {
                origin_ids[i + 1] != origin_id
            } else {
                false
            };

            if has_overlap {
                overlapping_points += 1;

                // Extend or start overlap run
                match &mut current_run {
                    Some(run) if run.end_timestamp >= timestamp - 1000 => {
                        // Extend current run (within 1 second)
                        run.end_timestamp = timestamp;
                        run.point_count += 1;
                        if !run.origins_involved.contains(&origin_id) {
                            run.origins_involved.push(origin_id);
                        }
                    }
                    _ => {
                        // Finish previous run and start new one
                        if let Some(finished_run) = current_run.take() {
                            overlap_runs.push(finished_run);
                        }
                        current_run = Some(OverlapRun {
                            start_timestamp: timestamp,
                            end_timestamp: timestamp,
                            duration_ms: 0,
                            point_count: 1,
                            origins_involved: vec![origin_id],
                        });
                    }
                }
            } else if let Some(finished_run) = current_run.take() {
                // End of overlap run
                overlap_runs.push(finished_run);
            }
        }
    }

    // Finish final run if needed
    if let Some(finished_run) = current_run {
        overlap_runs.push(finished_run);
    }

    // Calculate durations for runs
    for run in &mut overlap_runs {
        run.duration_ms = run.end_timestamp - run.start_timestamp;
    }

    // Sort runs by duration (longest first)
    overlap_runs.sort_by(|a, b| b.duration_ms.cmp(&a.duration_ms));

    Ok(OverlapAnalysis {
        total_points,
        overlapping_points,
        overlap_runs,
        origin_statistics,
    })
}

/// Print overlap analysis summary
fn print_overlap_summary(analysis: &OverlapAnalysis) {
    println!("Temporal Overlap Analysis Summary");
    println!("================================");
    println!("Total data points: {}", analysis.total_points);
    println!("Overlapping points: {}", analysis.overlapping_points);
    
    let overlap_percentage = if analysis.total_points > 0 {
        (analysis.overlapping_points as f64 / analysis.total_points as f64) * 100.0
    } else {
        0.0
    };
    println!("Overlap percentage: {:.2}%,", overlap_percentage);
    println!("Overlap runs detected: {}", analysis.overlap_runs.len());

    if !analysis.overlap_runs.is_empty() {
        println!("Top 5 Longest Overlap Runs:");
        for (i, run) in analysis.overlap_runs.iter().take(5).enumerate() {
            println!("  {}. Duration: {}ms, Points: {}, Origins: {:?}", 
                     i + 1, run.duration_ms, run.point_count, run.origins_involved);
        }
    }

    println!("Origin Statistics:");
    for (origin_id, stats) in &analysis.origin_statistics {
        if let Some((min_time, max_time)) = stats.time_range {
            println!("  Origin {}: {} points, range: {} to {} ({}ms span)", 
                     origin_id, stats.point_count, min_time, max_time, max_time - min_time);
        }
    }
}

/// Print detailed overlap analysis
fn print_overlap_details(analysis: &OverlapAnalysis) {
    print_overlap_summary(analysis);
    
    if !analysis.overlap_runs.is_empty() {
        println!("Detailed Overlap Runs:");
        for (i, run) in analysis.overlap_runs.iter().enumerate() {
            println!("Run {}: {} to {} ({}ms)", 
                     i + 1, run.start_timestamp, run.end_timestamp, run.duration_ms);
            println!("  Points: {}, Origins involved: {:?}", run.point_count, run.origins_involved);
        }
    }
}

/// Print overlap analysis as JSON
fn print_overlap_json(analysis: &OverlapAnalysis) -> Result<()> {
    let json_output = serde_json::json!({
        "total_points": analysis.total_points,
        "overlapping_points": analysis.overlapping_points,
        "overlap_percentage": if analysis.total_points > 0 {
            (analysis.overlapping_points as f64 / analysis.total_points as f64) * 100.0
        } else {
            0.0
        },
        "overlap_runs_count": analysis.overlap_runs.len(),
        "overlap_runs": analysis.overlap_runs.iter().map(|run| serde_json::json!({
            "start_timestamp": run.start_timestamp,
            "end_timestamp": run.end_timestamp,
            "duration_ms": run.duration_ms,
            "point_count": run.point_count,
            "origins_involved": run.origins_involved
        })).collect::<Vec<_>>(),
        "origin_statistics": analysis.origin_statistics.iter().map(|(id, stats)| {
            serde_json::json!({
                "origin_id": id,
                "point_count": stats.point_count,
                "time_range": stats.time_range.map(|(min, max)| serde_json::json!({
                    "min": min,
                    "max": max,
                    "span_ms": max - min
                }))
            })
        }).collect::<Vec<_>>()
    });

    println!("{}", serde_json::to_string_pretty(&json_output)?);
    Ok(())
}

#[cfg(test)]
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
