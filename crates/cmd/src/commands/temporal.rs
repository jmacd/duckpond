use anyhow::{Result, anyhow};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::common::{FilesystemChoice, ShipContext, };
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
    
    // Create a UNION query to get all data from all versions sorted by timestamp
    let mut union_parts = Vec::new();
    let mut origin_id = 0;
    
    for (path_str, node_id) in file_info.iter() {
        // Get all versions of this file
        let versions = tinyfs_root.list_file_versions(&path_str).await
            .map_err(|e| anyhow!("Failed to get versions for {}: {}", path_str, e))?;
        
        let version_count = versions.len();
        info!("Found file: {path_str} (node: {node_id}) with {version_count} versions", 
              path_str: path_str, node_id: node_id, version_count: version_count);
        
        // Create a NodeVersionTable for each version
        for version_info in versions {
            let table_provider = Arc::new(tlogfs::query::NodeVersionTable::new(
                node_id.clone(),
                Some(version_info.version), // Use specific version
                path_str.clone(),
                tinyfs_root.clone(),
            ).await.map_err(|e| anyhow!("Failed to create NodeVersionTable for {} v{}: {}", path_str, version_info.version, e))?);
            
            // Register table with unique name including version
            let table_name = format!("file_{}_{}", origin_id, version_info.version);
            ctx.register_table(&table_name, table_provider)
                .map_err(|e| anyhow!("Failed to register table '{}': {}", table_name, e))?;
            
            // Add to UNION query with origin tracking (timestamp and metadata)
            union_parts.push(format!("(SELECT timestamp, {} as _node_id, {} as _version, '{}' as _file_path FROM {})", 
                                    origin_id, version_info.version, path_str, table_name));
        }
        origin_id += 1;
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
    timeline: Vec<TimelineSegment>,
}

#[derive(Debug)]
enum TimelineSegment {
    Run {
        origin_id: i64,
        start_timestamp: i64,
        end_timestamp: i64,
        point_count: usize,
    },
    Overlap {
        start_timestamp: i64,
        end_timestamp: i64,
        points: Vec<(i64, i64)>, // (timestamp, origin_id) pairs
    },
    Gap {
        start_timestamp: i64,
        end_timestamp: i64,
        isolated_points: Vec<(i64, i64)>, // (timestamp, origin_id) pairs
    },
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
    // Collect all data points across all batches
    let mut data_points = Vec::new();
    let mut origin_statistics = HashMap::new();

    for batch in batches {
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

        // Collect all data points
        for (&timestamp, &origin_id) in timestamps.iter().zip(origin_ids.iter()) {
            data_points.push((timestamp, origin_id));
            
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
        }
    }

    // Sort all data points by timestamp to create timeline
    data_points.sort_by_key(|&(timestamp, _)| timestamp);
    
    // Analyze timeline for runs and overlaps
    let timeline = analyze_timeline(&data_points);
    
    Ok(OverlapAnalysis {
        total_points: data_points.len(),
        overlapping_points: timeline.iter().map(|segment| match segment {
            TimelineSegment::Overlap { points, .. } => points.len(),
            _ => 0,
        }).sum(),
        overlap_runs: Vec::new(), // Will be populated from timeline
        origin_statistics,
        timeline,
    })
}

/// Analyze timeline of data points to identify runs, overlaps, and gaps
fn analyze_timeline(data_points: &[(i64, i64)]) -> Vec<TimelineSegment> {
    let mut timeline = Vec::new();
    
    if data_points.is_empty() {
        return timeline;
    }
    
    let mut current_run: Option<(i64, i64, i64, usize)> = None; // (origin_id, start_ts, end_ts, count)
    let overlap_threshold_ms = 60000; // 1 minute - points within this are considered potentially overlapping
    
    for i in 0..data_points.len() {
        let (timestamp, origin_id) = data_points[i];
        
        match current_run {
            None => {
                // Start first run
                current_run = Some((origin_id, timestamp, timestamp, 1));
            }
            Some((run_origin, start_ts, _end_ts, count)) => {
                if origin_id == run_origin {
                    // Continue current run
                    current_run = Some((run_origin, start_ts, timestamp, count + 1));
                } else {
                    // Origin change - finish current run and check for overlap
                    let prev_timestamp = if i > 0 { data_points[i-1].0 } else { timestamp };
                    
                    if timestamp - prev_timestamp <= overlap_threshold_ms {
                        // Close overlap - look ahead to see if this is a true overlap or transition
                        let mut overlap_points = vec![(prev_timestamp, run_origin), (timestamp, origin_id)];
                        
                        // Look ahead for more overlapping points
                        let mut j = i + 1;
                        while j < data_points.len() {
                            let (next_ts, next_origin) = data_points[j];
                            if next_ts - timestamp <= overlap_threshold_ms {
                                overlap_points.push((next_ts, next_origin));
                                j += 1;
                            } else {
                                break;
                            }
                        }
                        
                        // Finish previous run
                        if count > 1 {
                            timeline.push(TimelineSegment::Run {
                                origin_id: run_origin,
                                start_timestamp: start_ts,
                                end_timestamp: prev_timestamp,
                                point_count: count,
                            });
                        }
                        
                        // Add overlap
                        timeline.push(TimelineSegment::Overlap {
                            start_timestamp: prev_timestamp,
                            end_timestamp: overlap_points.last().unwrap().0,
                            points: overlap_points,
                        });
                        
                        // Skip ahead past overlap
                        // TODO: Continue processing from after overlap
                        current_run = None;
                    } else {
                        // Clean transition - finish run and start new one
                        timeline.push(TimelineSegment::Run {
                            origin_id: run_origin,
                            start_timestamp: start_ts,
                            end_timestamp: prev_timestamp,
                            point_count: count,
                        });
                        
                        current_run = Some((origin_id, timestamp, timestamp, 1));
                    }
                }
            }
        }
    }
    
    // Finish final run
    if let Some((origin_id, start_ts, end_ts, count)) = current_run {
        timeline.push(TimelineSegment::Run {
            origin_id,
            start_timestamp: start_ts,
            end_timestamp: end_ts,
            point_count: count,
        });
    }
    
    timeline
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
    println!("Overlap percentage: {:.2}%", overlap_percentage);
    
    // Count different types of segments
    let mut run_count = 0;
    let mut overlap_count = 0;
    let mut gap_count = 0;
    
    for segment in &analysis.timeline {
        match segment {
            TimelineSegment::Run { .. } => run_count += 1,
            TimelineSegment::Overlap { .. } => overlap_count += 1,
            TimelineSegment::Gap { .. } => gap_count += 1,
        }
    }
    
    println!("Timeline segments: {} runs, {} overlaps, {} gaps", run_count, overlap_count, gap_count);

    if !analysis.timeline.is_empty() {
        println!("\nTimeline Analysis:");
        for (i, segment) in analysis.timeline.iter().enumerate() {
            match segment {
                TimelineSegment::Run { origin_id, start_timestamp, end_timestamp, point_count } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    let duration_hours = (*end_timestamp - *start_timestamp) as f64 / (1000.0 * 60.0 * 60.0);
                    
                    println!("  {}. RUN: Origin {} - {} to {} ({:.1} hours, {} points)", 
                             i + 1, origin_id, start_time, end_time, duration_hours, point_count);
                }
                TimelineSegment::Overlap { start_timestamp, end_timestamp, points } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    let origins: std::collections::HashSet<i64> = points.iter().map(|(_, origin)| *origin).collect();
                    
                    if *start_timestamp == *end_timestamp {
                        println!("  {}. OVERLAP: Single moment at {} - {} points from origins {:?}", 
                                 i + 1, start_time, points.len(), origins.iter().collect::<Vec<_>>());
                    } else {
                        println!("  {}. OVERLAP: {} to {} - {} points from origins {:?}", 
                                 i + 1, start_time, end_time, points.len(), origins.iter().collect::<Vec<_>>());
                    }
                }
                TimelineSegment::Gap { start_timestamp, end_timestamp, isolated_points } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    
                    println!("  {}. GAP: {} to {} - {} isolated points", 
                             i + 1, start_time, end_time, isolated_points.len());
                }
            }
        }
    }

    println!("\nOrigin Statistics:");
    for (origin_id, stats) in &analysis.origin_statistics {
        if let Some((min_ts, max_ts)) = stats.time_range {
            let min_time = format_timestamp(min_ts);
            let max_time = format_timestamp(max_ts);
            let span_hours = (max_ts - min_ts) as f64 / (1000.0 * 60.0 * 60.0);
            
            println!("  Origin {}: {} points, {} to {} ({:.1} hours span)", 
                     origin_id, stats.point_count, min_time, max_time, span_hours);
        } else {
            println!("  Origin {}: {} points, no time range", origin_id, stats.point_count);
        }
    }
}

/// Print detailed overlap analysis
fn print_overlap_details(analysis: &OverlapAnalysis) {
    print_overlap_summary(analysis);
    
    if !analysis.overlap_runs.is_empty() {
        println!("\nDetailed Overlap Runs:");
        for (i, run) in analysis.overlap_runs.iter().enumerate() {
            let start_time = format_timestamp(run.start_timestamp);
            let end_time = format_timestamp(run.end_timestamp);
            
            if run.start_timestamp == run.end_timestamp {
                println!("\nRun {}: Single point overlap", i + 1);
                println!("  Timestamp: {}", start_time);
            } else {
                println!("\nRun {}: Time range overlap", i + 1);
                println!("  Start: {}", start_time);
                println!("  End: {}", end_time);
                println!("  Duration: {:.1} hours", run.duration_ms as f64 / (1000.0 * 60.0 * 60.0));
            }
            println!("  Points: {}", run.point_count);
            println!("  Origins involved: {:?}", run.origins_involved);
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
