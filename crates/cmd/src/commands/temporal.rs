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

/// Advanced overlap detection using complete time series data analysis with origin tracking
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
    info!("Starting data-level temporal overlap detection for {pattern_count} patterns");

    // NEW APPROACH: Use NodeVersionTable providers for proper origin tracking
    
    // Get TinyFS root for file access
    let fs = tinyfs::FS::new(tx.state()?).await
        .map_err(|e| anyhow!("Failed to get TinyFS: {}", e))?;
    let tinyfs_root = Arc::new(fs.root().await
        .map_err(|e| anyhow!("Failed to get TinyFS root: {}", e))?);
    
    // Create DataFusion context
    let ctx = datafusion::execution::context::SessionContext::new();
    
    // Resolve all patterns to individual files and register them as separate tables
    let mut union_parts = Vec::new();
    let mut origin_id = 1i64;
    
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
                        
                        info!("Found file: {path_str} (node: {node_id})", path_str: path_str, node_id: node_id);
                        
                        // Create NodeVersionTable for this specific file/version
                        let table_provider = Arc::new(tlogfs::query::NodeVersionTable::new(
                            node_id.clone(),
                            None, // Use latest version
                            path_str.clone(),
                            tinyfs_root.clone(),
                        ).await.map_err(|e| anyhow!("Failed to create NodeVersionTable for {}: {}", path_str, e))?);
                        
                        // Register table with unique name
                        let table_name = format!("file_{}", origin_id);
                        ctx.register_table(&table_name, table_provider)
                            .map_err(|e| anyhow!("Failed to register table '{}': {}", table_name, e))?;
                        
                        // Add to UNION query with origin tracking
                        union_parts.push(format!("(SELECT *, {} as _origin_id FROM {})", origin_id, table_name));
                        origin_id += 1;
                    }
                }
            }
        }
    }
    
    if union_parts.is_empty() {
        println!("No FileSeries files found matching the specified patterns");
        return Ok(());
    }
    
    let file_count = union_parts.len();
    info!("Found {file_count} files for overlap analysis", file_count: file_count);
    
    if union_parts.is_empty() {
        println!("No FileSeries files found matching the specified patterns");
        return Ok(());
    }
    
    // Get timestamp column name from the first table's schema
    let first_table_name = "file_1";
    let first_table = ctx.table(first_table_name).await
        .map_err(|e| anyhow!("Failed to get first table '{}': {}", first_table_name, e))?;
    let first_df_schema = first_table.schema();
    
    // DFSchema implements AsRef<Schema>
    let timestamp_column = tlogfs::schema::detect_timestamp_column(first_df_schema.as_ref())
        .map_err(|e| anyhow!("Failed to detect timestamp column: {}", e))?;
    info!("Detected timestamp column: {timestamp_column}", timestamp_column: timestamp_column);
    
    // Create UNION query with only timestamp and _origin_id columns
    let projected_union_parts: Vec<String> = (1..=union_parts.len()).map(|i| {
        let table_name = format!("file_{}", i);
        format!("(SELECT {}, {} as _origin_id FROM {})", timestamp_column, i, table_name)
    }).collect();
    
    let union_query = projected_union_parts.join(" UNION ALL ");
    debug!("Generated projected UNION query: {union_query}", union_query: union_query);
    
    // Execute the unified query to get all data with origin tracking
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
        let origin_col = batch.column_by_name("_origin_id")
            .ok_or_else(|| anyhow!("No _origin_id column found"))?;

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
            .ok_or_else(|| anyhow!("Failed to cast _origin_id column"))?
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
                overlap_points: overlap_duration as u64,
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
