use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::*;
use tinyfs::NodeID;
use tlogfs::schema::{ExtendedAttributes, OplogEntry};

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
        return Err(anyhow!(
            "Control filesystem access not supported for overlap detection"
        ));
    }

    if patterns.is_empty() {
        return Err(anyhow!("At least one file pattern must be specified"));
    }

    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_transaction(ship_context.original_args.clone())
        .await?;

    let pattern_count = patterns.len();
    info!("Starting simplified temporal overlap detection for {pattern_count} patterns");

    // SIMPLIFIED APPROACH: Use TLogFS factory directly to get time series data

    // Get TinyFS root for file access
    let fs = tinyfs::FS::new(tx.state()?)
        .await
        .map_err(|e| anyhow!("Failed to get TinyFS: {}", e))?;
    let tinyfs_root = Arc::new(
        fs.root()
            .await
            .map_err(|e| anyhow!("Failed to get TinyFS root: {}", e))?,
    );

    // Create DataFusion context
    let ctx = datafusion::execution::context::SessionContext::new();

    // Collect all matching file paths and their metadata
    let mut file_info = Vec::new();

    for pattern in patterns {
        info!("Resolving pattern: {pattern}");

        // Resolve pattern to files using TinyFS pattern matching
        let matches = tinyfs_root
            .collect_matches(pattern)
            .await
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
        let versions = tinyfs_root
            .list_file_versions(&path_str)
            .await
            .map_err(|e| anyhow!("Failed to get versions for {}: {}", path_str, e))?;

        let version_count = versions.len();
        info!("Found file: {path_str} (node: {node_id}) with {version_count} versions", 
              path_str: path_str, node_id: node_id, version_count: version_count);

        // Create a NodeVersionTable for each version
        for version_info in versions {
            let table_provider = Arc::new(
                tlogfs::query::NodeVersionTable::new(
                    node_id.clone(),
                    Some(version_info.version), // Use specific version
                    path_str.clone(),
                    tinyfs_root.clone(),
                )
                .await
                .map_err(|e| {
                    anyhow!(
                        "Failed to create NodeVersionTable for {} v{}: {}",
                        path_str,
                        version_info.version,
                        e
                    )
                })?,
            );

            // Register table with unique name including version
            let table_name = format!("file_{}_{}", origin_id, version_info.version);
            ctx.register_table(&table_name, table_provider)
                .map_err(|e| anyhow!("Failed to register table '{}': {}", table_name, e))?;

            // Add to UNION query with origin tracking (timestamp and metadata)
            union_parts.push(format!(
                "(SELECT timestamp, {} as _node_id, {} as _version, '{}' as _file_path FROM {})",
                origin_id, version_info.version, path_str, table_name
            ));
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
    let dataframe = ctx
        .sql(&union_query)
        .await
        .map_err(|e| anyhow!("Failed to execute UNION query: {}", e))?;

    let all_batches = dataframe
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;

    if all_batches.is_empty() {
        println!("No data found in specified patterns");
        return Ok(());
    }

    // Create origin-to-path mapping for output
    let mut origin_to_path = HashMap::new();
    let mut origin_id = 0;
    for (path_str, _node_id) in file_info.iter() {
        origin_to_path.insert(origin_id, path_str.clone());
        origin_id += 1;
    }

    // Analyze the combined batches for overlaps
    let overlap_analysis = analyze_temporal_overlaps(&all_batches, verbose, origin_to_path)?;

    // Output results based on format
    match format {
        "summary" => print_overlap_summary(&overlap_analysis),
        "full" => print_overlap_details(&overlap_analysis),
        _ => {
            return Err(anyhow!(
                "Invalid format '{}'. Use summary, full, or json",
                format
            ));
        }
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
    origin_to_path: HashMap<i64, String>,
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
fn analyze_temporal_overlaps(
    batches: &[arrow::record_batch::RecordBatch],
    _verbose: bool,
    origin_to_path: HashMap<i64, String>,
) -> Result<OverlapAnalysis> {
    // Collect all data points across all batches
    let mut data_points = Vec::new();
    let mut origin_statistics = HashMap::new();

    for batch in batches {
        // Extract columns
        let timestamp_col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| anyhow!("No timestamp column found"))?;
        let origin_col = batch
            .column_by_name("_node_id")
            .ok_or_else(|| anyhow!("No _node_id column found"))?;

        // Handle timestamp as either millisecond or second timestamp types
        let timestamps = match timestamp_col.data_type() {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                timestamp_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| anyhow!("Failed to cast timestamp column"))?
                    .values()
                    .to_vec()
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                // Convert seconds to milliseconds for consistent internal representation
                timestamp_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| anyhow!("Failed to cast timestamp column"))?
                    .values()
                    .iter()
                    .map(|&ts| ts * 1000)
                    .collect() // Convert seconds to milliseconds
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported timestamp column type: {:?}",
                    timestamp_col.data_type()
                ));
            }
        };

        let origin_ids = origin_col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow!("Failed to cast _node_id column"))?
            .values()
            .to_vec();

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
        overlapping_points: timeline
            .iter()
            .map(|segment| match segment {
                TimelineSegment::Overlap { points, .. } => points.len(),
                _ => 0,
            })
            .sum(),
        overlap_runs: Vec::new(), // Will be populated from timeline
        origin_statistics,
        timeline,
        origin_to_path,
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
                    let prev_timestamp = if i > 0 {
                        data_points[i - 1].0
                    } else {
                        timestamp
                    };

                    if timestamp - prev_timestamp <= overlap_threshold_ms {
                        // Close overlap - look ahead to see if this is a true overlap or transition
                        let mut overlap_points =
                            vec![(prev_timestamp, run_origin), (timestamp, origin_id)];

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

    for segment in &analysis.timeline {
        match segment {
            TimelineSegment::Run { .. } => run_count += 1,
            TimelineSegment::Overlap { .. } => overlap_count += 1,
        }
    }

    println!(
        "Timeline segments: {} runs, {} overlaps",
        run_count, overlap_count
    );

    if !analysis.timeline.is_empty() {
        println!("\nTimeline Analysis:");
        for (i, segment) in analysis.timeline.iter().enumerate() {
            match segment {
                TimelineSegment::Run {
                    origin_id,
                    start_timestamp,
                    end_timestamp,
                    point_count,
                } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    let duration_hours =
                        (*end_timestamp - *start_timestamp) as f64 / (1000.0 * 60.0 * 60.0);
                    let file_path = analysis
                        .origin_to_path
                        .get(origin_id)
                        .map(|s| s.as_str())
                        .unwrap_or("unknown");

                    println!(
                        "  {}. RUN: {} - {} to {} ({:.1} hours, {} points)",
                        i + 1,
                        file_path,
                        start_time,
                        end_time,
                        duration_hours,
                        point_count
                    );
                }
                TimelineSegment::Overlap {
                    start_timestamp,
                    end_timestamp,
                    points,
                } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    let origins: std::collections::HashSet<i64> =
                        points.iter().map(|(_, origin)| *origin).collect();
                    let file_paths: Vec<&str> = origins
                        .iter()
                        .map(|id| {
                            analysis
                                .origin_to_path
                                .get(id)
                                .map(|s| s.as_str())
                                .unwrap_or("unknown")
                        })
                        .collect();

                    if *start_timestamp == *end_timestamp {
                        println!(
                            "  {}. OVERLAP: Single moment at {} - {} points from files {:?}",
                            i + 1,
                            start_time,
                            points.len(),
                            file_paths
                        );
                    } else {
                        println!(
                            "  {}. OVERLAP: {} to {} - {} points from files {:?}",
                            i + 1,
                            start_time,
                            end_time,
                            points.len(),
                            file_paths
                        );
                    }
                }
            }
        }
    }

    println!("\nOrigin Statistics:");
    for (origin_id, stats) in &analysis.origin_statistics {
        let file_path = analysis
            .origin_to_path
            .get(origin_id)
            .map(|s| s.as_str())
            .unwrap_or("unknown");
        if let Some((min_ts, max_ts)) = stats.time_range {
            let min_time = format_timestamp(min_ts);
            let max_time = format_timestamp(max_ts);
            let span_hours = (max_ts - min_ts) as f64 / (1000.0 * 60.0 * 60.0);

            println!(
                "  {}: {} points, {} to {} ({:.1} hours span)",
                file_path, stats.point_count, min_time, max_time, span_hours
            );
        } else {
            println!(
                "  {}: {} points, no time range",
                file_path, stats.point_count
            );
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
                println!(
                    "  Duration: {:.1} hours",
                    run.duration_ms as f64 / (1000.0 * 60.0 * 60.0)
                );
            }
            println!("  Points: {}", run.point_count);
            println!("  Origins involved: {:?}", run.origins_involved);
        }
    }
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
        }
        None => format!("Invalid timestamp: {}", timestamp_ms),
    }
}

/// Create a metadata-only FileSeries version with temporal bounds
pub async fn set_temporal_bounds_command(
    ship_context: &ShipContext, 
    target_path: String,
    min_bound: Option<String>,
    max_bound: Option<String>,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    if min_bound.is_none() && max_bound.is_none() {
	return Err(anyhow!("no bounds were provided"));
    }

    let transaction = ship.begin_transaction(vec!["set_temporal_bounds".into(), target_path.clone()]).await?;

    info!("Setting temporal bounds for target_path: {target_path}");

    // Resolve the target path to a NodePath to get the NodeID
    let node_path = transaction
        .root()
        .await?
        .get_node_path(&target_path)
        .await?;

    let node_id = node_path.id().await;
    info!("Resolved path to NodeID {node_id}");

    // Parse temporal bounds if provided
    let min_override = if let Some(min_str) = min_bound {
        let timestamp = parse_timestamp(&min_str)?;
        info!("Parsed temporal bounds - min timestamp", timestamp: timestamp);
        Some(timestamp)
    } else {
        None
    };

    let max_override = if let Some(max_str) = max_bound {
        let timestamp = parse_timestamp(&max_str)?;
        Some(timestamp)
    } else {
        None
    };

    // Access the transaction state to add OplogEntry records
    let state = transaction.state()?;

    // Create OplogEntry with temporal overrides for metadata-only version
    let mut temporal_entry = OplogEntry::new_file_series(
        NodeID::root(),                        // part_id (use root as part_id)
        node_id,                               // node_id (from path resolution)
        chrono::Utc::now().timestamp_micros(), // timestamp
        1,                                     // version (should increment properly)
        Vec::new(),                            // empty content for metadata-only version
        0,                                     // min_event_time (will be overridden)
        0,                                     // max_event_time (will be overridden)
        ExtendedAttributes::new(),             // empty extended_attributes
    );

    // Set temporal overrides
    temporal_entry.set_temporal_overrides(min_override, max_override);

    // Add to transaction state records via the persistence layer
    // The state object manages the pending records internally
    state.add_oplog_entry(temporal_entry).await?;

    transaction.commit().await?;

    info!("Created metadata-only FileSeries version with temporal bounds");

    Ok(())
}

/// Parse human-readable timestamp to milliseconds since Unix epoch
fn parse_timestamp(timestamp_str: &str) -> Result<i64> {
    // Try multiple common timestamp formats
    let formats = [
        "%Y-%m-%d %H:%M:%S",   // "2024-01-01 00:00:00"
        "%Y-%m-%dT%H:%M:%S",   // "2024-01-01T00:00:00"
        "%Y-%m-%dT%H:%M:%SZ",  // "2024-01-01T00:00:00Z"
        "%Y-%m-%dT%H:%M:%S%z", // "2024-01-01T00:00:00+00:00"
        "%Y-%m-%d",            // "2024-01-01" (assumes 00:00:00)
    ];

    for format in &formats {
        // Try parsing as naive datetime first, then assume UTC
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
            let utc_dt =
                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive_dt, chrono::Utc);
            return Ok(utc_dt.timestamp_millis());
        }

        // Try parsing as datetime with timezone
        if let Ok(dt) = chrono::DateTime::parse_from_str(timestamp_str, format) {
            return Ok(dt.timestamp_millis());
        }
    }

    // Try parsing date-only format and assume 00:00:00 UTC
    if let Ok(date) = chrono::NaiveDate::parse_from_str(timestamp_str, "%Y-%m-%d") {
        let naive_dt = date.and_hms_opt(0, 0, 0).unwrap();
        let utc_dt =
            chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive_dt, chrono::Utc);
        return Ok(utc_dt.timestamp_millis());
    }

    Err(anyhow!(
        "Could not parse timestamp '{}'. Supported formats: YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS[Z], YYYY-MM-DD",
        timestamp_str
    ))
}
