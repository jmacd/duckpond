// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::commands::temporal::parse_timestamp_seconds;
use crate::common::ShipContext;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use tinyfs::{EntryType, Error as TinyFsError, NodePath, Visitor};

// Re-export shared export types from provider crate
pub use provider::export::{
    ExportOutput, ExportSet, TemplateSchema, count_export_set_files, discover_exported_files,
    extract_timestamps_from_path, merge_export_sets, print_export_set, read_parquet_schema,
};

// TODO: the timestamps are confusingly local and/or UTC. do not trust the
// CLI arguments --start-time "2024-03-01 00:00:00" --end-time "2024-08-01 00:00:00"

/// Export summary for managing metadata across multiple patterns (for display/reporting only)
#[derive(Serialize, Clone, Default)]
pub struct ExportSummary {
    pub pattern_results: HashMap<String, ExportSet>,
}

impl ExportSummary {
    pub fn add_export_results(&mut self, pattern: &str, results: Vec<(Vec<String>, ExportOutput)>) {
        // Get existing export set or create empty one
        let existing = self.pattern_results.get_mut(pattern);

        if let Some(existing_set) = existing {
            // Merge new results into existing set
            for (captures, output) in results {
                existing_set.insert(&captures, output);
            }
        } else {
            // Create new export set for this pattern
            let export_set = ExportSet::construct(results);
            _ = self.pattern_results.insert(pattern.to_string(), export_set);
        }
    }
}

/// ExportRange limits the time range for temporal exports.
#[derive(Clone, Default)]
struct ExportRange {
    /// Start of export (inclusive, UTC seconds)
    start_seconds: Option<i64>,
    /// End of export (exclusive, UTC seconds)
    end_seconds: Option<i64>,
}

/// Export pond data to external files with time partitioning
pub async fn export_command(
    ship_context: &ShipContext,
    patterns: &[String],
    output_dir: &str,
    temporal: &str,
    start_time_str: Option<String>,
    end_time_str: Option<String>,
) -> Result<()> {
    // Phase 1: Validation and setup
    print_export_start(patterns, output_dir, temporal);
    validate_export_inputs(patterns, output_dir, temporal)?;

    let mut export_range = ExportRange::default();

    // Log parsed timestamp information if time ranges are provided
    if start_time_str.is_some() || end_time_str.is_some() {
        debug!("🕐 Temporal filtering enabled:");

        if let Some(start_str) = &start_time_str {
            match parse_timestamp_seconds(start_str) {
                Ok(start_seconds) => {
                    log::info!(
                        "Export start time: '{}' → {} seconds (UTC)",
                        start_str,
                        start_seconds
                    );
                    // @@@ Make this a unittest
                    // let start_dt = chrono::DateTime::from_timestamp(start_seconds, 0)
                    //     .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    //     .unwrap_or_else(|| "Invalid timestamp".to_string());
                    // log::debug!("Re-parsed as: {}", start_dt);
                    export_range.start_seconds = Some(start_seconds);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to parse start time '{}': {}",
                        start_str,
                        e
                    ));
                }
            }
        }

        if let Some(end_str) = &end_time_str {
            match parse_timestamp_seconds(end_str) {
                Ok(end_seconds) => {
                    log::info!(
                        "Export end time: '{}' → {} seconds (UTC)",
                        end_str,
                        end_seconds
                    );
                    export_range.end_seconds = Some(end_seconds);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to parse end time '{}': {}",
                        end_str,
                        e
                    ));
                }
            }
        }
    }

    // Phase 2: Core export logic
    let export_summary =
        export_pond_data(ship_context, patterns, output_dir, temporal, export_range).await?;

    // Phase 4: Results
    print_export_results(output_dir, &export_summary);
    Ok(())
}

/// Core export engine - handles business logic without UI concerns
///
/// MULTI-STAGE EXPORT PIPELINE IMPLEMENTATION:
///
/// This function implements a sophisticated multi-stage export pipeline where:
/// 1. Each pattern represents a "stage" that can export files or generate templates
/// 2. Stage N can use the export results from Stage N-1 as context for template processing
/// 3. Each target within a stage gets target-specific filtered context based on its captures
///
/// KEY ARCHITECTURAL FEATURES:
/// - Stage-to-stage handoff: Each stage only sees results from the previous stage (not all previous stages)
/// - Per-target context filtering: Targets with captures ["A", "B"] only see export data matching that path
/// - Clear separation: Stage discovery → Context setup → Target processing → Result handoff
///
/// EXAMPLE FLOW:
/// Stage 1: Export parquet files from `/sensors/*.series` → Results: {temp: files, pressure: files}
/// Stage 2: Process template `/templates/*.tmpl` with captures ["temp"] → Gets only temp-related export context
///
async fn export_pond_data(
    ship_context: &ShipContext,
    patterns: &[String],
    output_dir: &str,
    temporal: &str,
    export_range: ExportRange,
) -> Result<ExportSummary> {
    let mut export_summary = ExportSummary::default();
    let temporal_parts = parse_temporal_parts(temporal);

    // Create output directory
    std::fs::create_dir_all(output_dir)?;

    // Open transaction for all operations with CLI variables
    let mut ship = ship_context.open_pond().await?;

    // Pass CLI template variables to the transaction
    let template_variables = ship_context.template_variables.clone();

    let mut stx_guard = ship
        .begin_write(
            &steward::PondUserMetadata::new(vec!["export".to_string()])
                .with_vars(template_variables),
        )
        .await?;
    let tx_guard = stx_guard.transaction_guard()?;

    // Track results from previous stage to pass as context to next stage
    let mut previous_stage_results = ExportSet::Empty;

    // Multi-stage export pipeline: each stage processes a pattern and can use previous stage results
    for (stage_idx, pattern) in patterns.iter().enumerate() {
        log::info!(
            "🎯 STAGE {}: Processing pattern '{}'",
            stage_idx + 1,
            pattern
        );

        // Find all files matching this stage's pattern
        let export_targets = discover_export_targets(tx_guard, pattern.clone()).await?;
        log::info!(
            "🔍 STAGE {}: Found {} targets matching pattern",
            stage_idx + 1,
            export_targets.len()
        );

        // Determine context from previous stage (Stage 1 has no context, Stage 2+ uses previous results)
        let previous_stage_context = if stage_idx == 0 {
            None
        } else {
            Some(&previous_stage_results)
        };

        // Add previous stage export data to transaction state for template access
        if let Some(export_data) = previous_stage_context {
            let export_json = serde_json::to_value(export_data)
                .map_err(|e| anyhow::anyhow!("Failed to serialize export data: {}", e))?;
            let state = tx_guard.state()?;
            state
                .add_export_data(export_json.clone())
                .map_err(|e| anyhow::anyhow!("Failed to add export data: {}", e))?;
            log::debug!(
                "� STAGE {}: Made previous stage results available to templates",
                stage_idx + 1
            );
        } else {
            log::debug!(
                "� STAGE {}: No previous stage data (first stage)",
                stage_idx + 1
            );
        }

        // Process each individual target found by this stage's pattern
        let mut current_stage_export_set = ExportSet::Empty;

        for target in export_targets {
            log::debug!(
                "� STAGE {}: Processing target '{}' (captures: {:?})",
                stage_idx + 1,
                target.pond_path,
                target.captures
            );

            // FIXED: Create target-specific context filtered by this target's captures
            // This ensures each target gets the right subset of previous stage data
            let target_specific_context = if let Some(prev_stage_data) = previous_stage_context {
                // Filter previous stage data to only include entries matching this target's captures
                // This allows templates to access only the relevant export data based on pattern matching
                let filtered_data = prev_stage_data.filter_by_captures(&target.captures);

                // Only provide context if the filtered result is not empty
                match filtered_data {
                    ExportSet::Empty => {
                        log::debug!(
                            "🔍 STAGE {}: No matching previous stage data for target '{}' with captures {:?}",
                            stage_idx + 1,
                            target.pond_path,
                            target.captures
                        );
                        None
                    }
                    filtered => {
                        log::debug!(
                            "🎯 STAGE {}: Filtered previous stage data for target '{}' using captures {:?}",
                            stage_idx + 1,
                            target.pond_path,
                            target.captures
                        );
                        Some(filtered)
                    }
                }
            } else {
                None
            };

            // CORE EXPORT: This is where each target gets exported (parquet files, templates, etc.)
            let (target_metadata, target_schema) = export_target(
                tx_guard,
                &target,
                output_dir,
                &temporal_parts,
                target_specific_context.as_ref(), // Per-target filtered context
                export_range.clone(),
            )
            .await?;

            // Build ExportSet with this target's specific schema (preserves per-target schemas)
            let target_export_set =
                ExportSet::construct_with_schema(target_metadata.clone(), target_schema);

            // Merge this target's results into the stage's accumulated ExportSet
            current_stage_export_set =
                merge_export_sets(current_stage_export_set, target_export_set);

            // Also add to summary for reporting (this still needs the old format)
            export_summary.add_export_results(pattern, target_metadata.clone());

            log::debug!(
                "✅ STAGE {}: Target '{}' exported {} files",
                stage_idx + 1,
                target.pond_path,
                target_metadata.len()
            );
        }

        // Stage completed - results are already accumulated in current_stage_export_set
        let stage_result_count = count_export_set_files(&current_stage_export_set);
        log::info!(
            "📊 STAGE {}: Completed with {} export results",
            stage_idx + 1,
            stage_result_count
        );

        // FIXED: Pass only current stage results to next stage (not accumulated history)
        // This ensures Stage N+1 only sees Stage N results, not all previous stages
        previous_stage_results = current_stage_export_set;

        log::debug!("🔄 STAGE {}: Results ready for next stage", stage_idx + 1);
    }

    // Commit transaction
    _ = stx_guard.commit().await?;

    Ok(export_summary)
}

/// Print export startup information (matches original format)
fn print_export_start(patterns: &[String], output_dir: &str, temporal: &str) {
    // Print pattern processing (matches original "export {} ..." format)
    for pattern in patterns {
        debug!("export {} ...", pattern);
    }

    // Optional debug info (only shown with debug logging)
    if log::log_enabled!(log::Level::Debug) {
        log::debug!("  Output directory: {}", output_dir);
        log::debug!("  Temporal partitioning: {}", temporal);
    }
}

/// Print export results and summary (matches original format)
fn print_export_results(output_dir: &str, export_summary: &ExportSummary) {
    // Count total files (matches original behavior)
    let total_files = count_exported_files(output_dir);

    debug!("📁 Files exported to: {}", output_dir);

    // Show detailed export results (matches original behavior)
    if !export_summary.pattern_results.is_empty() {
        debug!("\n📊 Export Context Summary:");
        debug!("========================");
        debug!("📁 Output Directory: {}", output_dir);
        debug!("📄 Total Files Exported: {}", total_files);
        debug!("📋 Metadata by Pattern:");

        for (pattern, export_set) in &export_summary.pattern_results {
            debug!("  🎯 Pattern: {}", pattern);
            print_export_set(export_set, "    ");
        }
    } else {
        debug!("  (No export metadata collected)");
    }
}

fn count_exported_files(output_dir: &str) -> usize {
    use std::fs;

    fn count_files_recursive(dir: &std::path::Path) -> usize {
        let mut count = 0;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    count += count_files_recursive(&path);
                } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                    count += 1;
                }
            }
        }
        count
    }

    count_files_recursive(std::path::Path::new(output_dir))
}

fn validate_export_inputs(patterns: &[String], output_dir: &str, temporal: &str) -> Result<()> {
    // Validate patterns
    if patterns.is_empty() {
        return Err(anyhow::anyhow!("At least one pattern must be specified"));
    }

    // Validate output directory
    if output_dir.is_empty() {
        return Err(anyhow::anyhow!("Output directory must be specified"));
    }

    // Validate temporal partitioning options (skip if empty - used for non-temporal exports)
    if temporal.trim().is_empty() {
        log::debug!("  No temporal partitioning (non-temporal export)");
        log::debug!("✅ Input validation passed");
        log::debug!("  {} patterns to process", patterns.len());
        return Ok(());
    }

    let valid_temporal_parts = ["year", "month", "day", "hour", "minute", "second"];
    let temporal_parts: Vec<&str> = temporal.split(',').collect();

    for part in &temporal_parts {
        let part = part.trim();
        if !valid_temporal_parts.contains(&part) {
            return Err(anyhow::anyhow!(
                "Invalid temporal partition '{}'. Valid options: {}",
                part,
                valid_temporal_parts.join(", ")
            ));
        }
    }

    // Check for logical ordering (year should come before month, etc.)
    let part_order = ["year", "month", "day", "hour", "minute", "second"];
    let mut last_index = -1;

    for part in &temporal_parts {
        let part = part.trim();
        if let Some(index) = part_order.iter().position(|&x| x == part) {
            if (index as i32) < last_index {
                return Err(anyhow::anyhow!(
                    "Temporal partitioning must be in chronological order: {}",
                    part_order.join(" > ")
                ));
            }
            last_index = index as i32;
        }
    }

    log::debug!("✅ Input validation passed");
    log::debug!("  {} patterns to process", patterns.len());
    log::debug!("  {} temporal partition levels", temporal_parts.len());

    Ok(())
}

/// Represents a target for export - a pond file and its output configuration
#[derive(Debug, Clone)]
struct ExportTarget {
    /// Path in the pond ("/sensors/temp.series")
    pond_path: String,
    /// Derived output name for the export ("sensors/temp")
    output_name: String,
    /// Type of the file (series, table, etc.)
    file_type: EntryType,
    /// Captured groups from pattern matching
    captures: Vec<String>,
}

/// Parse temporal partitioning string into individual parts
fn parse_temporal_parts(temporal: &str) -> Vec<String> {
    temporal.split(',').map(|s| s.trim().to_string()).collect()
}

/// Discover all pond files matching the export patterns
async fn discover_export_targets(
    tx_guard: &mut tlogfs::TransactionGuard<'_>,
    pattern: String,
) -> Result<Vec<ExportTarget>> {
    let fs = &*tx_guard;
    let root = fs.root().await?;

    log::debug!("🔍 Processing pattern: {}", pattern);

    // Use our custom visitor to collect export targets
    let mut visitor = ExportTargetVisitor::new(&pattern);
    log::debug!("🔍 Starting TinyFS pattern matching for: {}", pattern);
    let result = root
        .visit_with_visitor(&pattern, &mut visitor)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to find files matching '{}': {}", &pattern, e));

    match result {
        Ok(targets) => {
            log::debug!(
                "  ✅ Found {} matches for pattern '{}'",
                targets.len(),
                &pattern
            );
            for (i, target) in targets.iter().enumerate() {
                log::debug!(
                    "    Match {}: {} -> {}",
                    i,
                    target.pond_path,
                    target.output_name
                );
            }
            Ok(targets)
        }
        Err(e) => {
            log::debug!("  ❌ Pattern '{}' failed: {}", &pattern, e);
            Err(e)
        }
    }
}

/// Simple visitor to collect export targets from pattern matches
struct ExportTargetVisitor;

impl ExportTargetVisitor {
    fn new(_pattern: &str) -> Self {
        Self
    }
}

/// Extract output name from pond path and captured groups
fn compute_output_name(pond_path: &str, captures: &[String]) -> String {
    if captures.is_empty() {
        // No captures, use the pond path with extension removed
        let path = std::path::Path::new(pond_path);
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unnamed");

        // Build directory path if there are parent components
        if let Some(parent) = path.parent() {
            let parent_str = parent.to_string_lossy();
            if parent_str == "/" {
                stem.to_string()
            } else {
                // Remove leading slash and combine with stem
                let clean_parent = parent_str.strip_prefix('/').unwrap_or(&parent_str);
                format!("{}/{}", clean_parent, stem)
            }
        } else {
            stem.to_string()
        }
    } else {
        // Use captures to build the output name
        captures.join("/")
    }
}

#[async_trait]
impl Visitor<ExportTarget> for ExportTargetVisitor {
    async fn visit(&mut self, node: NodePath, captured: &[String]) -> tinyfs::Result<ExportTarget> {
        let pond_path = node.path().to_string_lossy().to_string();
        let node_id = node.id();

        debug!(
            "🔍 TinyFS visitor called: path={}, node_id={:?}, captures={:?}",
            pond_path, node_id, captured
        );

        // Only process files, not directories
        if let Some(file_handle) = node.into_file().await {
            let metadata = file_handle.metadata().await?;
            let file_type = metadata.entry_type;

            let output_name = compute_output_name(&pond_path, captured);
            let captures = captured.to_vec();

            debug!(
                "Created export target: {} -> {} ({:?})",
                pond_path, output_name, file_type
            );

            Ok(ExportTarget {
                pond_path,
                output_name,
                file_type,
                captures,
            })
        } else if node.into_dir().await.is_some() {
            Err(TinyFsError::Other("Directories not exportable".to_string()))
        } else {
            Err(TinyFsError::Other("Symlinks not exportable".to_string()))
        }
    }
}

/// Export a single file from pond to external directory
async fn export_target(
    tx_guard: &mut tlogfs::TransactionGuard<'_>,
    target: &ExportTarget,
    output_dir: &str,
    temporal_parts: &[String],
    export_set: Option<&ExportSet>, // Template context from previous export stage
    export_range: ExportRange,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!("Exporting {} ({:?})", target.pond_path, target.file_type);

    // Build output path with captures included in hierarchy
    // For pattern /reduced/single_param/*/*.series capturing ["DO", "res=1h"]
    // Creates output path: OUTDIR/DO/res=1h/ (captures form directory hierarchy)
    let output_path = if target.captures.is_empty() {
        // No captures, use the target output name
        std::path::Path::new(output_dir).join(&target.output_name)
    } else {
        // Use captures to build hierarchical directory structure
        let mut path = PathBuf::from(output_dir);
        for capture in &target.captures {
            path = path.join(capture);
        }
        path
    };

    // Ensure output directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Dispatch to appropriate handler based on file type
    let (results, schema) = match target.file_type {
        EntryType::TablePhysicalSeries
        | EntryType::TableDynamic
        | EntryType::TablePhysicalVersion => {
            export_queryable_file(
                tx_guard,
                target,
                output_path.to_str().expect("utf8"),
                temporal_parts,
                output_dir,
                export_range,
            )
            .await
        }
        EntryType::FilePhysicalVersion | EntryType::FileDynamic => {
            export_raw_file(
                tx_guard,
                target,
                output_path.to_str().expect("utf8"),
                output_dir,
                export_set,
            )
            .await
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported file type: {:?}. Supported types: FileSeries, FileTable, FileData",
            target.file_type
        )),
    }?;

    // Return the results with schema from the specialized export functions
    Ok((results, schema))
}

/// Export queryable files (FileSeries/FileTable) with DataFusion and temporal partitioning
async fn export_queryable_file(
    tx_guard: &mut tlogfs::TransactionGuard<'_>,
    target: &ExportTarget,
    output_file_path: &str,
    temporal_parts: &[String],
    base_output_dir: &str,
    export_range: ExportRange,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!(
        "🔍 export_queryable_file START: target={}, output_path={}",
        target.pond_path,
        output_file_path
    );
    let root = tx_guard.root().await?;

    // Build SQL query with temporal partitioning columns
    let temporal_columns = temporal_parts
        .iter()
        .map(|part| match part.as_str() {
            "year" => "date_part('year', timestamp) as year".to_string(),
            "month" => "date_part('month', timestamp) as month".to_string(),
            "day" => "date_part('day', timestamp) as day".to_string(),
            "hour" => "date_part('hour', timestamp) as hour".to_string(),
            "minute" => "date_part('minute', timestamp) as minute".to_string(),
            _ => format!("date_part('{}', timestamp) as {}", part, part),
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Generate unique table name to avoid conflicts within the same process
    let unique_table_name = format!(
        "series_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("ok")
            .as_nanos()
    );

    // Build user-visible SQL query (using "series" name)
    let user_sql_query = if temporal_columns.is_empty() {
        "SELECT * FROM series".to_string()
    } else {
        format!("SELECT *, {} FROM series", temporal_columns)
    };

    // Translate user query to use unique table name
    let sql_query = user_sql_query.replace("series", &unique_table_name);

    log::debug!("🔍 User query: {}", user_sql_query);
    log::debug!("🔍 Executing translated query: {}", sql_query);
    log::debug!("🔍 Unique table name: {}", unique_table_name);

    // Execute direct COPY query with partitioning - no need for MemTable!
    let export_path = std::path::Path::new(output_file_path);
    log::debug!("  📂 Exporting to: {}", export_path.display());

    // Create output directory
    std::fs::create_dir_all(export_path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create export directory {}: {}",
            export_path.display(),
            e
        )
    })?;

    let total_rows = execute_direct_copy_query(
        &root,
        &target.pond_path,
        &user_sql_query,
        &unique_table_name,
        export_path,
        temporal_parts,
        tx_guard,
        export_range,
    )
    .await?;

    log::debug!(
        "  ✅ Successfully exported {} rows to {}",
        total_rows,
        export_path.display()
    );

    // Scan the output directory to find all files that were actually created
    // We need to compute relative paths from the base_output_dir (not export_path)
    // to include capture groups in the relative path
    let base_output_path = std::path::Path::new(base_output_dir);
    let exported_files = discover_exported_files(export_path, base_output_path)?;
    log::debug!(
        "📄 Discovered {} exported files for {}",
        exported_files.len(),
        target.output_name
    );

    // Read schema from first parquet file (fail fast if no schema available)
    let schema = read_parquet_schema(export_path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read schema from exported parquet files in {}: {}",
            export_path.display(),
            e
        )
    })?;
    log::debug!(
        "📊 Read schema with {} fields from exported parquet files",
        schema.fields.len()
    );

    // Create ExportOutput entries for each discovered file
    let mut results = Vec::new();
    for file_info in exported_files {
        log::debug!("📄 Adding exported file: {}", file_info.file.display());
        results.push((target.captures.clone(), file_info));
    }

    if results.is_empty() {
        return Err(anyhow::anyhow!(
            "No files were exported for target: {}",
            target.output_name
        ));
    }

    Ok((results, schema))
}

/// Execute direct COPY query without MemTable - much cleaner!
async fn execute_direct_copy_query(
    tinyfs_wd: &tinyfs::WD,
    pond_path: &str,
    user_sql_query: &str,
    unique_table_name: &str,
    export_path: &std::path::Path,
    temporal_parts: &[String],
    tx: &mut tlogfs::TransactionGuard<'_>,
    export_range: ExportRange,
) -> Result<usize> {
    use tinyfs::Lookup;

    log::debug!(
        "🔍 execute_direct_copy_query START: pond_path={}, table_name={}, export_path={}",
        pond_path,
        unique_table_name,
        export_path.display()
    );

    // Get SessionContext from transaction
    let ctx = tx
        .session_context()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get session context: {}", e))?;
    log::debug!(
        "🔍 EXPORT: Got SessionContext {:p} for pond_path={}",
        std::sync::Arc::as_ptr(&ctx),
        pond_path
    );
    log::debug!("🔍 Got session context");

    // Register the file as a table (same logic as execute_sql_on_file_with_table_name)
    let (_, lookup_result) = tinyfs_wd
        .resolve_path(pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to resolve path: {}", e))?;

    match lookup_result {
        Lookup::Found(node_path) => {
            let file_handle = node_path.as_file().await.map_err(|e| {
                anyhow::anyhow!("Path {} does not point to a file: {}", pond_path, e)
            })?;

            // Get the entry type and metadata
            let metadata = file_handle
                .metadata()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))?;

            match metadata.entry_type {
                EntryType::TablePhysicalVersion
                | EntryType::TablePhysicalSeries
                | EntryType::TableDynamic => {
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Get FileID
                    let node_id = node_path.id();

                    // Register table with our unique name
                    let queryable_file = file_guard.as_queryable();

                    if let Some(queryable_file) = queryable_file {
                        let state = tx.state()?;
                        let provider_context = state.as_provider_context();
                        let table_provider = queryable_file
                            .as_table_provider(node_id, &provider_context)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to get table provider: {}", e))?;

                        // TODO: RESTORE FAIL-FAST SCHEMA VALIDATION!
                        //
                        // The following schema validation was removed as a temporary workaround for nullable timestamp
                        // issues in SQL-derived series and temporal-reduce factories. This validation should be
                        // RESTORED once the root cause is fixed in the data pipeline.
                        //
                        // The validation prevents nullable timestamp columns that create invalid year=0/month=0 partitions
                        // in DataFusion's temporal partitioning. Without this validation, we silently create broken
                        // partition structures that confuse users and hide data pipeline bugs.
                        //
                        // FIXED: Root cause was NATURAL FULL OUTER JOIN queries in combine.yaml creating nullable timestamps.
                        // Solution: Use explicit COALESCE(table1.timestamp, table2.timestamp) AS timestamp in SQL queries.

                        // RESTORED: Schema validation for non-nullable timestamps
                        let schema = table_provider.schema();
                        log::debug!(
                            "🔍 SCHEMA VALIDATION for '{}': Schema has {} fields",
                            pond_path,
                            schema.fields().len()
                        );

                        // Log all field information for debugging
                        for (i, field) in schema.fields().iter().enumerate() {
                            log::debug!(
                                "🔍   Field {}: name='{}', data_type={:?}, nullable={}",
                                i,
                                field.name(),
                                field.data_type(),
                                field.is_nullable()
                            );
                        }

                        if let Ok(timestamp_field) = schema.field_with_name("timestamp") {
                            log::debug!(
                                "🔍 TIMESTAMP FIELD: name='{}', data_type={:?}, nullable={}",
                                timestamp_field.name(),
                                timestamp_field.data_type(),
                                timestamp_field.is_nullable()
                            );

                            if timestamp_field.is_nullable() {
                                log::error!("❌ NULLABLE TIMESTAMP DETECTED in '{}'", pond_path);
                                return Err(anyhow::anyhow!(
                                    "FileSeries schema violation in '{}': timestamp column is nullable. \
                                    FileSeries must have non-nullable timestamp columns for temporal partitioning. \
                                    Nullable timestamps would create invalid year=0/month=0 partitions. \
                                    This indicates a data pipeline bug in SQL queries (check for NATURAL FULL OUTER JOIN usage).",
                                    pond_path
                                ));
                            }
                            log::debug!(
                                "✅ Timestamp column is non-nullable - schema validation passed"
                            );
                        } else {
                            log::error!("❌ NO TIMESTAMP COLUMN found in '{}'", pond_path);
                            return Err(anyhow::anyhow!(
                                "FileSeries schema error in '{}': no timestamp column found",
                                pond_path
                            ));
                        }

                        drop(file_guard);

                        _ = ctx
                            .register_table(
                                datafusion::sql::TableReference::bare(unique_table_name),
                                table_provider,
                            )
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to register table '{}': {}",
                                    unique_table_name,
                                    e
                                )
                            })?;
                    } else {
                        return Err(anyhow::anyhow!(
                            "File does not implement QueryableFile trait"
                        ));
                    }

                    // Build COPY command with subquery - no MemTable needed!
                    let mut translated_query = user_sql_query.replace("series", unique_table_name);

                    // Add temporal filtering WHERE clauses if time ranges are specified
                    if export_range.start_seconds.is_some() || export_range.end_seconds.is_some() {
                        let mut where_clauses = Vec::new();

                        if let Some(start_seconds) = export_range.start_seconds {
                            where_clauses
                                .push(format!("timestamp >= CAST({} AS TIMESTAMP)", start_seconds));
                            log::debug!(
                                "  🕐 Adding start time filter: timestamp >= CAST({} AS TIMESTAMP)",
                                start_seconds
                            );
                        }

                        if let Some(end_seconds) = export_range.end_seconds {
                            where_clauses
                                .push(format!("timestamp <= CAST({} AS TIMESTAMP)", end_seconds));
                            log::debug!(
                                "  🕐 Adding end time filter: timestamp <= CAST({} AS TIMESTAMP)",
                                end_seconds
                            );
                        }

                        // Add WHERE clause to the query
                        let where_clause = where_clauses.join(" AND ");

                        // Check if query already has WHERE clause and append accordingly
                        if translated_query.to_lowercase().contains(" where ") {
                            translated_query = format!(
                                "SELECT * FROM ({}) WHERE {}",
                                translated_query, where_clause
                            );
                        } else {
                            // Simple case: add WHERE to basic SELECT
                            if translated_query
                                .trim()
                                .to_lowercase()
                                .starts_with("select ")
                            {
                                translated_query =
                                    format!("{} WHERE {}", translated_query, where_clause);
                            } else {
                                // Complex case: wrap in subquery
                                translated_query = format!(
                                    "SELECT * FROM ({}) WHERE {}",
                                    translated_query, where_clause
                                );
                            }
                        }

                        log::debug!("  📊 Temporal filtered query: {}", translated_query);
                    }

                    let mut copy_sql = format!(
                        "COPY ({}) TO '{}' STORED AS PARQUET",
                        translated_query,
                        export_path.to_string_lossy()
                    );

                    // Add partitioning if we have valid partition columns
                    if !temporal_parts.is_empty() {
                        copy_sql
                            .push_str(&format!(" PARTITIONED BY ({})", temporal_parts.join(", ")));
                    }

                    log::debug!("  🚀 Executing direct COPY: {}", copy_sql);

                    // Execute the COPY command directly
                    let df = ctx
                        .sql(&copy_sql)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to execute COPY query: {}", e))?;
                    let results = df
                        .collect()
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to execute COPY stream: {}", e))?;

                    // Extract row count from results
                    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
                    log::debug!(
                        "    ✅ DataFusion direct COPY completed: {} total rows exported",
                        total_rows
                    );

                    Ok(total_rows)
                }
                _ => Err(anyhow::anyhow!(
                    "File type {:?} does not support SQL queries",
                    metadata.entry_type
                )),
            }
        }
        _ => Err(anyhow::anyhow!("Path '{}' not found", pond_path)),
    }
}

/// Export raw data files (FileData) without temporal partitioning
async fn export_raw_file(
    tx_guard: &mut tlogfs::TransactionGuard<'_>,
    target: &ExportTarget,
    output_file_path: &str,
    base_output_dir: &str,
    _export_set: Option<&ExportSet>, // Template context (unused for raw files)
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    use tokio::io::AsyncReadExt;

    let output_path = std::path::Path::new(output_file_path);

    // Read file content from pond
    let data_wd = tx_guard.root().await?;
    let (_parent_wd, lookup_result) = data_wd.resolve_path(&target.pond_path).await?;

    match lookup_result {
        tinyfs::Lookup::Found(found) => {
            if let Some(file_handle) = found.into_file().await {
                let mut reader = file_handle.async_reader().await?;
                let mut content = Vec::new();
                _ = reader.read_to_end(&mut content).await?;

                // Export as raw data
                std::fs::write(output_path, &content)?;
                log::debug!("  💾 Exported raw data: {}", output_path.display());

                // Try to discover any temporal information from the output path structure
                // Compute relative path from base output directory to include captures
                let base_output_path = std::path::Path::new(base_output_dir);
                let relative_path = output_path
                    .strip_prefix(base_output_path)
                    .map(|p| p.to_path_buf())?;

                let (start_time, end_time) = extract_timestamps_from_path(&relative_path)?;

                let export_output = ExportOutput {
                    file: relative_path,
                    start_time,
                    end_time,
                };

                // Return flat representation with captures from pattern matching
                let empty_schema = TemplateSchema { fields: vec![] };
                Ok((vec![(target.captures.clone(), export_output)], empty_schema))
            } else {
                Err(anyhow::anyhow!("Path is not a file: {}", target.pond_path))
            }
        }
        tinyfs::Lookup::NotFound(_, _) => {
            Err(anyhow::anyhow!("File not found: {}", target.pond_path))
        }
        tinyfs::Lookup::Empty(_) => Err(anyhow::anyhow!(
            "Path points to empty directory: {}",
            target.pond_path
        )),
    }
}
