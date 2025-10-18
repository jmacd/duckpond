use crate::commands::temporal::parse_timestamp_seconds;
use crate::common::ShipContext;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tinyfs::{EntryType, Error as TinyFsError, NodePath, Visitor};

// TODO: the timestamps are confusingly local and/or UTC. do not trust the
// CLI arguments --start-time "2024-03-01 00:00:00" --end-time "2024-08-01 00:00:00"

/// Schema information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSchema {
    pub fields: Vec<TemplateField>,
}

/// Field information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateField {
    pub name: String,       // Field name
    pub instrument: String, // Instrument path (parsed from field name)
    pub unit: String,       // Unit (parsed from field name)
    pub agg: String,        // Aggregation type (parsed from field name)
}

/// Metadata about an exported file
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportOutput {
    /// Relative path to exported file
    pub file: PathBuf,
    /// UTC timestamp for start of data (seconds)
    pub start_time: Option<i64>,
    /// UTC timestamp for end of data (seconds)
    pub end_time: Option<i64>,
}

/// Export leaf containing files and schema information
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportLeaf {
    pub files: Vec<ExportOutput>,
    pub schema: TemplateSchema,
}

/// Hierarchical metadata structure for export results
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ExportSet {
    Empty,
    Files(ExportLeaf),
    Map(HashMap<String, Box<ExportSet>>),
}

impl ExportSet {
    /// Construct hierarchical export set from capture groups, outputs, and schema
    pub fn construct_with_schema(
        inputs: Vec<(Vec<String>, ExportOutput)>,
        schema: TemplateSchema,
    ) -> Self {
        let mut eset = ExportSet::Empty;
        for (captures, output) in inputs {
            eset.insert_with_schema(&captures, output, &schema);
        }
        eset
    }

    /// Construct hierarchical export set from capture groups and outputs (legacy compatibility)
    pub fn construct(inputs: Vec<(Vec<String>, ExportOutput)>) -> Self {
        // For backwards compatibility with raw file exports that don't have schema
        let empty_schema = TemplateSchema { fields: vec![] };
        Self::construct_with_schema(inputs, empty_schema)
    }

    /// Insert output at path specified by capture groups with schema (matches original exactly)
    fn insert_with_schema(
        &mut self,
        captures: &[String],
        output: ExportOutput,
        schema: &TemplateSchema,
    ) {
        if captures.is_empty() {
            // Base case: add to files list with schema
            if let ExportSet::Empty = self {
                *self = ExportSet::Files(ExportLeaf {
                    files: vec![],
                    schema: schema.clone(),
                });
            }
            if let ExportSet::Files(leaf) = self {
                leaf.files.push(output);
            }
            return;
        }

        // Recursive case: traverse capture hierarchy
        if let ExportSet::Empty = self {
            *self = ExportSet::Map(HashMap::new());
        }
        if let ExportSet::Map(map) = self {
            map.entry(captures[0].clone())
                .and_modify(|e| {
                    e.insert_with_schema(&captures[1..], output.clone(), schema);
                })
                .or_insert_with(|| {
                    let mut x = ExportSet::Empty;
                    x.insert_with_schema(&captures[1..], output.clone(), schema);
                    Box::new(x)
                });
        }
    }

    /// Legacy insert method for backwards compatibility
    fn insert(&mut self, captures: &[String], output: ExportOutput) {
        let empty_schema = TemplateSchema { fields: vec![] };
        self.insert_with_schema(captures, output, &empty_schema);
    }

    /// Filter export set to only include entries matching the given capture path
    /// This is used to provide target-specific context based on pattern captures
    pub fn filter_by_captures(&self, target_captures: &[String]) -> ExportSet {
        match self {
            ExportSet::Empty => ExportSet::Empty,
            ExportSet::Files(_leaf) => {
                // If no captures to match, return the whole leaf
                if target_captures.is_empty() {
                    self.clone()
                } else {
                    // Files don't have capture hierarchy, so no match
                    ExportSet::Empty
                }
            }
            ExportSet::Map(map) => {
                if target_captures.is_empty() {
                    // No specific captures requested, return empty
                    ExportSet::Empty
                } else {
                    // Look for matching capture at this level
                    let first_capture = &target_captures[0];
                    if let Some(nested_set) = map.get(first_capture) {
                        // Found matching capture, recurse with remaining captures
                        let remaining_captures = &target_captures[1..];
                        nested_set.filter_by_captures(remaining_captures)
                    } else {
                        // No match found at this level
                        ExportSet::Empty
                    }
                }
            }
        }
    }
}

/// Merge two ExportSets, preserving per-target schemas
fn merge_export_sets(base: ExportSet, other: ExportSet) -> ExportSet {
    match (base, other) {
        (ExportSet::Empty, other) => other,
        (base, ExportSet::Empty) => base,
        (ExportSet::Files(mut base_leaf), ExportSet::Files(other_leaf)) => {
            // Merge file lists, keeping the first non-empty schema
            base_leaf.files.extend(other_leaf.files);
            if base_leaf.schema.fields.is_empty() && !other_leaf.schema.fields.is_empty() {
                base_leaf.schema = other_leaf.schema;
            }
            ExportSet::Files(base_leaf)
        }
        (ExportSet::Map(mut base_map), ExportSet::Map(other_map)) => {
            // Merge maps recursively
            for (key, other_set) in other_map {
                base_map
                    .entry(key)
                    .and_modify(|existing| {
                        *existing =
                            Box::new(merge_export_sets(*existing.clone(), *other_set.clone()));
                    })
                    .or_insert(other_set);
            }
            ExportSet::Map(base_map)
        }
        (ExportSet::Files(base_leaf), ExportSet::Map(other_map)) => {
            // Convert Files to Map and merge
            let mut new_map = HashMap::new();
            new_map.insert("files".to_string(), Box::new(ExportSet::Files(base_leaf)));
            for (key, other_set) in other_map {
                new_map.insert(key, other_set);
            }
            ExportSet::Map(new_map)
        }
        (ExportSet::Map(mut base_map), ExportSet::Files(other_leaf)) => {
            // Add Files to Map
            base_map.insert("files".to_string(), Box::new(ExportSet::Files(other_leaf)));
            ExportSet::Map(base_map)
        }
    }
}

/// Count total number of files in an ExportSet
fn count_export_set_files(export_set: &ExportSet) -> usize {
    match export_set {
        ExportSet::Empty => 0,
        ExportSet::Files(leaf) => leaf.files.len(),
        ExportSet::Map(map) => map
            .values()
            .map(|nested| count_export_set_files(nested))
            .sum(),
    }
}

/// Read schema from the first parquet file in export directory
async fn read_parquet_schema(export_dir: &std::path::Path) -> Result<TemplateSchema> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    // Find first parquet file in the export directory structure
    let first_parquet = find_first_parquet_file(export_dir)?;

    // Open parquet file and extract Arrow schema
    let file = File::open(&first_parquet).map_err(|e| {
        anyhow::anyhow!(
            "Failed to open parquet file {}: {}",
            first_parquet.display(),
            e
        )
    })?;

    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create parquet reader for {}: {}",
            first_parquet.display(),
            e
        )
    })?;

    let arrow_schema = reader_builder.schema();

    // Transform Arrow schema to template format
    transform_arrow_to_template_schema(arrow_schema)
}

/// Find the first parquet file in directory tree (fails fast if none found)
fn find_first_parquet_file(dir: &std::path::Path) -> Result<PathBuf> {
    use std::fs;

    fn find_parquet_recursive(dir: &std::path::Path) -> Option<PathBuf> {
        let entries = fs::read_dir(dir).ok()?;

        for entry in entries {
            let entry = entry.ok()?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
                return Some(path);
            } else if path.is_dir() {
                if let Some(found) = find_parquet_recursive(&path) {
                    return Some(found);
                }
            }
        }
        None
    }

    find_parquet_recursive(dir).ok_or_else(|| {
        anyhow::anyhow!(
            "No parquet files found in export directory: {}",
            dir.display()
        )
    })
}

/// Transform Arrow schema to template schema format (matches original logic)
fn transform_arrow_to_template_schema(
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<TemplateSchema> {
    let mut fields = Vec::new();

    for field in arrow_schema.fields() {
        // Skip system fields like timestamp, rtimestamp (matches original)
        let field_name_lower = field.name().to_lowercase();
        if matches!(field_name_lower.as_str(), "timestamp" | "rtimestamp") {
            continue;
        }

        // Parse field name: expected format "instrument.name.unit.agg" (matches original)
        let template_field = parse_field_name(field.name())?;
        fields.push(template_field);
    }

    Ok(TemplateSchema { fields })
}

/// Parse field name into components (matches original logic exactly)
fn parse_field_name(field_name: &str) -> Result<TemplateField> {
    // Special case for count aggregations: "timestamp.count"
    if field_name == "timestamp.count" {
        return Ok(TemplateField {
            instrument: "system".to_string(),
            name: "timestamp".to_string(),
            unit: "count".to_string(),
            agg: "count".to_string(),
        });
    }

    let mut parts: Vec<&str> = field_name.split('.').collect();

    if parts.len() < 4 {
        return Err(anyhow::anyhow!(
            "field name: unknown format: {}",
            field_name
        ));
    }

    let agg = parts.pop().unwrap().to_string();
    let unit = parts.pop().unwrap().to_string();
    let name = parts.pop().unwrap().to_string();
    let instrument = parts.join(".");

    Ok(TemplateField {
        instrument,
        name,
        unit,
        agg,
    })
}

/// Export summary for managing metadata across multiple patterns (for display/reporting only)
#[derive(Serialize, Clone)]
pub struct ExportSummary {
    pub pattern_results: HashMap<String, ExportSet>,
}

impl ExportSummary {
    pub fn new() -> Self {
        Self {
            pattern_results: HashMap::new(),
        }
    }

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
            self.pattern_results.insert(pattern.to_string(), export_set);
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
        println!("🕐 Temporal filtering enabled:");

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
        println!();
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
    let mut export_summary = ExportSummary::new();
    let temporal_parts = parse_temporal_parts(temporal);

    // Create output directory
    std::fs::create_dir_all(output_dir)?;

    // Open transaction for all operations with CLI variables
    let mut ship = ship_context.open_pond().await?;

    // Pass CLI template variables to the transaction
    let template_variables = ship_context.template_variables.clone();

    let mut stx_guard = ship
        .begin_transaction(
            steward::TransactionOptions::write(vec!["export".to_string()])
                .with_variables(template_variables),
        )
        .await?;
    let mut tx_guard = stx_guard.transaction_guard()?;

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
        let export_targets = discover_export_targets(&mut tx_guard, pattern.clone()).await?;
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
                &mut tx_guard,
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
    stx_guard.commit().await?;

    Ok(export_summary)
}

/// Print export startup information (matches original format)
fn print_export_start(patterns: &[String], output_dir: &str, temporal: &str) {
    // Print pattern processing (matches original "export {} ..." format)
    for pattern in patterns {
        println!("export {} ...", pattern);
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

    println!("📁 Files exported to: {}", output_dir);

    // Show detailed export results (matches original behavior)
    if !export_summary.pattern_results.is_empty() {
        println!("\n📊 Export Context Summary:");
        println!("========================");
        println!("📁 Output Directory: {}", output_dir);
        println!("📄 Total Files Exported: {}", total_files);
        println!("📋 Metadata by Pattern:");

        for (pattern, export_set) in &export_summary.pattern_results {
            println!("  🎯 Pattern: {}", pattern);
            print_export_set(&export_set, "    ");
        }
    } else {
        println!("  (No export metadata collected)");
    }
}

/// Discover all parquet files that were created in the export directory
fn discover_exported_files(
    export_path: &std::path::Path,
    base_path: &std::path::Path,
) -> Result<Vec<ExportOutput>> {
    let mut files = Vec::new();

    fn collect_parquet_files(
        dir: &std::path::Path,
        base_path: &std::path::Path,
        files: &mut Vec<ExportOutput>,
    ) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                // Recursively search subdirectories
                collect_parquet_files(&path, base_path, files)?;
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                // Found a parquet file - calculate relative path
                let relative_path = path
                    .strip_prefix(base_path)
                    .map_err(|e| anyhow::anyhow!("Failed to compute relative path: {}", e))?
                    .to_path_buf();

                // Schema validation now prevents nullable timestamps, so we shouldn't see year=0/month=0 partitions
                // If we do see them, it indicates a new data pipeline bug that should be caught and fixed
                let path_str = relative_path.to_string_lossy();
                if path_str.contains("year=0") || path_str.contains("month=0") {
                    return Err(anyhow::anyhow!(
                        "Invalid temporal partition detected: '{}'. This indicates nullable timestamp data \
                        that should have been caught by schema validation. Check for data pipeline bugs \
                        in SQL queries (NATURAL FULL OUTER JOIN, missing COALESCE, etc.)",
                        relative_path.display()
                    ));
                }

                // Extract timestamps from temporal partition directories (matches original)
                let (start_time, end_time) = extract_timestamps_from_path(&relative_path)?;

                let file_info = ExportOutput {
                    file: relative_path,
                    start_time,
                    end_time,
                };

                files.push(file_info);
            }
        }
        Ok(())
    }

    collect_parquet_files(export_path, base_path, &mut files)?;

    // Sort files by relative path for consistent output
    files.sort_by(|a, b| a.file.cmp(&b.file));

    Ok(files)
}

/// Extract timestamps from temporal partition directory structure (matches original logic)
fn extract_timestamps_from_path(
    relative_path: &std::path::Path,
) -> Result<(Option<i64>, Option<i64>)> {
    log::debug!(
        "🕐 Extracting timestamps from path: {}",
        relative_path.display()
    );

    let components = relative_path.components();
    // Start with empty HashMap - only populated when temporal parts are actually found
    let mut temporal_parts = std::collections::HashMap::new();

    // Parse temporal partition directories like "year=2024/month=7"
    let mut parsed_parts = Vec::new();

    for component in components {
        if let Some(dir_name) = component.as_os_str().to_str() {
            if dir_name.contains('=') {
                let parts: Vec<&str> = dir_name.split('=').collect();
                if parts.len() == 2 {
                    let part_name = parts[0];
                    let part_value_str = parts[1];

                    // Validate that this is a known temporal part
                    if !["year", "month", "day", "hour", "minute", "second"].contains(&part_name) {
                        log::debug!("🕐 Ignoring unknown temporal part: {}", part_name);
                        continue;
                    }

                    // Parse the value, failing fast on invalid formats
                    let part_value = part_value_str.parse::<i32>()
                        .map_err(|e| anyhow::anyhow!(
                            "Invalid temporal partition value in path '{}': '{}={}' - value must be an integer: {}", 
                            relative_path.display(), part_name, part_value_str, e
                        ))?;

                    temporal_parts.insert(part_name, part_value);
                    parsed_parts.push(part_name);
                    log::debug!("🕐 Parsed temporal part: {}={}", part_name, part_value);
                }
            }
        }
    }

    log::debug!("🕐 Parsed temporal parts: {:?}", temporal_parts);
    log::debug!(
        "🕐 Found {} parsed parts: {:?}",
        parsed_parts.len(),
        parsed_parts
    );

    if parsed_parts.is_empty() {
        log::debug!("🕐 No temporal parts found, returning None timestamps");
        return Ok((None, None));
    }

    // Add defaults only for parts that logically should have defaults when missing
    // Year and month are always required if any temporal parsing occurred
    // Day defaults to 1, time components default to 0
    if !temporal_parts.contains_key("day") {
        temporal_parts.insert("day", 1);
    }
    if !temporal_parts.contains_key("hour") {
        temporal_parts.insert("hour", 0);
    }
    if !temporal_parts.contains_key("minute") {
        temporal_parts.insert("minute", 0);
    }
    if !temporal_parts.contains_key("second") {
        temporal_parts.insert("second", 0);
    }

    // Build start time
    let start_time = build_utc_timestamp(&temporal_parts)?;

    // Build end time by incrementing the last temporal part with proper date arithmetic
    if let Some(last_part) = parsed_parts.last() {
        let end_time = calculate_end_time(&temporal_parts, last_part)?;
        log::debug!(
            "🕐 Computed timestamps: start={}, end={}",
            start_time,
            end_time
        );
        Ok((Some(start_time), Some(end_time)))
    } else {
        Ok((Some(start_time), None))
    }
}

/// Calculate end time by properly incrementing the last temporal part using chrono date arithmetic
fn calculate_end_time(
    parts: &std::collections::HashMap<&str, i32>,
    last_part: &str,
) -> Result<i64> {
    use chrono::{Datelike, TimeZone, Utc};

    let year = *parts
        .get("year")
        .ok_or_else(|| anyhow::anyhow!("Missing year in temporal path for end time calculation"))?
        as i32;
    let month = *parts
        .get("month")
        .ok_or_else(|| anyhow::anyhow!("Missing month in temporal path for end time calculation"))?
        as u32;
    let day = *parts
        .get("day")
        .ok_or_else(|| anyhow::anyhow!("Missing day in temporal path for end time calculation"))?
        as u32;
    let hour = *parts
        .get("hour")
        .ok_or_else(|| anyhow::anyhow!("Missing hour in temporal path for end time calculation"))?
        as u32;
    let minute = *parts.get("minute").ok_or_else(|| {
        anyhow::anyhow!("Missing minute in temporal path for end time calculation")
    })? as u32;
    let second = *parts.get("second").ok_or_else(|| {
        anyhow::anyhow!("Missing second in temporal path for end time calculation")
    })? as u32;

    // Validate year - if 0, something went wrong with temporal parsing
    if year <= 0 {
        return Err(anyhow::anyhow!(
            "Invalid year {} in temporal path. Expected path with format like 'year=2024/month=7/day=15'",
            year
        ));
    }

    // Create the start datetime
    let start_dt = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid start date: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                year,
                month,
                day,
                hour,
                minute,
                second
            )
        })?;

    // Add one unit of the last temporal part using proper date arithmetic
    let end_dt = match last_part {
        "year" => {
            // Add 1 year
            start_dt
                .with_year(start_dt.year() + 1)
                .ok_or_else(|| anyhow::anyhow!("Failed to add 1 year to {}", start_dt))?
        }
        "month" => {
            // Add 1 month (handles year rollover automatically)
            if start_dt.month() == 12 {
                // December -> January of next year
                start_dt
                    .with_year(start_dt.year() + 1)
                    .and_then(|dt| dt.with_month(1))
                    .ok_or_else(|| anyhow::anyhow!("Failed to roll over year from December"))?
            } else {
                // Regular month increment
                start_dt
                    .with_month(start_dt.month() + 1)
                    .ok_or_else(|| anyhow::anyhow!("Failed to add 1 month to {}", start_dt))?
            }
        }
        "day" => {
            // Add 1 day (handles month/year rollover automatically)
            start_dt + chrono::Duration::days(1)
        }
        "hour" => {
            // Add 1 hour (handles day/month/year rollover automatically)
            start_dt + chrono::Duration::hours(1)
        }
        "minute" => {
            // Add 1 minute (handles hour/day/month/year rollover automatically)
            start_dt + chrono::Duration::minutes(1)
        }
        "second" => {
            // Add 1 second (handles minute/hour/day/month/year rollover automatically)
            start_dt + chrono::Duration::seconds(1)
        }
        _ => {
            return Err(anyhow::anyhow!("Unknown temporal part: {}", last_part));
        }
    };

    log::debug!(
        "🕐 Date arithmetic: {} + 1 {} = {}",
        start_dt,
        last_part,
        end_dt
    );
    Ok(end_dt.timestamp())
}

/// Build UTC timestamp from temporal parts (matches original build_utc function)
fn build_utc_timestamp(parts: &std::collections::HashMap<&str, i32>) -> Result<i64> {
    use chrono::{TimeZone, Utc};

    let year = *parts
        .get("year")
        .ok_or_else(|| anyhow::anyhow!("Missing year in temporal path"))? as i32;
    let month = *parts
        .get("month")
        .ok_or_else(|| anyhow::anyhow!("Missing month in temporal path"))? as u32;
    let day = *parts
        .get("day")
        .ok_or_else(|| anyhow::anyhow!("Missing day in temporal path"))? as u32;
    let hour = *parts
        .get("hour")
        .ok_or_else(|| anyhow::anyhow!("Missing hour in temporal path"))? as u32;
    let minute = *parts
        .get("minute")
        .ok_or_else(|| anyhow::anyhow!("Missing minute in temporal path"))? as u32;
    let second = *parts
        .get("second")
        .ok_or_else(|| anyhow::anyhow!("Missing second in temporal path"))? as u32;

    // Validate year - if 0, something went wrong with temporal parsing
    if year <= 0 {
        return Err(anyhow::anyhow!(
            "Invalid year {} in temporal path. Expected path with format like 'year=2024/month=7/day=15'",
            year
        ));
    }

    // Validate month range
    if month < 1 || month > 12 {
        return Err(anyhow::anyhow!(
            "Invalid month {} in temporal path. Expected month between 1-12",
            month
        ));
    }

    let timestamp = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid date: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                year,
                month,
                day,
                hour,
                minute,
                second
            )
        })?
        .timestamp();

    Ok(timestamp)
}
fn count_exported_files(output_dir: &str) -> usize {
    use std::fs;

    fn count_files_recursive(dir: &std::path::Path) -> usize {
        let mut count = 0;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        count += count_files_recursive(&path);
                    } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                        count += 1;
                    }
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

    // Validate temporal partitioning options
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
        let node_ref = node.borrow().await;
        let pond_path = node.path().to_string_lossy().to_string();
        let node_id = node.id().await;

        debug!(
            "🔍 TinyFS visitor called: path={}, node_id={:?}, captures={:?}",
            pond_path, node_id, captured
        );

        // Only process files, not directories
        match node_ref.node_type() {
            tinyfs::NodeType::File(file_handle) => {
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
            }
            tinyfs::NodeType::Directory(_) => {
                Err(TinyFsError::Other("Directories not exportable".to_string()))
            }
            tinyfs::NodeType::Symlink(_) => {
                Err(TinyFsError::Other("Symlinks not exportable".to_string()))
            }
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
        let mut path = std::path::PathBuf::from(output_dir);
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
        EntryType::FileSeriesPhysical
        | EntryType::FileSeriesDynamic
        | EntryType::FileTablePhysical
        | EntryType::FileTableDynamic => {
            export_queryable_file(
                tx_guard,
                target,
                output_path.to_str().unwrap(),
                temporal_parts,
                output_dir,
                export_range,
            )
            .await
        }
        EntryType::FileDataPhysical | EntryType::FileDataDynamic => {
            export_raw_file(
                tx_guard,
                target,
                output_path.to_str().unwrap(),
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
            .unwrap()
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
    std::fs::create_dir_all(&export_path).map_err(|e| {
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
        &export_path,
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
    let exported_files = discover_exported_files(&export_path, base_output_path)?;
    log::debug!(
        "📄 Discovered {} exported files for {}",
        exported_files.len(),
        target.output_name
    );

    // Read schema from first parquet file (fail fast if no schema available)
    let schema = read_parquet_schema(&export_path).await.map_err(|e| {
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
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                anyhow::anyhow!("Path {} does not point to a file: {}", pond_path, e)
            })?;

            // Get the entry type and metadata
            let metadata = file_handle
                .metadata()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))?;

            match metadata.entry_type {
                tinyfs::EntryType::FileTablePhysical
                | tinyfs::EntryType::FileTableDynamic
                | tinyfs::EntryType::FileSeriesPhysical
                | tinyfs::EntryType::FileSeriesDynamic => {
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Get NodeIDs
                    let node_id = node_path.id().await;
                    let part_id = {
                        let parent_path = node_path.dirname();
                        let parent_node_path = tinyfs_wd
                            .resolve_path(&parent_path)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to resolve parent path: {}", e))?;
                        match parent_node_path.1 {
                            tinyfs::Lookup::Found(parent_node) => parent_node.id().await,
                            _ => tinyfs::NodeID::root(),
                        }
                    };

                    // Register table with our unique name
                    let queryable_file = tlogfs::sql_derived::try_as_queryable_file(&**file_guard);

                    if let Some(queryable_file) = queryable_file {
                        let state = tx.state()?;
                        let table_provider = queryable_file
                            .as_table_provider(node_id, part_id, &state)
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

                        ctx.register_table(
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
            let node_ref = found.borrow().await;
            if let Ok(file_handle) = node_ref.as_file() {
                let mut reader = file_handle.async_reader().await?;
                let mut content = Vec::new();
                reader.read_to_end(&mut content).await?;

                // Export as raw data
                std::fs::write(&output_path, &content)?;
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

/// Helper function to recursively print export set structure for debugging
fn print_export_set(export_set: &ExportSet, indent: &str) {
    match export_set {
        ExportSet::Empty => {
            println!("{}(no files)", indent);
        }
        ExportSet::Files(leaf) => {
            println!("{}📄 {} exported files:", indent, leaf.files.len());
            for file_output in &leaf.files {
                let start_str = if let Some(start) = file_output.start_time {
                    match chrono::DateTime::from_timestamp(start, 0) {
                        Some(dt) => format!("{}", dt.format("%Y-%m-%d %H:%M:%S")),
                        None => format!("INVALID_TIMESTAMP({})", start),
                    }
                } else {
                    "N/A".to_string()
                };
                let end_str = if let Some(end) = file_output.end_time {
                    match chrono::DateTime::from_timestamp(end, 0) {
                        Some(dt) => format!("{}", dt.format("%Y-%m-%d %H:%M:%S")),
                        None => format!("INVALID_TIMESTAMP({})", end),
                    }
                } else {
                    "N/A".to_string()
                };
                println!(
                    "{}  📄 {} (🕐 {} → {})",
                    indent,
                    file_output.file.display(),
                    start_str,
                    end_str
                );
            }
        }
        ExportSet::Map(map) => {
            println!("{}🗂️  {} capture groups:", indent, map.len());
            for (key, nested_set) in map {
                println!("{}  📁 '{}' wildcard capture:", indent, key);
                print_export_set(nested_set, &format!("{}    ", indent));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_date_arithmetic_month_rollover() {
        // Test the specific bug case: December 2024 should roll to January 2025
        let mut parts = std::collections::HashMap::new();
        parts.insert("year", 2024);
        parts.insert("month", 12);
        parts.insert("day", 1);
        parts.insert("hour", 0);
        parts.insert("minute", 0);
        parts.insert("second", 0);

        let end_time = calculate_end_time(&parts, "month").unwrap();

        // December 1, 2024 00:00:00 + 1 month = January 1, 2025 00:00:00
        // January 1, 2025 00:00:00 UTC = 1735689600 seconds since epoch
        let expected_end_time = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        assert_eq!(
            end_time, expected_end_time,
            "December 2024 + 1 month should equal January 2025, got timestamp {} but expected {}",
            end_time, expected_end_time
        );
    }

    #[test]
    fn test_date_arithmetic_various_rollovers() {
        // Test year rollover
        let mut parts = std::collections::HashMap::new();
        parts.insert("year", 2024);
        parts.insert("month", 1);
        parts.insert("day", 1);
        parts.insert("hour", 0);
        parts.insert("minute", 0);
        parts.insert("second", 0);

        let end_time = calculate_end_time(&parts, "year").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected, "Year rollover failed");

        // Test regular month increment (non-December)
        parts.insert("month", 11);
        let end_time = calculate_end_time(&parts, "month").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2024, 12, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected, "Regular month increment failed");

        // Test day rollover (uses chrono's automatic handling)
        parts.insert("month", 1);
        parts.insert("day", 31);
        let end_time = calculate_end_time(&parts, "day").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2024, 2, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected, "Day rollover to next month failed");
    }
}
