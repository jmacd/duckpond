use crate::common::ShipContext;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tinyfs::{EntryType, Error as TinyFsError, NodePath, Visitor};

/// Schema information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSchema {
    pub fields: Vec<TemplateField>,
}

/// Field information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateField {
    pub name: String,        // Field name
    pub instrument: String,  // Instrument path (parsed from field name)
    pub unit: String,        // Unit (parsed from field name)
    pub agg: String,         // Aggregation type (parsed from field name)
}

/// Metadata about an exported file
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportOutput {
    /// Relative path to exported file
    pub file: PathBuf,
    /// Unix timestamp (milliseconds) for start of data
    pub start_time: Option<i64>,
    /// Unix timestamp (milliseconds) for end of data
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
    pub fn construct_with_schema(inputs: Vec<(Vec<String>, ExportOutput)>, schema: TemplateSchema) -> Self {
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
    fn insert_with_schema(&mut self, captures: &[String], output: ExportOutput, schema: &TemplateSchema) {
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
}

/// Read schema from the first parquet file in export directory
async fn read_parquet_schema(export_dir: &std::path::Path) -> Result<TemplateSchema> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    // Find first parquet file in the export directory structure
    let first_parquet = find_first_parquet_file(export_dir)?;
    
    // Open parquet file and extract Arrow schema
    let file = File::open(&first_parquet)
        .map_err(|e| anyhow::anyhow!("Failed to open parquet file {}: {}", first_parquet.display(), e))?;
    
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow::anyhow!("Failed to create parquet reader for {}: {}", first_parquet.display(), e))?;
    
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
    
    find_parquet_recursive(dir)
        .ok_or_else(|| anyhow::anyhow!("No parquet files found in export directory: {}", dir.display()))
}

/// Transform Arrow schema to template schema format (matches original logic)
fn transform_arrow_to_template_schema(arrow_schema: &arrow::datatypes::Schema) -> Result<TemplateSchema> {
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
        return Err(anyhow::anyhow!("field name: unknown format: {}", field_name));
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

/// Export pond data to external files with time partitioning
pub async fn export_command(
    ship_context: &ShipContext,
    patterns: &[String],
    output_dir: &str,
    temporal: &str,
) -> Result<()> {
    // Phase 1: Validation and setup
    print_export_start(patterns, output_dir, temporal);
    validate_export_inputs(patterns, output_dir, temporal)?;
    
    // Phase 2: Core export logic
    let export_summary = export_pond_data(
        ship_context,
        patterns,
        output_dir,
        temporal,
    ).await?;
    
    // Phase 4: Results
    print_export_results(output_dir, &export_summary);
    Ok(())
}

/// Core export engine - handles business logic without UI concerns
async fn export_pond_data(
    ship_context: &ShipContext,
    patterns: &[String], 
    output_dir: &str,
    temporal: &str,
) -> Result<ExportSummary> {
    let mut export_summary = ExportSummary::new();
    let temporal_parts = parse_temporal_parts(temporal);
    
    // Create output directory
    std::fs::create_dir_all(output_dir)?;
    
    // Open transaction for all operations with CLI variables
    let mut ship = ship_context.open_pond().await?;
    
    // Pass CLI template variables to the transaction
    let template_variables = ship_context.template_variables.clone();
    
    let mut stx_guard = ship.begin_transaction(vec!["export".to_string()], template_variables).await?;
    let mut tx_guard = stx_guard.transaction_guard()?;

    let mut accumulated_export_set = ExportSet::Empty;

    for (stage_idx, pattern) in patterns.iter().enumerate() {
        log::debug!("🎯 Stage {} - Exporting pattern: {}", stage_idx + 1, pattern);

        let export_targets = discover_export_targets(&mut tx_guard, pattern.clone()).await?;

        println!("  matched {} files", export_targets.len());

        // For Stage 1, export_set is None
        // For Stage 2+, export_set contains results from all previous stages
        let export_context = if stage_idx == 0 { 
            None 
        } else { 
            Some(&accumulated_export_set) 
        };

        // Add export data to transaction state BEFORE processing targets
        // This ensures template factories can access export data from previous stages
        if let Some(export_data) = export_context {
            let export_json = serde_json::to_value(export_data)
                .map_err(|e| anyhow::anyhow!("Failed to serialize export data: {}", e))?;
            let state = tx_guard.state()?;
            state.add_export_data(export_json.clone());
            log::info!("🔄 STAGE {}: Added export data to transaction state: {:?}", stage_idx + 1, export_json);
        } else {
            log::info!("🔄 STAGE {}: No export data (first stage)", stage_idx + 1);
        }

        // Process each target found by the pattern
        let mut stage_results = Vec::new();
        let mut stage_schemas = Vec::new();

        for target in export_targets {
            let (metadata_results, schema) = export_target(&mut tx_guard, &target, output_dir, &temporal_parts, export_context).await?;
            stage_results.extend(metadata_results.clone());
            stage_schemas.push(schema);
            export_summary.add_export_results(pattern, metadata_results);
        }

        // Accumulate results from this stage for next stages
        let stage_schema = stage_schemas.into_iter().find(|s| !s.fields.is_empty())
            .unwrap_or_else(|| TemplateSchema { fields: vec![] });


        let stage_export_set = ExportSet::construct_with_schema(stage_results.clone(), stage_schema);
        log::info!("📊 STAGE {}: Produced {} export results", stage_idx + 1, stage_results.len());

        // @@@ Hmm, not sure.
        if let ExportSet::Empty = accumulated_export_set {
            accumulated_export_set = stage_export_set;
        } else {
            // Merge stage results into accumulated set
            if let ExportSet::Map(ref mut map) = accumulated_export_set {
                map.insert(format!("stage_{}", stage_idx + 1), Box::new(stage_export_set));
            } else {
                // Convert to map format for multiple stages
                let mut map = std::collections::HashMap::new();
                map.insert("stage_1".to_string(), Box::new(accumulated_export_set));
                map.insert(format!("stage_{}", stage_idx + 1), Box::new(stage_export_set));
                accumulated_export_set = ExportSet::Map(map);
            }
        }
    }
    
    // Commit transaction
    stx_guard.commit().await?;
    
    Ok(export_summary)
}

/// Print export startup information (matches original format)
fn print_export_start(
    patterns: &[String],
    output_dir: &str, 
    temporal: &str,
) {
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

/// Information about an exported file discovered in output directory
#[derive(Debug)]
struct ExportedFileInfo {
    relative_path: std::path::PathBuf,
    start_time: Option<i64>,
    end_time: Option<i64>,
}

/// Discover all parquet files that were created in the export directory
fn discover_exported_files(export_path: &std::path::Path, _base_name: &str) -> Result<Vec<ExportedFileInfo>> {
    let mut files = Vec::new();
    
    fn collect_parquet_files(dir: &std::path::Path, base_path: &std::path::Path, files: &mut Vec<ExportedFileInfo>) -> Result<()> {
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
                let relative_path = path.strip_prefix(base_path)
                    .map_err(|e| anyhow::anyhow!("Failed to compute relative path: {}", e))?
                    .to_path_buf();
                
                // Extract timestamps from temporal partition directories (matches original)
                let (start_time, end_time) = extract_timestamps_from_path(&relative_path)?;
                
                let file_info = ExportedFileInfo {
                    relative_path,
                    start_time,
                    end_time,
                };
                
                files.push(file_info);
            }
        }
        Ok(())
    }
    
    collect_parquet_files(export_path, export_path, &mut files)?;
    
    // Sort files by relative path for consistent output
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    
    Ok(files)
}

/// Extract timestamps from temporal partition directory structure (matches original logic)
fn extract_timestamps_from_path(relative_path: &std::path::Path) -> Result<(Option<i64>, Option<i64>)> {
    log::debug!("🕐 Extracting timestamps from path: {}", relative_path.display());
    
    let components = relative_path.components();
    let mut temporal_parts = std::collections::HashMap::from([
        ("year", 0),
        ("month", 1),
        ("day", 1),
        ("hour", 0),
        ("minute", 0),
        ("second", 0),
    ]);
    
    // Parse temporal partition directories like "year=2024/month=7"
    let mut parsed_parts = Vec::new();
    
    for component in components {
        if let Some(dir_name) = component.as_os_str().to_str() {
            if dir_name.contains('=') {
                let parts: Vec<&str> = dir_name.split('=').collect();
                if parts.len() == 2 {
                    let part_name = parts[0];
                    if let Ok(part_value) = parts[1].parse::<i32>() {
                        if temporal_parts.contains_key(part_name) {
                            temporal_parts.insert(part_name, part_value);
                            parsed_parts.push(part_name);
                            log::debug!("🕐 Parsed temporal part: {}={}", part_name, part_value);
                        }
                    }
                }
            }
        }
    }
    
    log::debug!("🕐 Parsed temporal parts: {:?}", temporal_parts);
    
    if parsed_parts.is_empty() {
        log::debug!("🕐 No temporal parts found, returning None timestamps");
        return Ok((None, None));
    }
    
    // Build start time
    let start_time = build_utc_timestamp(&temporal_parts);
    
    // Build end time by incrementing the last temporal part (matches original logic)
    if let Some(last_part) = parsed_parts.last() {
        let mut end_temporal_parts = temporal_parts.clone();
        if let Some(value) = end_temporal_parts.get_mut(last_part) {
            *value += 1;
        }
        let end_time = build_utc_timestamp(&end_temporal_parts);
        log::debug!("🕐 Computed timestamps: start={}, end={}", start_time, end_time);
        Ok((Some(start_time), Some(end_time)))
    } else {
        Ok((Some(start_time), None))
    }
}

/// Build UTC timestamp from temporal parts (matches original build_utc function)
fn build_utc_timestamp(parts: &std::collections::HashMap<&str, i32>) -> i64 {
    use chrono::{TimeZone, Utc};
    
    let year = *parts.get("year").unwrap_or(&0) as i32;
    let month = *parts.get("month").unwrap_or(&1) as u32;
    let day = *parts.get("day").unwrap_or(&1) as u32;
    let hour = *parts.get("hour").unwrap_or(&0) as u32;
    let minute = *parts.get("minute").unwrap_or(&0) as u32;
    let second = *parts.get("second").unwrap_or(&0) as u32;
    
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .unwrap_or_default()
        .timestamp_millis()
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
    let result = 
        root.visit_with_visitor(&pattern, &mut visitor)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to find files matching '{}': {}", &pattern, e)) ;

    match result {
        Ok(targets) => {
            log::debug!(
                "  ✅ Found {} matches for pattern '{}'",
                targets.len(),
                &pattern
            );
            for (i, target) in targets.iter().enumerate() {
                log::debug!("    Match {}: {} -> {}", i, target.pond_path, target.output_name);
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
            pond_path,
            node_id,
            captured
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
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!("Exporting {} ({:?})", target.pond_path, target.file_type);
    
    // Build output path
    let output_path = std::path::Path::new(output_dir).join(&target.output_name);
    
    // Ensure output directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    // Dispatch to appropriate handler based on file type
    let (results, schema) = match target.file_type {
        EntryType::FileSeries | EntryType::FileTable => {
            export_queryable_file(tx_guard, target, output_path.to_str().unwrap(), temporal_parts).await
        }
        EntryType::FileData => {
            export_raw_file(tx_guard, target, output_path.to_str().unwrap(), export_set).await
        }
        _ => {
            Err(anyhow::anyhow!(
                "Unsupported file type: {:?}. Supported types: FileSeries, FileTable, FileData",
                target.file_type
            ))
        }
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
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!("🔍 export_queryable_file START: target={}, output_path={}", target.pond_path, output_file_path);
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
    let unique_table_name = format!("series_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    
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
    ).await?;

    log::debug!(
        "  ✅ Successfully exported {} rows to {}",
        total_rows,
        export_path.display()
    );

    // Scan the output directory to find all files that were actually created
    let exported_files = discover_exported_files(&export_path, &target.output_name)?;
    log::debug!("📄 Discovered {} exported files for {}", exported_files.len(), target.output_name);
    
    // Read schema from first parquet file (fail fast if no schema available)
    let schema = read_parquet_schema(&export_path).await
        .map_err(|e| anyhow::anyhow!("Failed to read schema from exported parquet files in {}: {}", export_path.display(), e))?;
    log::debug!("📊 Read schema with {} fields from exported parquet files", schema.fields.len());
    
    // Create ExportOutput entries for each discovered file
    let mut results = Vec::new();
    for file_info in exported_files {
        log::debug!("📄 Adding exported file: {}", file_info.relative_path.display());
        let export_output = ExportOutput {
            file: file_info.relative_path,
            start_time: file_info.start_time,
            end_time: file_info.end_time,
        };
        results.push((target.captures.clone(), export_output));
    }
    
    if results.is_empty() {
        return Err(anyhow::anyhow!("No files were exported for target: {}", target.output_name));
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
) -> Result<usize> {
    use tinyfs::Lookup;
    use tlogfs::query::QueryableFile;
    
    log::debug!("🔍 execute_direct_copy_query START: pond_path={}, table_name={}, export_path={}", pond_path, unique_table_name, export_path.display());
    
    // Get SessionContext from transaction 
    let ctx = tx.session_context().await.map_err(|e| anyhow::anyhow!("Failed to get session context: {}", e))?;
    log::debug!("🔍 Got session context");
    
    // Register the file as a table (same logic as execute_sql_on_file_with_table_name)
    let (_, lookup_result) = tinyfs_wd.resolve_path(pond_path).await.map_err(|e| anyhow::anyhow!("Failed to resolve path: {}", e))?;
    
    match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                anyhow::anyhow!("Path {} does not point to a file: {}", pond_path, e)
            })?;
            
            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))?;
            
            match metadata.entry_type {
                tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    
                    // Get NodeIDs
                    let node_id = node_path.id().await;
                    let part_id = {
                        let parent_path = node_path.dirname();
                        let parent_node_path = tinyfs_wd.resolve_path(&parent_path).await
                            .map_err(|e| anyhow::anyhow!("Failed to resolve parent path: {}", e))?;
                        match parent_node_path.1 {
                            tinyfs::Lookup::Found(parent_node) => parent_node.id().await,
                            _ => tinyfs::NodeID::root(),
                        }
                    };
                    
                    // Register table with our unique name
                    let queryable_file = {
                        let file_any = file_guard.as_any();
                        if let Some(sql_derived_file) = file_any.downcast_ref::<tlogfs::sql_derived::SqlDerivedFile>() {
                            Some(sql_derived_file as &dyn QueryableFile)
                        } else if let Some(oplog_file) = file_any.downcast_ref::<tlogfs::file::OpLogFile>() {
                            Some(oplog_file as &dyn QueryableFile)
                        } else {
                            None
                        }
                    };
                    
                    if let Some(queryable_file) = queryable_file {
                        let table_provider = queryable_file.as_table_provider(node_id, part_id, tx).await
                            .map_err(|e| anyhow::anyhow!("Failed to get table provider: {}", e))?;
                        drop(file_guard);
                        
                        ctx.register_table(datafusion::sql::TableReference::bare(unique_table_name), table_provider)
                            .map_err(|e| anyhow::anyhow!("Failed to register table '{}': {}", unique_table_name, e))?;
                    } else {
                        return Err(anyhow::anyhow!("File does not implement QueryableFile trait"));
                    }
                    
                    // Build COPY command with subquery - no MemTable needed!
                    let translated_query = user_sql_query.replace("series", unique_table_name);
                    let mut copy_sql = format!(
                        "COPY ({}) TO '{}' STORED AS PARQUET",
                        translated_query,
                        export_path.to_string_lossy()
                    );
                    
                    // Add partitioning if we have valid partition columns
                    if !temporal_parts.is_empty() {
                        copy_sql.push_str(&format!(
                            " PARTITIONED BY ({})",
                            temporal_parts.join(", ")
                        ));
                    }
                    
                    log::debug!("  🚀 Executing direct COPY: {}", copy_sql);
                    
                    // Execute the COPY command directly
                    let df = ctx.sql(&copy_sql).await
                        .map_err(|e| anyhow::anyhow!("Failed to execute COPY query: {}", e))?;
                    let results = df.collect().await
                        .map_err(|e| anyhow::anyhow!("Failed to execute COPY stream: {}", e))?;
                    
                    // Extract row count from results
                    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
                    log::debug!("    ✅ DataFusion direct COPY completed: {} total rows exported", total_rows);
                    
                    Ok(total_rows)
                }
                _ => {
                    Err(anyhow::anyhow!("File type {:?} does not support SQL queries", metadata.entry_type))
                }
            }
        }
        _ => {
            Err(anyhow::anyhow!("Path '{}' not found", pond_path))
        }
    }
}

/// Export raw data files (FileData) without temporal partitioning
async fn export_raw_file(
    tx_guard: &mut tlogfs::TransactionGuard<'_>,
    target: &ExportTarget,
    output_file_path: &str,
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
                let relative_path = output_path
                    .file_name()
                    .map(|name| std::path::Path::new(name).to_path_buf())
                    .unwrap_or(output_path.to_path_buf());
                
                let (start_time, end_time) = extract_timestamps_from_path(&relative_path).unwrap_or((None, None));

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
                    format!("{}", chrono::DateTime::from_timestamp_millis(start).unwrap_or_default().format("%Y-%m-%d %H:%M:%S"))
                } else {
                    "N/A".to_string()
                };
                let end_str = if let Some(end) = file_output.end_time {
                    format!("{}", chrono::DateTime::from_timestamp_millis(end).unwrap_or_default().format("%Y-%m-%d %H:%M:%S"))
                } else {
                    "N/A".to_string()
                };
                println!("{}  📄 {} (🕐 {} → {})", indent, file_output.file.display(), start_str, end_str);
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
