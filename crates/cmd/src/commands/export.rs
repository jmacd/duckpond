use anyhow::Result;
use async_trait::async_trait;
use crate::common::{ShipContext, FilesystemChoice};
use tinyfs::{Visitor, NodePath, Error as TinyFsError};
use log::debug;

/// Export pond data to external Parquet files with time partitioning
pub async fn export_command(
    ship_context: &ShipContext,
    patterns: &[String],
    output_dir: &str,
    temporal: &str,
    filesystem: FilesystemChoice,
    overwrite: bool,
    keep_partition_columns: bool,
) -> Result<()> {
    println!("🚀 Starting pond export...");
    println!("  Patterns: {:?}", patterns);
    println!("  Output directory: {}", output_dir);
    println!("  Temporal partitioning: {}", temporal);
    println!("  Filesystem: {:?}", filesystem);
    println!("  Overwrite: {}", overwrite);
    println!("  Keep partition columns: {}", keep_partition_columns);
    
    // Phase 1: Basic structure - validate inputs
    validate_export_inputs(patterns, output_dir, temporal)?;
    
    // Phase 2: Pattern matching and target discovery
    let temporal_parts = parse_temporal_parts(temporal);
    let export_targets = discover_export_targets(ship_context, patterns, filesystem.clone()).await?;
    
    println!("✅ Found {} export targets:", export_targets.len());
    for target in &export_targets {
        println!("  📄 {} -> {} ({})", target.pond_path, target.output_name, target.file_type);
    }
    
    if export_targets.is_empty() {
        println!("⚠️  No files found matching the specified patterns");
        return Ok(());
    }
    
    // Phase 3: DataFusion query construction and export execution
    execute_export_targets(
        ship_context,
        export_targets,
        output_dir,
        &temporal_parts,
        filesystem,
        overwrite,
        keep_partition_columns,
    ).await?;
    
    Ok(())
}

fn validate_export_inputs(
    patterns: &[String],
    output_dir: &str,
    temporal: &str,
) -> Result<()> {
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
    
    println!("✅ Input validation passed");
    println!("  {} patterns to process", patterns.len());
    println!("  {} temporal partition levels", temporal_parts.len());
    
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
    file_type: String,
    /// Captured groups from pattern matching
    captures: Vec<String>,
}

/// Parse temporal partitioning string into individual parts
fn parse_temporal_parts(temporal: &str) -> Vec<String> {
    temporal.split(',')
        .map(|s| s.trim().to_string())
        .collect()
}

/// Discover all pond files matching the export patterns
async fn discover_export_targets(
    ship_context: &ShipContext,
    patterns: &[String],
    filesystem: FilesystemChoice,
) -> Result<Vec<ExportTarget>> {
    // For now, only support data filesystem
    if filesystem == FilesystemChoice::Control {
        return Err(anyhow::anyhow!("Control filesystem export not yet implemented"));
    }
    
    let mut ship = ship_context.open_pond().await?;
    let tx = ship.begin_transaction(vec!["export".to_string()]).await
        .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
    
    let mut all_targets = Vec::new();
    
    // Process each pattern
    for pattern in patterns {
        println!("🔍 Processing pattern: {}", pattern);
        
        let result = {
            let fs = &*tx; // StewardTransactionGuard derefs to FS
            let root = fs.root().await?;
            
            // Use our custom visitor to collect export targets
            let mut visitor = ExportTargetVisitor::new(pattern);
            root.visit_with_visitor(pattern, &mut visitor).await
                .map_err(|e| anyhow::anyhow!("Failed to find files matching '{}': {}", pattern, e))
        };
        
        match result {
            Ok(targets) => {
                println!("  ✅ Found {} matches for pattern '{}'", targets.len(), pattern);
                all_targets.extend(targets);
            }
            Err(e) => {
                println!("  ❌ Pattern '{}' failed: {}", pattern, e);
                // Continue with other patterns rather than failing completely
            }
        }
    }
    
    // Commit the transaction
    tx.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
    
    Ok(all_targets)
}

/// Execute DataFusion query construction and export for discovered targets
async fn execute_export_targets(
    ship_context: &ShipContext,
    targets: Vec<ExportTarget>,
    output_dir: &str,
    temporal_parts: &[String],
    filesystem: FilesystemChoice,
    overwrite: bool,
    keep_partition_columns: bool,
) -> Result<()> {
    if targets.is_empty() {
        println!("⚠️  No files found matching the specified patterns");
        return Ok(());
    }

    println!("✅ Found {} export targets:", targets.len());
    for target in &targets {
        println!("  📄 {} -> {} ({})", target.pond_path, target.output_name, target.file_type);
    }

    // Execute exports for all targets
    for target in targets {
        match export_single_target(ship_context, &target, output_dir, temporal_parts, filesystem.clone(), overwrite, keep_partition_columns).await {
            Ok(()) => {
                println!("  ✅ Exported {} successfully", target.output_name);
            }
            Err(e) => {
                println!("  ❌ Failed to export {}: {}", target.output_name, e);
                // Continue with other targets rather than failing completely
            }
        }
    }
    
    println!("✅ Export completed successfully!");
    println!("� Files exported to: {}", output_dir);
    
    Ok(())
}

/// Custom visitor to collect export targets from pattern matches
struct ExportTargetVisitor {
    pattern: String,
}

impl ExportTargetVisitor {
    fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
        }
    }
    
    /// Extract output name from pond path and captured groups
    fn compute_output_name(&self, pond_path: &str, captures: &[String]) -> String {
        if captures.is_empty() {
            // No captures, use the pond path with extension removed
            let path = std::path::Path::new(pond_path);
            let stem = path.file_stem()
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
}

#[async_trait]
impl Visitor<ExportTarget> for ExportTargetVisitor {
    async fn visit(
        &mut self,
        node: NodePath,
        captured: &[String],
    ) -> tinyfs::Result<ExportTarget> {
        let node_ref = node.borrow().await;
        let pond_path = node.path().to_string_lossy().to_string();
        
        debug!("ExportTargetVisitor: Visiting path {} with {} captures", pond_path, captured.len());
        
        // Only process files, not directories
        match node_ref.node_type() {
            tinyfs::NodeType::File(file_handle) => {
                let metadata = file_handle.metadata().await?;
                let file_type = metadata.entry_type.as_str().to_string();
                
                // Only export queryable file types (series, table, etc.)
                if !is_exportable_file_type(&file_type) {
                    return Err(TinyFsError::Other(format!("Non-exportable file type: {}", file_type)));
                }
                
                let output_name = self.compute_output_name(&pond_path, captured);
                let captures = captured.to_vec();
                
                debug!("ExportTargetVisitor: Created target {} -> {} ({})", pond_path, output_name, file_type);
                
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

/// Check if a file type is exportable (contains queryable data)
fn is_exportable_file_type(file_type: &str) -> bool {
    // FileSeries files contain temporal data and are exportable
    // FileTable files contain queryable table data and are also exportable
    matches!(file_type, "file:series" | "file:table")
}

/// Export a single target using DataFusion query construction (Phase 3)
async fn export_single_target(
    ship_context: &ShipContext,
    target: &ExportTarget,
    output_dir: &str,
    temporal_parts: &[String],
    _filesystem: FilesystemChoice,
    _overwrite: bool,
    _keep_partition_columns: bool,
) -> Result<()> {
    use futures::StreamExt;
    
    // TODO: Phase 3 - DataFusion query construction and export
    // This follows the same pattern as cat command but exports to Parquet instead of stdout
    
    let mut ship = ship_context.open_pond().await?;
    let mut tx = ship.begin_transaction(vec!["export".to_string(), target.pond_path.clone()]).await
        .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
    
    // Use the same infrastructure as cat command
    let root = tx.root().await?;
    
    // Build SQL query with temporal partitioning columns
    let temporal_columns = temporal_parts.iter()
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
    
    let sql_query = if temporal_columns.is_empty() {
        "SELECT * FROM series".to_string()
    } else {
        format!("SELECT *, {} FROM series", temporal_columns)
    };
    
    println!("🔍 Executing query: {}", sql_query);
    
    // Execute the query using the same infrastructure as cat
    let mut stream = tlogfs::execute_sql_on_file(&root, &target.pond_path, &sql_query, tx.transaction_guard()?)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute SQL query on '{}': {}", target.pond_path, e))?;
    
    // Collect all batches for export
    let mut batches = Vec::new();
    let mut total_rows = 0;
    
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|e| anyhow::anyhow!("Failed to process batch from stream: {}", e))?;
        total_rows += batch.num_rows();
        batches.push(batch);
    }
    
    println!("  📊 Query returned {} rows in {} batches", total_rows, batches.len());
    
    // Commit the query transaction first
    tx.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
    
    // Phase 4: Execute actual Parquet export with partitioning
    if batches.is_empty() {
        println!("  ⚠️  No data to export");
        return Ok(());
    }
    
    let export_path = std::path::Path::new(output_dir).join(&target.output_name);
    println!("  📂 Exporting to: {}", export_path.display());
    
    // Create output directory
    std::fs::create_dir_all(&export_path)
        .map_err(|e| anyhow::anyhow!("Failed to create export directory {}: {}", export_path.display(), e))?;
    
    // For now, implement simple partitioning by grouping data manually
    // TODO: Use DataFusion's FileSinkConfig once we have the proper setup
    export_batches_with_temporal_partitioning(
        &batches,
        &export_path,
        temporal_parts,
    ).await?;
    
    println!("  ✅ Successfully exported {} rows to {}", total_rows, export_path.display());
    
    Ok(())
}

/// Export batches with temporal partitioning to Hive-style directory structure
async fn export_batches_with_temporal_partitioning(
    batches: &[arrow::record_batch::RecordBatch],
    export_path: &std::path::Path,
    temporal_parts: &[String],
) -> Result<()> {
    // Get the schema from the first batch
    if batches.is_empty() {
        return Ok(());
    }
    
    let schema = batches[0].schema();
    println!("  🔍 Schema: {:?}", schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    
    // Find temporal partition columns in the schema
    let mut partition_column_indices = Vec::new();
    for part in temporal_parts {
        if let Ok(index) = schema.index_of(part) {
            partition_column_indices.push((part.clone(), index));
        } else {
            println!("  ⚠️  Warning: Temporal partition column '{}' not found in schema", part);
        }
    }
    
    if partition_column_indices.is_empty() {
        // No partition columns found, export as single file
        let output_file = export_path.join("data.parquet");
        write_parquet_file(&output_file, batches).await?;
        println!("    📄 Wrote {}", output_file.display());
        return Ok(());
    }
    
    // Group batches by partition values
    use std::collections::HashMap;
    let mut partitioned_data: HashMap<Vec<String>, Vec<arrow::record_batch::RecordBatch>> = HashMap::new();
    
    for batch in batches {
        // Extract partition values from each row
        let row_count = batch.num_rows();
        
        for row_idx in 0..row_count {
            let mut partition_values = Vec::new();
            
            // Extract partition column values for this row
            for (part_name, col_idx) in &partition_column_indices {
                let column = batch.column(*col_idx);
                let value = extract_partition_value(column, row_idx, part_name)?;
                partition_values.push(value);
            }
            
            // Create a single-row batch for this partition
            let row_batch = create_single_row_batch(batch, row_idx)?;
            
            partitioned_data.entry(partition_values)
                .or_insert_with(Vec::new)
                .push(row_batch);
        }
    }
    
    // Write each partition to its Hive-style directory
    for (partition_values, partition_batches) in partitioned_data {
        let mut partition_path = export_path.to_path_buf();
        
        // Build Hive-style path: year=2024/month=05/day=15/
        for (i, (part_name, _)) in partition_column_indices.iter().enumerate() {
            let value = &partition_values[i];
            partition_path.push(format!("{}={}", part_name, value));
        }
        
        // Create the partition directory
        std::fs::create_dir_all(&partition_path)
            .map_err(|e| anyhow::anyhow!("Failed to create partition directory {}: {}", partition_path.display(), e))?;
        
        // Write the partition data
        let output_file = partition_path.join("data.parquet");
        write_parquet_file(&output_file, &partition_batches).await?;
        
        let total_partition_rows: usize = partition_batches.iter().map(|b| b.num_rows()).sum();
        println!("    📄 Wrote {} rows to {}", total_partition_rows, output_file.display());
    }
    
    Ok(())
}

/// Extract partition value from Arrow column at given row index
fn extract_partition_value(
    column: &arrow::array::ArrayRef,
    row_index: usize,
    part_name: &str,
) -> Result<String> {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    
    if column.is_null(row_index) {
        return Ok("null".to_string());
    }
    
    match column.data_type() {
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast Int32 column for {}", part_name))?;
            Ok(array.value(row_index).to_string())
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast Int64 column for {}", part_name))?;
            Ok(array.value(row_index).to_string())
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast String column for {}", part_name))?;
            Ok(array.value(row_index).to_string())
        }
        _ => {
            Err(anyhow::anyhow!("Unsupported partition column data type: {:?} for {}", column.data_type(), part_name))
        }
    }
}

/// Create a single-row batch from a larger batch
fn create_single_row_batch(
    batch: &arrow::record_batch::RecordBatch,
    row_index: usize,
) -> Result<arrow::record_batch::RecordBatch> {
    let columns: Result<Vec<_>, _> = batch.columns()
        .iter()
        .map(|col| arrow::compute::kernels::take::take(col, &arrow::array::UInt64Array::from(vec![row_index as u64]), None))
        .collect();
    
    let columns = columns
        .map_err(|e| anyhow::anyhow!("Failed to create single-row batch: {}", e))?;
    
    arrow::record_batch::RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}

/// Write batches to a single Parquet file
async fn write_parquet_file(
    output_path: &std::path::Path,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<()> {
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    
    if batches.is_empty() {
        return Ok(());
    }
    
    let file = File::create(output_path)
        .map_err(|e| anyhow::anyhow!("Failed to create parquet file {}: {}", output_path.display(), e))?;
    
    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)
        .map_err(|e| anyhow::anyhow!("Failed to create ArrowWriter: {}", e))?;
    
    for batch in batches {
        writer.write(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {}", e))?;
    }
    
    writer.close()
        .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;
    
    Ok(())
}