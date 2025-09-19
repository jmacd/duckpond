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
    let export_targets = discover_export_targets(ship_context, patterns, filesystem).await?;
    
    println!("✅ Found {} export targets:", export_targets.len());
    for target in &export_targets {
        println!("  📄 {} -> {} ({})", target.pond_path, target.output_name, target.file_type);
    }
    
    if export_targets.is_empty() {
        println!("⚠️  No files found matching the specified patterns");
        return Ok(());
    }
    
    println!("✅ Export target discovery completed (Phase 2)");
    println!("📋 Next: Implement DataFusion query construction");
    
    // TODO: Phase 3 - DataFusion query construction
    // TODO: Phase 4 - Export execution  
    // TODO: Phase 5 - Metadata extraction
    
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