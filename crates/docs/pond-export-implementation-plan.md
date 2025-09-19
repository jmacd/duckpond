# DuckPond Export Implementation Plan

## Overview

This document outlines the implementation plan for adding time-partitioned Parquet export functionality to DuckPond using DataFusion's partitioning system. The export feature will allow users to extract pond data into external Hive-formatted directory structures for integration with other data systems.

## Background

### Original Implementation (DuckDB)
The original DuckPond used DuckDB's `COPY TO` command with `PARTITION_BY` clauses:
```sql
COPY (SELECT *, YEAR(Timestamp) AS year, MONTH(Timestamp) AS month 
      FROM pond_data) 
TO '/export/path' 
(FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE)
```

### New Implementation (DataFusion)
DataFusion uses a programmatic approach with:
- `FileSinkConfig` for partition configuration
- `hive_style_partitions_demuxer` for dynamic partitioning
- `date_part()` functions for temporal extraction
- Async parallel writing to ObjectStore

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Pond Query    │───▶│  DataFusion SQL │───▶│ Temporal Columns│
│  (patterns)     │    │  + date_part()  │    │ (year,month,day)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Hive Directory  │◀───│ Partition Demux │◀───│ FileSinkConfig  │
│ Structure       │    │ (parallel write)│    │ Configuration   │
│ year=2024/...   │    └─────────────────┘    └─────────────────┘
└─────────────────┘
```

## Implementation Phases

## Phase 1: Core Export Infrastructure

### 1.1 Add Export Command to CLI
**File**: `crates/cmd/src/main.rs`
**Location**: Add to `Commands` enum

```rust
/// Export pond data to external Parquet files with time partitioning
Export {
    /// File patterns to export (e.g., "/sensors/*.series")
    patterns: Vec<String>,
    /// Output directory for exported files
    #[arg(long, short = 'o')]
    output_dir: String,
    /// Temporal partitioning levels (comma-separated: year,month,day,hour,minute)
    #[arg(long, default_value = "year,month,day")]
    temporal: String,
    /// Which filesystem to export from
    #[arg(long, short = 'f', default_value = "data")]
    filesystem: FilesystemChoice,
    /// Overwrite existing files
    #[arg(long)]
    overwrite: bool,
    /// Keep partition columns in the data files
    #[arg(long)]
    keep_partition_columns: bool,
},
```

### 1.2 Create Export Command Handler
**File**: `crates/cmd/src/commands/export.rs` (new file)

```rust
use anyhow::Result;
use datafusion::prelude::*;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use std::path::Path;
use crate::common::ShipContext;

pub async fn export_command(
    patterns: Vec<String>,
    output_dir: String,
    temporal: String,
    filesystem: crate::common::FilesystemChoice,
    overwrite: bool,
    keep_partition_columns: bool,
    ship_ctx: &ShipContext,
) -> Result<()> {
    // Implementation phases:
    // 1. Pattern matching and pond data discovery
    // 2. DataFusion query construction with temporal columns
    // 3. FileSinkConfig setup for Hive partitioning
    // 4. Export execution with progress reporting
    // 5. Metadata extraction and result reporting
    todo!("Implement in subsequent phases")
}
```

### 1.3 Pattern Matching and Data Discovery
**Objective**: Find all pond files matching export patterns

```rust
// Similar to original pond.visit_path() logic
async fn discover_export_targets(
    patterns: Vec<String>,
    tx_guard: &mut StewardTransactionGuard,
    filesystem: FilesystemChoice,
) -> Result<Vec<ExportTarget>> {
    let mut targets = Vec::new();
    
    for pattern in patterns {
        let (path, glob) = parse_pattern(&pattern)?;
        let matches = find_matching_files(tx_guard, &path, &glob, filesystem).await?;
        
        for file_match in matches {
            targets.push(ExportTarget {
                pond_path: file_match.path,
                output_name: extract_capture_groups(&pattern, &file_match.path)?,
                file_type: file_match.file_type,
            });
        }
    }
    
    Ok(targets)
}

struct ExportTarget {
    pond_path: String,           // Path in pond ("/sensors/temp.series")
    output_name: String,         // Derived output name ("sensors/temp")
    file_type: FileType,         // Series, Table, etc.
}
```

## Phase 2: DataFusion Query Integration

### 2.1 Query Construction with Temporal Columns
**Objective**: Convert pond queries to DataFusion SQL with temporal partitioning

```rust
async fn build_export_query(
    target: &ExportTarget,
    temporal_parts: &[String],
    tx_guard: &mut StewardTransactionGuard,
) -> Result<DataFrame> {
    // 1. Get the base query for this pond file (similar to sql_for())
    let base_query = get_pond_file_query(tx_guard, &target.pond_path).await?;
    
    // 2. Build temporal column extractions
    let temporal_columns = temporal_parts.iter()
        .map(|part| format!("date_part('{}', timestamp_col) as {}", part, part))
        .collect::<Vec<_>>()
        .join(", ");
    
    // 3. Construct final query
    let export_query = format!(
        "SELECT *, {} FROM ({})",
        temporal_columns,
        base_query
    );
    
    // 4. Execute via DataFusion
    let ctx = tx_guard.session_context().await?;
    let df = ctx.sql(&export_query).await?;
    
    Ok(df)
}
```

### 2.2 Pond TableProvider Integration
**Objective**: Ensure pond files are accessible as DataFusion tables

```rust
// Leverage existing QueryableFile trait from TLogFS
async fn register_pond_table(
    ctx: &SessionContext,
    tx_guard: &mut StewardTransactionGuard,
    pond_path: &str,
    table_name: &str,
) -> Result<()> {
    // Use existing pond infrastructure
    let file_node = tx_guard.root().lookup(pond_path).await?;
    let table_provider = get_table_provider_from_node(&file_node, tx_guard).await?;
    
    ctx.register_table(table_name, table_provider)?;
    Ok(())
}
```

## Phase 3: DataFusion Export Execution

### 3.1 FileSinkConfig Construction
**Objective**: Configure DataFusion for Hive-style partitioned writes

```rust
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::url::ListingTableUrl;
use datafusion_expr::dml::InsertOp;

async fn create_export_config(
    output_dir: &str,
    temporal_parts: &[String],
    df: &DataFrame,
    overwrite: bool,
    keep_partition_columns: bool,
) -> Result<FileSinkConfig> {
    let output_url = ListingTableUrl::parse(output_dir)?;
    
    // Configure partition columns (all as Int32 for temporal parts)
    let partition_cols = temporal_parts.iter()
        .map(|part| (part.clone(), DataType::Int32))
        .collect();
    
    let config = FileSinkConfig {
        original_url: output_dir.to_string(),
        object_store_url: output_url.object_store(),
        file_group: FileGroup::default(), // Will be populated during execution
        table_paths: vec![output_url],
        output_schema: df.schema(),
        table_partition_cols: partition_cols,
        insert_op: if overwrite { InsertOp::Overwrite } else { InsertOp::Append },
        keep_partition_by_columns: keep_partition_columns,
        file_extension: "parquet".to_string(),
    };
    
    Ok(config)
}
```

### 3.2 Export Execution Engine
**Objective**: Execute the partitioned write using DataFusion's system

```rust
async fn execute_export(
    df: DataFrame,
    config: FileSinkConfig,
    target: &ExportTarget,
) -> Result<ExportResult> {
    // Create local filesystem ObjectStore for export directory
    let object_store = create_local_object_store(&config.original_url)?;
    
    // Use DataFusion's write system
    let record_stream = df.execute_stream().await?;
    let task_context = Arc::new(TaskContext::default());
    
    // Start the demuxer for partitioned writing
    let (demux_task, file_stream_rx) = start_demuxer_task(&config, record_stream, &task_context);
    
    // Execute parallel writes
    let row_count = spawn_parquet_writers(
        &task_context,
        demux_task,
        file_stream_rx,
        object_store,
        &config,
    ).await?;
    
    Ok(ExportResult {
        target_name: target.output_name.clone(),
        row_count,
        files_written: vec![], // Will be populated by scanning output directory
    })
}

struct ExportResult {
    target_name: String,
    row_count: u64,
    files_written: Vec<ExportedFile>,
}

struct ExportedFile {
    path: PathBuf,
    start_time: Option<i64>,
    end_time: Option<i64>,
    row_count: Option<u64>,
}
```

## Phase 4: Metadata Extraction and Result Processing

### 4.1 Hive Directory Scanning
**Objective**: Extract temporal metadata from generated directory structure

```rust
// Similar to original list_recursive() + build_utc()
async fn scan_export_results(
    output_dir: &Path,
    target: &ExportTarget,
    temporal_parts: &[String],
) -> Result<Vec<ExportedFile>> {
    let mut files = Vec::new();
    
    // Recursively scan the Hive directory structure
    let parquet_files = find_parquet_files(output_dir)?;
    
    for file_path in parquet_files {
        // Parse Hive path: year=2024/month=01/day=15/file.parquet
        let partition_info = parse_hive_path(&file_path, temporal_parts)?;
        
        // Convert partition values to time bounds
        let (start_time, end_time) = compute_time_bounds(&partition_info, temporal_parts)?;
        
        // Get file row count (optional, from parquet metadata)
        let row_count = get_parquet_row_count(&file_path).await.ok();
        
        files.push(ExportedFile {
            path: file_path,
            start_time: Some(start_time),
            end_time: Some(end_time),
            row_count,
        });
    }
    
    // Sort by start time for consistent ordering
    files.sort_by(|a, b| a.start_time.cmp(&b.start_time));
    Ok(files)
}

fn parse_hive_path(
    file_path: &Path,
    temporal_parts: &[String],
) -> Result<HashMap<String, i32>> {
    let mut partition_values = HashMap::new();
    
    // Walk up the directory structure looking for key=value patterns
    let mut current = file_path.parent();
    
    while let Some(dir) = current {
        if let Some(name) = dir.file_name().and_then(|n| n.to_str()) {
            if let Some((key, value)) = name.split_once('=') {
                if temporal_parts.contains(&key.to_string()) {
                    partition_values.insert(key.to_string(), value.parse()?);
                }
            }
        }
        current = dir.parent();
    }
    
    Ok(partition_values)
}
```

### 4.2 Time Bounds Computation
**Objective**: Convert Hive partition values back to timestamp ranges

```rust
use chrono::{DateTime, Utc, TimeZone};

fn compute_time_bounds(
    partition_info: &HashMap<String, i32>,
    temporal_parts: &[String],
) -> Result<(i64, i64)> {
    // Default values for missing parts
    let year = partition_info.get("year").copied().unwrap_or(1970);
    let month = partition_info.get("month").copied().unwrap_or(1) as u32;
    let day = partition_info.get("day").copied().unwrap_or(1) as u32;
    let hour = partition_info.get("hour").copied().unwrap_or(0) as u32;
    let minute = partition_info.get("minute").copied().unwrap_or(0) as u32;
    let second = 0u32;
    
    // Create start time
    let start_dt = Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .ok_or_else(|| anyhow::anyhow!("Invalid date components"))?;
    
    // Calculate end time by incrementing the finest partition granularity
    let end_dt = if temporal_parts.contains(&"minute".to_string()) {
        start_dt + chrono::Duration::minutes(1)
    } else if temporal_parts.contains(&"hour".to_string()) {
        start_dt + chrono::Duration::hours(1)
    } else if temporal_parts.contains(&"day".to_string()) {
        start_dt + chrono::Duration::days(1)
    } else if temporal_parts.contains(&"month".to_string()) {
        // Handle month increment carefully
        if month == 12 {
            Utc.with_ymd_and_hms(year + 1, 1, day, hour, minute, second).single().unwrap()
        } else {
            Utc.with_ymd_and_hms(year, month + 1, day, hour, minute, second).single().unwrap()
        }
    } else {
        // Year increment
        Utc.with_ymd_and_hms(year + 1, month, day, hour, minute, second).single().unwrap()
    };
    
    Ok((start_dt.timestamp_millis(), end_dt.timestamp_millis()))
}
```

## Phase 5: Progress Reporting and Error Handling

### 5.1 Progress Reporting
**Objective**: Provide real-time feedback during export operations

```rust
use indicatif::{ProgressBar, ProgressStyle};

async fn export_with_progress(
    targets: Vec<ExportTarget>,
    // ... other params
) -> Result<Vec<ExportResult>> {
    let progress_bar = ProgressBar::new(targets.len() as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
    );
    
    let mut results = Vec::new();
    
    for (i, target) in targets.iter().enumerate() {
        progress_bar.set_message(format!("Exporting {}", target.output_name));
        
        let result = export_single_target(target, /* ... */).await?;
        results.push(result);
        
        progress_bar.set_position(i as u64 + 1);
    }
    
    progress_bar.finish_with_message("Export completed");
    Ok(results)
}
```

### 5.2 Error Handling and Cleanup
**Objective**: Robust error handling with partial cleanup

```rust
async fn export_with_cleanup(
    targets: Vec<ExportTarget>,
    output_dir: &str,
    overwrite: bool,
) -> Result<Vec<ExportResult>> {
    // Pre-flight checks
    validate_output_directory(output_dir, overwrite)?;
    
    let mut results = Vec::new();
    let mut partial_exports = Vec::new();
    
    for target in targets {
        match export_single_target(&target, /* ... */).await {
            Ok(result) => {
                results.push(result);
            }
            Err(e) => {
                eprintln!("Failed to export {}: {}", target.output_name, e);
                
                // Track partial exports for cleanup
                let target_dir = Path::new(output_dir).join(&target.output_name);
                if target_dir.exists() {
                    partial_exports.push(target_dir);
                }
                
                // Continue with other targets or fail fast based on user preference
                // For now, continue and report all errors at the end
            }
        }
    }
    
    // Cleanup partial exports if requested
    if !partial_exports.is_empty() {
        eprintln!("Cleaning up {} partial exports", partial_exports.len());
        for partial_dir in partial_exports {
            if let Err(e) = std::fs::remove_dir_all(&partial_dir) {
                eprintln!("Warning: Failed to clean up {}: {}", partial_dir.display(), e);
            }
        }
    }
    
    Ok(results)
}
```

## Phase 6: Integration and Testing

### 6.1 Command Integration
**File**: `crates/cmd/src/commands/mod.rs`

```rust
pub mod export;  // Add new module

// Update command handler in main.rs
Commands::Export { patterns, output_dir, temporal, filesystem, overwrite, keep_partition_columns } => {
    commands::export::export_command(
        patterns,
        output_dir,
        temporal,
        filesystem,
        overwrite,
        keep_partition_columns,
        &ship_ctx,
    ).await?;
}
```

### 6.2 Test Coverage
**File**: `crates/cmd/src/tests/export_tests.rs` (new file)

```rust
#[tokio::test]
async fn test_export_basic_partitioning() {
    // Test basic year/month/day partitioning
    let temp_pond = create_test_pond().await?;
    let temp_export = tempdir()?;
    
    // Create test data with timestamps
    create_test_series_data(&temp_pond, "sensors/temp.series", test_data_with_timestamps()).await?;
    
    // Run export
    let result = export_command(
        vec!["sensors/*.series".to_string()],
        temp_export.path().to_string_lossy().to_string(),
        "year,month,day".to_string(),
        FilesystemChoice::Data,
        true,
        false,
        &ship_ctx,
    ).await?;
    
    // Verify Hive directory structure
    assert!(temp_export.path().join("sensors/temp/year=2024/month=01/day=15").exists());
    
    // Verify parquet files
    let parquet_files = find_parquet_files(temp_export.path())?;
    assert!(!parquet_files.is_empty());
    
    // Verify file contents
    verify_exported_data(&parquet_files[0], expected_data).await?;
}

#[tokio::test]
async fn test_export_multiple_patterns() {
    // Test multiple pattern matching
}

#[tokio::test]
async fn test_export_fine_grained_partitioning() {
    // Test hour/minute partitioning
}

#[tokio::test]
async fn test_export_error_handling() {
    // Test partial failure scenarios
}
```

## Implementation Timeline

### Week 1: Foundation
- [ ] Add export command to CLI (`Commands` enum)
- [ ] Create basic command handler structure
- [ ] Implement pattern matching and target discovery
- [ ] Basic project structure and module organization

### Week 2: DataFusion Integration
- [ ] Query construction with temporal columns
- [ ] TableProvider integration with existing pond infrastructure
- [ ] Basic FileSinkConfig setup
- [ ] Simple export execution (single file, basic partitioning)

### Week 3: Partitioning and Parallel Writing
- [ ] Hive-style partitioning configuration
- [ ] Parallel write execution using DataFusion's demux system
- [ ] Multiple temporal granularity support (year through minute)
- [ ] Basic error handling

### Week 4: Metadata and Polish
- [ ] Export result scanning and metadata extraction
- [ ] Time bounds computation from Hive paths
- [ ] Progress reporting and user feedback
- [ ] Comprehensive error handling and cleanup

### Week 5: Testing and Documentation
- [ ] Comprehensive test suite
- [ ] Performance testing with large datasets
- [ ] Documentation updates
- [ ] Integration with existing pond workflows

## Success Criteria

### Functional Requirements
- ✅ Export pond data matching glob patterns
- ✅ Generate Hive-formatted directory structure (key=value/)
- ✅ Support multiple temporal granularities (year, month, day, hour, minute)
- ✅ Parallel writing for performance
- ✅ Proper error handling and partial cleanup
- ✅ Progress reporting for long-running exports

### Performance Requirements
- ✅ Handle large datasets (millions of rows) without memory issues
- ✅ Parallel writing should improve performance over sequential
- ✅ Memory usage should remain bounded during export

### Integration Requirements
- ✅ Work within steward transaction context
- ✅ Use existing pond query infrastructure (QueryableFile, TableProvider)
- ✅ Consistent with existing pond CLI patterns and error handling
- ✅ Export metadata should be extractable for further processing

## Future Enhancements

### Phase 7: Advanced Features (Future)
- **Custom Partitioning**: Support non-temporal partitioning columns
- **Compression Options**: Configurable parquet compression (gzip, snappy, lz4)
- **Schema Evolution**: Handle schema changes during export
- **Resume Support**: Resume interrupted exports
- **Cloud Storage**: Export directly to S3/GCS/Azure via ObjectStore
- **Export Scheduling**: Automated periodic exports
- **Delta Lake Output**: Export to Delta Lake format for better versioning integration

### Phase 8: Performance Optimization (Future)
- **Predicate Pushdown**: Push temporal filters to pond queries
- **Columnar Optimization**: Optimize column selection for export
- **Memory Pool Management**: Better memory management for large exports
- **Streaming Export**: Stream large datasets without full materialization

## Conclusion

This implementation plan provides a comprehensive roadmap for adding robust, DataFusion-based export functionality to DuckPond. The phased approach ensures steady progress while maintaining system stability and allowing for iterative testing and refinement.

The key innovation is leveraging DataFusion's native partitioning system instead of trying to replicate DuckDB's SQL approach, which provides better integration with pond's existing architecture and more flexible configuration options.