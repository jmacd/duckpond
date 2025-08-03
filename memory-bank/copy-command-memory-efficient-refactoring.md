# Copy Command Memory-Efficient Refactoring Plan

## Overview

The copy command currently has a memory efficiency problem when handling FileSeries files - it loads entire files into memory for temporal metadata extraction. This document outlines a multi-step refactoring to restore intelligent file type detection while maintaining strict memory efficiency.

## Current Problem Analysis

### Memory Violations in Current Code ❌

**In `copy_single_file_to_directory_with_name()` (lines 71-78):**
```rust
// For FileSeries, we need to read the file and store it properly with temporal metadata
let mut source_file = File::open(file_path).await
    .map_err(|e| format!("Failed to open source file: {}", e))?;
let mut file_content = Vec::new();
source_file.read_to_end(&mut file_content).await  // ⚠️ LOADS ENTIRE FILE INTO MEMORY
    .map_err(|e| format!("Failed to read source file: {}", e))?;
```

**In TLogFS `store_file_series_from_parquet()` (lines 427-440):**
```rust
let bytes = Bytes::from(content.to_vec());  // ⚠️ ENTIRE CONTENT IN MEMORY
let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
```

### Current Simplified Detection Logic

The copy command was previously simplified to avoid memory issues:

```rust
fn get_entry_type_for_file(source_path: &str, format: &str) -> tinyfs::EntryType {
    let entry_type = match format {
        "parquet" => tinyfs::EntryType::FileTable, // Force FileTable
        "series" => tinyfs::EntryType::FileSeries,  // Force FileSeries  
        _ => {
            // Default: detect based on file extension only
            if source_path.to_lowercase().ends_with(".parquet") {
                tinyfs::EntryType::FileTable
            } else {
                tinyfs::EntryType::FileData
            }
        }
    };
}
```

**Problems:**
- No intelligent detection based on Parquet content
- Requires manual `--format=series` specification
- Hard-coded timestamp column name ("timestamp")

## Desired Smart Detection Logic

### Automatic Entry Type Detection
- **Non-Parquet files** → `file:data`
- **Parquet files with case-insensitive "timestamp" column** → `file:series`
- **Other Parquet files** → `file:table`

### Manual Override Support
- `--format=data` → force `file:data`
- `--format=parquet` → force `file:table`
- `--format=series` → force `file:series`
- `--format=auto` (new default) → intelligent detection

## Proposed Solution Architecture

### Phase 1: Memory-Efficient Parquet Schema Analysis

**Goal:** Analyze Parquet schema without loading data content

```rust
// NEW FUNCTION: Memory-efficient schema analysis
async fn analyze_parquet_schema_for_entry_type(file_path: &str) -> Result<tinyfs::EntryType> {
    // Open file for seeking/reading metadata only
    let file = tokio::fs::File::open(file_path).await?;
    
    // Read only Parquet metadata (schema, not data)
    let metadata = read_parquet_metadata(file).await?;
    let schema = metadata.file_metadata().schema_descr();
    
    // Check for timestamp columns using existing detection logic
    if has_timestamp_column(schema) {
        Ok(tinyfs::EntryType::FileSeries)
    } else {
        Ok(tinyfs::EntryType::FileTable)
    }
}

fn has_timestamp_column(schema: &SchemaDescriptor) -> bool {
    // Check for case-insensitive "timestamp" column
    // Use same priority logic as tlogfs::schema::detect_timestamp_column()
    let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts", "datetime"];
    
    for field in schema.columns() {
        if candidates.contains(&field.name().as_str()) {
            return true;
        }
    }
    false
}
```

### Phase 2: Stream-First, Analyze-Later Architecture

**Key Insight:** Separate content streaming from metadata extraction

#### Current Flow (Memory-Intensive)
```
CLI → loads entire file → determines entry type → streams to TinyFS → TLogFS
```

#### Proposed Flow (Memory-Efficient)
```
CLI → analyzes schema only → determines entry type → streams to TinyFS → TLogFS seeks back for temporal metadata
```

### Phase 3: TLogFS Post-Write Temporal Extraction

**Location:** `OpLogFileWriter::poll_shutdown()` in TLogFS

```rust
// In OpLogFileWriter::poll_shutdown(), after content is stored:
match persistence.update_file_content_with_type(node_id, parent_node_id, &content, entry_type).await {
    Ok(_) => {
        // NEW: Post-write temporal metadata extraction for FileSeries
        if entry_type == EntryType::FileSeries {
            if let Ok(temporal_metadata) = extract_temporal_metadata_from_stored_content(
                &persistence, node_id, parent_node_id
            ).await {
                persistence.update_temporal_metadata(node_id, parent_node_id, temporal_metadata).await?;
            }
        }
        diagnostics::log_debug!("OpLogFileWriter::poll_shutdown() - successfully stored content");
    }
    Err(e) => { /* handle error */ }
}
```

## Implementation Steps

### Step 1: Enhanced get_entry_type_for_file() Function

```rust
async fn get_entry_type_for_file(source_path: &str, format: &str) -> Result<tinyfs::EntryType> {
    match format {
        "data" => Ok(tinyfs::EntryType::FileData),
        "parquet" => Ok(tinyfs::EntryType::FileTable), 
        "series" => Ok(tinyfs::EntryType::FileSeries),
        "auto" => {
            // Intelligent detection
            if source_path.to_lowercase().ends_with(".parquet") {
                // Analyze Parquet schema for timestamp columns
                analyze_parquet_schema_for_entry_type(source_path).await
            } else {
                Ok(tinyfs::EntryType::FileData)
            }
        }
        _ => {
            // Legacy support: default to "auto" behavior
            get_entry_type_for_file(source_path, "auto").await
        }
    }
}
```

### Step 2: Unified Streaming Copy Function

Remove the special FileSeries branch in `copy_single_file_to_directory_with_name()`:

```rust
async fn copy_single_file_to_directory_with_name(
    _ship: &steward::Ship,
    file_path: &str,
    dest_wd: &tinyfs::WD,
    filename: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Determine entry type with smart detection
    let entry_type = get_entry_type_for_file(file_path, format).await?;
    
    // UNIFIED STREAMING PATH: All file types use streaming
    let mut source_file = tokio::fs::File::open(file_path).await?;
    let mut dest_writer = dest_wd.async_writer_path_with_type(filename, entry_type).await?;
    
    // Stream copy with 64KB buffer for memory efficiency
    tokio::io::copy(&mut source_file, &mut dest_writer).await?;
    dest_writer.shutdown().await?; // TLogFS handles temporal metadata here
    
    Ok(())
}
```

### Step 3: TLogFS Temporal Metadata Post-Processing

Enhance `OpLogFileWriter::poll_shutdown()`:

```rust
// After successful content storage:
if entry_type == EntryType::FileSeries {
    // Extract temporal metadata from stored content (no additional memory loading)
    match extract_temporal_metadata_post_write(&persistence, node_id, parent_node_id).await {
        Ok((min_time, max_time, timestamp_col)) => {
            persistence.update_temporal_metadata(
                node_id, parent_node_id, min_time, max_time, &timestamp_col
            ).await?;
            diagnostics::log_debug!("✅ Temporal metadata extracted for FileSeries");
        }
        Err(e) => {
            diagnostics::log_warning!("⚠️ Failed to extract temporal metadata: {}", e);
            // Continue - file is still valid, just without optimized temporal queries
        }
    }
}
```

### Step 4: CLI Default Format Update

Update `main.rs` to default to auto-detection:

```rust
#[derive(Parser)]
struct CopyArgs {
    /// EXPERIMENTAL: Format handling [default: auto] [possible values: auto, data, parquet, series]
    #[arg(long, default_value = "auto")]
    format: String,
}
```

## Benefits of This Approach

### Memory Efficiency ✅
- **No full file loading** at any layer
- **Schema-only analysis** for format detection  
- **Streaming throughout** the entire pipeline
- **Seekable post-analysis** only for temporal metadata

### Functional Completeness ✅
- **Automatic format detection** based on Parquet schema
- **Manual override capability** for all entry types
- **Temporal metadata extraction** for FileSeries
- **Auto-detected timestamp columns** using existing priority logic

### Clean Architecture ✅
- **CLI remains simple** - just streams files
- **TinyFS handles routing** based on entry type
- **TLogFS handles complexity** of temporal metadata
- **Clear separation of concerns** between layers

### Backward Compatibility ✅
- **Existing `--format` flags** continue to work
- **Legacy behavior preserved** when explicit format specified
- **Enhanced default behavior** with auto-detection

## Error Handling Strategy

### Graceful Degradation
- **Invalid Parquet files** → fall back to `file:data`
- **Schema analysis failures** → fall back to extension-based detection
- **Temporal extraction failures** → store as FileSeries without temporal optimization
- **Unknown formats** → default to auto-detection

### Comprehensive Logging
- **Format detection decisions** logged at debug level
- **Temporal metadata extraction** results logged
- **Fallback scenarios** clearly documented in logs

## Migration Path

### Phase 1: TLogFS Enhancement
1. Implement post-write temporal metadata extraction
2. Add seekable content analysis functions
3. Test with existing FileSeries files

### Phase 2: CLI Schema Analysis  
1. Add Parquet metadata reading capability
2. Implement intelligent format detection
3. Update CLI default to "auto"

### Phase 3: Remove Legacy Code
1. Remove `copy_file_series_with_temporal_metadata()` function
2. Simplify copy logic to unified streaming
3. Clean up memory-intensive code paths

### Phase 4: Validation
1. Test with large Parquet files
2. Verify memory usage stays bounded
3. Confirm temporal metadata accuracy

## Success Criteria

### Memory Efficiency
- ✅ Memory usage remains constant regardless of file size
- ✅ No `read_to_end()` or similar memory-loading operations
- ✅ Streaming architecture maintained throughout

### Functional Requirements  
- ✅ Automatic detection of FileSeries from Parquet schema
- ✅ Correct temporal metadata extraction for time-series data
- ✅ Manual override capability for all scenarios
- ✅ Backward compatibility with existing workflows

### Performance
- ✅ Schema analysis adds minimal overhead
- ✅ Streaming performance maintained
- ✅ No regression in copy command speed

This refactoring will restore the intelligent file type detection capabilities while ensuring the copy command never loads large files into memory, maintaining the streaming architecture throughout the entire pipeline.
