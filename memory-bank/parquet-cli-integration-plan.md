# Parquet CLI Integration Plan (Experimental)

## Overview

This document outlines a **minimal, experimental** plan to add basic Parquet support to the DuckPond CLI. This is an optional feature designed to exercise the existing Arrow Parquet integration with simple CLI enhancements. **The feature may be removed later** if not needed.

## Background

DuckPond has successfully implemented comprehensive Arrow Parquet integration with:
- **ParquetExt trait**: High-level `Vec<T>` operations where `T: Serialize + Deserialize + ForArrow`
- **SimpleParquetExt trait**: Low-level `RecordBatch` operations
- **Entry type integration**: Proper `EntryType::FileTable` specification
- **Memory efficient batching**: Automatic 1000-row batching for large datasets
- **Full test coverage**: 9 passing tests including roundtrip, large datasets, and binary data preservation

## Goals (Minimal Scope)

1. **Basic Parquet detection**: Automatically recognize `.parquet` files and set correct entry type
2. **Simple CSV conversion**: Basic CSV → Parquet conversion to exercise write path
3. **Optional table display**: Pretty-print Parquet files when requested
4. **Minimal code footprint**: Keep changes small and easily removable
5. **No breaking changes**: All existing functionality must remain unchanged

## Implementation Plan (Minimal Changes)

### 1. Copy Command: Add Simple Format Flag

#### 1.1 Minimal CLI Changes
```rust
Copy {
    #[arg(required = true)]
    sources: Vec<String>,
    dest: String,
    /// EXPERIMENTAL: Format handling [default: auto] [possible values: auto, parquet]  
    #[arg(long, default_value = "auto")]
    format: Option<String>,
},
```

**Note**: Using `Option<String>` instead of enum to keep changes minimal and easily removable.

#### 1.2 Simple Detection Logic (Single Function)
```rust
// Add this single function to copy.rs - easy to remove later
fn should_convert_to_parquet(source_path: &str, format: &str) -> bool {
    match format {
        "auto" => source_path.to_lowercase().ends_with(".parquet"),
        "parquet" => source_path.to_lowercase().ends_with(".csv"),
        _ => false
    }
}
```

### 2. Cat Command: Add Simple Display Flag

#### 2.1 Minimal CLI Changes  
```rust
Cat {
    path: String,
    #[arg(long, short = 'f', default_value = "data")]
    filesystem: FilesystemChoice,
    /// EXPERIMENTAL: Display mode [default: raw] [possible values: raw, table]
    #[arg(long, default_value = "raw")]
    display: Option<String>,
},
```

### 3. Minimal Conversion Logic

#### 3.1 Single Conversion Function (Easy to Remove)
```rust
// Add to copy.rs as a separate, self-contained function
async fn try_convert_csv_to_parquet(source_path: &str) -> Result<Vec<u8>> {
    use arrow_csv::ReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;
    
    let file = std::fs::File::open(source_path)?;
    let mut csv_reader = ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(100))  // Just 100 rows for simplicity
        .build(file)?;
    
    let batch = csv_reader.next().transpose()?.ok_or("Empty CSV")?;
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(Cursor::new(&mut buffer), batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(buffer)
}
```

### 4. Implementation Strategy (Easy Removal)

#### 4.1 Code Organization for Easy Removal
- **All new code in clearly marked sections** with `// EXPERIMENTAL PARQUET` comments
- **No changes to core logic** - only additive changes
- **Self-contained functions** that can be deleted without affecting other code
- **Optional CLI flags only** - no required parameters

#### 4.2 Minimal Error Handling
```rust
// Simple error messages, no complex handling
if format == "parquet" && !source.ends_with(".csv") {
    return Err(anyhow!("EXPERIMENTAL: Only .csv files supported for --format=parquet"));
}
```

### 5. Testing Strategy (Minimal)

#### 5.1 Basic Tests Only
- **Parquet detection**: Verify `.parquet` files get `FileTable` entry type
- **CSV conversion**: Basic CSV → Parquet conversion works
- **Error cases**: Unsupported files fail gracefully
- **No regressions**: Existing functionality unchanged

### 6. Single Implementation Phase

#### Single Phase: Minimal Feature Addition
1. Add optional `--format` flag to copy command
2. Add optional `--display` flag to cat command  
3. Add single conversion function for CSV → Parquet
4. Add basic tests
5. Mark all code as EXPERIMENTAL for easy removal

## Dependencies

### Required Crates
- `arrow-csv`: For CSV to Arrow conversion (already available in ./arrow-rs/)
- `arrow-cast`: For pretty printing (already available, feature `prettyprint`)
- `clap`: For CLI argument parsing (already integrated)

### TinyFS Integration Points
- **ParquetExt trait**: Use existing high-level Parquet operations
- **Entry type system**: Leverage `EntryType::FileTable` vs `EntryType::FileData`
- **AsyncWrite streams**: Use existing TinyFS streaming architecture

## Design Decisions (Keeping It Simple)

Based on user feedback, this implementation prioritizes simplicity and easy removal:

1. **Minimal scope**: Only basic Parquet detection and CSV conversion
2. **Optional feature**: Can be completely removed without affecting core functionality  
3. **No complex logic**: Use simple file extension detection and basic conversion
4. **Leverage existing code**: Rely entirely on existing Arrow Parquet integration
5. **Small code footprint**: Keep all changes in isolated, easily removable sections

## Success Criteria (Minimal)

1. **Basic Parquet detection works** - `.parquet` files get `FileTable` entry type
2. **Simple CSV conversion works** - `--format=parquet` converts basic CSV files
3. **Table display works** - `--display=table` shows Parquet as ASCII tables
4. **Zero regressions** - all existing functionality unchanged
5. **Easy to remove** - all code clearly marked and self-contained

**Note**: This is an experimental feature that may be removed if not useful.
