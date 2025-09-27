# DuckPond Multi-Stage Export Pipeline Implementation Plan

## Overview

This document outlines the implementation plan for extending DuckPond's export functionality with a multi-stage pipeline that supports metadata carry-forward and template-based derivation.

## Current Status

✅ **Stage 1 Complete**: Basic Parquet export with Hive-style partitioning implemented  
🔄 **Next**: Implement template dynamic factory system

## Architecture Understanding

### Template Dynamic Factory Pattern

Based on analysis of the original `template.rs`, the template system works as a **dynamic directory factory** (like `temporal_reduce`):

1. **Template Collection** → **Dynamic Directory** (one per collection)
2. **Pattern Matches** → **Dynamic Files** (generated via pattern expansion)
3. **Template Rendering** → **File Content** (Tera-rendered text)

### Key Data Structures

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSpec {
    pub collections: Vec<TemplateCollection>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateCollection {
    pub name: String,
    pub in_pattern: String,      // Glob pattern matching exported files
    pub out_pattern: String,     // Output filename with $0, $1 placeholders  
    pub template: Option<String>,
    pub template_file: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportOutput {
    pub file: PathBuf,           // Relative path to exported file
    pub start_time: Option<i64>, // Unix timestamp (milliseconds)
    pub end_time: Option<i64>,   // Unix timestamp (milliseconds)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ExportSet {
    Empty,
    Files(Vec<ExportOutput>),                    // Leaf: list of files
    Map(HashMap<String, Box<ExportSet>>),        // Branch: nested structure
}
```

## Implementation Plan

### Task 1: Enable Tera Dependency
- Uncomment `tera = "1"` in root `Cargo.toml`

### Task 2: Create Template Dynamic Factory
**File**: `crates/tlogfs/src/template_factory.rs`
- Follow `temporal_reduce.rs` pattern
- Dynamic directory per template collection  
- Dynamic files generated via pattern matching
- Files contain Tera-rendered content

### Task 3: Template Processing Logic
**Key Functions**:
- `glob_placeholder()` - $N placeholder substitution
- Pattern matching against export metadata
- Tera context setup with export data + user variables
- Custom Tera functions (group, timeformat)

### Task 4: Export Integration
**File**: `crates/cmd/src/commands/export.rs`  
- Template argument processing
- Export context management with metadata
- Integration with dynamic factory system

### Task 5: Testing
- Single end-to-end test: Create FileSeries with different paths, add template directory via `mknod`, export timeseries (Stage 1), export templates (Stage 2), verify template output contains expected string contents

## Template Context Structure

Templates receive context matching original exactly:
- `export` - Hierarchical ExportSet with exported file metadata
- `args` - Pattern capture groups ($0, $1, etc.)
- `schema` - Parquet schema information  
- User variables from `-v key=value`

## Success Criteria

- ✅ Dynamic factory creates template directories
- ✅ Pattern expansion generates correct filenames  
- ✅ Tera rendering produces expected content
- ✅ Export pipeline integrates seamlessly
- ✅ CLI variables work in templates