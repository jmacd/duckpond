# DuckPond Multi-Stage Export Pipeline Implementation Plan

## Overview

This document outlines the implementation plan for extending DuckPond's export functionality with a multi-stage pipeline that supports metadata carry-forward and template-based derivation, matching the original implementation's capabilities.

## Current Status

✅ **Stage 1 Complete**: Basic Parquet export with Hive-style partitioning implemented
🔄 **Next**: Implement multi-pattern export with metadata collection and template derivation system

## Key Insights from Original System Analysis

### Original Export Command Pattern
```bash
# From README.md documentation
duckpond export -d OUTPUT_DIR -p PATTERN [-p PATTERN ...] --temporal=year,month

# From caspar/run.sh example (two-stage export)
cargo run export \
      -d "./data" \
      -p "/Reduce/${REDUCE}/caspar_water/param=*/*"    # Stage 1: Data export
      -p "/Template/${TMPL}/caspar_water/*"            # Stage 2: Template export
      --temporal year,month
```

### Multi-Pattern Export Flow
1. **Pattern 1**: Export data files → Generate partitioned Parquet files + collect metadata
2. **Pattern 2**: Export templates → Process templates with metadata context → Generate static content
3. **Export Context**: All patterns contribute to hierarchical `export` object in template context

### Template Configuration Discovery
- **Template specs stored as pond resources**: `pond apply` processes YAML configs
- **Template files resolved relative to config**: `template_file: "data.md.tmpl"` 
- **Template context structure**: Exactly matches original `ExportSet` with nested maps

## Architecture Understanding

Based on analysis of the original implementation, the export pipeline works as follows:

### Stage 1: Data Export (✅ Completed)
- Export pond data to partitioned Parquet files
- Collect file metadata (paths, time ranges) into `ExportOutput` structures
- Organize metadata into hierarchical `ExportSet` structures based on glob capture groups
- Store metadata in pond's `expctx` (export context) for subsequent stages

### Stage 2: Template Processing (🔄 To Implement)  
- Use collected export metadata as Tera template context
- Process template files that reference exported data
- Generate derived outputs (HTML, JSON, etc.) for static sites
- Support complex template logic with schema introspection

## Data Structures (From Original)

### ExportOutput
```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ExportOutput {
    file: PathBuf,           // Relative path to exported file
    start_time: Option<i64>, // Unix timestamp (milliseconds)
    end_time: Option<i64>,   // Unix timestamp (milliseconds)
}
```

### ExportSet (Hierarchical Metadata)
```rust
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ExportSet {
    Empty(),
    Files(Vec<ExportOutput>),                    // Leaf: list of files
    Map(HashMap<String, Box<ExportSet>>),        // Branch: nested structure
}
```

### Template Integration
```rust
// In pond context:
pond.expctx.insert("export", &export_set);

// In templates:
{%- for k2, v2 in export[args[0]] %}
  "{{ k2 }}": [
  {%- for v3 in v2 %}
    {
      "start_time": {{ v3.start_time }},
      "end_time": {{ v3.end_time }},
      "file": FileAttachment("{{ v3.file }}"),
    },
  {%- endfor %}
  ],
{%- endfor %}
```

## CLI Usage and Template Variable Expansion

### Command Line Interface

The DuckPond export command supports the same CLI interface as the original implementation:

```bash
pond export -d OUTPUT_DIR -p PATTERN [-p PATTERN ...] [--temporal=LEVELS] [-v KEY=VALUE ...]
```

### Template Variable Expansion with `-v key=value`

**Basic Usage**: Set template variables that can be referenced in templates
```bash
# Single variable
pond export -d /tmp/site -p "/templates/*" -v site=BDock

# Multiple variables  
pond export -d /tmp/site -p "/templates/*" -v site=BDock -v res=1d -v year=2023
```

**In Templates**: Variables become available in the template context
```tera
<h1>{{ site }} Water Quality Data</h1>
<p>Resolution: {{ res }}</p>
<p>Year: {{ year }}</p>
```

### Multi-Pattern Export with Variables

**Real-World Example** (from original `caspar/run.sh`):
```bash
# Stage 1: Export data with variables
pond export \
  -d "./output" \
  -p "/Reduce/res=1d/caspar_water/param=*/*" \
  -p "/Template/site_summary/caspar_water/*" \
  --temporal year,month \
  -v site=BDock \
  -v res=1d \
  -v project=CasparWater
```

**Template Context Structure**: Variables combine with export metadata
```json
{
  "site": "BDock",
  "res": "1d", 
  "project": "CasparWater",
  "export": {
    "param=temperature": [
      {
        "start_time": 1672531200,
        "end_time": 1672617600,
        "file": "output/year=2023/month=01/param=temperature/data.parquet"
      }
    ]
  }
}
```

### Template Variable Resolution Priority

1. **Command Line Variables** (`-v key=value`): Highest priority
2. **Pattern Capture Groups** (`$0`, `$1`, etc.): Medium priority  
3. **Export Metadata** (`export` object): Always available
4. **Schema Information** (`schema` object): Auto-generated

### Advanced Variable Usage

**Complex Template Logic**:
```tera
{% if site == "BDock" %}
  <h2>Bodega Dock Station</h2>
{% elif site == "Henderson" %}
  <h2>Henderson Creek Station</h2>  
{% endif %}

<table>
{% for param, files in export %}
  <tr>
    <td>{{ param }}</td>
    <td>{{ files|length }} files</td>
    <td>{{ res }} resolution</td>
  </tr>
{% endfor %}
</table>
```

**Conditional Exports**:
```bash
# Export different templates based on site
pond export -d /tmp/bdock -p "/templates/station/*" -v site=BDock -v type=pier
pond export -d /tmp/henderson -p "/templates/station/*" -v site=Henderson -v type=creek
```

### Error Handling

**Invalid Key-Value Format**:
```bash
pond export -v invalid_format  # ERROR: no '=' found
pond export -v =empty_key       # ERROR: empty key  
pond export -v empty_value=     # OK: empty string value
```

**Variable Name Conflicts**: Command line variables override pattern captures
```bash
# Pattern captures $0=temperature, but -v overrides it
pond export -p "/data/param=*" -v "0=pressure"  # Template gets "0"="pressure"
```

## Implementation Plan

## Phase 1: Export Metadata Collection

### 1.1 Update Export Command Structure
**File**: `crates/cmd/src/commands/export.rs`

**Current Status**: Basic export implemented, needs metadata collection

**Required Changes**:
```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportOutput {
    pub file: PathBuf,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ExportSet {
    Empty,
    Files(Vec<ExportOutput>),
    Map(HashMap<String, Box<ExportSet>>),
}

impl ExportSet {
    pub fn construct(inputs: Vec<(Vec<String>, ExportOutput)>) -> Self {
        let mut eset = ExportSet::Empty;
        for (captures, output) in inputs {
            eset.insert(&captures, output);
        }
        eset
    }
    
    fn insert(&mut self, captures: &[String], output: ExportOutput) {
        // Implementation from original: build hierarchical structure
        // based on glob capture groups
    }
}
```

### 1.2 Integrate Pattern Capture Groups
**Objective**: Extract capture groups from glob patterns for hierarchical organization

**Implementation**:
```rust
// In export_single_target function:
pub async fn export_single_target(
    target: &ExportTarget,
    pattern: &str,              // Add original pattern
    temporal_columns: &[String],
    output_dir: &Path,
    tx_guard: &mut StewardTransactionGuard,
) -> Result<Vec<(Vec<String>, ExportOutput)>> {
    
    // Parse pattern and extract capture groups
    let (path, glob) = parse_pattern(pattern)?;
    let captures = extract_captures(&glob, &target.pond_path)?;
    
    // ... existing export logic ...
    
    // Return metadata with capture groups
    let metadata = exported_files.into_iter()
        .map(|file_info| (captures.clone(), ExportOutput {
            file: file_info.relative_path,
            start_time: file_info.start_time,
            end_time: file_info.end_time,
        }))
        .collect();
        
    Ok(metadata)
}
```

### 1.3 Export Context Management
**Objective**: Store export metadata for template processing

**New Structure**:
```rust
// In steward or new export context manager
pub struct ExportContext {
    pub metadata: HashMap<String, ExportSet>,
}

impl ExportContext {
    pub fn add_export_results(&mut self, pattern: &str, results: Vec<(Vec<String>, ExportOutput)>) {
        let export_set = ExportSet::construct(results);
        self.metadata.insert(pattern, export_set);
    }
    
    pub fn to_tera_context(&self) -> tera::Context {
        let mut ctx = tera::Context::new();
        ctx.insert("export", &self.metadata);
        ctx
    }
}
```

## Phase 2: Template System Integration

### 2.1 Add Tera Dependency
**File**: `Cargo.toml` (workspace level)

```toml
[workspace.dependencies]
tera = "1.20"
```

**File**: `crates/cmd/Cargo.toml`
```toml
[dependencies]
tera = { workspace = true }
```

### 2.2 Template Factory System
**File**: `crates/tlogfs/src/template_factory.rs` (new)

**Objective**: Create dynamic factory for template-derived files

```rust
use tera::{Tera, Context};
use crate::factory::{FileFactory, FactoryError};
use crate::oplog_file::OpLogFile;
use std::collections::HashMap;

pub struct TemplateFactory {
    templates: HashMap<String, String>,  // template_name -> template_content
    tera_engine: Tera,
}

impl TemplateFactory {
    pub fn new() -> Self {
        Self {
            templates: HashMap::new(),
            tera_engine: Tera::default(),
        }
    }
    
    pub fn add_template(&mut self, name: String, content: String) -> Result<(), FactoryError> {
        self.tera_engine.add_raw_template(&name, &content)?;
        self.templates.insert(name.clone(), content);
        Ok(())
    }
}

impl FileFactory for TemplateFactory {
    fn create_file(
        &self,
        path: &str,
        context: &Context,
    ) -> Result<Box<dyn OpLogFile>, FactoryError> {
        // Extract template name from path
        let template_name = extract_template_name(path)?;
        
        // Render template with context
        let rendered = self.tera_engine.render(&template_name, context)?;
        
        // Create synthetic file with rendered content
        Ok(Box::new(SyntheticTextFile::new(rendered)))
    }
}

// Synthetic text file implementation
pub struct SyntheticTextFile {
    content: String,
}

impl SyntheticTextFile {
    pub fn new(content: String) -> Self {
        Self { content }
    }
}

impl OpLogFile for SyntheticTextFile {
    // Implement required methods to expose text content as queryable data
    fn as_any(&self) -> &dyn std::any::Any { self }
    
    async fn as_table_provider(&self, ...) -> Result<Arc<dyn TableProvider>> {
        // Create table provider that exposes the text content
        // Could be CSV parsing, JSON parsing, or raw text depending on format
    }
}
```

### 2.3 Template Configuration System
**File**: `crates/cmd/src/commands/template.rs` (new)

**Objective**: Handle template specification and configuration

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSpec {
    pub collections: Vec<TemplateCollection>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateCollection {
    pub name: String,
    pub in_pattern: String,      // Pattern matching exported files
    pub out_pattern: String,     // Output filename with $0, $1 placeholders  
    pub template: Option<String>,
    pub template_file: Option<String>,
}

pub async fn process_templates(
    template_specs: Vec<TemplateSpec>,
    export_context: &ExportContext,
    output_dir: &Path,
) -> Result<()> {
    let mut tera = Tera::default();
    let context = export_context.to_tera_context();
    
    for spec in template_specs {
        for collection in spec.collections {
            // Load template content
            let template_content = match (collection.template, collection.template_file) {
                (Some(content), None) => content,
                (None, Some(file)) => std::fs::read_to_string(&file)?,
                _ => return Err(anyhow!("Must specify either template or template_file")),
            };
            
            // Add to Tera engine
            tera.add_raw_template(&collection.name, &template_content)?;
            
            // Process pattern matches and generate outputs
            process_template_collection(&mut tera, &collection, &context, output_dir).await?;
        }
    }
    
    Ok(())
}

async fn process_template_collection(
    tera: &mut Tera,
    collection: &TemplateCollection,
    context: &Context,
    output_dir: &Path,
) -> Result<()> {
    // Match in_pattern against export metadata
    let matches = find_export_matches(&collection.in_pattern, context)?;
    
    for matched in matches {
        // Extract capture groups and build output filename
        let output_path = build_output_path(&collection.out_pattern, &matched.captures)?;
        let full_output = output_dir.join(output_path);
        
        // Create context with matched data
        let mut template_context = context.clone();
        template_context.insert("args", &matched.captures);
        
        // Add schema information if available
        if let Some(schema) = extract_schema_info(&matched.export_data)? {
            template_context.insert("schema", &schema);
        }
        
        // Render template
        let rendered = tera.render(&collection.name, &template_context)?;
        
        // Write output file
        std::fs::create_dir_all(full_output.parent().unwrap())?;
        std::fs::write(&full_output, rendered)?;
        
        println!("📄 Generated {} from template {}", full_output.display(), collection.name);
    }
    
    Ok(())
}
```

## Phase 3: CLI Integration (Updated for Original Compatibility)

### 3.1 Export Command Structure - EXACT Original Match
**File**: `crates/cmd/src/main.rs`

**Updated Command Structure** (matches original exactly):
```rust
/// Export pond data to external Parquet files with time partitioning  
Export {
    /// File patterns to export (e.g., "/sensors/*.series")
    #[arg(short, long)]
    pattern: Vec<String>,              // ORIGINAL: -p/--pattern (multiple)
    /// Output directory for exported files  
    #[arg(short, long)]
    dir: PathBuf,                      // ORIGINAL: -d/--dir
    /// Temporal partitioning levels (comma-separated: year,month,day,hour,minute)
    #[arg(long, default_value = "")]
    temporal: String,                  // ORIGINAL: --temporal (empty default)
    /// Template variables as key=value pairs (e.g., site=BDock res=1d)
    #[arg(short, value_parser = parse_key_val, number_of_values = 1)]
    vars: Vec<(String, String)>,       // ORIGINAL: -v key=value support
    /// Which filesystem to export from (DuckPond extension)
    #[arg(long, short = 'f', default_value = "data")]
    filesystem: FilesystemChoice,      // NEW: filesystem selection
    /// Overwrite existing files (DuckPond extension)
    #[arg(long)]
    overwrite: bool,                   // NEW: overwrite control
},

/// Parse a single key-value pair (from original)
fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s.find('=')
        .ok_or_else(|| anyhow!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
```

### 3.2 Multi-Pattern Export Orchestration (Original Style)
**File**: `crates/cmd/src/commands/export.rs`

**Enhanced Export Function** (processes multiple patterns like original):
```rust
pub async fn export_command(
    patterns: Vec<String>,              // Multiple -p patterns
    dir: PathBuf,                       // -d directory  
    temporal: String,                   // --temporal
    vars: Vec<(String, String)>,        // -v key=value pairs
    filesystem: FilesystemChoice,
    overwrite: bool,
    ship_ctx: &ShipContext,
) -> Result<()> {
    std::fs::create_dir_all(&dir)?;
    
    let temporal_columns = if temporal.is_empty() {
        vec![]
    } else {
        parse_temporal_columns(&temporal)
    };
    
    let mut export_context = ExportContext::new();
    
    // Process all patterns and collect metadata (like original)
    println!("🗂️  Processing {} export patterns...", patterns.len());
    
    let mut tx_guard = ship_ctx.steward.begin_transaction().await?;
    
    for pattern in &patterns {
        println!("📂 Exporting pattern: {}", pattern);
        
        let targets = discover_export_targets(vec![pattern.clone()], &mut tx_guard, filesystem).await?;
        
        for target in targets {
            // Determine if this is a data file or template file based on pattern
            if is_template_pattern(pattern) {
                // Process template files (like original /Template/${TMPL}/* patterns)
                process_template_target(&target, pattern, &vars, &dir, &export_context).await?;
            } else {
                // Process data files (like original /Reduce/${REDUCE}/* patterns)  
                let metadata = export_single_target(
                    &target,
                    pattern,
                    &temporal_columns,
                    &dir,
                    &mut tx_guard,
                ).await?;
                
                export_context.add_export_results(pattern, metadata);
            }
        }
    }
    
    tx_guard.commit(None).await?;
    
    println!("✅ Export completed successfully!");
    println!("� Files exported to: {}", dir.display());
    
    Ok(())
}

// Helper to detect template patterns vs data patterns
fn is_template_pattern(pattern: &str) -> bool {
    pattern.contains("/Template/") || pattern.ends_with(".tmpl") || pattern.ends_with(".md")
}
```

## Phase 4: Testing and Validation

### 4.1 Test Template Configuration
**File**: `test-export-template.yaml` (new)

```yaml
apiVersion: github.com/jmacd/duckpond/v1
kind: Template
name: testsite
desc: Test template for export pipeline
spec:
  collections:
  - name: data_overview
    in_pattern: "/test-locations/*/res=1d"
    out_pattern: "$0.md" 
    template: |
      ---
      title: "Data Overview: {{ args[0] }}"  
      ---
      
      # Exported Data Files
      
      {% for k2, v2 in export[args[0]] %}
      ## Resolution: {{ k2 }}
      
      Files: {{ v2 | length }}
      
      {% for v3 in v2 %}
      - **{{ v3.file }}**
        - Start: {{ v3.start_time }}
        - End: {{ v3.end_time }}
      {% endfor %}
      
      {% endfor %}
  
  - name: time_series_viz
    in_pattern: "/test-locations/*/res=1d"
    out_pattern: "$0-viz.html"
    template: |
      <!DOCTYPE html>
      <html>
      <head><title>{{ args[0] }} Time Series</title></head>
      <body>
        <h1>{{ args[0] }} Data Visualization</h1>
        
        <script>
        const dataFiles = {
        {% for k2, v2 in export[args[0]] %}
          "{{ k2 }}": [
          {% for v3 in v2 %}
            {
              start_time: {{ v3.start_time }},
              end_time: {{ v3.end_time }},
              file: "{{ v3.file }}"
            },
          {% endfor %}
          ],
        {% endfor %}
        };
        
        console.log("Data files:", dataFiles);
        </script>
      </body>
      </html>
```

### 4.2 Integration Test
**File**: `test-export-pipeline.sh` (new)

```bash
#!/bin/bash
set -e

# Setup test environment
export POND=/tmp/template-pond
rm -rf $POND /tmp/template-export
cargo run --bin pond init

# Create test data (reuse existing dynamic test data)
./test-hydrovu-dynamic-dir.sh

# Stage 1: Export with metadata collection
echo "🗂️  Testing Stage 1: Data Export"
cargo run --bin pond export \
  "/test-locations/BDockDownsampled/res=1d.series" \
  --output-dir /tmp/template-export \
  --temporal "year,month,day" \
  --template-spec test-export-template.yaml

# Verify Stage 1 outputs
echo "📊 Verifying exported Parquet files..."
find /tmp/template-export -name "*.parquet" | wc -l

# Verify Stage 2 outputs  
echo "📝 Verifying template-generated files..."
ls -la /tmp/template-export/*.md /tmp/template-export/*.html

# Test template content
echo "🔍 Checking generated markdown content..."
head -20 /tmp/template-export/BDockDownsampled.md

echo "✅ Multi-stage export pipeline test completed!"
```

## Phase 5: Advanced Features

### 5.1 Schema Introspection for Templates
**Objective**: Provide Parquet schema information to templates

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub fields: Vec<FieldInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldInfo {
    pub name: String,
    pub instrument: String,
    pub unit: String,
    pub agg: String,
}

fn extract_schema_info(export_outputs: &[ExportOutput]) -> Result<Option<SchemaInfo>> {
    if let Some(first_file) = export_outputs.first() {
        // Read Parquet schema
        let file = File::open(&first_file.file)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();
        
        // Parse field names (instrument.parameter.unit.aggregation format)
        let mut fields = Vec::new();
        for field in schema.fields() {
            if matches!(field.name().to_lowercase().as_str(), "timestamp" | "year" | "month" | "day") {
                continue;
            }
            
            let parts: Vec<&str> = field.name().split('.').collect();
            if parts.len() >= 4 {
                fields.push(FieldInfo {
                    agg: parts.last().unwrap().to_string(),
                    unit: parts[parts.len()-2].to_string(),
                    name: parts[parts.len()-3].to_string(),
                    instrument: parts[0..parts.len()-3].join("."),
                });
            }
        }
        
        Ok(Some(SchemaInfo { fields }))
    } else {
        Ok(None)
    }
}
```

### 5.2 Custom Tera Functions
**Objective**: Add helper functions for template processing

```rust
fn register_tera_functions(tera: &mut Tera) {
    tera.register_function("group", group_function);
    tera.register_function("timeformat", timeformat_function);
}

fn group_function(args: &HashMap<String, Value>) -> tera::Result<Value> {
    let by = args.get("by").ok_or_else(|| tera::Error::msg("missing 'by' parameter"))?;
    let input = args.get("in").ok_or_else(|| tera::Error::msg("missing 'in' parameter"))?;
    
    let by_field = by.as_str().ok_or_else(|| tera::Error::msg("'by' must be string"))?;
    let items: Vec<Value> = tera::from_value(input.clone())?;
    
    // Group items by specified field
    let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    
    for item in items {
        if let Some(group_key) = item.get(by_field).and_then(|v| v.as_str()) {
            groups.entry(group_key.to_string()).or_default().push(item);
        }
    }
    
    Ok(tera::to_value(groups)?)
}

fn timeformat_function(args: &HashMap<String, Value>) -> tera::Result<Value> {
    let timestamp = args.get("timestamp").and_then(|v| v.as_i64())
        .ok_or_else(|| tera::Error::msg("missing or invalid timestamp"))?;
    
    let format = args.get("format").and_then(|v| v.as_str())
        .unwrap_or("%Y-%m-%d %H:%M:%S");
    
    let dt = chrono::DateTime::from_timestamp_millis(timestamp)
        .ok_or_else(|| tera::Error::msg("invalid timestamp"))?;
    
    Ok(tera::to_value(dt.format(format).to_string())?)
}
```

## Success Criteria

### Stage 1 (Data Export) ✅
- Export pond data to partitioned Parquet files
- Collect file metadata with time ranges
- Organize metadata hierarchically based on glob captures
- Preserve all metadata for template processing

### Stage 2 (Template Processing) 🔄
- Load and parse YAML template specifications
- Process Tera templates with export metadata context
- Generate derived outputs (HTML, Markdown, JSON, etc.)
- Support complex template logic and helper functions
- Handle multiple template collections per specification

### Integration Requirements 🔄
- Seamless pipeline from export to template processing
- Backward compatibility with existing export functionality
- Comprehensive error handling and validation
- Performance suitable for large export datasets

### Testing and Documentation 🔄
- End-to-end pipeline testing with real temporal data
- Template validation and error reporting
- User documentation and examples
- Integration with existing pond CLI patterns

## Future Enhancements

### Advanced Template Features
- **Conditional Processing**: Template processing based on data characteristics
- **Multi-format Support**: Generate multiple output formats from single template
- **Template Inheritance**: Base templates with specialized overrides
- **Dynamic Data Loading**: Templates that query pond data directly

### Performance Optimizations  
- **Parallel Template Processing**: Process multiple templates concurrently
- **Incremental Updates**: Only regenerate changed templates
- **Caching**: Cache rendered templates for repeated use
- **Memory Management**: Stream large datasets through template processing

### Integration Features
- **Web Server Mode**: Serve generated content directly
- **Watch Mode**: Regenerate templates when data changes
- **Plugin System**: Custom template functions and filters
- **Export Scheduling**: Automated periodic export and template processing

## Conclusion

This implementation plan provides a comprehensive roadmap for extending DuckPond's export system with the multi-stage pipeline architecture from the original implementation. The key innovation is the **metadata carry-forward system** that enables powerful template-based derivation of static content from exported data.

The phased approach ensures:
- **Backward Compatibility**: Existing export functionality continues to work
- **Incremental Implementation**: Each phase builds on the previous
- **Robust Architecture**: Clean separation between data export and template processing
- **Extensibility**: Foundation for advanced template features and optimizations

This system enables DuckPond to generate sophisticated static websites and documentation directly from temporal data exports, making it a complete solution for data-driven static site generation.