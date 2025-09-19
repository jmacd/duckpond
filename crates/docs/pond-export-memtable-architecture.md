# DuckPond Multi-Stage Export with MemTable State Transfer

## Architecture Overview

Instead of serializing export metadata to files between stages, we use DataFusion's MemTable system to transfer state as RecordBatches. Each export stage registers temporary tables in the DataFusion SessionContext that subsequent stages can query.

## Stage Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Stage 1:      │───▶│   MemTable:     │───▶│   Stage 2:      │
│   Data Export   │    │   Export Meta   │    │   Template Gen  │
│                 │    │   (RecordBatch) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       SessionContext
                       .register_table(
                         "export_metadata", 
                         MemTable
                       )
```

## MemTable Schema Design

### Export Metadata Table Schema
```rust
// Schema for export_metadata table
let export_schema = Schema::new(vec![
    Field::new("pattern", DataType::Utf8, false),           // Original pattern
    Field::new("capture_group", DataType::Int32, false),    // Capture group index  
    Field::new("capture_value", DataType::Utf8, false),     // Captured value
    Field::new("file_path", DataType::Utf8, false),         // Exported file path
    Field::new("start_time", DataType::Int64, true),        // Unix timestamp ms
    Field::new("end_time", DataType::Int64, true),          // Unix timestamp ms
    Field::new("row_count", DataType::Int64, true),         // Number of rows
    Field::new("export_stage", DataType::Int32, false),     // Stage number
]);

// Example data in MemTable:
// pattern                               | capture_group | capture_value      | file_path                           | start_time    | end_time      | row_count | export_stage
// "/test-locations/*/res=1d.series"     | 0            | "BDockDownsampled" | "BDock../year=2024/month=7/day=20"  | 1721419200000 | 1721505600000 | 1         | 1
// "/test-locations/*/res=1d.series"     | 0            | "BDockDownsampled" | "BDock../year=2024/month=7/day=21"  | 1721505600000 | 1721592000000 | 1         | 1
```

## Implementation Architecture

### Stage 1: Data Export with MemTable Registration

```rust
// In export_single_target function
pub async fn export_single_target_with_metadata(
    target: &ExportTarget,
    pattern: &str,
    temporal_columns: &[String],
    output_dir: &Path,
    tx_guard: &mut StewardTransactionGuard,
    export_context: &mut ExportContext,
) -> Result<()> {
    
    // ... existing export logic ...
    
    // After successful export, create metadata records
    let metadata_records = create_export_metadata_records(
        pattern,
        &captures,
        &exported_files,
        1, // stage number
    ).await?;
    
    // Add to export context for MemTable registration
    export_context.add_metadata_records(metadata_records);
    
    Ok(())
}

// Export context manages MemTable state
pub struct ExportContext {
    metadata_records: Vec<RecordBatch>,
    session_context: SessionContext,
}

impl ExportContext {
    pub fn new() -> Self {
        Self {
            metadata_records: Vec::new(),
            session_context: SessionContext::new(),
        }
    }
    
    pub fn add_metadata_records(&mut self, records: RecordBatch) -> Result<()> {
        self.metadata_records.push(records);
        
        // Register/update the MemTable
        self.register_export_metadata_table()?;
        Ok(())
    }
    
    fn register_export_metadata_table(&mut self) -> Result<()> {
        let schema = export_metadata_schema();
        let mem_table = MemTable::try_new(schema, vec![self.metadata_records.clone()])?;
        
        self.session_context.register_table(
            "export_metadata", 
            Arc::new(mem_table)
        )?;
        
        Ok(())
    }
    
    pub async fn query_metadata(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.session_context.sql(sql).await?;
        df.collect().await.map_err(Into::into)
    }
}
```

### Stage 2: Template Processing with MemTable Queries

```rust
pub async fn process_templates_with_memtable(
    template_specs: Vec<TemplateSpec>,
    export_context: &ExportContext,
    output_dir: &Path,
) -> Result<()> {
    
    for spec in template_specs {
        for collection in spec.collections {
            
            // Query export metadata for this template's input pattern
            let metadata_sql = format!(
                r#"
                SELECT 
                    capture_group,
                    capture_value,
                    file_path,
                    start_time,
                    end_time,
                    row_count
                FROM export_metadata 
                WHERE pattern = '{}' 
                ORDER BY capture_group, start_time
                "#,
                collection.in_pattern
            );
            
            let metadata_batches = export_context.query_metadata(&metadata_sql).await?;
            
            // Convert to structured data for template context
            let template_data = build_template_context_from_batches(metadata_batches)?;
            
            // Process template with hierarchical data
            process_template_collection_with_context(
                &collection,
                &template_data,
                output_dir,
            ).await?;
        }
    }
    
    Ok(())
}

// Convert RecordBatches to template-friendly structure
fn build_template_context_from_batches(
    batches: Vec<RecordBatch>
) -> Result<TemplateContext> {
    let mut context = TemplateContext::new();
    
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let capture_group = get_i32_value(&batch, "capture_group", row_idx)?;
            let capture_value = get_string_value(&batch, "capture_value", row_idx)?;
            let file_path = get_string_value(&batch, "file_path", row_idx)?;
            let start_time = get_optional_i64_value(&batch, "start_time", row_idx)?;
            let end_time = get_optional_i64_value(&batch, "end_time", row_idx)?;
            
            // Build hierarchical export structure similar to original ExportSet
            context.add_export_entry(
                capture_group,
                capture_value,
                ExportOutput {
                    file: PathBuf::from(file_path),
                    start_time,
                    end_time,
                }
            );
        }
    }
    
    Ok(context)
}
```

### Advanced: Multi-Stage Pipeline with Chained MemTables

```rust
pub async fn run_multi_stage_export_pipeline(
    stages: Vec<ExportStage>,
    initial_patterns: Vec<String>,
    output_dir: &Path,
) -> Result<()> {
    
    let mut export_context = ExportContext::new();
    
    for (stage_num, stage) in stages.iter().enumerate() {
        println!("🔄 Running Stage {}: {}", stage_num + 1, stage.name);
        
        match stage.stage_type {
            ExportStageType::DataExport { temporal, filesystem } => {
                // Stage 1: Export data and populate metadata table
                run_data_export_stage(
                    &initial_patterns,
                    &temporal,
                    filesystem,
                    output_dir,
                    &mut export_context,
                    stage_num + 1,
                ).await?;
            }
            
            ExportStageType::TemplateProcessing { template_spec, vars } => {
                // Stage 2+: Process templates using metadata from previous stages
                run_template_processing_stage(
                    &template_spec,
                    &vars,
                    output_dir,
                    &mut export_context,
                    stage_num + 1,
                ).await?;
            }
            
            ExportStageType::CustomQuery { sql, output_table } => {
                // Stage N: Custom SQL processing with result as new MemTable
                run_custom_query_stage(
                    &sql,
                    &output_table,
                    &mut export_context,
                    stage_num + 1,
                ).await?;
            }
        }
        
        println!("✅ Stage {} completed", stage_num + 1);
    }
    
    Ok(())
}

#[derive(Debug)]
pub struct ExportStage {
    pub name: String,
    pub stage_type: ExportStageType,
}

#[derive(Debug)]
pub enum ExportStageType {
    DataExport {
        temporal: String,
        filesystem: FilesystemChoice,
    },
    TemplateProcessing {
        template_spec: PathBuf,
        vars: Vec<(String, String)>,
    },
    CustomQuery {
        sql: String,
        output_table: String,
    },
}
```

## CLI Integration

### Enhanced Export Command
```rust
/// Export pond data with multi-stage pipeline support
Export {
    /// File patterns to export
    #[arg(short, long)]
    pattern: Vec<String>,
    /// Output directory
    #[arg(short, long)]
    dir: PathBuf,
    /// Temporal partitioning levels
    #[arg(long, default_value = "")]
    temporal: String,
    /// Pipeline configuration file (YAML) defining multiple stages
    #[arg(long)]
    pipeline: Option<PathBuf>,
    /// Template specification file for single-stage template processing
    #[arg(short, long)]
    template: Option<PathBuf>,
    /// Template variables as key=value pairs
    #[arg(short, value_parser = parse_key_val, number_of_values = 1)]
    vars: Vec<(String, String)>,
    /// Which filesystem to export from
    #[arg(long, short = 'f', default_value = "data")]
    filesystem: FilesystemChoice,
    /// Overwrite existing files
    #[arg(long)]
    overwrite: bool,
}
```

### Pipeline Configuration Example
```yaml
# export-pipeline.yaml
stages:
  - name: "Export Sensor Data"
    type: data_export
    temporal: "year,month,day"
    filesystem: data
    
  - name: "Generate Site Pages"
    type: template_processing
    template_spec: "site-templates.yaml"
    vars:
      - key: "site_name"
        value: "Noyo Harbor"
      - key: "generated_date"
        value: "2024-09-19"
        
  - name: "Build Navigation Index"
    type: custom_query
    sql: |
      SELECT 
        capture_value as site_name,
        COUNT(*) as file_count,
        MIN(start_time) as earliest_data,
        MAX(end_time) as latest_data
      FROM export_metadata 
      WHERE export_stage = 1
      GROUP BY capture_value
      ORDER BY site_name
    output_table: "site_index"
    
  - name: "Generate Navigation"
    type: template_processing  
    template_spec: "navigation-templates.yaml"
    # This stage can query both export_metadata AND site_index tables
```

## Benefits of MemTable Approach

### Performance Benefits
- **No File I/O**: State transfer happens in memory via RecordBatches
- **SQL Queryable**: Each stage can use complex SQL to query previous stage results
- **Columnar Efficiency**: Arrow format provides efficient data processing
- **Parallel Processing**: Multiple stages can process different partitions concurrently

### Architectural Benefits  
- **Type Safety**: Arrow schema ensures consistent data types between stages
- **SQL Flexibility**: Each stage can perform complex data transformations
- **Composability**: Stages can be combined and reordered easily
- **Debugging**: Intermediate MemTables can be inspected and queried for troubleshooting

### Integration Benefits
- **DataFusion Native**: Leverages existing DuckPond DataFusion infrastructure
- **Transaction Aware**: Integrates with steward transaction system
- **Memory Management**: Arrow's memory pooling handles large datasets efficiently

## Example Usage

```bash
# Single-stage export (backward compatible)
pond export -p "/sensors/*.series" -d /tmp/export --temporal "year,month,day"

# Multi-stage pipeline export
pond export -p "/sensors/*.series" -d /tmp/export --pipeline export-pipeline.yaml

# Template export with variables
pond export -p "/sensors/*.series" -d /tmp/export \
  --template site-templates.yaml \
  -v site_name="Noyo Harbor" -v res="1d"
```

## Implementation Plan

### Phase 1: MemTable Infrastructure
- [ ] Implement ExportContext with MemTable management
- [ ] Define export metadata schema
- [ ] Create RecordBatch builders for metadata

### Phase 2: Stage Integration
- [ ] Update export_single_target to produce metadata RecordBatches
- [ ] Implement template processing with MemTable queries
- [ ] Add pipeline configuration parser

### Phase 3: Advanced Features
- [ ] Multi-stage pipeline orchestrator
- [ ] Custom SQL stages
- [ ] Pipeline validation and error handling

This architecture provides a clean, efficient, and SQL-queryable way to transfer state between export stages while maintaining compatibility with the original export patterns.