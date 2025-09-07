use crate::query::NodeTable;
use crate::error::TLogFSError;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;
use tinyfs::EntryType;
use diagnostics;

// DataFusion imports for table providers and execution plans
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{Statistics, PlanProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use std::any::Any;
use std::fmt;

/// Specialized table for querying file:table data 
/// 
/// This table provides queries over FileTable entries by directly reading 
/// Parquet files from TinyFS storage. Unlike FileSeries, FileTable entries
/// represent single-version tables without temporal metadata.
/// 
/// Architecture:
/// 1. **Single version**: FileTable entries are replaced, not appended
/// 2. **No temporal metadata**: No min/max event time tracking
/// 3. **Simple schema**: Direct Parquet schema without temporal extensions
/// 4. **Streaming execution**: Processes data without loading everything into memory
///
/// Example queries:
/// - SELECT * FROM table_data 
/// - SELECT column1, column2 FROM table_data WHERE column1 > 100
#[derive(Debug, Clone)]
pub struct TableTable {
    table_path: String,  // The original file path for reading data (e.g., "/path/to/data.table")
    node_id: Option<String>, // The node_id for metadata queries (when available)
    tinyfs_root: Option<Arc<tinyfs::WD>>,  // TinyFS root for file access
    schema: SchemaRef,  // The schema of the table data
    metadata_table: NodeTable,  // Delta Lake metadata table for OplogEntry queries
}

/// Information about a file that contains table data
#[derive(Debug, Clone)]
pub struct TableFileInfo {
    pub node_id: String,
    pub part_id: String,
    pub version: i64,
    pub size: Option<i64>, // Changed to i64 to match Delta Lake protocol
}

impl TableTable {
    /// Create a new TableTable for querying file:table data
    pub fn new(table_path: String, metadata_table: NodeTable) -> Self {
        Self {
            table_path,
            node_id: None,
            tinyfs_root: None,
            // Start with empty schema - will be loaded from data
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            metadata_table,
        }
    }

    /// Create a new TableTable with TinyFS access and node_id for direct file reading
    pub fn new_with_tinyfs_and_node_id(
        table_path: String, 
        node_id: String, 
        metadata_table: NodeTable, 
        tinyfs_root: Arc<tinyfs::WD>
    ) -> Self {
        Self {
            table_path,
            node_id: Some(node_id),
            tinyfs_root: Some(tinyfs_root),
            // Start with empty schema - will be loaded from data
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            metadata_table,
        }
    }

    /// Load schema from the actual Parquet data files
    /// This is required before registering with DataFusion for column validation
    pub async fn load_schema_from_data(&mut self) -> Result<(), TLogFSError> {
        diagnostics::log_debug!("TableTable::load_schema_from_data for {table_path}", table_path: self.table_path);

        if let Some(ref tinyfs_root) = self.tinyfs_root {
            if let Some(ref _node_id) = self.node_id {
                // Try to read the file directly from TinyFS to extract schema
                match tinyfs_root.async_reader_path(&self.table_path).await {
                    Ok(reader) => {
                        // Use Parquet to read schema
                        match parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await {
                            Ok(builder) => {
                                self.schema = builder.schema().clone();
                                let fields_count = self.schema.fields().len();
                                diagnostics::log_debug!("TableTable schema loaded from Parquet file: {fields_count} fields", fields_count: fields_count);
                                return Ok(());
                            },
                            Err(e) => {
                                diagnostics::log_info!("Failed to read Parquet schema from {table_path}: {e}", table_path: self.table_path, e: e);
                            }
                        }
                    },
                    Err(e) => {
                        diagnostics::log_info!("Failed to open file {table_path}: {e}", table_path: self.table_path, e: e);
                    }
                }
            }
        }

        // Fallback: query metadata table for schema information
        // This is less efficient but works when direct file access fails
        if let Some(ref node_id) = self.node_id {
            let records = self.metadata_table.query_records_for_node(node_id, EntryType::FileTable).await?;
            
            if let Some(entry) = records.first() {
                if let Some(ref attrs) = entry.get_extended_attributes() {
                    // Try to get schema from extended attributes if available
                    if let Some(schema_json) = attrs.attributes.get("schema_json") {
                        match serde_json::from_str::<arrow::datatypes::Schema>(schema_json) {
                            Ok(schema) => {
                                self.schema = Arc::new(schema);
                                let fields_count = self.schema.fields().len();
                                diagnostics::log_debug!("TableTable schema loaded from metadata: {fields_count} fields", fields_count: fields_count);
                                return Ok(());
                            },
                            Err(e) => {
                                diagnostics::log_info!("Failed to parse schema JSON: {e}", e: e);
                            }
                        }
                    }
                }
            }
        }

        // If we still don't have a schema, create a minimal one
        if self.schema.fields().is_empty() {
            diagnostics::log_info!("No schema found for TableTable {table_path}, using empty schema", table_path: self.table_path);
        }

        Ok(())
    }

    /// Get the current file info for this table (should be single version)
    async fn get_table_file_info(&self) -> Result<Option<TableFileInfo>, TLogFSError> {
        if let Some(ref node_id) = self.node_id {
            let records = self.metadata_table.query_records_for_node(node_id, EntryType::FileTable).await?;
            
            // For FileTable, there should be only one current version (unlike FileSeries)
            if let Some(entry) = records.last() { // Get the most recent version
                return Ok(Some(TableFileInfo {
                    node_id: node_id.clone(),
                    part_id: entry.part_id.clone(),
                    version: entry.version,
                    size: entry.size,
                }));
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl TableProvider for TableTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        diagnostics::log_debug!("TableTable::scan called for {table_path}", table_path: self.table_path);

        // Apply projection to schema if provided
        let projected_schema = if let Some(projection_indices) = projection {
            let original_fields = self.schema.fields();
            let projected_fields: Vec<_> = projection_indices
                .iter()
                .map(|&i| original_fields[i].clone())
                .collect();
            Arc::new(arrow::datatypes::Schema::new(projected_fields))
        } else {
            self.schema.clone()
        };

        // Create execution plan for streaming the table data
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        
        let exec_plan = Arc::new(TableExecutionPlan {
            table_table: self.clone(),
            projection: projection.cloned(),
            schema: projected_schema,
            properties,
        });

        Ok(exec_plan)
    }
}

/// Custom execution plan for streaming table data
#[derive(Debug)]
struct TableExecutionPlan {
    table_table: TableTable,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DisplayAs for TableExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TableExecutionPlan: table_path={}", self.table_table.table_path)
    }
}

#[async_trait]
impl ExecutionPlan for TableExecutionPlan {
    fn name(&self) -> &str {
        "TableExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        diagnostics::log_debug!("TableExecutionPlan::execute for {table_path}", table_path: self.table_table.table_path);

        // Create a stream that reads the single table file
        let table_table = self.table_table.clone();
        let projection = self.projection.clone();
        let stream = async_stream::stream! {
            // Get the table file info
            match table_table.get_table_file_info().await {
                Ok(Some(file_info)) => {
                    diagnostics::log_debug!("Found table file: node_id={node_id}, version={version}", 
                        node_id: file_info.node_id, version: file_info.version);

                    // Read the file from TinyFS
                    if let Some(ref tinyfs_root) = table_table.tinyfs_root {
                        match tinyfs_root.async_reader_path(&table_table.table_path).await {
                            Ok(reader) => {
                                // Stream Parquet data
                                match parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await {
                                    Ok(builder) => {
                                        match builder.build() {
                                            Ok(mut parquet_stream) => {
                                                use futures::stream::StreamExt;
                                                while let Some(batch_result) = parquet_stream.next().await {
                                                    match batch_result {
                                                        Ok(batch) => {
                                                            // Apply projection if needed
                                                            let projected_batch = if let Some(ref proj) = projection {
                                                                match batch.project(proj) {
                                                                    Ok(projected) => projected,
                                                                    Err(e) => {
                                                                        yield Err(datafusion::error::DataFusionError::Execution(
                                                                            format!("Failed to project batch: {}", e)
                                                                        ));
                                                                        continue;
                                                                    }
                                                                }
                                                            } else {
                                                                batch
                                                            };
                                                            yield Ok(projected_batch);
                                                        },
                                                        Err(e) => {
                                                            diagnostics::log_info!("Parquet stream error: {e}", e: e);
                                                            yield Err(datafusion::error::DataFusionError::Execution(
                                                                format!("Parquet stream error: {}", e)
                                                            ));
                                                            return;
                                                        }
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                diagnostics::log_info!("Failed to build Parquet stream: {e}", e: e);
                                                yield Err(datafusion::error::DataFusionError::Execution(
                                                    format!("Failed to build Parquet stream: {}", e)
                                                ));
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        diagnostics::log_info!("Failed to create Parquet builder: {e}", e: e);
                                        yield Err(datafusion::error::DataFusionError::Execution(
                                            format!("Failed to create Parquet builder: {}", e)
                                        ));
                                    }
                                }
                            },
                            Err(e) => {
                                diagnostics::log_info!("Failed to open table file: {e}", e: e);
                                yield Err(datafusion::error::DataFusionError::Execution(
                                    format!("Failed to open table file: {}", e)
                                ));
                            }
                        }
                    } else {
                        diagnostics::log_info!("No TinyFS root available for table access");
                        yield Err(datafusion::error::DataFusionError::Execution(
                            "No TinyFS root available for table access".to_string()
                        ));
                    }
                },
                Ok(None) => {
                    diagnostics::log_info!("No table file found for {table_path}", table_path: table_table.table_path);
                    // Return empty stream for non-existent table
                },
                Err(e) => {
                    diagnostics::log_info!("Failed to get table file info: {e}", e: e);
                    yield Err(datafusion::error::DataFusionError::Execution(
                        format!("Failed to get table file info: {}", e)
                    ));
                }
            }
        };

        let stream = RecordBatchStreamAdapter::new(self.schema.clone(), Box::pin(stream));
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics, datafusion::error::DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn properties(&self) -> &PlanProperties {
        // For now, return basic properties. This could be enhanced with more sophisticated
        // partitioning and ordering information in the future.
        &self.properties
    }
}
