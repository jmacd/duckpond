//! NodeVersionTable - DataFusion table provider for individual file versions in TLogFS
//! 
//! This table provider represents a single file version and can be used in UNION BY NAME
//! queries for origin tracking in temporal overlap detection.

use std::sync::Arc;
use std::any::Any;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::{
    PlanProperties, Partitioning, SendableRecordBatchStream
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{EmissionType, Boundedness};
use datafusion::physical_plan::DisplayAs;
use datafusion::execution::TaskContext;
use crate::error::TLogFSError;
use crate::query::nodes::NodeTable;
use diagnostics::*;

/// Table provider for a single TLogFS file version
/// 
/// This is simpler than SeriesTable - it represents exactly one file version
/// and can be used in UNION BY NAME queries for origin tracking.
#[derive(Debug, Clone)]
pub struct NodeVersionTable {
    node_id: String,
    version: Option<u64>,  // None means latest version
    file_path: String,     // Original file path for debugging
    tinyfs_root: Arc<tinyfs::WD>,
    metadata_table: NodeTable,  // For temporal override queries
    schema: SchemaRef,     // Will be loaded lazily
}

impl NodeVersionTable {
    /// Create a new NodeVersionTable for a specific file version
    /// This will eagerly load the schema from the file
    pub async fn new(
        node_id: String,
        version: Option<u64>,
        file_path: String,
        tinyfs_root: Arc<tinyfs::WD>,
        metadata_table: NodeTable,
    ) -> Result<Self, TLogFSError> {
        // Eagerly load the schema from the file
        let schema = Self::load_schema_from_file(&node_id, version, &file_path, &tinyfs_root).await?;
        
        Ok(Self {
            node_id,
            version,
            file_path,
            tinyfs_root,
            metadata_table,
            schema,
        })
    }
    
    /// Load the schema from the actual file data (static method)
    async fn load_schema_from_file(
        node_id: &str,
        version: Option<u64>,
        file_path: &str,
        tinyfs_root: &tinyfs::WD,
    ) -> Result<SchemaRef, TLogFSError> {
        let version_str = format!("{:?}", version);
        debug!("Loading schema for node {node_id} version {version_str}", node_id: node_id, version_str: version_str);
        
        // Read the file data
        let file_data = tinyfs_root
            .read_file_version(file_path, version).await
            .map_err(|e| TLogFSError::ArrowMessage(format!(
                "Failed to read file {} version {:?}: {}", 
                file_path, version, e
            )))?;
        
        // Parse as Parquet to get schema
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;
        
        let bytes = Bytes::from(file_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| TLogFSError::ArrowMessage(format!(
                "Failed to parse Parquet file {}: {}", 
                file_path, e
            )))?;
        
        let schema = parquet_reader.schema();
        let field_count = schema.fields().len();
        debug!("Loaded schema with {field_count} fields for node {node_id}", field_count: field_count, node_id: node_id);
        
        Ok(schema.clone())
    }
    }

#[async_trait::async_trait]
impl TableProvider for NodeVersionTable {
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
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],  // For now, ignore filters - UNION will handle them
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Schema is already loaded during construction
        let schema = self.schema.clone();
        
        // Create execution plan for this single file
        let execution_plan = Arc::new(NodeVersionExecutionPlan::new(
            self.node_id.clone(),
            self.version,
            self.file_path.clone(),
            schema,
            projection.cloned(),
            limit,
            self.tinyfs_root.clone(),
            self.metadata_table.clone(),
        ));
        
        Ok(execution_plan)
    }
}

/// Execution plan for reading a single TLogFS file version
#[derive(Debug)]
pub struct NodeVersionExecutionPlan {
    node_id: String,
    version: Option<u64>,
    file_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    tinyfs_root: Arc<tinyfs::WD>,
    metadata_table: NodeTable,  // For temporal override queries
    properties: PlanProperties,
}

impl NodeVersionExecutionPlan {
    pub fn new(
        node_id: String,
        version: Option<u64>,
        file_path: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        tinyfs_root: Arc<tinyfs::WD>,
        metadata_table: NodeTable,
    ) -> Self {
        // Create plan properties
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let output_partitioning = Partitioning::UnknownPartitioning(1);
        let properties = PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded
        );
        
        Self {
            node_id,
            version,
            file_path,
            schema,
            projection,
            limit,
            tinyfs_root,
            metadata_table,
            properties,
        }
    }
}

impl DisplayAs for NodeVersionExecutionPlan {
    fn fmt_as(&self, t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(f, "NodeVersionExecutionPlan(node={}, version={:?})", self.node_id, self.version)
            }
            datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(f, "NodeVersionExecutionPlan(node={}, version={:?}, file={})", 
                       self.node_id, self.version, self.file_path)
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => {
                write!(f, "NodeVersionExecutionPlan")
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for NodeVersionExecutionPlan {
    fn name(&self) -> &str {
        "NodeVersionExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
        let file_path = self.file_path.clone();
        let version = self.version;
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let tinyfs_root = self.tinyfs_root.clone();
        let metadata_table = self.metadata_table.clone();
        let node_id = self.node_id.clone();
        
        let future = async move {
            // Get temporal overrides from current version (if any)
            let current_overrides = {
                use tinyfs::EntryType;
                
                let current_records = metadata_table.query_records_for_node(&node_id, EntryType::FileSeries).await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                
                let record_count = current_records.len();
                diagnostics::log_info!("NodeVersionTable found {record_count} records for node {node_id}", 
                    record_count: record_count, node_id: node_id);
                
                // Find the latest (current) version
                let current_version = current_records.iter()
                    .inspect(|record| {
                        let min_str = record.min_override.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string());
                        let max_str = record.max_override.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string());
                        diagnostics::log_info!("NodeVersionTable record: version={version}, min_override={min_override}, max_override={max_override}",
                            version: record.version, min_override: min_str, max_override: max_str);
                    })
                    .max_by_key(|r| r.version);
                
                let overrides = current_version.and_then(|v| v.temporal_overrides());
                
                if let Some(current_ver) = current_version {
                    let has_overrides = overrides.is_some();
                    diagnostics::log_info!("NodeVersionTable current version: {version} with temporal_overrides result: {has_overrides}", 
                        version: current_ver.version, has_overrides: has_overrides);
                } else {
                    diagnostics::log_info!("NodeVersionTable no current version found", );
                }
                
                // Debug logging
                if let Some((min_override, max_override)) = overrides {
                    diagnostics::log_info!("NodeVersionTable found temporal overrides for {node_id}: {min_override} to {max_override}", 
                        node_id: node_id, min_override: min_override, max_override: max_override);
                } else {
                    diagnostics::log_info!("NodeVersionTable found no temporal overrides for {node_id}", node_id: node_id);
                }
                
                overrides
            };
            
            // Read the file data
            let file_data = tinyfs_root.read_file_version(&file_path, version).await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            
            // Parse as Parquet and get all record batches
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            use tokio_util::bytes::Bytes;
            
            let bytes = Bytes::from(file_data);
            let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(
                    TLogFSError::ArrowMessage(format!(
                        "Failed to parse Parquet file {}: {}", 
                        file_path, e
                    ))
                )))?;
            
            let record_batch_reader = parquet_reader.build()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(
                    TLogFSError::ArrowMessage(format!(
                        "Failed to build Parquet reader: {}", e
                    ))
                )))?;
            
            // Collect all batches and apply projection/limit
            let mut batches = Vec::new();
            let mut total_rows = 0;
            
            for batch_result in record_batch_reader {
                let batch = batch_result
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(
                        TLogFSError::ArrowMessage(format!(
                            "Failed to read record batch: {}", e
                        ))
                    )))?;
                
                // Apply temporal filtering if current version has overrides
                let filtered_batch = if let Some((min_time, max_time)) = current_overrides {
                    diagnostics::log_info!("Applying temporal filtering: {min_time} to {max_time}", 
                        min_time: min_time, max_time: max_time);
                    
                    // Find timestamp column (should be 'timestamp')
                    if let Ok(timestamp_col_idx) = batch.schema().index_of("timestamp") {
                        use arrow::array::{Array, Int64Array, BooleanArray, TimestampMicrosecondArray, TimestampSecondArray};
                        
                        let timestamp_column = batch.column(timestamp_col_idx);
                        
                        // Debug: Check the actual type of timestamp column
                        let timestamp_type = format!("{:?}", timestamp_column.data_type());
                        diagnostics::log_debug!("NodeVersionTable timestamp column type: {timestamp_type}", timestamp_type: timestamp_type);
                        
                        // Try different timestamp array types
                        let timestamp_values: Vec<i64> = if let Some(ts_array) = timestamp_column
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>() {
                            // Convert second timestamps to milliseconds for comparison
                            (0..ts_array.len())
                                .map(|i| {
                                    if ts_array.is_null(i) {
                                        0 // Will be filtered out anyway
                                    } else {
                                        ts_array.value(i) * 1000 // Convert seconds to milliseconds
                                    }
                                })
                                .collect()
                        } else if let Some(ts_array) = timestamp_column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>() {
                            // Convert microsecond timestamps to milliseconds for comparison
                            (0..ts_array.len())
                                .map(|i| {
                                    if ts_array.is_null(i) {
                                        0 // Will be filtered out anyway
                                    } else {
                                        ts_array.value(i) / 1000 // Convert microseconds to milliseconds
                                    }
                                })
                                .collect()
                        } else if let Some(int64_array) = timestamp_column
                            .as_any()
                            .downcast_ref::<Int64Array>() {
                            (0..int64_array.len())
                                .map(|i| {
                                    if int64_array.is_null(i) {
                                        0 // Will be filtered out anyway
                                    } else {
                                        int64_array.value(i)
                                    }
                                })
                                .collect()
                        } else {
                            return Err(datafusion::error::DataFusionError::Internal(
                                format!("Timestamp column has unsupported type: {:?}", timestamp_column.data_type())
                            ));
                        };
                        
                        // Create boolean mask for filtering: timestamp >= min_time AND timestamp <= max_time
                        let mut filter_mask = Vec::with_capacity(timestamp_values.len());
                        for (i, &ts) in timestamp_values.iter().enumerate() {
                            if timestamp_column.is_null(i) {
                                filter_mask.push(false);
                            } else {
                                filter_mask.push(ts >= min_time && ts <= max_time);
                            }
                        }
                        
                        let original_rows = batch.num_rows();
                        let filtered_rows = filter_mask.iter().filter(|&&x| x).count();
                        let filter_array = BooleanArray::from(filter_mask);
                        
                        diagnostics::log_info!("Temporal filter: {original_rows} → {filtered_rows} rows", 
                            original_rows: original_rows, filtered_rows: filtered_rows);
                        
                        // Apply filter to all columns
                        let filtered_columns: Result<Vec<_>, _> = batch.columns()
                            .iter()
                            .map(|col| arrow::compute::filter(col, &filter_array))
                            .collect();
                        
                        let filtered_columns = filtered_columns
                            .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;
                        
                        arrow::record_batch::RecordBatch::try_new(batch.schema(), filtered_columns)
                            .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?
                    } else {
                        // No timestamp column found, use original batch
                        batch
                    }
                } else {
                    // No temporal overrides, use original batch
                    batch
                };
                
                // Apply projection if needed
                let final_batch = if let Some(projection) = &projection {
                    let mut projected_columns = Vec::new();
                    for &i in projection {
                        projected_columns.push(filtered_batch.column(i).clone());
                    }
                    
                    let projected_schema = Arc::new(schema.project(projection)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?);
                    
                    arrow::record_batch::RecordBatch::try_new(projected_schema, projected_columns)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?
                } else {
                    filtered_batch
                };
                
                total_rows += final_batch.num_rows();
                batches.push(Ok(final_batch));
                
                // Apply limit if specified
                if let Some(limit) = limit {
                    if total_rows >= limit {
                        break;
                    }
                }
            }
            
            let batch_count = batches.len();
            debug!("NodeVersionExecutionPlan loaded {batch_count} batches with {total_rows} total rows", 
                   batch_count: batch_count, total_rows: total_rows);
            
            Ok::<Vec<Result<arrow::record_batch::RecordBatch, datafusion::error::DataFusionError>>, datafusion::error::DataFusionError>(batches)
        };
        
        // Convert the future into a stream
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream::{self, StreamExt};
        
        let batches_stream = stream::once(future).map(|batches_result| {
            match batches_result {
                Ok(batches) => stream::iter(batches).left_stream(),
                Err(e) => stream::once(async move { Err(e) }).right_stream(),
            }
        }).flatten();
        
        let record_batch_stream = RecordBatchStreamAdapter::new(self.schema.clone(), batches_stream);
        
        Ok(Box::pin(record_batch_stream))
    }
}

