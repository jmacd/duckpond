//! NodeVersionTable - DataFusion table provider for individual file versions in TLogFS
//! 
//! This table provider represents a single file version and can be used in UNION BY NAME
//! queries for origin tracking in temporal overlap detection.

use std::sync::Arc;
use std::any::Any;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
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
use tinyfs::AsyncReadSeek;
use crate::error::TLogFSError;
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
    ) -> Result<Self, TLogFSError> {
        // Eagerly load the schema from the file
        let schema = Self::load_schema_from_file(&node_id, version, &file_path, &tinyfs_root).await?;
        
        Ok(Self {
            node_id,
            version,
            file_path,
            tinyfs_root,
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
    
    /// Get an async reader for the file data
    async fn get_reader(&self) -> Result<std::pin::Pin<Box<dyn AsyncReadSeek>>, TLogFSError> {
        let version_str = format!("{:?}", self.version);
        debug!("Getting reader for node {node_id} version {version_str}", node_id: self.node_id, version_str: version_str);
        
        let file_data = self.tinyfs_root
            .read_file_version(&self.file_path, self.version).await
            .map_err(|e| TLogFSError::ArrowMessage(format!(
                "Failed to read file {} version {:?}: {}", 
                self.file_path, self.version, e
            )))?;
        
        // Create async cursor that implements AsyncReadSeek
        let cursor = AsyncCursor::new(file_data);
        Ok(Box::pin(cursor))
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
        let version_str = format!("{:?}", self.version);
        debug!("Executing NodeVersionExecutionPlan for node {node_id} version {version_str}", 
               node_id: self.node_id, version_str: version_str);
        
        // Clone the necessary data for the async operation
        let node_id = self.node_id.clone();
        let version = self.version;
        let file_path = self.file_path.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let tinyfs_root = self.tinyfs_root.clone();
        
        // Create a future that does the async work
        let future = async move {
            // Read the file data
            let file_data = tinyfs_root
                .read_file_version(&file_path, version).await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(
                    TLogFSError::ArrowMessage(format!(
                        "Failed to read file {} version {:?}: {}", 
                        file_path, version, e
                    ))
                )))?;
            
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
                
                // Apply projection if needed
                let final_batch = if let Some(projection) = &projection {
                    let mut projected_columns = Vec::new();
                    for &i in projection {
                        projected_columns.push(batch.column(i).clone());
                    }
                    
                    let projected_schema = Arc::new(schema.project(projection)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?);
                    
                    arrow::record_batch::RecordBatch::try_new(projected_schema, projected_columns)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?
                } else {
                    batch
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

/// Async wrapper around std::io::Cursor that implements AsyncReadSeek
/// This is a copy of the one from series.rs - should be moved to a common module
#[derive(Debug)]
struct AsyncCursor {
    data: Vec<u8>,
    position: usize,
}

impl AsyncCursor {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            position: 0,
        }
    }
}

impl tokio::io::AsyncRead for AsyncCursor {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let remaining_data = self.data.len() - self.position;
        let buf_remaining = buf.remaining();
        let bytes_to_read = buf_remaining.min(remaining_data);
        
        if bytes_to_read > 0 {
            let end_pos = self.position + bytes_to_read;
            buf.put_slice(&self.data[self.position..end_pos]);
            self.position = end_pos;
        }
        
        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for AsyncCursor {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let new_pos = match position {
            std::io::SeekFrom::Start(pos) => pos as usize,
            std::io::SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.position + offset as usize
                } else {
                    self.position.saturating_sub((-offset) as usize)
                }
            }
            std::io::SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.data.len() + offset as usize
                } else {
                    self.data.len().saturating_sub((-offset) as usize)
                }
            }
        };
        
        self.position = new_pos.min(self.data.len());
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.position as u64))
    }
}
