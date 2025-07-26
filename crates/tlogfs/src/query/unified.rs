/// Common TableProvider implementation for both FileTable and FileSeries
/// This eliminates ~80% duplication between table.rs and series.rs

use crate::query::MetadataTable;
use crate::error::TLogFSError;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;
use tinyfs::EntryType;
use diagnostics;

// DataFusion imports
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

/// Simple async cursor for Vec<u8> data
/// Implements AsyncReadSeek by wrapping data in memory 
#[derive(Debug)]
struct AsyncCursor {
    data: Vec<u8>,
    position: u64,
}

impl AsyncCursor {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, position: 0 }
    }
}

impl tokio::io::AsyncRead for AsyncCursor {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let remaining = buf.remaining();
        let data_len = self.data.len() as u64;
        let position = self.position;
        
        if position >= data_len {
            return std::task::Poll::Ready(Ok(()));
        }
        
        let available = (data_len - position) as usize;
        let bytes_to_read = remaining.min(available);
        
        if bytes_to_read > 0 {
            let start = position as usize;
            let end = start + bytes_to_read;
            buf.put_slice(&self.data[start..end]);
            self.position += bytes_to_read as u64;
        }
        
        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for AsyncCursor {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let new_position = match position {
            std::io::SeekFrom::Start(pos) => pos,
            std::io::SeekFrom::End(offset) => {
                let data_len = self.data.len() as i64;
                (data_len + offset) as u64
            }
            std::io::SeekFrom::Current(offset) => {
                let current = self.position as i64;
                (current + offset) as u64
            }
        };
        
        self.position = new_position.min(self.data.len() as u64);
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.position))
    }
}

// AsyncCursor automatically implements AsyncReadSeek via the blanket impl in tinyfs

/// Abstract file handle that can provide a reader
#[derive(Debug, Clone)]
pub struct FileHandle {
    pub file_path: String,
    pub version: Option<i64>,
    pub size: Option<u64>,
    pub metadata: FileMetadata,
}

#[derive(Debug, Clone)]
pub enum FileMetadata {
    Table { node_id: String },
    Series { 
        min_event_time: i64, 
        max_event_time: i64, 
        timestamp_column: String 
    },
}

impl FileHandle {
    /// Get async reader for this file
    pub async fn get_reader(&self, tinyfs_root: &Arc<tinyfs::WD>) -> Result<std::pin::Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        match &self.metadata {
            FileMetadata::Table { .. } => {
                // FileTable: read directly by path
                let reader = tinyfs_root.async_reader_path(&self.file_path).await
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read table file {}: {}", self.file_path, e)))?;
                Ok(reader)
            },
            FileMetadata::Series { .. } => {
                // FileSeries: read specific version
                let version = self.version.unwrap_or(1) as u64;
                let version_data = tinyfs_root.read_file_version(&self.file_path, Some(version)).await
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read series version {} of {}: {}", version, self.file_path, e)))?;
                
                // Create an async cursor from the version data that implements AsyncReadSeek
                let cursor = AsyncCursor::new(version_data);
                Ok(Box::pin(cursor) as std::pin::Pin<Box<dyn tinyfs::AsyncReadSeek>>)
            }
        }
    }
}

/// Unified table provider that works for both FileTable and FileSeries
#[derive(Debug, Clone)]
pub struct UnifiedTableProvider {
    path: String,
    node_id: Option<String>,
    tinyfs_root: Option<Arc<tinyfs::WD>>,
    schema: SchemaRef,
    metadata_table: MetadataTable,
    provider_type: ProviderType,
}

#[derive(Debug, Clone)]
pub enum ProviderType {
    Table,
    Series,
}

impl UnifiedTableProvider {
    /// Create new unified provider for FileTable
    pub fn new_table(
        path: String, 
        metadata_table: MetadataTable,
    ) -> Self {
        Self {
            path,
            node_id: None,
            tinyfs_root: None,
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            metadata_table,
            provider_type: ProviderType::Table,
        }
    }

    /// Create new unified provider for FileSeries
    pub fn new_series(
        path: String, 
        metadata_table: MetadataTable,
    ) -> Self {
        Self {
            path,
            node_id: None,
            tinyfs_root: None,
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            metadata_table,
            provider_type: ProviderType::Series,
        }
    }

    /// Create with TinyFS access and node_id for FileTable
    pub fn new_table_with_tinyfs_and_node_id(
        path: String,
        node_id: String,
        metadata_table: MetadataTable,
        tinyfs_root: Arc<tinyfs::WD>,
    ) -> Self {
        Self {
            path,
            node_id: Some(node_id),
            tinyfs_root: Some(tinyfs_root),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            metadata_table,
            provider_type: ProviderType::Table,
        }
    }

    /// Create with TinyFS access and node_id for FileSeries
    pub fn new_series_with_tinyfs_and_node_id(
        path: String,
        node_id: String,
        metadata_table: MetadataTable,
        tinyfs_root: Arc<tinyfs::WD>,
    ) -> Self {
        Self {
            path,
            node_id: Some(node_id),
            tinyfs_root: Some(tinyfs_root),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            metadata_table,
            provider_type: ProviderType::Series,
        }
    }

    /// Get files based on provider type - unified logic
    async fn get_files(&self) -> Result<Vec<FileHandle>, TLogFSError> {
        match &self.provider_type {
            ProviderType::Table => self.get_table_files().await,
            ProviderType::Series => self.get_series_files().await,
        }
    }

    async fn get_table_files(&self) -> Result<Vec<FileHandle>, TLogFSError> {
        if let Some(ref node_id) = self.node_id {
            // Get the FileTable record from metadata
            let records = self.metadata_table.query_records_for_node(node_id, EntryType::FileTable).await?;
            if let Some(record) = records.first() {
                return Ok(vec![FileHandle {
                    file_path: self.path.clone(),
                    version: None, // FileTable doesn't use versioning
                    size: record.size,
                    metadata: FileMetadata::Table { 
                        node_id: node_id.clone() 
                    },
                }]);
            }
        }
        Ok(vec![]) // No table file found
    }

    async fn get_series_files(&self) -> Result<Vec<FileHandle>, TLogFSError> {
        if let Some(ref node_id) = self.node_id {
            // Get all FileSeries records for this node
            let records = self.metadata_table.query_records_for_node(node_id, EntryType::FileSeries).await?;
            let mut handles = Vec::new();
            
            for record in records {
                handles.push(FileHandle {
                    file_path: self.path.clone(),
                    version: Some(record.version),
                    size: record.size,
                    metadata: FileMetadata::Series {
                        min_event_time: record.min_event_time.unwrap_or(0),
                        max_event_time: record.max_event_time.unwrap_or(0),
                        timestamp_column: "timestamp".to_string(), // Default column
                    },
                });
            }
            
            // Sort by version
            handles.sort_by_key(|h| h.version.unwrap_or(0));
            
            return Ok(handles);
        }
        Ok(vec![]) // No series files found
    }

    /// Load schema from actual Parquet data files - unified logic
    pub async fn load_schema_from_data(&mut self) -> Result<(), TLogFSError> {
        diagnostics::log_debug!("UnifiedTableProvider::load_schema_from_data for {path}", path: self.path);

        if let Some(ref tinyfs_root) = self.tinyfs_root {
            // Get files from the provider
            let files = self.get_files().await?;
            
            if let Some(file) = files.first() {
                // Use first file to extract schema
                match file.get_reader(tinyfs_root).await {
                    Ok(reader) => {
                        match parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await {
                            Ok(builder) => {
                                self.schema = builder.schema().clone();
                                let fields_count = self.schema.fields().len();
                                diagnostics::log_debug!("Loaded schema with {fields_count} fields from file {file_path}", 
                                    fields_count: fields_count, file_path: file.file_path);
                                return Ok(());
                            },
                            Err(e) => {
                                diagnostics::log_info!("Failed to build Parquet reader for schema extraction from {file_path}: {e}", 
                                    file_path: file.file_path, e: e);
                            }
                        }
                    },
                    Err(e) => {
                        diagnostics::log_info!("Failed to get reader for schema extraction from {file_path}: {e}", 
                            file_path: file.file_path, e: e);
                    }
                }
            }
        }
        
        Err(TLogFSError::ArrowMessage(format!("Failed to load schema for {}", self.path)))
    }

    fn execution_plan_name(&self) -> &str {
        match self.provider_type {
            ProviderType::Table => "TableExecutionPlan",
            ProviderType::Series => "SeriesExecutionPlan",
        }
    }
}

#[async_trait]
impl TableProvider for UnifiedTableProvider {
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
        // Apply projection to schema
        let projected_schema = if let Some(projection) = projection {
            Arc::new(self.schema.project(projection)?)
        } else {
            self.schema.clone()
        };

        Ok(Arc::new(UnifiedExecutionPlan {
            provider: self.clone(),
            projection: projection.cloned(),
            schema: projected_schema,
            properties: self.create_plan_properties(&projected_schema),
        }))
    }
}

impl UnifiedTableProvider {
    fn create_plan_properties(&self, schema: &SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let output_partitioning = Partitioning::UnknownPartitioning(1);
        PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded
        )
    }
}

/// Unified execution plan that works for both FileTable and FileSeries
#[derive(Debug)]
struct UnifiedExecutionPlan {
    provider: UnifiedTableProvider,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DisplayAs for UnifiedExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: path={}", self.provider.execution_plan_name(), self.provider.path)
    }
}

#[async_trait]
impl ExecutionPlan for UnifiedExecutionPlan {
    fn name(&self) -> &str {
        self.provider.execution_plan_name()
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
        let provider = self.provider.clone();
        let projection = self.projection.clone();

        let stream = async_stream::stream! {
            // Get files from provider
            match provider.get_files().await {
                Ok(files) => {
                    for file in files {
                        // Get reader for this file
                        if let Some(ref root) = provider.tinyfs_root {
                            match file.get_reader(root).await {
                                Ok(reader) => {
                                    // Stream Parquet data with unified logic
                                    match parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await {
                                        Ok(builder) => {
                                            match builder.build() {
                                                Ok(mut parquet_stream) => {
                                                    use futures::stream::StreamExt;
                                                    while let Some(batch_result) = parquet_stream.next().await {
                                                        match batch_result {
                                                            Ok(batch) => {
                                                                // Apply projection if needed - UNIFIED LOGIC
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
                                                                diagnostics::log_info!("Parquet stream error in {file_path}: {e}", 
                                                                    file_path: file.file_path, e: e);
                                                                yield Err(datafusion::error::DataFusionError::Execution(
                                                                    format!("Parquet stream error: {}", e)
                                                                ));
                                                            }
                                                        }
                                                    }
                                                },
                                                Err(e) => {
                                                    yield Err(datafusion::error::DataFusionError::Execution(
                                                        format!("Failed to build Parquet stream: {}", e)
                                                    ));
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            yield Err(datafusion::error::DataFusionError::Execution(
                                                format!("Failed to create Parquet builder: {}", e)
                                            ));
                                        }
                                    }
                                },
                                Err(e) => {
                                    yield Err(datafusion::error::DataFusionError::Execution(
                                        format!("Failed to get reader for {}: {}", file.file_path, e)
                                    ));
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    yield Err(datafusion::error::DataFusionError::Execution(
                        format!("Failed to get files: {}", e)
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
        &self.properties
    }
}
