/// File-Table Duality Integration for TinyFS and DataFusion
/// 
/// This module implements the FileTable trait that allows structured files
/// to expose both file-oriented and table-oriented interfaces.

use crate::error::TLogFSError;
use crate::file::OpLogFile; // Import the file types we'll implement FileTable for
use crate::sql_derived::SqlDerivedFile;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;
use std::any::Any;
use std::pin::Pin;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use futures::StreamExt;
use tinyfs::File; // Import File trait for async_reader() method

// DataFusion imports
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{Statistics, PlanProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter; // Add correct import
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::execution::TaskContext;
//use arrow::array::RecordBatch;  
//use arrow::error::ArrowError;
//use arrow::util::pretty;
// futures::StreamExt imported below in TableFileAdapter implementation

/// Trait for files that contain structured tabular data.
/// Provides both file-oriented and table-oriented access patterns.
#[async_trait]
pub trait FileTable: Send + Sync {
    /// Get a stream of RecordBatches from this table
    /// This is the primary interface for DataFusion integration
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError>;
    
    /// Get the schema of this table without reading data
    /// Should be efficient and cacheable
    async fn schema(&self) -> Result<SchemaRef, TLogFSError>;
    
    /// Get table statistics for query optimization (optional)
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        let schema = self.schema().await?;
        Ok(Statistics::new_unknown(&schema))
    }
    
}

/// Helper function to create TableProvider from FileTable  
pub fn create_table_provider<T: FileTable + 'static>(file_table: Arc<T>) -> Arc<dyn TableProvider> {
    let file_table_dyn: Arc<dyn FileTable> = file_table;
    Arc::new(FileTableProvider::new(file_table_dyn))
}

/// Generic TableProvider that wraps any FileTable
pub struct FileTableProvider {
    file_table: Arc<dyn FileTable>,
    cached_schema: Option<SchemaRef>,
}

impl FileTableProvider {
    pub fn new(file_table: Arc<dyn FileTable>) -> Self {
        Self {
            file_table,
            cached_schema: None,
        }
    }
    
    /// Load schema if not already cached
    pub async fn ensure_schema(&mut self) -> Result<(), TLogFSError> {
        if self.cached_schema.is_none() {
            self.cached_schema = Some(self.file_table.schema().await?);
        }
        Ok(())
    }
}

impl std::fmt::Debug for FileTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileTableProvider")
            .field("has_cached_schema", &self.cached_schema.is_some())
            .finish()
    }
}

#[async_trait]
impl TableProvider for FileTableProvider {
    fn as_any(&self) -> &dyn Any { 
        self 
    }
    
    fn schema(&self) -> SchemaRef {
        // For now, return empty schema if not cached
        // TODO: Make this async or require schema to be loaded during construction
        self.cached_schema.clone().unwrap_or_else(|| {
            Arc::new(arrow::datatypes::Schema::empty())
        })
    }
    
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Get stream from FileTable
        let stream = self.file_table.record_batch_stream().await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            
        Ok(Arc::new(StreamExecutionPlan::new(stream, self.schema())))
    }
}

/// ExecutionPlan that wraps a RecordBatch stream
pub struct StreamExecutionPlan {
    schema: SchemaRef,
    properties: PlanProperties,
}

impl StreamExecutionPlan {
    pub fn new(_stream: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        
        Self { schema, properties }
    }
}

impl std::fmt::Debug for StreamExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamExecutionPlan")
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for StreamExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamExecutionPlan")
    }
}

impl ExecutionPlan for StreamExecutionPlan {
    fn name(&self) -> &str {
        "StreamExecutionPlan"
    }
    
    fn as_any(&self) -> &dyn Any { 
        self 
    }
    
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition == 0 {
            // For now, we'll need to clone the stream
            // TODO: This needs proper stream handling for multiple partitions
            Err(datafusion::common::DataFusionError::Internal(
                "StreamExecutionPlan doesn't support multiple executions yet".to_string()
            ))
        } else {
            Err(datafusion::common::DataFusionError::Internal(format!(
                "Invalid partition: {}", partition
            )))
        }
    }
}

// Helper function to create a table provider from a TinyFS path
pub async fn create_table_provider_from_path(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    use tinyfs::Lookup;
    
    // Resolve the path to get the actual file
    let (_, lookup_result) = tinyfs_wd.resolve_path(path).await.map_err(TLogFSError::TinyFS)?;
    
    match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            
            // Try to get the file from the node - this returns Pathed<FileHandle>
            let file_handle = node_guard.as_file().map_err(|e| {
                TLogFSError::ArrowMessage(format!("Path {} does not point to a file: {}", path, e))
            })?;
            
            // For now, we need to determine the file type
            // This is a simplified approach - in practice we might check entry type or other metadata
            // Since we have FileHandle, we need to access the underlying implementation
            
            // For now, create a simple FileTable implementation that wraps the file handle
            // This uses the generic File trait interface without needing specific type detection
            let file_table: Arc<dyn FileTable> = Arc::new(GenericFileTable::new(file_handle));
            let mut provider = FileTableProvider::new(file_table);
            
            // Load schema during construction to avoid async issues later
            provider.ensure_schema().await?;
            
            Ok(Arc::new(provider))
        },
        Lookup::NotFound(full_path, _) => {
            Err(TLogFSError::ArrowMessage(format!("File not found: {}", full_path.display())))
        },
        Lookup::Empty(_) => {
            Err(TLogFSError::ArrowMessage("Empty path provided".to_string()))
        }
    }
}

// FileTable implementations for specific file types
// ============================================================================
// Common Parquet Processing Functions (Anti-Duplication)
// ============================================================================

/// Trait to provide a common async_reader interface for all FileTable implementors
/// This eliminates duplication in reader acquisition and error handling
trait FileTableReader {
    async fn get_reader(&self) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError>;
}

impl FileTableReader for OpLogFile {
    async fn get_reader(&self) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.async_reader().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get OpLogFile reader: {}", e)))
    }
}

impl FileTableReader for SqlDerivedFile {
    async fn get_reader(&self) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.async_reader().await.map_err(TLogFSError::TinyFS)
    }
}

impl FileTableReader for GenericFileTable {
    async fn get_reader(&self) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.file_handle.async_reader().await.map_err(TLogFSError::TinyFS)
    }
}

/// Common function to create a record batch stream from any FileTable reader
/// Eliminates duplication across all FileTable implementations
async fn create_parquet_stream<T: FileTableReader>(
    file_table: &T,
    context: &str,
) -> Result<SendableRecordBatchStream, TLogFSError> {
    let reader = file_table.get_reader().await?;
    let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet stream for {}: {}", context, e)))?;
        
    let stream = stream_builder.build()
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet stream for {}: {}", context, e)))?;
        
    // Wrap in RecordBatchStreamAdapter for DataFusion compatibility
    let schema = stream.schema().clone();
    let adapted_stream = RecordBatchStreamAdapter::new(
        schema,
        stream.map(|batch_result| batch_result.map_err(datafusion::error::DataFusionError::from))
    );
    
    Ok(Box::pin(adapted_stream))
}

/// Common function to extract schema from any FileTable reader
/// Eliminates duplication across all FileTable implementations
async fn extract_parquet_schema<T: FileTableReader>(
    file_table: &T,
    context: &str,
) -> Result<SchemaRef, TLogFSError> {
    let reader = file_table.get_reader().await?;
    let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read Parquet metadata for {}: {}", context, e)))?;
        
    Ok(stream_builder.schema().clone())
}

/// Common function to extract statistics from any FileTable reader
/// Eliminates duplication across all FileTable implementations
async fn extract_parquet_statistics<T: FileTableReader>(
    file_table: &T,
    context: &str,
) -> Result<Statistics, TLogFSError> {
    let reader = file_table.get_reader().await?;
    let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read Parquet metadata for {}: {}", context, e)))?;
        
    let schema = stream_builder.schema();
    Ok(Statistics::new_unknown(&schema))
}

// ============================================================================
// FileTable Implementation for OpLogFile (static Parquet files)
// ============================================================================

/// Implementation of FileTable for OpLogFile (static Parquet files)
#[async_trait]
impl FileTable for OpLogFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        create_parquet_stream(self, "OpLogFile").await
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        extract_parquet_schema(self, "OpLogFile").await
    }
    
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        extract_parquet_statistics(self, "OpLogFile").await
    }
}

// ============================================================================
// FileTable Implementation for SqlDerivedFile
// ============================================================================

/// Implementation of FileTable for SqlDerivedFile (dynamic SQL-generated files)
#[async_trait]
impl FileTable for SqlDerivedFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        create_parquet_stream(self, "SqlDerivedFile").await
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        // TODO: Implement proper schema detection for SqlDerivedFile
        // For now, extract from generated Parquet file
        extract_parquet_schema(self, "SqlDerivedFile").await
    }
    
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        extract_parquet_statistics(self, "SqlDerivedFile").await
    }
}

// ============================================================================
// Generic FileTable Implementation for any File Handle
// ============================================================================

/// Generic FileTable implementation that works with any FileHandle  
/// This provides a fallback when we can't detect specific file types
pub struct GenericFileTable {
    file_handle: tinyfs::Pathed<tinyfs::FileHandle>,
}

impl GenericFileTable {
    pub fn new(file_handle: tinyfs::Pathed<tinyfs::FileHandle>) -> Self {
        Self { file_handle }
    }
}

#[async_trait]
impl FileTable for GenericFileTable {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        create_parquet_stream(self, "GenericFileTable").await
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        extract_parquet_schema(self, "GenericFileTable").await
    }
    
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        extract_parquet_statistics(self, "GenericFileTable").await
    }
}

// ============================================================================
// Table-to-File Adapter: Generic conversion from FileTable to File
// ============================================================================

/// Adapter that converts any FileTable implementation into a File implementation
/// by materializing the RecordBatch stream as Parquet bytes on-demand.
/// 
/// This enables table-like files (like SqlDerivedFile) to provide File semantics
/// without implementing complex async reader logic - they just need to provide
/// RecordBatch streams and this adapter handles the file conversion.
pub struct TableFileAdapter<T: FileTable> {
    file_table: Arc<T>,
    cached_parquet: tokio::sync::OnceCell<Vec<u8>>,
}

impl<T: FileTable> TableFileAdapter<T> {
    pub fn new(file_table: Arc<T>) -> Self {
        Self {
            file_table,
            cached_parquet: tokio::sync::OnceCell::new(),
        }
    }
    
    /// Convert RecordBatch stream to Parquet bytes
    async fn materialize_as_parquet(&self) -> Result<Vec<u8>, TLogFSError> {
        // Get the RecordBatch stream from the FileTable
        let mut stream = self.file_table.record_batch_stream().await?;
        
        // Collect all batches
        let mut batches = Vec::new();
        let mut schema = None;
        
        use futures::StreamExt; // Import here where it's used
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| TLogFSError::ArrowMessage(format!("Stream error: {}", e)))?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }
        
        let schema = schema.ok_or_else(|| TLogFSError::ArrowMessage("No schema available - empty stream".to_string()))?;
        
        // Convert to Parquet bytes
        let mut buffer = Vec::new();
        {
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet writer: {}", e)))?;
            
            for batch in batches {
                writer.write(&batch)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to write batch: {}", e)))?;
            }
            
            writer.close()
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to close Parquet writer: {}", e)))?;
        }
        
        Ok(buffer)
    }
}

/// Async reader that provides access to materialized Parquet bytes
struct ParquetBytesReader {
    cursor: Cursor<Vec<u8>>,
}

impl ParquetBytesReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            cursor: Cursor::new(bytes),
        }
    }
}

impl AsyncRead for ParquetBytesReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.cursor).poll_read(cx, buf)
    }
}

impl AsyncSeek for ParquetBytesReader {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.cursor).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<u64>> {
        Pin::new(&mut self.cursor).poll_complete(cx)
    }
}

#[async_trait]
impl<T: FileTable + 'static> tinyfs::File for TableFileAdapter<T> {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        // Get or materialize the Parquet bytes
        let bytes = self.cached_parquet.get_or_try_init(|| self.materialize_as_parquet()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to materialize table as Parquet: {}", e)))?;
        
        // Create a reader for the bytes
        let reader = ParquetBytesReader::new(bytes.clone());
        Ok(Box::pin(reader))
    }
    
    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        // Table-like files are typically read-only
        Err(tinyfs::Error::Other("TableFileAdapter files are read-only".to_string()))
    }
}

#[async_trait]
impl<T: FileTable + 'static> tinyfs::Metadata for TableFileAdapter<T> {
    async fn metadata(&self) -> tinyfs::Result<tinyfs::NodeMetadata> {
        // For now, provide basic metadata
        // TODO: Extract actual metadata from the FileTable
        Ok(tinyfs::NodeMetadata {
            version: 1,
            size: None, // Size is dynamic for generated content
            sha256: None, // Cannot pre-compute for dynamic content
            entry_type: tinyfs::EntryType::FileTable,
            timestamp: chrono::Utc::now().timestamp(),
        })
    }
}