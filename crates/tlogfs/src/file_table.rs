/// File-Table Duality Integration for TinyFS and DataFusion
/// 
/// This module implements the FileTable trait that allows structured files
/// to expose both file-oriented and table-oriented interfaces.

use async_stream::stream;
use crate::error::TLogFSError;
use crate::file::OpLogFile; // Import the file types we'll implement FileTable for
use crate::sql_derived::SqlDerivedFile;
use crate::tinyfs_object_store::TinyFsObjectStore;
use arrow::datatypes::{SchemaRef, DataType, TimeUnit};
use arrow::array::RecordBatch;
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
use datafusion::common::{Result as DataFusionResult, Constraints, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{TableProviderFilterPushDown, Expr};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl, ListingOptions};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{Statistics, PlanProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter; // Add correct import
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::logical_expr::Operator;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::common::DataFusionError;

// Removed steward import - TLogFS can't depend on Steward (circular dependency)
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

/// Simple in-memory table provider for Parquet data
pub struct InMemoryTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

/// Wrapper that applies temporal filtering to a ListingTable
pub struct TemporalFilteredListingTable {
    listing_table: ListingTable,
    min_time: i64,
    max_time: i64,
}

impl TemporalFilteredListingTable {
    pub fn new(listing_table: ListingTable, min_time: i64, max_time: i64) -> Self {
        Self {
            listing_table,
            min_time,
            max_time,
        }
    }
    
    fn apply_temporal_filter_to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        min_seconds: i64,
        max_seconds: i64,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = plan.schema();
        
        // Find timestamp column index
        let timestamp_col_index = schema
            .fields()
            .iter()
            .position(|f| f.name() == "timestamp")
            .ok_or_else(|| {
                DataFusionError::Plan("Timestamp column not found in schema".to_string())
            })?;

        // Check the timestamp column's timezone
        let timestamp_field = &schema.fields()[timestamp_col_index];
        let timestamp_timezone = match timestamp_field.data_type() {
            DataType::Timestamp(TimeUnit::Second, tz) => tz.clone(),
            _ => return Err(DataFusionError::Plan("Expected timestamp column with second precision".to_string())),
        };

        // Create temporal bound expressions with matching timezone
        let min_timestamp = Arc::new(Literal::new(ScalarValue::TimestampSecond(
            Some(min_seconds),
            timestamp_timezone.clone(),
        )));
        
        let max_timestamp = Arc::new(Literal::new(ScalarValue::TimestampSecond(
            Some(max_seconds),
            timestamp_timezone,
        )));

        // Create column reference for timestamp
        let timestamp_col = Arc::new(Column::new("timestamp", timestamp_col_index));

        // Create filter expressions: timestamp >= min_timestamp AND timestamp <= max_timestamp
        let min_filter = Arc::new(BinaryExpr::new(
            timestamp_col.clone(),
            Operator::GtEq,
            min_timestamp,
        ));

        let max_filter = Arc::new(BinaryExpr::new(
            timestamp_col,
            Operator::LtEq,
            max_timestamp,
        ));

        let combined_filter = Arc::new(BinaryExpr::new(
            min_filter,
            Operator::And,
            max_filter,
        ));

        // Apply filter to the plan
        let filter_exec = FilterExec::try_new(combined_filter, plan)?;
        Ok(Arc::new(filter_exec))
    }
}

impl std::fmt::Debug for TemporalFilteredListingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalFilteredListingTable")
            .field("listing_table", &"<ListingTable>")
            .field("min_time", &self.min_time)
            .field("max_time", &self.max_time)
            .field("session_context", &"<SessionContext>")
            .finish()
    }
}

#[async_trait]
impl TableProvider for TemporalFilteredListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = self.listing_table.schema();
        let field_count = schema.fields().len();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let field_names_str = field_names.join(", ");
        diagnostics::log_debug!("🔍 TemporalFilteredListingTable.schema() called - returning {count} fields: [{names}]", 
            count: field_count, names: field_names_str);
        schema
    }

    fn table_type(&self) -> TableType {
        self.listing_table.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.listing_table.constraints()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.listing_table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        diagnostics::log_debug!("🚨 TemporalFilteredListingTable.scan() called - temporal filtering is active!");
        
        // Convert from milliseconds to seconds for HydroVu data
        let min_seconds = self.min_time / 1000;
        let max_seconds = self.max_time / 1000;
        
        diagnostics::log_debug!("⚡ Temporal filtering range: {min_seconds} to {max_seconds} (seconds)", min_seconds: min_seconds, max_seconds: max_seconds);
        
        // Use the provided session state (not our internal one) to avoid schema mismatches
        diagnostics::log_debug!("🔍 Using provided SessionState instead of internal one...");
        
        // Check if we actually need to apply temporal filtering
        if self.min_time == i64::MIN && self.max_time == i64::MAX {
            diagnostics::log_debug!("⚡ No temporal bounds - delegating to base ListingTable");
            return self.listing_table.scan(state, projection, filters, limit).await;
        }
        
        // Check if this is an empty projection (COUNT case)
        let is_empty_projection = projection.as_ref().map_or(false, |p| p.is_empty());
        
        if is_empty_projection {
            diagnostics::log_debug!("📊 Empty projection detected (COUNT query) - need to include timestamp for filtering");
            
            // For temporal filtering with empty projection, we need to:
            // 1. Scan with timestamp column included
            // 2. Apply temporal filter  
            // 3. Project back to empty schema
            
            // Find timestamp column index in the full schema
            let full_schema = self.listing_table.schema();
            let timestamp_col_index = full_schema
                .fields()
                .iter()
                .position(|f| f.name() == "timestamp")
                .ok_or_else(|| {
                    DataFusionError::Plan("No 'timestamp' field found in schema for temporal filtering".to_string())
                })?;
            
            diagnostics::log_debug!("🔍 Found timestamp column at index {index}", index: timestamp_col_index);
            
            // Scan with timestamp column included
            let timestamp_projection = vec![timestamp_col_index];
            let base_plan = self.listing_table.scan(state, Some(&timestamp_projection), filters, limit).await?;
            
            // Apply temporal filtering
            let filtered_plan = self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;
            
            // Project back to empty schema for COUNT
            let empty_projection: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
            let projection_exec = ProjectionExec::try_new(empty_projection, filtered_plan)?;
            
            diagnostics::log_debug!("✅ Temporal filtering applied successfully for COUNT query");
            return Ok(Arc::new(projection_exec));
        }
        
        // For non-empty projections, proceed normally but ensure timestamp is included if needed
        let base_plan = self.listing_table.scan(state, projection, filters, limit).await?;
        
        // Apply temporal filtering to the base plan
        let filtered_plan = self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;
        
        diagnostics::log_debug!("✅ Temporal filtering applied successfully");
        Ok(filtered_plan)
    }
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

impl InMemoryTableProvider {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl std::fmt::Debug for InMemoryTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryTableProvider")
            .field("schema", &self.schema)
            .field("num_batches", &self.batches.len())
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let full_schema = self.schema();
        
        // Apply projection to schema if specified
        let projected_schema = match projection {
            Some(indices) if indices.is_empty() => {
                // Empty projection for count(*) - create schema with no fields
                Arc::new(arrow::datatypes::Schema::empty())
            },
            Some(indices) => {
                // Project specific columns
                let projected_fields: Vec<_> = indices.iter()
                    .map(|&i| full_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(projected_fields))
            },
            None => {
                // No projection specified - use full schema
                full_schema
            }
        };
        
        // Create the execution plan with the projected schema
        Ok(Arc::new(StreamExecutionPlan::new(
            Arc::clone(&self.file_table), 
            projected_schema,
            projection.cloned()
        )))
    }
}

/// ExecutionPlan that wraps a FileTable for multiple executions
pub struct StreamExecutionPlan {
    file_table: Arc<dyn FileTable>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl StreamExecutionPlan {
    pub fn new(file_table: Arc<dyn FileTable>, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        
        Self { file_table, schema, projection, properties }
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
        if partition != 0 {
            return Err(datafusion::common::DataFusionError::Internal(format!(
                "Invalid partition: {}, only partition 0 is supported", partition
            )));
        }
        
        // Create a new stream each time execute() is called
        // This is required to support multiple executions from DataFusion
        let file_table = Arc::clone(&self.file_table);
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        
        // We need to create the stream asynchronously, so we use a RecordBatchStreamAdapter
        // that wraps a future-generated stream
        let stream = stream! {
            match file_table.record_batch_stream().await {
                Ok(mut stream) => {
                    // Forward all batches from the FileTable stream, applying projection
                    use futures::StreamExt;
                    while let Some(batch_result) = stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                match apply_projection_to_batch(&batch, &projection) {
                                    Ok(projected_batch) => yield Ok(projected_batch),
                                    Err(e) => yield Err(e),
                                }
                            },
                            Err(e) => yield Err(e),
                        }
                    }
                },
                Err(e) => {
                    // Convert TLogFSError to DataFusionError
                    yield Err(datafusion::common::DataFusionError::External(Box::new(e)));
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// Helper function to create a table provider from a TinyFS path using DataFusion ListingTable  
pub async fn create_table_provider_from_path(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    persistence_state: crate::persistence::State,
    ctx: &SessionContext,
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
            
            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;
            
            match metadata.entry_type {
                tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                    // Use DataFusion ListingTable approach - no more GenericFileTable duplication!
                    create_listing_table_provider(tinyfs_wd, file_handle, persistence_state, ctx).await
                },
                _ => {
                    return Err(TLogFSError::ArrowMessage(
                        format!("Path {} points to unsupported entry type for table operations: {:?}", path, metadata.entry_type)
                    ))
                }
            }
        },
        Lookup::NotFound(full_path, _) => {
            Err(TLogFSError::ArrowMessage(format!("File not found: {}", full_path.display())))
        },
        Lookup::Empty(_) => {
            Err(TLogFSError::ArrowMessage("Empty path provided".to_string()))
        }
    }
}

// Create a ListingTable provider using the same pattern as SQL-derived factory
async fn create_listing_table_provider(
    _tinyfs_wd: &tinyfs::WD,
    file_handle: tinyfs::Pathed<tinyfs::FileHandle>,
    persistence_state: crate::persistence::State,
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // Use the provided SessionContext and persistence state - no recreation!
    diagnostics::log_debug!("create_listing_table_provider called");
    
        // Create TinyFS ObjectStore using the provided persistence state
    let object_store = Arc::new(TinyFsObjectStore::new(persistence_state.clone()));
    
    // Register ObjectStore with the provided DataFusion context
    let url = url::Url::parse("tinyfs:///")
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse tinyfs URL: {}", e)))?;
    ctx.runtime_env()
        .object_store_registry
        .register_store(&url, object_store.clone());
    
    // Extract node_id and part_id from the OpLogFile
    let file_arc = file_handle.handle.get_file().await;
    let (node_id, part_id) = {
        let file_guard = file_arc.lock().await;
        let file_any = file_guard.as_any();
        let oplog_file = file_any.downcast_ref::<OpLogFile>()
            .ok_or_else(|| TLogFSError::ArrowMessage("FileHandle is not an OpLogFile".to_string()))?;
        (oplog_file.get_node_id(), oplog_file.get_part_id())
    };
    
    // Get temporal overrides from the current version of this FileSeries
    let temporal_overrides = get_temporal_overrides_for_node_id(&persistence_state, &node_id).await?;
    
    // Debug temporal overrides lookup result
    if let Some((min_time_ms, max_time_ms)) = temporal_overrides {
        diagnostics::log_debug!("Found temporal overrides for node {node_id}: {min_time_ms} to {max_time_ms}", 
            node_id: node_id, min_time_ms: min_time_ms, max_time_ms: max_time_ms);
    } else {
        diagnostics::log_debug!("No temporal overrides found for node {node_id}", node_id: node_id);
    }
    
    // Register all versions of this file with ObjectStore using the existing persistence
    object_store.register_file_versions(node_id, part_id).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register file versions: {}", e)))?;
    
    // Create ListingTable URL pointing to all versions of this node
    let table_url = ListingTableUrl::parse(&format!("tinyfs:///node/{}/version/", node_id))
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse table URL: {}", e)))?;
    
    // Create ListingTable configuration with Parquet format
    let file_format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(file_format);
    let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);
    
    // Use DataFusion's schema inference - this will automatically:
    // 1. Iterate through all versions of the file
    // 2. Skip 0-byte files (temporal override metadata-only versions)
    // 3. Merge schemas from all valid Parquet versions
    // 4. Provide the unified schema
    let config_with_schema = config.infer_schema(&ctx.state()).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Schema inference failed: {}", e)))?;
    
    // Create the ListingTable - DataFusion handles all the complexity!
    let listing_table = ListingTable::try_new(config_with_schema)
        .map_err(|e| TLogFSError::ArrowMessage(format!("ListingTable creation failed: {}", e)))?;
    
    // ALWAYS apply temporal filtering for FileSeries (use i64::MIN/MAX if no overrides)
    let (min_time, max_time) = temporal_overrides.unwrap_or((i64::MIN, i64::MAX));
    diagnostics::log_debug!("Creating TemporalFilteredListingTable with bounds: {min_time} to {max_time}", min_time: min_time, max_time: max_time);
    
    if temporal_overrides.is_some() {
        diagnostics::log_debug!("⚠️ TEMPORAL OVERRIDES FOUND - creating TemporalFilteredListingTable wrapper");
        Ok(Arc::new(TemporalFilteredListingTable::new(listing_table, min_time, max_time)))
    } else {
        diagnostics::log_debug!("⚠️ NO TEMPORAL OVERRIDES - returning raw ListingTable (no filtering!)");
        Ok(Arc::new(listing_table))
    }
}

/// Get temporal overrides from the current version of a FileSeries by node_id
async fn get_temporal_overrides_for_node_id(
    persistence_state: &crate::persistence::State,
    node_id: &tinyfs::NodeID,
) -> Result<Option<(i64, i64)>, TLogFSError> {
    use crate::query::NodeTable;
    use tinyfs::EntryType;
    
    // Create a metadata table to query the current version
    let table = persistence_state.table().await?
        .ok_or_else(|| TLogFSError::ArrowMessage("No Delta table available".to_string()))?;
    let node_table = NodeTable::new(table);
    
    // Query for all versions of this FileSeries
    let node_id_str = node_id.to_hex_string();
    let all_records = node_table.query_records_for_node(&node_id_str, EntryType::FileSeries).await?;
    
    // Find the current version (highest version number)
    if let Some(current_version) = all_records.iter().max_by_key(|r| r.version) {
        return Ok(current_version.temporal_overrides());
    }
    
    Ok(None)
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

// GenericFileTable FileTableReader implementation removed - using ListingTable instead

/// Common function to create a record batch stream from any FileTable reader
/// Eliminates duplication across all FileTable implementations
async fn create_parquet_stream<T: FileTableReader>(
    file_table: &T,
    context: &str,
) -> Result<SendableRecordBatchStream, TLogFSError> {
    create_parquet_stream_with_temporal_filter(file_table, context, None).await
}

/// Create a parquet stream with optional temporal filtering using predicate pushdown
async fn create_parquet_stream_with_temporal_filter<T: FileTableReader>(
    file_table: &T,
    context: &str,
    temporal_overrides: Option<(i64, i64)>, // (min_timestamp_ms, max_timestamp_ms)
) -> Result<SendableRecordBatchStream, TLogFSError> {
    let reader = file_table.get_reader().await?;
    let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet stream for {}: {}", context, e)))?;

    // TODO: Apply temporal filtering at Parquet reader level for better performance
    // For now, temporal filtering is applied post-read in the stream
        
    let stream = stream_builder.build()
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet stream for {}: {}", context, e)))?;
        
    // If we have temporal overrides, apply additional filtering to the stream
    if let Some((min_timestamp_ms, max_timestamp_ms)) = temporal_overrides {
        // Get schema before moving stream
        let schema = stream.schema().clone();
        
        // Create a filtered stream that applies temporal bounds
        let filtered_stream = stream.filter_map(move |batch_result| async move {
            match batch_result {
                Ok(batch) => {
                    match apply_temporal_filter_to_batch(&batch, min_timestamp_ms, max_timestamp_ms) {
                        Ok(Some(filtered_batch)) => Some(Ok(filtered_batch)),
                        Ok(None) => None, // Batch was completely filtered out
                        Err(e) => Some(Err(datafusion::error::DataFusionError::External(Box::new(e)))),
                    }
                }
                Err(e) => Some(Err(datafusion::error::DataFusionError::ParquetError(e))),
            }
        });
        let adapted_stream = RecordBatchStreamAdapter::new(
            schema,
            filtered_stream
        );
        Ok(Box::pin(adapted_stream))
    } else {
        // No temporal filtering - use original stream
        let schema = stream.schema().clone();
        let adapted_stream = RecordBatchStreamAdapter::new(
            schema,
            stream.map(|batch_result| batch_result.map_err(datafusion::error::DataFusionError::from))
        );
        Ok(Box::pin(adapted_stream))
    }
}

/// Common function to extract schema from any FileTable reader
/// Eliminates duplication across all FileTable implementations
async fn extract_parquet_schema<T: FileTableReader>(
    file_table: &T,
    context: &str,
) -> Result<SchemaRef, TLogFSError> {
    let mut reader = file_table.get_reader().await?;
    
    // Check file size by seeking to end
    use tokio::io::AsyncSeekExt;
    let file_size = reader.seek(std::io::SeekFrom::End(0)).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get file size for {}: {}", context, e)))?;
    
    // Reset position to beginning
    reader.seek(std::io::SeekFrom::Start(0)).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to reset file position for {}: {}", context, e)))?;
    
    // Handle empty files (temporal overrides create 0-byte files)
    if file_size == 0 {
        return Err(TLogFSError::ArrowMessage(format!(
            "Cannot read Parquet schema from empty file in {}: file size is 0 bytes (likely a temporal override metadata-only version)", 
            context
        )));
    }
    
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
        // Execute SQL query directly and return RecordBatch stream - no Parquet round-trip
        self.execute_query_to_record_batch_stream().await
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        // Execute SQL query and get schema directly from the result
        self.execute_query_to_schema().await
    }
    
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        // For now, return default statistics. TODO: Extract from query execution
        let schema = self.execute_query_to_schema().await?;
        Ok(Statistics::new_unknown(&schema))
    }
}

// GenericFileTable removed - using DataFusion ListingTable instead to eliminate duplication

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
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
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

/// Apply temporal filtering to a RecordBatch based on timestamp column
fn apply_temporal_filter_to_batch(
    batch: &RecordBatch,
    min_timestamp_ms: i64,
    max_timestamp_ms: i64,
) -> Result<Option<RecordBatch>, TLogFSError> {
    use arrow::array::{Array, BooleanArray, TimestampSecondArray, TimestampMillisecondArray, Int64Array};
    use arrow::compute;
    
    // Find the timestamp column
    let timestamp_col_idx = batch.schema().index_of("timestamp")
        .map_err(|_| TLogFSError::ArrowMessage("No timestamp column found for temporal filtering".to_string()))?;
        
    let timestamp_column = batch.column(timestamp_col_idx);
    
    // Create boolean mask based on timestamp column type
    let filter_mask = if let Some(ts_array) = timestamp_column.as_any().downcast_ref::<TimestampSecondArray>() {
        // Convert second timestamps to milliseconds for comparison
        let mut mask = Vec::with_capacity(ts_array.len());
        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                mask.push(false);
            } else {
                let ts_ms = ts_array.value(i) * 1000; // Convert seconds to milliseconds
                mask.push(ts_ms >= min_timestamp_ms && ts_ms <= max_timestamp_ms);
            }
        }
        BooleanArray::from(mask)
    } else if let Some(ts_array) = timestamp_column.as_any().downcast_ref::<TimestampMillisecondArray>() {
        // Millisecond timestamps - direct comparison
        let mut mask = Vec::with_capacity(ts_array.len());
        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                mask.push(false);
            } else {
                let ts_ms = ts_array.value(i);
                mask.push(ts_ms >= min_timestamp_ms && ts_ms <= max_timestamp_ms);
            }
        }
        BooleanArray::from(mask)
    } else if let Some(int64_array) = timestamp_column.as_any().downcast_ref::<Int64Array>() {
        // Int64 timestamps - assume milliseconds
        let mut mask = Vec::with_capacity(int64_array.len());
        for i in 0..int64_array.len() {
            if int64_array.is_null(i) {
                mask.push(false);
            } else {
                let ts_ms = int64_array.value(i);
                mask.push(ts_ms >= min_timestamp_ms && ts_ms <= max_timestamp_ms);
            }
        }
        BooleanArray::from(mask)
    } else {
        return Err(TLogFSError::ArrowMessage(format!(
            "Unsupported timestamp column type for temporal filtering: {:?}", 
            timestamp_column.data_type()
        )));
    };
    
    // Check if any rows passed the filter
    if filter_mask.true_count() == 0 {
        return Ok(None); // Entire batch filtered out
    }
    
    // Apply filter to all columns
    let filtered_columns: Result<Vec<_>, _> = batch.columns()
        .iter()
        .map(|col| compute::filter(col, &filter_mask))
        .collect();
        
    let filtered_columns = filtered_columns
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to apply temporal filter: {}", e)))?;
        
    let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create filtered batch: {}", e)))?;
        
    Ok(Some(filtered_batch))
}

/// Apply column projection to a RecordBatch
fn apply_projection_to_batch(
    batch: &arrow::array::RecordBatch, 
    projection: &Option<Vec<usize>>
) -> datafusion::common::Result<arrow::array::RecordBatch> {
    use arrow::array::RecordBatch;
    
    match projection {
        Some(indices) if indices.is_empty() => {
            // Empty projection for count(*) - return batch with no columns but same row count
            let empty_schema = Arc::new(arrow::datatypes::Schema::empty());
            RecordBatch::try_new_with_options(
                empty_schema, 
                vec![], 
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()))
            )
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))
        },
        Some(indices) => {
            // Project specific columns
            let projected_columns: Vec<_> = indices.iter()
                .map(|&i| batch.column(i).clone())
                .collect();
            
            let projected_fields: Vec<_> = indices.iter()
                .map(|&i| batch.schema().field(i).clone())
                .collect();
            
            let projected_schema = Arc::new(arrow::datatypes::Schema::new(projected_fields));
            
            RecordBatch::try_new(projected_schema, projected_columns)
                .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))
        },
        None => {
            // No projection - return original batch
            Ok(batch.clone())
        }
    }
}

// ============================================================================
// FileHandle Adapter - Implements FileTable for any FileHandle
// ============================================================================


