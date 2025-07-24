use crate::schema::ForArrow;
use crate::delta::DeltaTableManager;
use crate::OplogEntry;
use crate::error::TLogFSError;
use arrow::datatypes::{SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tinyfs::EntryType;
use diagnostics;

// DataFusion imports for table providers
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;

/// Table for querying filesystem metadata (OplogEntry records) without IPC deserialization
/// 
/// This table provides direct access to OplogEntry metadata stored in Delta Lake.
/// Unlike OperationsTable, it does NOT attempt to deserialize the content field,
/// making it suitable for metadata queries on all entry types.
/// 
/// Architecture:
/// - **Metadata Only**: Queries OplogEntry fields (node_id, file_type, version, etc.) 
/// - **No Content Deserialization**: Does not access the content field, avoiding IPC issues
/// - **Universal**: Works for files, directories, and symlinks without type-specific handling
/// 
/// Use cases:
/// - Finding FileSeries entries for a specific node_id
/// - Temporal filtering using min/max_event_time
/// - Version discovery and metadata queries
/// - Path resolution for SeriesTable creation
#[derive(Debug, Clone)]
pub struct MetadataTable {
    delta_manager: DeltaTableManager,
    table_path: String,
    schema: SchemaRef,
}

impl MetadataTable {
    /// Create a new MetadataTable for querying OplogEntry metadata
    pub fn new(table_path: String, delta_manager: DeltaTableManager) -> Self {
        // Use OplogEntry schema but exclude the content field to avoid deserialization issues
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        Self { 
            delta_manager,
            table_path,
            schema,
        }
    }

    /// Query OplogEntry records for a specific node_id and file_type
    pub async fn query_records_for_node(&self, node_id: &str, file_type: EntryType) -> Result<Vec<OplogEntry>, TLogFSError> {
        diagnostics::log_debug!("MetadataTable::query_records_for_node - node_id: {node_id}, file_type: {file_type:?}", 
            node_id: node_id, file_type: format!("{:?}", file_type));

        // Get the Delta table directly
        let table = self.delta_manager.get_table_for_read(&self.table_path).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get Delta table: {}", e)))?;

        // Use DataFusion SQL to query the table directly
        // This bypasses the content field deserialization completely
        use datafusion::execution::context::SessionContext;
        use datafusion::datasource::provider::TableProvider as _;
        
        let ctx = SessionContext::new();
        
        // Register the Delta table as a DataFusion table
        let table_provider = deltalake::datafusion::DeltaTableProvider::try_new(table, datafusion::logical_expr::LogicalPlanBuilder::default())
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create table provider: {}", e)))?;

        ctx.register_table("metadata", Arc::new(table_provider))
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table: {}", e)))?;

        // Build SQL query to find matching records
        let file_type_str = format!("{:?}", file_type);
        let sql = format!(
            "SELECT part_id, node_id, file_type, timestamp, version, sha256, size, min_event_time, max_event_time, extended_attributes 
             FROM metadata 
             WHERE node_id = '{}' AND file_type = '{}'",
            node_id.replace("'", "''"), // Basic SQL injection protection
            file_type_str.replace("'", "''")
        );

        diagnostics::log_debug!("MetadataTable executing SQL: {sql}", sql: &sql);

        let df = ctx.sql(&sql).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("SQL query failed: {}", e)))?;

        let results = df.collect().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect results: {}", e)))?;

        // Convert results to OplogEntry records (without content field)
        let mut entries = Vec::new();
        for batch in results {
            for row_idx in 0..batch.num_rows() {
                if let Ok(entry) = self.record_batch_to_oplog_entry(&batch, row_idx) {
                    entries.push(entry);
                }
            }
        }

        let entry_count = entries.len();
        diagnostics::log_debug!("MetadataTable found {entry_count} entries", entry_count: entry_count);

        Ok(entries)
    }

    /// Query OplogEntry records with temporal filtering
    pub async fn query_records_with_temporal_filter(
        &self,
        node_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<OplogEntry>, TLogFSError> {
        diagnostics::log_debug!("MetadataTable::query_records_with_temporal_filter - node_id: {node_id}, start: {start_time}, end: {end_time}", 
            node_id: node_id, start_time: start_time, end_time: end_time);

        // Get all FileSeries records for the node first
        let all_records = self.query_records_for_node(node_id, EntryType::FileSeries).await?;
        
        // Apply temporal filtering
        let mut filtered_records = Vec::new();
        for record in all_records {
            if let Some((min_time, max_time)) = record.temporal_range() {
                // Check for overlap: file overlaps if max_file >= start_query AND min_file <= end_query
                if max_time >= start_time && min_time <= end_time {
                    filtered_records.push(record);
                }
            }
        }
        
        // Sort by min_event_time for optimal processing order
        filtered_records.sort_by_key(|r| r.min_event_time.unwrap_or(0));

        let filtered_count = filtered_records.len();
        diagnostics::log_debug!("MetadataTable temporal filter found {filtered_count} overlapping entries", filtered_count: filtered_count);
        
        Ok(filtered_records)
    }

    /// Convert a RecordBatch row to OplogEntry (metadata only, no content)
    fn record_batch_to_oplog_entry(&self, batch: &RecordBatch, row_idx: usize) -> Result<OplogEntry, TLogFSError> {
        use arrow::array::{Array, StringArray, Int64Array, UInt64Array};

        // Extract fields from the record batch
        let part_id = batch.column_by_name("part_id")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx).to_string()) } else { None })
            .unwrap_or_default();

        let node_id = batch.column_by_name("node_id")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx).to_string()) } else { None })
            .unwrap_or_default();

        let file_type_str = batch.column_by_name("file_type")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx)) } else { None })
            .unwrap_or("File");

        let file_type = match file_type_str {
            "Directory" => EntryType::Directory,
            "FileSeries" => EntryType::FileSeries,
            "Symlink" => EntryType::Symlink,
            _ => EntryType::File,
        };

        let timestamp = batch.column_by_name("timestamp")
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx)) } else { None })
            .unwrap_or(0);

        let version = batch.column_by_name("version")
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx)) } else { None })
            .unwrap_or(1);

        let sha256 = batch.column_by_name("sha256")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx).to_string()) } else { None });

        let size = batch.column_by_name("size")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx)) } else { None });

        let min_event_time = batch.column_by_name("min_event_time")
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx)) } else { None });

        let max_event_time = batch.column_by_name("max_event_time")
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx)) } else { None });

        let extended_attributes = batch.column_by_name("extended_attributes")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| if arr.is_valid(row_idx) { Some(arr.value(row_idx).to_string()) } else { None });

        Ok(OplogEntry {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content: None, // Explicitly do not load content to avoid IPC issues
            sha256,
            size,
            min_event_time,
            max_event_time,
            extended_attributes,
        })
    }
}

#[async_trait]
impl TableProvider for MetadataTable {
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // For now, delegate to the Delta table directly
        // This implementation can be enhanced with proper filtering later
        Err(datafusion::error::DataFusionError::NotImplemented(
            "MetadataTable scan not yet implemented - use query methods instead".to_string()
        ))
    }
}
