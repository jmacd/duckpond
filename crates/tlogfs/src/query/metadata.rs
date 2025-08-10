use crate::schema::ForArrow;
use crate::delta::DeltaTableManager;
use crate::OplogEntry;
use crate::error::TLogFSError;
use arrow::datatypes::{SchemaRef};
use arrow::array::Array; // For null checking
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
        let file_type_debug = format!("{:?}", file_type);
        diagnostics::log_debug!("MetadataTable::query_records_for_node - node_id: {node_id}, file_type: {file_type}", 
            node_id: node_id, file_type: file_type_debug);

        // Get the Delta table and use DeltaOps to load data
        let table = self.delta_manager.get_table_for_read(&self.table_path).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get Delta table: {}", e)))?;

        // Use DeltaOps to load data from the table
        let ops = deltalake::DeltaOps::from(table);
        let (_table, stream) = ops.load().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to load Delta table data: {}", e)))?;

        // Collect all record batches
        let batches = deltalake::operations::collect_sendable_stream(stream).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect Delta table stream: {}", e)))?;

        let batch_count = batches.len();
        diagnostics::log_debug!("MetadataTable::query_records_for_node - collected {batch_count} batches", 
            batch_count: batch_count);

        // Convert record batches to OplogEntry records, filtering by node_id and file_type
        let mut results = Vec::new();
        
        for batch in batches {
            // Debug: Log the schema and check for temporal columns
            let schema = batch.schema();
            let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let columns_debug = format!("{:?}", column_names);
            diagnostics::log_debug!("MetadataTable batch schema columns: {columns}", columns: columns_debug);
            
            // Check if temporal columns exist
            let has_min_event_time = batch.column_by_name("min_event_time").is_some();
            let has_max_event_time = batch.column_by_name("max_event_time").is_some();
            diagnostics::log_debug!("MetadataTable temporal columns - min_event_time: {min_exists}, max_event_time: {max_exists}", 
                min_exists: has_min_event_time, max_exists: has_max_event_time);
            
            // Get column arrays
            let part_id_column = batch.column_by_name("part_id")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing part_id column".to_string()))?;
            
            // Handle Dictionary arrays by casting to string
            let part_id_array = if let Some(dict_array) = part_id_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
                // Cast dictionary to string array
                arrow::compute::cast(dict_array, &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to cast part_id dictionary to string: {}", e)))?
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| TLogFSError::ArrowMessage("Failed to convert part_id to StringArray after cast".to_string()))?
                    .clone()
            } else if let Some(string_array) = part_id_column.as_any().downcast_ref::<arrow::array::StringArray>() {
                string_array.clone()
            } else {
                return Err(TLogFSError::ArrowMessage(format!("part_id column has unsupported type: {:?}", part_id_column.data_type())));
            };

            let node_id_column = batch.column_by_name("node_id")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing node_id column".to_string()))?;
            
            // Handle Dictionary arrays by casting to string
            let node_id_array = if let Some(dict_array) = node_id_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
                // Cast dictionary to string array
                arrow::compute::cast(dict_array, &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to cast node_id dictionary to string: {}", e)))?
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| TLogFSError::ArrowMessage("Failed to convert node_id to StringArray after cast".to_string()))?
                    .clone()
            } else if let Some(string_array) = node_id_column.as_any().downcast_ref::<arrow::array::StringArray>() {
                string_array.clone()
            } else {
                return Err(TLogFSError::ArrowMessage(format!("node_id column has unsupported type: {:?}", node_id_column.data_type())));
            };

            let file_type_column = batch.column_by_name("file_type")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing file_type column".to_string()))?;
            
            // Handle Dictionary arrays by casting to string
            let file_type_array = if let Some(dict_array) = file_type_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
                // Cast dictionary to string array
                arrow::compute::cast(dict_array, &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to cast file_type dictionary to string: {}", e)))?
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| TLogFSError::ArrowMessage("Failed to convert file_type to StringArray after cast".to_string()))?
                    .clone()
            } else if let Some(string_array) = file_type_column.as_any().downcast_ref::<arrow::array::StringArray>() {
                string_array.clone()
            } else {
                return Err(TLogFSError::ArrowMessage(format!("file_type column has unsupported type: {:?}", file_type_column.data_type())));
            };

            let timestamp_array = batch.column_by_name("timestamp")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing timestamp column".to_string()))?
                .as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| TLogFSError::ArrowMessage("timestamp column is not TimestampMicrosecondArray".to_string()))?;

            let version_array = batch.column_by_name("version")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing version column".to_string()))?
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("version column is not Int64Array".to_string()))?;

            let min_event_time_column = batch.column_by_name("min_event_time")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing min_event_time column".to_string()))?;
            
            // Debug: Check the actual type of min_event_time column
            let min_event_time_type = format!("{:?}", min_event_time_column.data_type());
            diagnostics::log_debug!("MetadataTable min_event_time column type: {column_type}", column_type: min_event_time_type);
            
            let min_event_time_array = min_event_time_column
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("min_event_time column is not Int64Array".to_string()))?;

            let max_event_time_column = batch.column_by_name("max_event_time")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing max_event_time column".to_string()))?;
            
            // Debug: Check the actual type of max_event_time column
            let max_event_time_type = format!("{:?}", max_event_time_column.data_type());
            diagnostics::log_debug!("MetadataTable max_event_time column type: {column_type}", column_type: max_event_time_type);
            
            let max_event_time_array = max_event_time_column
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("max_event_time column is not Int64Array".to_string()))?;

            let sha256_column = batch.column_by_name("sha256")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing sha256 column".to_string()))?;
            
            // Handle Dictionary arrays by casting to string
            let sha256_array = if let Some(dict_array) = sha256_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
                // Cast dictionary to string array
                arrow::compute::cast(dict_array, &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to cast sha256 dictionary to string: {}", e)))?
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| TLogFSError::ArrowMessage("Failed to convert sha256 to StringArray after cast".to_string()))?
                    .clone()
            } else if let Some(string_array) = sha256_column.as_any().downcast_ref::<arrow::array::StringArray>() {
                string_array.clone()
            } else {
                return Err(TLogFSError::ArrowMessage(format!("sha256 column has unsupported type: {:?}", sha256_column.data_type())));
            };

            let size_column = batch.column_by_name("size")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing size column".to_string()))?;
            
            // Handle both Int64 and UInt64 size columns
            let size_array = if let Some(int64_array) = size_column.as_any().downcast_ref::<arrow::array::Int64Array>() {
                int64_array.clone()
            } else if let Some(uint64_array) = size_column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                // Convert UInt64 to Int64 (since OplogEntry expects u64 which can be represented as i64 values)
                let values: Vec<Option<i64>> = (0..uint64_array.len())
                    .map(|i| {
                        if uint64_array.is_valid(i) {
                            Some(uint64_array.value(i) as i64)
                        } else {
                            None
                        }
                    })
                    .collect();
                arrow::array::Int64Array::from(values)
            } else {
                return Err(TLogFSError::ArrowMessage(format!("size column has unsupported type: {:?}", size_column.data_type())));
            };

            let extended_attributes_column = batch.column_by_name("extended_attributes")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing extended_attributes column".to_string()))?;
            
            // Handle Dictionary arrays by casting to string
            let extended_attributes_array = if let Some(dict_array) = extended_attributes_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
                // Cast dictionary to string array
                arrow::compute::cast(dict_array, &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to cast extended_attributes dictionary to string: {}", e)))?
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| TLogFSError::ArrowMessage("Failed to convert extended_attributes to StringArray after cast".to_string()))?
                    .clone()
            } else if let Some(string_array) = extended_attributes_column.as_any().downcast_ref::<arrow::array::StringArray>() {
                string_array.clone()
            } else {
                return Err(TLogFSError::ArrowMessage(format!("extended_attributes column has unsupported type: {:?}", extended_attributes_column.data_type())));
            };

            // Convert EntryType to string for comparison
            let target_file_type = match file_type {
                EntryType::FileData => "file:data",
                EntryType::FileTable => "file:table",
                EntryType::FileSeries => "file:series",
                EntryType::Directory => "directory", 
                EntryType::Symlink => "symlink",
            };

            // Process each row
            for row_idx in 0..batch.num_rows() {
                // Check if this row matches our filter criteria
                if node_id_array.value(row_idx) == node_id && 
                   file_type_array.value(row_idx) == target_file_type {
                    
                    let entry = OplogEntry {
                        part_id: part_id_array.value(row_idx).to_string(),
                        node_id: node_id_array.value(row_idx).to_string(),
                        file_type: file_type,
                        timestamp: timestamp_array.value(row_idx),
                        version: version_array.value(row_idx),
                        content: None, // Don't deserialize content for metadata queries
                        sha256: if sha256_array.is_null(row_idx) { None } else { Some(sha256_array.value(row_idx).to_string()) },
                        size: if size_array.is_null(row_idx) { None } else { Some(size_array.value(row_idx)) },
                        min_event_time: if min_event_time_array.is_null(row_idx) { None } else { Some(min_event_time_array.value(row_idx)) },
                        max_event_time: if max_event_time_array.is_null(row_idx) { None } else { Some(max_event_time_array.value(row_idx)) },
                        extended_attributes: if extended_attributes_array.is_null(row_idx) { None } else { Some(extended_attributes_array.value(row_idx).to_string()) },
                        factory: None,
                    };
                    
                    // Debug: Log temporal metadata values
                    let min_time_str = entry.min_event_time.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    let max_time_str = entry.max_event_time.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    diagnostics::log_debug!("MetadataTable entry - version: {version}, min_event_time: {min_time}, max_event_time: {max_time}", 
                        version: entry.version, min_time: min_time_str, max_time: max_time_str);
                    
                    results.push(entry);
                }
            }
        }

        let result_count = results.len();
        diagnostics::log_debug!("MetadataTable::query_records_for_node - found {result_count} matching records", 
            result_count: result_count);
        
        Ok(results)
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
