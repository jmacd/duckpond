use crate::schema::ForArrow;
use crate::OplogEntry;
use crate::error::TLogFSError;
use arrow::datatypes::{SchemaRef, FieldRef};
use arrow::array::Array;
use tinyfs::EntryType;
use std::sync::Arc;
use diagnostics::*;

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
use deltalake::{DeltaTable, DeltaOps};
use std::any::Any;
use std::fmt;

/// Table for querying node metadata (OplogEntry records) without the content column
/// 
/// This table provides a filtered view of OplogEntry metadata stored in Delta Lake,
/// focusing on node-level information like temporal ranges, file types, and versions
/// while excluding the heavy binary content field for performance.
/// 
/// Architecture:
/// - **Filtered View**: Projects all OplogEntry fields EXCEPT the content column
/// - **Performance Optimized**: Avoids transferring large binary data for metadata queries
/// - **Leverages Delta Lake**: Uses DataFusion's built-in Delta integration with proper partition handling
/// - **Universal**: Works for files, directories, and symlinks without type-specific handling
/// 
/// Use cases:
/// - Finding FileSeries entries for temporal overlap detection
/// - Temporal filtering using min/max_event_time and overrides
/// - Version discovery and metadata queries
/// - Node metadata for JOIN operations with DirectoryTable
/// - Any query that needs metadata but not file content
#[derive(Debug, Clone)]
pub struct NodeTable {
    table: DeltaTable,
    schema: SchemaRef,
}

impl NodeTable {
    /// Create a new NodeTable for querying OplogEntry node metadata (without content)
    pub fn new(table: DeltaTable) -> Self {
        // Create schema that excludes the content column
        let full_fields = OplogEntry::for_arrow();
        let filtered_fields: Vec<FieldRef> = full_fields
            .into_iter()
            .filter(|field| field.name() != "content")  // Remove the heavy binary content field
            .collect();
        
        let schema = Arc::new(arrow::datatypes::Schema::new(filtered_fields));
        let field_count = schema.fields().len();
        debug!("NodeTable filtered schema: {field_count} fields (content column excluded)");
        
        Self { 
            table,
            schema,
        }
    }

    /// Query OplogEntry records for a specific node_id and file_type
    pub async fn query_records_for_node(&self, node_id: &str, file_type: EntryType) -> Result<Vec<OplogEntry>, TLogFSError> {
        let file_type_debug = format!("{:?}", file_type);
        debug!("NodeTable::query_records_for_node - node_id: {node_id}, file_type: {file_type_debug}");

        // Use DeltaOps to load data from the table
        let delta_ops = DeltaOps::from(self.table.clone());
        let (_, stream) = delta_ops.load().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to load Delta table data: {}", e)))?;

        // Collect all record batches
        let batches = deltalake::operations::collect_sendable_stream(stream).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect Delta table stream: {}", e)))?;

        let batch_count = batches.len();
        debug!("NodeTable::query_records_for_node - collected {batch_count} batches");

        // Convert record batches to OplogEntry records, filtering by node_id and file_type
        let mut results = Vec::new();
        
        for batch in batches {
            // Debug: Log the schema and check for temporal columns
            let schema = batch.schema();
            let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let columns_debug = format!("{:?}", column_names);
            debug!("NodeTable batch schema columns: {columns_debug}");
            
            // Check if temporal columns exist
            let has_min_event_time = batch.column_by_name("min_event_time").is_some();
            let has_max_event_time = batch.column_by_name("max_event_time").is_some();
            debug!("NodeTable temporal columns - min_event_time: {has_min_event_time}, max_event_time: {has_max_event_time}");
            
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
            debug!("NodeTable min_event_time column type: {min_event_time_type}");
            
            let min_event_time_array = min_event_time_column
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("min_event_time column is not Int64Array".to_string()))?;

            let max_event_time_column = batch.column_by_name("max_event_time")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing max_event_time column".to_string()))?;
            
            // Debug: Check the actual type of max_event_time column
            let max_event_time_type = format!("{:?}", max_event_time_column.data_type());
            debug!("NodeTable max_event_time column type: {max_event_time_type}");
            
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
                        min_override: None, // Not loaded from Delta Lake yet - needs override loading logic
                        max_override: None, // Not loaded from Delta Lake yet - needs override loading logic
                        extended_attributes: if extended_attributes_array.is_null(row_idx) { None } else { Some(extended_attributes_array.value(row_idx).to_string()) },
                        factory: None,
                    };
                    
                    // Debug: Log temporal metadata values
                    let min_time_str = entry.min_event_time.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    let max_time_str = entry.max_event_time.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    log_debug!("NodeTable entry - version: {version}, min_event_time: {min_time}, max_event_time: {max_time}", 
                        version: entry.version, min_time: min_time_str, max_time: max_time_str);
                    
                    results.push(entry);
                }
            }
        }

        let result_count = results.len();
        debug!("NodeTable::query_records_for_node - found {result_count} matching records");
        
        Ok(results)
    }

    /// Query OplogEntry records with temporal filtering
    pub async fn query_records_with_temporal_filter(
        &self,
        node_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<OplogEntry>, TLogFSError> {
        debug!("NodeTable::query_records_with_temporal_filter - node_id: {node_id}, start: {start_time}, end: {end_time}");

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
        debug!("NodeTable temporal filter found {filtered_count} overlapping entries");
        
        Ok(filtered_records)
    }

    /// Query all OplogEntry records for a specific file_type (e.g., all directories)
    pub async fn query_all_by_entry_type(&self, file_type: EntryType) -> Result<Vec<OplogEntry>, TLogFSError> {
        let file_type_debug = format!("{:?}", file_type);
        debug!("NodeTable::query_all_by_entry_type - file_type: {file_type_debug}");

        // Use DeltaOps to load data from the table
        let delta_ops = DeltaOps::from(self.table.clone());
        let (_, stream) = delta_ops.load().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to load Delta table data: {}", e)))?;

        // Collect all record batches
        let batches = deltalake::operations::collect_sendable_stream(stream).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect Delta table stream: {}", e)))?;

        let batch_count = batches.len();
        debug!("NodeTable::query_all_by_entry_type - collected {batch_count} batches");

        // Convert record batches to OplogEntry records, filtering by file_type only
        let mut results = Vec::new();
        
        for batch in batches {
            // Get file_type column and filter
            let file_type_column = batch.column_by_name("file_type")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing file_type column".to_string()))?;
            
            // Handle Dictionary arrays by casting to string
            let file_type_array = if let Some(dict_array) = file_type_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
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

            // Get indices where file_type matches our filter
            let target_file_type = format!("{:?}", file_type);
            let matching_indices: Vec<usize> = (0..file_type_array.len())
                .filter(|&i| {
                    if file_type_array.is_valid(i) {
                        file_type_array.value(i) == target_file_type
                    } else {
                        false
                    }
                })
                .collect();

            if matching_indices.is_empty() {
                continue; // No matches in this batch
            }

            // Extract other columns for the matching rows
            // Similar extraction logic as query_records_for_node but only for matching indices
            let part_id_column = batch.column_by_name("part_id")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing part_id column".to_string()))?;
            let part_id_array = if let Some(dict_array) = part_id_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
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
            let node_id_array = if let Some(dict_array) = node_id_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
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

            // Get other required columns
            let timestamp_array = batch.column_by_name("timestamp")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing timestamp column".to_string()))?
                .as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| TLogFSError::ArrowMessage("timestamp column is not TimestampMicrosecondArray".to_string()))?;

            let version_array = batch.column_by_name("version")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing version column".to_string()))?
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("version column is not Int64Array".to_string()))?;

            let min_event_time_array = batch.column_by_name("min_event_time")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing min_event_time column".to_string()))?
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("min_event_time column is not Int64Array".to_string()))?;

            let max_event_time_array = batch.column_by_name("max_event_time")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing max_event_time column".to_string()))?
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage("max_event_time column is not Int64Array".to_string()))?;

            let sha256_column = batch.column_by_name("sha256")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing sha256 column".to_string()))?;
            let sha256_array = if let Some(dict_array) = sha256_column.as_any().downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>() {
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
            let size_array = if let Some(int64_array) = size_column.as_any().downcast_ref::<arrow::array::Int64Array>() {
                int64_array.clone()
            } else if let Some(uint64_array) = size_column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
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

            // Extract content column if it exists (may be None for metadata queries)
            let content_array = batch.column_by_name("content")
                .and_then(|col| col.as_any().downcast_ref::<arrow::array::BinaryArray>());

            // Create OplogEntry records for matching indices only
            for &idx in &matching_indices {
                if part_id_array.is_valid(idx) && 
                   node_id_array.is_valid(idx) && 
                   timestamp_array.is_valid(idx) && 
                   version_array.is_valid(idx) {
                    
                    let content = if let Some(content_arr) = content_array {
                        if content_arr.is_valid(idx) {
                            Some(content_arr.value(idx).to_vec())
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let entry = OplogEntry {
                        part_id: part_id_array.value(idx).to_string(),
                        node_id: node_id_array.value(idx).to_string(),
                        file_type: file_type,
                        version: version_array.value(idx),
                        timestamp: timestamp_array.value(idx),
                        min_event_time: if min_event_time_array.is_valid(idx) { Some(min_event_time_array.value(idx)) } else { None },
                        max_event_time: if max_event_time_array.is_valid(idx) { Some(max_event_time_array.value(idx)) } else { None },
                        sha256: if sha256_array.is_valid(idx) { Some(sha256_array.value(idx).to_string()) } else { None },
                        size: if size_array.is_valid(idx) { Some(size_array.value(idx)) } else { None },
                        content,
                        min_override: None, // Not loaded from Arrow for now
                        max_override: None, // Not loaded from Arrow for now  
                        extended_attributes: None, // Not loaded from Arrow for now
                        factory: None, // Not loaded from Arrow for now
                    };
                    
                    results.push(entry);
                }
            }
        }

        let result_count = results.len();
        debug!("NodeTable::query_all_by_entry_type - found {result_count} matching records");
        
        Ok(results)
    }

    /// Scan all OplogEntry records without filtering, converting to Arrow RecordBatch
    /// Scan all OplogEntry records without filtering, converting to Arrow RecordBatch
    /// 
    /// This method assumes the DeltaTable has been properly initialized via OpLogPersistence::open_or_create().
    /// It will fail if called on a DeltaTable that doesn't exist (which should never happen in production).
    async fn scan_all_records(&self, _filters: &[Expr]) -> DataFusionResult<Vec<arrow::record_batch::RecordBatch>> {
        debug!("NodeTable: scanning all OplogEntry records");

        // Use DeltaOps to load data from the properly initialized table
        let delta_ops = DeltaOps::from(self.table.clone());
        let (_, stream) = delta_ops.load().await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Collect all record batches directly - they're already in OplogEntry format
        let batches = deltalake::operations::collect_sendable_stream(stream).await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let batch_count = batches.len();
        debug!("NodeTable: collected {batch_count} batches for SQL access");
        
        // DIAGNOSTIC: Check if we have empty results and schema mismatches
        if batches.is_empty() {
            warn!("NodeTable: No batches found in Delta table - this may cause schema inference issues");
            let field_names = self.schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ");
            debug!("NodeTable declared schema fields: {field_names}");
            
            // Return an empty batch with the correct schema to help DataFusion
            let empty_batch = arrow::record_batch::RecordBatch::new_empty(self.schema.clone());
            return Ok(vec![empty_batch]);
        }
        
        // DIAGNOSTIC: Check schema consistency
        if let Some(first_batch) = batches.first() {
            let actual_schema = first_batch.schema();
            let declared_schema = &self.schema;
            
            let actual_field_names = actual_schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ");
            let declared_field_names = declared_schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ");
            debug!("NodeTable actual schema fields: {actual_field_names}");
            debug!("NodeTable declared schema fields: {declared_field_names}");
            
            if actual_schema.fields().len() != declared_schema.fields().len() {
                let actual_count = actual_schema.fields().len();
                let declared_count = declared_schema.fields().len();
                warn!("NodeTable schema field count mismatch: actual={actual_count}, declared={declared_count}");
            }
            
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            info!("NodeTable: returning {batch_count} batches with {total_rows} total rows");
        }

        Ok(batches)
    }
}

/// Execution plan for NodeTable
#[derive(Debug)]
pub struct NodeExecutionPlan {
    node_table: NodeTable,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl NodeExecutionPlan {
    fn new(node_table: NodeTable) -> Self {
        let schema = node_table.schema.clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { node_table, schema, projection: None, properties }
    }
    
    fn new_with_projection(node_table: NodeTable, projected_schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { node_table, schema: projected_schema, projection, properties }
    }
}

impl ExecutionPlan for NodeExecutionPlan {
    fn name(&self) -> &str {
        "NodeExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        let node_table = self.node_table.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        
        debug!("NodeExecutionPlan: starting execution");
        let field_count = schema.fields().len();
        debug!("NodeExecutionPlan: schema has {field_count} fields");
        
        let stream_schema = schema.clone();
        let stream = async_stream::stream! {
            // Execute the node scan
            debug!("NodeExecutionPlan: calling scan_all_records");
            match node_table.scan_all_records(&[]).await {
                Ok(batches) => {
                    let batch_count = batches.len();
                    debug!("NodeExecutionPlan: got {batch_count} batches from scan");
                    for batch in batches {
                        let row_count = batch.num_rows();
                        debug!("NodeExecutionPlan: processing batch with {row_count} rows");
                        
                        // Apply projection if needed
                        let projected_batch = if let Some(ref indices) = projection {
                            if indices.is_empty() {
                                // For COUNT(*) queries, DataFusion doesn't need any columns, just row count
                                debug!("NodeExecutionPlan: empty projection (COUNT(*)), creating empty batch with row count");
                                let empty_columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = vec![];
                                match arrow::record_batch::RecordBatch::try_new_with_options(
                                    stream_schema.clone(),
                                    empty_columns,
                                    &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()))
                                ) {
                                    Ok(pb) => pb,
                                    Err(e) => {
                                        error!("NodeExecutionPlan: failed to create empty batch with row count: {e}");
                                        yield Err(datafusion::common::DataFusionError::Execution(format!("Empty batch error: {}", e)));
                                        continue;
                                    }
                                }
                            } else {
                                let columns: Vec<_> = indices.iter()
                                    .map(|&i| batch.column(i).clone())
                                    .collect();
                                match arrow::record_batch::RecordBatch::try_new(stream_schema.clone(), columns) {
                                    Ok(pb) => pb,
                                    Err(e) => {
                                        error!("NodeExecutionPlan: failed to create projected batch: {e}");
                                        yield Err(datafusion::common::DataFusionError::Execution(format!("Projection error: {}", e)));
                                        continue;
                                    }
                                }
                            }
                        } else {
                            batch
                        };
                        
                        debug!("NodeExecutionPlan: yielding projected batch with {row_count} rows");
                        yield Ok(projected_batch);
                    }
                },
                Err(e) => {
                    error!("NodeExecutionPlan: scan_all_records failed: {e}");
                    yield Err(e);
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

impl DisplayAs for NodeExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NodeExecutionPlan")
    }
}

#[async_trait]
impl TableProvider for NodeTable {
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Simply delegate to the underlying Delta table but apply our filtered schema
        // The DeltaTable already handles partition columns properly
        
        // Convert our table to a DataFusion TableProvider
        let delta_provider: Arc<dyn TableProvider> = Arc::new(self.table.clone());
        
        // Get the Delta table's schema and find indices for all columns except "content"
        let delta_schema = delta_provider.schema();
        let mut filtered_projection = Vec::new();
        
        for (i, field) in delta_schema.fields().iter().enumerate() {
            if field.name() != "content" {
                filtered_projection.push(i);
            }
        }
        
        let filtered_count = filtered_projection.len();
        let total_count = delta_schema.fields().len();
        debug!("NodeTable: excluding content column, projecting {filtered_count} out of {total_count} columns");
        
        // Scan the Delta table with our content-excluding projection
        let base_plan = delta_provider.scan(state, Some(&filtered_projection), filters, limit).await?;
        
        // If caller requested additional projection, apply it
        if let Some(caller_projection) = projection {
            let index_list = caller_projection.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
            debug!("NodeTable: applying additional projection for indices: {index_list}");
            
            let final_plan = Arc::new(datafusion::physical_plan::projection::ProjectionExec::try_new(
                caller_projection.iter().map(|&i| (Arc::new(datafusion::physical_expr::expressions::Column::new("", i)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>, format!("col_{}", i))).collect(),
                base_plan,
            )?);
            Ok(final_plan)
        } else {
            debug!("NodeTable: no additional projection needed");
            Ok(base_plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use deltalake::DeltaOps;

    #[tokio::test]
    async fn test_node_table_creation() {
        // Test basic NodeTable creation
        let table_path = "/tmp/test_node_table".to_string();
        let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
        
        let node_table = NodeTable::new(table);
        
        // Test schema
        let schema = node_table.schema();
        let fields = schema.fields();
        
        // OplogEntry should have the expected fields
        let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"part_id"));
        assert!(field_names.contains(&"node_id"));
        assert!(field_names.contains(&"file_type"));
        assert!(field_names.contains(&"version"));
        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"min_event_time"));
        assert!(field_names.contains(&"max_event_time"));
        assert!(field_names.contains(&"sha256"));
        assert!(field_names.contains(&"size"));
    }

    #[tokio::test]
    async fn test_node_execution_plan_creation() {
        let table_path = "/tmp/test_node_table".to_string();
        let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
        let node_table = NodeTable::new(table);
        
        // Test NodeExecutionPlan creation
        let execution_plan = NodeExecutionPlan::new(node_table.clone());
        
        assert_eq!(execution_plan.name(), "NodeExecutionPlan");
        assert_eq!(execution_plan.children().len(), 0);
        
        // Schema should match NodeTable schema
        assert_eq!(execution_plan.schema(), node_table.schema());
    }

    #[tokio::test]
    async fn test_table_provider_interface() {
        // Create a temporary directory for the test Delta table
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let table_path = temp_dir.path().to_str().unwrap();
        
        // Create an empty Delta table for testing
        let table = match DeltaOps::try_from_uri(table_path).await {
            Ok(delta_ops) => {
                // Try to create a table if it doesn't exist
                match delta_ops.create().await {
                    Ok(table) => table,
                    Err(_) => {
                        debug!("Failed to create Delta table, skipping test");
                        return;
                    }
                }
            },
            Err(_) => {
                debug!("Failed to initialize Delta ops, skipping test");
                return;
            }
        };
        
        let node_table = NodeTable::new(table);
        
        // Test TableProvider interface basics (these don't require actual data)
        assert_eq!(node_table.table_type(), TableType::Base);
        
        // Test schema access
        let schema = node_table.schema();
        assert!(schema.fields().len() > 0);
        
        // Test that scan() method now returns a plan instead of NotImplemented
        let mock_session = datafusion::execution::context::SessionContext::new();
        let session_state = mock_session.state();
        let result = node_table.scan(&session_state, None, &[], None).await;
        
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.name(), "NodeExecutionPlan");
        
        // Test that the execution plan has the correct schema
        assert_eq!(plan.schema(), node_table.schema());
    }
}
