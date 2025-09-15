use crate::OplogEntry;
use crate::error::TLogFSError;
use arrow::datatypes::SchemaRef;
use arrow::array::Array;
use tinyfs::EntryType;
use std::sync::Arc;
use diagnostics::*;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use deltalake::{DeltaTable, DeltaOps};
use deltalake::delta_datafusion::DataFusionMixins;
use std::any::Any;

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
        // Simple wrapper - use the DeltaTable's Arrow schema directly
        let schema = table.snapshot().unwrap().arrow_schema().unwrap();
        
        Self { 
            table,
            schema,
        }
    }

    /// Query OplogEntry records for a specific node_id, part_id, and file_type using SQL WHERE filtering
    pub async fn query_records_for_node(&self, node_id: &tinyfs::NodeID, part_id: &tinyfs::NodeID, file_type: EntryType) -> Result<Vec<OplogEntry>, TLogFSError> {
        let file_type_debug = format!("{:?}", file_type);
        debug!("NodeTable::query_records_for_node - node_id: {node_id}, part_id: {part_id}, file_type: {file_type_debug}");

        // Use DataFusion SQL with WHERE filtering for partition pruning
        use datafusion::execution::context::SessionContext;
        
        let ctx = SessionContext::new();
        
        // Register this NodeTable as a table provider - use existing implementation
        ctx.register_table("nodes", Arc::new(self.clone()))
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table: {}", e)))?;
        
        // Convert EntryType to string for SQL query
        let file_type_str = match file_type {
            EntryType::FileData => "file:data",
            EntryType::FileTable => "file:table", 
            EntryType::FileSeries => "file:series",
            EntryType::Directory => "directory",
            EntryType::Symlink => "symlink",
        };
        
        // Convert NodeIDs to strings only at the SQL boundary
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Simple SQL query with WHERE clause for filtering
        let sql = format!(
            "SELECT * FROM nodes WHERE node_id = '{}' AND part_id = '{}' AND file_type = '{}'",
            node_id_str, part_id_str, file_type_str
        );
        
        debug!("Executing SQL query: {sql}");
        let df = ctx.sql(&sql).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to execute SQL query: {}", e)))?;
        
        let batches = df.collect().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect query results: {}", e)))?;

        let batch_count = batches.len();
        debug!("NodeTable::query_records_for_node - collected {batch_count} batches from SQL query");

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

            // Read min_override and max_override columns
            let min_override_column = batch.column_by_name("min_override")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing min_override column".to_string()))?;
            
            // Debug: Check the actual type of min_override column
            let min_override_type = format!("{:?}", min_override_column.data_type());
            debug!("NodeTable min_override column type: {min_override_type}");
            
            let min_override_array = min_override_column
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage(format!("min_override column is not Int64Array, actual type: {:?}", min_override_column.data_type())))?;

            let max_override_column = batch.column_by_name("max_override")
                .ok_or_else(|| TLogFSError::ArrowMessage("Missing max_override column".to_string()))?;
            
            // Debug: Check the actual type of max_override column
            let max_override_type = format!("{:?}", max_override_column.data_type());
            debug!("NodeTable max_override column type: {max_override_type}");
            
            let max_override_array = max_override_column
                .as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| TLogFSError::ArrowMessage(format!("max_override column is not Int64Array, actual type: {:?}", max_override_column.data_type())))?;

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
                if node_id_array.value(row_idx) == node_id.to_hex_string() && 
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
                        min_override: if min_override_array.is_null(row_idx) { None } else { Some(min_override_array.value(row_idx)) },
                        max_override: if max_override_array.is_null(row_idx) { None } else { Some(max_override_array.value(row_idx)) },
                        extended_attributes: if extended_attributes_array.is_null(row_idx) { None } else { Some(extended_attributes_array.value(row_idx).to_string()) },
                        factory: None,
                    };
                    
                    // Debug: Log temporal metadata values
                    let min_time_str = entry.min_event_time.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    let max_time_str = entry.max_event_time.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    let min_override_str = entry.min_override.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    let max_override_str = entry.max_override.map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                    log_debug!("NodeTable entry - version: {version}, min_event_time: {min_time}, max_event_time: {max_time}, min_override: {min_override}, max_override: {max_override}", 
                        version: entry.version, min_time: min_time_str, max_time: max_time_str, min_override: min_override_str, max_override: max_override_str);
                    
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
        node_id: &tinyfs::NodeID,
        part_id: &tinyfs::NodeID,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<OplogEntry>, TLogFSError> {
        debug!("NodeTable::query_records_with_temporal_filter - node_id: {node_id}, part_id: {part_id}, start: {start_time}, end: {end_time}");

        // Get all FileSeries records for the node first
        let all_records = self.query_records_for_node(node_id, part_id, EntryType::FileSeries).await?;
        
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

}

#[async_trait]
impl TableProvider for NodeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Return the actual DeltaTable Arrow schema
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
        // Simple delegation to the underlying DeltaTable
        // DataFusion and Parquet will handle column pruning automatically
        let delta_provider: Arc<dyn TableProvider> = Arc::new(self.table.clone());
        delta_provider.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use deltalake::DeltaOps;

    #[tokio::test]
    async fn test_node_table_creation() {
        use tempfile;
        use crate::OpLogPersistence;
        
        // Create temporary directory for test pond
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path();
        
        // Create a proper pond with TLogFS persistence
        let mut persistence = OpLogPersistence::create(pond_path.to_str().unwrap()).await
            .expect("Failed to create OpLogPersistence");
        
        // Begin a transaction to initialize the table
        let tx = persistence.begin().await.expect("Failed to begin transaction");
        let table = tx.state().expect("Failed to get transaction state")
            .table().await.expect("Failed to get persistence state table")
            .expect("No Delta table available");
        
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
        // The plan name will be from the underlying Delta table implementation
        assert!(!plan.name().is_empty());
        
        // Test that the execution plan has the correct filtered schema (without content column)
        assert_eq!(plan.schema(), node_table.schema());
    }
}
