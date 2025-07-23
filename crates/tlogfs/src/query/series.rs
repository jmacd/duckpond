use crate::schema::ExtendedAttributes;
use crate::query::OperationsTable;
use crate::OplogEntry;
use crate::error::TLogFSError;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::array::Array; // Add this for is_valid method
use std::sync::Arc;
use tinyfs::EntryType;
use diagnostics;

// DataFusion imports for table providers and execution plans
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, BinaryExpr, Operator};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{Statistics, PlanProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::fmt;

/// Specialized table for querying file:series data with time-range filtering
/// 
/// This table provides efficient temporal queries over FileSeries entries by directly
/// reading Parquet files from TinyFS storage. It uses file discovery and Parquet
/// metadata for efficient time-range filtering.
/// 
/// Architecture:
/// 1. **Direct file discovery**: Uses TinyFS to discover series files
/// 2. **Parquet metadata filtering**: Uses Parquet statistics for time-range pruning
/// 3. **Streaming execution**: Processes files sequentially without loading everything into memory
///
/// Example queries:
/// - SELECT * FROM series WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
/// - SELECT * FROM series WHERE event_time >= 1672531200000 AND event_time <= 1675209599999
#[derive(Debug, Clone)]
pub struct SeriesTable {
    series_path: String,  // The series identifier (node path)
    tinyfs_root: Option<Arc<tinyfs::WD>>,  // TinyFS root for file access
    schema: SchemaRef,  // The schema of the series data
    operations_table: OperationsTable,  // Delta Lake operations table for metadata queries
}

/// Information about a file version that overlaps with a time range
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub file_path: String,
    pub version: i64,
    pub min_event_time: i64,
    pub max_event_time: i64,
    pub timestamp_column: String,
    pub size: Option<u64>,
}

impl FileInfo {
    /// Get an async reader for this file from TinyFS
    pub async fn get_reader(&self, root: &tinyfs::WD) -> Result<std::pin::Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        root.async_reader_path(&self.file_path).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get TinyFS reader for {}: {}", self.file_path, e)))
    }
}

impl SeriesTable {
    /// Create a new SeriesTable for querying a specific file:series (without TinyFS access)
    pub fn new(series_path: String, operations_table: OperationsTable) -> Self {
        // For now, create a basic schema - this should be derived from the actual data
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        Self { 
            series_path,
            tinyfs_root: None,
            schema,
            operations_table,
        }
    }

    /// Create a new SeriesTable with TinyFS access for actual file reading
    pub fn new_with_tinyfs(series_path: String, operations_table: OperationsTable, tinyfs_root: Arc<tinyfs::WD>) -> Self {
        // For now, create a basic schema - this should be derived from the actual data
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        Self { 
            series_path,
            tinyfs_root: Some(tinyfs_root),
            schema,
            operations_table,
        }
    }

    /// Create a new SeriesTable with TinyFS access and known node_id
    pub fn new_with_tinyfs_and_node_id(_series_path: String, node_id: String, operations_table: OperationsTable, tinyfs_root: Arc<tinyfs::WD>) -> Self {
        // For now, create a basic schema - this should be derived from the actual data
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        // Store the node_id directly instead of the path to avoid resolution issues
        Self { 
            series_path: node_id,  // Store node_id in series_path field for now
            tinyfs_root: Some(tinyfs_root),
            schema,
            operations_table,
        }
    }

    /// Scan for file versions that overlap with the given time range
    /// This is the key optimization - only loads relevant files based on temporal metadata
    pub async fn scan_time_range(&self, start_time: i64, end_time: i64) -> Result<Vec<FileInfo>, TLogFSError> {
        // Step 1: Query OplogEntry metadata to find overlapping files
        // This leverages the dedicated min/max_event_time columns for fast filtering
        let overlapping_entries = self.find_overlapping_entries(start_time, end_time).await?;
        
        // Step 2: Convert OplogEntry records to FileInfo for consumption
        let mut file_infos = Vec::new();
        for entry in overlapping_entries {
            if let Some(file_info) = self.entry_to_file_info(entry).await? {
                file_infos.push(file_info);
            }
        }
        
        // Step 3: Sort by min_event_time for optimal streaming order
        file_infos.sort_by_key(|f| f.min_event_time);
        
        Ok(file_infos)
    }

    /// Get all versions of this series (no time filtering)
    pub async fn scan_all_versions(&self) -> Result<Vec<FileInfo>, TLogFSError> {
        // Use the Delta Lake operations table to find all FileSeries entries
        let all_entries = self.find_all_series_entries().await?;
        
        let mut file_infos = Vec::new();
        for entry in all_entries {
            if let Some(file_info) = self.entry_to_file_info(entry).await? {
                file_infos.push(file_info);
            }
        }
        
        // Sort by version for logical ordering
        file_infos.sort_by_key(|f| f.version);
        
        Ok(file_infos)
    }

    /// Get the timestamp column name from the series metadata
    /// This reads from the first version's extended_attributes
    pub async fn get_timestamp_column(&self) -> Result<String, TLogFSError> {
        let first_entry = self.find_first_series_entry().await?
            .ok_or_else(|| TLogFSError::Missing)?;
        
        if let Some(extended_attrs_json) = &first_entry.extended_attributes {
            let attrs = ExtendedAttributes::from_json(extended_attrs_json)
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse extended attributes: {}", e)))?;
            Ok(attrs.timestamp_column().to_string())
        } else {
            // Default fallback
            Ok("Timestamp".to_string())
        }
    }

    /// Extract time range filters from DataFusion expressions
    /// This enables automatic pushdown of temporal predicates
    pub fn extract_time_range_from_filters(&self, filters: &[Expr]) -> Option<(i64, i64)> {
        let mut min_time = i64::MIN;
        let mut max_time = i64::MAX;
        let mut found_filter = false;

        for filter in filters {
            if let Some((col_name, op, value)) = Self::extract_binary_filter(filter) {
                // Look for filters on timestamp-like columns
                if self.is_timestamp_column(&col_name) {
                    if let Some(timestamp) = Self::extract_timestamp_value(&value) {
                        found_filter = true;
                        match op {
                            Operator::Gt | Operator::GtEq => {
                                min_time = min_time.max(timestamp);
                            }
                            Operator::Lt | Operator::LtEq => {
                                max_time = max_time.min(timestamp);
                            }
                            Operator::Eq => {
                                min_time = min_time.max(timestamp);
                                max_time = max_time.min(timestamp);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        if found_filter && min_time != i64::MIN && max_time != i64::MAX {
            Some((min_time, max_time))
        } else {
            None
        }
    }

    // Private helper methods

    async fn find_overlapping_entries(&self, start_time: i64, end_time: i64) -> Result<Vec<OplogEntry>, TLogFSError> {
        // This is the critical query that leverages dedicated temporal columns
        // for fast file-level filtering (like Delta Lake's Add.stats approach)
        
        // Convert series path to node_id for querying
        let node_id = self.series_path_to_node_id(&self.series_path)?;
        
        // Query for FileSeries entries that overlap with the time range
        // Uses the dedicated min/max_event_time columns for efficient filtering
        let records = self.operations_table.query_records_with_temporal_filter(
            &node_id,
            start_time,
            end_time,
        ).await?;
        
        Ok(records)
    }

    async fn find_all_series_entries(&self) -> Result<Vec<OplogEntry>, TLogFSError> {
        let node_id = self.series_path_to_node_id(&self.series_path)?;
        
        // Query all FileSeries entries for this node (no time filtering)
        let records = self.operations_table.query_records_for_node(&node_id).await?;
        
        Ok(records)
    }

    async fn find_first_series_entry(&self) -> Result<Option<OplogEntry>, TLogFSError> {
        let node_id = self.series_path_to_node_id(&self.series_path)?;
        
        // Query for the first version (version = 1) to get metadata
        let records = self.operations_table.query_records_for_node_version(&node_id, 1).await?;
        
        Ok(records.into_iter().next())
    }

    async fn entry_to_file_info(&self, entry: OplogEntry) -> Result<Option<FileInfo>, TLogFSError> {
        // Only process FileSeries entries with temporal metadata
        if entry.file_type != EntryType::FileSeries {
            return Ok(None);
        }

        let (min_time, max_time) = entry.temporal_range()
            .ok_or_else(|| TLogFSError::ArrowMessage("FileSeries entry missing temporal metadata".to_string()))?;

        // Extract timestamp column from extended attributes
        let timestamp_column = if let Some(extended_attrs_json) = &entry.extended_attributes {
            let attrs = ExtendedAttributes::from_json(extended_attrs_json)
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse extended attributes: {}", e)))?;
            attrs.timestamp_column().to_string()
        } else {
            "Timestamp".to_string() // Default fallback
        };

        // Construct file path (may need adjustment based on storage layout)
        let file_path = format!("{}/v{}", self.series_path, entry.version);

        Ok(Some(FileInfo {
            file_path,
            version: entry.version,
            min_event_time: min_time,
            max_event_time: max_time,
            timestamp_column,
            size: entry.size,
        }))
    }

    fn series_path_to_node_id(&self, path: &str) -> Result<String, TLogFSError> {
        // If this is a node_id directly (from new_with_tinyfs_and_node_id), return it
        // Node IDs are typically hex strings, paths start with /
        if !path.starts_with('/') {
            return Ok(path.to_string());
        }
        
        // For actual paths, we need proper resolution
        Err(TLogFSError::ArrowMessage(format!(
            "series_path_to_node_id not properly implemented - cannot resolve path '{}' to node_id. Use new_with_tinyfs_and_node_id instead.", 
            path
        )))
    }

    fn extract_binary_filter(expr: &Expr) -> Option<(String, Operator, ScalarValue)> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
            // Handle column op value
            if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                return Some((col.name.clone(), *op, val.clone()));
            }
            // Handle value op column (reverse)
            if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                let reversed_op = Self::reverse_operator(*op);
                return Some((col.name.clone(), reversed_op, val.clone()));
            }
        }
        None
    }

    fn reverse_operator(op: Operator) -> Operator {
        match op {
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            other => other,
        }
    }

    fn extract_timestamp_value(value: &ScalarValue) -> Option<i64> {
        match value {
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
            ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v / 1000), // Convert to milliseconds
            _ => None,
        }
    }

    fn is_timestamp_column(&self, col_name: &str) -> bool {
        // Check if this column name looks like a timestamp column
        matches!(col_name.to_lowercase().as_str(), 
            "timestamp" | "time" | "event_time" | "ts" | "datetime")
    }
}

#[async_trait]
impl TableProvider for SeriesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Return the schema for the actual series data, not OplogEntry
        // This should be the Arrow schema of the Parquet files themselves
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Step 1: Extract time range from filters for fast file elimination
        let time_range = self.extract_time_range_from_filters(filters);
        
        // Step 2: Get relevant file versions based on temporal metadata
        let file_infos = if let Some((start_time, end_time)) = time_range {
            self.scan_time_range(start_time, end_time).await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        } else {
            self.scan_all_versions().await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        };

        // Step 3: Create a custom execution plan that streams through the Parquet files
        // Create the SeriesExecutionPlan with the filtered files
        let execution_plan = Arc::new(SeriesExecutionPlan::new(
            file_infos,
            self.series_path.clone(),
            self.schema(),
            projection.cloned(),
            limit,
            self.tinyfs_root.clone(),
        ));

        Ok(execution_plan)
    }
}

impl SeriesTable {
    #[allow(dead_code)]
    fn remove_temporal_filters(&self, filters: &[Expr]) -> Vec<Expr> {
        // Remove filters that were already handled by temporal file elimination
        // This prevents duplicate filtering and improves performance
        filters.iter()
            .filter(|filter| {
                if let Some((col_name, _, _)) = Self::extract_binary_filter(filter) {
                    !self.is_timestamp_column(&col_name)
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }
}

// Add these methods to OperationsTable for SeriesTable support

impl OperationsTable {
    /// Query records with temporal filtering for efficient time-range queries
    pub async fn query_records_with_temporal_filter(
        &self,
        node_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Use the existing query infrastructure - get all records first, then filter
        let all_records = self.query_records_for_node(node_id).await?;
        
        let mut filtered_records = Vec::new();
        for record in all_records {
            if record.file_type == EntryType::FileSeries {
                if let Some((min_time, max_time)) = record.temporal_range() {
                    // Check for overlap: file overlaps if max_file >= start_query AND min_file <= end_query
                    if max_time >= start_time && min_time <= end_time {
                        filtered_records.push(record);
                    }
                }
            }
        }
        
        // Sort by min_event_time for optimal processing order
        filtered_records.sort_by_key(|r| r.min_event_time.unwrap_or(0));
        
        Ok(filtered_records)
    }

    /// Query all records for a specific node
    pub async fn query_records_for_node(&self, node_id: &str) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Create a DataFusion context and execute a query
        use datafusion::prelude::*;
        
        diagnostics::log_debug!("Querying for node_id: {node_id}", node_id: node_id);
        
        let ctx = SessionContext::new();
        
        // Register this table
        ctx.register_table("operations", Arc::new(self.clone()))
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table: {}", e)))?;
        
        // Execute SQL query to find records for this node_id
        let sql = format!(
            "SELECT * FROM operations WHERE node_id = '{}' AND file_type = 'FileSeries'",
            node_id.replace("'", "''") // Basic SQL injection protection
        );
        
        diagnostics::log_debug!("Executing SQL: {sql}", sql: &sql);
        
        let df = ctx.sql(&sql).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("SQL query failed: {}", e)))?;
        
        let results = df.collect().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect results: {}", e)))?;
        
        let batch_count = results.len();
        diagnostics::log_debug!("Got {batch_count} result batches", batch_count: batch_count);
        
        // Convert RecordBatch results back to OplogEntry
        let mut entries = Vec::new();
        for batch in results {
            let row_count = batch.num_rows();
            diagnostics::log_debug!("Processing batch with {row_count} rows", row_count: row_count);
            for row_idx in 0..batch.num_rows() {
                if let Ok(entry) = self.record_batch_to_oplog_entry(&batch, row_idx) {
                    entries.push(entry);
                }
            }
        }
        
        let entry_count = entries.len();
        diagnostics::log_debug!("Converted to {entry_count} OplogEntry records", entry_count: entry_count);
        
        Ok(entries)
    }

    /// Query records for a specific node and version
    pub async fn query_records_for_node_version(&self, node_id: &str, version: i64) -> Result<Vec<OplogEntry>, TLogFSError> {
        let all_records = self.query_records_for_node(node_id).await?;
        Ok(all_records.into_iter().filter(|r| r.version == version).collect())
    }
    
    /// Helper method to convert RecordBatch row to OplogEntry
    fn record_batch_to_oplog_entry(&self, batch: &RecordBatch, row_idx: usize) -> Result<OplogEntry, TLogFSError> {
        use arrow::array::{StringArray, Int64Array, UInt64Array, BinaryArray};
        
        // Get column arrays with proper error handling
        let part_id_array = batch.column(0).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| TLogFSError::ArrowMessage("part_id column is not StringArray".to_string()))?;
        let node_id_array = batch.column(1).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| TLogFSError::ArrowMessage("node_id column is not StringArray".to_string()))?;
        let file_type_array = batch.column(2).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| TLogFSError::ArrowMessage("file_type column is not StringArray".to_string()))?;
        let timestamp_array = batch.column(3).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| TLogFSError::ArrowMessage("timestamp column is not Int64Array".to_string()))?;
        let version_array = batch.column(4).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| TLogFSError::ArrowMessage("version column is not Int64Array".to_string()))?;
        let content_array = batch.column(5).as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| TLogFSError::ArrowMessage("content column is not BinaryArray".to_string()))?;
        let sha256_array = batch.column(6).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| TLogFSError::ArrowMessage("sha256 column is not StringArray".to_string()))?;
        let size_array = batch.column(7).as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| TLogFSError::ArrowMessage("size column is not UInt64Array".to_string()))?;
        let min_event_time_array = batch.column(8).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| TLogFSError::ArrowMessage("min_event_time column is not Int64Array".to_string()))?;
        let max_event_time_array = batch.column(9).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| TLogFSError::ArrowMessage("max_event_time column is not Int64Array".to_string()))?;
        let extended_attributes_array = batch.column(10).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| TLogFSError::ArrowMessage("extended_attributes column is not StringArray".to_string()))?;

        // Extract values with bounds checking
        if row_idx >= batch.num_rows() {
            return Err(TLogFSError::ArrowMessage(format!("Row index {} out of bounds (batch has {} rows)", row_idx, batch.num_rows())));
        }

        let part_id = part_id_array.value(row_idx).to_string();
        let node_id = node_id_array.value(row_idx).to_string();
        let file_type_str = file_type_array.value(row_idx);
        let timestamp = timestamp_array.value(row_idx);
        let version = version_array.value(row_idx);
        
        // Handle nullable fields - check nulls via the array's null buffer
        let content = if content_array.is_valid(row_idx) {
            Some(content_array.value(row_idx).to_vec())
        } else {
            None
        };
        
        let sha256 = if sha256_array.is_valid(row_idx) {
            Some(sha256_array.value(row_idx).to_string())
        } else {
            None
        };
        
        let size = if size_array.is_valid(row_idx) {
            Some(size_array.value(row_idx))
        } else {
            None
        };
        
        let min_event_time = if min_event_time_array.is_valid(row_idx) {
            Some(min_event_time_array.value(row_idx))
        } else {
            None
        };
        
        let max_event_time = if max_event_time_array.is_valid(row_idx) {
            Some(max_event_time_array.value(row_idx))
        } else {
            None
        };
        
        let extended_attributes = if extended_attributes_array.is_valid(row_idx) {
            Some(extended_attributes_array.value(row_idx).to_string())
        } else {
            None
        };

        // Convert file_type string to enum
        let file_type = match file_type_str {
            "Directory" => EntryType::Directory,
            "FileData" => EntryType::FileData,
            "FileTable" => EntryType::FileTable,
            "FileSeries" => EntryType::FileSeries,
            "Symlink" => EntryType::Symlink,
            _ => return Err(TLogFSError::ArrowMessage(format!("Unknown file_type: {}", file_type_str))),
        };

        Ok(OplogEntry {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content,
            sha256,
            size,
            min_event_time,
            max_event_time,
            extended_attributes,
        })
    }
}

/// Custom DataFusion execution plan for streaming through SeriesTable files
#[derive(Debug)]
pub struct SeriesExecutionPlan {
    file_infos: Vec<FileInfo>,
    schema: SchemaRef,
    limit: Option<usize>,
    properties: PlanProperties,
    tinyfs_root: Option<Arc<tinyfs::WD>>,  // TinyFS root for file access
}

impl SeriesExecutionPlan {
    pub fn new(
        file_infos: Vec<FileInfo>,
        _series_path: String,
        schema: SchemaRef,
        _projection: Option<Vec<usize>>,
        limit: Option<usize>,
        tinyfs_root: Option<Arc<tinyfs::WD>>,
    ) -> Self {
        // Create plan properties with basic settings
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let output_partitioning = Partitioning::UnknownPartitioning(1);
        let properties = PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded
        );
        
        Self {
            file_infos,
            schema,
            limit,
            properties,
            tinyfs_root,
        }
    }
}

impl DisplayAs for SeriesExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SeriesExecutionPlan: {} files", self.file_infos.len())
    }
}

impl ExecutionPlan for SeriesExecutionPlan {
    fn name(&self) -> &str {
        "SeriesExec"
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
        // Create a stream that reads through all the relevant Parquet files
        // sequentially, presenting them as a unified stream of RecordBatches
        
        let file_infos = self.file_infos.clone();
        let tinyfs_root = self.tinyfs_root.clone();
        let limit = self.limit;
        
        // Check if we have TinyFS access
        if tinyfs_root.is_none() {
            // No TinyFS access - return empty stream
            let stream = futures::stream::empty();
            let adapted_stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream);
            return Ok(Box::pin(adapted_stream));
        }
        
        let tinyfs_root = tinyfs_root.unwrap();
        
        // Create the stream that reads actual Parquet files
        use futures::stream::{self, StreamExt, TryStreamExt};
        use parquet::arrow::ParquetRecordBatchStreamBuilder;
        
        // For now, let's use a simpler approach that processes one file at a time
        let stream = stream::iter(file_infos.into_iter().enumerate())
            .then(move |(index, file_info)| {
                let tinyfs_root = tinyfs_root.clone();
                async move {
                    // Skip if we've hit the limit
                    if let Some(limit) = limit {
                        if index >= limit {
                            return Ok::<Vec<RecordBatch>, datafusion::error::DataFusionError>(Vec::new());
                        }
                    }
                    
                    // Get TinyFS reader for this file
                    let reader = match file_info.get_reader(&tinyfs_root).await {
                        Ok(reader) => reader,
                        Err(e) => {
                            // Log error and skip this file
                            diagnostics::log_info!("Failed to get reader for {file_path}: {error}", file_path: &file_info.file_path, error: e);
                            return Ok::<Vec<RecordBatch>, datafusion::error::DataFusionError>(Vec::new());
                        }
                    };
                    
                    // Create Parquet stream builder
                    let builder = match ParquetRecordBatchStreamBuilder::new(reader).await {
                        Ok(builder) => builder,
                        Err(e) => {
                            // Log error and skip this file (might not be Parquet)
                            diagnostics::log_info!("Failed to create Parquet stream for {file_path}: {error}", file_path: &file_info.file_path, error: e);
                            return Ok::<Vec<RecordBatch>, datafusion::error::DataFusionError>(Vec::new());
                        }
                    };
                    
                    // Build the stream and collect all batches from this file
                    let mut parquet_stream = match builder.build() {
                        Ok(stream) => stream,
                        Err(e) => {
                            diagnostics::log_info!("Failed to build Parquet stream for {file_path}: {error}", file_path: &file_info.file_path, error: e);
                            return Ok::<Vec<RecordBatch>, datafusion::error::DataFusionError>(Vec::new());
                        }
                    };
                    
                    // Collect all batches from this file into a vector
                    let mut file_batches = Vec::new();
                    while let Some(batch_result) = parquet_stream.try_next().await.transpose() {
                        match batch_result {
                            Ok(batch) => file_batches.push(batch),
                            Err(e) => {
                                diagnostics::log_info!("Error reading batch from {file_path}: {error}", file_path: &file_info.file_path, error: e);
                                break;
                            }
                        }
                    }
                    
                    Ok::<Vec<RecordBatch>, datafusion::error::DataFusionError>(file_batches)
                }
            })
            .map(|batches_result| {
                match batches_result {
                    Ok(batches) => batches,
                    Err(_) => Vec::new(), // On error, return empty vec
                }
            })
            .map(|batches| stream::iter(batches.into_iter().map(Ok)))
            .flatten();
        
        let adapted_stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream);
        Ok(Box::pin(adapted_stream))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}
