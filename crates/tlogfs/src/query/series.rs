use crate::schema::ExtendedAttributes;
use crate::query::OperationsTable;
use crate::OplogEntry;
use crate::error::TLogFSError;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tinyfs::EntryType;

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
/// This table provides efficient temporal queries over FileSeries entries by leveraging
/// the dedicated min_event_time and max_event_time columns in OplogEntry for fast
/// file-level filtering, followed by standard Parquet statistics for fine-grained pruning.
/// 
/// Architecture:
/// 1. **Fast file elimination**: Uses min/max_event_time columns to eliminate entire files
/// 2. **Automatic Parquet pruning**: DataFusion uses standard Parquet statistics within selected files
/// 3. **Delta Lake pattern**: Follows the same metadata approach as production lakehouse systems
///
/// Example queries:
/// - SELECT * FROM series WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
/// - SELECT * FROM series WHERE event_time >= 1672531200000 AND event_time <= 1675209599999
#[derive(Debug, Clone)]
pub struct SeriesTable {
    inner: OperationsTable,
    series_path: String,  // The series identifier (node path)
    tinyfs_root: Option<Arc<tinyfs::WD>>,  // TinyFS root for file access
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
        Self { 
            inner: operations_table,
            series_path,
            tinyfs_root: None,
        }
    }

    /// Create a new SeriesTable with TinyFS access for actual file reading
    pub fn new_with_tinyfs(series_path: String, operations_table: OperationsTable, tinyfs_root: Arc<tinyfs::WD>) -> Self {
        Self { 
            inner: operations_table,
            series_path,
            tinyfs_root: Some(tinyfs_root),
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
        // Query all FileSeries entries for this series path
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
        // Note: This may need adjustment based on how node_id mapping works
        let node_id = self.series_path_to_node_id(&self.series_path)?;
        
        // Query for FileSeries entries that overlap with the time range
        // Uses the dedicated min/max_event_time columns for efficient filtering
        let records = self.inner.query_records_with_temporal_filter(
            &node_id,
            start_time,
            end_time,
        ).await?;
        
        Ok(records)
    }

    async fn find_all_series_entries(&self) -> Result<Vec<OplogEntry>, TLogFSError> {
        let node_id = self.series_path_to_node_id(&self.series_path)?;
        
        // Query all FileSeries entries for this node (no time filtering)
        let records = self.inner.query_records_for_node(&node_id).await?;
        
        Ok(records)
    }

    async fn find_first_series_entry(&self) -> Result<Option<OplogEntry>, TLogFSError> {
        let node_id = self.series_path_to_node_id(&self.series_path)?;
        
        // Query for the first version (version = 1) to get metadata
        let records = self.inner.query_records_for_node_version(&node_id, 1).await?;
        
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
        // Convert series path to node_id
        // This implementation depends on the path -> node_id mapping in the system
        // For now, using the path directly - may need adjustment
        Ok(path.to_string())
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
        // For now, delegate to inner table but this will need refinement
        self.inner.schema()
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
        let remaining_filters = self.remove_temporal_filters(filters);
        
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
        // This leverages the dedicated min/max_event_time columns for fast filtering
        // The key insight: eliminate entire files based on temporal metadata
        
        // SQL equivalent:
        // SELECT * FROM operations_table 
        // WHERE file_type = 'FileSeries'
        //   AND node_id = ?
        //   AND max_event_time >= ? 
        //   AND min_event_time <= ?
        // ORDER BY min_event_time
        
        // For now, use the existing query infrastructure and filter in Rust
        // A full implementation would push this down to the Delta Lake layer
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
    pub async fn query_records_for_node(&self, _node_id: &str) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Use existing query infrastructure
        // This is a placeholder - the actual implementation would use the Delta Lake queries
        // that are already working in the system
        
        // For now, return empty vector - this needs to be connected to the actual
        // persistence layer queries that are already implemented
        Ok(Vec::new())
    }

    /// Query records for a specific node and version
    pub async fn query_records_for_node_version(&self, node_id: &str, version: i64) -> Result<Vec<OplogEntry>, TLogFSError> {
        let all_records = self.query_records_for_node(node_id).await?;
        Ok(all_records.into_iter().filter(|r| r.version == version).collect())
    }
}

/// Custom DataFusion execution plan for streaming through SeriesTable files
#[derive(Debug)]
pub struct SeriesExecutionPlan {
    file_infos: Vec<FileInfo>,
    series_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
    tinyfs_root: Option<Arc<tinyfs::WD>>,  // TinyFS root for file access
}

impl SeriesExecutionPlan {
    pub fn new(
        file_infos: Vec<FileInfo>,
        series_path: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
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
            series_path,
            schema,
            projection,
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
        let schema = self.schema.clone();
        let limit = self.limit;
        
        // Create the stream using futures::stream::iter and then_try for stable Rust
        use futures::stream::{self, StreamExt, TryStreamExt};
        use arrow::record_batch::RecordBatch;
        
        let stream = stream::iter(file_infos.into_iter().enumerate())
            .then(move |(index, _file_info)| {
                let schema = schema.clone();
                async move {
                    // Skip if we've hit the limit (simplified for now)
                    if let Some(limit) = limit {
                        if index >= limit {
                            return Ok(None);
                        }
                    }
                    
                    // TODO: Get TinyFS reader for this file_info
                    // For now, we'll create an empty batch to demonstrate the architecture
                    // In real implementation:
                    // 1. file_info.get_reader() -> AsyncRead + AsyncSeek
                    // 2. ParquetRecordBatchStreamBuilder::new(reader)
                    // 3. Stream through that file's batches
                    
                    // Placeholder: Create an empty batch with the correct schema
                    let batch = RecordBatch::new_empty(schema);
                    Ok(Some(batch))
                }
            })
            .try_filter_map(|opt_batch| async move { Ok(opt_batch) });
        
        let adapted_stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream);
        Ok(Box::pin(adapted_stream))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}
