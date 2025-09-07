use crate::schema::{ForArrow, VersionedDirectoryEntry};
use crate::query::NodeTable;
use arrow::datatypes::{SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tinyfs::EntryType;
use diagnostics;

use deltalake::DeltaTable;
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

/// Table for querying directory content (VersionedDirectoryEntry records)
/// 
/// This table provides a DataFusion interface specifically for the contents of directories
/// by deserializing VersionedDirectoryEntry records from directory OplogEntry content fields.
/// Unlike the old implementation, this properly exposes directory entries, not OplogEntry metadata.
/// 
/// Architecture:
/// 1. Uses NodeTable to find directory OplogEntry records  
/// 2. Deserializes the content field to get VersionedDirectoryEntry records
/// 3. Presents these entries as queryable SQL tables
/// 
/// Example queries:
/// - SELECT * FROM directory_contents WHERE name LIKE 'test%'
/// - SELECT name, child_node_id FROM directory_contents WHERE operation_type = 'Insert'
/// - SELECT COUNT(*) FROM directory_contents WHERE node_type = 'File'
#[derive(Debug, Clone)]
pub struct DirectoryTable {
    metadata_table: NodeTable,
    directory_node_id: Option<String>,  // Optional filter for specific directory @@@ When not?
    schema: SchemaRef,
}

impl DirectoryTable {
    /// Create a new DirectoryTable for querying all directory contents
    pub fn new(table: DeltaTable) -> Self {
        // Use VersionedDirectoryEntry schema since that's what we expose via SQL
        let schema = Arc::new(arrow::datatypes::Schema::new(VersionedDirectoryEntry::for_arrow()));
        let metadata_table = NodeTable::new(table);
        Self { 
            metadata_table,
            directory_node_id: None,
            schema 
        }
    }
    
    /// Create a new DirectoryTable for a specific directory by node_id
    pub fn for_directory(table: DeltaTable, directory_node_id: String) -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(VersionedDirectoryEntry::for_arrow()));
        let metadata_table = NodeTable::new(table);
        Self { 
            metadata_table,
            directory_node_id: Some(directory_node_id),
            schema 
        }
    }

    /// Deserialize VersionedDirectoryEntry records from directory content
    async fn parse_directory_content(&self, content: &[u8]) -> Result<Vec<VersionedDirectoryEntry>, crate::error::TLogFSError> {
        if content.is_empty() {
            return Ok(Vec::new());
        }
        
        use arrow::ipc::reader::StreamReader;
        
        let cursor = std::io::Cursor::new(content);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| crate::error::TLogFSError::ArrowMessage(format!("Failed to create IPC reader: {}", e)))?;
        
        let mut all_entries = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| crate::error::TLogFSError::ArrowMessage(format!("Failed to read batch: {}", e)))?;
            let entries: Vec<VersionedDirectoryEntry> = serde_arrow::from_record_batch(&batch)
                .map_err(|e| crate::error::TLogFSError::Serialization(e))?;
            all_entries.extend(entries);
        }
        
        Ok(all_entries)
    }

    /// Query directory OplogEntry records and extract VersionedDirectoryEntry content
    async fn scan_directory_entries(&self, _filters: &[Expr]) -> DataFusionResult<Vec<RecordBatch>> {
        // Query NodeTable for directory entries
        let oplog_entries = if let Some(ref node_id) = self.directory_node_id {
            // Query for specific directory by node_id
            self.metadata_table.query_records_for_node(node_id, EntryType::Directory).await
        } else {
            // For now, we can only query specific directories since NodeTable doesn't have query_all_by_type
            // TODO: Add query_all_by_entry_type method to NodeTable
            diagnostics::log_debug!("DirectoryTable: no specific directory node_id provided, returning empty results");
            Ok(Vec::new())
        }.map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let oplog_count = oplog_entries.len();
        diagnostics::log_debug!("DirectoryTable found {count} directory OplogEntry records", count: oplog_count);

        let mut all_entries = Vec::new();
        
        // Process each directory OplogEntry to extract VersionedDirectoryEntry records
        for oplog_entry in oplog_entries {
            // Skip entries without content
            let content = match &oplog_entry.content {
                Some(content_bytes) => content_bytes,
                None => {
                    diagnostics::log_debug!("Directory {node_id} has no content, skipping", node_id: oplog_entry.node_id);
                    continue;
                }
            };

            match self.parse_directory_content(content).await {
                Ok(dir_entries) => {
                    let entry_count = dir_entries.len();
                    let node_id = &oplog_entry.node_id;
                    diagnostics::log_debug!("Parsed {count} VersionedDirectoryEntry records from directory {node_id}", 
                        count: entry_count, node_id: node_id);
                    all_entries.extend(dir_entries);
                },
                Err(e) => {
                    diagnostics::log_info!("Failed to parse directory content for {node_id}: {error}", 
                        node_id: oplog_entry.node_id, error: e);
                    // Continue processing other directories instead of failing completely
                }
            }
        }

        // Convert VersionedDirectoryEntry records to Arrow RecordBatch
        if all_entries.is_empty() {
            diagnostics::log_debug!("No VersionedDirectoryEntry records found");
            return Ok(vec![]);
        }

        let batch = serde_arrow::to_record_batch(&VersionedDirectoryEntry::for_arrow(), &all_entries)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let rows = batch.num_rows();
        let cols = batch.num_columns();
        diagnostics::log_debug!("Created RecordBatch with {rows} rows, {cols} columns", 
            rows: rows, cols: cols);

        Ok(vec![batch])
    }
}

/// Execution plan for DirectoryTable
#[derive(Debug)]
pub struct DirectoryExecutionPlan {
    directory_table: DirectoryTable,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DirectoryExecutionPlan {
    fn new(directory_table: DirectoryTable) -> Self {
        let schema = directory_table.schema.clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { directory_table, schema, properties }
    }
}

impl ExecutionPlan for DirectoryExecutionPlan {
    fn name(&self) -> &str {
        "DirectoryExecutionPlan"
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
        let directory_table = self.directory_table.clone();
        let schema = self.schema.clone();
        
        let stream = async_stream::stream! {
            // Execute the directory scan
            match directory_table.scan_directory_entries(&[]).await {
                Ok(batches) => {
                    for batch in batches {
                        yield Ok(batch);
                    }
                },
                Err(e) => {
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

impl DisplayAs for DirectoryExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DirectoryExecutionPlan")
    }
}

#[async_trait]
impl TableProvider for DirectoryTable {
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
        // For now, we ignore filters and projection for simplicity
        // In a full implementation, we would push down filters to the metadata query
        Ok(Arc::new(DirectoryExecutionPlan::new(self.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use deltalake::DeltaOps;

    #[tokio::test]
    async fn test_directory_table_creation() {
        // Test basic DirectoryTable creation
        let table_path = "/tmp/test_directory_table".to_string();
	let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
        
        // Test general DirectoryTable creation
        let directory_table = DirectoryTable::new(table.clone());
        assert_eq!(directory_table.directory_node_id, None);
        
        // Test specific directory creation
        let specific_table = DirectoryTable::for_directory(
            table.clone(), 
            "test_node_123".to_string()
        );
        assert_eq!(specific_table.directory_node_id, Some("test_node_123".to_string()));
        
        // Test schema
        let schema = directory_table.schema();
        let fields = schema.fields();
        
        // VersionedDirectoryEntry should have 4 fields: name, child_node_id, operation_type, node_type
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].name(), "name");
        assert_eq!(fields[1].name(), "child_node_id");
        assert_eq!(fields[2].name(), "operation_type");
        assert_eq!(fields[3].name(), "node_type");
    }

    #[tokio::test]
    async fn test_parse_directory_content_empty() {
        let table_path = "/tmp/test_directory_table".to_string();
	let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
        let directory_table = DirectoryTable::new(table);
        
        // Test empty content
        let empty_content = &[];
        let result = directory_table.parse_directory_content(empty_content).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }
}
