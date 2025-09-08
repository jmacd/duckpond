use crate::schema::{ForArrow, VersionedDirectoryEntry};
use arrow::datatypes::{SchemaRef, FieldRef};
use arrow::record_batch::RecordBatch;
use arrow::array::Array; // For is_null method
use std::sync::Arc;
use datafusion::execution::context::SessionContext;
use diagnostics::*;

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
/// This properly exposes directory entries, not OplogEntry metadata.
/// 
/// Architecture:
/// 1. Uses SQL queries to find directory OplogEntry records from Delta table  
/// 2. Deserializes the content field to get VersionedDirectoryEntry records
/// 3. Presents these entries as queryable SQL tables
/// 
/// Example queries:
/// - SELECT * FROM directory_entries WHERE name LIKE 'test%'
/// - SELECT name, child_node_id FROM directory_entries WHERE operation_type = 'Insert'
/// - SELECT COUNT(*) FROM directory_entries WHERE node_type = 'File'
#[derive(Debug, Clone)]
pub struct DirectoryTable {
    delta_table: DeltaTable,
    directory_node_id: Option<String>,  // Optional filter for specific directory @@@ When not?
    schema: SchemaRef,
}

impl DirectoryTable {
    /// Create a new DirectoryTable for querying all directory contents
    pub fn new(table: DeltaTable) -> Self {
        // Use VersionedDirectoryEntry schema since that's what we expose via SQL
        let schema = Arc::new(arrow::datatypes::Schema::new(VersionedDirectoryEntry::for_arrow()));
        Self { 
            delta_table: table,
            directory_node_id: None,
            schema 
        }
    }
    
    /// Create a new DirectoryTable for a specific directory by node_id
    pub fn for_directory(table: DeltaTable, directory_node_id: String) -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(VersionedDirectoryEntry::for_arrow()));
        Self { 
            delta_table: table,
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
        // Create DataFusion context and register Delta table
        let ctx = SessionContext::new();
        let delta_table = Arc::new(self.delta_table.clone());
        ctx.register_table("oplog_entries", delta_table)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Build SQL query for directory entries
        let sql = if let Some(ref node_id) = self.directory_node_id {
            format!(
                "SELECT node_id, content FROM oplog_entries WHERE file_type = 'directory' AND node_id = '{}'",
                node_id
            )
        } else {
            "SELECT node_id, content FROM oplog_entries WHERE file_type = 'directory'".to_string()
        };

        // Execute query to get directory OplogEntry records
        let df = ctx.sql(&sql).await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        
        let batches = df.collect().await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Extract OplogEntry data from query results  
        let mut all_entries = Vec::new();
        
        for batch in batches {
            let node_id_array = batch.column_by_name("node_id")
                .ok_or_else(|| datafusion::common::DataFusionError::Plan("Missing node_id column".to_string()))?
                .as_any().downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| datafusion::common::DataFusionError::Plan("node_id column is not string type".to_string()))?;
                
            let content_array = batch.column_by_name("content")
                .ok_or_else(|| datafusion::common::DataFusionError::Plan("Missing content column".to_string()))?
                .as_any().downcast_ref::<arrow::array::BinaryArray>()
                .ok_or_else(|| datafusion::common::DataFusionError::Plan("content column is not binary type".to_string()))?;

            // Process each directory OplogEntry to extract VersionedDirectoryEntry records
            for i in 0..batch.num_rows() {
                let node_id = node_id_array.value(i);
                
                // Skip entries without content
                if content_array.is_null(i) {
                    debug!("Directory {node_id} has no content, skipping");
                    continue;
                }
                
                let content = content_array.value(i);
                
                match self.parse_directory_content(content).await {
                    Ok(dir_entries) => {
                        let entry_count = dir_entries.len();
                        debug!("Parsed {entry_count} VersionedDirectoryEntry records from directory {node_id}");
                        all_entries.extend(dir_entries);
                    },
                    Err(e) => {
                        info!("Failed to parse directory content for {node_id}: {e}");
                        // Continue processing other directories instead of failing completely
                    }
                }
            }
        }

        // Convert VersionedDirectoryEntry records to Arrow RecordBatch
        if all_entries.is_empty() {
            debug!("No VersionedDirectoryEntry records found");
            return Ok(vec![]);
        }

        let batch = serde_arrow::to_record_batch(&VersionedDirectoryEntry::for_arrow(), &all_entries)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let rows = batch.num_rows();
        let cols = batch.num_columns();
        debug!("Created RecordBatch with {rows} rows, {cols} columns");

        Ok(vec![batch])
    }
}

/// Execution plan for DirectoryTable
#[derive(Debug)]
pub struct DirectoryExecutionPlan {
    directory_table: DirectoryTable,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
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
        Self { directory_table, schema, projection: None, properties }
    }
    
    fn new_with_projection(directory_table: DirectoryTable, projected_schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { directory_table, schema: projected_schema, projection, properties }
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
        let stream_schema = schema.clone(); // Clone for the stream adapter
        let projection = self.projection.clone();
        
        let stream = async_stream::stream! {
            // Execute the directory scan
            match directory_table.scan_directory_entries(&[]).await {
                Ok(batches) => {
                    for batch in batches {
                        // Apply projection if needed
                        let projected_batch = if let Some(ref indices) = projection {
                            if indices.is_empty() {
                                // For COUNT(*) queries, create empty batch with just row count
                                let empty_columns: Vec<Arc<dyn Array>> = vec![];
                                match arrow::record_batch::RecordBatch::try_new_with_options(
                                    schema.clone(),
                                    empty_columns,
                                    &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()))
                                ) {
                                    Ok(pb) => pb,
                                    Err(e) => {
                                        yield Err(datafusion::common::DataFusionError::Execution(format!("Empty batch error: {}", e)));
                                        continue;
                                    }
                                }
                            } else {
                                // Project only the requested columns
                                let columns: Vec<_> = indices.iter()
                                    .map(|&i| batch.column(i).clone())
                                    .collect();
                                match arrow::record_batch::RecordBatch::try_new(schema.clone(), columns) {
                                    Ok(pb) => pb,
                                    Err(e) => {
                                        yield Err(datafusion::common::DataFusionError::Execution(format!("Projection error: {}", e)));
                                        continue;
                                    }
                                }
                            }
                        } else {
                            batch
                        };
                        
                        yield Ok(projected_batch);
                    }
                },
                Err(e) => {
                    yield Err(e);
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, stream)))
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Handle projection properly
        if let Some(projection_indices) = projection {
            // Create projected schema
            let full_fields = self.schema.fields();
            let projected_fields: Vec<FieldRef> = projection_indices.iter()
                .map(|&i| full_fields[i].clone())
                .collect();
            let projected_schema = Arc::new(arrow::datatypes::Schema::new(projected_fields));
            
            Ok(Arc::new(DirectoryExecutionPlan::new_with_projection(
                self.clone(),
                projected_schema,
                Some(projection_indices.clone())
            )))
        } else {
            // No projection, return all columns
            Ok(Arc::new(DirectoryExecutionPlan::new(self.clone())))
        }
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

    #[tokio::test]
    async fn test_directory_table_provider_interface() {
        let table_path = "/tmp/test_directory_table".to_string();
        let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
        let directory_table = DirectoryTable::new(table);
        
        // Test TableProvider interface
        assert_eq!(directory_table.table_type(), TableType::Base);
        
        // Test that scan() method returns a valid execution plan
        let mock_session = datafusion::execution::context::SessionContext::new();
        let session_state = mock_session.state();
        let result = directory_table.scan(&session_state, None, &[], None).await;
        
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.name(), "DirectoryExecutionPlan");
    }
}
