use crate::schema::{ForArrow, VersionedDirectoryEntry};
use arrow::datatypes::{SchemaRef, FieldRef};
use arrow::record_batch::RecordBatch;
use arrow::array::Array; // For is_null method
use std::sync::Arc;
use datafusion::datasource::TableProvider;
use log::info;

use async_trait::async_trait;
use datafusion::catalog::Session;
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
use log::debug;

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
#[derive(Clone)]
pub struct DirectoryTable {
    directory_node_id: Option<String>,  // Optional filter for specific directory @@@ When not?
    schema: SchemaRef,
    session_context: Arc<datafusion::execution::context::SessionContext>,
}

impl std::fmt::Debug for DirectoryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryTable")
            .field("directory_node_id", &self.directory_node_id)
            .field("schema", &self.schema)
            .field("session_context", &"<SessionContext>")
            .finish()
    }
}

impl DirectoryTable {
    /// Create a new DirectoryTable with specific options
    /// 
    /// # Arguments
    /// * `directory_node_id` - Optional specific directory node_id for partition pruning.
    ///   - `Some(node_id)` - Query specific directory with optimal partition pruning
    ///   - `None` - **NOT RECOMMENDED** - Scans all directories (full table scan)
    /// 
    /// # Performance Warning
    /// Using `None` for directory_node_id will result in full table scans and poor performance.
    /// This should only be used in test scenarios. Production code should always specify a node_id.
    pub fn new(directory_node_id: Option<String>, session_context: Arc<datafusion::execution::context::SessionContext>) -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(VersionedDirectoryEntry::for_arrow()));
        Self { 
            directory_node_id,
            schema,
            session_context,
        }
    }
    
    /// Convenience constructor for creating a partition-aware DirectoryTable
    /// This is the recommended way to create DirectoryTable instances in production.
    pub fn for_directory(directory_node_id: String, session_context: Arc<datafusion::execution::context::SessionContext>) -> Self {
        log::info!("📋 CREATING DirectoryTable for directory node_id: {}", directory_node_id);
        Self::new(Some(directory_node_id), session_context)
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
        // Use stored SessionContext (single context principle)

        // Build SQL query for directory entries
        let sql = if let Some(ref node_id) = self.directory_node_id {
            // For directories, node_id == part_id, so include both for proper partition pruning
            format!(
                "SELECT node_id, content FROM delta_table WHERE file_type = 'directory' AND node_id = '{}' AND part_id = '{}'",
                node_id, node_id
            )
        } else {
            "SELECT node_id, content FROM delta_table WHERE file_type = 'directory'".to_string()
        };

        // Execute query to get directory OplogEntry records
        let df = self.session_context.sql(&sql).await
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
            // Use DataFusion's SessionState directly from TaskContext (no SessionContext violation)
            // This avoids creating a new SessionContext during execution
            
            // Execute the directory scan using stored SessionContext
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

    #[tokio::test]
    async fn test_directory_table_creation() {
        // Create SessionContext for testing
        let temp_ctx_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let pond_path = temp_ctx_dir.path();
        let mut persistence = crate::OpLogPersistence::create(pond_path.to_str().unwrap()).await
            .expect("Failed to create OpLogPersistence");
        let mut tx = persistence.begin().await.expect("Failed to begin transaction");
        let session_ctx = tx.session_context().await.expect("Failed to get session context");
        
        // Test explicit unscoped DirectoryTable creation (for testing purposes only)
        let directory_table = DirectoryTable::new( None, session_ctx.clone());
        assert_eq!(directory_table.directory_node_id, None);
        
        // Test specific directory creation
        let specific_table = DirectoryTable::for_directory(
            "test_node_123".to_string(),
            session_ctx.clone()
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
        // Create SessionContext for testing
        let temp_ctx_dir2 = tempfile::tempdir().expect("Failed to create temp dir");
        let pond_path2 = temp_ctx_dir2.path();
        let mut persistence2 = crate::OpLogPersistence::create(pond_path2.to_str().unwrap()).await
            .expect("Failed to create OpLogPersistence");
        let mut tx2 = persistence2.begin().await.expect("Failed to begin transaction");
        let session_ctx2 = tx2.session_context().await.expect("Failed to get session context");
        
        let directory_table = DirectoryTable::for_directory("test_dir_node_123".to_string(), session_ctx2);
        
        // Test empty content
        let empty_content = &[];
        let result = directory_table.parse_directory_content(empty_content).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_directory_table_provider_interface() {
        // Create SessionContext for testing
        let temp_ctx_dir3 = tempfile::tempdir().expect("Failed to create temp dir");
        let pond_path3 = temp_ctx_dir3.path();
        let mut persistence3 = crate::OpLogPersistence::create(pond_path3.to_str().unwrap()).await
            .expect("Failed to create OpLogPersistence");
        let mut tx3 = persistence3.begin().await.expect("Failed to begin transaction");
        let session_ctx3 = tx3.session_context().await.expect("Failed to get session context");
        
        let directory_table = DirectoryTable::for_directory( "test_interface_node_456".to_string(), session_ctx3);
        
        // Test TableProvider interface
        assert_eq!(directory_table.table_type(), TableType::Base);
        
        // Test that scan() method returns a valid execution plan
        // NOTE: This test only validates the TableProvider interface, not actual data access
        // In real usage, SessionContext comes from the transaction state
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path();
        let mut persistence = crate::OpLogPersistence::create(pond_path.to_str().unwrap()).await
            .expect("Failed to create OpLogPersistence");
        let mut tx = persistence.begin().await.expect("Failed to begin transaction");
        let session_state = tx.session_context().await.expect("Failed to get session context").state();
        
        let result = directory_table.scan(&session_state, None, &[], None).await;
        
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.name(), "DirectoryExecutionPlan");
    }
}
