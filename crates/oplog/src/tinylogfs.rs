use crate::delta::ForArrow;
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// DataFusion imports for table providers
use arrow_array::{Array, RecordBatch};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::Boundedness, execution_plan::EmissionType, stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;
use std::any::Any;

/// Filesystem entry stored in the operation log
/// This represents a single filesystem operation (create, update, delete)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OplogEntry {
    /// Hex-encoded partition ID (parent directory for files/symlinks, self for directories)
    pub part_id: String,
    /// Hex-encoded NodeID from TinyFS
    pub node_id: String,
    /// Type of filesystem entry: "file", "directory", or "symlink"
    pub file_type: String,
    /// Extensible metadata attributes (JSON-encoded for simplicity)
    pub metadata: String,
    /// Type-specific content:
    /// - For files: raw file data
    /// - For symlinks: target path
    /// - For directories: Arrow IPC encoded DirectoryEntry records
    pub content: Vec<u8>,
}

impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
            Arc::new(Field::new("file_type", DataType::Utf8, false)),
            Arc::new(Field::new("metadata", DataType::Utf8, false)), // JSON string
            Arc::new(Field::new("content", DataType::Binary, false)),
        ]
    }

    fn for_delta() -> Vec<DeltaStructField> {
        vec![
            DeltaStructField {
                name: "part_id".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::String),
                nullable: false,
                metadata: HashMap::new(),
            },
            DeltaStructField {
                name: "node_id".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::String),
                nullable: false,
                metadata: HashMap::new(),
            },
            DeltaStructField {
                name: "file_type".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::String),
                nullable: false,
                metadata: HashMap::new(),
            },
            DeltaStructField {
                name: "metadata".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::String),
                nullable: false,
                metadata: HashMap::new(),
            },
            DeltaStructField {
                name: "content".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::Binary),
                nullable: false,
                metadata: HashMap::new(),
            },
        ]
    }
}

/// Directory entry for nested storage within OplogEntry content
/// Used when OplogEntry.file_type == "directory"
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name within the directory
    pub name: String,
    /// Hex-encoded NodeID of the child
    pub child_node_id: String,
}

impl ForArrow for DirectoryEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("child_node_id", DataType::Utf8, false)),
        ]
    }

    fn for_delta() -> Vec<DeltaStructField> {
        vec![
            DeltaStructField {
                name: "name".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::String),
                nullable: false,
                metadata: HashMap::new(),
            },
            DeltaStructField {
                name: "child_node_id".to_string(),
                data_type: DeltaDataType::Primitive(PrimitiveType::String),
                nullable: false,
                metadata: HashMap::new(),
            },
        ]
    }
}

/// Table provider for OplogEntry records
/// This enables SQL queries over filesystem operations stored in Delta Lake
#[derive(Debug, Clone)]
pub struct OplogEntryTable {
    schema: SchemaRef,
    table_path: String,
}

impl OplogEntryTable {
    pub fn new(table_path: String) -> Self {
        // Use OplogEntry schema since that's what we want to expose via SQL
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        Self { schema, table_path }
    }
}

#[async_trait]
impl TableProvider for OplogEntryTable {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use a custom OplogEntry execution plan
        Ok(Arc::new(OplogEntryExec::new(
            self.table_path.clone(),
            self.schema.clone(),
        )))
    }
}

/// Execution plan that reads OplogEntry records from Delta Lake Record.content field
pub struct OplogEntryExec {
    table_path: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl OplogEntryExec {
    pub fn new(table_path: String, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Self {
            table_path,
            schema,
            properties,
        }
    }

    /// Extract OplogEntry records from Record batch content field
    fn extract_oplog_entries(batch: RecordBatch) -> Result<Vec<Result<RecordBatch>>> {
        batch
            .column_by_name("content")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>())
            .map(Self::process_content_array)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Process content binary array to extract OplogEntry records
    fn process_content_array(
        binary_array: &arrow_array::BinaryArray,
    ) -> Result<Vec<Result<RecordBatch>>> {
        Ok((0..binary_array.len())
            .filter_map(|i| binary_array.value(i).get(0..))
            .map(Self::deserialize_oplog_entry_bytes)
            .collect())
    }

    /// Deserialize OplogEntry IPC bytes to record batches
    fn deserialize_oplog_entry_bytes(bytes: &[u8]) -> Result<RecordBatch> {
        use arrow::ipc::reader::StreamReader;

        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))?;

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "No OplogEntry batches found in IPC stream".to_string(),
                )
            })
    }
}

impl std::fmt::Debug for OplogEntryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OplogEntryExec")
            .field("table_path", &self.table_path)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for OplogEntryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "OplogEntryExec: {}", self.table_path)
            }
        }
    }
}

impl ExecutionPlan for OplogEntryExec {
    fn name(&self) -> &'static str {
        "OplogEntryExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table_path = self.table_path.clone();
        let schema = self.schema.clone();

        let stream = async_stream::stream! {
            // Load Delta Lake records and extract OplogEntry data
            match Self::load_delta_stream(&table_path).await {
                Ok(mut delta_stream) => {
                    while let Some(batch_result) = delta_stream.next().await {
                        let results = batch_result
                            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
                            .and_then(Self::extract_oplog_entries);

                        match results {
                            Ok(oplog_batches) => {
                                for batch in oplog_batches {
                                    yield batch;
                                }
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                }
                Err(e) => yield Err(e),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl OplogEntryExec {
    /// Load Delta stream in a functional style
    async fn load_delta_stream(table_path: &str) -> Result<SendableRecordBatchStream> {
        use deltalake::DeltaOps;

        let delta_ops = DeltaOps::try_from_uri(table_path)
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let (_table, stream) = delta_ops
            .load()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        Ok(stream)
    }
}

/// Table provider for DirectoryEntry records  
/// This enables SQL queries over directory contents nested within OplogEntry
#[derive(Debug, Clone)]
pub struct DirectoryEntryTable {
    schema: SchemaRef,
    table_path: String,
}

impl DirectoryEntryTable {
    pub fn new(table_path: String) -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(DirectoryEntry::for_arrow()));
        Self { schema, table_path }
    }
}

#[async_trait]
impl TableProvider for DirectoryEntryTable {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // For DirectoryEntry, we need a custom execution plan that:
        // 1. Reads OplogEntry records where file_type == "directory"
        // 2. Deserializes the content field as Arrow IPC DirectoryEntry records
        // 3. Returns the nested DirectoryEntry data
        let byte_stream_table =
            crate::delta::ByteStreamTable::new(self.schema.clone(), self.table_path.clone());
        Ok(Arc::new(crate::delta::ByteStreamExec::new(
            byte_stream_table,
        )))
    }
}

/// Creates a new Delta table with Record schema and initializes it with a root directory OplogEntry
pub async fn create_oplog_table(table_path: &str) -> Result<(), crate::error::Error> {
    use chrono::Utc;
    use deltalake::DeltaOps;
    use deltalake::protocol::SaveMode;
    use uuid::Uuid;

    // Create the table with Record schema (same as the original delta.rs approach)
    let table = DeltaOps::try_from_uri(table_path).await?;
    let table = table
        .create()
        .with_columns(crate::delta::Record::for_delta())
        .with_partition_columns(["part_id"])
        .await?;

    // Create a root directory entry as the initial OplogEntry
    let root_node_id = Uuid::new_v4().to_string();
    let root_entry = OplogEntry {
        part_id: root_node_id.clone(), // Root directory is its own partition
        node_id: root_node_id.clone(),
        file_type: "directory".to_string(),
        metadata: "{}".to_string(),                  // Empty JSON metadata
        content: encode_directory_entries(&vec![])?, // Empty directory
    };

    // Serialize the OplogEntry as a Record for storage
    let record = crate::delta::Record {
        part_id: root_node_id.clone(), // Use the same part_id
        timestamp: Utc::now().timestamp_micros(),
        version: 0,
        content: encode_oplog_entry_to_buffer(root_entry)?,
    };

    // Create a record batch and write it
    let batch = serde_arrow::to_record_batch(&crate::delta::Record::for_arrow(), &[record])?;
    let _table = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    Ok(())
}

/// Encode OplogEntry as Arrow IPC bytes for storage in Record.content
fn encode_oplog_entry_to_buffer(entry: OplogEntry) -> Result<Vec<u8>, crate::error::Error> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[entry])?;

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buffer)
}

/// Encode DirectoryEntry records as Arrow IPC bytes for storage in OplogEntry.content
fn encode_directory_entries(entries: &Vec<DirectoryEntry>) -> Result<Vec<u8>, crate::error::Error> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), entries)?;

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buffer)
}
