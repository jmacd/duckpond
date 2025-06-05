use super::error;
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef, TimeUnit};
use arrow_array::{Array, RecordBatch};
use chrono::Utc;
use std::any::Any;
use std::collections::HashMap;

use datafusion::catalog::{Session, TableProvider};

use deltalake::DeltaOps;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use deltalake::operations::collect_sendable_stream;
use deltalake::protocol::SaveMode;

use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::Boundedness, execution_plan::EmissionType, stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;

pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    fn for_delta() -> Vec<DeltaStructField> {
        let afs = Self::for_arrow();

        afs.into_iter()
            .map(|af| {
                let prim_type = match af.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
                    DataType::Utf8 => PrimitiveType::String,
                    DataType::Binary => PrimitiveType::Binary,
                    DataType::Int64 => PrimitiveType::Long,
                    _ => panic!("configure this type"),
                };

                DeltaStructField {
                    name: af.name().to_string(),
                    data_type: DeltaDataType::Primitive(prim_type),
                    nullable: af.is_nullable(),
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    pub part_id: String,  // Hex encoded unsigned (partition key, a directory name)
    pub timestamp: i64,   // Microsecond precision
    pub version: i64,     // Incrementing
    pub content: Vec<u8>, // Content
}

impl ForArrow for Record {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(
                    // Delta requires "UTC"
                    // Arrow recommends "+00:00"
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                false,
            )),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, false)),
        ]
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    pub name: String,
    pub part_id: String,
}

impl ForArrow for Entry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
        ]
    }
}

/// Creates a new Delta table with the required schema

pub async fn create_table(table_path: &str) -> Result<(), error::Error> {
    // Create the table, give it a schema, drop it.
    let table = DeltaOps::try_from_uri(table_path).await?;
    let table = table
        .create()
        .with_columns(Record::for_delta())
        .with_partition_columns(["part_id"])
        .await?;

    let entries = vec![
        Entry {
            name: "hello".into(),
            part_id: nodestr(5678),
        },
        Entry {
            name: "world".into(),
            part_id: nodestr(1234),
        },
    ];

    // Create a record batch with a new log entry
    let ibytes = serde_arrow::to_record_batch(&Entry::for_arrow(), &entries)?;
    let items = vec![Record {
        part_id: nodestr(0),
        timestamp: Utc::now().timestamp_micros(),
        version: 0,
        content: encode_batch_to_buffer(ibytes)?,
    }];

    let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &items)?;

    // Write the record batch to the table
    let table = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    // Write the record batch to the table
    let (_table, stream) = DeltaOps(table)
        .load()
        .with_columns(["timestamp", "version"])
        .await?;

    let data = collect_sendable_stream(stream).await?;

    arrow::util::pretty::print_batches(&data)?;

    Ok(())
}

fn nodestr(id: u64) -> String {
    format!("{:016x}", id)
}

fn encode_batch_to_buffer(batch: RecordBatch) -> Result<Vec<u8>, parquet::errors::ParquetError> {
    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buffer)
}

/// A custom table that reads byte arrays from Delta Lake content field
/// and converts them to RecordBatches using Arrow IPC
#[derive(Debug, Clone)]
pub struct ByteStreamTable {
    schema: SchemaRef,
    table_path: String,
}

impl ByteStreamTable {
    pub fn new(schema: SchemaRef, table_path: String) -> Self {
        Self { schema, table_path }
    }
}

#[async_trait]
impl TableProvider for ByteStreamTable {
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
        Ok(Arc::new(ByteStreamExec::new(self.clone())))
    }
}

/// Execution plan that reads from byte stream and converts to RecordBatches
pub struct ByteStreamExec {
    table: ByteStreamTable,
    properties: PlanProperties,
}

impl ByteStreamExec {
    pub fn new(table: ByteStreamTable) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(table.schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Self { table, properties }
    }
}

impl std::fmt::Debug for ByteStreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteStreamExec")
            .field("schema", &self.table.schema)
            .finish()
    }
}

impl DisplayAs for ByteStreamExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "ByteStreamExec")
            }
        }
    }
}

impl ExecutionPlan for ByteStreamExec {
    fn name(&self) -> &'static str {
        "ByteStreamExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
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
        let table_path = self.table.table_path.clone();
        let schema = self.table.schema.clone();

        let stream = async_stream::stream! {
            let batches = Self::load_delta_stream(&table_path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)));

            match batches {
                Ok(mut delta_stream) => {
                    while let Some(batch_result) = delta_stream.next().await {
                        let results = batch_result
                            .map_err(|e| DataFusionError::External(Box::new(e)))
                            .and_then(Self::extract_content_batches);

                        match results {
                            Ok(inner_batches) => {
                                for batch in inner_batches {
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

impl ByteStreamExec {
    /// Load Delta stream in a functional style
    async fn load_delta_stream(
        table_path: &str,
    ) -> Result<SendableRecordBatchStream, deltalake::DeltaTableError> {
        let delta_ops = DeltaOps::try_from_uri(table_path).await?;
        let (_table, stream) = delta_ops.load().await?;
        Ok(stream)
    }

    /// Extract and process content batches from a Delta Lake record batch
    fn extract_content_batches(batch: RecordBatch) -> Result<Vec<Result<RecordBatch>>> {
        batch
            .column_by_name("content")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>())
            .map(Self::process_binary_array)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Process binary array using functional style
    fn process_binary_array(
        binary_array: &arrow_array::BinaryArray,
    ) -> Result<Vec<Result<RecordBatch>>> {
        Ok((0..binary_array.len())
            .filter_map(|i| binary_array.value(i).get(0..))
            .map(Self::deserialize_ipc_bytes)
            .collect())
    }

    /// Deserialize IPC bytes to record batches
    fn deserialize_ipc_bytes(bytes: &[u8]) -> Result<RecordBatch> {
        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::ArrowError(e, None))?
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("No batches found in IPC stream".to_string()))
    }
}
