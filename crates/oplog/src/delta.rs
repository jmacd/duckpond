use std::collections::HashMap;
use super::error;
use arrow::datatypes::{
    DataType,
    Field,
    FieldRef,
    TimeUnit,
    SchemaRef,
};
use std::pin::Pin;
use std::any::Any;
use arrow_array::{
     RecordBatch,
//     Int64Array,
//     BinaryArray,
//     StringArray,
//     TimestampMicrosecondArray,
};
use chrono::Utc;

use datafusion::catalog::{Session, TableProvider};

//use datafusion::prelude::SessionContext;
// use datafusion::physical_plan::collect;
use deltalake::protocol::SaveMode;
use deltalake::{
    //open_table,
    DeltaOps,
};
use deltalake::operations::collect_sendable_stream;
use deltalake::kernel::{
    // Action,
    DataType as DeltaDataType,
    StructField as DeltaStructField,
    PrimitiveType,
};

use std::sync::Arc;
use arrow::ipc::writer::{
    IpcWriteOptions,
    StreamWriter,
};

use serde::{Deserialize, Serialize};

// use std::any::Any;
// use std::fmt;
// use std::pin::Pin;
// use std::sync::Arc;
// use std::task::{Context, Poll};

// use arrow::array::{Int32Array, RecordBatch, StringArray};
// use 1arrow::datatypes::{DataType, Field, Schema, SchemaRef};
// use arrow::ipc::writer::StreamWriter;
// use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
// use bytes::Bytes;
// use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{
    EquivalenceProperties,
};
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    //ExecutionPlanProperties,
    PlanProperties,
    Partitioning,
    execution_plan::EmissionType,
    //execution_plan::Boundedness,
    stream::RecordBatchStreamAdapter,
};
use datafusion::prelude::*;
use futures::{Stream, StreamExt};

trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    fn for_delta() -> Vec<DeltaStructField> {
	let afs = Self::for_arrow();

	afs.into_iter().map(|af| {
	    let prim_type = match af.data_type() {
		DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
		DataType::Utf8 => PrimitiveType::String,
		DataType::Binary => PrimitiveType::Binary,
		DataType::Int64 => PrimitiveType::Long,
		_ => panic!("configure this type"),
	    };

	    DeltaStructField{
		name: af.name().to_string(),
		data_type: DeltaDataType::Primitive(prim_type),
		nullable: af.is_nullable(),
		metadata: HashMap::new(),
	    }
	}).collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    pub node_id: String,  // Hex encoded unsigned (partition key, a directory name)
    pub timestamp: i64,   // Microsecond precision
    pub version: i64,     // Incrementing
    pub content: Vec<u8>, // Content
}

impl ForArrow for Record {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
            Arc::new(Field::new("timestamp",
				DataType::Timestamp(
				    // Delta requires "UTC"
				    // Arrow recommends "+00:00"
				    TimeUnit::Microsecond,
				    Some("UTC".into())),
				false)),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, false)),
	]
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    pub name: String,
    pub node_id: String,
}

impl ForArrow for Entry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
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
        .with_partition_columns(["node_id"])
        .await?;

    let entries = vec![
	Entry{
	    name: "hello".into(),
	    node_id: nodestr(5678),
	},
	Entry{
	    name: "world".into(),
	    node_id: nodestr(1234),
	},
    ];


    // Create a record batch with a new log entry
    let ibytes = serde_arrow::to_record_batch(&Entry::for_arrow(), &entries)?;
    let items = vec![
	Record {
	    node_id: nodestr(0),
	    timestamp: Utc::now().timestamp_micros(),
	    version: 0,
	    content: encode_batch_to_buffer(ibytes)?,
	},
    ];

    let batch =
	serde_arrow::to_record_batch(&Record::for_arrow(), &items)?;

    // Write the record batch to the table
    let table = DeltaOps(table).write(vec![batch])
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
    let mut writer = StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buffer)
}

/// A custom table that simulates receiving byte arrays from another system
/// and converts them to RecordBatches using Arrow IPC
#[derive(Debug)]
pub struct ByteStreamTable {
    schema: SchemaRef,
}

impl ByteStreamTable {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        
        Ok(Arc::new(ByteStreamExec::new(
            projected_schema,
            self.get_byte_stream().await,
        )))
    }
    
    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Unsupported; _filters.len()])
    }
}

/// Execution plan that reads from byte stream and converts to RecordBatches
pub struct ByteStreamExec {
    schema: SchemaRef,
    byte_stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>,
    properties: PlanProperties,
}

impl ByteStreamExec {
    pub fn new(
        schema: SchemaRef,
        byte_stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send + 'static>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
	    EmissionType::Both,
            ExecutionMode::Bounded,
        );
        
        Self {
            schema,
            byte_stream,
            properties,
        }
    }
}

impl std::fmt::Debug for ByteStreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteStreamExec")
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for ByteStreamExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
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
        // Create a stream that converts bytes to RecordBatches
        let schema = self.schema.clone();
        
        // Note: In a real implementation, you'd need to handle the stream differently
        // since we can't move self.byte_stream. You'd typically recreate the stream
        // or use a factory pattern.
        let byte_stream = self.create_new_byte_stream();
        
        let record_batch_stream = byte_stream.map(move |bytes_result| {
            match bytes_result {
                Ok(bytes) => {
                    // Convert bytes to RecordBatch using Arrow IPC reader
                    let cursor = std::io::Cursor::new(bytes);
                    let mut reader = StreamReader::try_new(cursor, None)
                        .map_err(|e| DataFusionError::ArrowError(e, None))?;
                    
                    // Read the first (and typically only) batch from this IPC stream
                    match reader.next() {
                        Some(batch_result) => batch_result.map_err(|e| DataFusionError::ArrowError(e, None)),
                        None => plan_err!("No batch found in IPC stream"),
                    }
                }
                Err(e) => Err(e),
            }
        });
        
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            record_batch_stream,
        )))
    }
}

impl ByteStreamExec {
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new DataFusion context
    let ctx = SessionContext::new();
    
    // Register our custom table
    let byte_stream_table = Arc::new(ByteStreamTable::new());
    ctx.register_table("stream_table", byte_stream_table)?;
    
    // Query the table
    let df = ctx.sql("SELECT * FROM stream_table").await?;
    let results = df.collect().await?;
    
    // Print results
    for batch in results {
        println!("{}", arrow::util::pretty::pretty_format_batches(&[batch])?);
    }
    
    // More complex query
    println!("\n--- Filtered Query ---");
    let df = ctx.sql("SELECT id, name FROM stream_table WHERE id > 15").await?;
    let results = df.collect().await?;
    
    for batch in results {
        println!("{}", arrow::util::pretty::pretty_format_batches(&[batch])?);
    }
    
    Ok(())
}
