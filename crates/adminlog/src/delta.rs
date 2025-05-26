use std::collections::HashMap;
use super::error;
use arrow::datatypes::{
    DataType,
    Field,
    FieldRef,
    TimeUnit,
    //Fields
};
// use arrow_array::{
//     RecordBatch,
//     Int64Array,
//     BinaryArray,
//     StringArray,
//     TimestampMicrosecondArray,
// };
//use arrow_schema::Schema;
use chrono::Utc;
//use datafusion::prelude::SessionContext;
// use datafusion::physical_plan::collect;
use deltalake::protocol::SaveMode;
use deltalake::{
    //open_table,
    DeltaOps,
};
use deltalake::operations::collect_sendable_stream;
//use deltalake::delta_datafusion::DeltaTableProvider;
use deltalake::kernel::{
    // Action,
    DataType as DeltaDataType,
    StructField as DeltaStructField,
    PrimitiveType,
};

use std::sync::Arc;
use arrow_array::{Int64Array, RecordBatch, ArrayRef};
use arrow_schema::{Schema, Field, DataType};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use std::sync::Arc;
use serde::{Deserialize, Serialize};

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

    let entrybatch =
	serde_arrow::to_record_batch(&Record::for_arrow(), &entries)?;

    // Create a record batch with a new log entry
    let items = vec![
	Record {
	    node_id: nodestr(0),
	    timestamp: Utc::now().timestamp_micros(),
	    version: 0,
	    content: b"1234".to_vec(),
	},
    ];

    let batch =
	serde_arrow::to_record_batch(&Record::for_arrow(), &items)?;

    // Write the record batch to the table
    let table = DeltaOps(table).write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    //let mut ctx = SessionContext::new();
    // let table = open_table(table_path)
    // 	.await
    // 	.unwrap();

    // Write the record batch to the table
    let (_table, stream) = DeltaOps(table)
	.load()
	//.with_columns(["timestamp", "version"])
	.await?;

    let data = collect_sendable_stream(stream).await?;

    arrow::util::pretty::print_batches(&data)?;

    Ok(())
}

fn nodestr(id: u64) -> String {
    format!("{:016x}", id) 
}

fn encode_batch_to_buffer(batch: &RecordBatch) -> Result<Vec<u8>, parquet::errors::ParquetError> {
    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build(); // Or configure as needed
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buffer)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example RecordBatch
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids.clone()])?;

    let parquet_buffer = encode_batch_to_buffer(&batch)?;

    println!("Encoded Parquet data to in-memory buffer of size: {} bytes", parquet_buffer.len());

    // You can now use parquet_buffer, for example, to write to a file or send over a network.
    // To verify, you could read it back:
    // let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes::Bytes::from(parquet_buffer), 1024)?;
    // let read_batch = reader.next().unwrap()?;
    // assert_eq!(batch, read_batch);

    Ok(())
}
