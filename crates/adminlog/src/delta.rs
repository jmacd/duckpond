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
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    pub node_id: String,  // Hex encoded unsigned (partition key, a directory name)
    pub timestamp: i64,   // Microsecond precision
    pub version: i64,     // Incrementing
    pub content: Vec<u8>, // Content
}

trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    fn for_delta() -> Vec<DeltaStructField> {
	let afs = Self::for_arrow();

	afs.into_iter().map(|af| {
	    let prim_type = match af.data_type() {
		DataType::Timestamp(TimeUnit::Microsecond, None) => PrimitiveType::Timestamp,
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

impl ForArrow for Record {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
            Arc::new(Field::new("timestamp",
				DataType::Timestamp(TimeUnit::Microsecond, None),
				false)),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, false)),
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

    // Create a record batch with a new log entry
    let items = vec![
	Record {
	    node_id: format!("{:#16x}", 0),
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
    let (_table, stream) = DeltaOps(table).load().with_columns(["timestamp", "version"]).await?;

    let data = collect_sendable_stream(stream).await?;

    arrow::util::pretty::print_batches(&data)?;

    Ok(())
}

// pub async fn get_last_record(table_path: &str) -> Result<Record, error::Error> {
//     let batch = get_last_record_batch(table_path).await?;

//     let all: Vec<Record> = serde_arrow::from_record_batch(&batch)?;

//     if all.len() != 1 {
// 	Err(error::Error::Missing)
//     } else {
// 	Ok(all.into_iter().next().expect("have one record"))
//     }
// }

// async fn get_last_record_batch(table_path: &str) -> Result<RecordBatch, error::Error> {
//     use deltalake::datafusion::prelude::*;

//     // Open the table
//     let table = open_table(table_path).await?;

//     // Create a DataFusion session
//     let ctx = SessionContext::new();

//     // Register the Delta table
//     ctx.register_table("my_table", Arc::new(table.clone()))?;

//     // Execute a query to sort by timestamp (or any relevant column) and get the last row
//     // This assumes you have a timestamp column named "timestamp"
//     let df = ctx
//         .sql("SELECT * FROM my_table ORDER BY timestamp DESC LIMIT 1")
//         .await?;

//     // Execute and collect the results
//     let results = df.collect().await?;

//     if results.is_empty() || results[0].num_rows() == 0 {
//         Err(error::Error::Missing)
//     } else {
//         Ok(results[0].clone())
//     }
// }

// /// Adds a new log entry to the Delta Lake table
// pub async fn add_log_entry(
//     table_path: &str,
//     message: &str,
// ) -> Result<(), error::Error> {
//     // Open the table
//     let table = open_table(table_path).await?;

//     // Prepare the schema
//     let schema = table.get_schema()?;
//     let arrow_schema = Arc::new(Schema::try_from(schema)?);

//     // Create a new log entry
//     let timestamp = Utc::now().to_rfc3339();

//     // Create record batch with our log entry
//     let batch = RecordBatch::try_new(
//         arrow_schema,
//         vec![
//             Arc::new(StringArray::from(vec![timestamp])),
//             Arc::new(StringArray::from(vec![message])),
//         ],
//     )?;

//     // Write the record batch to the table
//     let ops = DeltaOps::from(table);
//     ops.write(vec![batch])
//         .with_save_mode(SaveMode::Append)
//         .await?;

//     Ok(())
// }

// /// Reads the message from the last row of the last appended batch of data.
// /// This function identifies the files added in the very last write transaction
// /// and reads only those files, avoiding a full table scan.
// async fn read_last_appended_log_entry_directly(table_path: &str) -> Result<(), error::Error> {
//     let default_message = "No log entries found or last op not an append".to_string();

//     let mut table = open_table(table_path).await?;

//     let log_store = table.log_store();

//     // let history = table.history(None).await?;
//     // if history.is_empty() {
//     //     return Ok(default_message);
//     // }
//     // let mut last_write_commit_timestamp: Option<i64> = None;
//     // // Iterate history from newest to oldest to find the last relevant WRITE operation
//     // for commit_info in history.into_iter().rev() {
//     //     if commit_info.operation == Some("WRITE".to_string()) {
//     //         let wrote_data = commit_info.operation_parameters.as_ref()
//     //             .and_then(|params| params.get("numOutputRows"))
//     //             .and_then(|val| val.as_str())
//     //             .and_then(|s| s.parse::<i64>().ok())
//     //             .map_or(false, |count| count > 0);
//     //         let is_append_mode = commit_info.operation_parameters.as_ref()
//     //             .and_then(|params| params.get("mode"))
//     //             .and_then(|val| val.as_str())
//     //             .map_or(false, |mode_str| mode_str == "Append");
//     //         // If it's a write operation that produced rows, or explicitly an append, or a blind append
//     //         if wrote_data || is_append_mode || commit_info.is_blind_append == Some(true) {
//     //             last_write_commit_timestamp = commit_info.timestamp;
//     //             break;
//     //         }
//     //     }
//     // }
//     // let target_timestamp = match last_write_commit_timestamp {
//     //     Some(v) => v,
//     //     None => return Ok(default_message),
//     // };

//     let protocol = table.protocol()?;
//     let version = log_store.get_latest_version(0).await?;

//     let commit_log_bytes = log_store.read_commit_entry(version)
// 	.await?
// 	.ok_or_else(|| error::Error::Missing)?;

//     let actions = deltalake::logstore::get_actions(version, commit_log_bytes).await?;

//     let added_files: Vec<_> = actions.into_iter().filter_map(|action| {
//         if let Action::Add(add_file) = action {
//             if add_file.data_change { // Ensure it's an Add action that changes data
//                 Some(add_file)
//             } else {
//                 None
//             }
//         } else {
//             None
//         }
//     }).collect();

//     if added_files.is_empty() {
//         // This could happen if a "WRITE" operation didn't actually add data files
//         // (e.g., only metadata changes), though our checks above aim to prevent this.
//         return Err(error::Error::Missing);
//     }

//     // Create a DataFusion execution plan to read only these specific files
//     let session_ctx = SessionContext::new(); // Create a new context for this operation
//     let config = deltalake::delta_datafusion::DeltaScanConfig::default();
//     let provider = DeltaTableProvider::try_new(table.snapshot()?.clone(), log_store, config)?
//         .with_files(added_files);

//     session_ctx.register_table("last_appended_files", Arc::new(provider))?;
//     let df = session_ctx.sql("SELECT * FROM last_appended_files").await?;
//     let data_exec_plan = df.create_physical_plan().await?;

//     let task_ctx = session_ctx.task_ctx();

//     let batches = collect(data_exec_plan, task_ctx).await?;

//     // Extract the message from the last row of the last batch
//     if let Some(last_batch) = batches.last() {
//         if last_batch.num_rows() > 0 {
//             let message_column = last_batch
//                 .column_by_name("message")
//                 .ok_or_else(|| Box::<dyn std::error::Error>::from("Message column not found in the last batch"))?;

//             if let Some(message_array) = message_column.as_any().downcast_ref::<StringArray>() {
//                 let last_row_index_in_batch = last_batch.num_rows() - 1;
//                 return Ok(message_array.value(last_row_index_in_batch).to_string());
//             }
//         }
//     }

//     Ok(default_message)
// }
