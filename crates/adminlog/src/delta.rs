
use super::error;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::Schema;
use chrono::Utc;
use deltalake::protocol::SaveMode;
use deltalake::{open_table, DeltaOps};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Record {
    pub timestamp: String,
    pub message: String,
}

/// Creates a new Delta table with the required schema
pub async fn create_table(table_path: &str) -> Result<(), error::Error> {
    // Create the table, give it a schema, drop it.
    let ops = DeltaOps::try_from_uri(table_path).await?;
    
    _ = ops.create()
        .with_column("timestamp", DeltaDataType::Primitive(PrimitiveType::String), false, None)
        .with_column("message", DeltaDataType::Primitive(PrimitiveType::String), false, None)
        .await?;

    Ok(())
}

pub async fn get_last_record(table_path: &str) -> Result<Record, error::Error> {
    let batch = get_last_record_batch(table_path).await?;

    let all: Vec<Record> = serde_arrow::from_record_batch(&batch)?;

    if all.len() != 1 {
	Err(error::Error::Missing)
    } else {
	Ok(all.into_iter().next().expect("have one record"))
    }
}

async fn get_last_record_batch(table_path: &str) -> Result<RecordBatch, error::Error> {
    use deltalake::datafusion::prelude::*;

    // Open the table
    let table = open_table(table_path).await?;

    // Create a DataFusion session
    let ctx = SessionContext::new();

    // Register the Delta table
    ctx.register_table("my_table", Arc::new(table.clone()))?;

    // Execute a query to sort by timestamp (or any relevant column) and get the last row
    // This assumes you have a timestamp column named "timestamp"
    let df = ctx
        .sql("SELECT * FROM my_table ORDER BY timestamp DESC LIMIT 1")
        .await?;

    // Execute and collect the results
    let results = df.collect().await?;

    if results.is_empty() || results[0].num_rows() == 0 {
        Err(error::Error::Missing)
    } else {
        Ok(results[0].clone())
    }
}

/// Adds a new log entry to the Delta Lake table
pub async fn add_log_entry(
    table_path: &str,
    message: &str,
) -> Result<(), error::Error> {
    // Open the table
    let table = open_table(table_path).await?;

    // Prepare the schema
    let schema = table.get_schema()?;
    let arrow_schema = Arc::new(Schema::try_from(schema)?);

    // Create a new log entry
    let timestamp = Utc::now().to_rfc3339();

    // Create record batch with our log entry
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringArray::from(vec![timestamp])),
            Arc::new(StringArray::from(vec![message])),
        ],
    )?;

    // Write the record batch to the table
    let ops = DeltaOps::from(table);
    ops.write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    Ok(())
}
