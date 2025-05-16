use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{Schema};
use chrono::Utc;
use deltalake::protocol::SaveMode;
use deltalake::{open_table, DeltaOps};
use std::sync::Arc;

use deltalake::DeltaTableError;
use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AdminLogError {
    #[error("table error")]
    DeltaError(#[from] DeltaTableError),

    #[error("table error")]
    FusionError(#[from] DataFusionError),

    #[error("missing data")]
    Missing,
}

async fn get_last_record(table_path: &str) -> Result<RecordBatch, AdminLogError> {
    use deltalake::datafusion::prelude::*;    
    
    // Open the table
    let table = open_table(table_path).await?;
    
    // Create a DataFusion session
    let ctx = SessionContext::new();
    
    // Register the Delta table
    ctx.register_table("my_table", Arc::new(table.clone())) ?;
    
    // Execute a query to sort by timestamp (or any relevant column) and get the last row
    // This assumes you have a timestamp column named "timestamp"
    let df = ctx.sql("SELECT * FROM my_table ORDER BY timestamp DESC LIMIT 1").await?;
    
    // Execute and collect the results
    let results = df.collect().await?;

    if results.is_empty() || results[0].num_rows() == 0 {
	Err(AdminLogError::Missing)
    } else {
        Ok(results[0].clone())
    }
}

/// Adds a new log entry to the Delta Lake table
async fn add_log_entry(table_path: &str, message: &str) -> Result<(), Box<dyn std::error::Error>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use arrow_schema::{DataType, Field};
    
    #[tokio::test]
    async fn test_adminlog() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let table_path = temp_dir.path().to_string_lossy().to_string();
        println!("Creating Delta Lake table at: {}", table_path);

        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
        ]);

        // Create initial empty table if it doesn't exist
        let table_result = open_table(&table_path).await;

        if table_result.is_err() {
            println!("Creating new Delta table...");
            // Initialize the table with the schema
            let table = deltalake::DeltaTableBuilder::from_uri(&table_path)
                .build()?;

            // Add initial log entry
            add_log_entry(&table_path, "Table created").await?;
        }

        // Read the latest message and append a new one
        let last_messages = get_last_record(&table_path).await?;
	
        println!("Last log message: {}", last_messages.display());

        // Add a new log entry
        add_log_entry(&table_path, "System check performed").await?;

        // Read again to confirm our change
        let updated_message = get_last_record(&table_path).await?;
        println!("New log message: {}", updated_message);
        // --8<-- [end:read_append]

        // --8<-- [start:additional_operations]
        // Add another log entry
        add_log_entry(&table_path, "Maintenance scheduled").await?;

        // Display table history
        let table = open_table(&table_path).await?;
        println!("Table version: {}", table.version());
        println!("Total log entries: {}", table.get_files().len());
        // --8<-- [end:additional_operations]

        println!("Example completed successfully");
        Ok(())
    }
}
