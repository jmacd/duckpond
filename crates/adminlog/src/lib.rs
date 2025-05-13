
use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use deltalake::arrow::array::ArrayRef;
use deltalake::kernel::StructField;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use futures::StreamExt;

/// Error type for AdminLog operations
#[derive(Debug)]
pub enum AdminLogError {
    DeltaError(DeltaTableError),
    ArrowError(arrow::error::ArrowError),
    LogClosed,
    ReadError(String),
    Other(String),
}

impl std::fmt::Display for AdminLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdminLogError::DeltaError(err) => write!(f, "Delta Lake error: {}", err),
            AdminLogError::ArrowError(err) => write!(f, "Arrow error: {}", err),
            AdminLogError::LogClosed => write!(f, "Log is closed"),
            AdminLogError::ReadError(msg) => write!(f, "Failed to read log: {}", msg),
            AdminLogError::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl std::error::Error for AdminLogError {}

impl From<DeltaTableError> for AdminLogError {
    fn from(err: DeltaTableError) -> Self {
        AdminLogError::DeltaError(err)
    }
}

impl From<arrow::error::ArrowError> for AdminLogError {
    fn from(err: arrow::error::ArrowError) -> Self {
        AdminLogError::ArrowError(err)
    }
}

pub type Result<T> = std::result::Result<T, AdminLogError>;

/// Represents an administrative action log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// The timestamp of the action
    pub timestamp: DateTime<Utc>,
    
    /// The message describing the action
    pub message: String,
}

/// The AdminLog struct manages a log of administrative actions using a Delta table
pub struct AdminLog {
    table: Option<DeltaTable>,
    log_path: String,
}

impl AdminLog {
    /// Open an existing admin log or create a new one if it doesn't exist
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        
        // Check if the table exists
        let table = match DeltaTableBuilder::from_uri(&path_str).build() {
            Ok(mut existing_table) => {
                // Try to load the existing table
                match existing_table.load().await {
                    Ok(_) => Ok(existing_table),
                    Err(e) => {
                        // If the table doesn't exist, create it
                        if e.to_string().contains("No such file or directory") || 
                           e.to_string().contains("is not a Delta table") {
                            Self::create_table(&path_str).await
                        } else {
                            Err(e.into())
                        }
                    }
                }
            },
            Err(_) => {
                // If building the table failed, try to create it
                Self::create_table(&path_str).await
            }
        }?;

        Ok(Self { 
            table: Some(table),
            log_path: path_str,
        })
    }

    /// Create a new Delta table for the admin log
    async fn create_table(path: &str) -> Result<DeltaTable> {
        // Create a schema for our log entries
        let schema_fields = vec![
            StructField::new(
                "timestamp".to_string(), 
                deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp),
                false,
            ),
            StructField::new(
                "message".to_string(),
                deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                false,
            ),
        ];

        // Create the table
        let table = DeltaOps::try_from_uri(path)
            .await?
            .create()
            .with_columns(schema_fields)
            .with_table_name("admin_log")
            .with_comment("Administrative actions log")
            .await?;

        Ok(table)
    }

    /// Write a log entry to the admin log
    pub async fn write(&mut self, message: &str) -> Result<()> {
        // Make sure we have an open table
        let table = match &mut self.table {
            Some(table) => table,
            None => return Err(AdminLogError::LogClosed),
        };

        // Create a record batch with the log entry
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("message", DataType::Utf8, false),
        ]));

        let now = Utc::now();
        let timestamp = now.timestamp_millis();

        // Create arrays
        let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![timestamp]));
        let messages: ArrayRef = Arc::new(StringArray::from(vec![message]));

        // Create a record batch
        let batch = RecordBatch::try_new(schema, vec![timestamps, messages])?;
        
        // Write the batch to the table
        let updated_table = DeltaOps(table.clone())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await?;
        
        // Update our table reference
        self.table = Some(updated_table);
        
        Ok(())
    }

    /// Read the latest log entry from the admin log
    pub async fn read_latest(&self) -> Result<Option<LogEntry>> {
        let table = match &self.table {
            Some(table) => table,
            None => return Err(AdminLogError::LogClosed),
        };

        // Use DeltaOps to load the data
        let (_, mut batches) = DeltaOps(table.clone())
            .load()
            .with_limit(Some(1))
            .with_order_by(vec!["timestamp DESC"])
            .await?;

        // Collect the results
        let batch = match batches.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(AdminLogError::ReadError(e.to_string())),
            None => return Ok(None), // No entries
        };

        // Extract the timestamp and message
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // Get the timestamp column
        let timestamp_array = batch.column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| AdminLogError::ReadError("Failed to downcast timestamp column".into()))?;
        
        // Get the message column
        let message_array = batch.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| AdminLogError::ReadError("Failed to downcast message column".into()))?;

        // Extract values from the first row
        let timestamp_millis = timestamp_array.value(0);
        let message = message_array.value(0).to_string();
        
        // Convert timestamp to DateTime
        let timestamp = DateTime::from_timestamp_millis(timestamp_millis)
            .ok_or_else(|| AdminLogError::ReadError("Invalid timestamp value".into()))?;

        Ok(Some(LogEntry { timestamp, message }))
    }

    /// Read all log entries from the admin log
    pub async fn read_all(&self) -> Result<Vec<LogEntry>> {
        let table = match &self.table {
            Some(table) => table,
            None => return Err(AdminLogError::LogClosed),
        };

        // Use DeltaOps to load the data
        let (_, mut batches) = DeltaOps(table.clone())
            .load()
            .with_order_by(vec!["timestamp ASC"])
            .await?;

        let mut entries = Vec::new();

        // Process all batches
        while let Some(batch_result) = batches.next().await {
            let batch = batch_result.map_err(|e| AdminLogError::ReadError(e.to_string()))?;
            
            // Get the timestamp column
            let timestamp_array = batch.column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| AdminLogError::ReadError("Failed to downcast timestamp column".into()))?;
            
            // Get the message column
            let message_array = batch.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| AdminLogError::ReadError("Failed to downcast message column".into()))?;

            // Process each row
            for i in 0..batch.num_rows() {
                let timestamp_millis = timestamp_array.value(i);
                let message = message_array.value(i).to_string();
                
                // Convert timestamp to DateTime
                let timestamp = DateTime::from_timestamp_millis(timestamp_millis)
                    .ok_or_else(|| AdminLogError::ReadError("Invalid timestamp value".into()))?;

                entries.push(LogEntry { timestamp, message });
            }
        }

        Ok(entries)
    }

    /// Close the admin log
    pub fn close(&mut self) {
        self.table = None;
    }

    /// Commit changes to the admin log
    pub async fn commit(&mut self) -> Result<()> {
        // Delta tables commit automatically after write operations
        // This is a placeholder for any additional commit logic we might want to add
        Ok(())
    }
}