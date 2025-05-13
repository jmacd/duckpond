use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use deltalake::{DeltaTable, DeltaTableBuilder};
// Import writer directly
use deltalake::writer::{RecordBatchWriter, DeltaWriter};
use url::Url;

#[derive(Debug, Clone)]
pub struct AdminLogEntry {
    pub timestamp: DateTime<Utc>,
    pub action: String,
    pub details: Option<String>,
}

impl AdminLogEntry {
    pub fn new(action: impl Into<String>, details: Option<impl Into<String>>) -> Self {
        Self {
            timestamp: Utc::now(),
            action: action.into(),
            details: details.map(|d| d.into()),
        }
    }
    
    pub fn action(&self) -> &str {
        &self.action
    }
    
    pub fn details(&self) -> Option<&str> {
        self.details.as_deref()
    }
}

#[derive(Debug)]
pub struct AdminLog {
    table: DeltaTable,
}

#[derive(Debug)]
pub enum AdminLogError {
    DeltaError(deltalake::DeltaTableError),
    ArrowError(arrow::error::ArrowError),
    Other(String),
}

impl fmt::Display for AdminLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeltaError(e) => write!(f, "Delta table error: {}", e),
            Self::ArrowError(e) => write!(f, "Arrow error: {}", e),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

impl Error for AdminLogError {}

impl From<deltalake::DeltaTableError> for AdminLogError {
    fn from(err: deltalake::DeltaTableError) -> Self {
        Self::DeltaError(err)
    }
}

impl From<arrow::error::ArrowError> for AdminLogError {
    fn from(err: arrow::error::ArrowError) -> Self {
        Self::ArrowError(err)
    }
}

impl AdminLog {
    /// Create a new admin log or open an existing one
    pub async fn open(location: impl AsRef<str>) -> Result<Self, AdminLogError> {
        let table_url = Url::parse(location.as_ref())
            .map_err(|e| AdminLogError::Other(format!("Invalid URL: {}", e)))?;

        // Try to load existing table
        let table_result = DeltaTableBuilder::from_uri(table_url.as_str())
            .load()
            .await;

        let table = match table_result {
            Ok(table) => table,
            Err(_) => {
                // Create a new table with the required schema
                let schema = Schema::new(vec![
                    Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
                    Field::new("action", DataType::Utf8, false),
                    Field::new("details", DataType::Utf8, true),
                ]);

                // Create a new table with manual initialization using the low-level API
                let mut table = DeltaTableBuilder::from_uri(table_url.as_str())
                    .with_allow_http(true)
                    .build()?;
                
                // Create an initial empty batch with the schema
                let batch = RecordBatch::try_new(
                    Arc::new(schema.clone()),
                    vec![
                        Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())),
                        Arc::new(StringArray::from(Vec::<&str>::new())),
                        Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                    ],
                )?;

                // Create the table by writing an empty batch
                let mut writer = RecordBatchWriter::for_table(&table)?;
                writer.write(batch).await?;
                writer.flush_and_commit(&mut table).await?;
                
                table
            }
        };

        Ok(Self { table })
    }

    /// Write a new log entry
    pub async fn write(&mut self, entry: AdminLogEntry) -> Result<(), AdminLogError> {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
            Field::new("action", DataType::Utf8, false),
            Field::new("details", DataType::Utf8, true),
        ]);
        let schema_ref = Arc::new(schema);

        // Convert entry to Arrow RecordBatch
        let times = TimestampMicrosecondArray::from(vec![entry.timestamp.timestamp_micros()]);
        let actions = StringArray::from(vec![entry.action.as_str()]);
        let details = StringArray::from(
            vec![entry.details.as_deref()]);

        let batch = RecordBatch::try_new(
            schema_ref.clone(),
            vec![
                Arc::new(times),
                Arc::new(actions),
                Arc::new(details),
            ],
        )?;

        // Write the batch using the RecordBatchWriter
        let mut writer = RecordBatchWriter::for_table(&self.table)?;
        writer.write(batch).await?;
        writer.flush_and_commit(&mut self.table).await?;

        Ok(())
    }

    /// Read the latest entry from the log
    pub async fn read_latest(&self) -> Result<Option<AdminLogEntry>, AdminLogError> {
        // First make sure the table is properly loaded
        let mut table_clone = DeltaTableBuilder::from_uri(self.table.table_uri())
            .load()
            .await?;
        
        // Check if there are any file actions in the table
        let files = table_clone.snapshot()?.file_actions()?;
        
        // If there are no files, return None
        if files.is_empty() {
            return Ok(None);
        }
        
        // In deltalake v0.26, we need to use direct file access to read parquet data
        use std::io::Cursor;
        use arrow::record_batch::RecordBatchReader;
        // Import parquet crate directly
        use parquet::arrow::arrow_reader::ParquetFileArrowReader;
        use parquet::file::serialized_reader::SerializedFileReader;
        
        let mut all_data: Vec<RecordBatch> = Vec::new();
        let mut error_count = 0;
        
        // Load data from each parquet file
        for file in files {
            // Get the file path from the stored URI
            let path = file.path.clone();
            
            // Use the object_store to get the file bytes
            let store = table_clone.log_store().object_store(None);
            // Create a path from the string
            let uri = object_store::path::Path::from(path);
            
            // Try to read the file contents
            match store.get(&uri).await {
                Ok(bytes) => {
                    // Try to create a parquet reader from the bytes
                    let bytes_content = bytes.bytes().await
                        .map_err(|e| AdminLogError::Other(format!("Error reading file bytes: {}", e)))?;
                    
                    // Create a cursor to read the bytes
                    let cursor = Cursor::new(bytes_content);
                    
                    // Try to create a parquet reader
                    match SerializedFileReader::new(cursor) {
                        Ok(reader) => {
                            // Get the Arrow reader
                            let mut arrow_reader = parquet::arrow::arrow_reader::ParquetFileArrowReader::new(Arc::new(reader));
                            
                            // Read record batches
                            match arrow_reader.get_record_reader(1024) {
                                Ok(mut batch_reader) => {
                                    // Read all batches
                                    while let Some(batch) = batch_reader.next() {
                                        match batch {
                                            Ok(b) => all_data.push(b),
                                            Err(e) => {
                                                error_count += 1;
                                                if error_count <= 3 {
                                                    eprintln!("Error reading batch: {}", e);
                                                }
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    error_count += 1;
                                    if error_count <= 3 {
                                        eprintln!("Error creating record batch reader: {}", e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            error_count += 1;
                            if error_count <= 3 {
                                eprintln!("Error creating parquet reader: {}", e);
                            }
                        }
                    }
                },
                Err(e) => {
                    error_count += 1;
                    if error_count <= 3 {
                        eprintln!("Error fetching file {}: {}", path, e);
                    }
                }
            }
        }
        
        // Check if we have any data
        if all_data.is_empty() {
            return Ok(None);
        }
        
        // Find the latest entry by timestamp
        let mut latest_timestamp = i64::MIN;
        let mut latest_entry: Option<AdminLogEntry> = None;
        
        for batch in all_data.iter() {
            if batch.num_rows() == 0 {
                continue;
            }
            
            let timestamp_array = batch
                .column_by_name("timestamp")
                .ok_or_else(|| AdminLogError::Other("Missing timestamp column".to_string()))?;
            let action_array = batch
                .column_by_name("action")
                .ok_or_else(|| AdminLogError::Other("Missing action column".to_string()))?;
            let details_array = batch
                .column_by_name("details")
                .ok_or_else(|| AdminLogError::Other("Missing details column".to_string()))?;
                
            let timestamp_microseconds = timestamp_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| AdminLogError::Other("Failed to downcast timestamp column".to_string()))?;
                
            // Find the latest entry in this batch
            for row in 0..batch.num_rows() {
                let timestamp = timestamp_microseconds.value(row);
                if timestamp > latest_timestamp {
                    latest_timestamp = timestamp;
                    
                    let action = action_array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| AdminLogError::Other("Failed to downcast action column".to_string()))?
                        .value(row);
                        
                    let details = if details_array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| AdminLogError::Other("Failed to downcast details column".to_string()))?
                        .is_null(row) 
                    {
                        None
                    } else {
                        Some(details_array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row))
                    };
                            
                    // Convert timestamp_micros to DateTime<Utc>
                    let seconds = timestamp / 1_000_000;
                    let nanos = ((timestamp % 1_000_000) * 1000) as u32;
                    let datetime = Utc.timestamp_opt(seconds, nanos)
                        .single()
                        .ok_or_else(|| AdminLogError::Other("Invalid timestamp".to_string()))?;

                    latest_entry = Some(AdminLogEntry {
                        timestamp: datetime,
                        action: action.to_string(),
                        details: details.map(String::from),
                    });
                }
            }
        }
        
        Ok(latest_entry)
    }
    
    /// Close the log and ensure all writes are committed
    pub async fn close(self) -> Result<(), AdminLogError> {
        // The DeltaTable should automatically flush on drop,
        // but we could add explicit cleanup here if needed
        Ok(())
    }
}