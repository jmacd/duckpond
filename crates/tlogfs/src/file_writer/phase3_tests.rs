// Phase 3 Content Processors Tests
use crate::OpLogPersistence;
use tinyfs::{NodeID, EntryType};
use tempfile::TempDir;
use arrow::array::{Int64Array, TimestampMillisecondArray, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[tokio::test]
async fn test_series_processor_with_real_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Create a real Parquet file with timestamp data
    let parquet_data = create_test_parquet_with_timestamps();
    
    // Write using FileSeries type to trigger temporal metadata extraction
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileSeries).unwrap();
        writer.write(&parquet_data).await.unwrap();
        let result = writer.finish().await.unwrap();
        
        // Verify that temporal metadata was extracted
        match result.metadata {
            crate::file_writer::FileMetadata::Series { min_timestamp, max_timestamp, timestamp_column } => {
                assert!(min_timestamp > 0, "Min timestamp should be extracted");
                assert!(max_timestamp > min_timestamp, "Max timestamp should be greater than min");
                assert_eq!(timestamp_column, "timestamp", "Should detect the timestamp column");
                
                println!("Extracted temporal range: {} to {} in column {}", 
                         min_timestamp, max_timestamp, timestamp_column);
            }
            other => panic!("Expected Series metadata, got {:?}", other),
        }
    }
    
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn test_table_processor_with_parquet_schema() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Create a structured Parquet file for table validation
    let parquet_data = create_test_parquet_table_schema();
    
    // Write using FileTable type to trigger schema validation
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileTable).unwrap();
        writer.write(&parquet_data).await.unwrap();
        let result = writer.finish().await.unwrap();
        
        // Verify that schema was extracted
        match result.metadata {
            crate::file_writer::FileMetadata::Table { schema } => {
                assert!(schema.contains("fields"), "Schema should contain fields");
                assert!(schema.contains("id"), "Should detect the id field");
                assert!(schema.contains("name"), "Should detect the name field");
                
                println!("Extracted table schema: {}", schema);
            }
            other => panic!("Expected Table metadata, got {:?}", other),
        }
    }
    
    tx.commit().await.unwrap();
}

#[tokio::test]  
async fn test_table_processor_with_csv_content() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Create CSV content for table validation
    let csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n";
    
    // Write using FileTable type to trigger schema validation
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileTable).unwrap();
        writer.write(csv_content).await.unwrap();
        let result = writer.finish().await.unwrap();
        
        // Verify that CSV was detected and analyzed
        match result.metadata {
            crate::file_writer::FileMetadata::Table { schema } => {
                assert!(schema.contains("csv"), "Should detect CSV format");
                assert!(schema.contains("rows"), "Should count rows");
                
                println!("Detected CSV schema: {}", schema);
            }
            other => panic!("Expected Table metadata, got {:?}", other),
        }
    }
    
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn test_data_processor_no_special_processing() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Write using FileData type (no special processing)
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"This is just raw file data with no special structure").await.unwrap();
        let result = writer.finish().await.unwrap();
        
        // Verify that no special processing was done
        match result.metadata {
            crate::file_writer::FileMetadata::Data => {
                // Expected - no special metadata for raw data
            }
            other => panic!("Expected Data metadata, got {:?}", other),
        }
    }
    
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn test_empty_file_processors() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id1 = NodeID::generate();
    let node_id2 = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Test empty FileSeries
    {
        let writer = tx.create_file_writer(node_id1, part_id, EntryType::FileSeries).unwrap();
        let result = writer.finish().await.unwrap();
        
        match result.metadata {
            crate::file_writer::FileMetadata::Series { min_timestamp, max_timestamp, timestamp_column } => {
                assert_eq!(min_timestamp, 0);
                assert_eq!(max_timestamp, 0);
                assert_eq!(timestamp_column, "timestamp");
            }
            other => panic!("Expected Series metadata for empty file, got {:?}", other),
        }
    }
    
    // Test empty FileTable
    {
        let writer = tx.create_file_writer(node_id2, part_id, EntryType::FileTable).unwrap();
        let result = writer.finish().await.unwrap();
        
        match result.metadata {
            crate::file_writer::FileMetadata::Table { schema } => {
                assert!(schema.contains("fields"));
                assert!(schema.contains("[]")); // Empty fields array
            }
            other => panic!("Expected Table metadata for empty file, got {:?}", other),
        }
    }
    
    tx.commit().await.unwrap();
}

/// Helper function to create a test Parquet file with timestamp data
fn create_test_parquet_with_timestamps() -> Vec<u8> {
    // Create Arrow schema with timestamp column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Utf8, true),
    ]));
    
    // Create test data
    let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let timestamp_array = Arc::new(TimestampMillisecondArray::from(vec![
        1609459200000, // 2021-01-01 00:00:00
        1609545600000, // 2021-01-02 00:00:00  
        1609632000000, // 2021-01-03 00:00:00
        1609718400000, // 2021-01-04 00:00:00
        1609804800000, // 2021-01-05 00:00:00
    ]));
    let value_array = Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"]));
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, timestamp_array, value_array],
    ).unwrap();
    
    // Write to Parquet format
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    
    buffer
}

/// Helper function to create a test Parquet file with structured table schema
fn create_test_parquet_table_schema() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    
    let id_array = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec!["Product A", "Product B", "Product C"]));
    let category_array = Arc::new(StringArray::from(vec![Some("Electronics"), None, Some("Books")]));
    let amount_array = Arc::new(Int64Array::from(vec![Some(100), Some(200), None]));
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, name_array, category_array, amount_array],
    ).unwrap();
    
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    
    buffer
}
