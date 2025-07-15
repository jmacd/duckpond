//! Tests for streaming support in Phase 1
//! 
//! These tests verify that the async_reader/async_writer functionality works correctly
//! with simple memory buffering for Arrow/Parquet integration.

use crate::{memory::new_fs, error::Result, file::File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use arrow_array::{Int32Array, Float64Array, StringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::{AsyncArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use bytes::Bytes;
use std::sync::Arc;

/// Create a simple test Record Batch
fn create_simple_batch() -> RecordBatch {
    let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let values = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)]));
    let names = Arc::new(StringArray::from(vec!["alice", "bob", "charlie"]));
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
        Field::new("name", DataType::Utf8, false),
    ]);
    
    RecordBatch::try_new(Arc::new(schema), vec![ids, values, names]).unwrap()
}

/// Create a larger test batch for memory testing
fn create_large_batch(num_rows: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let values: Vec<Option<f64>> = (0..num_rows).map(|i| Some(i as f64 * 1.5)).collect();
    let names: Vec<&str> = (0..num_rows).map(|i| if i % 2 == 0 { "even" } else { "odd" }).collect();
    
    let ids_array = Arc::new(Int32Array::from(ids));
    let values_array = Arc::new(Float64Array::from(values));
    let names_array = Arc::new(StringArray::from(names));
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, false),
    ]);
    
    RecordBatch::try_new(Arc::new(schema), vec![ids_array, values_array, names_array]).unwrap()
}

#[tokio::test]
async fn test_async_reader_basic() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create a file with some test data
    let test_data = b"Hello, streaming world!";
    root.create_file_path("/test.txt", test_data).await?;
    
    // Get the file and test async reading
    let node_path = root.get_node_path("/test.txt").await?;
    let file_node = node_path.borrow().await.as_file()?;
    let mut reader = file_node.async_reader().await?;
    
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    
    assert_eq!(buffer, test_data);
    Ok(())
}

#[tokio::test]
async fn test_async_writer_basic() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create an empty file
    root.create_file_path("/output.txt", b"").await?;
    let node_path = root.get_node_path("/output.txt").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    // Write data using async writer
    let test_data = b"Hello from async writer!";
    {
        let mut writer = file_node.async_writer().await?;
        writer.write_all(test_data).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap(); // Explicitly call shutdown
    }
    
    // Add a small delay to ensure background task completes
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    // Read back the data to verify
    let content = root.read_file_path_to_vec("/output.txt").await?;
    assert_eq!(content, test_data);
    
    Ok(())
}

#[tokio::test]
async fn test_async_writer_memory_buffering() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create a file and write small data (buffered in memory)
    root.create_file_path("/small.txt", b"").await?;
    let node_path = root.get_node_path("/small.txt").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    let small_data = vec![42u8; 1024]; // 1KB - buffered in memory during Phase 1
    {
        let mut small_writer = file_node.async_writer().await?;
        small_writer.write_all(&small_data).await.unwrap();
        small_writer.flush().await.unwrap();
        small_writer.shutdown().await.unwrap(); // Explicitly call shutdown
    }
    
    // Verify small file content
    let content = root.read_file_path_to_vec("/small.txt").await?;
    assert_eq!(content, small_data);
    
    Ok(())
}

#[tokio::test] 
async fn test_async_writer_large_data() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create a file and write large data (all buffered in memory during Phase 1)
    root.create_file_path("/large.txt", b"").await?;
    let node_path = root.get_node_path("/large.txt").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    let large_data = vec![42u8; 1024 * 1024 + 1]; // Just over 1MB - all buffered in memory
    {
        let mut large_writer = file_node.async_writer().await?;
        large_writer.write_all(&large_data).await.unwrap();
        large_writer.flush().await.unwrap();
        large_writer.shutdown().await.unwrap(); // Explicitly call shutdown
    }
    
    // Verify large file content
    let content = root.read_file_path_to_vec("/large.txt").await?;
    assert_eq!(content, large_data);
    
    Ok(())
}

#[tokio::test]
async fn test_parquet_roundtrip_single_batch() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create a parquet file with Arrow data
    root.create_file_path("/test.parquet", b"").await?;
    let node_path = root.get_node_path("/test.parquet").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    let batch = create_simple_batch();
    let schema = batch.schema();
    
    // Write parquet data using async writer
    {
        let writer = file_node.async_writer().await?;
        let mut parquet_writer = AsyncArrowWriter::try_new(writer, schema.clone(), None).unwrap();
        parquet_writer.write(&batch).await.unwrap();
        parquet_writer.close().await.unwrap();
    }
    
    // Read back and verify
    let content = root.read_file_path_to_vec("/test.parquet").await?;
    let bytes = Bytes::from(content);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let reader = builder.build().unwrap();
    
    let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    
    let read_batch = &batches[0];
    assert_eq!(read_batch.num_rows(), batch.num_rows());
    assert_eq!(read_batch.num_columns(), batch.num_columns());
    
    Ok(())
}

#[tokio::test]
async fn test_parquet_roundtrip_multiple_batches() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    root.create_file_path("/multi.parquet", b"").await?;
    let node_path = root.get_node_path("/multi.parquet").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    // Create multiple batches with the same schema
    let batch1 = create_simple_batch();
    let batch2 = {
        // Create a second batch with same schema as batch1
        let ids = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let values = Arc::new(Float64Array::from(vec![Some(4.0), Some(5.0), None]));
        let names = Arc::new(StringArray::from(vec!["dave", "eve", "frank"]));
        
        RecordBatch::try_new(batch1.schema(), vec![ids, values, names]).unwrap()
    };
    let batch3 = {
        // Create a third batch with same schema as batch1
        let ids = Arc::new(Int32Array::from(vec![7, 8]));
        let values = Arc::new(Float64Array::from(vec![None, Some(8.0)]));
        let names = Arc::new(StringArray::from(vec!["grace", "henry"]));
        
        RecordBatch::try_new(batch1.schema(), vec![ids, values, names]).unwrap()
    };
    
    let schema = batch1.schema();
    
    // Write multiple batches
    {
        let writer = file_node.async_writer().await?;
        let mut parquet_writer = AsyncArrowWriter::try_new(writer, schema.clone(), None).unwrap();
        
        parquet_writer.write(&batch1).await.unwrap();
        parquet_writer.write(&batch2).await.unwrap();
        parquet_writer.write(&batch3).await.unwrap();
        parquet_writer.close().await.unwrap();
    }
    
    // Read back and verify
    let content = root.read_file_path_to_vec("/multi.parquet").await?;
    let bytes = Bytes::from(content);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let reader = builder.build().unwrap();
    
    let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
    // Note: Parquet may consolidate multiple small batches into one for efficiency
    assert!(!batches.is_empty());
    
    // Verify total row count across all batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 8); // 3 + 3 + 2 = 8 total rows
    
    Ok(())
}

#[tokio::test]
async fn test_memory_bounded_large_parquet() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    root.create_file_path("/huge.parquet", b"").await?;
    let node_path = root.get_node_path("/huge.parquet").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    // Create a large batch (all buffered in memory during Phase 1)
    let large_batch = create_large_batch(10000); // Demonstrates memory buffering in Phase 1
    let schema = large_batch.schema();
    
    // Write using async writer (buffered in memory)
    {
        let writer = file_node.async_writer().await?;
        let mut parquet_writer = AsyncArrowWriter::try_new(writer, schema.clone(), None).unwrap();
        parquet_writer.write(&large_batch).await.unwrap();
        parquet_writer.close().await.unwrap();
    }
    
    // Read back using async reader - first read to memory, then parse
    let mut reader = file_node.async_reader().await?;
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let bytes = Bytes::from(buffer);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let mut stream_reader = builder.build().unwrap();
    
    let mut total_rows = 0;
    while let Some(batch) = stream_reader.next() {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
    }
    
    assert_eq!(total_rows, 10000);
    Ok(())
}

#[tokio::test]
async fn test_concurrent_writers() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create multiple files and write concurrently
    let tasks = (0..5).map(|i| {
        let root = root.clone();
        tokio::spawn(async move {
            let filename = format!("/concurrent_{}.parquet", i);
            root.create_file_path(&filename, b"").await.unwrap();
            let node_path = root.get_node_path(&filename).await.unwrap();
            let file_node = node_path.borrow().await.as_file().unwrap();
            
            let batch = create_large_batch(1000);
            let schema = batch.schema();
            
            let writer = file_node.async_writer().await.unwrap();
            let mut parquet_writer = AsyncArrowWriter::try_new(writer, schema, None).unwrap();
            parquet_writer.write(&batch).await.unwrap();
            parquet_writer.close().await.unwrap();
            
            // Verify the written data
            let content = root.read_file_path_to_vec(&filename).await.unwrap();
            assert!(!content.is_empty());
        })
    });
    
    // Wait for all concurrent writes to complete
    for task in tasks {
        task.await.unwrap();
    }
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_read_write_protection() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create a file
    root.create_file_path("/protected.txt", b"initial content").await?;
    let node_path = root.get_node_path("/protected.txt").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    // Start writing
    let _writer = file_node.async_writer().await?;
    
    // Try to read while writing - should fail
    let read_result = file_node.async_reader().await;
    assert!(read_result.is_err());
    if let Err(e) = read_result {
        assert!(e.to_string().contains("currently being written"));
    }
    
    // Try to write while writing - should fail  
    let write_result = file_node.async_writer().await;
    assert!(write_result.is_err());
    if let Err(e) = write_result {
        assert!(e.to_string().contains("already being written"));
    }
    
    // Try to read file content while writing - should fail
    let content_result = root.read_file_path_to_vec("/protected.txt").await;
    assert!(content_result.is_err());
    if let Err(e) = content_result {
        assert!(e.to_string().contains("currently being written"));
    }
    
    // Drop the writer to release the lock
    drop(_writer);
    
    // Now reads should work again
    let content = root.read_file_path_to_vec("/protected.txt").await?;
    assert_eq!(content, b"initial content"); // Content unchanged since writer was never finished
    
    Ok(())
}

#[tokio::test]
async fn test_write_protection_with_completed_write() -> Result<()> {
    let fs = new_fs().await;
    let root = fs.root().await?;
    
    // Create a file and completely write to it
    root.create_file_path("/complete.txt", b"").await?;
    let node_path = root.get_node_path("/complete.txt").await?;
    let file_node = node_path.borrow().await.as_file()?;
    
    let test_data = b"new content";
    {
        let mut writer = file_node.async_writer().await?;
        writer.write_all(test_data).await.unwrap();
        writer.shutdown().await.unwrap();
    } // Writer dropped, lock released
    
    // Now reads should work
    let content = root.read_file_path_to_vec("/complete.txt").await?;
    assert_eq!(content, test_data);
    
    // And new writes should work
    let mut new_writer = file_node.async_writer().await?;
    new_writer.write_all(b"newer content").await.unwrap();
    new_writer.shutdown().await.unwrap();
    drop(new_writer);
    
    let final_content = root.read_file_path_to_vec("/complete.txt").await?;
    assert_eq!(final_content, b"newer content");
    
    Ok(())
}
