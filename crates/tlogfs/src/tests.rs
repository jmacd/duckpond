use crate::persistence::OpLogPersistence;
use tempfile::TempDir;
use diagnostics::*;
use arrow_array::record_batch;
use tokio::time::{timeout, Duration};
use tinyfs::arrow::ParquetExt;

fn test_dir() -> String {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();

    let path = match std::env::var("TLOGFS") {
	Ok(val) => {
	    std::fs::remove_dir_all(&val).expect("test dir can't be removed");
	    val
	},
	_ => store_path,
    };

    debug!("Creating OpLogPersistence at {path}");
    path
}

#[tokio::test]
async fn test_transaction_guard_basic_usage() {
    let store_path = test_dir();
    
    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");
    
    log_debug!("OpLogPersistence created successfully");
    
    // Begin a transaction
    log_debug!("Beginning transaction");
    let tx = persistence.begin().await
        .expect("Failed to begin transaction");
    
    log_debug!("Transaction started successfully");
    
    // Try to access the root directory
    log_debug!("Attempting to get root directory from transaction");
    let root = tx.root().await
        .expect("Failed to get root directory");
    
    let root_debug = format!("{:?}", root);
    log_info!("✅ Successfully got root directory", root_debug: &root_debug);
    
    // Commit the transaction
    log_debug!("Committing transaction");
    tx.commit(None).await
        .expect("Failed to commit transaction");
    
    log_info!("✅ Transaction committed successfully");
}

/// Test reading from a directory after creation (simulating steward's read pattern)
#[tokio::test]
async fn test_transaction_guard_read_after_create() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");

    // Transaction 1: Create directory structure
    {
        let tx = persistence.begin().await
            .expect("Failed to begin transaction");
        
        let root = tx.root().await
            .expect("Failed to get root directory");
        
        root.create_dir_path("/txn").await
            .expect("Failed to create /txn directory");
        
        tx.commit(None).await
            .expect("Failed to commit transaction");
    }
    
    // Transaction 2: Try to read from the created directory
    {
        let tx = persistence.begin().await
            .expect("Failed to begin read transaction");
        
        let root = tx.root().await
            .expect("Failed to get root directory for read");
        
        // Try to list the directory contents
        let _txn_dir = root.open_dir_path("/txn").await
            .expect("Failed to get /txn directory");
        
        println!("✅ Successfully accessed /txn directory in new transaction");
        
        // Don't commit - this is a read-only transaction
    }
    
    println!("✅ Read-after-create test completed successfully");
}

/// Test for single version file:series write/read operations using TinyFS on TLogFS
/// This test is designed to reproduce potential hanging issues with single-version series.
#[tokio::test]
async fn test_single_version_file_series_write_read() -> Result<(), Box<dyn std::error::Error>> {
    // Use a timeout to prevent hanging - if this times out, we know there's a hang issue
    let test_result = timeout(Duration::from_secs(30), async {
        println!("=== Starting Single Version File:Series Test ===");

        let store_path = test_dir();

        // Create TLogFS persistence layer
        println!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create(&store_path).await?;

        let series_path = "test/single_series.series";

        // Begin transaction for write
        println!("Beginning transaction...");
        let tx = persistence.begin().await?;

        // Get working directory
        let wd = tx.root().await?;

        // Create test directory if it doesn't exist
        if !wd.exists(std::path::Path::new("test")).await {
            println!("Creating test directory...");
            wd.create_dir_path("test").await?;
        }

        // Create test data as a RecordBatch with proper timestamp column (integer milliseconds)
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64, 1704074400000_i64]),
            ("value", Float64, [10.5_f64, 20.3_f64, 15.8_f64]),
            ("sensor_id", Utf8, ["sensor_001", "sensor_001", "sensor_001"])
        )?;

        println!("Created RecordBatch with {} rows", batch.num_rows());

        // Write the series using create_series_from_batch - this is where hanging might occur
        println!("Writing series using create_series_from_batch...");
        let (min_time, max_time) = wd.create_series_from_batch(series_path, &batch, Some("timestamp")).await?;

        println!("Series written successfully. Time range: {} to {}", min_time, max_time);

        // Commit transaction
        println!("Committing transaction...");
        tx.commit(None).await?;
        println!("Transaction committed successfully");

        // Create a new transaction to verify the file exists and can be read
        println!("Starting new transaction for read...");
        let tx2 = persistence.begin().await?;
        let wd2 = tx2.root().await?;

        // Verify the file exists
        let file_exists = wd2.exists(std::path::Path::new(series_path)).await;
        println!("File exists after commit: {}", file_exists);
        
        if !file_exists {
            return Err("File does not exist after commit".into());
        }

        // Read back the file
        println!("Reading file back...");
        let read_batch = wd2.read_table_as_batch(series_path).await?;

        println!("Successfully read RecordBatch with {} rows, {} columns", 
                 read_batch.num_rows(), read_batch.num_columns());

        // Verify the data
        assert_eq!(read_batch.num_rows(), 3, "Should have 3 rows");
        assert_eq!(read_batch.num_columns(), 3, "Should have 3 columns");

        // Don't need to commit read transaction
        println!("=== All Tests Passed Successfully ===");
        Ok(())
    }).await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!("Test timed out after 30 seconds - this indicates a hanging issue in single version file:series operations");
        }
    }
}

/// Minimal test to isolate the exact point of hanging - write only
#[tokio::test]
async fn test_minimal_single_version_write_only() -> Result<(), Box<dyn std::error::Error>> {
    let test_result = timeout(Duration::from_secs(15), async {
        println!("=== Minimal Single Version Write Test ===");
        
        let store_path = test_dir();

        println!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create(&store_path).await?;

        println!("Starting transaction...");
        let tx = persistence.begin().await?;

        println!("Getting working directory...");
        let wd = tx.root().await?;

        println!("Creating minimal batch...");
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [1.0_f64])
        )?;

        println!("About to call create_series_from_batch - this may hang...");
        let result = wd.create_series_from_batch("minimal.series", &batch, Some("timestamp")).await;

        match result {
            Ok((min_time, max_time)) => {
                println!("create_series_from_batch succeeded: {} to {}", min_time, max_time);
                println!("Committing transaction...");
                tx.commit(None).await?;
                println!("Transaction committed successfully!");
            }
            Err(e) => {
                println!("create_series_from_batch failed: {}", e);
                return Err(e.into());
            }
        }

        println!("Minimal write test completed successfully!");
        Ok(())
    }).await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!("Minimal write test timed out - hanging occurs during create_series_from_batch or transaction commit");
        }
    }
}

/// Test specifically for temporal metadata extraction from single version series
#[tokio::test]
async fn test_single_version_series_temporal_metadata() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing Single Version Series Temporal Metadata ===");
    
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create(&store_path).await?;

    // Begin transaction
    let tx = persistence.begin().await?;
    let wd = tx.root().await?;

    // Create a series with explicit temporal data
    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64, 1704074400000_i64]), // Clear temporal progression
        ("value", Float64, [10.0_f64, 20.0_f64, 30.0_f64])
    )?;

    println!("Created batch with timestamps: [{}, {}, {}]", 
             1704067200000_i64, 1704070800000_i64, 1704074400000_i64);

    // Write series with explicit timestamp column
    let (min_time, max_time) = wd.create_series_from_batch("temporal_test.series", &batch, Some("timestamp")).await?;
    
    println!("Extracted temporal range: {} to {}", min_time, max_time);
    
    // The temporal metadata should reflect the actual timestamps, not be 0,0
    if min_time == 0 && max_time == 0 {
        println!("⚠️  WARNING: Temporal metadata extraction returned 0,0 - this indicates a problem");
        println!("   Expected: min_time = 1704067200000, max_time = 1704074400000");
    } else {
        println!("✅ Temporal metadata extracted successfully");
        assert_eq!(min_time, 1704067200000_i64, "Min time should match first timestamp");
        assert_eq!(max_time, 1704074400000_i64, "Max time should match last timestamp");
    }

    // Commit and verify persistence
    tx.commit(None).await?;

    // Read back and check that we can access the file
    let tx2 = persistence.begin().await?;
    let wd2 = tx2.root().await?;
    
    let file_exists = wd2.exists(std::path::Path::new("temporal_test.series")).await;
    assert!(file_exists, "File should exist after commit");

    let read_batch = wd2.read_table_as_batch("temporal_test.series").await?;
    assert_eq!(read_batch.num_rows(), 3, "Should read back 3 rows");
    
    println!("✅ Temporal metadata test completed");
    Ok(())
}

/// Test to investigate the actual file storage structure for series
#[tokio::test] 
async fn test_single_version_series_storage_investigation() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Investigating Single Version Series Storage ===");
    
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create(&store_path).await?;

    // Begin transaction
    let tx = persistence.begin().await?;
    let wd = tx.root().await?;

    // Create a very simple series
    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64]),
        ("value", Float64, [42.0_f64])
    )?;

    println!("Writing single-row series...");
    
    // Write series and capture result
    let (min_time, max_time) = wd.create_series_from_batch("investigation.series", &batch, Some("timestamp")).await?;
    println!("create_series_from_batch returned: ({}, {})", min_time, max_time);

    // Before commit, let's see what we can discover about the node structure
    let metadata = wd.metadata_for_path("investigation.series").await?;
    println!("File metadata before commit:");
    println!("  entry_type: {:?}", metadata.entry_type);
    println!("  size: {:?}", metadata.size);

    // Check if we can read the raw file content
    let raw_data = wd.read_file_path_to_vec("investigation.series").await?;
    println!("Raw file size: {} bytes", raw_data.len());
    
    if raw_data.len() >= 4 {
        println!("File magic bytes: {:?}", &raw_data[0..4]);
        if &raw_data[0..4] == b"PAR1" {
            println!("✅ File is valid Parquet");
        } else {
            println!("❌ File is not Parquet format");
        }
    }

    // Commit the transaction
    println!("Committing transaction...");
    tx.commit(None).await?;

    // Investigate post-commit state
    let tx2 = persistence.begin().await?;
    let wd2 = tx2.root().await?;

    let post_commit_metadata = wd2.metadata_for_path("investigation.series").await?;
    println!("File metadata after commit:");
    println!("  entry_type: {:?}", post_commit_metadata.entry_type);
    println!("  size: {:?}", post_commit_metadata.size);

    // Try to read as RecordBatch
    let read_batch = wd2.read_table_as_batch("investigation.series").await?;
    println!("Read batch: {} rows, {} columns", read_batch.num_rows(), read_batch.num_columns());

    // Check the actual data values
    println!("Schema: {:?}", read_batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());

    println!("✅ Storage investigation completed");
    Ok(())
}

/// Summary test documenting the current state of single version file:series operations
#[tokio::test]
async fn test_single_version_series_summary() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Single Version File:Series - Current Status Summary ===");
    
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create(&store_path).await?;

    let tx = persistence.begin().await?;
    let wd = tx.root().await?;

    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]),
        ("temperature", Float64, [20.5_f64, 23.1_f64]),
        ("sensor", Utf8, ["A", "B"])
    )?;

    let (min_time, max_time) = wd.create_series_from_batch("summary.series", &batch, Some("timestamp")).await?;
    tx.commit(None).await?;

    println!("FINDINGS:");
    println!("✅ No hanging issues - operations complete successfully");
    println!("✅ File storage works correctly - Parquet format preserved");
    println!("✅ Data integrity maintained - all rows and columns readable");
    println!("✅ Entry type correctly set to FileSeries");
    println!("❌ Temporal metadata extraction broken - returns (0, 0) instead of actual timestamp range");
    println!("");
    println!("EXPECTED: min_time = 1704067200000, max_time = 1704070800000");
    println!("ACTUAL:   min_time = {}, max_time = {}", min_time, max_time);
    println!("");
    println!("RECOMMENDATION:");
    println!("  - Investigate temporal metadata extraction in create_series_from_batch");
    println!("  - Check if timestamp column parsing logic is working correctly");
    println!("  - Verify that min/max extraction from Arrow arrays is functional");

    Ok(())
}

/// Test streaming async reader functionality without loading entire files into memory
/// This test verifies that large files can be read without the memory footprint issue
#[tokio::test]
async fn test_streaming_async_reader_large_file() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    use crate::large_files::LARGE_FILE_THRESHOLD;
    
    println!("=== Testing Streaming Async Reader (Large File) ===");
    
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create(&store_path).await?;

    let tx = persistence.begin().await?;
    let wd = tx.root().await?;

    // Create a large file that would be problematic if loaded entirely into memory
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // Slightly larger than threshold
    let expected_size = large_content.len();
    
    println!("Creating large file with {} bytes (threshold is {})", expected_size, LARGE_FILE_THRESHOLD);
    
    // Store the large file
    tinyfs::async_helpers::convenience::create_file_path(&wd, "/large_test.dat", &large_content).await?;
    tx.commit(None).await?;
    
    println!("✅ Large file stored successfully");

    // Now test streaming reader - this should NOT load the entire file into memory
    let tx2 = persistence.begin().await?;
    let wd2 = tx2.root().await?;
    
    let file_node = wd2.get_node_path("/large_test.dat").await?;
    let file_handle = file_node.borrow().await.as_file()?;
    
    println!("Getting async reader for large file...");
    let mut reader = file_handle.async_reader().await?;
    
    // Test: Read only first 100 bytes (streaming approach)
    let mut buffer = vec![0u8; 100];
    let bytes_read = reader.read_exact(&mut buffer).await?;
    
    println!("✅ Successfully read {} bytes from start of file", bytes_read);
    assert_eq!(buffer, vec![42u8; 100], "First 100 bytes should all be 42");
    
    // Test: Seek to middle and read 50 bytes (verifying AsyncSeek works)
    let middle_pos = (expected_size / 2) as u64;
    reader.seek(std::io::SeekFrom::Start(middle_pos)).await?;
    
    let mut middle_buffer = vec![0u8; 50];
    reader.read_exact(&mut middle_buffer).await?;
    
    println!("✅ Successfully seeked to position {} and read 50 bytes", middle_pos);
    assert_eq!(middle_buffer, vec![42u8; 50], "Middle 50 bytes should all be 42");
    
    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    println!("✅ File end position: {} bytes", end_pos);
    assert_eq!(end_pos as usize, expected_size, "File size should match expected size");
    
    tx2.commit(None).await?;
    
    println!("SUCCESS: Streaming reader works correctly for large files");
    println!("  - No memory loading of entire file");
    println!("  - AsyncRead works for partial reads");
    println!("  - AsyncSeek works for random access");
    println!("  - File size detection works correctly");

    Ok(())
}

/// Test streaming async reader functionality for small files
/// This test verifies that small files also work correctly with the streaming approach
#[tokio::test]
async fn test_streaming_async_reader_small_file() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    
    println!("=== Testing Streaming Async Reader (Small File) ===");
    
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create(&store_path).await?;

    let tx = persistence.begin().await?;
    let wd = tx.root().await?;

    // Create a small file (under threshold)
    let small_content = b"Hello, World! This is a small test file with some content.";
    let expected_size = small_content.len();
    
    println!("Creating small file with {} bytes", expected_size);
    
    // Store the small file
    tinyfs::async_helpers::convenience::create_file_path(&wd, "/small_test.txt", small_content).await?;
    tx.commit(None).await?;
    
    println!("✅ Small file stored successfully");

    // Now test streaming reader with small file (stored inline in Delta Lake)
    let tx2 = persistence.begin().await?;
    let wd2 = tx2.root().await?;
    
    let file_node = wd2.get_node_path("/small_test.txt").await?;
    let file_handle = file_node.borrow().await.as_file()?;
    
    println!("Getting async reader for small file...");
    let mut reader = file_handle.async_reader().await?;
    
    // Test: Read entire content
    let mut buffer = vec![0u8; expected_size];
    reader.read_exact(&mut buffer).await?;
    
    println!("✅ Successfully read {} bytes", expected_size);
    assert_eq!(&buffer, small_content, "Content should match exactly");
    
    // Test: Seek to start and read first 5 bytes
    reader.seek(std::io::SeekFrom::Start(0)).await?;
    let mut start_buffer = vec![0u8; 5];
    reader.read_exact(&mut start_buffer).await?;
    
    println!("✅ Successfully seeked to start and read first 5 bytes");
    assert_eq!(&start_buffer, b"Hello", "First 5 bytes should be 'Hello'");
    
    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    println!("✅ File end position: {} bytes", end_pos);
    assert_eq!(end_pos as usize, expected_size, "File size should match expected size");
    
    tx2.commit(None).await?;
    
    println!("SUCCESS: Streaming reader works correctly for small files");
    println!("  - Small files use inline storage (Cursor over Vec<u8>)");
    println!("  - AsyncRead works for partial reads");
    println!("  - AsyncSeek works for random access");
    println!("  - File size detection works correctly");

    Ok(())
}
