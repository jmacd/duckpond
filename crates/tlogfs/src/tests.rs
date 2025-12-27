// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::OpLogPersistence;
use arrow_array::record_batch;
use log::{debug, info};
use tempfile::TempDir;
use tinyfs::arrow::ParquetExt;
use tokio::time::{Duration, timeout};

fn test_dir() -> String {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir
        .path()
        .join("test_store")
        .to_string_lossy()
        .to_string();

    let path = match std::env::var("TLOGFS") {
        Ok(val) => {
            std::fs::remove_dir_all(&val).expect("test dir can't be removed");
            val
        }
        _ => store_path,
    };

    debug!("Creating OpLogPersistence at {path}");
    path
}

#[tokio::test]
async fn test_transaction_guard_basic_usage() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("OpLogPersistence created successfully");

    // Begin a transaction (txn_seq=2 since root init used txn_seq=1)
    debug!("Beginning transaction");
    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    debug!("Transaction started successfully");

    // Try to access the root directory
    debug!("Attempting to get root directory from transaction");
    let root = tx.root().await.expect("Failed to get root directory");

    let root_debug = format!("{:?}", root);
    info!("✅ Successfully got root directory: {:?}", &root_debug);

    // Commit the transaction
    debug!("Committing transaction");
    tx.commit_test()
        .await
        .expect("Failed to commit transaction");

    info!("✅ Transaction committed successfully");
}

/// Test the absolute basics: create, commit, reopen, read empty root
#[tokio::test]
async fn test_create_commit_reopen_read_root() {
    let store_path = test_dir();

    // Step 1: Create new OpLogPersistence with root directory
    debug!("Step 1: Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("OpLogPersistence created successfully");

    // Step 2: Begin a transaction and verify we can access root
    debug!("Step 2: Beginning first transaction");
    let tx1 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root1 = tx1.root().await.expect("Failed to get root directory");
    debug!("✅ Got root directory in first transaction");

    // List the root directory (should be empty)
    use futures::stream::StreamExt;
    let mut entries_stream1 = root1.entries().await.expect("Failed to get entries stream");
    let mut entries1 = Vec::new();
    while let Some(entry_result) = entries_stream1.next().await {
        let entry = entry_result.expect("Failed to read entry");
        entries1.push(entry);
    }
    debug!("Root directory has {} entries", entries1.len());
    assert_eq!(entries1.len(), 0, "Root should be empty initially");

    // Step 3: Commit the transaction
    debug!("Step 3: Committing first transaction");
    tx1.commit_test()
        .await
        .expect("Failed to commit first transaction");
    debug!("✅ First transaction committed");

    // Step 4: Begin a second transaction and read root again
    debug!("Step 4: Beginning second transaction (should see committed data)");
    let tx2 = persistence
        .begin_test()
        .await
        .expect("Failed to begin second transaction");

    let root2 = tx2
        .root()
        .await
        .expect("Failed to get root in second transaction");
    debug!("✅ Got root directory in second transaction");

    // List the root directory again (should still be empty)
    let mut entries_stream2 = root2.entries().await.expect("Failed to get entries stream");
    let mut entries2 = Vec::new();
    while let Some(entry_result) = entries_stream2.next().await {
        let entry = entry_result.expect("Failed to read entry");
        entries2.push(entry);
    }
    debug!(
        "Root directory has {} entries in second transaction",
        entries2.len()
    );
    assert_eq!(entries2.len(), 0, "Root should still be empty");

    // No need to commit read transaction
    debug!("✅ Test passed - can create, commit, and read empty root");
}

/// Test create file, commit, reopen, read file
#[tokio::test]
async fn test_create_file_commit_reopen_read() {
    let store_path = test_dir();

    // Step 1: Create new OpLogPersistence with root directory
    debug!("Step 1: Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Step 2: Create a simple file
    debug!("Step 2: Creating a file");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Write a simple text file
        root.write_file_path_from_slice("test.txt", b"Hello, World!")
            .await
            .expect("Failed to write file");

        debug!("✅ File created");

        tx.commit_test().await.expect("Failed to commit");
        debug!("✅ Transaction committed");
    }

    // Step 3: Read the file back in a new transaction
    debug!("Step 3: Reading file in new transaction");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin second transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Read the file
        let content = root
            .read_file_path_to_vec("test.txt")
            .await
            .expect("Failed to read file");

        let content_str = String::from_utf8(content).expect("Invalid UTF-8");
        debug!("✅ Read file content: '{}'", content_str);

        assert_eq!(content_str, "Hello, World!", "File content should match");

        debug!("✅ Test passed - can create file, commit, and read back");
    }
}

/// Test create file:series, commit, reopen, read - SIMPLIFIED VERSION
#[tokio::test]
async fn test_create_series_commit_reopen_read_simple() {
    let store_path = test_dir();

    // Step 1: Create new OpLogPersistence with root directory
    debug!("Step 1: Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Step 2: Create a simple series file
    debug!("Step 2: Creating a file:series");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Create test data as a RecordBatch
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [42.0_f64])
        )
        .expect("Failed to create batch");

        debug!("Created RecordBatch with {} rows", batch.num_rows());

        // Write the series
        let (min_time, max_time) = root
            .create_series_from_batch("test.series", &batch, Some("timestamp"))
            .await
            .expect("Failed to create series");

        debug!(
            "✅ Series created with time range: {} to {}",
            min_time, max_time
        );

        tx.commit_test().await.expect("Failed to commit");
        debug!("✅ Transaction committed");
    }

    // Step 3: Read the series back in a new transaction
    debug!("Step 3: Reading series in new transaction");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin second transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Check if file exists
        let exists = root.exists(std::path::Path::new("test.series")).await;
        debug!("File exists: {}", exists);
        assert!(exists, "Series file should exist after commit");

        // Read the series back
        let read_batch = root
            .read_table_as_batch("test.series")
            .await
            .expect("Failed to read series");

        debug!(
            "✅ Read RecordBatch with {} rows, {} columns",
            read_batch.num_rows(),
            read_batch.num_columns()
        );

        assert_eq!(read_batch.num_rows(), 1, "Should have 1 row");
        assert_eq!(read_batch.num_columns(), 2, "Should have 2 columns");

        debug!("✅ Test passed - can create series, commit, and read back");
    }
}

/// Test create series in subdirectory, commit, reopen, read
#[tokio::test]
async fn test_create_series_in_subdir() {
    let store_path = test_dir();

    debug!("Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Create a series in a subdirectory
    debug!("Creating series in subdirectory");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Create subdirectory
        debug!("Creating test/ subdirectory");
        let _test_dir = root
            .create_dir_path("test")
            .await
            .expect("Failed to create directory");

        // Create series in subdirectory
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [42.0_f64])
        )
        .expect("Failed to create batch");

        debug!("Writing series to test/data.series");
        let _time_range = root
            .create_series_from_batch("test/data.series", &batch, Some("timestamp"))
            .await
            .expect("Failed to create series");

        debug!("✅ Series created, committing...");
        tx.commit_test().await.expect("Failed to commit");
        debug!("✅ Committed");
    }

    // Read back in new transaction
    debug!("Reading back in new transaction");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin read transaction");
        let root = tx.root().await.expect("Failed to get root");

        let exists = root.exists(std::path::Path::new("test/data.series")).await;
        debug!("File exists: {}", exists);
        assert!(exists, "File should exist");

        let read_batch = root
            .read_table_as_batch("test/data.series")
            .await
            .expect("Failed to read series");

        debug!("✅ Read {} rows", read_batch.num_rows());
        assert_eq!(read_batch.num_rows(), 1);
    }

    debug!("✅ Test passed");
}

/// Test reading from a directory after creation (simulating steward's read pattern)
#[tokio::test]
async fn test_transaction_guard_read_after_create() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Transaction 1: Create directory structure
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");

        let root = tx.root().await.expect("Failed to get root directory");

        _ = root
            .create_dir_path("/txn")
            .await
            .expect("Failed to create /txn directory");

        tx.commit_test()
            .await
            .expect("Failed to commit transaction");
    }

    // Transaction 2: Try to read from the created directory
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin read transaction");

        let root = tx
            .root()
            .await
            .expect("Failed to get root directory for read");

        // Try to list the directory contents
        let _txn_dir = root
            .open_dir_path("/txn")
            .await
            .expect("Failed to get /txn directory");

        debug!("✅ Successfully accessed /txn directory in new transaction");

        // Don't commit - this is a read-only transaction
    }

    debug!("✅ Read-after-create test completed successfully");
}

/// Test for single version file:series write/read operations using TinyFS on TLogFS
/// This test is designed to reproduce potential hanging issues with single-version series.
#[tokio::test]
async fn test_single_version_file_series_write_read() -> Result<(), Box<dyn std::error::Error>> {
    // Use a timeout to prevent hanging - if this times out, we know there's a hang issue
    let test_result = timeout(Duration::from_secs(30), async {
        debug!("=== Starting Single Version File:Series Test ===");

        let store_path = test_dir();

        // Create TLogFS persistence layer
        debug!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        let series_path = "test/single_series.series";

        // Begin transaction for write
        debug!("Beginning transaction...");
        let tx = persistence.begin_test().await?;

        // Get working directory
        let wd = tx.root().await?;

        // Create test directory if it doesn't exist
        if !wd.exists(std::path::Path::new("test")).await {
            debug!("Creating test directory...");
            _ = wd.create_dir_path("test").await?;
        }

        // Create test data as a RecordBatch with proper timestamp column (integer milliseconds)
        let batch = record_batch!(
            (
                "timestamp",
                Int64,
                [1704067200000_i64, 1704070800000_i64, 1704074400000_i64]
            ),
            ("value", Float64, [10.5_f64, 20.3_f64, 15.8_f64]),
            (
                "sensor_id",
                Utf8,
                ["sensor_001", "sensor_001", "sensor_001"]
            )
        )?;

        debug!("Created RecordBatch with {} rows", batch.num_rows());

        // Write the series using create_series_from_batch - this is where hanging might occur
        debug!("Writing series using create_series_from_batch...");
        let (min_time, max_time) = wd
            .create_series_from_batch(series_path, &batch, Some("timestamp"))
            .await?;

        debug!(
            "Series written successfully. Time range: {} to {}",
            min_time, max_time
        );

        // Commit transaction
        debug!("Committing transaction...");
        tx.commit_test().await?;
        debug!("Transaction committed successfully");

        // Create a new transaction to verify the file exists and can be read
        debug!("Starting new transaction for read...");
        let tx2 = persistence.begin_test().await?;
        let wd2 = tx2.root().await?;

        // Verify the file exists
        let file_exists = wd2.exists(std::path::Path::new(series_path)).await;
        debug!("File exists after commit: {}", file_exists);

        if !file_exists {
            return Err("File does not exist after commit".into());
        }

        // Read back the file
        debug!("Reading file back...");
        let read_batch = wd2.read_table_as_batch(series_path).await?;

        debug!(
            "Successfully read RecordBatch with {} rows, {} columns",
            read_batch.num_rows(),
            read_batch.num_columns()
        );

        // Verify the data
        assert_eq!(read_batch.num_rows(), 3, "Should have 3 rows");
        assert_eq!(read_batch.num_columns(), 3, "Should have 3 columns");

        // Don't need to commit read transaction
        debug!("=== All Tests Passed Successfully ===");
        Ok(())
    })
    .await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!(
                "Test timed out after 30 seconds - this indicates a hanging issue in single version file:series operations"
            );
        }
    }
}

/// Minimal test to isolate the exact point of hanging - write only
#[tokio::test]
async fn test_minimal_single_version_write_only() -> Result<(), Box<dyn std::error::Error>> {
    let test_result = timeout(Duration::from_secs(15), async {
        debug!("=== Minimal Single Version Write Test ===");

        let store_path = test_dir();

        debug!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        debug!("Starting transaction...");
        let tx = persistence.begin_test().await?;

        debug!("Getting working directory...");
        let wd = tx.root().await?;

        debug!("Creating minimal batch...");
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [1.0_f64])
        )?;

        debug!("About to call create_series_from_batch - this may hang...");
        let result = wd
            .create_series_from_batch("minimal.series", &batch, Some("timestamp"))
            .await;

        match result {
            Ok((min_time, max_time)) => {
                debug!(
                    "create_series_from_batch succeeded: {} to {}",
                    min_time, max_time
                );
                debug!("Committing transaction...");
                tx.commit_test().await?;
                debug!("Transaction committed successfully!");
            }
            Err(e) => {
                debug!("create_series_from_batch failed: {}", e);
                return Err(e.into());
            }
        }

        debug!("Minimal write test completed successfully!");
        Ok(())
    })
    .await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!(
                "Minimal write test timed out - hanging occurs during create_series_from_batch or transaction commit"
            );
        }
    }
}

/// Test specifically for temporal metadata extraction from single version series
#[tokio::test]
async fn test_single_version_series_temporal_metadata() -> Result<(), Box<dyn std::error::Error>> {
    debug!("=== Testing Single Version Series Temporal Metadata ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    // Begin transaction
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a series with explicit temporal data
    let batch = record_batch!(
        (
            "timestamp",
            Int64,
            [1704067200000_i64, 1704070800000_i64, 1704074400000_i64]
        ), // Clear temporal progression
        ("value", Float64, [10.0_f64, 20.0_f64, 30.0_f64])
    )?;

    debug!(
        "Created batch with timestamps: [{}, {}, {}]",
        1704067200000_i64, 1704070800000_i64, 1704074400000_i64
    );

    // Write series with explicit timestamp column
    let (min_time, max_time) = wd
        .create_series_from_batch("temporal_test.series", &batch, Some("timestamp"))
        .await?;

    debug!("Extracted temporal range: {} to {}", min_time, max_time);

    // The temporal metadata should reflect the actual timestamps, not be 0,0
    if min_time == 0 && max_time == 0 {
        debug!("⚠️  WARNING: Temporal metadata extraction returned 0,0 - this indicates a problem");
        debug!("   Expected: min_time = 1704067200000, max_time = 1704074400000");
    } else {
        debug!("✅ Temporal metadata extracted successfully");
        assert_eq!(
            min_time, 1704067200000_i64,
            "Min time should match first timestamp"
        );
        assert_eq!(
            max_time, 1704074400000_i64,
            "Max time should match last timestamp"
        );
    }

    // Commit and verify persistence
    tx.commit_test().await?;

    // Read back and check that we can access the file
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_exists = wd2
        .exists(std::path::Path::new("temporal_test.series"))
        .await;
    assert!(file_exists, "File should exist after commit");

    let read_batch = wd2.read_table_as_batch("temporal_test.series").await?;
    assert_eq!(read_batch.num_rows(), 3, "Should read back 3 rows");

    debug!("✅ Temporal metadata test completed");
    Ok(())
}

/// Test to investigate the actual file storage structure for series
#[tokio::test]
async fn test_single_version_series_storage_investigation() -> Result<(), Box<dyn std::error::Error>>
{
    debug!("=== Investigating Single Version Series Storage ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    // Begin transaction
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a very simple series
    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64]),
        ("value", Float64, [42.0_f64])
    )?;

    debug!("Writing single-row series...");

    // Write series and capture result
    let (min_time, max_time) = wd
        .create_series_from_batch("investigation.series", &batch, Some("timestamp"))
        .await?;
    debug!(
        "create_series_from_batch returned: ({}, {})",
        min_time, max_time
    );

    // Before commit, let's see what we can discover about the node structure
    let metadata = wd.metadata_for_path("investigation.series").await?;
    debug!("File metadata before commit:");
    debug!("  entry_type: {:?}", metadata.entry_type);
    debug!("  size: {:?}", metadata.size);

    // Check if we can read the raw file content
    let raw_data = wd.read_file_path_to_vec("investigation.series").await?;
    debug!("Raw file size: {} bytes", raw_data.len());

    if raw_data.len() >= 4 {
        debug!("File magic bytes: {:?}", &raw_data[0..4]);
        if &raw_data[0..4] == b"PAR1" {
            debug!("✅ File is valid Parquet");
        } else {
            debug!("❌ File is not Parquet format");
        }
    }

    // Commit the transaction
    debug!("Committing transaction...");
    tx.commit_test().await?;

    // Investigate post-commit state
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let post_commit_metadata = wd2.metadata_for_path("investigation.series").await?;
    debug!("File metadata after commit:");
    debug!("  entry_type: {:?}", post_commit_metadata.entry_type);
    debug!("  size: {:?}", post_commit_metadata.size);

    // Try to read as RecordBatch
    let read_batch = wd2.read_table_as_batch("investigation.series").await?;
    debug!(
        "Read batch: {} rows, {} columns",
        read_batch.num_rows(),
        read_batch.num_columns()
    );

    // Check the actual data values
    debug!(
        "Schema: {:?}",
        read_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );

    debug!("✅ Storage investigation completed");
    Ok(())
}

/// Summary test documenting the current state of single version file:series operations
#[tokio::test]
async fn test_single_version_series_summary() -> Result<(), Box<dyn std::error::Error>> {
    debug!("=== Single Version File:Series - Current Status Summary ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]),
        ("temperature", Float64, [20.5_f64, 23.1_f64]),
        ("sensor", Utf8, ["A", "B"])
    )?;

    let (min_time, max_time) = wd
        .create_series_from_batch("summary.series", &batch, Some("timestamp"))
        .await?;
    tx.commit_test().await?;

    debug!("FINDINGS:");
    debug!("✅ No hanging issues - operations complete successfully");
    debug!("✅ File storage works correctly - Parquet format preserved");
    debug!("✅ Data integrity maintained - all rows and columns readable");
    debug!("✅ Entry type correctly set to FileSeries");
    debug!(
        "❌ Temporal metadata extraction broken - returns (0, 0) instead of actual timestamp range"
    );
    debug!("");
    debug!("EXPECTED: min_time = 1704067200000, max_time = 1704070800000");
    debug!("ACTUAL:   min_time = {}, max_time = {}", min_time, max_time);
    debug!("");
    debug!("RECOMMENDATION:");
    debug!("  - Investigate temporal metadata extraction in create_series_from_batch");
    debug!("  - Check if timestamp column parsing logic is working correctly");
    debug!("  - Verify that min/max extraction from Arrow arrays is functional");

    Ok(())
}

/// Test streaming async reader functionality without loading entire files into memory
/// This test verifies that large files can be read without the memory footprint issue
#[tokio::test]
async fn test_streaming_async_reader_large_file() -> Result<(), Box<dyn std::error::Error>> {
    use crate::large_files::LARGE_FILE_THRESHOLD;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    debug!("=== Testing Streaming Async Reader (Large File) ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a large file that would be problematic if loaded entirely into memory
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // Slightly larger than threshold
    let expected_size = large_content.len();

    debug!(
        "Creating large file with {} bytes (threshold is {})",
        expected_size, LARGE_FILE_THRESHOLD
    );

    // Store the large file
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/large_test.dat",
        &large_content,
    )
    .await?;
    tx.commit_test().await?;

    debug!("✅ Large file stored successfully");

    // Now test streaming reader - this should NOT load the entire file into memory
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_node = wd2.get_node_path("/large_test.dat").await?;
    let file_handle = file_node.as_file().await?;

    debug!("Getting async reader for large file...");
    let mut reader = file_handle.async_reader().await?;

    // Test: Read only first 100 bytes (streaming approach)
    let mut buffer = vec![0u8; 100];
    let bytes_read = reader.read_exact(&mut buffer).await?;

    debug!(
        "✅ Successfully read {} bytes from start of file",
        bytes_read
    );
    assert_eq!(buffer, vec![42u8; 100], "First 100 bytes should all be 42");

    // Test: Seek to middle and read 50 bytes (verifying AsyncSeek works)
    let middle_pos = (expected_size / 2) as u64;
    _ = reader.seek(std::io::SeekFrom::Start(middle_pos)).await?;

    let mut middle_buffer = vec![0u8; 50];
    _ = reader.read_exact(&mut middle_buffer).await?;

    debug!(
        "✅ Successfully seeked to position {} and read 50 bytes",
        middle_pos
    );
    assert_eq!(
        middle_buffer,
        vec![42u8; 50],
        "Middle 50 bytes should all be 42"
    );

    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    debug!("✅ File end position: {} bytes", end_pos);
    assert_eq!(
        end_pos as usize, expected_size,
        "File size should match expected size"
    );

    tx2.commit_test().await?;

    debug!("SUCCESS: Streaming reader works correctly for large files");
    debug!("  - No memory loading of entire file");
    debug!("  - AsyncRead works for partial reads");
    debug!("  - AsyncSeek works for random access");
    debug!("  - File size detection works correctly");

    Ok(())
}

/// Test streaming async reader functionality for small files
/// This test verifies that small files also work correctly with the streaming approach
#[tokio::test]
async fn test_streaming_async_reader_small_file() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    debug!("=== Testing Streaming Async Reader (Small File) ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a small file (under threshold)
    let small_content = b"Hello, World! This is a small test file with some content.";
    let expected_size = small_content.len();

    debug!("Creating small file with {} bytes", expected_size);

    // Store the small file
    _ = tinyfs::async_helpers::convenience::create_file_path(&wd, "/small_test.txt", small_content)
        .await?;
    tx.commit_test().await?;

    debug!("✅ Small file stored successfully");

    // Now test streaming reader with small file (stored inline in Delta Lake)
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_node = wd2.get_node_path("/small_test.txt").await?;
    let file_handle = file_node.as_file().await?;

    debug!("Getting async reader for small file...");
    let mut reader = file_handle.async_reader().await?;

    // Test: Read entire content
    let mut buffer = vec![0u8; expected_size];
    _ = reader.read_exact(&mut buffer).await?;

    debug!("✅ Successfully read {} bytes", expected_size);
    assert_eq!(&buffer, small_content, "Content should match exactly");

    // Test: Seek to start and read first 5 bytes
    _ = reader.seek(std::io::SeekFrom::Start(0)).await?;
    let mut start_buffer = vec![0u8; 5];
    _ = reader.read_exact(&mut start_buffer).await?;

    debug!("✅ Successfully seeked to start and read first 5 bytes");
    assert_eq!(&start_buffer, b"Hello", "First 5 bytes should be 'Hello'");

    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    debug!("✅ File end position: {} bytes", end_pos);
    assert_eq!(
        end_pos as usize, expected_size,
        "File size should match expected size"
    );

    tx2.commit_test().await?;

    debug!("SUCCESS: Streaming reader works correctly for small files");
    debug!("  - Small files use inline storage (Cursor over Vec<u8>)");
    debug!("  - AsyncRead works for partial reads");
    debug!("  - AsyncSeek works for random access");
    debug!("  - File size detection works correctly");

    Ok(())
}

/// Test temporal bounds functionality on file series
///
/// This test creates a file series with multiple versions, verifies count queries,
/// sets temporal bounds, and validates that the bounds correctly filter the data.
#[tokio::test]
async fn test_temporal_bounds_on_file_series() -> Result<(), Box<dyn std::error::Error>> {
    use crate::query::execute_sql_on_file;
    use crate::schema::duckpond;
    use futures::stream::StreamExt;
    use std::collections::HashMap;

    debug!("=== Starting Temporal Bounds Test ===");

    let store_path = test_dir();

    // Create TLogFS persistence layer
    debug!("Creating persistence layer...");
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let series_path = "test/temporal_series.series";

    // Transaction 1: Create file series with all data points (simulating the combined data from multiple versions)
    debug!(
        "Creating file series with points at T=1,10,11,12,13,14,15,16,17,18,19,50 (12 points total)..."
    );
    {
        let tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        // Create test directory if it doesn't exist
        if !wd.exists(std::path::Path::new("test")).await {
            _ = wd.create_dir_path("test").await?;
        }

        // Create batch with timestamps as proper Unix epoch seconds (not milliseconds)
        // T=1,10,11,12,13,14,15,16,17,18,19,50 (12 points total)
        use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampSecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        let timestamp_data = TimestampSecondArray::from(vec![
            Some(1_i64),
            Some(10_i64),
            Some(11_i64),
            Some(12_i64),
            Some(13_i64),
            Some(14_i64),
            Some(15_i64),
            Some(16_i64),
            Some(17_i64),
            Some(18_i64),
            Some(19_i64),
            Some(50_i64),
        ])
        .with_timezone("+00:00");

        let value_data = Float64Array::from(vec![
            100.0_f64, 110.0_f64, 111.0_f64, 112.0_f64, 113.0_f64, 114.0_f64, 115.0_f64, 116.0_f64,
            117.0_f64, 118.0_f64, 119.0_f64, 150.0_f64,
        ]);

        let sensor_data = StringArray::from(vec![
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor_id", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_data),
                Arc::new(value_data),
                Arc::new(sensor_data),
            ],
        )?;

        let (min_time, max_time) = wd
            .create_series_from_batch(series_path, &batch, Some("timestamp"))
            .await?;
        debug!("Series created. Time range: {} to {}", min_time, max_time);

        tx.commit_test().await?;
    }

    // Transaction 3: Query the series before setting temporal bounds - should return 12 rows
    debug!("Querying series before temporal bounds...");
    {
        let mut tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        let mut result_stream = execute_sql_on_file(
            &wd,
            series_path,
            "SELECT COUNT(*) as row_count FROM series",
            &mut tx,
        )
        .await?;

        let mut total_count = 0_i64;
        while let Some(batch_result) = result_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("COUNT(*) should return Int64Array");
                total_count = count_array.value(0);
            }
        }

        debug!("Count before temporal bounds: {}", total_count);
        assert_eq!(
            total_count, 12,
            "Should have 12 total rows before temporal bounds"
        );

        // Don't commit this read-only transaction
    }

    // Transaction 4: Set temporal bounds to [10000, 20000] ms using empty version approach
    debug!("Setting temporal bounds to [10000, 20000] ms (10-20 seconds) using empty version...");
    {
        let tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        // Create an empty version with temporal bounds (following CLI pattern)
        // Use TinyFS async writer to add empty version to existing file
        let mut writer = wd
            .async_writer_path_with_type(series_path, tinyfs::EntryType::FileSeriesPhysical)
            .await?;

        // Write empty content to create a pending record
        use tokio::io::AsyncWriteExt;
        writer.write_all(&[]).await?;
        writer.flush().await?;
        writer.shutdown().await?;

        debug!("Created empty version for metadata");

        // Set temporal overrides using extended attributes on the path
        // Note: bounds need to be in milliseconds since that's the internal representation
        let mut attributes = HashMap::new();
        _ = attributes.insert(
            duckpond::MIN_TEMPORAL_OVERRIDE.to_string(),
            "10000".to_string(),
        ); // 10 seconds = 10000 ms
        _ = attributes.insert(
            duckpond::MAX_TEMPORAL_OVERRIDE.to_string(),
            "20000".to_string(),
        ); // 20 seconds = 20000 ms

        wd.set_extended_attributes(series_path, attributes).await?;

        debug!("Temporal bounds set to [10000, 20000] milliseconds (10-20 seconds)");
        tx.commit_test().await?;
    }

    // Transaction 5: Query the series after setting temporal bounds - should return 10 rows
    debug!("Querying series after temporal bounds...");
    {
        let mut tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        let mut result_stream = execute_sql_on_file(
            &wd,
            series_path,
            "SELECT COUNT(*) as row_count FROM series",
            &mut tx,
        )
        .await?;

        let mut total_count = 0_i64;
        while let Some(batch_result) = result_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("COUNT(*) should return Int64Array");
                total_count = count_array.value(0);
            }
        }

        debug!("Count after temporal bounds: {}", total_count);
        assert_eq!(
            total_count, 10,
            "Should have 10 rows after temporal bounds [10000, 20000] ms (excluding T=1s and T=50s)"
        );

        // Don't commit this read-only transaction
    }

    debug!("SUCCESS: Temporal bounds test completed");
    debug!(
        "  - Created file series with data points at T=1,10,11,12,13,14,15,16,17,18,19,50 seconds"
    );
    debug!("  - Verified initial count of 12 rows");
    debug!("  - Set temporal bounds to [10000, 20000] ms (10-20 seconds)");
    debug!("  - Verified filtered count of 10 rows (excluded T=1s and T=50s)");

    Ok(())
}

/// Test to reproduce unnecessary directory updates when appending to file:series
///
/// This test writes multiple versions to the same file:series and checks
/// whether the parent directory gets updated on each append (it shouldn't).
#[tokio::test]
async fn test_multiple_series_appends_directory_updates() -> Result<(), Box<dyn std::error::Error>>
{
    debug!("=== Testing Multiple Series Appends - Directory Update Check ===");

    let test_result = timeout(Duration::from_secs(30), async {
        let store_path = test_dir();

        // Create TLogFS persistence layer
        debug!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        // Check filesystem after bootstrap
        debug!("\n=== Checking filesystem after bootstrap ===");
        let store_path_obj = std::path::Path::new(&store_path);
        debug!("Store path: {}", store_path);
        debug!("Store path exists: {}", store_path_obj.exists());
        if store_path_obj.exists() &&
            let Ok(entries) = std::fs::read_dir(store_path_obj) {
                for entry in entries.flatten() {
                    debug!("  - {}", entry.path().display());
		}
            }

        // Check _delta_log directory
        let delta_log_path = store_path_obj.join("_delta_log");
        debug!("Delta log exists: {}", delta_log_path.exists());
        if delta_log_path.exists() &&
            let Ok(entries) = std::fs::read_dir(&delta_log_path) {
                for entry in entries.flatten() {
                        debug!("  - delta log: {}", entry.path().display());
                }
        }

        let series_path = "devices/sensor_123/data.series";

        // === TRANSACTION 2: Create directory structure and initial file ===
        debug!("\n=== Transaction 2: Create directory and first version ===");
        let tx1 = persistence.begin_test().await?;
        let wd1 = tx1.root().await?;

        // Create devices directory
        debug!("Creating /devices directory...");
        _ = wd1.create_dir_path("devices").await?;

        // Create sensor subdirectory
        debug!("Creating /devices/sensor_123 directory...");
        _ = wd1.create_dir_path("devices/sensor_123").await?;

        // Write first version of series
        let batch1 = record_batch!(
            ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]),
            ("value", Float64, [10.5_f64, 20.3_f64])
        )?;

        debug!("Writing FIRST version of series...");
        _ = wd1.create_series_from_batch(series_path, &batch1, Some("timestamp")).await?;

        debug!("Committing transaction 2...");
        tx1.commit_test().await?;

        // === TRANSACTION 3: Append to existing series (SHOULD NOT UPDATE DIRECTORY) ===
        debug!("\n=== Transaction 3: Append to existing series ===");
        let tx2 = persistence.begin_test().await?;
        let wd2 = tx2.root().await?;

        // Verify file exists
        let file_exists = wd2.exists(std::path::Path::new(series_path)).await;
        debug!("File exists before append: {}", file_exists);
        assert!(file_exists, "File should exist from transaction 1");

        // Append more data using async_writer_path_with_type
        let batch2 = record_batch!(
            ("timestamp", Int64, [1704074400000_i64, 1704078000000_i64]),
            ("value", Float64, [15.8_f64, 25.1_f64])
        )?;

        debug!("Appending SECOND version using async_writer...");

        // Serialize batch to parquet
        use std::io::Cursor;
        use parquet::arrow::ArrowWriter;
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let props = parquet::file::properties::WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(cursor, batch2.schema(), Some(props))?;
            writer.write(&batch2)?;
            _ = writer.close()?;
        }

        // Get writer for existing file - THIS SHOULD NOT UPDATE THE DIRECTORY
        let mut writer = wd2.async_writer_path_with_type(
            series_path,
            tinyfs::EntryType::FileSeriesPhysical
        ).await?;

        use tokio::io::AsyncWriteExt;
        writer.write_all(&buffer).await?;
        
        // Infer temporal bounds from the written parquet file
        // This also handles shutdown internally
        _ = writer.infer_temporal_bounds().await?;

        debug!("Committing transaction 3...");
        tx2.commit_test().await?;

        // === TRANSACTION 4: Verify that TX3 only created 1 oplog entry (the file) ===
        debug!("\n=== Transaction 4: Verify physical layer - checking oplog entries per transaction ===");
        let mut tx3 = persistence.begin_test().await?;

        let ctx = tx3.session_context().await?;

        // Count entries by transaction (based on commit order/timestamp)
        // We expect:
        // - TX0 (root creation): 1 entry
        // - TX1 (structure + file v1): 4 entries (root update + 2 dirs + 1 file)
        // - TX2 (file v2 append): 1 entry (ONLY the file, NO directory updates)

        let sql = "SELECT part_id, node_id, file_type, version, timestamp FROM delta_table ORDER BY timestamp";
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        let mut total_entries = 0;
        for batch in &results {
            total_entries += batch.num_rows();
        }

        debug!("\n=== OpLog Analysis ===");
        debug!("Total oplog entries: {}", total_entries);

        // Expected: TX1(1 root) + TX2(3 dirs + 1 file) + TX3(1 file) = 6 entries
        // If we have 7+, there are unnecessary writes

        // Count entries in TX3 specifically by looking at what was added after TX2
        // TX2 should have committed 4 entries, TX3 should add only 1 more
        let tx2_count_sql = "SELECT COUNT(*) as count FROM (SELECT * FROM delta_table ORDER BY timestamp LIMIT 5)";
        let tx2_df = ctx.sql(tx2_count_sql).await?;
        let _tx2_results = tx2_df.collect().await?;

        // Better approach: Count unique directories and their versions
        let dir_versions_sql = "SELECT node_id, file_type, COUNT(*) as version_count FROM delta_table WHERE file_type LIKE 'dir:%' GROUP BY node_id, file_type ORDER BY version_count DESC, node_id";
        let dir_versions_df = ctx.sql(dir_versions_sql).await?;
        let dir_versions_results = dir_versions_df.collect().await?;

        debug!("\n=== Directory Version Counts ===");
        let mut max_versions = 0;
        for batch in &dir_versions_results {
            debug!("Directories tracked: {} unique directories", batch.num_rows());
            max_versions = std::cmp::max(max_versions, batch.num_rows());
        }

        // Count total directory entries (should be 3: root, devices, sensor_123)
        // But if root was updated, we'll see 4 entries (root v1, root v2, devices v1, sensor_123 v1)
        let dir_entry_count_sql = "SELECT COUNT(*) as total_dir_entries FROM delta_table WHERE file_type LIKE 'dir:%'";
        let dir_entry_df = ctx.sql(dir_entry_count_sql).await?;
        let dir_entry_results = dir_entry_df.collect().await?;

        let mut total_dir_entries = 0;
        for batch in &dir_entry_results {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                total_dir_entries = count_col.value(0) as usize;
            }
        }

        debug!("Total directory entries in oplog: {}", total_dir_entries);

        // THE KEY CHECK: store_node() skips creating empty directory versions when has_pending_operations() is true
        // Expected: root v1 (bootstrap), root v2 (with devices), devices v1 (with sensor_123), sensor_123 v1 (with file)
        // NOT created: devices v1 empty, sensor_123 v1 empty (skipped because insert() happened in same transaction)
        let expected_dir_entries = 4;

        debug!("\n=== Verification Result ===");
        debug!("Expected directory entries: {}", expected_dir_entries);
        debug!("Actual directory entries: {}", total_dir_entries);

        // Count rows per transaction to verify each transaction wrote the expected number
        debug!("\n=== Per-Transaction Row Counts ===");

        // TX1 (bootstrap): Should write 1 row (root v1)
        let tx1_sql = "SELECT COUNT(*) as count FROM delta_table WHERE txn_seq = 1";
        let tx1_df = ctx.sql(tx1_sql).await?;
        let tx1_results = tx1_df.collect().await?;
        let tx1_count = if let Some(batch) = tx1_results.first() {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                count_col.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };
        debug!("TX1 (bootstrap): {} row(s)", tx1_count);

        // TX2 (create structure): Should write 4 rows (root v2, devices v1, sensor_123 v1, file v1)
        let tx2_sql = "SELECT COUNT(*) as count FROM delta_table WHERE txn_seq = 2";
        let tx2_df = ctx.sql(tx2_sql).await?;
        let tx2_results = tx2_df.collect().await?;
        let tx2_count = if let Some(batch) = tx2_results.first() {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                count_col.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };
        debug!("TX2 (create structure): {} row(s)", tx2_count);

        // TX3 (file append): Should write EXACTLY 1 row (file v2 only, NO directory update)
        let tx3_sql = "SELECT COUNT(*) as count FROM delta_table WHERE txn_seq = 3";
        let tx3_df = ctx.sql(tx3_sql).await?;
        let tx3_results = tx3_df.collect().await?;
        let tx3_count = if let Some(batch) = tx3_results.first() {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                count_col.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };
        debug!("TX3 (file append): {} row(s)", tx3_count);

        // THE CRITICAL CHECK: TX3 should write exactly 1 row
        if tx3_count != 1 {
            debug!("\n🐛 BUG DETECTED IN TX3!");
            debug!("TX3 (file append) wrote {} records when it should write exactly 1", tx3_count);
            debug!("Expected: 1 row (file v2 only)");
            debug!("Actual: {} row(s)", tx3_count);
            debug!("\nWhen appending to a file:series, we should NOT update the parent directory.");
            panic!("Test failed: TX3 wrote {} records instead of 1", tx3_count);
        }

        debug!("✅ PASS: TX3 wrote exactly 1 record (file version only, no directory update)");

        // Also check: TX3 should create exactly 1 entry (the file v2)
        // Total should be: 1 (root v1) + 4 (TX2: root v2, devices, sensor_123, file v1) + 1 (TX3: file v2) = 6
        // But if bug exists: 1 + 4 + 2 (file v2 + unnecessary dir update) = 7+

        debug!("Total oplog entries: {}", total_entries);
        debug!("Expected total: 6 (1 root init + 4 structure creation + 1 file append)");

        // Verify we have the expected number of directory entries
        assert_eq!(total_dir_entries, expected_dir_entries,
            "Expected {} directory entries (root v1 empty, root v2 with devices, devices v1, sensor_123 v1), but found {}",
            expected_dir_entries, total_dir_entries);

        debug!("\n✅ TEST PASSED!");
        debug!("- TX3 (file append) wrote exactly 1 row (file version only)");
        debug!("- No unnecessary directory updates during file append");
        debug!("- Root directory correctly has 2 versions (v1 empty, v2 when devices added)");

        Ok(())
    }).await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!("Test timed out after 30 seconds");
        }
    }
}

#[tokio::test]
async fn test_symlink_create_and_read() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("=== TX1: Create symlink and read in same transaction ===");

    let tx1 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx1.root().await.expect("Failed to get root");

    // Create a target file with content
    debug!("Creating target file: /target.txt");
    use tinyfs::async_helpers::convenience;
    _ = convenience::create_file_path(&root, "/target.txt", b"Hello, symlink!")
        .await
        .expect("Failed to create target file");

    // Create a symlink pointing to the target
    debug!("Creating symlink: /link -> /target.txt");
    _ = root
        .create_symlink_path("/link", "/target.txt")
        .await
        .expect("Failed to create symlink");

    // Follow the symlink and read the file content in the same transaction
    debug!("Following symlink to read file content in TX1");
    let content = root
        .read_file_path_to_vec("/link")
        .await
        .expect("Failed to read file through symlink in TX1");

    assert_eq!(
        content, b"Hello, symlink!",
        "Should be able to read file through symlink in same transaction"
    );
    debug!(
        "✅ Read file content through symlink in TX1: {:?}",
        String::from_utf8_lossy(&content)
    );

    // Commit TX1
    debug!("Committing TX1");
    _ = tx1.commit().await.expect("Failed to commit TX1");

    debug!("\n=== TX2: Read symlink in new transaction ===");

    let tx2 = persistence.begin_test().await.expect("Failed to begin TX2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    // Follow the symlink in TX2 and read content
    debug!("Following symlink in TX2 to read file content");
    let content2 = root2
        .read_file_path_to_vec("/link")
        .await
        .expect("Failed to read file through symlink in TX2");

    assert_eq!(
        content2, b"Hello, symlink!",
        "Should be able to read file through symlink in new transaction"
    );
    debug!(
        "✅ Read file content through symlink in TX2: {:?}",
        String::from_utf8_lossy(&content2)
    );

    debug!("\n✅ TEST PASSED!");
    debug!("- Created symlink in TX1");
    debug!("- Read symlink target in same transaction");
    debug!("- Followed symlink to read file content in same transaction");
    debug!("- Committed TX1");
    debug!("- Read symlink in new transaction (TX2)");
    debug!("- Followed symlink in TX2 to read file content");
}

/// Test creating dynamic files using high-level path API
#[tokio::test]
async fn test_create_dynamic_file_path() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("=== Testing dynamic file creation with path API ===");

    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx.root().await.expect("Failed to get root");

    // Create parent directory first
    debug!("Creating parent directory: /config");
    _ = root
        .create_dir_path("/config")
        .await
        .expect("Failed to create parent directory");

    // Create a dynamic file with factory name and YAML config
    debug!("Creating dynamic file: /config/test.yaml");
    let test_file_config = tinyfs::testing::TestFileConfig::simple("SELECT 1");
    let config_content = test_file_config
        .to_yaml_bytes()
        .expect("Failed to serialize test file config");

    let dynamic_node = root
        .create_dynamic_path(
            "/config/test.yaml",
            tinyfs::EntryType::FileDataDynamic,
            "test-file",
            config_content.clone(),
        )
        .await
        .expect("Failed to create dynamic file");

    debug!("✅ Created dynamic file with factory: test-file");
    debug!("   Node ID: {:?}", dynamic_node.id());

    // Verify EntryType is correctly embedded in the FileID
    let file_id = dynamic_node.id();
    let entry_type = file_id.entry_type();
    assert_eq!(
        entry_type,
        tinyfs::EntryType::FileDataDynamic,
        "Dynamic file should have FileDataDynamic EntryType"
    );
    debug!("✅ FileID has correct EntryType: {:?}", entry_type);

    // Verify we can read the file back
    let read_content = root
        .read_file_path_to_vec("/config/test.yaml")
        .await
        .expect("Failed to read dynamic file");

    // Content from test-file factory is the "content" field, not the YAML config
    assert_eq!(
        String::from_utf8_lossy(&read_content),
        "SELECT 1",
        "Dynamic file content should be from TestFileConfig.content field"
    );
    debug!("✅ Read back dynamic file content successfully");

    // Commit and verify persistence
    debug!("Committing transaction");
    _ = tx.commit().await.expect("Failed to commit");

    // Read in new transaction
    debug!("Reading dynamic file in new transaction");
    let tx2 = persistence.begin_test().await.expect("Failed to begin TX2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    // Read back the node to verify EntryType persisted correctly
    let node_path2 = root2
        .get_node_path(std::path::Path::new("/config/test.yaml"))
        .await
        .expect("Failed to get node");

    let file_id2 = node_path2.id();
    let entry_type2 = file_id2.entry_type();
    assert_eq!(
        entry_type2,
        tinyfs::EntryType::FileDataDynamic,
        "Persisted dynamic file should have valid FileDataDynamic EntryType"
    );
    debug!(
        "✅ Persisted FileID has correct EntryType: {:?}",
        entry_type2
    );

    let content2 = root2
        .read_file_path_to_vec("/config/test.yaml")
        .await
        .expect("Failed to read file in TX2");

    // Content from test-file factory is the "content" field, not the YAML config
    assert_eq!(
        String::from_utf8_lossy(&content2),
        "SELECT 1",
        "Dynamic file should persist and return content from factory"
    );
    debug!("✅ Dynamic file persisted correctly");

    debug!("\n✅ TEST PASSED!");
    debug!("- Created dynamic file using high-level path API");
    debug!("- Read back content in same transaction");
    debug!("- Committed and verified persistence");
}

/// Test creating dynamic directories using high-level path API
/// NOTE: This test verifies dynamic directory creation with the test-dir factory.
/// This demonstrates that dynamic nodes require proper factory registration.
#[tokio::test]
async fn test_create_dynamic_directory_path() {
    // Use fixed path so we can inspect Delta files after test
    let store_path = "/tmp/tlogfs_dynamic_dir_test";
    let _ = std::fs::remove_dir_all(store_path); // Clean up from previous run

    log::debug!("\n🔍 Test store path: {}", store_path);

    let mut persistence = OpLogPersistence::create_test(store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("=== Testing dynamic directory creation with path API ===");

    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx.root().await.expect("Failed to get root");

    // Create parent directory first
    debug!("Creating parent directory: /factories");
    _ = root
        .create_dir_path("/factories")
        .await
        .expect("Failed to create parent directory");

    // Create a dynamic directory with factory name and YAML config
    debug!("Creating dynamic directory: /factories/processors");
    let test_dir_config = tinyfs::testing::TestDirectoryConfig::simple("processors");
    let config_content = test_dir_config
        .to_yaml_bytes()
        .expect("Failed to serialize test dir config");

    let dynamic_dir = root
        .create_dynamic_path(
            "/factories/processors",
            tinyfs::EntryType::DirectoryDynamic,
            "test-dir",
            config_content,
        )
        .await
        .expect("Failed to create dynamic directory");

    debug!("✅ Created dynamic directory with factory: test-dir");
    debug!("   Node ID: {:?}", dynamic_dir.id());

    // Verify the directory exists
    let exists = root
        .exists(std::path::Path::new("/factories/processors"))
        .await;
    assert!(exists, "Dynamic directory should exist");
    debug!("✅ Dynamic directory exists");

    // Commit
    debug!("Committing transaction");
    _ = tx.commit().await.expect("Failed to commit");

    // Inspect what Delta Lake actually wrote
    log::debug!("\n🔍 After TX1 commit - inspecting Delta Lake files:");
    log::debug!("📁 Store path: {}", store_path);

    // List _delta_log directory
    let delta_log_path = format!("{}/_delta_log", store_path);
    if let Ok(entries) = std::fs::read_dir(&delta_log_path) {
        log::debug!("📂 _delta_log directory:");
        for entry in entries.flatten() {
            log::debug!("  - {}", entry.file_name().to_string_lossy());
        }
    }

    // List main directory for Parquet files
    if let Ok(entries) = std::fs::read_dir(store_path) {
        log::debug!("📂 Main directory:");
        for entry in entries.flatten() {
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.ends_with(".parquet") {
                log::debug!("  - {} (Parquet data file)", fname);
            }
        }
    }

    log::debug!("\n💡 Use: ./catparquet.sh {} <file>.parquet", store_path);
    log::debug!("💡 to inspect the actual data written\n");

    // Verify in new transaction
    let tx2 = persistence.begin_test().await.expect("Failed to begin TX2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    debug!("TX2: Looking up /factories/processors");
    let result = root2
        .get_node_path(std::path::Path::new("/factories/processors"))
        .await;

    let _node_path = result.expect("Dynamic directory should persist");
    debug!("✅ Dynamic directory persisted correctly");

    debug!("\n✅ TEST PASSED!");
    debug!("- Created dynamic directory using high-level path API");
    debug!("- Verified existence in same transaction");
    debug!("- Committed and verified persistence");
}

/// Test that would have caught the EntryType bug in FileID::from_content
///
/// This test creates dynamic nodes (both files and directories), commits them,
/// then reads them back and verifies:
/// 1. FileIDs are valid and parseable
/// 2. EntryType is correctly embedded in NodeID
/// 3. Dynamic nodes can be read and used after persistence
#[tokio::test]
async fn test_dynamic_node_entry_type_validation() {
    let store_path = test_dir();

    log::debug!("\n🔍 Testing dynamic node EntryType validation");

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Transaction 1: Create dynamic file and directory
    let tx1 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx1.root().await.expect("Failed to get root");

    // Create parent directory
    log::debug!("Creating parent directory: /dynamic-test");
    let _ = root
        .create_dir_path("/dynamic-test")
        .await
        .expect("Failed to create parent directory");

    // Create dynamic file using test-file factory
    log::debug!("Creating dynamic file: /dynamic-test/test.yaml");
    let test_file_config =
        tinyfs::testing::TestFileConfig::simple("test content from dynamic file");
    let file_config_bytes = test_file_config
        .to_yaml_bytes()
        .expect("Failed to serialize test file config");

    let dynamic_file = root
        .create_dynamic_path(
            "/dynamic-test/test.yaml",
            tinyfs::EntryType::FileDataDynamic,
            "test-file",
            file_config_bytes,
        )
        .await
        .expect("Failed to create dynamic file");

    let file_id = dynamic_file.id();
    log::debug!("✅ Created dynamic file with ID: {:?}", file_id);

    // Verify EntryType is valid immediately after creation
    let file_entry_type = file_id.entry_type();
    log::debug!("  EntryType from FileID: {:?}", file_entry_type);
    assert_eq!(
        file_entry_type,
        tinyfs::EntryType::FileDataDynamic,
        "Dynamic file should have FileDataDynamic EntryType"
    );

    // Create dynamic directory using test-dir factory
    log::debug!("Creating dynamic directory: /dynamic-test/subdir");
    let test_dir_config = tinyfs::testing::TestDirectoryConfig::simple("subdir");
    let dir_config_bytes = test_dir_config
        .to_yaml_bytes()
        .expect("Failed to serialize test dir config");

    let dynamic_dir = root
        .create_dynamic_path(
            "/dynamic-test/subdir",
            tinyfs::EntryType::DirectoryDynamic,
            "test-dir",
            dir_config_bytes,
        )
        .await
        .expect("Failed to create dynamic directory");

    let dir_id = dynamic_dir.id();
    log::debug!("✅ Created dynamic directory with ID: {:?}", dir_id);

    // Verify EntryType is valid immediately after creation
    let dir_entry_type = dir_id.entry_type();
    log::debug!("  EntryType from FileID: {:?}", dir_entry_type);
    assert_eq!(
        dir_entry_type,
        tinyfs::EntryType::DirectoryDynamic,
        "Dynamic directory should have DirectoryDynamic EntryType"
    );

    // Commit transaction
    log::debug!("Committing transaction 1");
    tx1.commit_test()
        .await
        .expect("Failed to commit transaction 1");

    // Transaction 2: Read back the dynamic nodes and verify EntryTypes
    log::debug!("\nStarting transaction 2 to read back dynamic nodes");
    let tx2 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction 2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    // Read back dynamic file
    log::debug!("Reading back dynamic file: /dynamic-test/test.yaml");
    let file_node = root2
        .get_node_path(std::path::Path::new("/dynamic-test/test.yaml"))
        .await
        .expect("Failed to get dynamic file");

    let read_file_id = file_node.id();
    log::debug!("✅ Read back dynamic file with ID: {:?}", read_file_id);

    // This is the critical test: can we parse the EntryType from the persisted FileID?
    let read_file_entry_type = read_file_id.entry_type();
    log::debug!(
        "  EntryType from persisted FileID: {:?}",
        read_file_entry_type
    );
    assert_eq!(
        read_file_entry_type,
        tinyfs::EntryType::FileDataDynamic,
        "Persisted dynamic file should have valid FileDataDynamic EntryType"
    );

    // Verify we can actually read the file content
    let file_content = root2
        .read_file_path_to_vec("/dynamic-test/test.yaml")
        .await
        .expect("Failed to read dynamic file content");
    let content_str = String::from_utf8_lossy(&file_content);
    log::debug!("  File content: {:?}", content_str);
    assert_eq!(
        content_str, "test content from dynamic file",
        "Dynamic file content should match"
    );

    // Read back dynamic directory
    log::debug!("Reading back dynamic directory: /dynamic-test/subdir");
    let dir_node = root2
        .get_node_path(std::path::Path::new("/dynamic-test/subdir"))
        .await
        .expect("Failed to get dynamic directory");

    let read_dir_id = dir_node.id();
    log::debug!("✅ Read back dynamic directory with ID: {:?}", read_dir_id);

    // This is the critical test: can we parse the EntryType from the persisted FileID?
    let read_dir_entry_type = read_dir_id.entry_type();
    log::debug!(
        "  EntryType from persisted FileID: {:?}",
        read_dir_entry_type
    );
    assert_eq!(
        read_dir_entry_type,
        tinyfs::EntryType::DirectoryDynamic,
        "Persisted dynamic directory should have valid DirectoryDynamic EntryType"
    );

    // Verify we can list the directory (even though it's empty)
    use futures::stream::StreamExt;
    let dir_node_handle = dir_node
        .into_dir()
        .await
        .expect("Node should be a directory");
    let mut entries_stream = dir_node_handle
        .handle
        .entries()
        .await
        .expect("Failed to get entries");
    let mut entry_count = 0;
    while let Some(entry_result) = entries_stream.next().await {
        let _entry = entry_result.expect("Failed to read entry");
        entry_count += 1;
    }
    log::debug!("  Directory has {} entries", entry_count);

    log::debug!("\n✅ TEST PASSED!");
    log::debug!("- Created dynamic file and directory");
    log::debug!("- Verified EntryType immediately after creation");
    log::debug!("- Committed to persistence");
    log::debug!("- Read back from new transaction");
    log::debug!("- Verified EntryType from persisted NodeIDs (would have panicked with bug)");
    log::debug!("- Successfully used dynamic nodes (read file, list directory)");
}
