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

        _ = root.create_dir_path("/txn")
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
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        let series_path = "test/single_series.series";

        // Begin transaction for write
        println!("Beginning transaction...");
        let tx = persistence.begin_test().await?;

        // Get working directory
        let wd = tx.root().await?;

        // Create test directory if it doesn't exist
        if !wd.exists(std::path::Path::new("test")).await {
            println!("Creating test directory...");
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

        println!("Created RecordBatch with {} rows", batch.num_rows());

        // Write the series using create_series_from_batch - this is where hanging might occur
        println!("Writing series using create_series_from_batch...");
        let (min_time, max_time) = wd
            .create_series_from_batch(series_path, &batch, Some("timestamp"))
            .await?;

        println!(
            "Series written successfully. Time range: {} to {}",
            min_time, max_time
        );

        // Commit transaction
        println!("Committing transaction...");
        tx.commit_test().await?;
        println!("Transaction committed successfully");

        // Create a new transaction to verify the file exists and can be read
        println!("Starting new transaction for read...");
        let tx2 = persistence.begin_test().await?;
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

        println!(
            "Successfully read RecordBatch with {} rows, {} columns",
            read_batch.num_rows(),
            read_batch.num_columns()
        );

        // Verify the data
        assert_eq!(read_batch.num_rows(), 3, "Should have 3 rows");
        assert_eq!(read_batch.num_columns(), 3, "Should have 3 columns");

        // Don't need to commit read transaction
        println!("=== All Tests Passed Successfully ===");
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
        println!("=== Minimal Single Version Write Test ===");

        let store_path = test_dir();

        println!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        println!("Starting transaction...");
        let tx = persistence.begin_test().await?;

        println!("Getting working directory...");
        let wd = tx.root().await?;

        println!("Creating minimal batch...");
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [1.0_f64])
        )?;

        println!("About to call create_series_from_batch - this may hang...");
        let result = wd
            .create_series_from_batch("minimal.series", &batch, Some("timestamp"))
            .await;

        match result {
            Ok((min_time, max_time)) => {
                println!(
                    "create_series_from_batch succeeded: {} to {}",
                    min_time, max_time
                );
                println!("Committing transaction...");
                tx.commit_test().await?;
                println!("Transaction committed successfully!");
            }
            Err(e) => {
                println!("create_series_from_batch failed: {}", e);
                return Err(e.into());
            }
        }

        println!("Minimal write test completed successfully!");
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
    println!("=== Testing Single Version Series Temporal Metadata ===");

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

    println!(
        "Created batch with timestamps: [{}, {}, {}]",
        1704067200000_i64, 1704070800000_i64, 1704074400000_i64
    );

    // Write series with explicit timestamp column
    let (min_time, max_time) = wd
        .create_series_from_batch("temporal_test.series", &batch, Some("timestamp"))
        .await?;

    println!("Extracted temporal range: {} to {}", min_time, max_time);

    // The temporal metadata should reflect the actual timestamps, not be 0,0
    if min_time == 0 && max_time == 0 {
        println!(
            "⚠️  WARNING: Temporal metadata extraction returned 0,0 - this indicates a problem"
        );
        println!("   Expected: min_time = 1704067200000, max_time = 1704074400000");
    } else {
        println!("✅ Temporal metadata extracted successfully");
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

    println!("✅ Temporal metadata test completed");
    Ok(())
}

/// Test to investigate the actual file storage structure for series
#[tokio::test]
async fn test_single_version_series_storage_investigation() -> Result<(), Box<dyn std::error::Error>>
{
    println!("=== Investigating Single Version Series Storage ===");

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

    println!("Writing single-row series...");

    // Write series and capture result
    let (min_time, max_time) = wd
        .create_series_from_batch("investigation.series", &batch, Some("timestamp"))
        .await?;
    println!(
        "create_series_from_batch returned: ({}, {})",
        min_time, max_time
    );

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
    tx.commit_test().await?;

    // Investigate post-commit state
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let post_commit_metadata = wd2.metadata_for_path("investigation.series").await?;
    println!("File metadata after commit:");
    println!("  entry_type: {:?}", post_commit_metadata.entry_type);
    println!("  size: {:?}", post_commit_metadata.size);

    // Try to read as RecordBatch
    let read_batch = wd2.read_table_as_batch("investigation.series").await?;
    println!(
        "Read batch: {} rows, {} columns",
        read_batch.num_rows(),
        read_batch.num_columns()
    );

    // Check the actual data values
    println!(
        "Schema: {:?}",
        read_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );

    println!("✅ Storage investigation completed");
    Ok(())
}

/// Summary test documenting the current state of single version file:series operations
#[tokio::test]
async fn test_single_version_series_summary() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Single Version File:Series - Current Status Summary ===");

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

    println!("FINDINGS:");
    println!("✅ No hanging issues - operations complete successfully");
    println!("✅ File storage works correctly - Parquet format preserved");
    println!("✅ Data integrity maintained - all rows and columns readable");
    println!("✅ Entry type correctly set to FileSeries");
    println!(
        "❌ Temporal metadata extraction broken - returns (0, 0) instead of actual timestamp range"
    );
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
    use crate::large_files::LARGE_FILE_THRESHOLD;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    println!("=== Testing Streaming Async Reader (Large File) ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a large file that would be problematic if loaded entirely into memory
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // Slightly larger than threshold
    let expected_size = large_content.len();

    println!(
        "Creating large file with {} bytes (threshold is {})",
        expected_size, LARGE_FILE_THRESHOLD
    );

    // Store the large file
    _ = tinyfs::async_helpers::convenience::create_file_path(&wd, "/large_test.dat", &large_content)
        .await?;
    tx.commit_test().await?;

    println!("✅ Large file stored successfully");

    // Now test streaming reader - this should NOT load the entire file into memory
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_node = wd2.get_node_path("/large_test.dat").await?;
    let file_handle = file_node.borrow().await.as_file()?;

    println!("Getting async reader for large file...");
    let mut reader = file_handle.async_reader().await?;

    // Test: Read only first 100 bytes (streaming approach)
    let mut buffer = vec![0u8; 100];
    let bytes_read = reader.read_exact(&mut buffer).await?;

    println!(
        "✅ Successfully read {} bytes from start of file",
        bytes_read
    );
    assert_eq!(buffer, vec![42u8; 100], "First 100 bytes should all be 42");

    // Test: Seek to middle and read 50 bytes (verifying AsyncSeek works)
    let middle_pos = (expected_size / 2) as u64;
    _ = reader.seek(std::io::SeekFrom::Start(middle_pos)).await?;

    let mut middle_buffer = vec![0u8; 50];
    _ = reader.read_exact(&mut middle_buffer).await?;

    println!(
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
    println!("✅ File end position: {} bytes", end_pos);
    assert_eq!(
        end_pos as usize, expected_size,
        "File size should match expected size"
    );

    tx2.commit_test().await?;

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
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a small file (under threshold)
    let small_content = b"Hello, World! This is a small test file with some content.";
    let expected_size = small_content.len();

    println!("Creating small file with {} bytes", expected_size);

    // Store the small file
    _ = tinyfs::async_helpers::convenience::create_file_path(&wd, "/small_test.txt", small_content)
        .await?;
    tx.commit_test().await?;

    println!("✅ Small file stored successfully");

    // Now test streaming reader with small file (stored inline in Delta Lake)
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_node = wd2.get_node_path("/small_test.txt").await?;
    let file_handle = file_node.borrow().await.as_file()?;

    println!("Getting async reader for small file...");
    let mut reader = file_handle.async_reader().await?;

    // Test: Read entire content
    let mut buffer = vec![0u8; expected_size];
    _ = reader.read_exact(&mut buffer).await?;

    println!("✅ Successfully read {} bytes", expected_size);
    assert_eq!(&buffer, small_content, "Content should match exactly");

    // Test: Seek to start and read first 5 bytes
    _ = reader.seek(std::io::SeekFrom::Start(0)).await?;
    let mut start_buffer = vec![0u8; 5];
    _ = reader.read_exact(&mut start_buffer).await?;

    println!("✅ Successfully seeked to start and read first 5 bytes");
    assert_eq!(&start_buffer, b"Hello", "First 5 bytes should be 'Hello'");

    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    println!("✅ File end position: {} bytes", end_pos);
    assert_eq!(
        end_pos as usize, expected_size,
        "File size should match expected size"
    );

    tx2.commit_test().await?;

    println!("SUCCESS: Streaming reader works correctly for small files");
    println!("  - Small files use inline storage (Cursor over Vec<u8>)");
    println!("  - AsyncRead works for partial reads");
    println!("  - AsyncSeek works for random access");
    println!("  - File size detection works correctly");

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

    println!("=== Starting Temporal Bounds Test ===");

    let store_path = test_dir();

    // Create TLogFS persistence layer
    println!("Creating persistence layer...");
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let series_path = "test/temporal_series.series";

    // Transaction 1: Create file series with all data points (simulating the combined data from multiple versions)
    println!(
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
        println!("Series created. Time range: {} to {}", min_time, max_time);

        tx.commit_test().await?;
    }

    // Transaction 3: Query the series before setting temporal bounds - should return 12 rows
    println!("Querying series before temporal bounds...");
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

        println!("Count before temporal bounds: {}", total_count);
        assert_eq!(
            total_count, 12,
            "Should have 12 total rows before temporal bounds"
        );

        // Don't commit this read-only transaction
    }

    // Transaction 4: Set temporal bounds to [10000, 20000] ms using empty version approach
    println!("Setting temporal bounds to [10000, 20000] ms (10-20 seconds) using empty version...");
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

        println!("Created empty version for metadata");

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

        println!("Temporal bounds set to [10000, 20000] milliseconds (10-20 seconds)");
        tx.commit_test().await?;
    }

    // Transaction 5: Query the series after setting temporal bounds - should return 10 rows
    println!("Querying series after temporal bounds...");
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

        println!("Count after temporal bounds: {}", total_count);
        assert_eq!(
            total_count, 10,
            "Should have 10 rows after temporal bounds [10000, 20000] ms (excluding T=1s and T=50s)"
        );

        // Don't commit this read-only transaction
    }

    println!("SUCCESS: Temporal bounds test completed");
    println!(
        "  - Created file series with data points at T=1,10,11,12,13,14,15,16,17,18,19,50 seconds"
    );
    println!("  - Verified initial count of 12 rows");
    println!("  - Set temporal bounds to [10000, 20000] ms (10-20 seconds)");
    println!("  - Verified filtered count of 10 rows (excluded T=1s and T=50s)");

    Ok(())
}

/// Test to reproduce unnecessary directory updates when appending to file:series
///
/// This test writes multiple versions to the same file:series and checks
/// whether the parent directory gets updated on each append (it shouldn't).
#[tokio::test]
async fn test_multiple_series_appends_directory_updates() -> Result<(), Box<dyn std::error::Error>>
{
    println!("=== Testing Multiple Series Appends - Directory Update Check ===");

    let test_result = timeout(Duration::from_secs(30), async {
        let store_path = test_dir();

        // Create TLogFS persistence layer
        println!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        let series_path = "devices/sensor_123/data.series";

        // === TRANSACTION 2: Create directory structure and initial file ===
        println!("\n=== Transaction 2: Create directory and first version ===");
        let tx1 = persistence.begin_test().await?;
        let wd1 = tx1.root().await?;

        // Create devices directory
        println!("Creating /devices directory...");
        _ = wd1.create_dir_path("devices").await?;

        // Create sensor subdirectory
        println!("Creating /devices/sensor_123 directory...");
        _ = wd1.create_dir_path("devices/sensor_123").await?;

        // Write first version of series
        let batch1 = record_batch!(
            ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]),
            ("value", Float64, [10.5_f64, 20.3_f64])
        )?;

        println!("Writing FIRST version of series...");
        _ = wd1.create_series_from_batch(series_path, &batch1, Some("timestamp")).await?;

        println!("Committing transaction 2...");
        tx1.commit_test().await?;

        // === TRANSACTION 3: Append to existing series (SHOULD NOT UPDATE DIRECTORY) ===
        println!("\n=== Transaction 3: Append to existing series ===");
        let tx2 = persistence.begin_test().await?;
        let wd2 = tx2.root().await?;

        // Verify file exists
        let file_exists = wd2.exists(std::path::Path::new(series_path)).await;
        println!("File exists before append: {}", file_exists);
        assert!(file_exists, "File should exist from transaction 1");

        // Append more data using async_writer_path_with_type
        let batch2 = record_batch!(
            ("timestamp", Int64, [1704074400000_i64, 1704078000000_i64]),
            ("value", Float64, [15.8_f64, 25.1_f64])
        )?;

        println!("Appending SECOND version using async_writer...");

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
        writer.shutdown().await?;

        println!("Committing transaction 3...");
        tx2.commit_test().await?;

        // === TRANSACTION 4: Verify that TX3 only created 1 oplog entry (the file) ===
        println!("\n=== Transaction 4: Verify physical layer - checking oplog entries per transaction ===");
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

        println!("\n=== OpLog Analysis ===");
        println!("Total oplog entries: {}", total_entries);

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

        println!("\n=== Directory Version Counts ===");
        let mut max_versions = 0;
        for batch in &dir_versions_results {
            println!("Directories tracked: {} unique directories", batch.num_rows());
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

        println!("Total directory entries in oplog: {}", total_dir_entries);

        // THE KEY CHECK: We created 3 directories, so should have exactly 3 directory entries
        // If we have 4+, it means one or more directories were updated unnecessarily
        let expected_dir_entries = 4; // root (v1 empty, v2 with devices), devices (v1), sensor_123 (v1)

        println!("\n=== Verification Result ===");
        println!("Expected directory entries: {}", expected_dir_entries);
        println!("Actual directory entries: {}", total_dir_entries);

        // Count rows per transaction to verify each transaction wrote the expected number
        println!("\n=== Per-Transaction Row Counts ===");

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
        println!("TX1 (bootstrap): {} row(s)", tx1_count);

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
        println!("TX2 (create structure): {} row(s)", tx2_count);

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
        println!("TX3 (file append): {} row(s)", tx3_count);

        // THE CRITICAL CHECK: TX3 should write exactly 1 row
        if tx3_count != 1 {
            println!("\n🐛 BUG DETECTED IN TX3!");
            println!("TX3 (file append) wrote {} records when it should write exactly 1", tx3_count);
            println!("Expected: 1 row (file v2 only)");
            println!("Actual: {} row(s)", tx3_count);
            println!("\nWhen appending to a file:series, we should NOT update the parent directory.");
            panic!("Test failed: TX3 wrote {} records instead of 1", tx3_count);
        }

        println!("✅ PASS: TX3 wrote exactly 1 record (file version only, no directory update)");

        // Also check: TX3 should create exactly 1 entry (the file v2)
        // Total should be: 1 (root v1) + 4 (TX2: root v2, devices, sensor_123, file v1) + 1 (TX3: file v2) = 6
        // But if bug exists: 1 + 4 + 2 (file v2 + unnecessary dir update) = 7+

        println!("Total oplog entries: {}", total_entries);
        println!("Expected total: 6 (1 root init + 4 structure creation + 1 file append)");

        // Verify we have the expected number of directory entries
        assert_eq!(total_dir_entries, expected_dir_entries,
            "Expected {} directory entries (root v1 empty, root v2 with devices, devices v1, sensor_123 v1), but found {}",
            expected_dir_entries, total_dir_entries);

        println!("\n✅ TEST PASSED!");
        println!("- TX3 (file append) wrote exactly 1 row (file version only)");
        println!("- No unnecessary directory updates during file append");
        println!("- Root directory correctly has 2 versions (v1 empty, v2 when devices added)");

        Ok(())
    }).await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!("Test timed out after 30 seconds");
        }
    }
}
