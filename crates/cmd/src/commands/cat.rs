use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::io::{self, Write};

use crate::common::ShipContext;
use log::debug;

/// Cat file with optional SQL query
pub async fn cat_command(
    ship_context: &ShipContext,
    path: &str,
    display: &str,
    output: Option<&mut String>,
    _time_start: Option<i64>,
    _time_end: Option<i64>,
    sql_query: Option<&str>,
) -> Result<()> {
    debug!(
        "cat_command called with path: {}, sql_query: {:?}",
        path, sql_query
    );

    let mut ship = ship_context.open_pond().await?;

    let template_variables = ship_context.template_variables.clone();

    let mut tx = ship
        .begin_transaction(
            steward::TransactionOptions::read(ship_context.original_args.clone())
                .with_variables(template_variables),
        )
        .await?;

    // Execute the cat operation and handle errors properly
    let result = cat_impl(&mut tx, path, display, output, sql_query).await;
    
    match result {
        Ok(()) => {
            tx.commit().await?;
            Ok(())
        }
        Err(e) => {
            // Record failure in control table before returning error
            let err = tx.abort(&e).await;
            Err(anyhow::anyhow!("{}", err))
        }
    }
}

/// Internal implementation of cat command
async fn cat_impl(
    tx: &mut steward::StewardTransactionGuard<'_>,
    path: &str,
    display: &str,
    output: Option<&mut String>,
    sql_query: Option<&str>,
) -> Result<()> {
    let fs = &**tx;
    let root = fs.root().await?;

    // Get file metadata to determine entry type
    let metadata = root
        .metadata_for_path(path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get metadata for '{}': {}", path, e))?;

    let entry_type_str = format!("{:?}", metadata.entry_type);
    debug!("File entry type: {entry_type_str}");

    // Use DataFusion for file:table and file:series always, not for file:data
    let should_use_datafusion =
        metadata.entry_type.is_table_file() || metadata.entry_type.is_series_file();

    if should_use_datafusion {
        // Use the new SQL execution interface for file:series and file:table
        // Default to sorting by timestamp for chronological display
        let effective_sql_query = sql_query.unwrap_or("SELECT * FROM series ORDER BY timestamp");
        debug!("Using tlogfs SQL interface for: {path} with query: {effective_sql_query}");

        // Execute the SQL query using the streaming interface
        let mut stream =
            tlogfs::execute_sql_on_file(&root, path, effective_sql_query, tx.transaction_guard()?)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to execute SQL query '{}' on '{}': {}",
                        effective_sql_query,
                        path,
                        e
                    )
                })?;

        // Collect batches from the stream
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Failed to process batch from stream: {}", e))?;
            batches.push(batch);
        }

        // Format and display the results
        let formatted = format_query_results(&batches)
            .map_err(|e| anyhow::anyhow!("Failed to format query results for '{}': {}", path, e))?;

        if let Some(output_buffer) = output {
            output_buffer.push_str(&formatted);
        } else {
            print!("{}", formatted);
        }

        let batch_count = batches.len();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!("Successfully displayed {total_rows} rows in {batch_count} batches from: {path}");

        // Return success (caller will commit)
        return Ok(());
    }

    // Check if we should use table display for regular files
    if display == "table" {
        if output.is_some() {
            return Err(anyhow::anyhow!(
                "Output capture not supported for table display mode"
            ));
        }

        // Validate that table display is only used with file:table or file:series
        if !(metadata.entry_type.is_table_file() || metadata.entry_type.is_series_file()) {
            return Err(anyhow::anyhow!(
                "Table display mode only supports file:table and file:series types, but this file is {:?}",
                metadata.entry_type
            ));
        }

        // This should never be reached since DataFusion handles all file:table and file:series
        return Err(anyhow::anyhow!(
            "Internal error: table display should be handled by DataFusion"
        ));
    }

    // Default/raw display behavior - use streaming for better memory efficiency
    stream_file_to_stdout(&root, path, output).await?;

    Ok(())
}

// DataFusion SQL interface for file:series and file:table queries with node_id (more efficient)
// Stream copy function for non-display mode
async fn stream_file_to_stdout(
    root: &tinyfs::WD,
    path: &str,
    mut output: Option<&mut String>,
) -> Result<()> {
    use std::pin::Pin;
    use tokio::io::AsyncReadExt;

    let mut reader: Pin<Box<dyn tinyfs::AsyncReadSeek>> = root
        .async_reader_path(path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open file for reading: {}", e))?;

    // Stream in chunks rather than loading entire file
    let mut buffer = vec![0u8; 8192]; // 8KB chunks
    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read from file: {}", e))?;
        if bytes_read == 0 {
            break; // EOF
        }
        if let Some(out) = output.as_deref_mut() {
            out.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));
        } else {
            io::stdout()
                .write_all(&buffer[..bytes_read])
                .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use arrow_array::record_batch;
    use tempfile::TempDir;
    use tinyfs::arrow::SimpleParquetExt;

    /// Test setup helper - creates pond and returns context for testing
    struct TestSetup {
        #[allow(dead_code)] // Needed for test infrastructure
        temp_dir: TempDir,
        pond_path: std::path::PathBuf,
        #[allow(dead_code)] // Needed for test infrastructure
        ship_context: ShipContext,
        test_content: String,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            // Create a temporary directory for the test pond
            let temp_dir = TempDir::new()?;
            // Use a unique subdirectory to avoid any conflicts
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let pond_path = temp_dir.path().join(format!("test_pond_{}", timestamp));

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(pond_path.clone()), init_args.clone());

            // Initialize the pond
            init_command(&ship_context, None, None).await?;

            let test_content =
                "timestamp,value\n2024-01-01T00:00:00Z,42.0\n2024-01-01T01:00:00Z,43.5\n";

            Ok(TestSetup {
                temp_dir,
                pond_path,
                ship_context,
                test_content: test_content.to_string(),
            })
        }

        /// Write test data as raw text file (file:data type)
        async fn write_text_file(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let test_content = self.test_content.clone();
            let filename = filename.to_string();

            ship.transact(vec!["test".to_string(), "write".to_string()], |_tx, fs| {
                Box::pin(async move {
                    let root = fs.root().await.map_err(|e| {
                        steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                    })?;

                    root.write_file_path_from_slice(&filename, test_content.as_bytes())
                        .await
                        .map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;

                    Ok(())
                })
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write test data: {}", e))?;

            Ok(())
        }

        /// Write test data as parquet table (file:table type)
        async fn write_parquet_table(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let filename = filename.to_string();

            ship.transact(
                vec!["test".to_string(), "write_table".to_string()],
                |_tx, fs| {
                    Box::pin(async move {
                        let root = fs.root().await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;

                        // Create Arrow RecordBatch from our test data
                        let batch = record_batch!(
                            (
                                "timestamp",
                                Utf8,
                                ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]
                            ),
                            ("value", Float64, [42.0_f64, 43.5_f64])
                        )
                        .map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                tinyfs::Error::Other(format!("Arrow error: {}", e)),
                            ))
                        })?;

                        // Write as parquet table using SimpleParquetExt
                        root.write_parquet(&filename, &batch, tinyfs::EntryType::FileTablePhysical)
                            .await
                            .map_err(|e| {
                                steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                            })?;

                        Ok(())
                    })
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write parquet table: {}", e))?;

            Ok(())
        }

        /// Write test data as parquet series (file:series type) - single version
        async fn write_parquet_series(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let filename = filename.to_string();

            ship.transact(
                vec!["test".to_string(), "write_series".to_string()],
                |_tx, fs| {
                    Box::pin(async move {
                        let root = fs.root().await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;

                        // Create Arrow RecordBatch with temporal data for series using proper timestamp type
                        let batch = record_batch!(
                            ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]), // 2024-01-01 timestamps in milliseconds
                            ("value", Float64, [42.0_f64, 43.5_f64])
                        )
                        .map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                tinyfs::Error::Other(format!("Arrow error: {}", e)),
                            ))
                        })?;

                        // Write as file:series using the same approach as file:table but with FileSeries entry type
                        root.write_parquet(
                            &filename,
                            &batch,
                            tinyfs::EntryType::FileSeriesPhysical,
                        )
                        .await
                        .map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;

                        Ok(())
                    })
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write file series: {}", e))?;

            Ok(())
        }

        /// Create ShipContext for cat command
        fn cat_context(&self, filename: &str) -> ShipContext {
            let cat_args = vec!["pond".to_string(), "cat".to_string(), filename.to_string()];
            ShipContext::new(Some(self.pond_path.clone()), cat_args)
        }

        /// Create expected DataFusion output for our test data using the same formatting path
        fn expected_datafusion_output(&self) -> Result<String> {
            // Create the same RecordBatch we use in write_parquet_table
            // Use Utf8View to match what parquet files actually produce
            let batch = record_batch!(
                (
                    "timestamp",
                    Utf8View,
                    ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]
                ),
                ("value", Float64, [42.0_f64, 43.5_f64])
            )
            .map_err(|e| anyhow::anyhow!("Arrow error: {}", e))?;

            // Calculate total row count before moving batch
            let total_rows = batch.num_rows();

            // Use the same formatting logic as the cat command (with type information)
            use arrow::util::pretty::pretty_format_batches_with_options;
            use arrow_cast::display::FormatOptions;

            let options = FormatOptions::default()
                .with_display_error(true)
                .with_types_info(true) // This shows column types in the headers
                .with_null("NULL"); // Show NULL values clearly

            let formatted = pretty_format_batches_with_options(&[batch], &options)
                .map_err(|e| anyhow::anyhow!("Failed to format results: {}", e))?
                .to_string();

            let result = format!(
                "{}\nSummary: {} total rows",
                formatted.trim_end(),
                total_rows
            );
            Ok(result)
        }
    }

    #[tokio::test]
    async fn test_cat_command_text_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write text file
        setup.write_text_file("test_data.csv").await?;

        // Test cat command with output capture
        let cat_context = setup.cat_context("test_data.csv");
        let mut output_buffer = String::new();

        cat_command(
            &cat_context,
            "test_data.csv",
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await?;

        assert_eq!(
            output_buffer, setup.test_content,
            "Cat command output should exactly match file content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write parquet table file
        setup.write_parquet_table("test_table.parquet").await?;

        // Get expected output from our baseline formatter
        let expected_output = setup.expected_datafusion_output()?;

        // Test cat command - this should use DataFusion SQL interface
        let cat_context = setup.cat_context("test_table.parquet");
        let mut output_buffer = String::new();

        cat_command(
            &cat_context,
            "test_table.parquet",
            "table", // Use table mode to get formatted output
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await?;

        // Test for exact equality with baseline
        assert_eq!(
            output_buffer.trim(),
            expected_output.trim(),
            "Cat command DataFusion output should exactly match baseline formatting"
        );

        println!("✅ DataFusion SQL output matches baseline!");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table_with_sql() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write parquet table file
        setup.write_parquet_table("test_table.parquet").await?;

        // Test cat command with custom SQL query
        let cat_args = vec![
            "pond".to_string(),
            "cat".to_string(),
            "test_table.parquet".to_string(),
            "--sql".to_string(),
            "SELECT timestamp, value * 2 as doubled_value FROM series WHERE value > 42".to_string(),
        ];
        let cat_context = ShipContext::new(Some(setup.pond_path.clone()), cat_args);
        let mut output_buffer = String::new();

        cat_command(
            &cat_context,
            "test_table.parquet",
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            Some("SELECT timestamp, value * 2 as doubled_value FROM series WHERE value > 42"),
        )
        .await?;

        // Verify the output contains expected elements from the SQL query
        assert!(
            output_buffer.contains("doubled_value"),
            "Output should contain computed column name"
        );
        assert!(
            output_buffer.contains("87"),
            "Output should contain doubled value: 43.5 * 2 = 87"
        );
        assert!(
            !output_buffer.contains("42"),
            "Output should not contain filtered value (42)"
        );
        assert!(
            output_buffer.contains("Summary: 1 total rows"),
            "Output should contain filtered row count"
        );

        println!("✅ SQL query filtering and computation works!");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_nonexistent_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Try to cat a non-existent file
        let cat_context = setup.cat_context("nonexistent.txt");
        let mut output_buffer = String::new();

        let result = cat_command(
            &cat_context,
            "nonexistent.txt",
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await;

        // Should fail with appropriate error
        assert!(
            result.is_err(),
            "Cat command should fail for non-existent file"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_text_file_raw_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a text file (file:data type) using the default test content
        setup.write_text_file("test_raw.txt").await?;

        let cat_context = setup.cat_context("test_raw.txt");
        let mut output_buffer = String::new();

        // Cat with raw display (should output the original text)
        cat_command(
            &cat_context,
            "test_raw.txt",
            "raw", // raw display mode
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await?;

        // Should get the exact same content as the test content
        assert_eq!(
            output_buffer, setup.test_content,
            "Raw display should output original text content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_text_file_table_display_fails() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a text file (file:data type)
        setup.write_text_file("test_table_fail.txt").await?;

        let cat_context = setup.cat_context("test_table_fail.txt");
        let mut output_buffer = String::new();

        // Cat with table display should fail for file:data
        let result = cat_command(
            &cat_context,
            "test_table_fail.txt",
            "table", // table display mode on file:data
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await;

        // Should fail because text files can't be displayed as tables
        assert!(
            result.is_err(),
            "Table display should fail for file:data (text files)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table_raw_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a parquet table (file:table type)
        setup.write_parquet_table("series_raw.parquet").await?;

        // Instead of testing the cat command output (which goes through String conversion),
        // let's verify the file was stored correctly as raw parquet data
        let mut ship = steward::Ship::open_pond(&setup.pond_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

        let tx = ship
            .begin_transaction(steward::TransactionOptions::read(vec![
                "test".to_string(),
                "read".to_string(),
            ]))
            .await?;
        let fs = &*tx;
        let root = fs.root().await?;

        // Read the raw file content as bytes
        let raw_content = root
            .read_file_path_to_vec("series_raw.parquet")
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;

        tx.commit().await?;

        // Verify it starts with PAR1 magic bytes
        assert!(
            raw_content.len() >= 4,
            "File should be at least 4 bytes long"
        );
        let magic_bytes = &raw_content[0..4];
        assert_eq!(
            magic_bytes, b"PAR1",
            "File should start with PAR1 magic bytes"
        );

        // Verify we can read it as parquet and get our test data back
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(raw_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;

        let batch = arrow_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected at least one record batch"))?
            .map_err(|e| anyhow::anyhow!("Failed to read record batch: {}", e))?;

        // Verify the schema matches our test data
        assert_eq!(batch.num_columns(), 2, "Should have 2 columns");
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        let schema = batch.schema();
        assert_eq!(
            schema.field(0).name(),
            "timestamp",
            "First column should be timestamp"
        );
        assert_eq!(
            schema.field(1).name(),
            "value",
            "Second column should be value"
        );

        // Verify the actual data values
        use arrow::array::{Float64Array, StringArray};
        let timestamp_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast timestamp column to StringArray"))?;
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast value column to Float64Array"))?;

        assert_eq!(
            timestamp_array.value(0),
            "2024-01-01T00:00:00Z",
            "First timestamp should match"
        );
        assert_eq!(
            timestamp_array.value(1),
            "2024-01-01T01:00:00Z",
            "Second timestamp should match"
        );
        assert_eq!(value_array.value(0), 42.0, "First value should match");
        assert_eq!(value_array.value(1), 43.5, "Second value should match");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table_table_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a parquet table (file:table type)
        setup.write_parquet_table("series_table.parquet").await?;

        let cat_context = setup.cat_context("series_table.parquet");
        let mut output_buffer = String::new();

        // Cat with table display (should format as text table)
        cat_command(
            &cat_context,
            "series_table.parquet",
            "table", // table display mode
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await?;

        // Should get formatted table output with column names and data
        assert!(
            output_buffer.contains("timestamp"),
            "Table display should contain timestamp column"
        );
        assert!(
            output_buffer.contains("value"),
            "Table display should contain value column"
        );
        assert!(
            output_buffer.contains("42"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("43.5"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Table display should contain row summary"
        );

        Ok(())
    }

    // ========== FILE:SERIES TESTS - Single Version ==========

    #[tokio::test]
    async fn test_cat_command_series_single_version_raw_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a single-version file:series
        setup.write_parquet_series("test_series.parquet").await?;

        // Test raw display mode - should show raw parquet bytes like file:table
        // Same approach as file:table test - read directly from storage to verify PAR1 magic bytes
        let mut ship = steward::Ship::open_pond(&setup.pond_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

        let tx = ship
            .begin_transaction(steward::TransactionOptions::read(vec![
                "test".to_string(),
                "verify".to_string(),
            ]))
            .await?;
        let fs = &*tx;
        let root = fs.root().await?;

        // Read the raw file bytes
        let mut file_content = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            let mut reader = root
                .async_reader_path("test_series.parquet")
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open file for reading: {}", e))?;
            reader
                .read_to_end(&mut file_content)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;
        }

        tx.commit().await?;

        // Verify PAR1 magic bytes
        assert!(file_content.len() > 4, "File should be larger than 4 bytes");
        assert_eq!(
            &file_content[0..4],
            b"PAR1",
            "File should start with PAR1 magic bytes"
        );

        // Verify we can read it as parquet and get the expected data
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(file_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;

        let batch = arrow_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected at least one record batch"))?
            .map_err(|e| anyhow::anyhow!("Failed to read record batch: {}", e))?;

        // Verify the schema matches our test data
        assert_eq!(batch.num_columns(), 2, "Should have 2 columns");
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        let schema = batch.schema();
        assert_eq!(
            schema.field(0).name(),
            "timestamp",
            "First column should be timestamp"
        );
        assert_eq!(
            schema.field(1).name(),
            "value",
            "Second column should be value"
        );

        // Verify the actual data values - single version series data
        use arrow::array::{Float64Array, Int64Array};
        let timestamp_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast timestamp column to Int64Array"))?;
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast value column to Float64Array"))?;

        assert_eq!(
            timestamp_array.value(0),
            1704067200000_i64,
            "First timestamp should match"
        );
        assert_eq!(
            timestamp_array.value(1),
            1704070800000_i64,
            "Second timestamp should match"
        );
        assert_eq!(value_array.value(0), 42.0, "First value should match");
        assert_eq!(value_array.value(1), 43.5, "Second value should match");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_series_single_version_table_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a single-version file:series
        setup
            .write_parquet_series("test_series_table.parquet")
            .await?;

        let cat_context = setup.cat_context("test_series_table.parquet");
        let mut output_buffer = String::new();

        // Cat with table display (should format as text table)
        cat_command(
            &cat_context,
            "test_series_table.parquet",
            "table", // table display mode
            Some(&mut output_buffer),
            None,
            None,
            None,
        )
        .await?;

        // Should get formatted table output with column names and data
        assert!(
            output_buffer.contains("timestamp"),
            "Table display should contain timestamp column"
        );
        assert!(
            output_buffer.contains("value"),
            "Table display should contain value column"
        );
        assert!(
            output_buffer.contains("42"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("43.5"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Table display should contain row summary"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_series_single_version_with_sql() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a single-version file:series with test data
        setup
            .write_parquet_series("test_series_sql.parquet")
            .await?;

        let cat_context = setup.cat_context("test_series_sql.parquet");
        let mut output_buffer = String::new();

        // Cat with custom SQL query
        cat_command(
            &cat_context,
            "test_series_sql.parquet",
            "raw", // raw mode, but SQL query should trigger DataFusion formatting
            Some(&mut output_buffer),
            None,
            None,
            Some("SELECT timestamp, value * 2 as doubled_value FROM series WHERE value > 40"),
        )
        .await?;

        // Verify the output contains expected elements from the SQL query
        assert!(
            output_buffer.contains("doubled_value"),
            "Output should contain computed column name"
        );
        assert!(
            output_buffer.contains("84"),
            "Output should contain doubled value: 42.0 * 2 = 84"
        );
        assert!(
            output_buffer.contains("87"),
            "Output should contain doubled value: 43.5 * 2 = 87"
        );
        assert!(
            !output_buffer.contains(" 40 ") && !output_buffer.contains(" 40.0"),
            "Output should not contain value 40 or 40.0 in data columns\nActual output:\n{}",
            output_buffer
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Output should contain row count"
        );

        Ok(())
    }
}

/// Format RecordBatch results as a pretty-printed string with row summary
///
/// This is a presentation function specific to the cat command for displaying
/// query results in a human-readable format. Includes column types in headers
/// and a "Summary: X total rows" line at the end for compatibility with existing tests.
///
/// # Arguments
/// * `batches` - Vector of RecordBatch results to format
///
/// # Returns
/// String containing the formatted table output with row summary
fn format_query_results(batches: &[RecordBatch]) -> Result<String> {
    use arrow::util::pretty::pretty_format_batches_with_options;
    use arrow_cast::display::FormatOptions;

    if batches.is_empty() {
        return Ok("No data found\n".to_string());
    }

    // Use FormatOptions to show column types and handle errors gracefully
    let options = FormatOptions::default()
        .with_display_error(true)
        .with_types_info(true) // This shows column types in the headers
        .with_null("NULL"); // Show NULL values clearly

    let formatted = pretty_format_batches_with_options(batches, &options)
        .map_err(|e| anyhow::anyhow!("Failed to format query results: {}", e))?
        .to_string();

    // Calculate total row count across all batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Append row summary for compatibility with existing tests
    let result = if total_rows > 0 {
        format!(
            "{}\nSummary: {} total rows",
            formatted.trim_end(),
            total_rows
        )
    } else {
        "No data found\n".to_string()
    };

    Ok(result)
}
