//! Integration test: CSV streaming with provider crate
//!
//! This test demonstrates:
//! 1. Creating CSV files via InfiniteCsvFile test factory
//! 2. Using CsvProvider to stream CSV data into RecordBatches
//! 3. Querying the CSV data through DataFusion
//!
//! NOTE: Full CSV URL integration with timeseries_join (csv:///pattern)
//! requires Layer 3 (Provider API) to be implemented. That layer will:
//! - Parse csv:// URLs and apply format providers
//! - Register CSV tables automatically in DataFusion
//! - Support glob patterns like csv:///sensors/**.csv
//!
//! For now, this tests CSV streaming in isolation.

#[cfg(test)]
mod tests {
    use crate::factory::test_factory::{InfiniteCsvConfig, InfiniteCsvFile};
    use crate::{CsvProvider, FileProvider, FormatProvider, Url};
    use datafusion::execution::context::SessionContext;
    use futures::StreamExt;
    use std::sync::Arc;
    use tinyfs::{File, FS, MemoryPersistence};

    /// Helper to create test environment with MemoryPersistence
    async fn create_test_environment() -> FS {
        let persistence = MemoryPersistence::default();
        FS::new(persistence.clone())
            .await
            .expect("Failed to create FS")
    }

    /// Helper to create a CSV file from InfiniteCsvFile generator
    async fn create_csv_file(
        fs: &FS,
        path: &str,
        config: InfiniteCsvConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let root = fs.root().await?;

        // Create parent directories if needed
        if let Some(parent) = std::path::Path::new(path).parent() {
            if parent.to_str() != Some("/") && !parent.as_os_str().is_empty() {
                // Create each directory in the path
                let path_parts: Vec<_> = parent
                    .components()
                    .filter_map(|c| match c {
                        std::path::Component::Normal(name) => name.to_str(),
                        _ => None,
                    })
                    .collect();

                let mut current_path = String::from("/");
                for part in path_parts {
                    if !current_path.ends_with('/') {
                        current_path.push('/');
                    }
                    current_path.push_str(part);

                    // Try to create directory, ignore if already exists
                    let _ = root.create_dir_path(&current_path).await;
                }
            }
        }

        // Generate CSV data using InfiniteCsvFile
        let infinite_file = InfiniteCsvFile::new(config);
        let mut reader = infinite_file.async_reader().await?;

        // Read all data into memory (for test simplicity)
        use tokio::io::AsyncReadExt;
        let mut csv_data = Vec::new();
        let _ = reader.read_to_end(&mut csv_data).await?;

        // Write to TinyFS
        let mut file_writer = root
            .async_writer_path_with_type(path, tinyfs::EntryType::FileDataDynamic)
            .await?;
        use tokio::io::AsyncWriteExt;
        file_writer.write_all(&csv_data).await?;
        file_writer.flush().await?;
        file_writer.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_csv_streaming_from_tinyfs() {
        // Create test environment
        let fs = create_test_environment().await;

        // Create a CSV file with timeseries data (100 rows)
        let temp_config = InfiniteCsvConfig {
            row_count: 100,
            batch_size: 1024,
            columns: vec!["timestamp".to_string(), "temperature".to_string()],
        };
        create_csv_file(&fs, "/data/temp.csv", temp_config)
            .await
            .expect("Failed to create temp.csv");

        // Open the file using URL-based API
        let url = Url::parse("csv:///data/temp.csv?batch_size=50")
            .expect("Failed to parse URL");
        
        let reader = fs.open_url(&url).await.expect("Failed to open URL");

        // Use CsvProvider to stream the data
        let csv_provider = CsvProvider::new();
        let (schema, mut stream) = csv_provider
            .open_stream(reader, &url)
            .await
            .expect("Failed to open CSV stream");

        println!("CSV Schema: {:?}", schema);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "temperature");

        // Stream through all batches
        let mut total_rows = 0;
        let mut batch_count = 0;
        while let Some(result) = stream.next().await {
            let batch = result.expect("Failed to get batch");
            total_rows += batch.num_rows();
            batch_count += 1;
        }

        assert_eq!(total_rows, 100);
        println!("✅ Successfully streamed {} rows in {} batches from CSV file via URL", total_rows, batch_count);
    }

    #[tokio::test]
    async fn test_csv_in_datafusion() {
        // Test CSV data queried through DataFusion using CSV provider
        let fs = create_test_environment().await;

        // Create CSV file
        let config = InfiniteCsvConfig {
            row_count: 50,
            batch_size: 1024,
            columns: vec!["id".to_string(), "value".to_string(), "label".to_string()],
        };
        create_csv_file(&fs, "/data/metrics.csv", config)
            .await
            .expect("Failed to create metrics.csv");

        // Open with CSV provider
        let url = Url::parse("csv:///data/metrics.csv").expect("Failed to parse URL");
        let reader = fs.open_url(&url).await.expect("Failed to open URL");

        let csv_provider = CsvProvider::new();
        let (schema, stream) = csv_provider
            .open_stream(reader, &url)
            .await
            .expect("Failed to open CSV stream");

        // Create a simple MemTable from the streamed data
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect batches");

        // Register in DataFusion
        let ctx = SessionContext::new();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
            .expect("Failed to create MemTable");
        let _ = ctx.register_table("metrics", Arc::new(mem_table))
            .expect("Failed to register table");

        // Query the data
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM metrics")
            .await
            .expect("Failed to execute query");

        let results = df.collect().await.expect("Failed to collect results");
        assert!(!results.is_empty());

        println!("✅ Successfully queried CSV data through DataFusion");
        println!("✅ Query results: {:?}", results[0]);
    }
}
