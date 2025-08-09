use crate::delta::DeltaTableManager;
use crate::persistence::OpLogPersistence;
use crate::query::{MetadataTable};
use arrow_array::record_batch;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tinyfs::{EntryType, Lookup};
use tokio::io::AsyncWriteExt;
use diagnostics::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Test data structure for schema evolution testing
#[derive(Debug, Clone)]
struct TestRecord {
    timestamp: i64,
    column_a: f64,
    column_b: String,
    column_c: Option<i64>, // This will be None for v3, Some for v1/v2
}

impl TestRecord {
    fn new_v1(timestamp: i64, column_a: f64, column_b: String) -> Self {
        Self {
            timestamp,
            column_a,
            column_b,
            column_c: None, // v1 doesn't have column_c
        }
    }
    
    fn new_v2(timestamp: i64, column_a: f64, column_b: String, column_c: i64) -> Self {
        Self {
            timestamp,
            column_a,
            column_b,
            column_c: Some(column_c), // v2 has column_c
        }
    }
    
    fn new_v3(timestamp: i64, column_a: f64, column_b: String, column_c: i64) -> Self {
        Self {
            timestamp,
            column_a,
            column_b,
            column_c: Some(column_c), // v3 also has column_c
        }
    }

    /// Convert to v1 RecordBatch (largest schema - 12 fields)  
    fn to_record_batch_v1(data: &[Self]) -> Result<RecordBatch, arrow::error::ArrowError> {
        Ok(record_batch!(
            ("timestamp", Int64, [data[0].timestamp, data[1].timestamp]),
            ("column_a", Float64, [data[0].column_a, data[1].column_a]),
            ("column_b", Utf8, [data[0].column_b.as_str(), data[1].column_b.as_str()]),
            ("column_c", Int64, [data[0].column_c.unwrap_or(0), data[1].column_c.unwrap_or(0)]),
            ("extra_col1", Int64, [100, 200]),
            ("extra_col2", Float64, [1.1, 2.2]),
            ("extra_col3", Utf8, ["v1_1", "v1_2"]),
            ("extra_col4", Int64, [101, 102]),
            ("extra_col5", Float64, [1.3, 1.4]),
            ("extra_col6", Utf8, ["large1", "large2"]),
            ("extra_col7", Int64, [701, 702]),
            ("extra_col8", Float64, [7.1, 7.2])
        )?)
    }

    /// Convert to v2 RecordBatch (medium schema - 6 fields)
    fn to_record_batch_v2(data: &[Self]) -> Result<RecordBatch, arrow::error::ArrowError> {
        Ok(record_batch!(
            ("timestamp", Int64, [data[0].timestamp, data[1].timestamp]),
            ("column_a", Float64, [data[0].column_a, data[1].column_a]),
            ("column_b", Utf8, [data[0].column_b.as_str(), data[1].column_b.as_str()]),
            ("column_c", Int64, [data[0].column_c.unwrap_or(0), data[1].column_c.unwrap_or(0)]),
            ("extra_col1", Int64, [200, 300]),
            ("extra_col2", Float64, [2.1, 2.2])
        )?)
    }

    /// Convert to v3 RecordBatch (smallest schema - 3 fields)
    fn to_record_batch_v3(data: &[Self]) -> Result<RecordBatch, arrow::error::ArrowError> {
        Ok(record_batch!(
            ("timestamp", Int64, [data[0].timestamp, data[1].timestamp]),
            ("column_a", Float64, [data[0].column_a, data[1].column_a]),
            ("column_b", Utf8, [data[0].column_b.as_str(), data[1].column_b.as_str()])
        )?)
    }

    fn to_parquet_bytes_v1(data: &[Self]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = Self::to_record_batch_v1(data)?;
        Self::batch_to_parquet_bytes(&batch)
    }

    fn to_parquet_bytes_v2(data: &[Self]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = Self::to_record_batch_v2(data)?;
        Self::batch_to_parquet_bytes(&batch)
    }

    fn to_parquet_bytes_v3(data: &[Self]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = Self::to_record_batch_v3(data)?;
        Self::batch_to_parquet_bytes(&batch)
    }

    fn batch_to_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut buffer = Vec::new();
        {
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
        }
        Ok(buffer)
    }
}

/// Helper struct for schema evolution testing
struct SchemaEvolutionHelper {
    temp_dir: tempfile::TempDir,
    fs: tinyfs::FS,
    series_path: String,
}

impl SchemaEvolutionHelper {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = tempfile::TempDir::new()?;
        let store_path = temp_dir.path().join("schema_evolution_pond");
        let store_path_str = store_path.to_string_lossy().to_string();
        
        let persistence = OpLogPersistence::new(&store_path_str).await?;
        let fs = tinyfs::FS::with_persistence_layer(persistence.clone()).await?;

        fs.begin_transaction().await?;
        persistence.initialize_root_directory().await?;
        fs.commit().await?;
        
        let series_path = format!("/test/schema_evolution_{}.series", uuid7::uuid7());
        
        Ok(Self {
            temp_dir,
            fs,
            series_path,
        })
    }

    async fn store_version_1(&self, data: &[TestRecord]) -> Result<(), Box<dyn std::error::Error>> {
        info!("Storing version 1 with fewer columns (smaller schema)");
        let parquet_bytes = TestRecord::to_parquet_bytes_v1(data)?;
        
        self.fs.begin_transaction().await?;
        let wd = self.fs.root().await?;
        
        if !wd.exists(std::path::Path::new("test")).await {
            wd.create_dir_path("test").await?;
        }
        
        let (_path, mut writer) = wd.create_file_path_streaming_with_type(&self.series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&parquet_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        self.fs.commit().await?;
        Ok(())
    }

    async fn store_version_2(&self, data: &[TestRecord]) -> Result<(), Box<dyn std::error::Error>> {
        info!("Storing version 2 with medium schema");
        let parquet_bytes = TestRecord::to_parquet_bytes_v2(data)?;
        
        self.fs.begin_transaction().await?;
        let wd = self.fs.root().await?;
        
        // Get writer for existing FileSeries (append new version)
        let mut writer = wd.async_writer_path_with_type(&self.series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&parquet_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        self.fs.commit().await?;
        Ok(())
    }

    async fn store_version_3(&self, data: &[TestRecord]) -> Result<(), Box<dyn std::error::Error>> {
        info!("Storing version 3 with largest schema");
        let parquet_bytes = TestRecord::to_parquet_bytes_v3(data)?;
        
        self.fs.begin_transaction().await?;
        let wd = self.fs.root().await?;
        
        // Get writer for existing FileSeries (append new version)
        let mut writer = wd.async_writer_path_with_type(&self.series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&parquet_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        self.fs.commit().await?;
        Ok(())
    }
}

/// Test that reproduces the schema evolution bug
#[tokio::test]
async fn test_schema_evolution_bug_reproduction() -> TestResult<()> {
    info!("🧪 Starting schema evolution bug reproduction test");
    
    let helper = SchemaEvolutionHelper::new().await?;
    
    // Version 1: 12 columns (largest schema - the bug is that this schema is used for all queries)
    let version_1_data = vec![
        TestRecord::new_v1(1000, 1.0, "row1".to_string()),
        TestRecord::new_v1(2000, 2.0, "row2".to_string()),
    ];
    
    // Version 2: 6 columns (medium schema)
    let version_2_data = vec![
        TestRecord::new_v2(3000, 3.0, "row3".to_string(), 300),
        TestRecord::new_v2(4000, 4.0, "row4".to_string(), 400),
    ];
    
    // Version 3: 3 columns (smallest schema - this will cause out-of-bounds when projected with v1 schema)
    let version_3_data = vec![
        TestRecord::new_v3(5000, 5.0, "row5".to_string(), 500),
        TestRecord::new_v3(6000, 6.0, "row6".to_string(), 600),
    ];
    
    // Store version 1 first (largest schema - this will be used for all queries)
    helper.store_version_1(&version_1_data).await?;
    
    // Store version 2 second (medium schema)
    helper.store_version_2(&version_2_data).await?;
    
    // Store version 3 third (smallest schema - this should cause the bug)
    helper.store_version_3(&version_3_data).await?;
    
    // Query should fail due to schema evolution bug - use same code path as real cat command
    let data_path = helper.temp_dir.path().join("schema_evolution_pond");
    let data_path_str = data_path.to_string_lossy().to_string();
    let delta_manager = DeltaTableManager::new();
    let metadata_table = MetadataTable::new(data_path_str, delta_manager);

    let wd = helper.fs.root().await?;
    let series_path = helper.series_path.clone(); // Clone to avoid borrow issues
    let series_path_for_closure = series_path.clone(); // Additional clone for closure
    
    let actual_node_id = wd.in_path(std::path::Path::new(&series_path[1..]), |_wd, lookup_result| async move {
        match lookup_result {
            Lookup::Found(node_path) => {
                Ok(node_path.id().await.to_string())
            }
            _ => Err(tinyfs::Error::not_found(&series_path_for_closure[1..]))
        }
    }).await?;

    // Use the same UnifiedTableProvider creation as the real cat command
    let mut provider = crate::query::UnifiedTableProvider::create_series_table_with_tinyfs_and_node_id(
        series_path.clone(),
        actual_node_id,
        metadata_table,
        Arc::new(wd),
    );
    
    // This is where the schema evolution bug should occur - same as real cat command
    let schema_result = provider.load_schema_from_data().await;
    match schema_result {
        Ok(_) => {
            // Continue with DataFusion query like real cat command
            let ctx = SessionContext::new();
            ctx.register_table("test_data", Arc::new(provider))?;
            
            let result = ctx.sql("SELECT * FROM test_data ORDER BY timestamp").await;
            match result {
                Ok(df) => {
                    let collect_result = df.collect().await;
                    match collect_result {
                        Ok(batches) => {
                            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                            error!("Query succeeded with {total_rows} rows");
                            return Ok(())
                        }
                        Err(e) => {
                            return Err(format!("Unexpected schema evolution error, got: {e}").into());
                        }
                    }
                }
                Err(e) => {
                    error!("Got different error than expected: {e}");
                    return Err(format!("Expected schema evolution error, got: {e}").into());
                }
            }
        }
        Err(e) => {
            error!("Got different error than expected during schema loading: {e}");
            return Err(format!("Expected schema evolution error, got: {e}").into());
        }
    }
}
