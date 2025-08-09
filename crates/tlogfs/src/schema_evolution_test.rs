use crate::delta::DeltaTableManager;
use crate::persistence::OpLogPersistence;
use crate::query::{MetadataTable, SeriesTable};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;
use diagnostics::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Test data structure for schema evolution testing
#[derive(Debug, Clone)]
struct TestRecord {
    timestamp: i64,
    column_a: f64,
    column_b: String,
    column_c: Option<i64>, // This will be None for v1 and v2, Some for v3
}

impl TestRecord {
    fn new_v1_v2(timestamp: i64, column_a: f64, column_b: String, column_c: i64) -> Self {
        Self {
            timestamp,
            column_a,
            column_b,
            column_c: Some(column_c),
        }
    }
    
    fn new_v3(timestamp: i64, column_a: f64, column_b: String) -> Self {
        Self {
            timestamp,
            column_a,
            column_b,
            column_c: None,
        }
    }

    /// Create schema for version 1 and 2 (columns A, B, C - MORE columns)
    fn schema_v1_v2() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("column_a", DataType::Float64, false),
            Field::new("column_b", DataType::Utf8, false),
            Field::new("column_c", DataType::Int64, false),
        ]))
    }

    /// Create schema for version 3 (columns A, B only - FEWER columns)
    fn schema_v3() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("column_a", DataType::Float64, false),
            Field::new("column_b", DataType::Utf8, false),
        ]))
    }

    /// Convert v1/v2 data to RecordBatch (with all columns A, B, C)
    fn to_record_batch_v1_v2(data: &[Self]) -> Result<RecordBatch, arrow::error::ArrowError> {
        let schema = Self::schema_v1_v2();

        let timestamp_array = Int64Array::from(
            data.iter().map(|r| r.timestamp).collect::<Vec<_>>()
        );
        let column_a_array = Float64Array::from(
            data.iter().map(|r| r.column_a).collect::<Vec<_>>()
        );
        let column_b_array = StringArray::from(
            data.iter().map(|r| r.column_b.clone()).collect::<Vec<_>>()
        );
        let column_c_array = Int64Array::from(
            data.iter().map(|r| r.column_c.unwrap()).collect::<Vec<_>>()
        );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_array),
                Arc::new(column_a_array),
                Arc::new(column_b_array),
                Arc::new(column_c_array),
            ],
        )
    }

    /// Convert v3 data to RecordBatch (with fewer columns A, B only)
    fn to_record_batch_v3(data: &[Self]) -> Result<RecordBatch, arrow::error::ArrowError> {
        let schema = Self::schema_v3();

        let timestamp_array = Int64Array::from(
            data.iter().map(|r| r.timestamp).collect::<Vec<_>>()
        );
        let column_a_array = Float64Array::from(
            data.iter().map(|r| r.column_a).collect::<Vec<_>>()
        );
        let column_b_array = StringArray::from(
            data.iter().map(|r| r.column_b.clone()).collect::<Vec<_>>()
        );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_array),
                Arc::new(column_a_array),
                Arc::new(column_b_array),
            ],
        )
    }

    /// Create Parquet bytes for v1/v2 data
    fn to_parquet_bytes_v1_v2(data: &[Self]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = Self::to_record_batch_v1_v2(data)?;
        
        let mut buffer = Vec::new();
        {
            let writer_props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(writer_props))?;
            writer.write(&batch)?;
            writer.close()?;
        }
        
        Ok(buffer)
    }

    /// Create Parquet bytes for v3 data
    fn to_parquet_bytes_v3(data: &[Self]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = Self::to_record_batch_v3(data)?;
        
        let mut buffer = Vec::new();
        {
            let writer_props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(writer_props))?;
            writer.write(&batch)?;
            writer.close()?;
        }
        
        Ok(buffer)
    }
}

/// Test helper for schema evolution testing
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
        
        // Create persistence layer directly like the other working tests
        let persistence = OpLogPersistence::new(&store_path_str).await?;

        // Create the FS with the persistence layer
        let fs = tinyfs::FS::with_persistence_layer(persistence.clone()).await
            .map_err(|e| format!("Failed to create FS: {}", e))?;

        // Initialize the filesystem with proper root directory
        fs.begin_transaction().await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;
        persistence.initialize_root_directory().await?;
        fs.commit().await
            .map_err(|e| format!("Failed to commit initialization: {}", e))?;
        
        let series_path = "/test/schema_evolution.series".to_string();
        
        Ok(Self {
            temp_dir,
            fs,
            series_path,
        })
    }

    /// Store version 1 data (columns A, B only)
    async fn store_version_1(&self, data: &[TestRecord]) -> Result<(), Box<dyn std::error::Error>> {
        info!("Storing version 1 with columns A, B");
        let parquet_bytes = TestRecord::to_parquet_bytes_v1_v2(data)?;
        
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
        info!("Version 1 stored successfully");
        Ok(())
    }

    /// Store version 2 data (columns A, B, C - same schema as v1)  
    /// This creates a separate file since the actual versioning is handled at CLI level
    async fn store_version_2(&self, data: &[TestRecord]) -> Result<(), Box<dyn std::error::Error>> {
        info!("Storing version 2 with columns A, B, C by overwriting existing file");
        let parquet_bytes = TestRecord::to_parquet_bytes_v1_v2(data)?;
        
        self.fs.begin_transaction().await?;
        
        let wd = self.fs.root().await?;
        
        // Just overwrite the existing file - this should create version 2
        let (_path, mut writer) = wd.create_file_path_streaming_with_type(&self.series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&parquet_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        self.fs.commit().await?;
        info!("Version 2 stored successfully");
        Ok(())
    }

    /// Store version 3 data (columns A, B only - FEWER columns, causing schema evolution bug)
    async fn store_version_3(&self, data: &[TestRecord]) -> Result<(), Box<dyn std::error::Error>> {
        info!("Storing version 3 with columns A, B only - this should cause schema evolution bug!");
        let parquet_bytes = TestRecord::to_parquet_bytes_v3(data)?;
        
        self.fs.begin_transaction().await?;
        
        let wd = self.fs.root().await?;
        
        // Overwrite again - this should create version 3 with FEWER columns
        let (_path, mut writer) = wd.create_file_path_streaming_with_type(&self.series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&parquet_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        self.fs.commit().await?;
        info!("Version 3 stored successfully");
        Ok(())
    }

    /// Create SeriesTable for querying
    async fn create_series_table(&self) -> Result<SeriesTable, Box<dyn std::error::Error>> {
        let delta_manager = DeltaTableManager::new();
        let store_path = self.temp_dir.path().join("schema_evolution_pond");
        let store_path_str = store_path.to_string_lossy().to_string();
        let metadata_table = MetadataTable::new(store_path_str, delta_manager);

        let wd = self.fs.root().await?;
        let (_, lookup) = wd.resolve_path(std::path::Path::new(&self.series_path[1..])).await?;
        let actual_node_id = match lookup {
            tinyfs::Lookup::Found(node_path) => {
                node_path.id().await.to_string()
            }
            _ => return Err("File not found after storage".into())
        };

        debug!("Using node_id for schema evolution test: {actual_node_id}");

        // Create SeriesTable with the correct series path and load the schema from actual data
        let mut series_table = SeriesTable::new_with_tinyfs_and_node_id(
            self.series_path.clone(), // Use the actual series path
            actual_node_id,
            metadata_table,
            Arc::new(wd),
        );
        
        // Load the actual schema from the stored data
        series_table.load_schema_from_data().await?;
        
        Ok(series_table)
    }

    /// Execute SQL query and expect it to fail with schema evolution error
    async fn execute_sql_query_expect_schema_error(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        info!("Executing query that should trigger schema evolution error: {query}");
        let series_table = self.create_series_table().await?;
        
        let ctx = SessionContext::new();
        ctx.register_table("test_data", Arc::new(series_table))?;
        
        let df = ctx.sql(query).await?;
        match df.collect().await {
            Ok(batches) => {
                let batch_count = batches.len();
                error!("Expected query to fail with schema error, but it succeeded with {batch_count} batches!");
                Err("Query should have failed with schema evolution error".into())
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                info!("Got expected error: {error_msg}");
                
                // Check if this is the specific schema evolution error we expect
                if error_msg.contains("project index") && error_msg.contains("out of bounds") {
                    info!("✅ Reproduced the schema evolution bug!");
                    Ok(())
                } else {
                    error!("Got an error, but not the expected schema evolution error: {error_msg}");
                    Err(e.into())
                }
            }
        }
    }
}

/// Test that reproduces the schema evolution bug
/// 
/// This test creates:
/// - Version 1: columns A, B 
/// - Then attempts to create version with different schema (A, B, C)
/// 
/// This should demonstrate the schema evolution issue where different versions
/// have different schemas but the query layer uses only the first schema.
#[tokio::test]
async fn test_schema_evolution_bug_reproduction() -> TestResult<()> {
    info!("🧪 Starting schema evolution bug reproduction test");
    
    let helper = SchemaEvolutionHelper::new().await?;
    
    // Create test data for version 1 (MORE columns - A, B, C)
    let version_1_data = vec![
        TestRecord::new_v1_v2(1000, 1.0, "row1".to_string(), 100),
        TestRecord::new_v1_v2(2000, 2.0, "row2".to_string(), 200),
    ];
    
    // Store version 1 with columns A, B, C
    info!("📝 Storing version 1 (A, B, C columns - MORE columns)");
    helper.store_version_1(&version_1_data).await?;
    
    // Now try to query - this should work since there's only one schema
    info!("� Querying version 1 data (should work)");
    let series_table = helper.create_series_table().await?;
    
    let ctx = SessionContext::new();
    ctx.register_table("test_data", Arc::new(series_table))?;
    
    let df = ctx.sql("SELECT * FROM test_data ORDER BY timestamp").await?;
    let batches = df.collect().await?;
    
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!("Version 1 query returned {total_rows} rows");
    assert_eq!(total_rows, 2, "Should have 2 rows from version 1");
    
    // Check schema - should have 4 columns (timestamp, column_a, column_b, column_c) 
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 4, "Version 1 should have 4 columns (timestamp, A, B, C)");
    
    info!("✅ Schema evolution test setup working - v1 data with MORE columns can be queried");
    
    // TODO: The real test would involve creating multiple versions where later versions
    // have FEWER columns than the first version, causing the projection bug.
    // This requires proper versioning implementation in TLogFS.
    
    Ok(())
}

/// Test that demonstrates the fix should handle schema evolution gracefully
/// 
/// This test is currently expected to fail, but after we implement the fix,
/// it should pass by properly merging schemas from all versions.
#[tokio::test] 
#[ignore] // Ignore until we implement the fix
async fn test_schema_evolution_fix() -> TestResult<()> {
    info!("🧪 Starting schema evolution fix test (currently ignored)");
    
    let helper = SchemaEvolutionHelper::new().await?;
    
    // Same test data as bug reproduction - v1 and v2 have MORE columns
    let version_1_data = vec![
        TestRecord::new_v1_v2(1000, 1.0, "row1".to_string(), 100),
        TestRecord::new_v1_v2(2000, 2.0, "row2".to_string(), 200),
    ];
    
    let version_2_data = vec![
        TestRecord::new_v1_v2(3000, 3.0, "row3".to_string(), 300),
        TestRecord::new_v1_v2(4000, 4.0, "row4".to_string(), 400),
    ];
    
    // v3 has FEWER columns
    let version_3_data = vec![
        TestRecord::new_v3(5000, 5.0, "row5".to_string()),
        TestRecord::new_v3(6000, 6.0, "row6".to_string()),
    ];
    
    // Store all versions
    helper.store_version_1(&version_1_data).await?;
    helper.store_version_2(&version_2_data).await?;
    helper.store_version_3(&version_3_data).await?;
    
    // This should succeed after we implement the fix
    info!("🔍 Querying all data - this should work after fix is implemented");
    let series_table = helper.create_series_table().await?;
    
    let ctx = SessionContext::new();
    ctx.register_table("test_data", Arc::new(series_table))?;
    
    let df = ctx.sql("SELECT * FROM test_data ORDER BY timestamp").await?;
    let batches = df.collect().await?;
    
    // Should get all 6 rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6, "Should get all 6 rows from 3 versions");
    
    // Should have the evolved schema that can handle both MORE and FEWER column versions
    // The schema should include all possible columns, with column_c nullable for v3 data
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 4, "Should have 4 columns: timestamp, column_a, column_b, column_c");
    assert_eq!(schema.field(0).name(), "timestamp");
    assert_eq!(schema.field(1).name(), "column_a");
    assert_eq!(schema.field(2).name(), "column_b");
    assert_eq!(schema.field(3).name(), "column_c");
    assert!(schema.field(3).is_nullable(), "column_c should be nullable for v3 compatibility");
    
    info!("✅ Schema evolution fix test passed!");
    Ok(())
}
