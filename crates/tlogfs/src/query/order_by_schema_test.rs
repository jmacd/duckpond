use crate::persistence::OpLogPersistence;
use crate::query::{MetadataTable, UnifiedTableProvider};
use arrow_array::record_batch;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::datasource::TableProvider;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tinyfs::{EntryType, Lookup};
use tokio::io::AsyncWriteExt;
use diagnostics::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Test that demonstrates ORDER BY schema harmonization issue
/// 
/// This test creates a file:series with 3 versions having different schemas:
/// - Version 1: timestamp, value, name (3 columns)
/// - Version 2: timestamp, value, name, extra_field (4 columns) 
/// - Version 3: timestamp, value (2 columns)
/// 
/// Without schema harmonization, ORDER BY queries fail with "index out of bounds"
/// because DataFusion's SortPreservingMergeStream expects identical schemas.
/// With the fix, all batches are harmonized to the unified schema before DataFusion processing.
#[tokio::test]
async fn test_order_by_schema_harmonization() -> TestResult<()> {
    log_info!("Starting ORDER BY schema harmonization test");
    
    // Set up test environment
    let temp_dir = tempfile::TempDir::new()?;
    let data_path = temp_dir.path().join("order_by_test_pond");
    let data_path_str = data_path.to_string_lossy().to_string();
    
    log_info!("Creating test pond", pond_path: &data_path_str);
    
    let mut persistence = OpLogPersistence::create(&data_path_str).await?;
    
    // Create the series file path
    let series_path = "/test/order_by_test.series";
    
    log_info!("Creating series", series_path: series_path);
    
    // Create Version 1 data: 3 columns (timestamp, value, name)
    let version_1_batch = record_batch!(
        ("timestamp", Int64, [1000, 1100, 1200]),
        ("value", Float64, [10.0, 11.0, 12.0]),
        ("name", Utf8, ["first_a", "first_b", "first_c"])
    )?;
    
    // Create Version 2 data: 4 columns (timestamp, value, name, extra_field)
    let version_2_batch = record_batch!(
        ("timestamp", Int64, [2000, 2100, 2200]),
        ("value", Float64, [20.0, 21.0, 22.0]),
        ("name", Utf8, ["second_a", "second_b", "second_c"]),
        ("extra_field", Int64, [100, 101, 102])
    )?;
    
    // Create Version 3 data: 2 columns (timestamp, value) - smallest schema
    let version_3_batch = record_batch!(
        ("timestamp", Int64, [3000, 3100, 3200]),
        ("value", Float64, [30.0, 31.0, 32.0])
    )?;
    
    // Convert each batch to Parquet bytes  
    let version_1_bytes = batch_to_parquet_bytes(&version_1_batch)?;
    let version_2_bytes = batch_to_parquet_bytes(&version_2_batch)?;
    let version_3_bytes = batch_to_parquet_bytes(&version_3_batch)?;
    
    // Store version 1 (initial file creation)
    {
        let tx = persistence.begin().await?;
        let root = tx.root().await?;
        
        // Create test directory
        if !root.exists(std::path::Path::new("test")).await {
            root.create_dir_path("test").await?;
        }
        
        let (_path, mut writer) = root.create_file_path_streaming_with_type(&series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&version_1_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        tx.commit(None).await?;
    }
    log_info!("Stored version 1 data", columns: 3);
    
    // Store version 2 (append to existing series)
    {
        let tx = persistence.begin().await?;
        let root = tx.root().await?;
        
        let mut writer = root.async_writer_path_with_type(&series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&version_2_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        tx.commit(None).await?;
    }
    log_info!("Stored version 2 data", columns: 4);
    
    // Store version 3 (append to existing series)
    {
        let tx = persistence.begin().await?;
        let root = tx.root().await?;
        
        let mut writer = root.async_writer_path_with_type(&series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&version_3_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        tx.commit(None).await?;
    }
    log_info!("Stored version 3 data", columns: 2);
    
    log_info!("All versions stored successfully");
    
    // Now test the ORDER BY query that would previously fail
    let mut query_persistence = OpLogPersistence::open(&data_path.to_string_lossy()).await?;
    let metadata_table = MetadataTable::new(query_persistence.table().clone());
    
    // Get a fresh transaction for querying
    let query_tx = query_persistence.begin().await?;
    let query_root = query_tx.root().await?;
    
    // Get the node_id for the series
    let node_id = query_root.in_path(std::path::Path::new(&series_path[1..]), |_wd, lookup_result| async move {
        match lookup_result {
            Lookup::Found(node_path) => {
                Ok(node_path.id().await.to_string())
            }
            _ => Err(tinyfs::Error::not_found(&series_path[1..]))
        }
    }).await?;
    
    log_info!("Found series node_id", node_id: &node_id);
    
    // Create UnifiedTableProvider - this is where schema harmonization happens
    let mut provider = UnifiedTableProvider::create_series_table_with_tinyfs_and_node_id(
        series_path.to_string(),
        node_id,
        metadata_table,
        Arc::new(query_root),
    );
    
    // Load schema - this should create the unified schema from all versions
    provider.load_schema_from_data().await?;
    
    // Get the schema that was loaded
    let schema = provider.schema();
    let field_count = schema.fields().len();
    log_info!("Loaded unified schema", field_count: field_count);
    
    for (i, field) in schema.fields().iter().enumerate() {
        let data_type_str = format!("{}", field.data_type());
        log_debug!("Schema field", index: i, name: field.name(), data_type: &data_type_str);
    }
    
    // Now execute the ORDER BY query that would previously cause "index out of bounds"
    let ctx = SessionContext::new();
    ctx.register_table("test_data", Arc::new(provider))?;
    
    log_info!("Executing ORDER BY query...");
    
    // This query should trigger the schema harmonization during sort operations
    let df = ctx.sql("SELECT timestamp, value FROM test_data ORDER BY timestamp").await?;
    let batches = df.collect().await?;
    
    log_info!("Query executed successfully!");
    
    // Verify the results
    let mut all_timestamps = Vec::new();
    let mut all_values = Vec::new();
    
    for batch in &batches {
        let timestamp_array = batch.column_by_name("timestamp").unwrap()
            .as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        let value_array = batch.column_by_name("value").unwrap()
            .as_any().downcast_ref::<arrow_array::Float64Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            all_timestamps.push(timestamp_array.value(i));
            all_values.push(value_array.value(i));
        }
    }
    
    let total_rows = all_timestamps.len();
    log_info!("Retrieved total rows", total_rows: total_rows);
    
    // Verify the data is correctly sorted by timestamp
    for i in 1..all_timestamps.len() {
        if all_timestamps[i] < all_timestamps[i-1] {
            return Err(format!("Data not properly sorted: timestamp {} comes after {}", 
                all_timestamps[i], all_timestamps[i-1]).into());
        }
    }
    
    // Verify we got all expected data points
    let expected_timestamps = vec![1000, 1100, 1200, 2000, 2100, 2200, 3000, 3100, 3200];
    let expected_values = vec![10.0, 11.0, 12.0, 20.0, 21.0, 22.0, 30.0, 31.0, 32.0];
    
    if all_timestamps != expected_timestamps {
        return Err(format!("Unexpected timestamps: got {:?}, expected {:?}", 
            all_timestamps, expected_timestamps).into());
    }
    
    if all_values != expected_values {
        return Err(format!("Unexpected values: got {:?}, expected {:?}", 
            all_values, expected_values).into());
    }
    
    // Commit the query transaction
    query_tx.commit(None).await?;
    
    let total_rows = all_timestamps.len();
    log_info!("ORDER BY schema harmonization test passed!", 
              total_rows: total_rows,
              message: "Successfully queried rows from 3 schema versions with proper sorting");
    
    Ok(())
}

/// Convert a RecordBatch to Parquet bytes
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

/// Test that demonstrates the specific bug that was fixed
/// 
/// This test documents the exact issue: ORDER BY queries on file:series with evolving schemas
/// would fail with "index out of bounds" because DataFusion's SortPreservingMergeStream 
/// expected all input batches to have identical schemas.
/// 
/// The fix ensures universal schema harmonization occurs BEFORE any DataFusion processing.
#[tokio::test]
async fn test_order_by_index_out_of_bounds_bug_fixed() -> TestResult<()> {
    log_info!("Testing the specific 'index out of bounds' bug that was fixed");
    
    /* 
    OLD CODE BEHAVIOR (would cause the bug):
    
    In UnifiedExecutionPlan::execute(), each Parquet file would be processed with its own schema:
    - Version 1 batch: 3 columns (timestamp, value, name) 
    - Version 2 batch: 4 columns (timestamp, value, name, extra_field)
    - Version 3 batch: 2 columns (timestamp, value)
    
    When DataFusion's ORDER BY created a SortPreservingMergeStream, it expected all input 
    streams to have the same schema. But the old code applied projections individually:
    
    ```rust
    let projected_batch = if let Some(ref proj) = projection {
        let batch_column_count = batch.num_columns();
        let valid_projection: Vec<usize> = proj.iter()
            .copied()
            .filter(|&idx| idx < batch_column_count)
            .collect();
        // ... apply projection to individual batch
    }
    ```
    
    This meant each batch kept its native schema through to DataFusion's sort operations,
    causing "the len is X but the index is X" errors when the sort stream tried to access
    columns that existed in the unified schema but not in individual batch schemas.
    */
    
    // NEW CODE BEHAVIOR (with the fix):
    // All batches are harmonized to the unified schema BEFORE any DataFusion processing
    
    // This test uses the same logic as test_order_by_schema_harmonization but focuses
    // on documenting the specific bug that was fixed
    
    // The key fix was in UnifiedExecutionPlan::execute() where we now do:
    // 1. FIRST: Harmonize every batch to the unified schema 
    // 2. THEN: Apply projections and other operations
    // This ensures DataFusion's SortPreservingMergeStream gets identical schemas
    
    log_info!("✅ ORDER BY 'index out of bounds' bug is documented!");
    log_info!("The fix ensures schema harmonization prevents the bug from occurring");
    
    // Note: The actual test logic is in test_order_by_schema_harmonization()
    // This test serves as documentation of the fix
    
    Ok(())
}
