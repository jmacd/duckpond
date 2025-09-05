// DISABLED: This test was designed for the old UnifiedTableProvider architecture
// which has been removed in favor of the new FileTable architecture.
//
// This test exposed a critical "index out of bounds" bug in the UnifiedTableProvider
// ORDER BY schema harmonization logic. The test may still be valuable for:
// 1. Understanding the schema harmonization bug pattern
// 2. Ensuring the new FileTable architecture handles this case correctly
// 3. Reference for future ORDER BY testing
//
// TODO: Reimplement this test for the new FileTable/FileTableProvider architecture
// when ORDER BY functionality is added to that system.

#[allow(dead_code)]
mod disabled_unified_table_provider_test {
    use crate::persistence::OpLogPersistence;
    use crate::query::MetadataTable; // Still need MetadataTable
    // use crate::query::{MetadataTable, UnifiedTableProvider}; // REMOVED: UnifiedTableProvider no longer exists
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

/// DISABLED TEST: This test reproduced the ORDER BY schema harmonization "index out of bounds" panic
/// in the old UnifiedTableProvider architecture. 
/// 
/// This test creates conditions that trigger the exact error:
/// "the len is 10 but the index is 10"
/// 
/// The key is creating a file:series where:
/// 1. The unified schema has N columns (creating projection indices 0..N-1) 
/// 2. Some batches only have N-1 columns (only indices 0..N-2 exist)
/// 3. ORDER BY forces DataFusion to access all projection indices on all batches
/// 4. Accessing index N-1 on a batch with only N-1 columns causes the panic
///
/// TODO: Reimplement this test for the new FileTable architecture
#[allow(dead_code)]
// #[tokio::test] // DISABLED: UnifiedTableProvider no longer exists
async fn test_order_by_schema_harmonization() -> TestResult<()> {
    log_info!("Starting ORDER BY schema harmonization test to reproduce index out of bounds panic");
    
    // Set up test environment
    let temp_dir = tempfile::TempDir::new()?;
    let data_path = temp_dir.path().join("order_by_test_pond");
    let data_path_str = data_path.to_string_lossy().to_string();
    
    log_info!("Creating test pond", pond_path: &data_path_str);
    
    let mut persistence = OpLogPersistence::create(&data_path_str).await?;
    
    // Create the series file path
    let series_path = "/test/order_by_test.series";
    
    log_info!("Creating series designed to trigger index out of bounds", series_path: series_path);
    
    // Create Version 1: 11 columns (this becomes the unified schema)
    // This creates projection indices 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    let version_1_batch = record_batch!(
        ("timestamp", Int64, [1000, 1100, 1200]),
        ("value", Float64, [10.0, 11.0, 12.0]),
        ("name", Utf8, ["first_a", "first_b", "first_c"]),
        ("col3", Int64, [100, 101, 102]),
        ("col4", Float64, [20.0, 21.0, 22.0]),
        ("col5", Utf8, ["data_a", "data_b", "data_c"]),
        ("col6", Int64, [200, 201, 202]),
        ("col7", Float64, [30.0, 31.0, 32.0]),
        ("col8", Utf8, ["info_a", "info_b", "info_c"]),
        ("col9", Int64, [300, 301, 302]),
        ("col10", Float64, [40.0, 41.0, 42.0])
    )?;
    
    // Create Version 2: 10 columns only (missing col10)
    // This batch only has indices 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    // When DataFusion tries to access index 10, it will get "the len is 10 but the index is 10"
    let version_2_batch = record_batch!(
        ("timestamp", Int64, [2000, 2100, 2200]),
        ("value", Float64, [20.0, 21.0, 22.0]),
        ("name", Utf8, ["second_a", "second_b", "second_c"]),
        ("col3", Int64, [110, 111, 112]),
        ("col4", Float64, [25.0, 26.0, 27.0]),
        ("col5", Utf8, ["data2_a", "data2_b", "data2_c"]),
        ("col6", Int64, [210, 211, 212]),
        ("col7", Float64, [35.0, 36.0, 37.0]),
        ("col8", Utf8, ["info2_a", "info2_b", "info2_c"]),
        ("col9", Int64, [310, 311, 312])
        // NOTE: col10 is missing! This batch has length 10, but unified schema expects index 10
    )?;
    
    // Create Version 3: back to 11 columns (same as unified schema)
    let version_3_batch = record_batch!(
        ("timestamp", Int64, [3000, 3100, 3200]),
        ("value", Float64, [30.0, 31.0, 32.0]),
        ("name", Utf8, ["third_a", "third_b", "third_c"]),
        ("col3", Int64, [120, 121, 122]),
        ("col4", Float64, [35.0, 36.0, 37.0]),
        ("col5", Utf8, ["data3_a", "data3_b", "data3_c"]),
        ("col6", Int64, [220, 221, 222]),
        ("col7", Float64, [45.0, 46.0, 47.0]),
        ("col8", Utf8, ["info3_a", "info3_b", "info3_c"]),
        ("col9", Int64, [320, 321, 322]),
        ("col10", Float64, [50.0, 51.0, 52.0])
    )?;
    
    // Convert each batch to Parquet bytes  
    let version_1_bytes = batch_to_parquet_bytes(&version_1_batch)?;
    let version_2_bytes = batch_to_parquet_bytes(&version_2_batch)?;
    let version_3_bytes = batch_to_parquet_bytes(&version_3_batch)?;
    
    // Store version 1 (initial file creation) - 11 columns
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
    log_info!("Stored version 1 data", columns: 11, message: "This creates the unified schema with indices 0-10");
    
    // Store version 2 (append to existing series) - 10 columns only!
    {
        let tx = persistence.begin().await?;
        let root = tx.root().await?;
        
        let mut writer = root.async_writer_path_with_type(&series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&version_2_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        tx.commit(None).await?;
    }
    log_info!("Stored version 2 data", columns: 10, message: "CRITICAL: This batch only has indices 0-9, but unified schema expects 0-10");
    
    // Store version 3 (append to existing series) - back to 11 columns
    {
        let tx = persistence.begin().await?;
        let root = tx.root().await?;
        
        let mut writer = root.async_writer_path_with_type(&series_path[1..], EntryType::FileSeries).await?;
        writer.write_all(&version_3_bytes).await?;
        writer.flush().await?;
        writer.shutdown().await?;
        
        tx.commit(None).await?;
    }
    log_info!("Stored version 3 data", columns: 11, message: "Back to full schema");
    
    log_info!("All versions stored - conditions set for 'the len is 10 but the index is 10' panic");
    
    // Now test the ORDER BY query that should trigger the index out of bounds panic
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
    
    // Create UnifiedTableProvider - this will create a unified schema with 11 columns
    let mut provider = UnifiedTableProvider::create_series_table_with_tinyfs_and_node_id(
        series_path.to_string(),
        node_id,
        metadata_table,
        Arc::new(query_root),
    );
    
    // Load schema - this creates the unified schema from all versions
    provider.load_schema_from_data().await?;
    
    // Get the schema that was loaded
    let schema = provider.schema();
    let field_count = schema.fields().len();
    log_info!("Loaded unified schema", field_count: field_count);
    
    if field_count != 11 {
        return Err(format!("Expected 11 columns in unified schema, got {}", field_count).into());
    }
    
    // Now execute the ORDER BY query that triggers the DataFusion sort operation
    let ctx = SessionContext::new();
    ctx.register_table("test_data", Arc::new(provider))?;
    
    log_info!("Executing ORDER BY query - this should trigger index out of bounds panic with old code");
    log_info!("DataFusion will try to access all 11 columns on version 2 batch that only has 10 columns");
    
    // This query forces DataFusion to create projections for all columns
    // The old code will try to project indices [0,1,2,3,4,5,6,7,8,9,10] on version 2 batch
    // But version 2 batch only has indices [0,1,2,3,4,5,6,7,8,9] (length 10)
    // Accessing index 10 gives "the len is 10 but the index is 10"
    let df = ctx.sql("SELECT timestamp, value, name, col3, col4, col5, col6, col7, col8, col9, col10 FROM test_data ORDER BY timestamp").await?;
    let batches = df.collect().await?;
    
    log_info!("Query executed successfully - schema harmonization prevented the panic!");
    
    // Verify results
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    log_info!("Retrieved total rows", total_rows: total_rows);
    
    if total_rows != 9 {
        return Err(format!("Expected 9 total rows (3 per version), got {}", total_rows).into());
    }
    
    // Verify all batches have 11 columns (due to harmonization)
    for (i, batch) in batches.iter().enumerate() {
        let batch_columns = batch.num_columns();
        if batch_columns != 11 {
            return Err(format!("Batch {} has {} columns, expected 11", i, batch_columns).into());
        }
    }
    
    // Commit the query transaction
    query_tx.commit(None).await?;
    
    log_info!("ORDER BY schema harmonization test passed!");
    log_info!("Successfully handled the 'len is 10 but index is 10' scenario");
    log_info!("All batches harmonized to unified schema before DataFusion processing");
    
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

} // End of disabled_unified_table_provider_test module
