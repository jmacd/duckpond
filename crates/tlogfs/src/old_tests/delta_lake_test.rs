use super::schema::{OplogEntry, ForArrow, encode_versioned_directory_entries};
use serde_arrow;
use chrono::Utc;
use tinyfs::NodeID;
use deltalake::{DeltaOps, protocol::SaveMode};
use tempfile::TempDir;

#[tokio::test]
async fn test_delta_lake_write_with_oplog_entry() {
    println!("Testing Delta Lake write with OplogEntry...");
    
    // Create a temporary directory for the test
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().join("test_table");
    let table_path_str = table_path.to_str().unwrap();
    
    // Create the Delta table
    let table = DeltaOps::try_from_uri(table_path_str).await.unwrap();
    let table = table
        .create()
        .with_columns(OplogEntry::for_delta())
        .with_partition_columns(["part_id"])
        .await.unwrap();
    
    println!("Delta table created successfully");
    
    // Create a root directory entry
    let root_node_id = NodeID::root().to_string();
    let now = Utc::now().timestamp_micros();
    
    let empty_content = encode_versioned_directory_entries(&vec![]).unwrap();
    println!("Empty content encoded: {} bytes", empty_content.len());
    
    let root_entry = OplogEntry::new_inline(
        root_node_id.clone(),
        root_node_id.clone(),
        tinyfs::EntryType::Directory,
        now,
        1,
        empty_content,
    );
    
    println!("OplogEntry created successfully");
    
    // Create the batch
    let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[root_entry]).unwrap();
    println!("Record batch created: {} rows, {} columns", batch.num_rows(), batch.num_columns());
    
    // This is where the error likely occurs - the Delta Lake write
    match DeltaOps(table).write(vec![batch]).with_save_mode(SaveMode::Append).await {
        Ok(_) => {
            println!("SUCCESS: Delta Lake write completed successfully");
        }
        Err(e) => {
            println!("ERROR: Delta Lake write failed: {}", e);
            println!("Error debug: {:?}", e);
            panic!("Delta Lake write failed: {}", e);
        }
    }
}
