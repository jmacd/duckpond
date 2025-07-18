use super::schema::{OplogEntry, ForArrow, encode_versioned_directory_entries};
use serde_arrow;
use chrono::Utc;
use tinyfs::NodeID;
use deltalake::{DeltaOps, protocol::SaveMode};
use tempfile::TempDir;

/// Copy of create_oplog_table with debugging
async fn create_oplog_table_debug(table_path: &str) -> Result<(), crate::error::TLogFSError> {
    println!("DEBUG: create_oplog_table_debug called with path: {}", table_path);
    
    // Try to open existing table first
    match deltalake::open_table(table_path).await {
        Ok(_) => {
            println!("DEBUG: Table already exists, returning early");
            return Ok(());
        }
        Err(e) => {
            println!("DEBUG: Table doesn't exist ({}), creating it", e);
        }
    }
    
    // Create the table with OplogEntry schema directly - no more Record nesting
    println!("DEBUG: Creating DeltaOps from URI");
    let table = DeltaOps::try_from_uri(table_path).await?;
    
    println!("DEBUG: Creating table with schema");
    let table = table
        .create()
        .with_columns(OplogEntry::for_delta())  // Use OplogEntry directly
        .with_partition_columns(["part_id"])
        .await?;

    println!("DEBUG: Table created, preparing root entry");
    
    // Create a root directory entry as the initial OplogEntry
    let root_node_id = NodeID::root().to_string();
    let now = Utc::now().timestamp_micros();
    
    println!("DEBUG: Encoding directory entries");
    let content = encode_versioned_directory_entries(&vec![])?;
    println!("DEBUG: Encoded {} bytes", content.len());
    
    let root_entry = OplogEntry {
        part_id: root_node_id.clone(), // Root directory is its own partition
        node_id: root_node_id.clone(),
        file_type: tinyfs::EntryType::Directory,
        content: content,
        timestamp: now, // Node modification time
        version: 1, // First version of root directory node
    };

    println!("DEBUG: Creating record batch");
    // Write OplogEntry directly to Delta Lake - no more Record wrapper
    let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[root_entry])?;
    println!("DEBUG: Created batch with {} rows, {} columns", batch.num_rows(), batch.num_columns());
    
    println!("DEBUG: Writing to Delta Lake");
    let _table = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("DEBUG: Successfully wrote to Delta Lake");
    Ok(())
}

#[tokio::test]
async fn test_create_oplog_table_debug() {
    println!("Testing create_oplog_table_debug...");
    
    // Create a temporary directory for the test
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().join("test_table");
    let table_path_str = table_path.to_str().unwrap();
    
    println!("Using table path: {}", table_path_str);
    
    match create_oplog_table_debug(table_path_str).await {
        Ok(_) => {
            println!("SUCCESS: create_oplog_table_debug completed");
        }
        Err(e) => {
            println!("ERROR: create_oplog_table_debug failed: {}", e);
            println!("Error debug: {:?}", e);
            panic!("Test failed: {}", e);
        }
    }
}
