// Test to isolate the integration test scenario

use tempfile::TempDir;
use crate::delta::DeltaTableManager;
use crate::schema::{create_oplog_table, OplogEntry, ForArrow};
use tinyfs::NodeID;
use tinyfs::persistence::PersistenceLayer;
use tinyfs::DirectoryOperation;
use std::sync::Arc;
use crate::OpLogPersistence;

#[tokio::test]
async fn debug_integration_test_scenario() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let data_path = tmp.path().join("data");
    let control_path = tmp.path().join("control");
    
    // Create directories if they don't exist
    std::fs::create_dir_all(&data_path)?;
    std::fs::create_dir_all(&control_path)?;
    
    let data_path_str = data_path.to_string_lossy().to_string();
    let control_path_str = control_path.to_string_lossy().to_string();
    
    println!("DEBUG: Data path: {}", data_path_str);
    println!("DEBUG: Control path: {}", control_path_str);
    
    // EXACTLY simulate what Ship::create_infrastructure does
    
    // 1. Force cache invalidation before creating new filesystem instances
    let temp_delta_manager = DeltaTableManager::new();
    temp_delta_manager.invalidate_table(&data_path_str).await;
    temp_delta_manager.invalidate_table(&control_path_str).await;
    println!("DEBUG: Cache invalidated");
    
    // 2. Initialize data filesystem with direct persistence access
    println!("DEBUG: About to create data persistence");
    let data_persistence = crate::OpLogPersistence::new(&data_path_str).await?;
    println!("DEBUG: Data persistence created");
    
    // 3. Initialize control filesystem
    println!("DEBUG: About to create control persistence");
    let control_persistence = crate::OpLogPersistence::new(&control_path_str).await?;
    println!("DEBUG: Control persistence created");
    
    // 4. Create tinyfs instances (simulate what happens in Ship::create_infrastructure)
    let data_fs = tinyfs::FS::with_persistence_layer(data_persistence.clone()).await?;
    println!("DEBUG: Data FS created");
    
    let _control_fs = tinyfs::FS::with_persistence_layer(control_persistence).await?;
    println!("DEBUG: Control FS created");
    
    // 5. Now simulate what happens in initialize_new_pond
    // This is where the error occurs: data_fs().root()
    println!("DEBUG: About to call data_fs.root()");
    
    let _root = data_fs.root().await?;
    println!("DEBUG: Successfully called data_fs.root()");
    
    // 6. Try to access the root again
    let _root2 = data_fs.root().await?;
    println!("DEBUG: Successfully called data_fs.root() again");
    
    println!("DEBUG: Integration test scenario completed successfully");
    Ok(())
}

#[tokio::test]
async fn debug_create_oplog_table_twice() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_path = tmp.path().join("test_table").to_string_lossy().to_string();
    
    println!("DEBUG: Creating table at: {}", table_path);
    
    // First call - should create the table
    create_oplog_table(&table_path).await?;
    println!("DEBUG: First create_oplog_table call succeeded");
    
    // Second call - should be a no-op (table already exists)
    create_oplog_table(&table_path).await?;
    println!("DEBUG: Second create_oplog_table call succeeded");
    
    // Third call - should also be a no-op
    create_oplog_table(&table_path).await?;
    println!("DEBUG: Third create_oplog_table call succeeded");
    
    println!("DEBUG: Multiple create_oplog_table calls completed successfully");
    Ok(())
}

#[tokio::test]
async fn debug_empty_vs_non_empty_batches() -> Result<(), Box<dyn std::error::Error>> {
    // Test what happens with empty vs non-empty batches
    
    // Empty batch
    let empty_entries: Vec<OplogEntry> = vec![];
    let empty_batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &empty_entries)?;
    println!("DEBUG: Empty batch - rows: {}, columns: {}", empty_batch.num_rows(), empty_batch.num_columns());
    
    // Non-empty batch
    let root_node_id = NodeID::root().to_string();
    let non_empty_entries = vec![OplogEntry {
        part_id: root_node_id.clone(),
        node_id: root_node_id.clone(),
        file_type: tinyfs::EntryType::Directory,
        content: vec![],
        timestamp: 123456789,
        version: 1,
    }];
    
    let non_empty_batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &non_empty_entries)?;
    println!("DEBUG: Non-empty batch - rows: {}, columns: {}", non_empty_batch.num_rows(), non_empty_batch.num_columns());
    
    // Check if either batch is considered "empty" by some criteria
    println!("DEBUG: Empty batch schema: {:?}", empty_batch.schema());
    println!("DEBUG: Non-empty batch schema: {:?}", non_empty_batch.schema());
    
    Ok(())
}

#[tokio::test]
async fn debug_directory_entry_issue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store_path = temp_dir.path().join("test_store");
    
    // Create persistence layer
    let persistence = Arc::new(OpLogPersistence::new(&store_path.to_string_lossy()).await.unwrap());
    
    // Create root directory
    let root_id = NodeID::root();
    let root_node = tinyfs::NodeType::Directory(
        tinyfs::memory::MemoryDirectory::new_handle()
    );
    
    // Store root directory
    persistence.store_node(root_id, root_id, &root_node).await.unwrap();
    
    // Create child directory
    let child_id = NodeID::generate();
    let child_node = tinyfs::NodeType::Directory(
        tinyfs::memory::MemoryDirectory::new_handle()
    );
    
    // Store child directory
    persistence.store_node(child_id, child_id, &child_node).await.unwrap();
    
    // Add child to root directory
    let entry_type = tinyfs::EntryType::Directory;
    let operation = DirectoryOperation::InsertWithType(child_id, entry_type.clone());
    
    persistence.update_directory_entry_with_type(
        root_id,
        "test_dir",
        operation,
        &entry_type
    ).await.unwrap();
    
    // Commit
    persistence.commit().await.unwrap();
    
    // Now query for the entry
    let entries = persistence.load_directory_entries(root_id).await.unwrap();
    
    println!("Directory entries: {:?}", entries);
    
    // Check if we can find the entry
    let found_entry = persistence.query_single_directory_entry(root_id, "test_dir").await.unwrap();
    println!("Found entry: {:?}", found_entry);
    
    assert!(entries.contains_key("test_dir"));
    assert!(found_entry.is_some());
}
