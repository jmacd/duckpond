use std::sync::Arc;
use tlogfs::OpLogPersistence;
use tinyfs::NodeID;
use tinyfs::persistence::PersistenceLayer;

#[tokio::test]
async fn debug_directory_issue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store_path = temp_dir.path().join("test_store");
    
    // Create persistence layer
    let persistence = Arc::new(OpLogPersistence::new(&store_path.to_string_lossy()).await.unwrap());
    
    // Create root directory
    let root_id = NodeID::root();
    let root_node = tinyfs::NodeType::Directory(
        tinyfs::memory::MemoryDirectory::new_handle(root_id)
    );
    
    // Store root directory
    persistence.store_node(root_id, root_id, &root_node).await.unwrap();
    
    // Create child directory
    let child_id = NodeID::new();
    let child_node = tinyfs::NodeType::Directory(
        tinyfs::memory::MemoryDirectory::new_handle(child_id)
    );
    
    // Store child directory
    persistence.store_node(child_id, child_id, &child_node).await.unwrap();
    
    // Add child to root directory
    let entry_type = tinyfs::EntryType::Directory;
    let operation = tlogfs::DirectoryOperation::InsertWithType(child_id, entry_type.clone());
    
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
