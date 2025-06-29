// Minimal test to isolate persistence layer commit/query issue

#[cfg(test)]
mod persistence_debug {
    use tempfile;
    use tinyfs::persistence::PersistenceLayer;  // Import the trait
    
    #[tokio::test]
    async fn test_persistence_commit_query_cycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store_path = temp_dir.path().join("persistence_test");
        let store_uri = format!("file://{}", store_path.display());
        
        println!("=== PHASE 1: CREATE FIRST PERSISTENCE LAYER ===");
        
        // Create first persistence layer
        let persistence1 = crate::persistence::OpLogPersistence::new(&store_uri).await.unwrap();
        
        // Add a directory entry
        let parent_node_id = tinyfs::NodeID::new(0);
        let child_node_id = tinyfs::NodeID::new(1);
        
        println!("Adding directory entry via persistence1");
        persistence1.update_directory_entry(
            parent_node_id, 
            "test_entry", 
            tinyfs::persistence::DirectoryOperation::Insert(child_node_id)
        ).await.unwrap();
        
        // Commit
        println!("Committing via persistence1");
        persistence1.commit().await.unwrap();
        
        // Check if Delta table files actually exist
        println!("=== CHECKING FILE SYSTEM ===");
        println!("Store path: {}", store_path.display());
        println!("Store URI: {}", store_uri);
        
        if store_path.exists() {
            println!("Store path exists!");
            for entry in std::fs::read_dir(&store_path).unwrap() {
                let entry = entry.unwrap();
                println!("  - {}", entry.file_name().to_string_lossy());
            }
        } else {
            println!("Store path does NOT exist!");
        }
        
        println!("=== PHASE 2: CREATE SECOND PERSISTENCE LAYER ===");
        
        // Create second persistence layer (simulating reopening the filesystem)
        let persistence2 = crate::persistence::OpLogPersistence::new(&store_uri).await.unwrap();
        
        // Query directory entries
        println!("Querying directory entries via persistence2");
        let entries = persistence2.load_directory_entries(parent_node_id).await.unwrap();
        
        println!("Found {} entries", entries.len());
        for (name, node_id) in &entries {
            println!("  {}: {}", name, node_id.to_hex_string());
        }
        
        // Assert the entry exists
        assert!(entries.contains_key("test_entry"), "Entry should persist after commit");
        assert_eq!(entries.get("test_entry"), Some(&child_node_id), "Entry should have correct node ID");
        
        println!("SUCCESS: Persistence layer commit/query cycle works!");
    }
}
