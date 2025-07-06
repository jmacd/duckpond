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
        
        diagnostics::log_info!("=== PHASE 1: CREATE FIRST PERSISTENCE LAYER ===");
        
        // Create first persistence layer
        let persistence1 = crate::persistence::OpLogPersistence::new(&store_uri).await.unwrap();
        
        // Add a directory entry
        let parent_node_id = tinyfs::NodeID::new(0);
        let child_node_id = tinyfs::NodeID::new(1);
        
        diagnostics::log_info!("Adding directory entry via persistence1");
        persistence1.update_directory_entry(
            parent_node_id, 
            "test_entry", 
            tinyfs::persistence::DirectoryOperation::Insert(child_node_id)
        ).await.unwrap();
        
        // Commit
        diagnostics::log_info!("Committing via persistence1");
        persistence1.commit().await.unwrap();
        
        // Check if Delta table files actually exist
        diagnostics::log_info!("=== CHECKING FILE SYSTEM ===");
        let store_path_display = store_path.display().to_string();
        let store_uri_bound = &store_uri;
        diagnostics::log_info!("Store path: {store_path}", store_path: store_path_display);
        diagnostics::log_info!("Store URI: {store_uri}", store_uri: store_uri_bound);
        
        if store_path.exists() {
            diagnostics::log_info!("Store path exists!");
            for entry in std::fs::read_dir(&store_path).unwrap() {
                let entry = entry.unwrap();
                let filename = entry.file_name().to_string_lossy().to_string();
                diagnostics::log_info!("  - {filename}", filename: filename);
            }
        } else {
            diagnostics::log_info!("Store path does NOT exist!");
        }
        
        diagnostics::log_info!("=== PHASE 2: CREATE SECOND PERSISTENCE LAYER ===");
        
        // Create second persistence layer (simulating reopening the filesystem)
        let persistence2 = crate::persistence::OpLogPersistence::new(&store_uri).await.unwrap();
        
        // Query directory entries
        diagnostics::log_info!("Querying directory entries via persistence2");
        let entries = persistence2.load_directory_entries(parent_node_id).await.unwrap();
        
        let entry_count = entries.len();
        diagnostics::log_info!("Found {entry_count} entries", entry_count: entry_count);
        for (name, node_id) in &entries {
            let name_bound = name;
            let node_hex = node_id.to_hex_string();
            diagnostics::log_info!("  {name}: {node_hex}", name: name_bound, node_hex: node_hex);
        }
        
        // Assert the entry exists
        assert!(entries.contains_key("test_entry"), "Entry should persist after commit");
        assert_eq!(entries.get("test_entry"), Some(&child_node_id), "Entry should have correct node ID");
        
        diagnostics::log_info!("SUCCESS: Persistence layer commit/query cycle works!");
    }
}
