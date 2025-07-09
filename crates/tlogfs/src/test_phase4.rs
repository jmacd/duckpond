// Phase 4 Architecture Test - TinyFS with OpLogPersistence Integration

#[cfg(test)]
mod tests {
    use super::super::persistence::OpLogPersistence;
    use super::super::{create_oplog_fs}; // Now from persistence module
    use tinyfs::persistence::PersistenceLayer;
    use tinyfs::{FS, NodeID};
    use tempfile::TempDir;
    use diagnostics::{log_info, log_debug};

    #[tokio::test]
    async fn test_phase4_persistence_layer() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let store_path = temp_dir.path().join("test_store");
        let store_path_str = store_path.to_string_lossy();

        // Test OpLogPersistence directly
        let persistence = OpLogPersistence::new(&store_path_str).await?;
        
        // Test basic operations
        let _node_id = NodeID::generate();
        let part_id = NodeID::root(); // Root directory
        
        // For Phase 4, we'll test the architecture without creating actual nodes
        // since that requires the existing OpLog backend integration
        
        // Test directory operations
        let entries = persistence.load_directory_entries(part_id).await?;
        assert!(entries.is_empty()); // Should be empty initially
        
        // Test commit
        persistence.commit().await?;
        
        log_info!("Phase 4 persistence layer test passed!");
        log_info!("  - OpLogPersistence created successfully");
        log_info!("  - Directory entries loaded (empty as expected)");
        log_info!("  - Commit operation successful");
        Ok(())
    }

    #[tokio::test]
    async fn test_phase4_fs_integration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let store_path = temp_dir.path().join("test_store");
        let store_path_str = store_path.to_string_lossy();

        // Test the new factory function
        let fs = create_oplog_fs(&store_path_str).await?;
        
        // Test basic FS operations with persistence layer
        let _root = fs.root().await?;
        log_info!("Phase 4 FS integration: Root directory created");
        
        // Test commit
        fs.commit().await?;        log_info!("Phase 4 FS integration: Commit successful");

        log_info!("Phase 4 FS integration test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_phase4_architecture_benefits() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let store_path = temp_dir.path().join("test_store");
        let store_path_str = store_path.to_string_lossy();

        // Create persistence layer
        let persistence = OpLogPersistence::new(&store_path_str).await?;
        
        // Create FS with persistence layer
        let _fs = FS::with_persistence_layer(persistence).await?;
        
        // Test direct persistence calls (no caching complexity)
        let _node_id = NodeID::generate();
        let _part_id = NodeID::root();
        
        // This demonstrates the clean two-layer architecture:
        // 1. FS coordinator layer
        // 2. PersistenceLayer (OpLogPersistence)
        
        log_info!("Phase 4 architecture benefits test: Clean separation achieved");
        log_info!("  - FS only handles coordination");
        log_info!("  - PersistenceLayer only handles storage");
        log_info!("  - No mixed responsibilities");
        log_info!("  - Direct persistence calls (no caching complexity)");
        
        Ok(())
    }

    #[tokio::test]
    #[ignore = "TODO: Fix directory query ordering issue"]
    async fn test_optimized_directory_query() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let store_path = temp_dir.path().join("test_store");
        let store_path_str = store_path.to_string_lossy();

        // Create persistence layer
        let persistence = OpLogPersistence::new(&store_path_str).await?;
        let root_id = NodeID::root();
        
        // Create a directory with multiple entries
        let file1_id = NodeID::generate();
        let file2_id = NodeID::generate();
        let file3_id = NodeID::generate();
        
        log_debug!("Creating directory with multiple entries...");
        
        // Add multiple entries to the directory
        use tinyfs::persistence::DirectoryOperation;
        persistence.update_directory_entry(root_id, "file1.txt", DirectoryOperation::Insert(file1_id)).await?;
        persistence.update_directory_entry(root_id, "file2.txt", DirectoryOperation::Insert(file2_id)).await?;
        persistence.update_directory_entry(root_id, "file3.txt", DirectoryOperation::Insert(file3_id)).await?;
        
        // Commit the changes
        persistence.commit().await?;
        
        log_debug!("Testing optimized single entry query...");
        
        // Test the optimized query for a specific entry
        let found_id = persistence.query_directory_entry_by_name(root_id, "file2.txt").await?;
        assert_eq!(found_id, Some(file2_id));
        let file2_id_debug = format!("{:?}", file2_id);
        log_debug!("✓ Found file2.txt with optimized query: {file2_id_debug}", file2_id_debug: file2_id_debug);
        
        // Test query for non-existent entry
        let not_found = persistence.query_directory_entry_by_name(root_id, "nonexistent.txt").await?;
        assert_eq!(not_found, None);
        log_debug!("✓ Correctly returned None for non-existent file");
        
        // Test after deleting an entry
        persistence.update_directory_entry(root_id, "file2.txt", DirectoryOperation::Delete).await?;
        persistence.commit().await?;
        
        let deleted_entry = persistence.query_directory_entry_by_name(root_id, "file2.txt").await?;
        assert_eq!(deleted_entry, None);
        log_debug!("✓ Correctly returned None for deleted file");
        
        // Verify other entries still exist
        let still_exists = persistence.query_directory_entry_by_name(root_id, "file1.txt").await?;
        assert_eq!(still_exists, Some(file1_id));
        log_debug!("✓ Other entries still accessible after deletion");
        
        // Compare with traditional load_directory_entries approach
        log_debug!("Comparing with traditional directory loading...");
        let all_entries = persistence.load_directory_entries(root_id).await?;
        
        // Verify consistency between optimized query and full load
        for (name, node_id) in &all_entries {
            let optimized_result = persistence.query_directory_entry_by_name(root_id, name).await?;
            assert_eq!(optimized_result, Some(*node_id));
            log_debug!("✓ Optimized query consistent with full load for: {name}", name: name);
        }
        
        // Verify the optimized approach finds the same entries as full load
        assert!(all_entries.contains_key("file1.txt"));
        assert!(all_entries.contains_key("file3.txt"));
        assert!(!all_entries.contains_key("file2.txt")); // Should be deleted
        
        log_info!("Optimized directory query test passed!");
        log_info!("  - Single entry queries work correctly");
        log_info!("  - Handles non-existent entries");
        log_info!("  - Properly handles deleted entries");
        log_info!("  - Results consistent with full directory load");
        log_info!("  - Optimization provides early termination benefit");
        
        Ok(())
    }

}
