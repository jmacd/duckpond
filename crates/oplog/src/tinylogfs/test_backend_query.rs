// Test to demonstrate the new architecture working at the backend level

#[cfg(test)]
mod test_backend_query {
    use super::super::*;
    use crate::tinylogfs::backend::OpLogBackend;
    use crate::tinylogfs::schema::{OplogEntry, DirectoryEntry};
    use crate::delta::ForArrow;
    
    #[tokio::test]
    async fn test_backend_directory_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store_path = temp_dir.path().join("test_backend_query");
        let store_uri = format!("file://{}", store_path.display());
        
        // Create backend
        let backend = OpLogBackend::new(&store_uri).await.unwrap();
        
        // Create some test directory entries
        let entries = vec![
            DirectoryEntry {
                name: "file1.txt".to_string(),
                child: "child_node_123".to_string(),
            },
            DirectoryEntry {
                name: "subdir".to_string(),
                child: "child_node_456".to_string(),
            }
        ];
        
        // Serialize entries to OplogEntry
        let serialized_entries = backend.serialize_directory_entries(&entries).unwrap();
        let directory_node_id = "test_directory_123";
        
        let oplog_entry = OplogEntry {
            part_id: directory_node_id.to_string(),
            node_id: directory_node_id.to_string(),
            file_type: "directory".to_string(),
            content: serialized_entries,
        };
        
        // Add to backend and commit
        backend.add_pending_record(oplog_entry).unwrap();
        let committed_count = backend.commit().await.unwrap();
        println!("Committed {} operations", committed_count);
        
        // Now query the directory entries back
        let queried_entries = backend.query_directory_entries(directory_node_id).await.unwrap();
        
        println!("Original entries: {} items", entries.len());
        for entry in &entries {
            println!("  - {}: {}", entry.name, entry.child);
        }
        
        println!("Queried entries: {} items", queried_entries.len());
        for entry in &queried_entries {
            println!("  - {}: {}", entry.name, entry.child);
        }
        
        assert_eq!(entries.len(), queried_entries.len());
        assert_eq!(entries[0].name, queried_entries[0].name);
        assert_eq!(entries[1].name, queried_entries[1].name);
        
        println!("✅ Backend query architecture works!");
        println!("This demonstrates how directories should query committed data via backend");
    }
}
