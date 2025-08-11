// Phase 2 Enhanced Replace Logic Tests
use crate::OpLogPersistence;
use tinyfs::{NodeID, EntryType};
use tempfile::TempDir;

#[tokio::test]
async fn test_version_preservation_within_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // First write
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"First content v1").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Second write to same file within same transaction - should preserve version
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"Replaced content v1").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Third write - still same version within transaction
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"Final content v1").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Should still be manageable number of operations due to replacement
    assert!(tx.operation_count() <= 3); // Directory entry creation plus final file content
    
    // Commit to finalize
    tx.commit().await.unwrap();
    
    // Verify the final content is the last write
    let content = persistence.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(content, b"Final content v1");
}

#[tokio::test]
async fn test_different_file_types_in_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id1 = NodeID::generate();
    let node_id2 = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Write FileData
    {
        let mut writer = tx.create_file_writer(node_id1, part_id, EntryType::FileData).unwrap();
        writer.write(b"Data file content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Write FileTable  
    {
        let mut writer = tx.create_file_writer(node_id2, part_id, EntryType::FileTable).unwrap();
        writer.write(b"Table file content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Replace FileData with more content
    {
        let mut writer = tx.create_file_writer(node_id1, part_id, EntryType::FileData).unwrap();
        writer.write(b"Updated data file content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Should have reasonable operation count
    assert!(tx.operation_count() >= 2); // At least the two different files
    assert!(tx.operation_count() <= 4); // But not too many due to replacement logic
    
    tx.commit().await.unwrap();
    
    // Verify both files have correct final content
    let data_content = persistence.load_file_content(node_id1, part_id).await.unwrap();
    let table_content = persistence.load_file_content(node_id2, part_id).await.unwrap();
    
    assert_eq!(data_content, b"Updated data file content");
    assert_eq!(table_content, b"Table file content");
}

#[tokio::test]
async fn test_cross_transaction_versioning() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // First transaction
    {
        let tx = persistence.begin_transaction_with_guard().await.unwrap();
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"Version 1 content").await.unwrap();
        writer.finish().await.unwrap();
        tx.commit().await.unwrap();
    }
    
    // Second transaction - should create new version
    {
        let tx = persistence.begin_transaction_with_guard().await.unwrap();
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"Version 2 content").await.unwrap();
        writer.finish().await.unwrap();
        tx.commit().await.unwrap();
    }
    
    // Verify we can see both versions (latest content returned by default)
    let content = persistence.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(content, b"Version 2 content");
    
    // Test the versioned interface if available
    // Note: This might need to be adjusted based on the actual versioned API
    // For now, just verify the basic load_file_content returns the latest
}
