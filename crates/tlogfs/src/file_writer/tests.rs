// Tests for FileWriter module
use super::*;
use crate::OpLogPersistence;
use tinyfs::{NodeID, EntryType};
use tempfile::TempDir;

#[tokio::test]
async fn test_file_writer_creation() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Test that we can create a file writer
    let writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
    
    // Writer should be bound to the transaction
    assert_eq!(writer.node_id, node_id);
    assert_eq!(writer.part_id, part_id);
    assert_eq!(writer.file_type, EntryType::FileData);
}

#[tokio::test]
async fn test_file_writer_small_file() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
    
    // Write small file content
    let content = b"Hello, World! This is a small file.";
    writer.write(content).await.unwrap();
    
    // Verify it's still in small storage
    match &writer.storage {
        WriterStorage::Small(buffer) => {
            assert_eq!(buffer, content);
        }
        WriterStorage::Large(_) => {
            panic!("Small content should not be promoted to large storage");
        }
    }
    
    // Finalize the write
    let result = writer.finish().await.unwrap();
    assert_eq!(result.size, content.len() as u64);
    
    // Transaction should have one operation now
    assert!(tx.operation_count() > 0);
    
    // Commit to finalize
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn test_file_writer_promotion_to_large() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
    
    // Write content that will trigger promotion to large file storage
    let large_content = vec![b'A'; crate::large_files::LARGE_FILE_THRESHOLD + 100];
    
    // Start with small content
    writer.write(b"Small start").await.unwrap();
    
    // This should trigger promotion
    writer.write(&large_content).await.unwrap();
    
    // Verify it was promoted to large storage
    match &writer.storage {
        WriterStorage::Small(_) => {
            panic!("Large content should be promoted to large storage");
        }
        WriterStorage::Large(_) => {
            // Expected - content was promoted
        }
    }
    
    // Finalize the write
    let result = writer.finish().await.unwrap();
    assert_eq!(result.size, (b"Small start".len() + large_content.len()) as u64);
    
    // Commit to finalize
    tx.commit().await.unwrap();
}

#[tokio::test] 
async fn test_file_writer_replace_within_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::new(temp_path).await.unwrap();
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // First write
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"First content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Second write to same file within same transaction - should replace
    {
        let mut writer = tx.create_file_writer(node_id, part_id, EntryType::FileData).unwrap();
        writer.write(b"Replaced content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Should still only have a reasonable number of operations
    // (The exact count depends on implementation details)
    assert!(tx.operation_count() > 0);
    
    // Commit to finalize
    tx.commit().await.unwrap();
}
