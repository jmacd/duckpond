// Phase 2 Enhancement Tests - Version control and transaction optimization
use crate::OpLogPersistence;
use crate::file_writer::FileWriter;
use tinyfs::{NodeID, EntryType};
use tempfile::TempDir;

#[tokio::test]
async fn test_version_preservation_within_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::create(temp_path).await.unwrap();
    let tx = persistence.begin().await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // First write
    {
        let mut writer = FileWriter::new(node_id, part_id, EntryType::FileData, &tx);
        writer.write(b"First content v1").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Second write to same file within same transaction - should preserve version
    {
        let mut writer = FileWriter::new(node_id, part_id, EntryType::FileData, &tx);
        writer.write(b"Updated content v2").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Third write - more content to different nodes, should batch efficiently
    {
        let mut writer = FileWriter::new(node_id, part_id, EntryType::FileData, &tx);
        writer.write(b"Final content v1").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Commit to finalize
    tx.commit(None).await.unwrap();
    
    // Verify the final content is the last write (Note: this will fail until we fix the API)
    let content = tx.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(content, b"Final content v1");
}

#[tokio::test]
async fn test_different_file_types_in_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::create(temp_path).await.unwrap();
    let tx = persistence.begin().await.unwrap();
    
    let node_id1 = NodeID::generate();
    let node_id2 = NodeID::generate();
    let part_id = NodeID::generate();
    
    // Write FileData
    {
        let mut writer = FileWriter::new(node_id1, part_id, EntryType::FileData, &tx);
        writer.write(b"Data file content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Write FileTable  
    {
        let mut writer = FileWriter::new(node_id2, part_id, EntryType::FileTable, &tx);
        writer.write(b"Table file content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Replace FileData with more content
    {
        let mut writer = FileWriter::new(node_id1, part_id, EntryType::FileData, &tx);
        writer.write(b"Updated data file content").await.unwrap();
        writer.finish().await.unwrap();
    }
    
    // Note: Verification commented out until we fix the API calls
    let data_content = tx.load_file_content(node_id1, part_id).await.unwrap();
    let table_content = tx.load_file_content(node_id2, part_id).await.unwrap();
    assert_eq!(data_content, b"Updated data file content");
    assert_eq!(table_content, b"Table file content");

    tx.commit(None).await.unwrap();
}

#[tokio::test]
async fn test_cross_transaction_versioning() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();
    
    let persistence = OpLogPersistence::create(temp_path).await.unwrap();
    
    let node_id = NodeID::generate();
    let part_id = NodeID::generate();
    
    // First transaction - version 1
    {
        let tx = persistence.begin().await.unwrap();
        let mut writer = FileWriter::new(node_id, part_id, EntryType::FileData, &tx);
        writer.write(b"Version 1 content").await.unwrap();
        writer.finish().await.unwrap();
        tx.commit(None).await.unwrap();
    }
    
    // Second transaction - version 2
    {
        let tx = persistence.begin().await.unwrap();
        let mut writer = FileWriter::new(node_id, part_id, EntryType::FileData, &tx);
        writer.write(b"Version 2 content").await.unwrap();
        writer.finish().await.unwrap();
        tx.commit(None).await.unwrap();
    }
    
    let tx = persistence.begin().await.unwrap();
    let content = tx.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(content, b"Version 2 content");
    tx.commit(None).await.unwrap();
}
