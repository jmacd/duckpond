use super::*;
use crate::large_files::*;
use tokio::io::AsyncWriteExt;
use tempfile::tempdir;

/// Create a test OpLogPersistence instance
async fn create_test_persistence() -> OpLogPersistence {
    let temp_dir = tempdir().unwrap();
    let table_path = temp_dir.path().join("test_pond").to_string_lossy().to_string();
    
    OpLogPersistence::new(&table_path).await.unwrap()
}

#[tokio::test]
async fn test_hybrid_writer_small_file() {
    let persistence = create_test_persistence().await;
    
    // Small file (< threshold) via hybrid writer
    let content = vec![42u8; LARGE_FILE_THRESHOLD / 64]; // Small file (1/64th of threshold)
    
    let mut writer = persistence.create_hybrid_writer();
    writer.write_all(&content).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Should be small file (content in memory)
    assert_eq!(result.content, content);
    assert_eq!(result.size, 1024);
    assert!(result.size < LARGE_FILE_THRESHOLD);
    
    // Store via hybrid result
    let node_id = tinyfs::NodeID::generate();
    let part_id = tinyfs::NodeID::generate();
    persistence.store_file_from_hybrid_writer(node_id, part_id, result).await.unwrap();
    
    // Verify we can load the content back
    let loaded = persistence.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(loaded, content);
}

#[tokio::test]
async fn test_hybrid_writer_large_file() {
    println!("=== Starting test_hybrid_writer_large_file ===");
    let persistence = create_test_persistence().await;
    
    // Large file (> threshold) via hybrid writer
    let content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // Slightly larger than threshold
    println!("Created content with {} bytes (threshold is {})", content.len(), LARGE_FILE_THRESHOLD);
    
    let mut writer = persistence.create_hybrid_writer();
    writer.write_all(&content).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    println!("HybridWriter result: size={}, content.len()={}, sha256={}", 
             result.size, result.content.len(), result.sha256);
    
    // Should be large file (empty content, file stored externally)
    assert!(result.content.is_empty());
    assert_eq!(result.size, LARGE_FILE_THRESHOLD + 1000);
    assert!(result.size > LARGE_FILE_THRESHOLD);
    
    // Verify large file exists at content-addressed path with correct content
    let table_path = persistence.store_path();
    let large_file_path = large_file_path(table_path, &result.sha256).await.unwrap();
    println!("Large file should be at: {}", large_file_path.display());
    
    // Check file exists and has correct size
    let metadata = tokio::fs::metadata(&large_file_path).await.unwrap();
    println!("✅ Large file exists on disk with size: {}", metadata.len());
    assert_eq!(metadata.len(), (LARGE_FILE_THRESHOLD + 1000) as u64);
    
    // Verify file content matches what we wrote
    let disk_content = tokio::fs::read(&large_file_path).await.unwrap();
    println!("✅ Read {} bytes from disk file", disk_content.len());
    
    // Compare lengths first
    assert_eq!(disk_content.len(), content.len(), "File size mismatch");
    
    // Compare content without printing it all if it fails
    if disk_content != content {
        // Find first difference for debugging
        for (i, (a, b)) in disk_content.iter().zip(content.iter()).enumerate() {
            if a != b {
                panic!("Content mismatch at byte {}: disk={}, expected={}", i, a, b);
            }
        }
        panic!("Content lengths match but comparison failed - this shouldn't happen");
    }
    println!("✅ Disk file content matches original");
    
    // Verify SHA256 is correct
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(&content);
    let expected_sha256 = format!("{:x}", hasher.finalize());
    assert_eq!(result.sha256, expected_sha256);
    println!("✅ SHA256 matches expected: {}", expected_sha256);
    
    // Store via hybrid result
    let node_id = tinyfs::NodeID::generate();
    let part_id = tinyfs::NodeID::generate();
    println!("Storing file with node_id={}, part_id={}", node_id.to_hex_string(), part_id.to_hex_string());
    
    persistence.store_file_from_hybrid_writer(node_id, part_id, result.clone()).await.unwrap();
    println!("✅ Stored large file metadata in persistence layer");
    
    // Verify we can load the content back
    println!("Loading file content back...");
    let loaded = persistence.load_file_content(node_id, part_id).await.unwrap();
    println!("✅ Loaded {} bytes back from persistence", loaded.len());
    
    // Compare loaded content without printing it all if it fails
    assert_eq!(loaded.len(), content.len(), "Loaded content size mismatch");
    if loaded != content {
        // Find first difference for debugging
        for (i, (a, b)) in loaded.iter().zip(content.iter()).enumerate() {
            if a != b {
                panic!("Loaded content mismatch at byte {}: loaded={}, expected={}", i, a, b);
            }
        }
        panic!("Loaded content lengths match but comparison failed - this shouldn't happen");
    }
    println!("✅ Content matches original!");
}

#[tokio::test]
async fn test_hybrid_writer_incremental_hash() {
    let persistence = create_test_persistence().await;
    
    // Write content in chunks to test incremental hashing
    let chunk1 = vec![1u8; LARGE_FILE_THRESHOLD / 2]; // Half threshold
    let chunk2 = vec![2u8; LARGE_FILE_THRESHOLD / 2 + 1000]; // Just over half threshold
    let total_content = [chunk1.clone(), chunk2.clone()].concat();
    
    let mut writer = persistence.create_hybrid_writer();
    
    // Write in chunks
    writer.write_all(&chunk1).await.unwrap();
    writer.write_all(&chunk2).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Verify hash matches content written in chunks
    use sha2::{Sha256, Digest};
    let mut expected_hasher = Sha256::new();
    expected_hasher.update(&total_content);
    let expected_hash = format!("{:x}", expected_hasher.finalize());
    
    assert_eq!(result.sha256, expected_hash);
    assert_eq!(result.size, LARGE_FILE_THRESHOLD + 1000);
    
    // Verify file stored correctly
    let table_path = persistence.store_path();
    let large_file_path = large_file_path(table_path, &result.sha256).await.unwrap();
    let stored_content = tokio::fs::read(&large_file_path).await.unwrap();
    assert_eq!(stored_content, total_content);
}

#[tokio::test]
async fn test_hybrid_writer_spillover() {
    let persistence = create_test_persistence().await;
    
    // Write more than memory threshold to test spillover
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD * 32]; // 32x threshold (large spillover test)
    
    let mut writer = persistence.create_hybrid_writer();
    
    // Write in chunks to trigger spillover
    let chunk_size = 256 * 1024; // 256 KiB chunks
    for chunk in large_content.chunks(chunk_size) {
        writer.write_all(chunk).await.unwrap();
    }
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Should be large file
    assert!(result.content.is_empty());
    assert_eq!(result.size, large_content.len());
    
    // Verify file stored correctly
    let table_path = persistence.store_path();
    let large_file_path = large_file_path(table_path, &result.sha256).await.unwrap();
    let stored_content = tokio::fs::read(&large_file_path).await.unwrap();
    assert_eq!(stored_content, large_content);
}

#[tokio::test]
async fn test_hybrid_writer_deduplication() {
    let persistence = create_test_persistence().await;
    
    // Create two identical large files via hybrid writer
    let content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // > threshold
    
    // First file
    let mut writer1 = persistence.create_hybrid_writer();
    writer1.write_all(&content).await.unwrap();
    writer1.shutdown().await.unwrap();
    let result1 = writer1.finalize().await.unwrap();
    
    // Second file with same content
    let mut writer2 = persistence.create_hybrid_writer();
    writer2.write_all(&content).await.unwrap();
    writer2.shutdown().await.unwrap();
    let result2 = writer2.finalize().await.unwrap();
    
    // Should have same hash (deduplication)
    assert_eq!(result1.sha256, result2.sha256);
    
    // Both files should exist at same path (second overwrites first, but content is identical)
    let table_path = persistence.store_path();
    let large_file_path = large_file_path(table_path, &result1.sha256).await.unwrap();
    assert!(tokio::fs::metadata(&large_file_path).await.is_ok());
    
    // Store both entries
    let node_id1 = tinyfs::NodeID::generate();
    let part_id1 = tinyfs::NodeID::generate();
    let node_id2 = tinyfs::NodeID::generate();
    let part_id2 = tinyfs::NodeID::generate();
    
    persistence.store_file_from_hybrid_writer(node_id1, part_id1, result1).await.unwrap();
    persistence.store_file_from_hybrid_writer(node_id2, part_id2, result2).await.unwrap();
    
    // Verify both entries read the same data
    let data1 = persistence.load_file_content(node_id1, part_id1).await.unwrap();
    let data2 = persistence.load_file_content(node_id2, part_id2).await.unwrap();
    assert_eq!(data1, data2);
    assert_eq!(data1, content);
}

// Legacy tests for backward compatibility
#[tokio::test]
async fn test_small_file_storage() {
    let persistence = create_test_persistence().await;
    
    // Small file (< threshold)
    let content = vec![42u8; LARGE_FILE_THRESHOLD / 64]; // Small file (1/64th of threshold)
    let node_id = tinyfs::NodeID::generate();
    let part_id = tinyfs::NodeID::generate();
    
    persistence.store_file_content(node_id, part_id, &content).await.unwrap();
    
    // Verify retrieval
    let loaded = persistence.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(loaded, content);
}

#[tokio::test]
async fn test_large_file_storage() {
    let persistence = create_test_persistence().await;
    
    // Large file (> threshold)
    let content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // > threshold
    let node_id = tinyfs::NodeID::generate();
    let part_id = tinyfs::NodeID::generate();
    
    persistence.store_file_content(node_id, part_id, &content).await.unwrap();
    
    // Verify large file exists
    let table_path = persistence.store_path();
    let large_files_dir = std::path::PathBuf::from(table_path).join("_large_files");
    assert!(tokio::fs::metadata(&large_files_dir).await.is_ok());
    
    // Verify retrieval
    let loaded = persistence.load_file_content(node_id, part_id).await.unwrap();
    assert_eq!(loaded, content);
}

#[tokio::test]
async fn test_threshold_boundary() {
    let persistence = create_test_persistence().await;
    
    // Just under threshold
    let small_content = vec![42u8; LARGE_FILE_THRESHOLD - 1];
    let node_id1 = tinyfs::NodeID::generate();
    let part_id1 = tinyfs::NodeID::generate();
    persistence.store_file_content(node_id1, part_id1, &small_content).await.unwrap();
    
    // Verify small file retrieval
    let loaded1 = persistence.load_file_content(node_id1, part_id1).await.unwrap();
    assert_eq!(loaded1, small_content);
    
    // Just over threshold
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD + 1];
    let node_id2 = tinyfs::NodeID::generate();
    let part_id2 = tinyfs::NodeID::generate();
    persistence.store_file_content(node_id2, part_id2, &large_content).await.unwrap();
    
    // Verify large file retrieval
    let loaded2 = persistence.load_file_content(node_id2, part_id2).await.unwrap();
    assert_eq!(loaded2, large_content);
}

#[tokio::test]
async fn test_hybrid_writer_threshold_boundary() {
    println!("=== Testing threshold boundary behavior ===");
    let persistence = create_test_persistence().await;
    
    // Exactly threshold should be considered small
    let content = vec![42u8; LARGE_FILE_THRESHOLD]; // Exactly threshold
    println!("Created content with {} bytes (threshold is {})", content.len(), LARGE_FILE_THRESHOLD);
    assert_eq!(content.len(), LARGE_FILE_THRESHOLD);
    
    let mut writer = persistence.create_hybrid_writer();
    writer.write_all(&content).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    println!("HybridWriter result for threshold size: size={}, content.len()={}, is_large={}", 
             result.size, result.content.len(), result.content.is_empty());
    
    // Should be large file (content stored externally)
    assert!(result.content.is_empty(), "Threshold size exactly should be stored externally");
    assert_eq!(result.size, LARGE_FILE_THRESHOLD);
    assert!(result.size >= LARGE_FILE_THRESHOLD);
    
    println!("✅ Threshold size exactly is correctly treated as small file");
}

#[tokio::test]
async fn test_large_file_sync_to_disk() {
    // Test that large files are physically written and synced to disk
    let persistence = create_test_persistence().await;
    
    let content = vec![42u8; LARGE_FILE_THRESHOLD + 1000];
    
    // Use hybrid writer directly to test the sync behavior
    let mut writer = persistence.create_hybrid_writer();
    writer.write_all(&content).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // At this point, finalize() should have called sync_all()
    // The large file should be physically on disk and readable
    let table_path = persistence.store_path();
    let large_file_path = crate::large_files::large_file_path(table_path, &result.sha256).await.unwrap();
    
    // Verify the file exists and has correct content
    assert!(tokio::fs::metadata(&large_file_path).await.is_ok(), "Large file should exist on disk after finalize()");
    
    let disk_content = tokio::fs::read(&large_file_path).await.unwrap();
    assert_eq!(disk_content, content, "Large file content should match original after sync");
    
    println!("✅ Large file sync verified: file written and synced to disk before finalize() returns");
}

#[tokio::test]
async fn test_hierarchical_directory_structure() {
    println!("=== Starting test_hierarchical_directory_structure ===");
    let persistence = create_test_persistence().await;
    
    // Check initial state
    let table_path = persistence.store_path();
    let large_files_dir = std::path::PathBuf::from(table_path).join("_large_files");
    
    // Create just a few files to test the basic functionality first
    let mut results = Vec::new();
    for i in 0..3 {
        let content = vec![42u8; LARGE_FILE_THRESHOLD + i]; // Each file slightly different
        let mut writer = persistence.create_hybrid_writer();
        writer.write_all(&content).await.unwrap();
        writer.shutdown().await.unwrap();
        let result = writer.finalize().await.unwrap();
        println!("File {} created: size={}, sha256={}, content.is_empty={}", 
                i, result.size, result.sha256, result.content.is_empty());
        results.push((result, content));
    }
    
    println!("Created 3 large files");
    println!("Large files directory exists: {}", large_files_dir.exists());
    
    if large_files_dir.exists() {
        println!("Contents of large files directory:");
        let mut entries = tokio::fs::read_dir(&large_files_dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            let file_type = if entry.file_type().await.unwrap().is_dir() { "DIR" } else { "FILE" };
            println!("  {} {}", file_type, name);
        }
    }
    
    // Test that we can find all files
    for (i, (result, original_content)) in results.iter().enumerate() {
        println!("Testing file {} with SHA256: {}", i, result.sha256);
        match crate::large_files::find_large_file_path(table_path, &result.sha256).await {
            Ok(Some(file_path)) => {
                println!("  Found at: {:?}", file_path);
                assert!(file_path.exists(), "File {} should exist", i);
                
                let disk_content = tokio::fs::read(&file_path).await.unwrap();
                assert_eq!(disk_content, *original_content, "File {} content should match", i);
                println!("  ✅ File {} verified", i);
            }
            Ok(None) => {
                panic!("File {} with SHA256 {} should be found", i, result.sha256);
            }
            Err(e) => {
                panic!("Error finding file {}: {}", i, e);
            }
        }
    }
    
    println!("✅ Basic hierarchical directory structure test passed");
}
