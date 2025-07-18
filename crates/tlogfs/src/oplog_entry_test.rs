use super::schema::{OplogEntry, ForArrow, encode_versioned_directory_entries};
use serde_arrow;
use chrono::Utc;
use tinyfs::NodeID;

#[test]
fn test_oplog_entry_with_empty_content() {
    println!("Testing OplogEntry with empty content...");
    
    // Create a root directory entry just like in create_oplog_table
    let root_node_id = NodeID::root().to_string();
    let now = Utc::now().timestamp_micros();
    
    // First, test if encode_versioned_directory_entries works with empty vec
    let empty_content_result = encode_versioned_directory_entries(&vec![]);
    println!("Empty content encoding result: {:?}", empty_content_result);
    
    match empty_content_result {
        Ok(content) => {
            println!("Empty content encoded successfully, {} bytes", content.len());
            
            // Now test OplogEntry with this content
            let root_entry = OplogEntry {
                part_id: root_node_id.clone(),
                node_id: root_node_id.clone(),
                file_type: tinyfs::EntryType::Directory,
                content: content,
                timestamp: now,
                version: 1,
            };
            
            println!("OplogEntry created successfully");
            
            // Test serialization to record batch
            let batch_result = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[root_entry]);
            match batch_result {
                Ok(batch) => {
                    println!("SUCCESS: OplogEntry batch created with {} rows", batch.num_rows());
                }
                Err(e) => {
                    println!("ERROR: OplogEntry batch failed: {}", e);
                    println!("Error debug: {:?}", e);
                    panic!("OplogEntry batch test failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("ERROR: Empty content encoding failed: {}", e);
            panic!("Empty content encoding failed: {}", e);
        }
    }
}
