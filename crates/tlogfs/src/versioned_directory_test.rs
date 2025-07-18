use super::schema::{VersionedDirectoryEntry, ForArrow};
use serde_arrow;

#[test]
fn test_versioned_directory_entry_empty_batch() {
    println!("Testing VersionedDirectoryEntry with empty vector...");
    
    // Test with empty vector
    let empty_entries: Vec<VersionedDirectoryEntry> = vec![];
    let schema = VersionedDirectoryEntry::for_arrow();
    
    println!("Schema: {:?}", schema);
    println!("Empty entries length: {}", empty_entries.len());
    
    let result = serde_arrow::to_record_batch(&schema, &empty_entries);
    match result {
        Ok(batch) => {
            println!("SUCCESS: Empty VersionedDirectoryEntry batch worked! {} rows", batch.num_rows());
        }
        Err(e) => {
            println!("ERROR: Empty VersionedDirectoryEntry batch failed: {}", e);
            println!("Error debug: {:?}", e);
            panic!("Test failed: {}", e);
        }
    }
}
