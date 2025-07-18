use serde::{Deserialize, Serialize};
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedDirectoryEntry {
    pub name: String,
    pub child_node_id: String,
    pub operation_type: String,
    pub node_type: String,
    pub version: i64,
}

fn for_arrow() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("child_node_id", DataType::Utf8, false)),
        Arc::new(Field::new("operation_type", DataType::Utf8, false)),
        Arc::new(Field::new("node_type", DataType::Utf8, false)),
        Arc::new(Field::new("version", DataType::Int64, false)),
    ]
}

fn main() {
    let empty_vec: Vec<VersionedDirectoryEntry> = vec![];
    
    // Test 1: Try to serialize empty vector
    println!("Test 1: Serialize empty vector");
    let result = serde_arrow::to_record_batch(&for_arrow(), &empty_vec);
    match result {
        Ok(batch) => {
            println!("Success! Batch has {} rows and {} columns", batch.num_rows(), batch.num_columns());
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
    
    // Test 2: Try to create empty batch manually
    println!("\nTest 2: Create empty batch manually");
    let schema = Arc::new(arrow::datatypes::Schema::new(for_arrow()));
    let empty_batch = arrow::array::RecordBatch::new_empty(schema);
    println!("Manual empty batch has {} rows and {} columns", empty_batch.num_rows(), empty_batch.num_columns());
    
    // Test 3: Try with one item and then empty
    println!("\nTest 3: Serialize single item vector");
    let single_item = vec![VersionedDirectoryEntry {
        name: "test".to_string(),
        child_node_id: "node1".to_string(),
        operation_type: "create".to_string(),
        node_type: "file".to_string(),
        version: 1,
    }];
    
    let result = serde_arrow::to_record_batch(&for_arrow(), &single_item);
    match result {
        Ok(batch) => {
            println!("Success! Batch has {} rows and {} columns", batch.num_rows(), batch.num_columns());
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}
