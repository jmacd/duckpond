use serde_arrow;
use serde::{Deserialize, Serialize};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEntry {
    pub name: String,
    pub value: i64,
}

#[test]
fn test_empty_vector_with_serde_arrow() {
    let fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Int64, false)),
    ];

    println!("Testing empty vector with serde_arrow...");
    
    // Test 1: Empty vector
    let empty_data: Vec<TestEntry> = vec![];
    let result = serde_arrow::to_record_batch(&fields, &empty_data);
    match result {
        Ok(batch) => {
            println!("SUCCESS: Empty vector worked! Batch has {} rows", batch.num_rows());
        }
        Err(e) => {
            println!("ERROR: Empty vector failed: {}", e);
            panic!("Empty vector test failed: {}", e);
        }
    }
    
    // Test 2: Non-empty vector for comparison
    let non_empty_data = vec![TestEntry {
        name: "test".to_string(),
        value: 42,
    }];
    let result = serde_arrow::to_record_batch(&fields, &non_empty_data);
    match result {
        Ok(batch) => {
            println!("SUCCESS: Non-empty vector worked! Batch has {} rows", batch.num_rows());
        }
        Err(e) => {
            println!("ERROR: Non-empty vector failed: {}", e);
            panic!("Non-empty vector test failed: {}", e);
        }
    }
}
