use deltalake::{DeltaOps, open_table};
use tempfile::tempdir;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let table_path = temp_dir.path().join("debug_table");
    let table_uri = table_path.to_string_lossy();
    
    println!("Table path: {}", table_uri);
    
    // Step 1: Try to open non-existent table
    println!("\n=== Step 1: Try to open non-existent table ===");
    match open_table(&table_uri).await {
        Ok(_) => println!("Unexpectedly found table"),
        Err(e) => println!("Expected error: {}", e),
    }
    
    // Step 2: Create table
    println!("\n=== Step 2: Create table ===");
    let ops = DeltaOps::try_from_uri(&table_uri).await?;
    let table = ops.create()
        .with_columns(vec![
            deltalake::kernel::StructField {
                name: "id".to_string(),
                data_type: deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                nullable: false,
                metadata: std::collections::HashMap::new(),
            }
        ])
        .await?;
    
    println!("Table created successfully, version: {}", table.version());
    
    // Step 3: List files immediately after creation
    println!("\n=== Step 3: Check filesystem immediately after creation ===");
    if table_path.exists() {
        println!("Table directory exists");
        let log_dir = table_path.join("_delta_log");
        if log_dir.exists() {
            println!("_delta_log directory exists");
            for entry in std::fs::read_dir(&log_dir)? {
                let entry = entry?;
                println!("  File: {}", entry.file_name().to_string_lossy());
            }
        } else {
            println!("_delta_log directory does NOT exist");
        }
    } else {
        println!("Table directory does NOT exist");
    }
    
    // Step 4: Try to open immediately after creation
    println!("\n=== Step 4: Try to open immediately after creation ===");
    match open_table(&table_uri).await {
        Ok(table) => println!("Successfully opened table, version: {}", table.version()),
        Err(e) => println!("Failed to open table: {}", e),
    }
    
    // Step 5: Write some data
    println!("\n=== Step 5: Write data to table ===");
    let batch = {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
        ]));
        
        let id_array = StringArray::from(vec!["test1"]);
        RecordBatch::try_new(schema, vec![Arc::new(id_array)])?
    };
    
    let ops = DeltaOps::from(table);
    let table = ops.write(vec![batch])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .await?;
    
    println!("Data written successfully, version: {}", table.version());
    
    // Step 6: List files after writing
    println!("\n=== Step 6: Check filesystem after writing ===");
    let log_dir = table_path.join("_delta_log");
    for entry in std::fs::read_dir(&log_dir)? {
        let entry = entry?;
        println!("  File: {}", entry.file_name().to_string_lossy());
    }
    
    // Step 7: Try to open after writing
    println!("\n=== Step 7: Try to open after writing ===");
    match open_table(&table_uri).await {
        Ok(table) => println!("Successfully opened table, version: {}", table.version()),
        Err(e) => println!("Failed to open table: {}", e),
    }
    
    Ok(())
}
