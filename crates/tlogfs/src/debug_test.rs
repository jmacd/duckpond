use tempfile::tempdir;
use std::path::PathBuf;

#[tokio::test]
async fn debug_delta_table_issue() {
    let temp_dir = tempdir().unwrap();
    let table_path = temp_dir.path().join("debug_table");
    let table_uri = table_path.to_string_lossy().to_string();
    
    println!("=== Table path: {} ===", table_uri);
    
    // Step 1: Create table using our schema creation function
    println!("\n=== Creating table ===");
    tlogfs::schema::create_oplog_table(&table_uri).await.unwrap();
    println!("Table creation completed");
    
    // Step 2: Check filesystem immediately
    println!("\n=== Checking filesystem ===");
    if table_path.exists() {
        println!("Table directory exists");
        let log_dir = table_path.join("_delta_log");
        if log_dir.exists() {
            println!("_delta_log directory exists");
            for entry in std::fs::read_dir(&log_dir).unwrap() {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                println!("  File: {} (size: {} bytes)", 
                    entry.file_name().to_string_lossy(),
                    metadata.len()
                );
            }
        } else {
            println!("❌ _delta_log directory does NOT exist");
        }
    } else {
        println!("❌ Table directory does NOT exist");
    }
    
    // Step 3: Try to open with deltalake directly
    println!("\n=== Opening with deltalake::open_table ===");
    match deltalake::open_table(&table_uri).await {
        Ok(table) => {
            println!("✅ Successfully opened table, version: {}", table.version());
            println!("Number of files: {}", table.get_files_count());
        }
        Err(e) => {
            println!("❌ Failed to open table: {}", e);
        }
    }
    
    // Step 4: Try with DeltaTableManager 
    println!("\n=== Opening with DeltaTableManager ===");
    let manager = tlogfs::delta::manager::DeltaTableManager::new();
    match manager.get_table_for_read(&table_uri).await {
        Ok(table) => {
            println!("✅ Successfully opened with manager, version: {}", table.version());
        }
        Err(e) => {
            println!("❌ Failed to open with manager: {}", e);
        }
    }
}
