// Temporary test to verify directory entry schema fix
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Clean up any existing test data
    let test_dir = "/tmp/schema_test_clean";
    if Path::new(test_dir).exists() {
        std::fs::remove_dir_all(test_dir)?;
    }
    
    // Create fresh filesystem
    let fs = oplog::tinylogfs::create_oplog_fs(test_dir).await?;
    let working_dir = fs.root().await?;
    
    println!("✅ Created fresh filesystem");
    
    // Create a directory
    let dir1 = working_dir.create_dir_path("test_dir").await?;
    println!("✅ Created directory");
    
    // Verify it exists
    assert!(working_dir.exists(Path::new("test_dir")).await);
    println!("✅ Directory exists");
    
    // Create a file in the directory  
    let _file = dir1.create_file_path("test.txt", b"test content").await?;
    println!("✅ Created file in directory");
    
    // Verify file exists
    assert!(dir1.exists(Path::new("test.txt")).await);
    println!("✅ File exists in directory");
    
    println!("🎉 All tests passed - schema fix is working!");
    Ok(())
}
