// Example of Phase 4: Two-Layer Architecture Usage
//
// This example demonstrates how to use the new OpLogPersistence with
// the simplified two-layer FS architecture from the refactoring plan.

use oplog::tinylogfs::create_oplog_fs;
use std::error::Error;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a temporary directory for this example
    let temp_dir = TempDir::new()?;
    let store_path = temp_dir.path().join("example_store");
    let store_path_str = store_path.to_string_lossy();

    println!("Phase 4 Example: Two-Layer Architecture");
    println!("Store path: {}", store_path_str);

    // Create filesystem using the new factory function
    println!("\n1. Creating filesystem with OpLogPersistence layer...");
    let fs = create_oplog_fs(&store_path_str).await?;
    println!("✅ Filesystem created successfully");

    // Get root directory
    println!("\n2. Accessing root directory...");
    let root = fs.root().await?;
    println!("✅ Root directory accessed");

    // Create a file
    println!("\n3. Creating a file...");
    let content = b"Hello from Phase 4 two-layer architecture!";
    let _file = root.create_file_path("example.txt", content).await?;
    println!("✅ File created: example.txt");

    // Read the file back
    println!("\n4. Reading file back...");
    let read_content = root.read_file_path("example.txt").await?;
    println!("✅ File content: {}", String::from_utf8_lossy(&read_content));

    // Commit changes to Delta Lake
    println!("\n5. Committing changes to Delta Lake...");
    fs.commit().await?;
    println!("✅ Changes committed to persistent storage");

    // Create a directory
    println!("\n6. Creating a directory...");
    let _dir = root.create_dir_path("subdir").await?;
    println!("✅ Directory created: subdir");

    // Create a file in the subdirectory
    println!("\n7. Creating file in subdirectory...");
    let subdir = root.open_dir_path("subdir").await?;
    let _subfile = subdir.create_file_path("nested.txt", b"Nested file content").await?;
    println!("✅ Nested file created: subdir/nested.txt");

    // Final commit
    println!("\n8. Final commit...");
    fs.commit().await?;
    println!("✅ All changes committed");

    println!("\n🎉 Phase 4 example completed successfully!");
    println!("✨ The two-layer architecture (FS + OpLogPersistence) is working correctly");

    Ok(())
}
