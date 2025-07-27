use tempfile::tempdir;

// Useful test command:
//
// sudo CARGO_PROFILE_BENCH_DEBUG=true
// cargo flamegraph --test  integration_tests -- test_complex_multipartition_wildcard_patterns

// Import the command functions directly
use cmd::commands::{init, copy, show, mkdir, list};
use cmd::common::{FilesystemChoice, ShipContext};
use tinyfs::async_helpers::convenience;

/// Setup a test environment with a temporary pond
fn setup_test_pond() -> Result<(tempfile::TempDir, std::path::PathBuf), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    
    Ok((tmp, pond_path))
}

/// Create test files in a temporary directory
fn create_test_files(dir: &std::path::Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file1_path = dir.join("file1.txt");
    let file2_path = dir.join("file2.txt");
    let file3_path = dir.join("file3.txt");

    std::fs::write(&file1_path, "Content of file1")?;
    std::fs::write(&file2_path, "Content of file2")?;
    std::fs::write(&file3_path, "Content of file3")?;

    Ok(vec![
        file1_path.to_string_lossy().to_string(),
        file2_path.to_string_lossy().to_string(),
        file3_path.to_string_lossy().to_string(),
    ])
}

// Test helper functions that provide empty args for testing
async fn init_command_with_pond(pond_path: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    let args = vec!["pond".to_string(), "init".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    init::init_command(&ship_context).await
}

/// Helper function for tests to list files and return paths as a vector
async fn list_files_for_test(pattern: &str, show_all: bool, pond_path: Option<std::path::PathBuf>) -> anyhow::Result<Vec<String>> {
    let args = vec!["pond".to_string(), "list".to_string(), pattern.to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    
    let mut results = Vec::new();
    list::list_command(&ship_context, pattern, show_all, FilesystemChoice::Data, |output| {
        // Extract the path from the formatted output
        // Format: "emoji size hash version timestamp /path"
        // The path is the last part that starts with "/"
        let trimmed = output.trim();
        if let Some(path_start) = trimmed.rfind(" /") {
            let path = &trimmed[path_start + 1..]; // +1 to skip the space
            results.push(path.to_string());
        }
    }).await?;
    
    Ok(results)
}

/// Helper function for tests to show pond contents and return output as a string
async fn show_for_test(pond_path: Option<std::path::PathBuf>, filesystem: FilesystemChoice) -> anyhow::Result<String> {
    let args = vec!["pond".to_string(), "show".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    
    let mut result = String::new();
    show::show_command(&ship_context, filesystem, |output| {
        result = output;
    }).await?;
    
    Ok(result)
}

/// Batch setup function that performs all mkdir and copy operations in a single transaction
/// This is much faster than doing individual commits for each operation
async fn batch_setup_directories_and_files(
    directories: &[&str], 
    file_operations: &[(String, &str)], // (source_file_path, destination_path)
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec!["pond".to_string(), "batch_setup".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    let mut ship = ship_context.create_ship_with_transaction().await?;
    
    // Get the data filesystem from ship
    let fs = ship.data_fs();
    let root = fs.root().await?;
    
    // Perform all mkdir operations without committing
    for dir in directories {
        root.create_dir_path(dir).await?;
    }
    
    // Perform all copy operations without committing
    for (source_file, dest_path) in file_operations {
        let content = std::fs::read(source_file)
            .map_err(|e| anyhow::anyhow!("Failed to read '{}': {}", source_file, e))?;
        
        let copy_result = root.resolve_copy_destination(dest_path).await;
        match copy_result {
            Ok((dest_wd, dest_type)) => {
                match dest_type {
                    tinyfs::CopyDestination::Directory | tinyfs::CopyDestination::ExistingDirectory => {
                        let source_path = std::path::Path::new(source_file);
                        let filename = source_path.file_name()
                            .ok_or_else(|| anyhow::anyhow!("Cannot determine filename from source path: {}", source_file))?
                            .to_string_lossy()
                            .to_string();
                        convenience::create_file_path(&dest_wd, &filename, &content).await?;
                    }
                    tinyfs::CopyDestination::NewPath(name) => {
                        convenience::create_file_path(&dest_wd, &name, &content).await?;
                    }
                    tinyfs::CopyDestination::ExistingFile => {
                        return Err(anyhow::anyhow!("Destination '{}' exists but is not a directory", dest_path));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to resolve destination '{}': {}", dest_path, e));
            }
        }
    }
    
    // Single commit for all operations
    ship.commit_transaction().await?;
    Ok(())
}

async fn copy_command_with_pond(sources: &[String], dest: &str, pond_path: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    let args = vec!["pond".to_string(), "copy".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    let ship = ship_context.create_ship_with_transaction().await?;
    copy::copy_command(ship, sources, dest, "auto").await
}

async fn mkdir_command_with_pond(path: &str, pond_path: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    let args = vec!["pond".to_string(), "mkdir".to_string(), path.to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    let ship = ship_context.create_ship_with_transaction().await?;
    mkdir::mkdir_command(ship, path).await
}

#[tokio::test]
async fn test_init_and_show_direct() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, pond_path) = setup_test_pond()?;

    // Test init command directly
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Verify the data and control directories were created (steward architecture)
    let data_path = pond_path.join("data");
    let control_path = pond_path.join("control");
    assert!(data_path.exists(), "Data directory was not created");
    assert!(control_path.exists(), "Control directory was not created");

    // Test show command directly
    let show_output = show_for_test(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    
    // Basic checks on show output - should have at least one transaction
    assert!(show_output.contains("Transaction"), "Expected transaction output");
    assert!(show_output.contains("Directory"), "Expected directory entry");

    // Test that init fails if run again
    let init_result = init_command_with_pond(Some(pond_path)).await;
    assert!(init_result.is_err(), "Init should fail when pond already exists");

    Ok(())
}

#[tokio::test]
async fn test_show_without_init_direct() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("nonexistent_pond");

    // Test show command on non-existent pond - this should now work 
    // as steward auto-creates minimal structure
    
    let show_result = show_for_test(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    
    // We should get a transaction with "No metadata" indicating auto-created structure
    assert!(show_result.contains("No metadata"), "Expected 'No metadata' in show output for auto-created pond, got: {}", show_result);
    assert!(show_result.contains("empty"), "Expected 'empty' directory in show output for auto-created pond, got: {}", show_result);

    Ok(())
}

#[tokio::test]
async fn test_copy_command_atomic_direct() -> Result<(), Box<dyn std::error::Error>> {
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create test files
    let file_paths = create_test_files(&temp_files_dir)?;

    println!("=== TESTING ATOMIC COPY WITH DIRECT FUNCTIONS ===");

    // Step 1: Initialize pond
    println!("1. Initializing pond...");
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Step 2: Copy 3 files to pond root atomically
    println!("2. Copying files atomically...");
    copy_command_with_pond(&file_paths, "/", Some(pond_path.clone())).await?;

    // === BEHAVIOR-FOCUSED VERIFICATION ===
    
    // R1: Verify exactly 3 files exist in the filesystem (atomicity: all or nothing)
    let file_list = list_files_for_test("/*", false, Some(pond_path.clone())).await?;
    println!("=== Files in filesystem: {} ===", file_list.len());
    for file in &file_list {
        println!("  {}", file);
    }
    
    // Count each expected file (should be exactly 1 of each)
    let file1_count = file_list.iter().filter(|f| f.contains("file1.txt")).count();
    let file2_count = file_list.iter().filter(|f| f.contains("file2.txt")).count();
    let file3_count = file_list.iter().filter(|f| f.contains("file3.txt")).count();
    
    assert_eq!(file1_count, 1, "file1.txt should appear exactly once");
    assert_eq!(file2_count, 1, "file2.txt should appear exactly once");
    assert_eq!(file3_count, 1, "file3.txt should appear exactly once");
    assert_eq!(file_list.len(), 3, "Should have exactly 3 files total");
    
    // R2: Verify file contents are correct and accessible (no corruption)
    println!("=== Verifying file contents ===");
    
    let cat1_output = cat_command_with_pond("/file1.txt", Some(pond_path.clone())).await?;
    let cat2_output = cat_command_with_pond("/file2.txt", Some(pond_path.clone())).await?;
    let cat3_output = cat_command_with_pond("/file3.txt", Some(pond_path.clone())).await?;
    
    let cat1_str = String::from_utf8(cat1_output)?;
    let cat2_str = String::from_utf8(cat2_output)?;
    let cat3_str = String::from_utf8(cat3_output)?;
    
    assert_eq!(cat1_str.trim(), "Content of file1", "file1.txt should have correct content");
    assert_eq!(cat2_str.trim(), "Content of file2", "file2.txt should have correct content");
    assert_eq!(cat3_str.trim(), "Content of file3", "file3.txt should have correct content");
    
    println!("✅ All tests passed: Atomic copy working correctly");

    Ok(())
}

#[tokio::test]
async fn test_copy_single_file_direct() -> Result<(), Box<dyn std::error::Error>> {
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create one test file
    let file_path = temp_files_dir.join("single_file.txt");
    std::fs::write(&file_path, "Single file content")?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Copy single file to new name
    copy_command_with_pond(&[file_path.to_string_lossy().to_string()], "renamed_file.txt", Some(pond_path.clone())).await?;

    // Verify with show
    let show_output = show_for_test(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    assert!(show_output.contains("renamed_file.txt"));
    assert!(show_output.contains("Single file content"));

    Ok(())
}

#[tokio::test]
async fn test_copy_to_directory_direct() -> Result<(), Box<dyn std::error::Error>> {
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    let file_paths = create_test_files(&temp_files_dir)?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Copy files to root directory (trailing slash indicates directory)
    copy_command_with_pond(&file_paths, "/", Some(pond_path.clone())).await?;

    // Verify all files are in the pond
    let show_output = show_for_test(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    assert!(show_output.contains("file1.txt"));
    assert!(show_output.contains("file2.txt"));
    assert!(show_output.contains("file3.txt"));

    Ok(())
}

#[tokio::test]
async fn test_copy_multiple_files_to_nonexistent_fails_direct() -> Result<(), Box<dyn std::error::Error>> {
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    let file_paths = create_test_files(&temp_files_dir)?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Try to copy multiple files to non-existent destination - should fail
    let copy_result = copy_command_with_pond(&file_paths, "nonexistent_destination", Some(pond_path)).await;
    assert!(copy_result.is_err(), "Copying multiple files to non-existent destination should fail");

    Ok(())
}

#[tokio::test]
async fn test_complex_multipartition_wildcard_patterns() -> Result<(), Box<dyn std::error::Error>> {
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    println!("=== COMPLEX MULTIPARTITION WILDCARD PATTERN TEST ===");
    
    // Create diverse test files with different extensions
    let test_files = vec![
        ("config.txt", "config data"),
        ("README.md", "readme content"),
        ("app.log", "log entry 1"),
        ("setup.sh", "script content"),
        ("data.bin", "binary data"),
        ("config.json", "json config"),
        ("data.xml", "xml data"),
        ("export.csv", "csv data"),
        ("temp.tmp", "temporary file"),
        ("backup.bak", "backup file"),
        ("database.db", "database file"),
        ("archive.tar", "archive file"),
        ("image.png", "image data"),
        ("style.css", "stylesheet"),
        ("script.js", "javascript"),
    ];

    let mut file_paths = Vec::new();
    for (filename, content) in &test_files {
        let file_path = temp_files_dir.join(filename);
        std::fs::write(&file_path, content)?;
        file_paths.push(file_path.to_string_lossy().to_string());
    }

    println!("✓ Created {} test files", test_files.len());

    // Step 1: Initialize pond
    println!("1. Initializing pond...");
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Step 2 & 3: Create directory structure and distribute files in a single transaction
    println!("2. Creating complex directory structure and distributing files...");
    let directories = vec![
        "/projects",           // Creates partition 1
        "/configs",           // Creates partition 2  
        "/logs",              // Creates partition 3
        "/backups",           // Creates partition 4
        "/temp",              // Creates partition 5
        "/projects/web",      // Subdirectory in partition 1
        "/projects/mobile",   // Subdirectory in partition 1
        "/projects/desktop",  // Subdirectory in partition 1
        "/configs/app",       // Subdirectory in partition 2
        "/configs/db",        // Subdirectory in partition 2
        "/logs/error",        // Subdirectory in partition 3
        "/logs/access",       // Subdirectory in partition 3
        "/backups/daily",     // Subdirectory in partition 4
        "/backups/weekly",    // Subdirectory in partition 4
    ];

    let file_operations = vec![
        // Root partition files
        (file_paths[1].clone(), "/"),                      // README.md
        (file_paths[3].clone(), "/"),                      // setup.sh
        
        // Projects partition files
        (file_paths[0].clone(), "/projects/"),             // config.txt
        (file_paths[6].clone(), "/projects/"),             // data.xml
        (file_paths[5].clone(), "/projects/web/"),         // config.json
        (file_paths[4].clone(), "/projects/mobile/"),      // data.bin
        (file_paths[7].clone(), "/projects/desktop/"),     // export.csv
        (file_paths[14].clone(), "/projects/web/"),        // script.js
        (file_paths[13].clone(), "/projects/web/"),        // style.css
        
        // Configs partition files
        (file_paths[0].clone(), "/configs/app/"),          // config.txt
        (file_paths[5].clone(), "/configs/db/"),           // config.json
        (file_paths[10].clone(), "/configs/db/"),          // database.db
        
        // Logs partition files
        (file_paths[2].clone(), "/logs/"),                 // app.log
        (file_paths[2].clone(), "/logs/error/"),           // app.log
        (file_paths[2].clone(), "/logs/access/"),          // app.log
        
        // Backups partition files
        (file_paths[9].clone(), "/backups/daily/"),        // backup.bak
        (file_paths[9].clone(), "/backups/weekly/"),       // backup.bak
        (file_paths[11].clone(), "/backups/daily/"),       // archive.tar
        
        // Temp partition files
        (file_paths[8].clone(), "/temp/"),                 // temp.tmp
        (file_paths[12].clone(), "/temp/"),                // image.png
    ];

    // Perform all operations in a single transaction for much better performance
    batch_setup_directories_and_files(&directories, &file_operations, Some(pond_path.clone())).await?;
    
    println!("✓ Created {} directories and distributed {} files across 5 partitions (single transaction)", 
             directories.len(), file_operations.len());

    // Step 4: Test comprehensive wildcard patterns using efficient cached reads
    println!("4. Testing wildcard patterns (reusing ship context for performance)...");
    
    // Create a single ship context for all read operations to benefit from caching
    let ship = steward::Ship::open_existing_pond(&pond_path).await?;
    let fs = ship.data_fs();
    let root = fs.root().await?;
    
    // Test 1: List all files recursively  
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut all_files_info = root.visit_with_visitor("/**", &mut visitor).await
        .map_err(|e| anyhow::anyhow!("Failed to list files matching '/**': {}", e))?;
    all_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let all_files: Vec<String> = all_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ All files (/**): {} matches", all_files.len());
    assert!(all_files.len() >= 20, "Should find at least 20 files");
    
    // Test 2: List only txt files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut txt_files_info = root.visit_with_visitor("/**/*.txt", &mut visitor).await
        .map_err(|e| anyhow::anyhow!("Failed to list txt files: {}", e))?;
    txt_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let txt_files: Vec<String> = txt_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ TXT files (**/*.txt): {} matches", txt_files.len());
    assert!(txt_files.len() >= 2, "Should find at least 2 txt files");
    assert!(txt_files.iter().all(|f| f.ends_with(".txt")), "All results should be .txt files");
    
    // Test 3: List config files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut config_files_info = root.visit_with_visitor("**/config.*", &mut visitor).await
        .map_err(|e| anyhow::anyhow!("Failed to list config files: {}", e))?;
    config_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let config_files: Vec<String> = config_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Config files (**/config.*): {} matches", config_files.len());
    assert!(config_files.len() >= 3, "Should find at least 3 config files");
    
    // Test 4: List files in projects directory
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut project_files_info = root.visit_with_visitor("/projects/**", &mut visitor).await
        .map_err(|e| anyhow::anyhow!("Failed to list project files: {}", e))?;
    project_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let project_files: Vec<String> = project_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Project files (/projects/**): {} matches", project_files.len());
    assert!(project_files.len() >= 6, "Should find at least 6 project files");
    
    // Test 5: List log files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut log_files_info = root.visit_with_visitor("/**/*.log", &mut visitor).await?;
    log_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let log_files: Vec<String> = log_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Log files (**/*.log): {} matches", log_files.len());
    assert!(log_files.len() >= 3, "Should find at least 3 log files");
    
    // Test 6: List backup files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut backup_files_info = root.visit_with_visitor("/**/*.bak", &mut visitor).await?;
    backup_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let backup_files: Vec<String> = backup_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Backup files (**/*.bak): {} matches", backup_files.len());
    assert!(backup_files.len() >= 2, "Should find at least 2 backup files");
    
    // Test 7: List files in root directory only
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut root_files_info = root.visit_with_visitor("/*", &mut visitor).await?;
    root_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let root_files: Vec<String> = root_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Root files (/*): {} matches", root_files.len());
    // Note: This includes directories too, so we expect more than just files
    
    // Test 8: List web project files specifically
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut web_files_info = root.visit_with_visitor("/projects/web/**", &mut visitor).await?;
    web_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let web_files: Vec<String> = web_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Web project files (/projects/web/**): {} matches", web_files.len());
    assert!(web_files.len() >= 3, "Should find at least 3 web project files");
    
    // Test 9: List database files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut db_files_info = root.visit_with_visitor("/**/*.db", &mut visitor).await?;
    db_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let db_files: Vec<String> = db_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Database files (**/*.db): {} matches", db_files.len());
    assert!(db_files.len() >= 1, "Should find at least 1 database file");
    
    // Test 10: List files in specific partition directories
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut config_partition_files_info = root.visit_with_visitor("/configs/**", &mut visitor).await?;
    config_partition_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let config_partition_files: Vec<String> = config_partition_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Config partition files (/configs/**): {} matches", config_partition_files.len());
    assert!(config_partition_files.len() >= 3, "Should find at least 3 config partition files");
    
    // Test 11: List temporary files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut temp_files_info = root.visit_with_visitor("/**/*.tmp", &mut visitor).await?;
    temp_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let temp_files: Vec<String> = temp_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Temporary files (**/*.tmp): {} matches", temp_files.len());
    assert!(temp_files.len() >= 1, "Should find at least 1 temporary file");
    
    // Test 12: List archive files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut archive_files_info = root.visit_with_visitor("/**/*.tar", &mut visitor).await?;
    archive_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let archive_files: Vec<String> = archive_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Archive files (**/*.tar): {} matches", archive_files.len());
    assert!(archive_files.len() >= 1, "Should find at least 1 archive file");
    
    // Test 13: List image files
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut image_files_info = root.visit_with_visitor("/**/*.png", &mut visitor).await?;
    image_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let image_files: Vec<String> = image_files_info.into_iter().map(|info| info.path).collect();
    println!("✓ Image files (**/*.png): {} matches", image_files.len());
    assert!(image_files.len() >= 1, "Should find at least 1 image file");
    
    // Test 14: List web assets (css, js)
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut css_files_info = root.visit_with_visitor("/**/*.css", &mut visitor).await?;
    css_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let css_files: Vec<String> = css_files_info.into_iter().map(|info| info.path).collect();
    
    let mut visitor = cmd::common::FileInfoVisitor::new(false);
    let mut js_files_info = root.visit_with_visitor("/**/*.js", &mut visitor).await?;
    js_files_info.sort_by(|a, b| a.path.cmp(&b.path));
    let js_files: Vec<String> = js_files_info.into_iter().map(|info| info.path).collect();
    
    println!("✓ CSS files (**/*.css): {} matches", css_files.len());
    println!("✓ JS files (**/*.js): {} matches", js_files.len());
    assert!(css_files.len() >= 1, "Should find at least 1 CSS file");
    assert!(js_files.len() >= 1, "Should find at least 1 JS file");

    println!("=== PARTITION VERIFICATION ===");
    
    // Verify that files are distributed across multiple partitions
    // We should have files in:
    // 1. Root partition: README.md, setup.sh
    // 2. Projects partition: config.txt, data.xml, and subdirectory files
    // 3. Configs partition: config files in app/ and db/
    // 4. Logs partition: log files in error/ and access/
    // 5. Backups partition: backup files in daily/ and weekly/
    // 6. Temp partition: temporary files
    
    let partitions_tested = vec![
        ("root", "/*", 2),  // At least 2 files in root + directories
        ("projects", "/projects/**", 6),  // At least 6 files in projects partition
        ("configs", "/configs/**", 3),   // At least 3 files in configs partition
        ("logs", "/logs/**", 3),         // At least 3 files in logs partition
        ("backups", "/backups/**", 3),   // At least 3 files in backups partition
        ("temp", "/temp/**", 2),         // At least 2 files in temp partition
    ];
    
    for (partition_name, pattern, min_expected) in partitions_tested {
        let mut visitor = cmd::common::FileInfoVisitor::new(false);
        let mut partition_files_info = root.visit_with_visitor(pattern, &mut visitor).await?;
        partition_files_info.sort_by(|a, b| a.path.cmp(&b.path));
        let partition_files: Vec<String> = partition_files_info.into_iter().map(|info| info.path).collect();
        println!("✓ Partition '{}' ({}): {} files (expected >= {})", 
                 partition_name, pattern, partition_files.len(), min_expected);
        assert!(partition_files.len() >= min_expected, 
                "Partition '{}' should have at least {} files", partition_name, min_expected);
    }
    
    println!("=== COMPLEX WILDCARD PATTERN TEST SUMMARY ===");
    println!("✓ Created pond with complex directory structure");
    println!("✓ Tested 6 main partitions (root + 5 subdirectories)");
    println!("✓ Distributed 20 files across multiple partitions");
    println!("✓ Successfully tested 14 different wildcard patterns");
    println!("✓ Verified cross-partition file operations");
    println!("✓ Demonstrated tlogfs partition system with transactions");
    
    Ok(())
}

#[tokio::test]
async fn test_mkdir_and_copy_basic() -> Result<(), anyhow::Error> {
    let (tmp, pond_path) = setup_test_pond().map_err(|e| anyhow::anyhow!("Setup failed: {}", e))?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir).map_err(|e| anyhow::anyhow!("Failed to create temp dir: {}", e))?;

    println!("=== BASIC MKDIR AND COPY TEST ===");
    
    // Create a simple test file
    let test_file = temp_files_dir.join("test.txt");
    std::fs::write(&test_file, "test content").map_err(|e| anyhow::anyhow!("Failed to write test file: {}", e))?;
    let test_file_path = test_file.to_string_lossy().to_string();

    println!("✓ Created test file: {}", test_file_path);

    // Step 1: Initialize pond
    println!("1. Initializing pond...");
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Step 2: Create a directory
    println!("2. Creating directory...");
    mkdir_command_with_pond("/testdir", Some(pond_path.clone())).await?;

    // Step 3: List to verify directory was created
    println!("3. Listing root directory...");
    let files = list_files_for_test("/*", false, Some(pond_path.clone())).await?;
    println!("Files in root: {:?}", files);
    
    // Step 4: Try to copy file to the directory
    println!("4. Copying file to directory...");
    let copy_result = copy_command_with_pond(&[test_file_path], "/testdir/", Some(pond_path.clone())).await;
    match copy_result {
        Ok(()) => println!("✓ Copy successful"),
        Err(e) => {
            println!("✗ Copy failed: {}", e);
            return Err(e);
        }
    }
    
    // Step 5: List to verify file was copied
    println!("5. Listing directory contents...");
    
    // Wait a moment to ensure the transaction is fully committed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let dir_files = list_files_for_test("/testdir/**", false, Some(pond_path.clone())).await?;
    println!("Files in testdir: {:?}", dir_files);
    
    // Also try listing all files to see what's in the pond
    let all_files = list_files_for_test("/**", false, Some(pond_path.clone())).await?;
    println!("All files in pond: {:?}", all_files);
    
    assert!(dir_files.contains(&"/testdir/test.txt".to_string()), "File should be copied to directory");
    
    println!("✓ Test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_large_file_copy_correctness_non_utf8() -> Result<(), Box<dyn std::error::Error>> {
    use sha2::{Sha256, Digest};
    
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create a large binary file with non-UTF8 data (>64 KiB to trigger large file storage)
    let large_file_path = temp_files_dir.join("large_binary.bin");
    let large_file_size = 64 * 1024 + 256; // 64.25 KiB - just enough to trigger large file storage
    
    // Generate non-UTF8 binary data with patterns that could be corrupted by UTF-8 conversion
    let mut large_content = vec![0u8; large_file_size];
    
    // Fill with pattern that includes all problematic byte values efficiently
    for (i, byte) in large_content.iter_mut().enumerate() {
        *byte = match i % 256 {
            // Include all possible byte values including UTF-8 problematic ones
            0x80..=0xFF => (i % 256) as u8, // High-bit bytes that are not valid UTF-8
            0x00..=0x1F => (i % 32) as u8,  // Control characters
            _ => ((i * 37) % 256) as u8,    // Pseudo-random pattern
        };
    }
    
    // Ensure we have some specific problematic sequences
    // Invalid UTF-8 continuation bytes
    large_content[1000] = 0x80;
    large_content[1001] = 0x81;
    large_content[1002] = 0x82;
    // Invalid UTF-8 start bytes
    large_content[2000] = 0xFF;
    large_content[2001] = 0xFE;
    large_content[2002] = 0xFD;
    // Null bytes
    large_content[3000] = 0x00;
    large_content[3001] = 0x00;
    large_content[3002] = 0x00;
    
    std::fs::write(&large_file_path, &large_content)?;
    
    // Calculate original SHA256 for verification
    let mut hasher = Sha256::new();
    hasher.update(&large_content);
    let original_sha256 = hasher.finalize();

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Copy the large binary file to the pond
    let large_file_str = large_file_path.to_string_lossy().to_string();
    copy_command_with_pond(&[large_file_str], "/large_binary.bin", Some(pond_path.clone())).await?;

    // Verify the file exists in the pond
    let show_output = show_for_test(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    assert!(show_output.contains("large_binary.bin"), "Large binary file should appear in pond listing");

    // Read the file back using cat command and verify content integrity
    let retrieved_content = cat_command_with_pond("/large_binary.bin", Some(pond_path.clone())).await?;
    
    // Calculate retrieved SHA256
    let mut hasher = Sha256::new();
    hasher.update(&retrieved_content);
    let retrieved_sha256 = hasher.finalize();
    
    // Verify size matches exactly
    assert_eq!(large_content.len(), retrieved_content.len(), 
               "Retrieved file size should match original exactly");
    
    // Verify SHA256 matches exactly (ensures no corruption)
    assert_eq!(original_sha256.as_slice(), retrieved_sha256.as_slice(),
               "SHA256 checksums should match exactly - no corruption allowed");
    
    // Verify byte-for-byte equality
    assert_eq!(large_content, retrieved_content,
               "File contents should be identical byte-for-byte");
    
    // Verify specific problematic bytes are preserved
    assert_eq!(retrieved_content[1000], 0x80, "Invalid UTF-8 continuation byte should be preserved");
    assert_eq!(retrieved_content[1001], 0x81, "Invalid UTF-8 continuation byte should be preserved");
    assert_eq!(retrieved_content[2000], 0xFF, "Invalid UTF-8 start byte should be preserved");
    assert_eq!(retrieved_content[2001], 0xFE, "Invalid UTF-8 start byte should be preserved");
    assert_eq!(retrieved_content[3000], 0x00, "Null byte should be preserved");
    assert_eq!(retrieved_content[3001], 0x00, "Null byte should be preserved");

    Ok(())
}

/// Helper function to create cat command for testing
async fn cat_command_with_pond(path: &str, pond_path: Option<std::path::PathBuf>) -> anyhow::Result<Vec<u8>> {
    let args = vec!["pond".to_string(), "cat".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    
    let ship = ship_context.create_ship().await?;
    let fs = ship.data_fs();
    let root = fs.root().await?;
    
    match root.async_reader_path(path).await {
        Ok(reader) => {
            let content = tinyfs::buffer_helpers::read_all_to_vec(reader).await
                .map_err(|e| anyhow::anyhow!("Failed to read file content: {}", e))?;
            Ok(content)
        },
        Err(e) => Err(anyhow::anyhow!("Failed to read file '{}': {}", path, e)),
    }
}

#[tokio::test]
async fn test_small_and_large_file_boundary() -> Result<(), Box<dyn std::error::Error>> {
    use sha2::{Sha256, Digest};
    
    let (tmp, pond_path) = setup_test_pond()?;
    let temp_files_dir = tmp.path().join("temp_files");
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create files at the boundary of large file threshold (64 KiB = 65,536 bytes)
    let sizes_to_test = vec![
        ("small_file.bin", 65535),      // 1 byte under threshold
        ("exact_threshold.bin", 65536), // Exactly at threshold  
        ("large_file.bin", 65537),      // 1 byte over threshold
    ];
    
    let mut test_files = Vec::new();
    let mut expected_checksums = Vec::new();
    
    for (filename, size) in sizes_to_test {
        let file_path = temp_files_dir.join(filename);
        
        // Generate binary content with patterns
        let mut content = Vec::with_capacity(size);
        for i in 0..size {
            content.push(((i * 7) % 256) as u8); // Simple but varied pattern
        }
        
        // Add some non-UTF8 bytes
        if size > 1000 {
            content[500] = 0xFF;
            content[501] = 0x80;
            content[502] = 0x00;
        }
        
        std::fs::write(&file_path, &content)?;
        
        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let checksum = hasher.finalize();
        
        test_files.push((filename.to_string(), file_path.to_string_lossy().to_string(), content));
        expected_checksums.push(checksum);
        
        println!("Created {}: {} bytes", filename, size);
    }

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Copy all files to pond
    for (filename, file_path, _) in &test_files {
        let dest_path = format!("/{}", filename);
        copy_command_with_pond(&[file_path.clone()], &dest_path, Some(pond_path.clone())).await?;
    }

    // Verify all files and their integrity
    for (i, (filename, _, original_content)) in test_files.iter().enumerate() {
        let path = format!("/{}", filename);
        let retrieved_content = cat_command_with_pond(&path, Some(pond_path.clone())).await?;
        
        // Calculate retrieved checksum
        let mut hasher = Sha256::new();
        hasher.update(&retrieved_content);
        let retrieved_checksum = hasher.finalize();
        
        assert_eq!(original_content.len(), retrieved_content.len(),
                   "File {} size should be preserved", filename);
        assert_eq!(expected_checksums[i].as_slice(), retrieved_checksum.as_slice(),
                   "File {} checksum should match", filename);
        assert_eq!(original_content, &retrieved_content,
                   "File {} content should be identical", filename);
        
        println!("✅ {} integrity verified", filename);
    }
    
    println!("✅ All boundary size files preserved correctly");
    Ok(())
}
