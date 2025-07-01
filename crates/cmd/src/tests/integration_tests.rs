use assert_cmd::prelude::*;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_pond_init_and_show() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");

    // Get the path to the pond binary
    let mut cmd = Command::cargo_bin("pond")?;

    // Test init command
    let output = cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Check that init succeeded
    assert!(
        output.status.success(),
        "Init command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Initializing pond at:"));
    assert!(stdout.contains("✅ Pond initialized successfully"));

    // Verify the store directory was created
    let store_path = pond_path.join("store");
    assert!(store_path.exists(), "Store directory was not created");

    // Test show command
    let mut show_cmd = Command::cargo_bin("pond")?;
    let show_output = show_cmd
        .arg("show")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Check that show succeeded
    assert!(
        show_output.status.success(),
        "Show command failed: {}",
        String::from_utf8_lossy(&show_output.stderr)
    );

    let show_stdout = String::from_utf8_lossy(&show_output.stdout);
    assert!(show_stdout.contains("=== DuckPond Operation Log ==="));
    assert!(show_stdout.contains("=== Summary ==="));
    assert!(show_stdout.contains("Transactions:"));
    assert!(show_stdout.contains("Entries:"));

    // Test that init fails if run again
    let mut init_again_cmd = Command::cargo_bin("pond")?;
    let init_again_output = init_again_cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Should fail with error
    assert!(
        !init_again_output.status.success(),
        "Init should fail when pond already exists"
    );
    let init_again_stderr = String::from_utf8_lossy(&init_again_output.stderr);
    assert!(init_again_stderr.contains("Pond already exists"));

    Ok(())
}

#[test]
fn test_show_without_init() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("nonexistent_pond");

    // Test show command on non-existent pond
    let mut cmd = Command::cargo_bin("pond")?;
    let output = cmd
        .arg("show")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Should fail with error
    assert!(
        !output.status.success(),
        "Show should fail when pond doesn't exist"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Pond does not exist"));
    assert!(stderr.contains("Run 'pond init' first"));

    Ok(())
}

#[test]
fn test_missing_pond_env() -> Result<(), Box<dyn std::error::Error>> {
    // Test init command without POND environment variable
    let mut cmd = Command::cargo_bin("pond")?;
    let output = cmd.arg("init").env_remove("POND").output()?;

    // Should fail with error
    assert!(
        !output.status.success(),
        "Init should fail when POND env var is not set"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("POND environment variable not set"));

    Ok(())
}

#[test]
fn test_help_command() -> Result<(), Box<dyn std::error::Error>> {
    // Test help command
    let mut cmd = Command::cargo_bin("pond")?;
    let output = cmd.arg("--help").output()?;

    // Should succeed
    assert!(output.status.success(), "Help command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pond"));
    assert!(stdout.contains("init"));
    assert!(stdout.contains("show"));

    Ok(())
}

#[tokio::test]
async fn test_copy_command_unit() -> Result<(), Box<dyn std::error::Error>> {
    use crate::commands::copy::copy_command;
    use tempfile::tempdir;
    use std::env;
    
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    let temp_files_dir = tmp.path().join("temp_files");

    // Create temporary directory for source files
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create 3 temporary files with different content
    let file1_path = temp_files_dir.join("file1.txt");
    let file2_path = temp_files_dir.join("file2.txt");
    let file3_path = temp_files_dir.join("file3.txt");

    std::fs::write(&file1_path, "Content of file1")?;
    std::fs::write(&file2_path, "Content of file2")?;
    std::fs::write(&file3_path, "Content of file3")?;

    // Set up pond environment
    unsafe {
        env::set_var("POND", pond_path.to_string_lossy().to_string());
    }

    // Initialize the pond using the filesystem directly
    let store_path = pond_path.join("store");
    std::fs::create_dir_all(&store_path)?;
    let fs = tinylogfs::create_oplog_fs(&store_path.to_string_lossy()).await?;
    fs.commit().await?; // Initialize empty pond
    
    // Test copying files
    let sources = vec![
        file1_path.to_string_lossy().to_string(),
        file2_path.to_string_lossy().to_string(),
        file3_path.to_string_lossy().to_string(),
    ];
    
    // This should succeed without hanging
    copy_command(&sources, "/").await?;
    
    // Verify the files were copied by checking the filesystem
    let fs = tinylogfs::create_oplog_fs(&store_path.to_string_lossy()).await?;
    let root = fs.root().await?;
    
    // Check that all three files exist in the pond
    use tinyfs::Lookup;
    
    let file1_result = root.in_path("file1.txt", |_wd, lookup| async move {
        match lookup {
            Lookup::Found(_) => Ok(true),
            _ => Ok(false),
        }
    }).await?;
    assert!(file1_result, "file1.txt should exist in pond");
    
    let file2_result = root.in_path("file2.txt", |_wd, lookup| async move {
        match lookup {
            Lookup::Found(_) => Ok(true),
            _ => Ok(false),
        }
    }).await?;
    assert!(file2_result, "file2.txt should exist in pond");
    
    let file3_result = root.in_path("file3.txt", |_wd, lookup| async move {
        match lookup {
            Lookup::Found(_) => Ok(true),
            _ => Ok(false),
        }
    }).await?;
    assert!(file3_result, "file3.txt should exist in pond");

    Ok(())
}

#[test]
fn test_copy_command_integration_debug() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    let temp_files_dir = tmp.path().join("temp_files");

    // Create temporary directory for source files
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create 3 temporary files with different content
    let file1_path = temp_files_dir.join("file1.txt");
    let file2_path = temp_files_dir.join("file2.txt");
    let file3_path = temp_files_dir.join("file3.txt");

    std::fs::write(&file1_path, "Content of file1")?;
    std::fs::write(&file2_path, "Content of file2")?;
    std::fs::write(&file3_path, "Content of file3")?;

    eprintln!("DEBUG: Test setup complete, about to run init");

    // Step 1: Initialize a new pond
    let mut init_cmd = Command::cargo_bin("pond")?;
    let init_output = init_cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    assert!(
        init_output.status.success(),
        "Init command failed: {}",
        String::from_utf8_lossy(&init_output.stderr)
    );

    eprintln!("DEBUG: Init completed successfully, about to run copy");

    // Step 2: Copy 3 temporary files from host FS into pond root
    let mut copy_cmd = Command::cargo_bin("pond")?;
    
    // Add a timeout to the copy command using std::process
    eprintln!("DEBUG: About to execute copy command");
    let copy_output = copy_cmd
        .arg("copy")
        .arg(file1_path.to_string_lossy().to_string())
        .arg(file2_path.to_string_lossy().to_string())
        .arg(file3_path.to_string_lossy().to_string())
        .arg("/")  // Copy to pond root directory
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    eprintln!("DEBUG: Copy command completed");
    eprintln!("DEBUG: Copy stdout: {}", String::from_utf8_lossy(&copy_output.stdout));
    eprintln!("DEBUG: Copy stderr: {}", String::from_utf8_lossy(&copy_output.stderr));

    assert!(
        copy_output.status.success(),
        "Copy command failed: {}",
        String::from_utf8_lossy(&copy_output.stderr)
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_and_show_output() -> Result<(), Box<dyn std::error::Error>> {
    use tempfile::tempdir;
    
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    let temp_files_dir = tmp.path().join("temp_files");

    // Create temporary directory for source files
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create 3 temporary files with different content
    let file1_path = temp_files_dir.join("file1.txt");
    let file2_path = temp_files_dir.join("file2.txt");
    let file3_path = temp_files_dir.join("file3.txt");

    std::fs::write(&file1_path, "Content of file1")?;
    std::fs::write(&file2_path, "Content of file2")?;
    std::fs::write(&file3_path, "Content of file3")?;

    // Step 1: Initialize a new pond using CLI binary
    println!("=== INITIALIZING POND ===");
    let mut init_cmd = Command::cargo_bin("pond")?;
    let init_output = init_cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    assert!(
        init_output.status.success(),
        "Init command failed: {}",
        String::from_utf8_lossy(&init_output.stderr)
    );

    // Step 2: Copy 3 files to pond root using CLI binary
    println!("=== COPYING FILES ===");
    let mut copy_cmd = Command::cargo_bin("pond")?;
    let copy_output = copy_cmd
        .arg("copy")
        .arg(file1_path.to_string_lossy().to_string())
        .arg(file2_path.to_string_lossy().to_string())
        .arg(file3_path.to_string_lossy().to_string())
        .arg("/")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    println!("=== COPY COMMAND OUTPUT ===");
    println!("STDOUT: {}", String::from_utf8_lossy(&copy_output.stdout));
    println!("STDERR: {}", String::from_utf8_lossy(&copy_output.stderr));
    println!("==========================");

    assert!(
        copy_output.status.success(),
        "Copy command failed: {}",
        String::from_utf8_lossy(&copy_output.stderr)
    );

    // Step 3: Get show output using CLI binary
    println!("=== GETTING SHOW OUTPUT ===");
    
    let mut show_cmd = Command::cargo_bin("pond")?;
    let show_output = show_cmd
        .arg("show")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    let show_stdout = String::from_utf8_lossy(&show_output.stdout);
    
    println!("=== SHOW OUTPUT ===");
    println!("{}", show_stdout);
    
    // === REQUIREMENT CHECKS ===
    
    // R1: Each filename should appear exactly once in the final directory listing
    let final_directory_section = extract_final_directory_section(&show_stdout);
    println!("=== FINAL DIRECTORY SECTION ===");
    println!("{}", final_directory_section);
    
    let file1_final_count = final_directory_section.matches("file1.txt").count();
    let file2_final_count = final_directory_section.matches("file2.txt").count();
    let file3_final_count = final_directory_section.matches("file3.txt").count();
    
    println!("=== R1: Final Directory Filename Counts ===");
    println!("file1.txt: {} (expected: 1)", file1_final_count);
    println!("file2.txt: {} (expected: 1)", file2_final_count);
    println!("file3.txt: {} (expected: 1)", file3_final_count);
    
    assert_eq!(file1_final_count, 1, "file1.txt should appear exactly once in final directory");
    assert_eq!(file2_final_count, 1, "file2.txt should appear exactly once in final directory");
    assert_eq!(file3_final_count, 1, "file3.txt should appear exactly once in final directory");
    
    // R2: Each file content should appear exactly once
    let content1_count = show_stdout.matches("Content of file1").count();
    let content2_count = show_stdout.matches("Content of file2").count();
    let content3_count = show_stdout.matches("Content of file3").count();
    
    println!("=== R2: File Content Counts ===");
    println!("'Content of file1': {} (expected: 1)", content1_count);
    println!("'Content of file2': {} (expected: 1)", content2_count);
    println!("'Content of file3': {} (expected: 1)", content3_count);
    
    assert_eq!(content1_count, 1, "Content of file1 should appear exactly once");
    assert_eq!(content2_count, 1, "Content of file2 should appear exactly once");
    assert_eq!(content3_count, 1, "Content of file3 should appear exactly once");
    
    // R3: We should have exactly 4 unique node IDs (root + 3 files)
    let node_ids = extract_unique_node_ids(&show_stdout);
    println!("=== R3: Unique Node IDs ===");
    for (i, node_id) in node_ids.iter().enumerate() {
        println!("{}: {}", i + 1, node_id);
    }
    println!("Total unique node IDs: {} (expected: 4)", node_ids.len());
    
    assert_eq!(node_ids.len(), 4, "Should have exactly 4 unique node IDs (root + 3 files)");
    
    // R4: Transaction count should be reasonable (init + copy operation)
    let transaction_count = count_transactions(&show_stdout);
    println!("=== R4: Transaction Count ===");
    println!("Total transactions: {} (expected: ≤ 3)", transaction_count);
    
    // We expect: 1 init transaction + ideally 1 copy transaction, but up to 3 is acceptable
    assert!(transaction_count <= 3, "Should not have more than 3 transactions for this operation");
    
    // R5: Each file should have correct size (16 bytes for our test content)
    let file_sizes = extract_file_sizes(&show_stdout);
    println!("=== R5: File Sizes ===");
    for (filename, size) in &file_sizes {
        println!("{}: {} (expected: 16B)", filename, size);
    }
    
    assert_eq!(file_sizes.len(), 3, "Should have size information for 3 files");
    for (filename, size) in file_sizes {
        assert_eq!(size, "16B", "File {} should be 16 bytes", filename);
    }

    Ok(())
}

fn extract_final_directory_section(show_output: &str) -> String {
    // Find the last directory entry section
    let lines: Vec<&str> = show_output.lines().collect();
    let mut final_section = String::new();
    
    // Find the last occurrence of "Directory entries:"
    for (i, line) in lines.iter().enumerate().rev() {
        if line.contains("Directory entries:") {
            // Collect lines from this directory entry until the end of the entry
            for j in i..lines.len() {
                final_section.push_str(lines[j]);
                final_section.push('\n');
                if lines[j].starts_with("└─") || 
                   (j + 1 < lines.len() && lines[j + 1].starts_with("===")) {
                    break;
                }
            }
            break;
        }
    }
    
    final_section
}

fn extract_unique_node_ids(show_output: &str) -> Vec<String> {
    use std::collections::HashSet;
    let mut node_ids = HashSet::new();
    
    // Look for patterns like "0000..0001", "0000..0002", etc.
    for line in show_output.lines() {
        if let Some(start) = line.find("0000..") {
            if let Some(end) = line[start..].find(' ') {
                let node_id = &line[start..start + end];
                node_ids.insert(node_id.to_string());
            } else if let Some(end) = line[start..].find('[') {
                let node_id = &line[start..start + end].trim();
                node_ids.insert(node_id.to_string());
            }
        }
    }
    
    let mut sorted_ids: Vec<String> = node_ids.into_iter().collect();
    sorted_ids.sort();
    sorted_ids
}

fn count_transactions(show_output: &str) -> usize {
    show_output.lines()
        .filter(|line| line.contains("=== Transaction #"))
        .count()
}

fn extract_file_sizes(show_output: &str) -> Vec<(String, String)> {
    let mut file_sizes = Vec::new();
    let mut current_filename: Option<String> = None;
    
    for line in show_output.lines() {
        // Look for file entries
        if line.contains("[file]") {
            // Next lines should contain filename and size
            current_filename = None;
        } else if line.contains("File size:") {
            if let Some(size_start) = line.find("File size: ") {
                let size_part = &line[size_start + 11..];
                if let Some(size_end) = size_part.find(' ') {
                    let size = &size_part[..size_end];
                    if let Some(filename) = &current_filename {
                        file_sizes.push((filename.clone(), size.to_string()));
                    }
                }
            }
        } else if line.contains("'") && line.contains("->") {
            // Extract filename from directory entry like "'file1.txt' -> 0000..0001"
            if let Some(start) = line.find('\'') {
                if let Some(end) = line[start + 1..].find('\'') {
                    let filename = &line[start + 1..start + 1 + end];
                    current_filename = Some(filename.to_string());
                }
            }
        }
    }
    
    file_sizes
}

// ...existing code...
