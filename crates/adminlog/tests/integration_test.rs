use adminlog::{AdminLog, AdminLogEntry};
use tempfile::tempdir;
use std::path::Path;

#[tokio::test]
async fn test_local_adminlog() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the test
    let temp_dir = tempdir()?;
    let log_path = temp_dir.path().join("admin-logs");
    std::fs::create_dir_all(&log_path)?;
    
    // Convert path to file URL
    let log_url = format!("file://{}", log_path.to_string_lossy());
    
    // Open the admin log
    let mut log = AdminLog::open(&log_url).await?;
    
    // Write a test entry
    let test_action = "open FS";
    let test_details = "test filesystem opened";
    log.write(AdminLogEntry::new(test_action, Some(test_details))).await?;
    
    // Read the entry back
    let entry = log.read_latest().await?;
    
    // Verify the entry
    assert!(entry.is_some());
    let entry = entry.unwrap();
    assert_eq!(entry.action(), test_action);
    assert_eq!(entry.details(), Some(test_details));
    
    // Write another entry
    let second_action = "sync FS";
    log.write(AdminLogEntry::new(second_action, None)).await?;
    
    // Read latest again to verify it's updated
    let latest_entry = log.read_latest().await?.unwrap();
    assert_eq!(latest_entry.action(), second_action);
    assert!(latest_entry.details().is_none());
    
    // Close the log
    log.close().await?;
    
    // Verify Delta Lake files were created in the directory
    let _delta_log_dir = Path::new(&log_path).join("_delta_log");
    assert!(Path::new(&log_path).exists());
    assert!(_delta_log_dir.exists());
    
    // Clean up temp dir is automatic when tempdir is dropped
    
    Ok(())
}