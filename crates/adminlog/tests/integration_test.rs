
use adminlog::{AdminLog, LogEntry};
use tempfile::tempdir;
use chrono::Utc;
use std::time::Duration;

#[tokio::test]
async fn test_open_and_create() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("admin_log");

    // Open or create a new log
    let log = AdminLog::open(&log_path).await.unwrap();

    // The log should exist now
    assert!(log_path.exists());
}

#[tokio::test]
async fn test_write_and_read_latest() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("admin_log");

    // Open a new log
    let mut log = AdminLog::open(&log_path).await.unwrap();

    // Write a test message
    let test_message = "open FS";
    log.write(test_message).await.unwrap();

    // Read the latest message
    let entry = log.read_latest().await.unwrap().unwrap();

    // Verify message content
    assert_eq!(entry.message, test_message);
    
    // Verify timestamp is recent
    let now = Utc::now();
    let time_diff = now.signed_duration_since(entry.timestamp);
    assert!(time_diff.num_seconds() < 5);
}

#[tokio::test]
async fn test_multiple_entries() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("admin_log");

    // Open a new log
    let mut log = AdminLog::open(&log_path).await.unwrap();

    // Write multiple entries
    log.write("open FS").await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await; // Ensure timestamp difference
    log.write("sync FS").await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    log.write("close FS").await.unwrap();

    // Read all entries
    let entries = log.read_all().await.unwrap();

    // Check count
    assert_eq!(entries.len(), 3);

    // Check order (should be chronological)
    assert_eq!(entries[0].message, "open FS");
    assert_eq!(entries[1].message, "sync FS");
    assert_eq!(entries[2].message, "close FS");

    // Read latest should return the most recent entry
    let latest = log.read_latest().await.unwrap().unwrap();
    assert_eq!(latest.message, "close FS");
}

#[tokio::test]
async fn test_close_and_reopen() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("admin_log");

    // Open a new log
    let mut log = AdminLog::open(&log_path).await.unwrap();

    // Write a test message
    log.write("open FS").await.unwrap();

    // Close the log
    log.close();

    // Reopen the log
    let mut log = AdminLog::open(&log_path).await.unwrap();

    // Write another message
    log.write("sync FS").await.unwrap();

    // Read all entries
    let entries = log.read_all().await.unwrap();

    // Should have both entries
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].message, "open FS");
    assert_eq!(entries[1].message, "sync FS");
}