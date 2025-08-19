use crate::persistence::OpLogPersistence;
use tempfile::TempDir;
use diagnostics::*;

fn test_dir() -> String {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();

    let path = match std::env::var("TLOGFS") {
	Ok(val) => {
	    std::fs::remove_dir_all(&val).expect("test dir can't be removed");
	    val
	},
	_ => store_path,
    };

    debug!("Creating OpLogPersistence at {path}");
    path
}

#[tokio::test]
async fn test_transaction_guard_basic_usage() {
    let store_path = test_dir();
    
    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");
    
    log_debug!("OpLogPersistence created successfully");
    
    // Begin a transaction
    log_debug!("Beginning transaction");
    let tx = persistence.begin().await
        .expect("Failed to begin transaction");
    
    log_debug!("Transaction started successfully");
    
    // Try to access the root directory
    log_debug!("Attempting to get root directory from transaction");
    let root = tx.root().await
        .expect("Failed to get root directory");
    
    let root_debug = format!("{:?}", root);
    log_info!("✅ Successfully got root directory", root_debug: &root_debug);
    
    // Commit the transaction
    log_debug!("Committing transaction");
    tx.commit(None).await
        .expect("Failed to commit transaction");
    
    log_info!("✅ Transaction committed successfully");
}

/// Test reading from a directory after creation (simulating steward's read pattern)
#[tokio::test]
async fn test_transaction_guard_read_after_create() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");

    // Transaction 1: Create directory structure
    {
        let tx = persistence.begin().await
            .expect("Failed to begin transaction");
        
        let root = tx.root().await
            .expect("Failed to get root directory");
        
        root.create_dir_path("/txn").await
            .expect("Failed to create /txn directory");
        
        tx.commit(None).await
            .expect("Failed to commit transaction");
    }
    
    // Transaction 2: Try to read from the created directory
    {
        let tx = persistence.begin().await
            .expect("Failed to begin read transaction");
        
        let root = tx.root().await
            .expect("Failed to get root directory for read");
        
        // Try to list the directory contents
        let _txn_dir = root.open_dir_path("/txn").await
            .expect("Failed to get /txn directory");
        
        println!("✅ Successfully accessed /txn directory in new transaction");
        
        // Don't commit - this is a read-only transaction
    }
    
    println!("✅ Read-after-create test completed successfully");
}
