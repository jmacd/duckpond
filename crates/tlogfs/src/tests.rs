use crate::persistence::OpLogPersistence;
use tempfile::TempDir;
use diagnostics::*;

#[tokio::test]
async fn test_transaction_guard_basic_usage() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    
    log_debug!("Creating OpLogPersistence at path", path: &store_path);
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

/// Test to investigate the transaction isolation issue
#[tokio::test]
async fn test_transaction_isolation_debug() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    
    log_debug!("=== PHASE 1: Creating OpLogPersistence ===");
    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");
    
    log_debug!("=== PHASE 2: Beginning first transaction ===");
    let tx1 = persistence.begin().await
        .expect("Failed to begin first transaction");
    
    log_debug!("=== PHASE 3: Attempting to access root directory from first transaction ===");
    let result1 = tx1.root().await;
    match result1 {
        Ok(_root) => {
            log_info!("✅ First transaction can access root directory");
            // Commit this transaction
            log_debug!("=== PHASE 4: Committing first transaction ===");
            tx1.commit(None).await.expect("Failed to commit first transaction");
        },
        Err(e) => {
            let error_msg = e.to_string();
            log_error!("❌ First transaction failed to access root", error: &error_msg);
            return;
        }
    }
    
    log_debug!("=== PHASE 5: Beginning second transaction ===");
    let tx2 = persistence.begin().await
        .expect("Failed to begin second transaction");
    
    log_debug!("=== PHASE 6: Attempting to access root directory from second transaction ===");
    let result2 = tx2.root().await;
    match result2 {
        Ok(_root) => {
            log_info!("✅ Second transaction can access root directory");
        },
        Err(e) => {
            let error_msg = e.to_string();
            log_error!("❌ Second transaction failed to access root", error: &error_msg);
        }
    }
}

/// Test reading from a directory after creation (simulating steward's read pattern)
#[tokio::test]
async fn test_transaction_guard_read_after_create() {
    //let temp_dir = TempDir::new().expect("Failed to create temp dir");
    //let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    let store_path = "/tmp/tester";
    
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

/// Test multiple persistence instances (simulating steward's pattern)
#[tokio::test]
async fn test_multiple_persistence_instances() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    
    // Create and write with first persistence instance
    {
        let mut persistence1 = OpLogPersistence::create(&store_path).await
            .expect("Failed to create first persistence layer");
        
        let tx = persistence1.begin().await
            .expect("Failed to begin transaction on first instance");
        
        let root = tx.root().await
            .expect("Failed to get root directory from first instance");
        
        root.create_dir_path("/txn").await
            .expect("Failed to create /txn directory");
        
        tx.commit(None).await
            .expect("Failed to commit transaction on first instance");
        
        println!("✅ First persistence instance created directory");
    }
    
    // Read with second persistence instance (this is what steward was trying to do)
    {
        let mut persistence2 = OpLogPersistence::open(&store_path).await
            .expect("Failed to open second persistence layer");
        
        let tx = persistence2.begin().await
            .expect("Failed to begin transaction on second instance");
        
        let root = tx.root().await
            .expect("Failed to get root directory from second instance");
        
        let _txn_dir = root.open_dir_path("/txn").await
            .expect("Failed to get /txn directory from second instance");
        
        println!("✅ Second persistence instance successfully read directory");
    }
    
    println!("✅ Multiple persistence instances test completed successfully");
}
