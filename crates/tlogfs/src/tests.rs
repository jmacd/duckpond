use crate::persistence::OpLogPersistence;
use tempfile::TempDir;

#[tokio::test]
async fn test_transaction_guard_basic_usage() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    
    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");
    
    // Begin a transaction
    let tx = persistence.begin().await
        .expect("Failed to begin transaction");
    
    // Try to access the root directory
    let root = tx.root().await
        .expect("Failed to get root directory");
    
    println!("✅ Successfully got root directory: {:?}", root);
    
    // Commit the transaction
    tx.commit(None).await
        .expect("Failed to commit transaction");
    
    println!("✅ Transaction committed successfully");
}

/// Test creating a directory through the transaction guard
#[tokio::test]
async fn test_transaction_guard_directory_creation() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    
    let mut persistence = OpLogPersistence::create(&store_path).await
        .expect("Failed to create persistence layer");
    
    // Begin a transaction
    let tx = persistence.begin().await
        .expect("Failed to begin transaction");
    
    // Get root and create a directory
    let root = tx.root().await
        .expect("Failed to get root directory");
    
    // This is similar to what steward is trying to do
    root.create_dir_path("/txn").await
        .expect("Failed to create /txn directory");
    
    println!("✅ Successfully created /txn directory");
    
    // Commit the transaction
    tx.commit(None).await
        .expect("Failed to commit transaction");
    
    println!("✅ Transaction with directory creation committed successfully");
}

/// Test reading from a directory after creation (simulating steward's read pattern)
#[tokio::test]
async fn test_transaction_guard_read_after_create() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir.path().join("test_store").to_string_lossy().to_string();
    
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
