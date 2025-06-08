//! Integration tests for TinyLogFS Phase 2 implementation
//! 
//! These tests verify the refined single-threaded architecture with Arrow builder
//! transaction state works correctly for basic filesystem operations.

#[cfg(test)]
mod tests {
    use std::path::Path;
    use tempfile::TempDir;
    use crate::tinylogfs::{TinyLogFS, TinyLogFSError};

    async fn create_test_filesystem() -> Result<(TinyLogFS, TempDir), TinyLogFSError> {
        let temp_dir = TempDir::new().map_err(TinyLogFSError::Io)?;
        let store_path = temp_dir.path().join("test_store");
        let store_path_str = store_path.to_string_lossy();
        
        let fs = TinyLogFS::init_empty(&store_path_str).await?;
        Ok((fs, temp_dir))
    }

    #[tokio::test]
    async fn test_filesystem_initialization() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Verify initial state
        let status = fs.get_status();
        assert_eq!(status.pending_operations, 0);
        assert!(status.total_nodes > 0); // Should have root directory
        
        Ok(())
    }

    #[tokio::test]
    async fn test_create_file_and_commit() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create a test file
        let content = b"Hello, TinyLogFS Phase 2!";
        let file_path = Path::new("test.txt");
        
        let _node_path = fs.create_file(file_path, content).await?;
        
        // Verify file exists in memory
        assert!(fs.exists(file_path));
        
        // Check transaction state before commit
        let status_before = fs.get_status();
        assert!(status_before.pending_operations > 0);
        
        // Commit the transaction
        let commit_result = fs.commit().await?;
        assert!(commit_result.operations_committed > 0);
        assert!(commit_result.bytes_written > 0);
        
        // Check transaction state after commit
        let status_after = fs.get_status();
        assert_eq!(status_after.pending_operations, 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_create_directory() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create a test directory
        let dir_path = Path::new("test_dir");
        let _working_dir = fs.create_directory(dir_path).await?;
        
        // Verify directory exists in memory
        assert!(fs.exists(dir_path));
        
        // Verify transaction has pending operations
        let status = fs.get_status();
        assert!(status.pending_operations > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create and commit a file
        let content = b"Test file content for reading";
        let file_path = Path::new("readable.txt");
        
        let _node_path = fs.create_file(file_path, content).await?;
        let _commit_result = fs.commit().await?;
        
        // Read the file back
        let read_content = fs.read_file(file_path).await?;
        assert_eq!(read_content, content);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_update_file() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create initial file
        let initial_content = b"Initial content";
        let file_path = Path::new("updateable.txt");
        
        let _node_path = fs.create_file(file_path, initial_content).await?;
        let _commit_result = fs.commit().await?;
        
        // Update the file
        let updated_content = b"Updated content";
        fs.update_file(file_path, updated_content).await?;
        
        // Verify updated content
        let read_content = fs.read_file(file_path).await?;
        assert_eq!(read_content, updated_content);
        
        // Verify transaction has pending operations
        let status = fs.get_status();
        assert!(status.pending_operations > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_query_history() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create and commit some files
        let _node1 = fs.create_file(Path::new("file1.txt"), b"Content 1").await?;
        let _node2 = fs.create_file(Path::new("file2.txt"), b"Content 2").await?;
        let _commit_result = fs.commit().await?;
        
        // Query the filesystem history
        let query = "SELECT part_id, node_id, file_type FROM filesystem_ops ORDER BY part_id";
        let batches = fs.query_history(query).await?;
        
        // Should have at least some results (root directory + created files)
        assert!(!batches.is_empty());
        
        Ok(())
    }

    #[tokio::test]
    async fn test_working_directory_access() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Get working directory
        let working_dir = fs.working_directory()?;
        
        // Verify we can use it for basic operations
        assert!(working_dir.exists(Path::new("/")));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_fs_access() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Get reference to underlying TinyFS
        let memory_fs = fs.memory_fs();
        
        // Verify we can create working directory from it
        let _working_dir = memory_fs.working_dir();
        
        Ok(())
    }
}
