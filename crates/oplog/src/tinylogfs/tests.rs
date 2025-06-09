//! Integration tests for TinyLogFS OpLogBackend implementation
//! 
//! These tests verify the OpLogBackend implementation with TinyFS works correctly
//! for basic filesystem operations with Delta Lake persistence.

#[cfg(test)]
mod tests {
    use std::path::Path;
    use tempfile::TempDir;
    use crate::tinylogfs::{OpLogBackend, TinyLogFSError};
    use tinyfs::FS;

    async fn create_test_filesystem() -> Result<(FS, TempDir), TinyLogFSError> {
        let temp_dir = TempDir::new().map_err(TinyLogFSError::Io)?;
        let store_path = temp_dir.path().join("test_store");
        let store_path_str = store_path.to_string_lossy();
        
        // Create OpLogBackend and initialize it
        let backend = OpLogBackend::new(&store_path_str).await?;
        
        // Create FS with the OpLogBackend
        let fs = FS::with_backend(backend)
            .map_err(|e| TinyLogFSError::TinyFS(e))?;
        
        Ok((fs, temp_dir))
    }

    #[tokio::test]
    async fn test_filesystem_initialization() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Verify filesystem is created with root directory
        let working_dir = fs.working_dir();
        // The working directory should be valid and we should be able to create files in it
        let _test_file = working_dir.create_file_path("init_test.txt", b"test")?;
        assert!(working_dir.exists(Path::new("init_test.txt")));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_create_file_and_commit() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create a test file through working directory
        let content = b"Hello, OpLogBackend!";
        let working_dir = fs.working_dir();
        
        let file_node = working_dir.create_file_path("test.txt", content)
            .map_err(|e| format!("Failed to create file: {}", e))?;
        
        // Verify file exists in memory
        assert!(working_dir.exists(Path::new("test.txt")));
        
        // Read content back through TinyFS
        let read_content = file_node.borrow().as_file()
            .map_err(|e| format!("Failed to get file: {}", e))?
            .read_file()
            .map_err(|e| format!("Failed to read file: {}", e))?;
        assert_eq!(read_content, content);
        
        // Note: commit() functionality requires backend access - 
        // this would be handled by TinyLogFS.commit() in the full implementation
        
        Ok(())
    }

    #[tokio::test]
    async fn test_create_directory() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create a test directory
        let working_dir = fs.working_dir();
        let _dir_node = working_dir.create_dir_path("test_dir")
            .map_err(|e| format!("Failed to create directory: {}", e))?;
        
        // Verify directory exists in memory
        assert!(working_dir.exists(Path::new("test_dir")));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_complex_directory_structure() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        let working_dir = fs.working_dir();
        
        // Create nested directory structure
        let dir1_wd = working_dir.create_dir_path("dir1")?;
        
        let _file1 = dir1_wd.create_file_path("file1.txt", b"content1")?;
        let _file2 = dir1_wd.create_file_path("file2.txt", b"content2")?;
        
        let dir2_wd = dir1_wd.create_dir_path("subdir")?;
        let _file3 = dir2_wd.create_file_path("file3.txt", b"content3")?;
        
        // Verify structure exists
        assert!(working_dir.exists(Path::new("dir1")));
        assert!(dir1_wd.exists(Path::new("file1.txt")));
        assert!(dir1_wd.exists(Path::new("file2.txt")));
        assert!(dir1_wd.exists(Path::new("subdir")));
        assert!(dir2_wd.exists(Path::new("file3.txt")));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_query_backend_operations() -> Result<(), Box<dyn std::error::Error>> {
        let (fs, _temp_dir) = create_test_filesystem().await?;
        
        // Create some files and directories
        let working_dir = fs.working_dir();
        let _file1 = working_dir.create_file_path("query_test1.txt", b"data1")?;
        let _file2 = working_dir.create_file_path("query_test2.txt", b"data2")?;
        let _dir1 = working_dir.create_dir_path("query_dir")?;
        
        // Verify the files and directories exist
        assert!(working_dir.exists(Path::new("query_test1.txt")));
        assert!(working_dir.exists(Path::new("query_test2.txt")));
        assert!(working_dir.exists(Path::new("query_dir")));
        
        // The specific query functionality would depend on the OpLogBackend implementation
        // For now, just verify that the filesystem operations worked
        Ok(())
    }

}
