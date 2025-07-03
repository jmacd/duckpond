// Test to demonstrate the new architecture working at the backend level

#[cfg(test)]
mod test_backend_query {
    use futures::StreamExt;
    use tempfile;
    
    #[tokio::test]
    async fn test_backend_directory_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store_path = temp_dir.path().join("test_backend_query");
        let store_uri = format!("file://{}", store_path.display());
        
        // Create filesystem with OpLog backend using factory function
        let fs = crate::create_oplog_fs(&store_uri).await.unwrap();
        
        // Create a test directory with some files using the actual filesystem
        let working_dir = fs.root().await.unwrap();
        
        // Create a subdirectory
        let test_dir = working_dir.create_dir_path("test_dir").await.unwrap();
        
        // Create some files in the directory
        let _file1 = test_dir.create_file_path("file1.txt", b"Hello, world!").await.unwrap();
        let _file2 = test_dir.create_file_path("file2.txt", b"Another file").await.unwrap();
        let _subdir = test_dir.create_dir_path("subdir").await.unwrap();
        
        // Commit the changes
        let _ = fs.commit().await.unwrap();
        diagnostics::log_info!("Committed");
        
        // Now try to read the directory entries by reopening the filesystem
        // This tests the on-demand loading functionality
        let fs2 = crate::create_oplog_fs(&store_uri).await.unwrap();
        
        let working_dir2 = fs2.root().await.unwrap();
        let test_dir2 = working_dir2.open_dir_path("test_dir").await.unwrap();
        
        // Read the directory contents
        let mut entries: Vec<tinyfs::NodePath> = Vec::new();
        let mut stream = test_dir2.read_dir().await.unwrap();
        while let Some(entry) = stream.next().await {
            entries.push(entry);
        }
        
        let entry_count = entries.len();
        diagnostics::log_info!("Found {entry_count} entries in restored directory", entry_count: entry_count);
        for entry in &entries {
            let path_display = entry.path().display().to_string();
            diagnostics::log_info!("  - {path}", path: path_display);
        }
        
        // Verify we can access the files
        assert_eq!(entries.len(), 3); // file1.txt, file2.txt, subdir
        
        // Verify the files have correct content
        let content1 = test_dir2.read_file_path("file1.txt").await.unwrap();
        assert_eq!(content1, b"Hello, world!");
        
        let content2 = test_dir2.read_file_path("file2.txt").await.unwrap();
        assert_eq!(content2, b"Another file");
        
        diagnostics::log_info!("✅ Backend query architecture works!");
        diagnostics::log_info!("This demonstrates the complete on-demand loading functionality");
    }
}
