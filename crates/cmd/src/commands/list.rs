use anyhow::Result;
use crate::common::{FilesystemChoice, ShipContext, FileInfoVisitor};
use std::collections::HashMap;

/// List files with a closure for handling output
pub async fn list_command<F>(
    ship_context: &ShipContext, 
    pattern: &str, 
    show_all: bool, 
    filesystem: FilesystemChoice,
    mut handler: F
) -> Result<()>
where
    F: FnMut(String),
{
    // For now, only support data filesystem - control filesystem access would require different API
    if filesystem == FilesystemChoice::Control {
        return Err(anyhow::anyhow!("Control filesystem access not yet implemented for list command"));
    }
    
    let mut ship = ship_context.open_pond().await?;
    
    // Use transaction for consistent filesystem access
    let tx = ship.begin_transaction(vec!["list".to_string(), pattern.to_string()], HashMap::new()).await
        .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
    
    let result = {
        let fs = &*tx; // StewardTransactionGuard derefs to FS
        let root = fs.root().await?;
        
        // Use FileInfoVisitor to collect file information - always allow all files at visitor level
        let mut visitor = FileInfoVisitor::new(true); // Always allow all at visitor level
        root.visit_with_visitor(pattern, &mut visitor).await
            .map_err(|e| anyhow::anyhow!("Failed to list files matching '{}' from data filesystem: {}", pattern, e))
    };
    
    // Commit the transaction before processing results
    tx.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
    
    let mut file_results = result?;
    
    // Filter hidden files if show_all is false
    if !show_all {
        file_results.retain(|file_info| {
            let basename = std::path::Path::new(&file_info.path).file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            !basename.starts_with('.') || basename == "." || basename == ".."
        });
    }
    
    // Sort results by path for consistent output
    file_results.sort_by(|a, b| a.path.cmp(&b.path));
    
    for file_info in file_results {
        handler(file_info.format_duckpond_style());
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use crate::common::{ShipContext, FilesystemChoice};
    use crate::commands::init::init_command;

    struct TestSetup {
        #[allow(dead_code)] // Needed for test infrastructure
        temp_dir: TempDir,
        ship_context: ShipContext,
        #[allow(dead_code)] // Needed for test infrastructure
        pond_path: PathBuf,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let pond_path = temp_dir.path().join("test_pond");
            
            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(pond_path.clone()), init_args.clone());
            
            // Initialize the pond
            init_command(&ship_context).await?;
            
            Ok(Self {
                temp_dir,
                ship_context,
                pond_path,
            })
        }

        async fn create_pond_file(&self, path: &str, content: &str, entry_type: tinyfs::EntryType) -> Result<()> {
            use tokio::io::AsyncWriteExt;
            
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship.begin_transaction(vec!["test_setup".to_string(), path.to_string()], HashMap::new()).await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
            
            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                let mut writer = root.async_writer_path_with_type(path, entry_type).await?;
                writer.write_all(content.as_bytes()).await?;
                writer.flush().await
            };
            
            tx.commit().await.map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            result.map_err(|e| anyhow::anyhow!("Failed to write file content: {}", e))?;
            Ok(())
        }

        async fn create_pond_directory(&self, path: &str) -> Result<()> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship.begin_transaction(vec!["test_setup".to_string(), path.to_string()], HashMap::new()).await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
            
            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                root.create_dir_path(path).await.map(|_| ())
            };
            
            tx.commit().await.map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            result.map_err(|e| anyhow::anyhow!("Failed to create directory: {}", e))?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_list_single_file() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create test file in pond
        setup.create_pond_file("test.txt", "hello world", tinyfs::EntryType::FileData).await
            .expect("Failed to create pond file");

        let mut results = Vec::new();
        list_command(
            &setup.ship_context, 
            "test.txt", 
            false, 
            FilesystemChoice::Data,
            |output| results.push(output)
        ).await.expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("test.txt"));
        assert!(results[0].contains("📄")); // FileData emoji
    }

    #[tokio::test]
    async fn test_list_multiple_files() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create multiple test files in pond
        setup.create_pond_file("file1.txt", "content1", tinyfs::EntryType::FileData).await
            .expect("Failed to create pond file1");
        setup.create_pond_file("file2.csv", "header,value\nrow,1", tinyfs::EntryType::FileTable).await
            .expect("Failed to create pond file2");
        setup.create_pond_file("file3.parquet", "series data", tinyfs::EntryType::FileSeries).await
            .expect("Failed to create pond file3");

        // First, let's try listing all files to see what's actually there
        let mut all_results = Vec::new();
        list_command(
            &setup.ship_context, 
            "*", 
            false, 
            FilesystemChoice::Data,
            |output| all_results.push(output)
        ).await.expect("List all command failed");
        
        println!("All files found with '*': {:?}", all_results);

        let mut results = Vec::new();
        list_command(
            &setup.ship_context, 
            "file*", 
            false, 
            FilesystemChoice::Data,
            |output| results.push(output)
        ).await.expect("List command failed");

        println!("Files found with 'file*': {:?}", results);

        assert_eq!(results.len(), 3);
        
        // Results should be sorted by path
        assert!(results[0].contains("file1.txt"));
        assert!(results[0].contains("📄")); // FileData emoji
        assert!(results[1].contains("file2.csv"));
        assert!(results[1].contains("📊")); // FileTable emoji
        assert!(results[2].contains("file3.parquet"));
        assert!(results[2].contains("📈")); // FileSeries emoji
    }

    #[tokio::test]
    async fn test_list_directory() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create directory and file in pond
        setup.create_pond_directory("testdir").await
            .expect("Failed to create pond directory");
        setup.create_pond_file("testdir/nested.txt", "nested content", tinyfs::EntryType::FileData).await
            .expect("Failed to create nested pond file");

        let mut results = Vec::new();
        list_command(
            &setup.ship_context, 
            "testdir", 
            false, 
            FilesystemChoice::Data,
            |output| results.push(output)
        ).await.expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("testdir"));
        assert!(results[0].contains("📁")); // Directory emoji
    }

    #[tokio::test]
    async fn test_list_recursive_pattern() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create nested directory structure - need to create parent directories first
        setup.create_pond_directory("dir1").await
            .expect("Failed to create dir1");
        setup.create_pond_directory("dir1/subdir").await
            .expect("Failed to create dir1/subdir");
        setup.create_pond_file("dir1/subdir/file1.txt", "content1", tinyfs::EntryType::FileData).await
            .expect("Failed to create nested pond file1");
            
        setup.create_pond_directory("dir2").await
            .expect("Failed to create dir2");
        setup.create_pond_file("dir2/file2.txt", "content2", tinyfs::EntryType::FileData).await
            .expect("Failed to create nested pond file2");
            
        setup.create_pond_file("file3.txt", "content3", tinyfs::EntryType::FileData).await
            .expect("Failed to create root pond file");

        let mut results = Vec::new();
        list_command(
            &setup.ship_context, 
            "**/*.txt", 
            false, 
            FilesystemChoice::Data,
            |output| results.push(output)
        ).await.expect("List command failed");

        assert_eq!(results.len(), 3);
        assert!(results.iter().any(|r| r.contains("dir1/subdir/file1.txt")));
        assert!(results.iter().any(|r| r.contains("dir2/file2.txt")));
        assert!(results.iter().any(|r| r.contains("file3.txt")));
    }

    #[tokio::test]
    async fn test_list_hidden_files() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create hidden and regular files
        setup.create_pond_file(".hidden.txt", "hidden content", tinyfs::EntryType::FileData).await
            .expect("Failed to create hidden pond file");
        setup.create_pond_file("visible.txt", "visible content", tinyfs::EntryType::FileData).await
            .expect("Failed to create visible pond file");

        // Test without show_all - should only see visible file
        let mut results = Vec::new();
        list_command(
            &setup.ship_context, 
            "*", 
            false, 
            FilesystemChoice::Data,
            |output| results.push(output)
        ).await.expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("visible.txt"));

        // Test with show_all - should see both files
        let mut results_all = Vec::new();
        list_command(
            &setup.ship_context, 
            "*", 
            true, 
            FilesystemChoice::Data,
            |output| results_all.push(output)
        ).await.expect("List command with show_all failed");

        assert_eq!(results_all.len(), 2);
        assert!(results_all.iter().any(|r| r.contains(".hidden.txt")));
        assert!(results_all.iter().any(|r| r.contains("visible.txt")));
    }

    #[tokio::test]
    async fn test_list_no_matches() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create a file that won't match the pattern
        setup.create_pond_file("file.txt", "content", tinyfs::EntryType::FileData).await
            .expect("Failed to create pond file");

        let mut results = Vec::new();
        list_command(
            &setup.ship_context, 
            "*.nonexistent", 
            false, 
            FilesystemChoice::Data,
            |output| results.push(output)
        ).await.expect("List command failed");

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_list_control_filesystem_error() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        let mut results = Vec::new();
        let result = list_command(
            &setup.ship_context, 
            "*", 
            false, 
            FilesystemChoice::Control,
            |output| results.push(output)
        ).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Control filesystem access not yet implemented"));
    }
}
