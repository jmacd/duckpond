// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::{FileInfoVisitor, ShipContext};
use anyhow::Result;

/// List files with a closure for handling output
pub async fn list_command<F>(
    ship_context: &ShipContext,
    pattern: &str,
    show_all: bool,
    mut handler: F,
) -> Result<()>
where
    F: FnMut(&str),
{
    let mut ship = ship_context.open_pond().await?;

    // Use transaction for consistent filesystem access
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "list".to_string(),
            pattern.to_string(),
        ]))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

    let result = {
        let fs = &*tx; // StewardTransactionGuard derefs to FS
        let root = fs.root().await?;

        // Use FileInfoVisitor to collect file information - always allow all files at visitor level
        let mut visitor = FileInfoVisitor::new(true); // Always allow all at visitor level
        root.visit_with_visitor(pattern, &mut visitor)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to list files matching '{}' from data filesystem: {}",
                    pattern,
                    e
                )
            })
    };

    // Commit the transaction before processing results
    _ = tx
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;

    let mut file_results = result?;

    // Filter hidden files if show_all is false
    if !show_all {
        file_results.retain(|file_info| {
            let basename = std::path::Path::new(&file_info.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            !basename.starts_with('.') || basename == "." || basename == ".."
        });
    }

    // Sort results by path for consistent output
    file_results.sort_by(|a, b| a.path.cmp(&b.path));

    for file_info in file_results {
        handler(&file_info.format_duckpond_style());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use log::debug;

    struct TestSetup {
        ship_context: ShipContext,
        _temp_dir: tempfile::TempDir,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(&pond_path), init_args.clone());

            // Initialize the pond
            init_command(&ship_context, None, None).await?;

            Ok(Self {
                ship_context,
                _temp_dir: temp_dir,
            })
        }

        async fn create_pond_file(
            &self,
            path: &str,
            content: &str,
            entry_type: tinyfs::EntryType,
        ) -> Result<()> {
            use tokio::io::AsyncWriteExt;

            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test_setup".to_string(),
                    path.to_string(),
                ]))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                let mut writer = root.async_writer_path_with_type(path, entry_type).await?;
                writer.write_all(content.as_bytes()).await?;
                writer.flush().await?;
                writer.shutdown().await
            };

            // Check shutdown result BEFORE committing
            result.map_err(|e| anyhow::anyhow!("Failed to write file content: {}", e))?;

            let _ = tx
                .commit()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            Ok(())
        }

        async fn create_pond_directory(&self, path: &str) -> Result<()> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test_setup".to_string(),
                    path.to_string(),
                ]))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                root.create_dir_path(path).await.map(|_| ())
            };

            _ = tx
                .commit()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            result.map_err(|e| anyhow::anyhow!("Failed to create directory: {}", e))?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_list_single_file() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create test file in pond
        setup
            .create_pond_file(
                "test.txt",
                "hello world",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "test.txt", false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("test.txt"));
        assert!(results[0].contains("📄")); // FileData emoji
    }

    #[tokio::test]
    async fn test_list_multiple_files() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create multiple test files in pond
        setup
            .create_pond_file("file1.txt", "content1", tinyfs::EntryType::FilePhysicalVersion)
            .await
            .expect("Failed to create pond file1");
        setup
            .create_pond_file(
                "file2.csv",
                "header,value\nrow,1",
                tinyfs::EntryType::TablePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file2");
        setup
            .create_pond_file(
                "file3.parquet",
                "data file",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file3");

        // First, let's try listing all files to see what's actually there
        let mut all_results = Vec::new();
        list_command(&setup.ship_context, "*", false, |output| {
            all_results.push(output.to_string())
        })
        .await
        .expect("List all command failed");

        debug!("All files found with '*': {:?}", all_results);

        let mut results = Vec::new();
        list_command(&setup.ship_context, "file*", false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        debug!("Files found with 'file*': {:?}", results);

        assert_eq!(results.len(), 3);

        // Results should be sorted by path
        assert!(results[0].contains("file1.txt"));
        assert!(results[1].contains("file2.csv"));
        assert!(results[2].contains("file3.parquet"));
    }

    #[tokio::test]
    async fn test_list_directory() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create directory and file in pond
        setup
            .create_pond_directory("testdir")
            .await
            .expect("Failed to create pond directory");
        setup
            .create_pond_file(
                "testdir/nested.txt",
                "nested content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "testdir", false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("testdir"));
        assert!(results[0].contains("📁")); // Directory emoji
    }

    #[tokio::test]
    async fn test_list_recursive_pattern() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create nested directory structure - need to create parent directories first
        setup
            .create_pond_directory("dir1")
            .await
            .expect("Failed to create dir1");
        setup
            .create_pond_directory("dir1/subdir")
            .await
            .expect("Failed to create dir1/subdir");
        setup
            .create_pond_file(
                "dir1/subdir/file1.txt",
                "content1",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested pond file1");

        setup
            .create_pond_directory("dir2")
            .await
            .expect("Failed to create dir2");
        setup
            .create_pond_file(
                "dir2/file2.txt",
                "content2",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested pond file2");

        setup
            .create_pond_file("file3.txt", "content3", tinyfs::EntryType::FilePhysicalVersion)
            .await
            .expect("Failed to create root pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "**/*.txt", false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 3);
        assert!(results.iter().any(|r| r.contains("dir1/subdir/file1.txt")));
        assert!(results.iter().any(|r| r.contains("dir2/file2.txt")));
        assert!(results.iter().any(|r| r.contains("file3.txt")));
    }

    #[tokio::test]
    async fn test_list_hidden_files() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create hidden and regular files
        setup
            .create_pond_file(
                ".hidden.txt",
                "hidden content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create hidden pond file");
        setup
            .create_pond_file(
                "visible.txt",
                "visible content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create visible pond file");

        // Test without show_all - should only see visible file
        let mut results = Vec::new();
        list_command(&setup.ship_context, "*", false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("visible.txt"));

        // Test with show_all - should see both files
        let mut results_all = Vec::new();
        list_command(&setup.ship_context, "*", true, |output| {
            results_all.push(output.to_string())
        })
        .await
        .expect("List command with show_all failed");

        assert_eq!(results_all.len(), 2);
        assert!(results_all.iter().any(|r| r.contains(".hidden.txt")));
        assert!(results_all.iter().any(|r| r.contains("visible.txt")));
    }

    #[tokio::test]
    async fn test_list_no_matches() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create a file that won't match the pattern
        setup
            .create_pond_file("file.txt", "content", tinyfs::EntryType::FilePhysicalVersion)
            .await
            .expect("Failed to create pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "*.nonexistent", false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 0);
    }
}
