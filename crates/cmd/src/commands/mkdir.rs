// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::ShipContext;
use anyhow::Result;
use log::debug;

/// Create a directory in the pond using scoped transactions
///
/// This command operates on an existing pond via the provided Ship.
/// Uses scoped transactions for automatic commit/rollback handling.
///
/// # Arguments
/// * `ship_context` - The ship context for pond operations
/// * `path` - The directory path to create
/// * `create_parents` - If true, creates parent directories as needed (like `mkdir -p`)
pub async fn mkdir_command(
    ship_context: &ShipContext,
    path: &str,
    create_parents: bool,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let path_for_closure = path.to_string();
    let path_display = path.to_string();

    debug!("Creating directory in pond: {path} (create_parents: {create_parents})");

    ship.write_transaction(
        &steward::PondUserMetadata::new(vec![
            "mkdir".to_string(),
            path_for_closure.clone(),
            if create_parents {
                "-p".to_string()
            } else {
                "".to_string()
            },
        ]),
        async |fs| {
            let root = fs.root().await?;

            if create_parents {
                let path_components: Vec<&str> = path_for_closure
                    .trim_start_matches('/')
                    .split('/')
                    .filter(|s| !s.is_empty())
                    .collect();

                if path_components.is_empty() {
                    return Ok(());
                }

                let mut current_path = String::new();
                for (i, component) in path_components.iter().enumerate() {
                    if path_for_closure.starts_with('/') || i > 0 {
                        current_path.push('/');
                    }
                    current_path.push_str(component);

                    if root.exists(&current_path).await {
                        match root.open_dir_path(&current_path).await {
                            Ok(_) => continue,
                            Err(_) => {
                                return Err(tinyfs::Error::Other(format!(
                                    "Path '{}' exists but is not a directory",
                                    current_path
                                ))
                                .into());
                            }
                        }
                    } else {
                        _ = root.create_dir_path(&current_path).await?;
                    }
                }
            } else {
                if path_for_closure.is_empty() || path_for_closure == "/" {
                    return Ok(());
                }

                if let Some(parent_pos) = path_for_closure.rfind('/') {
                    let parent_path = if parent_pos == 0 {
                        "/"
                    } else {
                        &path_for_closure[..parent_pos]
                    };

                    if !root.exists(parent_path).await {
                        return Err(tinyfs::Error::not_found(format!(
                            "Parent directory '{}' does not exist",
                            parent_path
                        ))
                        .into());
                    }
                }

                if root.exists(&path_for_closure).await {
                    return Err(tinyfs::Error::already_exists(&path_for_closure).into());
                }

                _ = root.create_dir_path(&path_for_closure).await?;
            }
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create directory: {}", e))?;

    log::debug!("Directory created successfully {path_display}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use tempfile::TempDir;

    struct TestSetup {
        _temp_dir: TempDir,
        ship_context: ShipContext,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());

            // Initialize pond
            init_command(&ship_context, None, None)
                .await
                .expect("Failed to initialize pond");

            Ok(TestSetup {
                _temp_dir: temp_dir,
                ship_context,
            })
        }

        /// Verify a directory exists in the pond
        async fn verify_directory_exists(&self, path: &str) -> Result<bool> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "test_verify".to_string(),
                    path.to_string(),
                ]))
                .await?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;

                // First check if the path exists at all
                if !root.exists(path).await {
                    Ok(false)
                } else {
                    // Try to open as directory to verify it's actually a directory
                    match root.open_dir_path(path).await {
                        Ok(_) => Ok(true),
                        Err(_) => Ok(false), // Exists but not a directory
                    }
                }
            };

            _ = tx.commit().await?;
            result
        }

        /// Create a test file in the pond (to test mkdir with existing structure)
        async fn create_pond_file(&self, path: &str, content: &str) -> Result<()> {
            use tokio::io::AsyncWriteExt;

            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test_setup".to_string(),
                    path.to_string(),
                ]))
                .await?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                let mut writer = root
                    .async_writer_path_with_type(path, tinyfs::EntryType::FilePhysicalVersion)
                    .await?;
                writer.write_all(content.as_bytes()).await?;
                writer.flush().await?;
                writer.shutdown().await
            };

            _ = tx.commit().await?;
            result.map_err(|e| anyhow::anyhow!("Failed to write file content: {}", e))?;
            Ok(())
        }

        /// List contents of a directory in the pond
        async fn list_directory_contents(&self, path: &str) -> Result<Vec<String>> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "test_list".to_string(),
                    path.to_string(),
                ]))
                .await?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;

                // Use a simple visitor to collect directory entries
                use crate::common::FileInfoVisitor;
                let mut visitor = FileInfoVisitor::new(true);
                let pattern = if path == "/" || path.is_empty() {
                    "*".to_string()
                } else {
                    format!("{}/*", path)
                };

                let file_infos = root.visit_with_visitor(&pattern, &mut visitor).await?;

                let files: Vec<String> = file_infos.into_iter().map(|info| info.path).collect();

                Ok(files)
            };

            _ = tx.commit().await?;
            result
        }
    }

    #[tokio::test]
    async fn test_mkdir_single_directory() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create a single directory (no -p flag needed for root level)
        mkdir_command(&setup.ship_context, "testdir", false)
            .await
            .expect("Failed to create directory");

        // Verify the directory exists
        assert!(
            setup
                .verify_directory_exists("testdir")
                .await
                .expect("Failed to check directory"),
            "Directory should exist after mkdir"
        );

        // Verify it's actually a directory type
        let contents = setup
            .list_directory_contents("/")
            .await
            .expect("Failed to list root");
        assert!(
            contents.contains(&"/testdir".to_string()),
            "Directory should appear in root listing"
        );
    }

    #[tokio::test]
    async fn test_mkdir_nested_directory_with_parents() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create nested directory structure with -p flag
        mkdir_command(&setup.ship_context, "parent/child/grandchild", true)
            .await
            .expect("Failed to create nested directory");

        // Verify all levels exist
        assert!(
            setup
                .verify_directory_exists("parent")
                .await
                .expect("Failed to check parent"),
            "Parent directory should exist"
        );
        assert!(
            setup
                .verify_directory_exists("parent/child")
                .await
                .expect("Failed to check child"),
            "Child directory should exist"
        );
        assert!(
            setup
                .verify_directory_exists("parent/child/grandchild")
                .await
                .expect("Failed to check grandchild"),
            "Grandchild directory should exist"
        );
    }

    #[tokio::test]
    async fn test_mkdir_already_exists() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create directory first time (without -p)
        mkdir_command(&setup.ship_context, "existingdir", false)
            .await
            .expect("Failed to create directory first time");

        // Try to create the same directory again without -p - should fail
        let result = mkdir_command(&setup.ship_context, "existingdir", false).await;
        assert!(
            result.is_err(),
            "mkdir without -p should fail for existing directories"
        );

        // Try to create the same directory with -p - should succeed (idempotent behavior)
        mkdir_command(&setup.ship_context, "existingdir", true)
            .await
            .expect("mkdir -p should succeed for existing directories");

        // Verify directory still exists
        assert!(
            setup
                .verify_directory_exists("existingdir")
                .await
                .expect("Failed to check directory"),
            "Directory should still exist after mkdir operations"
        );
    }

    #[tokio::test]
    async fn test_mkdir_with_existing_file_conflict() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create a file first
        setup
            .create_pond_file("conflictpath", "file content")
            .await
            .expect("Failed to create test file");

        // Try to create a directory with the same path - should fail
        let result = mkdir_command(&setup.ship_context, "conflictpath", false).await;

        assert!(
            result.is_err(),
            "mkdir should fail when path already exists as file"
        );

        // Verify the original file still exists
        assert!(
            !setup
                .verify_directory_exists("conflictpath")
                .await
                .expect("Failed to check path"),
            "Path should not be a directory"
        );
    }

    #[tokio::test]
    async fn test_mkdir_multiple_directories() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create multiple directories at different levels
        mkdir_command(&setup.ship_context, "dir1", false)
            .await
            .expect("Failed to create dir1");
        mkdir_command(&setup.ship_context, "dir2", false)
            .await
            .expect("Failed to create dir2");
        mkdir_command(&setup.ship_context, "dir1/subdir", false)
            .await
            .expect("Failed to create subdir"); // This works because dir1 already exists

        // Verify all directories exist
        assert!(
            setup
                .verify_directory_exists("dir1")
                .await
                .expect("Failed to check dir1"),
            "dir1 should exist"
        );
        assert!(
            setup
                .verify_directory_exists("dir2")
                .await
                .expect("Failed to check dir2"),
            "dir2 should exist"
        );
        assert!(
            setup
                .verify_directory_exists("dir1/subdir")
                .await
                .expect("Failed to check subdir"),
            "dir1/subdir should exist"
        );

        // Verify the directory structure by listing contents
        let root_contents = setup
            .list_directory_contents("/")
            .await
            .expect("Failed to list root");
        assert!(
            root_contents.contains(&"/dir1".to_string()),
            "Root should contain dir1"
        );
        assert!(
            root_contents.contains(&"/dir2".to_string()),
            "Root should contain dir2"
        );

        let dir1_contents = setup
            .list_directory_contents("dir1")
            .await
            .expect("Failed to list dir1");
        assert!(
            dir1_contents.contains(&"/dir1/subdir".to_string()),
            "dir1 should contain subdir"
        );
    }

    #[tokio::test]
    async fn test_mkdir_root_directory() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Try to create root directory - should succeed as idempotent no-op
        mkdir_command(&setup.ship_context, "/", false)
            .await
            .expect("Creating root directory should succeed (idempotent)");

        // Root should still be accessible
        assert!(
            setup
                .verify_directory_exists("/")
                .await
                .expect("Failed to check root"),
            "Root directory should exist"
        );
    }

    #[tokio::test]
    async fn test_mkdir_absolute_vs_relative_paths() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create directory with absolute path (needs -p for nested creation)
        mkdir_command(&setup.ship_context, "/absolute/path", true)
            .await
            .expect("Failed to create absolute path");

        // Create directory with relative path (needs -p for nested creation)
        mkdir_command(&setup.ship_context, "relative/path", true)
            .await
            .expect("Failed to create relative path");

        // Both should exist
        assert!(
            setup
                .verify_directory_exists("/absolute/path")
                .await
                .expect("Failed to check absolute"),
            "Absolute path should exist"
        );
        assert!(
            setup
                .verify_directory_exists("relative/path")
                .await
                .expect("Failed to check relative"),
            "Relative path should exist"
        );
    }

    #[tokio::test]
    async fn test_mkdir_empty_path() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Try to create directory with empty path (root directory)
        // Should succeed as idempotent no-op (root already exists)
        mkdir_command(&setup.ship_context, "", false)
            .await
            .expect("Creating root directory should succeed (idempotent)");
    }

    #[tokio::test]
    async fn test_mkdir_special_characters() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create directories with special characters in names
        mkdir_command(&setup.ship_context, "dir-with-dashes", false)
            .await
            .expect("Failed to create dir with dashes");
        mkdir_command(&setup.ship_context, "dir_with_underscores", false)
            .await
            .expect("Failed to create dir with underscores");
        mkdir_command(&setup.ship_context, "dir.with.dots", false)
            .await
            .expect("Failed to create dir with dots");

        // Verify all exist
        assert!(
            setup
                .verify_directory_exists("dir-with-dashes")
                .await
                .expect("Failed to check dashes"),
            "Directory with dashes should exist"
        );
        assert!(
            setup
                .verify_directory_exists("dir_with_underscores")
                .await
                .expect("Failed to check underscores"),
            "Directory with underscores should exist"
        );
        assert!(
            setup
                .verify_directory_exists("dir.with.dots")
                .await
                .expect("Failed to check dots"),
            "Directory with dots should exist"
        );
    }

    #[tokio::test]
    async fn test_mkdir_transaction_rollback_on_error() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create a file to cause conflict
        setup
            .create_pond_file("conflict", "content")
            .await
            .expect("Failed to create conflict file");

        // Try to create directory over the file - should fail and rollback
        let result = mkdir_command(&setup.ship_context, "conflict", false).await;

        assert!(result.is_err(), "mkdir should fail on conflict");

        // Verify the file still exists and is not a directory
        assert!(
            !setup
                .verify_directory_exists("conflict")
                .await
                .expect("Failed to check conflict"),
            "Conflict path should not be a directory after failed mkdir"
        );

        // The original file should still be there (transaction rolled back)
        let contents = setup
            .list_directory_contents("/")
            .await
            .expect("Failed to list root");
        assert!(
            contents.contains(&"/conflict".to_string()),
            "Original file should still exist after failed mkdir"
        );
    }
}
