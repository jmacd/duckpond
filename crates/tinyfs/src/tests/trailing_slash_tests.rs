use crate::async_helpers::convenience;
use crate::memory::new_fs;
/// Tests for trailing slash semantics in WD operations
/// This module tests the specific behavior of trailing slashes in filesystem operations
use crate::*;

#[tokio::test]
async fn test_trailing_slash_detection() -> Result<()> {
    // Test has_trailing_slash helper - we'll test it indirectly through resolve_copy_destination
    // since the helper methods are private

    // We'll test the behavior through the public API
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Test that non-trailing slash paths are detected correctly
    convenience::create_file_path(&root, "file.txt", b"content").await?;

    let result = root.resolve_copy_destination("file.txt").await?;
    assert!(matches!(result.1, CopyDestination::ExistingFile));

    Ok(())
}

#[tokio::test]
async fn test_trailing_slash_stripping() -> Result<()> {
    // Test that trailing slash stripping works correctly through the public API
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create a directory
    root.create_dir_path("testdir").await?;

    // Test that both "testdir" and "testdir/" resolve to the same directory
    let result_no_slash = root.resolve_copy_destination("testdir").await?;
    let result_with_slash = root.resolve_copy_destination("testdir/").await?;

    // Both should resolve to the same directory behavior
    assert!(matches!(
        result_no_slash.1,
        CopyDestination::ExistingDirectory
    ));
    assert!(matches!(result_with_slash.1, CopyDestination::Directory));

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_directory_with_trailing_slash() -> Result<()> {
    // Test that trailing slash forces directory interpretation
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create a test directory
    root.create_dir_path("testdir").await?;

    // Test trailing slash - should resolve to directory
    let result = root.resolve_copy_destination("testdir/").await;
    assert!(result.is_ok());
    let (_, dest_type) = result.unwrap();
    assert!(matches!(dest_type, CopyDestination::Directory));

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_directory_without_trailing_slash() -> Result<()> {
    // Test that existing directory without trailing slash is still treated as directory
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create a test directory
    root.create_dir_path("testdir").await?;

    // Test without trailing slash - should resolve to existing directory
    let result = root.resolve_copy_destination("testdir").await;
    assert!(result.is_ok());
    let (_, dest_type) = result.unwrap();
    assert!(matches!(dest_type, CopyDestination::ExistingDirectory));

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_file_without_trailing_slash() -> Result<()> {
    // Test that existing file without trailing slash is treated as file
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create a test file
    convenience::create_file_path(&root, "testfile.txt", b"content").await?;

    // Test without trailing slash - should resolve to existing file
    let result = root.resolve_copy_destination("testfile.txt").await;
    assert!(result.is_ok());
    let (_, dest_type) = result.unwrap();
    assert!(matches!(dest_type, CopyDestination::ExistingFile));

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_file_with_trailing_slash_fails() -> Result<()> {
    // Test that trailing slash on existing file fails
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create a test file
    convenience::create_file_path(&root, "testfile.txt", b"content").await?;

    // Test with trailing slash - should fail because file is not a directory
    let result = root.resolve_copy_destination("testfile.txt/").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_nonexistent_without_trailing_slash() -> Result<()> {
    // Test that non-existent path without trailing slash creates new path
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Test non-existent path - should resolve to new path
    let result = root.resolve_copy_destination("newfile.txt").await;
    assert!(result.is_ok());
    let (_, dest_type) = result.unwrap();
    assert!(matches!(dest_type, CopyDestination::NewPath(_)));

    if let CopyDestination::NewPath(name) = dest_type {
        assert_eq!(name, "newfile.txt");
    }

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_nonexistent_with_trailing_slash_fails() -> Result<()> {
    // Test that non-existent path with trailing slash fails
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Test non-existent path with trailing slash - should fail
    let result = root.resolve_copy_destination("nonexistent/").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_nested_directory_with_trailing_slash() -> Result<()> {
    // Test trailing slash with nested directories
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create nested directories
    root.create_dir_path("parent").await?;
    let parent = root.get_node_path("parent").await?;
    let parent_wd = fs.wd(&parent).await?;
    parent_wd.create_dir_path("child").await?;

    // Test trailing slash on nested directory
    let result = root.resolve_copy_destination("parent/child/").await;
    assert!(result.is_ok());
    let (_, dest_type) = result.unwrap();
    assert!(matches!(dest_type, CopyDestination::Directory));

    Ok(())
}

#[tokio::test]
async fn test_copy_destination_nested_directory_without_trailing_slash() -> Result<()> {
    // Test no trailing slash with nested directories
    let fs = new_fs().await;
    let root = fs.root().await?;

    // Create nested directories
    root.create_dir_path("parent").await?;
    let parent = root.get_node_path("parent").await?;
    let parent_wd = fs.wd(&parent).await?;
    parent_wd.create_dir_path("child").await?;

    // Test no trailing slash on nested directory
    let result = root.resolve_copy_destination("parent/child").await;
    assert!(result.is_ok());
    let (_, dest_type) = result.unwrap();
    assert!(matches!(dest_type, CopyDestination::ExistingDirectory));

    Ok(())
}
