use crate::error::Error;
use crate::fs::FS;
use crate::path::normalize;
use crate::path::strip_root;
use super::super::memory::new_fs;

use std::path::PathBuf;

#[tokio::test]
async fn test_create_file() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file in the root directory
    root.create_file_path("/newfile", b"content").await.unwrap();

    let content = root.read_file_path("/newfile").await.unwrap();

    assert_eq!(content, b"content");
}

#[tokio::test]
async fn test_create_symlink() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file
    root.create_file_path("/targetfile", b"target content").await.unwrap();

    // Create a symlink to the file
    root.create_symlink_path("/linkfile", "/targetfile").await.unwrap();
}

#[tokio::test]
async fn test_follow_symlink() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file
    root.create_file_path("/targetfile", b"target content").await.unwrap();

    // Create a symlink to the file
    root.create_symlink_path("/linkfile", "/targetfile").await.unwrap();

    // Follow the symlink and verify it reaches the target
    let content = root.read_file_path("/linkfile").await.unwrap();
    assert_eq!(content, b"target content");
}

#[tokio::test]
async fn test_normalize() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();
    let a_node = root.create_dir_path("/a").await.unwrap();
    let b_node = root.create_dir_path("/a/b").await.unwrap();

    // Create node stack with actual NodeRefs
    let node_stack = [root.node_path(), a_node.node_path(), b_node.node_path()];

    // Test 1: ../a/../b should normalize to "b" with the a_node as parent
    let (stacklen, path) = normalize("../a/../b", &node_stack).unwrap();
    assert_eq!(stacklen, 2);
    assert_eq!(path, PathBuf::from("b"));

    // Test 2: Multiple parent dirs
    let (stacklen, path) = normalize("../../file.txt", &node_stack).unwrap();
    assert_eq!(stacklen, 1);
    assert_eq!(path, PathBuf::from("file.txt"));

    // Test 3: Current dir components should be ignored
    let (stacklen, path) = normalize("./a/./b", &node_stack).unwrap();
    assert_eq!(stacklen, 3);
    assert_eq!(path, PathBuf::from("a/b"));

    // Test 4: Too many parent dirs should fail
    let result = normalize("../../../too-far", &node_stack);
    assert_eq!(result, Err(Error::parent_path_invalid("../../../too-far")));

    // Test 5: No parent dirs means use current node
    let (stacklen, path) = normalize("just/a/path", &node_stack).unwrap();
    assert_eq!(stacklen, 3);
    assert_eq!(path, PathBuf::from("just/a/path"));
}

#[tokio::test]
async fn test_relative_symlink() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create directories
    root.create_dir_path("/a").await.unwrap();
    root.create_dir_path("/c").await.unwrap();

    // Create the target file
    root.create_file_path("/c/d", b"relative symlink target").await.unwrap();

    // Create a symlink with a relative path
    root.create_symlink_path("/a/b", "../c/d").await.unwrap();
    root.create_symlink_path("/a/e", "/c/d").await.unwrap();

    // Follow the symlink and verify it reaches the target
    let content = root.read_file_path("/a/b").await.unwrap();
    assert_eq!(content, b"relative symlink target");

    // Open directory "/a" directly
    let wd_a = root.open_dir_path("/a").await.unwrap();

    // Attempting to resolve "b" from within "/a" should fail
    // because the symlink target "../c/d" requires backtracking
    let result = wd_a.read_file_path("b").await;
    assert_eq!(result, Err(Error::parent_path_invalid("../c/d")));

    // Can't read an absolute path except from the root.
    let result = wd_a.read_file_path("e").await;
    assert_eq!(result, Err(Error::root_path_from_non_root("/c/d")));
}

#[tokio::test]
async fn test_open_dir_path() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a directory and a file
    root.create_dir_path("/testdir").await.unwrap();
    root.create_file_path("/testfile", b"content").await.unwrap();

    // Successfully open a directory
    let wd = root.open_dir_path("/testdir").await.unwrap();

    // Create a file inside the opened directory
    wd.create_file_path("file_in_dir", b"inner content").await.unwrap();

    // Verify we can read the file through the original path
    let content = root.read_file_path("/testdir/file_in_dir").await.unwrap();
    assert_eq!(content, b"inner content");

    // Trying to open a file as directory should fail
    assert_eq!(
        root.open_dir_path("/testfile").await,
        Err(Error::not_a_directory("/testfile"))
    );

    // Trying to open a non-existent path should fail
    assert_eq!(
        root.open_dir_path("/nonexistent").await,
        Err(Error::not_found("/nonexistent"))
    );
}

#[tokio::test]
async fn test_symlink_loop() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create directories to work with
    root.create_dir_path("/dir1").await.unwrap();
    root.create_dir_path("/dir2").await.unwrap();

    // Create a circular symlink reference:
    // /dir1/link1 -> /dir2/link2
    // /dir2/link2 -> /dir1/link1
    root.create_symlink_path("/dir1/link1", "../dir2/link2").await.unwrap();
    root.create_symlink_path("/dir2/link2", "../dir1/link1").await.unwrap();

    // Attempt to access through the symlink loop
    let result = root.read_file_path("/dir1/link1").await;

    // Verify we get a SymlinkLoop error
    assert_eq!(result, Err(Error::symlink_loop("../dir2/link2")));

    // Test a more complex loop
    root.create_dir_path("/loop").await.unwrap();
    root.create_symlink_path("/loop/a", "/loop/b").await.unwrap();
    root.create_symlink_path("/loop/b", "/loop/c").await.unwrap();
    root.create_symlink_path("/loop/c", "/loop/d").await.unwrap();
    root.create_symlink_path("/loop/d", "/loop/e").await.unwrap();
    root.create_symlink_path("/loop/e", "/loop/f").await.unwrap();
    root.create_symlink_path("/loop/f", "/loop/g").await.unwrap();
    root.create_symlink_path("/loop/g", "/loop/h").await.unwrap();
    root.create_symlink_path("/loop/h", "/loop/i").await.unwrap();
    root.create_symlink_path("/loop/i", "/loop/j").await.unwrap();
    root.create_symlink_path("/loop/j", "/loop/a").await.unwrap();

    // This should exceed the SYMLINK_LOOP_LIMIT (10)
    let result = root.read_file_path("/loop/a").await;
    assert_eq!(result, Err(Error::symlink_loop("/loop/b")));
}

#[tokio::test]
async fn test_symlink_to_nonexistent() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a symlink pointing to a non-existent target
    root.create_symlink_path("/broken_link", "/nonexistent_target").await.unwrap();

    // Attempt to follow the symlink
    let result = root.read_file_path("/broken_link").await;

    // Should fail with NotFound error
    assert_eq!(result, Err(Error::not_found("/nonexistent_target")));

    // Test with relative path to non-existent target
    root.create_dir_path("/dir").await.unwrap();
    root.create_symlink_path("/dir/broken_rel", "../nonexistent_file").await.unwrap();

    let result = root.read_file_path("/dir/broken_rel").await;
    assert_eq!(result, Err(Error::not_found("../nonexistent_file")));

    // Test with a chain of symlinks where the last one is broken
    root.create_symlink_path("/link1", "/link2").await.unwrap();
    root.create_symlink_path("/link2", "/nonexistent_file").await.unwrap();

    let result = root.read_file_path("/link1").await;
    assert_eq!(result, Err(Error::not_found("/nonexistent_file")));
}

#[tokio::test]
async fn test_strip_root() {
    // Test with absolute path
    let path = PathBuf::from("/a/b/c");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from("a/b/c"));

    // Test with relative path (should remain unchanged)
    let path = PathBuf::from("a/b/c");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from("a/b/c"));

    // Test with multiple root components
    let path = PathBuf::from("//a/b");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from("a/b"));

    // Test with just a root component
    let path = PathBuf::from("/");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from(""));
}

#[tokio::test]
async fn test_visit_glob_matching() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create test directory structure
    root.create_dir_path("/a").await.unwrap();
    root.create_dir_path("/a/b").await.unwrap();
    root.create_dir_path("/a/b/c").await.unwrap();
    root.create_dir_path("/a/d").await.unwrap();
    root.create_file_path("/a/file1.txt", b"content1").await.unwrap();
    root.create_file_path("/a/file2.txt", b"content2").await.unwrap();
    root.create_file_path("/a/other.dat", b"data").await.unwrap();
    root.create_file_path("/a/b/file3.txt", b"content3")
        .await.unwrap();
    root.create_file_path("/a/b/c/file4.txt", b"content4")
        .await.unwrap();
    root.create_file_path("/a/d/file5.txt", b"content5")
        .await.unwrap();

    // Test case 1: Simple direct match
    let mut visitor = FileContentVisitor::new();
    root.visit_with_visitor("/a/file1.txt", &mut visitor).await.unwrap();
    assert_eq!(visitor.contents, vec![b"content1"]);

    // Test case 2: Multiple match
    let mut visitor = FileContentVisitor::new();
    root.visit_with_visitor("/a/file*.txt", &mut visitor).await.unwrap();
    assert_eq!(visitor.contents, vec![b"content1", b"content2"]);

    // Test case 3: Multiple ** match
    let mut visitor = FileContentVisitor::new();
    root.visit_with_visitor("/**/*.txt", &mut visitor).await.unwrap();
    assert_eq!(
        visitor.contents,
        vec![
            b"content4",
            b"content3",
            b"content5",
            b"content1",
            b"content2"
        ],
    );

    // Test case 4: Single ** match
    let mut visitor = FileContentVisitor::new();
    root.visit_with_visitor("/**/file4.txt", &mut visitor).await.unwrap();
    assert_eq!(visitor.contents, vec![b"content4"]);

    // Test case 5: Single ** match
    let mut visitor = FileContentVisitor::new();
    root.visit_with_visitor("/*/*.dat", &mut visitor).await.unwrap();
    assert_eq!(visitor.contents, vec![b"data"]);
}

/// Visitor for collecting file contents
struct FileContentVisitor {
    contents: Vec<Vec<u8>>,
}

impl FileContentVisitor {
    fn new() -> Self {
        Self { contents: Vec::new() }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<Vec<u8>> for FileContentVisitor {
    async fn visit(&mut self, node: crate::node::NodePath, _captured: &[String]) -> crate::error::Result<Vec<u8>> {
        let content = node.read_file().await?;
        self.contents.push(content.clone());
        Ok(content)
    }
}
