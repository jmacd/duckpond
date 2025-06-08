use crate::error::Error;
use crate::fs::FS;
use crate::path::normalize;
use crate::path::strip_root;
use super::super::memory::new_fs;

use std::path::PathBuf;

#[test]
fn test_create_file() {
    let fs = new_fs();

    // Create a file in the root directory
    fs.root().create_file_path("/newfile", b"content").unwrap();

    let content = fs.root().read_file_path("/newfile").unwrap();

    assert_eq!(content, b"content");
}

#[test]
fn test_create_symlink() {
    let fs = new_fs();

    // Create a file
    fs.root()
        .create_file_path("/targetfile", b"target content")
        .unwrap();

    // Create a symlink to the file
    fs.root()
        .create_symlink_path("/linkfile", "/targetfile")
        .unwrap();
}

#[test]
fn test_follow_symlink() {
    let fs = new_fs();

    // Create a file
    fs.root()
        .create_file_path("/targetfile", b"target content")
        .unwrap();

    // Create a symlink to the file
    fs.root()
        .create_symlink_path("/linkfile", "/targetfile")
        .unwrap();

    // Follow the symlink and verify it reaches the target
    let content = fs.root().read_file_path("/linkfile").unwrap();
    assert_eq!(content, b"target content");
}

#[test]
fn test_normalize() {
    let fs = new_fs();
    let root = fs.root();
    let a_node = root.create_dir_path("/a").unwrap();
    let b_node = root.create_dir_path("/a/b").unwrap();

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

#[test]
fn test_relative_symlink() {
    let fs = new_fs();

    // Create directories
    fs.root().create_dir_path("/a").unwrap();
    fs.root().create_dir_path("/c").unwrap();

    // Create the target file
    fs.root()
        .create_file_path("/c/d", b"relative symlink target")
        .unwrap();

    // Create a symlink with a relative path
    fs.root().create_symlink_path("/a/b", "../c/d").unwrap();
    fs.root().create_symlink_path("/a/e", "/c/d").unwrap();

    // Follow the symlink and verify it reaches the target
    let content = fs.root().read_file_path("/a/b").unwrap();
    assert_eq!(content, b"relative symlink target");

    // Open directory "/a" directly
    let wd_a = fs.root().open_dir_path("/a").unwrap();

    // Attempting to resolve "b" from within "/a" should fail
    // because the symlink target "../c/d" requires backtracking
    let result = wd_a.read_file_path("b");
    assert_eq!(result, Err(Error::parent_path_invalid("../c/d")));

    // Can't read an absolute path except from the root.
    let result = wd_a.read_file_path("e");
    assert_eq!(result, Err(Error::root_path_from_non_root("/c/d")));
}

#[test]
fn test_open_dir_path() {
    let fs = new_fs();
    let root = fs.root();

    // Create a directory and a file
    root.create_dir_path("/testdir").unwrap();
    root.create_file_path("/testfile", b"content").unwrap();

    // Successfully open a directory
    let wd = fs.root().open_dir_path("/testdir").unwrap();

    // Create a file inside the opened directory
    wd.create_file_path("file_in_dir", b"inner content")
        .unwrap();

    // Verify we can read the file through the original path
    let content = root.read_file_path("/testdir/file_in_dir").unwrap();
    assert_eq!(content, b"inner content");

    // Trying to open a file as directory should fail
    assert_eq!(
        root.open_dir_path("/testfile"),
        Err(Error::not_a_directory("/testfile"))
    );

    // Trying to open a non-existent path should fail
    assert_eq!(
        root.open_dir_path("/nonexistent"),
        Err(Error::not_found("/nonexistent"))
    );
}

#[test]
fn test_symlink_loop() {
    let fs = new_fs();

    // Create directories to work with
    fs.root().create_dir_path("/dir1").unwrap();
    fs.root().create_dir_path("/dir2").unwrap();

    // Create a circular symlink reference:
    // /dir1/link1 -> /dir2/link2
    // /dir2/link2 -> /dir1/link1
    fs.root()
        .create_symlink_path("/dir1/link1", "../dir2/link2")
        .unwrap();
    fs.root()
        .create_symlink_path("/dir2/link2", "../dir1/link1")
        .unwrap();

    // Attempt to access through the symlink loop
    let result = fs.root().read_file_path("/dir1/link1");

    // Verify we get a SymlinkLoop error
    assert_eq!(result, Err(Error::symlink_loop("../dir2/link2")));

    // Test a more complex loop
    fs.root().create_dir_path("/loop").unwrap();
    fs.root().create_symlink_path("/loop/a", "/loop/b").unwrap();
    fs.root().create_symlink_path("/loop/b", "/loop/c").unwrap();
    fs.root().create_symlink_path("/loop/c", "/loop/d").unwrap();
    fs.root().create_symlink_path("/loop/d", "/loop/e").unwrap();
    fs.root().create_symlink_path("/loop/e", "/loop/f").unwrap();
    fs.root().create_symlink_path("/loop/f", "/loop/g").unwrap();
    fs.root().create_symlink_path("/loop/g", "/loop/h").unwrap();
    fs.root().create_symlink_path("/loop/h", "/loop/i").unwrap();
    fs.root().create_symlink_path("/loop/i", "/loop/j").unwrap();
    fs.root().create_symlink_path("/loop/j", "/loop/a").unwrap();

    // This should exceed the SYMLINK_LOOP_LIMIT (10)
    let result = fs.root().read_file_path("/loop/a");
    assert_eq!(result, Err(Error::symlink_loop("/loop/b")));
}

#[test]
fn test_symlink_to_nonexistent() {
    let fs = new_fs();

    // Create a symlink pointing to a non-existent target
    fs.root()
        .create_symlink_path("/broken_link", "/nonexistent_target")
        .unwrap();

    // Attempt to follow the symlink
    let result = fs.root().read_file_path("/broken_link");

    // Should fail with NotFound error
    assert_eq!(result, Err(Error::not_found("/nonexistent_target")));

    // Test with relative path to non-existent target
    fs.root().create_dir_path("/dir").unwrap();
    fs.root()
        .create_symlink_path("/dir/broken_rel", "../nonexistent_file")
        .unwrap();

    let result = fs.root().read_file_path("/dir/broken_rel");
    assert_eq!(result, Err(Error::not_found("../nonexistent_file")));

    // Test with a chain of symlinks where the last one is broken
    fs.root().create_symlink_path("/link1", "/link2").unwrap();
    fs.root()
        .create_symlink_path("/link2", "/nonexistent_file")
        .unwrap();

    let result = fs.root().read_file_path("/link1");
    assert_eq!(result, Err(Error::not_found("/nonexistent_file")));
}

#[test]
fn test_strip_root() {
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

#[test]
fn test_visit_glob_matching() {
    let fs = new_fs();
    let root = fs.root();

    // Create test directory structure
    root.create_dir_path("/a").unwrap();
    root.create_dir_path("/a/b").unwrap();
    root.create_dir_path("/a/b/c").unwrap();
    root.create_dir_path("/a/d").unwrap();
    root.create_file_path("/a/file1.txt", b"content1").unwrap();
    root.create_file_path("/a/file2.txt", b"content2").unwrap();
    root.create_file_path("/a/other.dat", b"data").unwrap();
    root.create_file_path("/a/b/file3.txt", b"content3")
        .unwrap();
    root.create_file_path("/a/b/c/file4.txt", b"content4")
        .unwrap();
    root.create_file_path("/a/d/file5.txt", b"content5")
        .unwrap();

    // Test case 1: Simple direct match
    let paths: Vec<_> = root
        .visit("/a/file1.txt", |node, _| Ok(node.read_file()?))
        .unwrap();
    assert_eq!(paths, vec![b"content1"]);

    // Test case 2: Multiple match
    let paths: Vec<_> = root
        .visit("/a/file*.txt", |node, _| {
            Ok(node.borrow().read_file()?.to_vec())
        })
        .unwrap();
    assert_eq!(paths, vec![b"content1", b"content2"]);

    // Test case 3: Multiple ** match
    let paths: Vec<_> = root
        .visit("/**/*.txt", |node, _| {
            Ok(node.borrow().read_file()?.to_vec())
        })
        .unwrap();
    assert_eq!(
        paths,
        vec![
            b"content4",
            b"content3",
            b"content5",
            b"content1",
            b"content2"
        ],
    );

    // Test case 4: Single ** match
    let paths: Vec<_> = root
        .visit("/**/file4.txt", |node, _| {
            Ok(node.borrow().read_file()?.to_vec())
        })
        .unwrap();
    assert_eq!(paths, vec![b"content4"]);

    // Test case 5: Single ** match
    let paths: Vec<_> = root
        .visit("/*/*.dat", |node, _| {
            Ok(node.borrow().read_file()?.to_vec())
        })
        .unwrap();
    assert_eq!(paths, vec![b"data"]);
}
