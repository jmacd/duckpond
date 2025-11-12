use crate::async_helpers::convenience;
use crate::error::Result;
use crate::memory::new_fs;
use crate::wd::*;
use async_trait::async_trait;
use log::debug;

/// Test visitor that collects just the basenames of matching files
struct BaseNameVisitor {
    pub results: Vec<String>,
}

impl BaseNameVisitor {
    fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

#[async_trait]
impl Visitor<()> for BaseNameVisitor {
    async fn visit(&mut self, node: crate::node::NodePath, _captured: &[String]) -> Result<()> {
        let name = node
            .path()
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "<root>".to_string());
        self.results.push(name);
        Ok(())
    }
}

#[tokio::test]
async fn test_double_wildcard_root_bug() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a test directory structure:
    // /
    // ├── file1.txt
    // ├── file2.txt
    // ├── subdir1/
    // │   ├── file3.txt
    // │   └── file4.txt
    // └── subdir2/
    //     └── file5.txt

    // Create files at root
    _ = convenience::create_file_path(&root, "/file1.txt", b"content1")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/file2.txt", b"content2")
        .await
        .unwrap();

    // Create subdirectories and files
    _ = root.create_dir_path("/subdir1").await.unwrap();
    _ = convenience::create_file_path(&root, "/subdir1/file3.txt", b"content3")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/subdir1/file4.txt", b"content4")
        .await
        .unwrap();

    _ = root.create_dir_path("/subdir2").await.unwrap();
    _ = convenience::create_file_path(&root, "/subdir2/file5.txt", b"content5")
        .await
        .unwrap();

    // Test case 1: The bug - "/**" should match all files recursively
    let mut visitor = BaseNameVisitor::new();
    _ = root.visit_with_visitor("/**", &mut visitor).await.unwrap();

    // This should find all files and directories
    let mut results = visitor.results;
    results.sort();

    debug!("Found items with '/**': {:?}", results);

    // Should find all files at root and in subdirectories
    assert!(
        !results.is_empty(),
        "Bug confirmed: '/**' pattern finds no results"
    );
    assert!(
        results.contains(&"file1.txt".to_string()),
        "Should find file1.txt"
    );
    assert!(
        results.contains(&"file2.txt".to_string()),
        "Should find file2.txt"
    );
    assert!(
        results.contains(&"file3.txt".to_string()),
        "Should find file3.txt"
    );
    assert!(
        results.contains(&"file4.txt".to_string()),
        "Should find file4.txt"
    );
    assert!(
        results.contains(&"file5.txt".to_string()),
        "Should find file5.txt"
    );

    // Test case 2: Compare with working pattern "/**/*.txt"
    let mut visitor2 = BaseNameVisitor::new();
    _ = root
        .visit_with_visitor("/**/*.txt", &mut visitor2)
        .await
        .unwrap();

    let mut results2 = visitor2.results;
    results2.sort();

    debug!("Found items with '/**/*.txt': {:?}", results2);

    // This should work and find all txt files including root level ones
    assert!(!results2.is_empty(), "Comparison pattern should work");
    // **/*.txt should match ALL .txt files recursively (including current directory)
    assert_eq!(
        results2.len(),
        5,
        "/**/*.txt should find all 5 .txt files (root + subdirs)"
    );
    debug!(
        "Success: Found {} txt files with '/**/*.txt', expected 5",
        results2.len()
    );
}

#[tokio::test]
async fn test_double_wildcard_non_root() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create test structure:
    // /
    // └── testdir/
    //     ├── file1.txt
    //     ├── file2.txt
    //     └── subdir/
    //         └── file3.txt

    _ = root.create_dir_path("/testdir").await.unwrap();
    _ = convenience::create_file_path(&root, "/testdir/file1.txt", b"content1")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/testdir/file2.txt", b"content2")
        .await
        .unwrap();
    _ = root.create_dir_path("/testdir/subdir").await.unwrap();
    _ = convenience::create_file_path(&root, "/testdir/subdir/file3.txt", b"content3")
        .await
        .unwrap();

    // Test "testdir/**" - should find all files in testdir recursively
    let mut visitor = BaseNameVisitor::new();
    _ = root
        .visit_with_visitor("testdir/**", &mut visitor)
        .await
        .unwrap();

    let mut results = visitor.results;
    results.sort();

    debug!("Found items with 'testdir/**': {:?}", results);

    // Should find all files in testdir and subdirectories
    assert!(
        !results.is_empty(),
        "Pattern 'testdir/**' should find results"
    );
    assert!(
        results.contains(&"file1.txt".to_string()),
        "Should find file1.txt"
    );
    assert!(
        results.contains(&"file2.txt".to_string()),
        "Should find file2.txt"
    );
    assert!(
        results.contains(&"file3.txt".to_string()),
        "Should find file3.txt"
    );
}

#[tokio::test]
async fn test_single_double_wildcard_patterns() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create files and directories
    _ = convenience::create_file_path(&root, "/root_file.txt", b"root content")
        .await
        .unwrap();
    _ = root.create_dir_path("/dir1").await.unwrap();
    _ = convenience::create_file_path(&root, "/dir1/file1.txt", b"dir1 content")
        .await
        .unwrap();
    _ = root.create_dir_path("/dir1/nested").await.unwrap();
    _ = convenience::create_file_path(&root, "/dir1/nested/deep.txt", b"deep content")
        .await
        .unwrap();

    // Test various single double-wildcard patterns
    let test_cases = vec![
        ("/**", "All files from root"),
        ("**", "All files from root (no leading slash)"),
        ("dir1/**", "All files from dir1"),
    ];

    for (pattern, description) in test_cases {
        let mut visitor = BaseNameVisitor::new();
        _ = root
            .visit_with_visitor(pattern, &mut visitor)
            .await
            .unwrap();

        debug!("{}: {:?}", description, visitor.results);

        // Each pattern should find some results
        assert!(
            !visitor.results.is_empty(),
            "Pattern '{}' ({}) should find results",
            pattern,
            description
        );
    }
}

#[tokio::test]
async fn test_trailing_slash_behavior() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create test structure
    _ = convenience::create_file_path(&root, "/file1.txt", b"content1")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/file2.txt", b"content2")
        .await
        .unwrap();
    _ = root.create_dir_path("/subdir1").await.unwrap();
    _ = root.create_dir_path("/subdir2").await.unwrap();
    _ = convenience::create_file_path(&root, "/subdir1/file3.txt", b"content3")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/subdir2/file4.txt", b"content4")
        .await
        .unwrap();
    _ = root.create_dir_path("/subdir1/nested").await.unwrap();
    _ = convenience::create_file_path(&root, "/subdir1/nested/file5.txt", b"content5")
        .await
        .unwrap();

    // Test different trailing slash patterns
    debug!("=== Testing trailing slash patterns ===");

    // Test 1: /** vs /**/
    let results1 = root.collect_matches("/**").await.unwrap();
    let results2 = root.collect_matches("/**/").await.unwrap();
    debug!("/** found {} items", results1.len());
    debug!("/**/ found {} items", results2.len());

    // Test 2: /subdir1 vs /subdir1/
    let results3 = root.collect_matches("/subdir1").await.unwrap();
    let results4 = root.collect_matches("/subdir1/").await.unwrap();
    debug!("/subdir1 found {} items", results3.len());
    debug!("/subdir1/ found {} items", results4.len());

    // Test 3: /subdir1/* vs /subdir1/*/
    let results5 = root.collect_matches("/subdir1/*").await.unwrap();
    let results6 = root.collect_matches("/subdir1/*/").await.unwrap();
    debug!("/subdir1/* found {} items", results5.len());
    debug!("/subdir1/*/ found {} items", results6.len());

    // Test 4: /**/*.txt vs /**/*.txt/
    let results7 = root.collect_matches("/**/*.txt").await.unwrap();
    let results8 = root.collect_matches("/**/*.txt/").await.unwrap();
    debug!("/**/*.txt found {} items", results7.len());
    debug!("/**/*.txt/ found {} items", results8.len());

    // Print actual results for inspection
    for (i, (node, captured)) in results1.iter().enumerate() {
        debug!("/**[{}]: {} (captured: {:?})", i, node.basename(), captured);
    }

    for (i, (node, captured)) in results2.iter().enumerate() {
        debug!(
            "/**/[{}]: {} (captured: {:?})",
            i,
            node.basename(),
            captured
        );
    }

    for (i, (node, captured)) in results3.iter().enumerate() {
        debug!(
            "/subdir1[{}]: {} (captured: {:?})",
            i,
            node.basename(),
            captured
        );
    }

    for (i, (node, captured)) in results4.iter().enumerate() {
        debug!(
            "/subdir1/[{}]: {} (captured: {:?})",
            i,
            node.basename(),
            captured
        );
    }
}
