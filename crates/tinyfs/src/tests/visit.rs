use std::collections::BTreeSet;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use futures::stream::{self, Stream};

use crate::dir::Directory;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::fs::FS;
use crate::node::NodeRef;
use crate::node::NodeType;
use super::super::memory::new_fs;

/// A directory implementation that derives its contents from wildcard matches
pub struct VisitDirectory {
    fs: Arc<FS>,
    pattern: String,
    derived_manager: Option<Arc<crate::derived::DerivedFileManager>>,
}

impl VisitDirectory {
    pub fn new<P: AsRef<str>>(fs: Arc<FS>, pattern: P) -> Self {
        Self {
            fs,
            pattern: pattern.as_ref().to_string(),
            derived_manager: None,
        }
    }

    pub fn new_handle<P: AsRef<str>>(fs: Arc<FS>, pattern: P) -> DirectoryHandle {
        DirectoryHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self::new(fs, pattern)))))
    }
    
    async fn get_or_create_derived_manager(&mut self) -> Arc<crate::derived::DerivedFileManager> {
        if let Some(ref manager) = self.derived_manager {
            manager.clone()
        } else {
            let manager = Arc::new(crate::derived::DerivedFileManager::new(self.fs.clone()).await.unwrap());
            self.derived_manager = Some(manager.clone());
            manager
        }
    }
}

/// A visitor that creates filename and node ref pairs for directory iteration
struct FilenameCollector {
    items: Vec<(String, NodeRef)>,
}

impl FilenameCollector {
    fn new() -> Self {
        Self { items: Vec::new() }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<(String, NodeRef)> for FilenameCollector {
    async fn visit(&mut self, node: crate::node::NodePath, captured: &[String]) -> error::Result<(String, NodeRef)> {
        let filename = if captured.is_empty() {
            node.basename()
        } else {
            captured.join("_")
        };
        let result = (filename, node.node.clone());
        self.items.push(result.clone());
        Ok(result)
    }
}

/// A visitor that creates filename and file content pairs
struct FileContentVisitor {
    results: Vec<(String, Vec<u8>)>,
}

impl FileContentVisitor {
    fn new() -> Self {
        Self { results: Vec::new() }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<(String, Vec<u8>)> for FileContentVisitor {
    async fn visit(&mut self, node: crate::node::NodePath, _captured: &[String]) -> error::Result<(String, Vec<u8>)> {
        let result = (node.basename(), node.read_file().await.unwrap());
        self.results.push(result.clone());
        Ok(result)
    }
}

/// A visitor that collects just the basename of files
struct BasenameVisitor {
    results: Vec<String>,
}

impl BasenameVisitor {
    fn new() -> Self {
        Self { results: Vec::new() }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<String> for BasenameVisitor {
    async fn visit(&mut self, node: crate::node::NodePath, _captured: &[String]) -> error::Result<String> {
        let result = node.basename();
        self.results.push(result.clone());
        Ok(result)
    }
}

#[async_trait::async_trait]
impl Directory for VisitDirectory {
    async fn get(&self, name: &str) -> error::Result<Option<NodeRef>> {
        // For now, fall back to the original visitor pattern implementation
        // TODO: Use derived manager for better performance and caching
        let mut visitor = FilenameCollector::new();
        let root = self.fs.root().await?;
        let result = root.visit_with_visitor(&self.pattern, &mut visitor).await;
        result?;
        Ok(visitor.items.into_iter().find(|(n, _)| n == name).map(|(_, r)| r))
    }

    async fn insert(&mut self, name: String, _id: NodeRef) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    async fn entries(&self) -> error::Result<Pin<Box<dyn Stream<Item = error::Result<(String, NodeRef)>> + Send>>> {
        // For now, fall back to the original visitor pattern implementation
        // TODO: Use derived manager for better performance and caching
        let mut visitor = FilenameCollector::new();
        let root = self.fs.root().await?;
        let result = root.visit_with_visitor(&self.pattern, &mut visitor).await;
        result?;
        let items: Vec<_> = visitor.items.into_iter().map(Ok).collect();
        Ok(Box::pin(stream::iter(items)))
    }
}

#[tokio::test]
async fn test_visit_directory() {
    // Create a filesystem with some test files
    let fs = new_fs().await;
    let fs_arc = Arc::new(fs);
    let root = fs_arc.root().await.unwrap();

    // Create test files in various locations
    root.create_dir_path("/away").await.unwrap();
    root.create_dir_path("/in").await.unwrap();
    root.create_dir_path("/in/a").await.unwrap();
    root.create_file_path("/in/a/1.txt", b"Content A").await.unwrap();

    root.create_dir_path("/in/a/b").await.unwrap();
    root.create_file_path("/in/a/b/1.txt", b"Content A-B").await.unwrap();

    root.create_dir_path("/in/a/c").await.unwrap();
    root.create_file_path("/in/a/c/1.txt", b"Content A-C").await.unwrap();

    root.create_dir_path("/in/b").await.unwrap();
    root.create_dir_path("/in/b/a").await.unwrap();
    root.create_file_path("/in/b/a/1.txt", b"Content B-A").await.unwrap();

    // Test the visitor pattern directly first
    let mut visitor = FilenameCollector::new();
    let _result = root.visit_with_visitor("/in/**/1.txt", &mut visitor).await;
    
    // If the direct visitor works, proceed with VisitDirectory
    if visitor.items.is_empty() {
        panic!("Direct visitor pattern failed - no items found for /in/**/1.txt");
    }

    // Create a virtual directory that matches all "1.txt" files in any subfolder
    root.create_node_path("/away/visit-test", || {
        Ok(NodeType::Directory(VisitDirectory::new_handle(fs_arc.clone(), "/in/**/1.txt")))
    }).await.unwrap();

    // Access the visit directory and check its contents
    let visit_dir = root.open_dir_path("/away/visit-test").await.unwrap();

    // Test accessing files through the visit directory
    let result1 = root.read_file_path("/away/visit-test/a").await.unwrap();
    assert_eq!(result1, b"Content A");

    let result2 = root.read_file_path("/away/visit-test/a_b").await.unwrap();
    assert_eq!(result2, b"Content A-B");

    let result3 = root.read_file_path("/away/visit-test/a_c").await.unwrap();
    assert_eq!(result3, b"Content A-C");

    let result4 = root.read_file_path("/away/visit-test/b_a").await.unwrap();
    assert_eq!(result4, b"Content B-A");

    // Test iterator functionality of VisitDirectory
    let mut entries = BTreeSet::new();
    let mut dir_stream = visit_dir.read_dir().await.unwrap();
    use futures::StreamExt;
    while let Some(np) = dir_stream.next().await {
        entries.insert((np.basename(), np.read_file().await.unwrap()));
    }

    let expected = BTreeSet::from([
        ("a".to_string(), b"Content A".to_vec()),
        ("a_b".to_string(), b"Content A-B".to_vec()),
        ("a_c".to_string(), b"Content A-C".to_vec()),
        ("b_a".to_string(), b"Content B-A".to_vec()),
    ]);

    assert_eq!(entries, expected);
}

#[tokio::test]
async fn test_visit_directory_loop() {
    let fs = new_fs().await;
    let fs_arc = Arc::new(fs);
    let root = fs_arc.root().await.unwrap();

    root.create_dir_path("/loop").await.unwrap();
    root.create_file_path("/loop/test.txt", b"Test content").await.unwrap();
    root.create_node_path("/loop/visit", || {
        Ok(NodeType::Directory(VisitDirectory::new_handle(fs_arc.clone(), "/loop/**")))
    }).await.unwrap();

    // Should see a VisitLoop error.
    let mut visitor = crate::wd::CollectingVisitor::new();
    let result = root.visit_with_visitor("/loop/visit/**", &mut visitor).await;

    match result {
        Err(error::Error::VisitLoop(p)) => {
            assert_eq!(p, Path::new("/loop"));
        }
        _ => panic!(
            "Expected VisitLoop error but got a different error: {:?}",
            result
        ),
    }
}

#[tokio::test]
async fn test_visit_with_symlinks() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a directory structure with a symlink
    root.create_dir_path("/a").await.unwrap();
    root.create_dir_path("/a/123456").await.unwrap();
    root.create_file_path("/a/123456/b.txt", b"Symlink test content")
        .await.unwrap();

    // Create a symlink from /a/name -> "123456"
    root.create_symlink_path("/a/name", "123456").await.unwrap();

    // Test visiting with different patterns that should all find b.txt through the symlink

    // Pattern 1: Generic pattern that would find all .txt files
    let mut basename_visitor1 = BasenameVisitor::new();
    root.visit_with_visitor("/**/*.txt", &mut basename_visitor1).await.unwrap();
    let results1 = basename_visitor1.results;
    assert!(results1.contains(&"b.txt".to_string()),
        "Should find b.txt with generic pattern /**/*.txt");

    // Pattern 2: Pattern explicitly going through the symlink
    let mut visitor = FileContentVisitor::new();
    root.visit_with_visitor("/a/name/*.txt", &mut visitor).await.unwrap();
    let results2 = visitor.results;
    assert!(
        results2.contains(&("b.txt".to_string(), b"Symlink test content".to_vec())),
        "Should find b.txt through the symlink with /a/name/*.txt"
    );

    // Pattern 3: Another pattern using a wildcard with the symlink parent
    let mut basename_visitor3 = BasenameVisitor::new();
    root.visit_with_visitor("/*/name/*.txt", &mut basename_visitor3).await.unwrap();
    let results3 = basename_visitor3.results;
    assert!(results3.contains(&"b.txt".to_string()),
        "Should find b.txt through the symlink with /*/name/*.txt");
}
