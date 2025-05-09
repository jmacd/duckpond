use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::path::Path;

use crate::dir::Directory;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::node::NodeRef;
use crate::node::NodeType;
use crate::fs::FS;

/// A directory implementation that derives its contents from wildcard matches
pub struct VisitDirectory {
    fs: FS,
    pattern: String,
}

impl VisitDirectory {
    pub fn new<P: AsRef<str>>(fs: FS, pattern: P) -> Self {
        Self {
            fs,
            pattern: pattern.as_ref().to_string(),
        }
    }

    pub fn new_handle<P: AsRef<str>>(fs: FS, pattern: P) -> DirectoryHandle {
        DirectoryHandle::new(Rc::new(RefCell::new(Box::new(Self::new(fs, pattern)))))
    }    
}

impl Directory for VisitDirectory {
    fn get(&self, name: &str) -> error::Result<Option<NodeRef>> {
	Ok(self.iter()?.find(|(n, _)| n == name).map(|(_, r)| r))
    }

    fn insert(&mut self, name: String, _id: NodeRef) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    fn iter(&self) -> error::Result<Box<dyn Iterator<Item = (String, NodeRef)>>> {
        let items: Vec<_> = self.fs.root().visit::<_, _, (String, NodeRef), Vec<_>>(&self.pattern, |np, captures| {
            let filename = if captures.is_empty() {
                np.basename()
            } else {
                captures.join("_")
            };
            
            Ok((filename, np.node.clone()))
        })?;

        Ok(Box::new(items.into_iter()))
    }
}

#[test]
fn test_visit_directory() {
    // Create a filesystem with some test files
    let fs = FS::new();
    let root = fs.root();
    
    // Create test files in various locations
    root.create_dir_path("/away").unwrap();
    root.create_dir_path("/in").unwrap();
    root.create_dir_path("/in/a").unwrap();
    root.create_file_path("/in/a/1.txt", b"Content A").unwrap();

    root.create_dir_path("/in/a/b").unwrap();
    root.create_file_path("/in/a/b/1.txt", b"Content A-B").unwrap();
    
    root.create_dir_path("/in/a/c").unwrap();
    root.create_file_path("/in/a/c/1.txt", b"Content A-C").unwrap();
    
    root.create_dir_path("/in/b").unwrap();
    root.create_dir_path("/in/b/a").unwrap();
    root.create_file_path("/in/b/a/1.txt", b"Content B-A").unwrap();
    
    // Create a virtual directory that matches all "1.txt" files in any subfolder
    root.create_node_path("/away/visit-test", || {
        NodeType::Directory(VisitDirectory::new_handle(fs.clone(), "/in/**/1.txt"))
    })
    .unwrap();
    
    // Access the visit directory and check its contents
    let visit_dir = root.open_dir_path("/away/visit-test").unwrap();

    // Test accessing files through the visit directory
    let result1 = root.read_file_path("/away/visit-test/a").unwrap();
    assert_eq!(result1, b"Content A");
    
    let result2 = root.read_file_path("/away/visit-test/a_b").unwrap();
    assert_eq!(result2, b"Content A-B");
    
    let result3 = root.read_file_path("/away/visit-test/a_c").unwrap();
    assert_eq!(result3, b"Content A-C");
    
    let result4 = root.read_file_path("/away/visit-test/b_a").unwrap();
    assert_eq!(result4, b"Content B-A");
    
    // Test iterator functionality of VisitDirectory
    let entries: BTreeSet<_> = visit_dir
        .read_dir()
        .unwrap()
        .into_iter()
        .map(|np| (np.basename(), np.read_file().unwrap()))
        .collect();
    
    let expected = BTreeSet::from([
        ("a".to_string(), b"Content A".to_vec()),
        ("a_b".to_string(), b"Content A-B".to_vec()),
        ("a_c".to_string(), b"Content A-C".to_vec()),
        ("b_a".to_string(), b"Content B-A".to_vec()),
    ]);
    
    assert_eq!(entries, expected);
}

#[test]
fn test_visit_directory_loop() {
    let fs = FS::new();
    let root = fs.root();

    root.create_dir_path("/loop").unwrap();
    root.create_file_path("/loop/test.txt", b"Test content").unwrap();
    root.create_node_path("/loop/visit", || {
        NodeType::Directory(VisitDirectory::new_handle(fs.clone(), "/loop/**"))
    })
    .unwrap();
    
    // Should see a VisitLoop error.
    let result: error::Result<Vec<_>> = root.visit("/loop/visit/**", |_, _| Ok(()));
    
    match result {
        Err(error::Error::VisitLoop(p)) => {
	    assert_eq!(p, Path::new("/loop"));
        },
        _ => panic!("Expected VisitLoop error but got a different error: {:?}", result),
    }
}
