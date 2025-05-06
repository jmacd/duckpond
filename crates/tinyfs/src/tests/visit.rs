use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use crate::dir::Directory;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::NodeRef;
use crate::NodeType;
use crate::FS;

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
    // @@@ 	Option? No.
    fn get(&self, _name: &str) -> Option<NodeRef> {
        // self.iter()
        //     .filter(|x| name == x)
	// @@@
	None
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
    root.create_dir_path("/a").unwrap();
    root.create_file_path("/a/1.txt", b"Content A").unwrap();
    
    root.create_dir_path("/a/b").unwrap();
    root.create_file_path("/a/b/1.txt", b"Content A-B").unwrap();
    
    root.create_dir_path("/a/c").unwrap();
    root.create_file_path("/a/c/1.txt", b"Content A-C").unwrap();
    
    root.create_dir_path("/b").unwrap();
    root.create_dir_path("/b/a").unwrap();
    root.create_file_path("/b/a/1.txt", b"Content B-A").unwrap();
    
    // Create a virtual directory that matches all "1.txt" files in any subfolder
    root.create_node_path("/visit-test", || {
        NodeType::Directory(VisitDirectory::new_handle(fs.clone(), "/**/1.txt"))
    })
    .unwrap();
    
    // Access the visit directory and check its contents
    let visit_dir = root.open_dir_path("/visit-test").unwrap();
    
    // // Test accessing files through the visit directory
    // let result1 = root.read_file_path("/visit-test/a_1.txt").unwrap();
    // assert_eq!(result1, b"Content A");
    
    // let result2 = root.read_file_path("/visit-test/a_b_1.txt").unwrap();
    // assert_eq!(result2, b"Content A-B");
    
    // let result3 = root.read_file_path("/visit-test/a_c_1.txt").unwrap();
    // assert_eq!(result3, b"Content A-C");
    
    // let result4 = root.read_file_path("/visit-test/b_a_1.txt").unwrap();
    // assert_eq!(result4, b"Content B-A");
    
    // Test iterator functionality of VisitDirectory
    let entries: BTreeSet<_> = visit_dir
        .read_dir()
        .unwrap()
        .into_iter()
        .map(|np| (np.basename(), np.read_file().unwrap()))
        .collect();
    
    let expected = BTreeSet::from([
        ("a_1.txt".to_string(), b"Content A".to_vec()),
        ("a_b_1.txt".to_string(), b"Content A-B".to_vec()),
        ("a_c_1.txt".to_string(), b"Content A-C".to_vec()),
        ("b_a_1.txt".to_string(), b"Content B-A".to_vec()),
    ]);
    
    assert_eq!(entries, expected);
}
