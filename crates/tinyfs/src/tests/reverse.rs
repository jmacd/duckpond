
use std::cell::RefCell;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

use crate::dir::Directory;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::NodeRef;
use crate::NodeType;
use crate::FS;
use std::collections::BTreeSet;

pub struct ReverseDirectory {
    fs: FS,
    target_path: PathBuf,
}

impl ReverseDirectory {
    pub fn new<P: AsRef<Path>>(fs: FS, target_path: P) -> Self {
        Self {
            fs,
            target_path: target_path.as_ref().into(),
        }
    }

    pub fn new_handle<P: AsRef<Path>>(fs: FS, target_path: P) -> DirectoryHandle {
        DirectoryHandle::new(Rc::new(RefCell::new(Box::new(Self::new(fs, target_path)))))
    }
}

impl Directory for ReverseDirectory {
    fn get(&self, name: &str) -> error::Result<Option<NodeRef>> {
        let original_name = reverse_string(name);
        let path = self.target_path.join(&original_name);

        match self.fs.root().get_node_path(&path) {
	    Ok(node) => Ok(Some(node.node)),
	    Err(error::Error::NotFound(_)) => Ok(None),
	    Err(err) => Err(err)
	}
    }

    fn insert(&mut self, name: String, _id: NodeRef) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    fn iter(&self) -> error::Result<Box<dyn Iterator<Item = (String, NodeRef)>>> {
        let sub: Vec<_> = self
            .fs
            .root()
            .open_dir_path(&self.target_path)?
            .read_dir()?
            .into_iter()
            .collect();

        Ok(Box::new(sub.into_iter().map(|np| {
            (reverse_string(&np.basename()), np.node.clone())
        })))
    }
}

fn reverse_string(s: &str) -> String {
    s.chars().rev().collect()
}

#[test]
fn test_reverse_directory() {
    // Create a filesystem with some test files
    let fs = FS::new();
    let root = fs.root();
    root.create_dir_path("/1").unwrap();
    root.create_file_path("/1/hello.txt", b"Hello World")
        .unwrap();
    root.create_file_path("/1/test.bin", b"Binary Data")
        .unwrap();

    root.create_node_path("/2", || {
        NodeType::Directory(ReverseDirectory::new_handle(fs.clone(), "/1"))
    })
    .unwrap();

    // Try to access the reversed filenames through the reverse directory
    let result1 = root.read_file_path("/2/txt.olleh").unwrap();
    assert_eq!(result1, b"Hello World");

    let result2 = root.read_file_path("/2/nib.tset").unwrap();
    assert_eq!(result2, b"Binary Data");

    // Test iterator functionality of ReverseDirectory
    let reverse_dir = root.open_dir_path("/2").unwrap();

    let actual: BTreeSet<_> = reverse_dir
        .read_dir()
        .unwrap()
        .into_iter()
        .map(|np| (np.basename(), np.read_file().unwrap()))
        .collect();

    let expected = BTreeSet::from([
        ("txt.olleh".to_string(), b"Hello World".to_vec()),
        ("nib.tset".to_string(), b"Binary Data".to_vec()),
    ]);
    assert_eq!(actual, expected);
}
