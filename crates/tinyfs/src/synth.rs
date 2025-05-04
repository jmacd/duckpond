use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;
use std::path::Path;

use crate::FS;
use crate::dir::Directory;
use crate::NodeRef;
use crate::error;
use crate::dir::Handle as DirectoryHandle;

pub struct SyntheticDirectory {
    fs: FS,
    target_path: PathBuf,
}

impl SyntheticDirectory {
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

impl Directory for SyntheticDirectory {
    fn get(&self, name: &str) -> Option<NodeRef> {
        let original_name = reverse_string(name);
        let path = self.target_path.join(&original_name);
        
        self.fs.root().get_node_path(&path).ok().map(|x| x.node)
    }
    
    fn insert(&mut self, _name: String, _id: NodeRef) -> error::Result<()> {
        Err(error::Error::parent_path_invalid(&self.target_path))
    }
    
    fn iter(&self) -> error::Result<Box<dyn Iterator<Item = (String, NodeRef)>>> {
        self.fs.root()
            .open_dir_path(&self.target_path)
            .and_then(|dir| dir.iter())
            .map(|entries| -> Box<dyn Iterator<Item = (String, NodeRef)>> {
		Box::new(entries.map(|np| (reverse_string(&np.basename()), np.node)))
	    })
    }
}

fn reverse_string(s: &str) -> String {
    s.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FS, NodeType};
    use std::collections::BTreeSet;
    
    #[test]
    fn test_synthetic_directory() {
        // Create a filesystem with some test files
        let fs = FS::new();
        let root = fs.root();
        root.create_dir_path("/1").unwrap();
        root.create_file_path("/1/hello.txt", b"Hello World").unwrap();
        root.create_file_path("/1/test.bin", b"Binary Data").unwrap();

        root.create_node_path(
            "/2",
            || NodeType::Directory(SyntheticDirectory::new_handle(fs.clone(), "/1")),
        ).unwrap();

        // Try to access the reversed filenames through the synthetic directory
        let result1 = root.read_file_path("/2/txt.olleh").unwrap();
        assert_eq!(result1, b"Hello World");
        
        let result2 = root.read_file_path("/2/nib.tset").unwrap();
        assert_eq!(result2, b"Binary Data");
        
        // Test iterator functionality of SyntheticDirectory
        let synthetic_dir = root.open_dir_path("/2").unwrap();
        
        let actual: BTreeSet<_> = synthetic_dir.iter().unwrap().
	    map(|np| (np.basename(), np.read_file().unwrap())).collect();

	
        let expected = BTreeSet::from([("txt.olleh".to_string(), b"Hello World".to_vec()),
				       ("nib.tset".to_string(), b"Binary Data".to_vec())]);
        assert_eq!(actual, expected);
    }
}
