use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
use std::ops::Deref;

/// Represents errors that can occur in filesystem operations
#[derive(Debug, PartialEq)]
pub enum FSError {
    NotFound(PathBuf),
    NotADirectory(PathBuf),
    NotAFile(PathBuf),
    InvalidPath(PathBuf),
    AlreadyExists(PathBuf),
    SymlinkLoop(), // TODO
}

impl fmt::Display for FSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FSError::NotFound(path) => write!(f, "Path not found: {}", path.display()),
            FSError::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
            FSError::NotAFile(path) => write!(f, "Not a file: {}", path.display()),
            FSError::InvalidPath(path) => write!(f, "Invalid path: {}", path.display()),
            FSError::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
            FSError::SymlinkLoop() => write!(f, "Too many symbolic links"),
        }
    }
}

impl std::error::Error for FSError {}

pub type Result<T> = std::result::Result<T, FSError>;

/// Unique identifier for a node in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeID(pub usize);

const ROOT_DIR: NodeID = NodeID(0);

/// Common interface for both files and directories
pub enum Node {
    File(File),
    Directory(Directory),
    Symlink(Symlink),
}

/// Represents a file with binary content
pub struct File {
    content: Vec<u8>,
}

/// Represents a directory containing named entries
pub struct Directory {
    entries: BTreeMap<String, NodeID>,
}

/// Represents a symbolic link to another path
pub struct Symlink {
    target: PathBuf,
}

impl Node {
    pub fn as_file(&self) -> Option<&File> {
	match self {
	    Node::File(f) => Some(f),
	    _ => None,
	}
    }

    pub fn as_symlink(&self) -> Option<&Symlink> {
	match self {
	    Node::Symlink(f) => Some(f),
	    _ => None,
	}
    }
    
    pub fn as_dir(&self) -> Option<&Directory> {
	match self {
	    Node::Directory(d) => Some(d),
	    _ => None,
	}
    }

    pub fn as_dir_mut(&mut self) -> Option<&mut Directory> {
	match self {
	    Node::Directory(d) => Some(d),
	    _ => None,
	}
    }
    
    pub fn as_dir_or_else<F>(&self, or: F) -> Result<&Directory>
    where
	F: FnOnce()->FSError,
    {
	self.as_dir().ok_or_else(or)
    }
}

impl File {
    pub fn new(content: Vec<u8>) -> Node {
        Node::File(File {
	    content,
	})
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

impl Directory {
    pub fn new() -> Node {
        Node::Directory(Directory {
            entries: BTreeMap::new(),
        })
    }

    pub fn get(&self, name: &str) -> Option<NodeID> {
        self.entries.get(name).copied()
    }

    pub fn insert(&mut self, name: String, id: NodeID) -> Option<NodeID> {
        self.entries.insert(name, id)
    }
}

impl Symlink {
    pub fn new(target: PathBuf) -> Node {
        Node::Symlink(Symlink {
	    target,
	})
    }

    pub fn target(&self) -> &Path {
        &self.target
    }
}

/// Main filesystem structure that owns all nodes
pub struct FS {
    nodes: RefCell<Vec<Rc<RefCell<Node>>>>,
}

/// Context for operations within a specific directory
pub struct WD<'a> {
    dir_id: NodeID,
    fs: &'a FS,
}

/// Result of path resolution
pub enum Handle {
    Found(NodeID),
    NotFound(String), // Contains the name of the missing component
}

impl Handle {
    pub fn is_none(&self) -> bool {
        matches!(self, Handle::NotFound(_))
    }

    pub fn is_found(&self) -> bool {
        matches!(self, Handle::Found(_))
    }

    pub fn unwrap_id(&self) -> NodeID {
        match self {
            Handle::Found(id) => *id,
            Handle::NotFound(name) => panic!("Called unwrap_id on NotFound: {}", name),
        }
    }
}

impl FS {
    /// Creates a new filesystem with an empty root directory
    pub fn new() -> Self {
        let root = Directory::new();
        let mut nodes = Vec::new();
        nodes.push(Rc::new(RefCell::new(root)));
        FS {
            nodes: RefCell::new(nodes),
        }
    }

    /// Returns a working directory context for the root directory
    pub fn root(&self) -> WD {
        WD {
            dir_id: ROOT_DIR,
            fs: self,
        }
    }

    /// Retrieves a node by its ID
    fn get_node(&self, id: NodeID) -> Rc<RefCell<Node>> {
        self.nodes.borrow()[id.0].clone()
    }

    /// Adds a new node to the filesystem
    fn add_node(&self, node: Node) -> NodeID {
        let id = NodeID(self.nodes.borrow().len());
        self.nodes.borrow_mut().push(Rc::new(RefCell::new(node)));
        id
    }

    /// Looks up an entry in a directory
    fn dir_lookup(&self, dir_id: NodeID, name: &str) -> Result<Option<NodeID>> {
        self.get_node(dir_id)
            .borrow()
            .as_dir()
            .ok_or_else(|| FSError::NotADirectory(PathBuf::from(name)))
            .map(|dir| dir.get(name))
    }

    /// Opens a directory at the specified path
    pub fn open_dir_path<P>(&self, path: P) -> Result<WD<'_>>
    where
        P: AsRef<Path>,
    {
        // Use a temporary root WD to resolve the path
        let node_id = self.root().resolve_dir_path(path.as_ref())?;

        // Create a new WD with the resolved directory ID
        Ok(WD {
            dir_id: node_id,
            fs: self,
        })
    }
}

/// Working directory methods
impl<'a> WD<'a> {
    /// Performs an operation on a path
    pub fn in_path<F, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(&WD, Handle) -> Result<T>,
        P: AsRef<Path>,
    {
	let (node, handle) = self.resolve(path)?;
	let cd = WD{
	    dir_id: node,
	    fs: self.fs,
	};

	op(&cd, handle)
    }

    fn resolve<P>(&self, path: P) -> Result<(NodeID, Handle)>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
	let mut stack = vec![self.dir_id];
	let mut components = path.components().peekable();

        // Iterate through the components of the path
        for comp in &mut components {
            match comp {
                Component::Prefix(_) => {
                    return Err(FSError::InvalidPath(path.to_path_buf()));
                }
                Component::RootDir => {
                    if self.dir_id != ROOT_DIR {
			// @@@ can a Root element occur after another Root?
                        return Err(FSError::InvalidPath(path.to_path_buf()));
                    }
                    continue;
                }
                Component::CurDir => continue,
                Component::ParentDir => {
                    if stack.is_empty() {
                        return Err(FSError::InvalidPath(path.to_path_buf()));
                    }
                    stack.pop();
                }
                Component::Normal(name) => {
		    let dirid = stack.last().unwrap().clone();
		    let dnode = self.fs.get_node(dirid);
		    let dbor = dnode.borrow();
		    let dir = dbor.as_dir_or_else(|| FSError::NotADirectory(path.to_path_buf()))?; // @@@ this path right?
		    
                    let name = name.to_string_lossy().to_string();

		    match dir.entries.get(&name) { // @@@ how to avoid this new string
			None => {
			    // This is OK in the last position
			    if components.peek().is_some() {
				return Err(FSError::NotFound(path.to_path_buf()));
			    } else {
				return Ok((dirid, Handle::NotFound(name)))
			    }
			},
			Some(cid) => {
			    let cnode = self.fs.get_node(*cid);
			    let cbor = cnode.borrow();
			    let child = cbor.deref();
			    match child {
				Node::Symlink(symlink) => {
				    let (pdid, relp) = normalize(symlink.target(), &stack);
				    let pd = WD{
					dir_id: pdid,
					fs: self.fs,
				    };
				    let (_, han) = pd.resolve(relp)?;
				    match han {
					Handle::Found(tgtid) => {
					    stack.push(tgtid);
					},
					Handle::NotFound(_) => {
					    return Err(FSError::NotFound(symlink.target().to_path_buf()));
					},
				    }				
				},
				_ => {
				    // File or Directory.
				    stack.push(*cid);
				},
			    }
			},
		    }
                }
            }
        }

	Err(FSError::InvalidPath(path.to_path_buf()))
    }

    // Helper method to get directory and validate common conditions
    fn with_directory<F, T>(&self, name: &str, f: F) -> Result<T>
    where
        F: FnOnce(&mut Directory, &FS) -> Result<T>,
    {
        self.fs
            .get_node(self.dir_id)
            .borrow_mut()
            .as_dir_mut()
            .ok_or_else(|| FSError::NotADirectory(PathBuf::from(name)))
            .and_then(|dir| {
                dir.get(name).map_or(f(dir, self.fs), |_| {
                    Err(FSError::AlreadyExists(PathBuf::from(name)))
                })
            })
    }

    /// Creates a new file in the current working directory
    pub fn create_file(&self, name: &str, content: &str) -> Result<NodeID> {
        self.with_directory(name, |dir, fs| {
            let file = File::new(content.as_bytes().to_vec());
            let id = fs.add_node(file);
            dir.insert(name.to_string(), id);
            Ok(id)
        })
    }

    /// Creates a new symlink in the current working directory
    pub fn create_symlink(&self, name: &str, target: &Path) -> Result<NodeID> {
        self.with_directory(name, |dir, fs| {
            let symlink = Symlink::new(target.to_path_buf());
            let id = fs.add_node(symlink);
            dir.insert(name.to_string(), id);
            Ok(id)
        })
    }

    /// Creates a new directory in the current working directory
    pub fn create_dir(&self, name: &str) -> Result<NodeID> {
        self.with_directory(name, |dir, fs| {
            let new_dir = Directory::new();
            let id = fs.add_node(new_dir);
            dir.insert(name.to_string(), id);
            Ok(id)
        })
    }

    /// Creates a file at the specified path
    pub fn create_file_path<P>(&self, path: P, content: &str) -> Result<NodeID>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Handle::NotFound(name) => wd.create_file(&name, content),
            Handle::Found(_) => Err(FSError::AlreadyExists(path.as_ref().to_path_buf())),
        })
    }

    /// Creates a symlink at the specified path
    pub fn create_symlink_path<P, T>(&self, path: P, target: T) -> Result<NodeID>
    where
        P: AsRef<Path>,
        T: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Handle::NotFound(name) => wd.create_symlink(&name, target.as_ref()),
            Handle::Found(_) => Err(FSError::AlreadyExists(path.as_ref().to_path_buf())),
        })
    }

    /// Creates a directory at the specified path
    pub fn create_dir_path<P>(&self, path: P) -> Result<NodeID>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Handle::NotFound(name) => wd.create_dir(&name),
            Handle::Found(_) => Err(FSError::AlreadyExists(path.as_ref().to_path_buf())),
        })
    }

    /// Reads the content of a file at the specified path
    pub fn read_file_path<P>(&self, path: P) -> Result<Vec<u8>>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Handle::Found(node_id) => {
                let node = wd.fs.get_node(node_id);
                let node_borrow = node.borrow();
                node_borrow
                    .as_file()
                    .ok_or_else(|| FSError::NotAFile(path.as_ref().to_path_buf()))
                    .map(|file| file.content().to_vec())
            }
            Handle::NotFound(_) => Err(FSError::NotFound(path.as_ref().to_path_buf())),
        })
    }

    /// Opens a directory at the specified path and returns a new working directory for it
    pub fn open_dir_path<P>(&self, path: P) -> Result<WD<'_>>
    where
        P: AsRef<Path>,
    {
        let node_id = self.resolve_dir_path(path.as_ref())?;
        Ok(WD {
            dir_id: node_id,
            fs: self.fs,
        })
    }

    /// Helper method to resolve a directory path to a NodeID
    pub fn resolve_dir_path(&self, path: &Path) -> Result<NodeID> {
        self.in_path(path, |wd, entry| match entry {
            Handle::Found(node_id) => {
                let node = wd.fs.get_node(node_id);
                if node.borrow().as_dir().is_some() {
                    Ok(node_id)
                } else {
                    Err(FSError::NotADirectory(path.to_path_buf()))
                }
            }
            Handle::NotFound(_) => Err(FSError::NotFound(path.to_path_buf())),
        })
    }
}

fn normalize<P>(path: P, stack: &[NodeID]) -> (NodeID, PathBuf) 
where
    P: AsRef<Path>,
{
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_file() {
        let fs = FS::new();

        // Create a file in the root directory
        fs.root().create_file_path("/newfile", "content").unwrap();
    }

    #[test]
    fn test_create_symlink() {
        let fs = FS::new();

        // Create a file
        fs.root()
            .create_file_path("/targetfile", "target content")
            .unwrap();

        // Create a symlink to the file
        fs.root()
            .create_symlink_path("/linkfile", "/targetfile")
            .unwrap();
    }

    #[test]
    fn test_follow_symlink() {
        let fs = FS::new();

        // Create a file
        fs.root()
            .create_file_path("/targetfile", "target content")
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
    fn test_relative_symlink() {
        let fs = FS::new();

        // Create directories
        fs.root().create_dir_path("/a").unwrap();
        fs.root().create_dir_path("/c").unwrap();

        // Create the target file
        fs.root()
            .create_file_path("/c/d", "relative symlink target")
            .unwrap();

        // Create a symlink with a relative path
        fs.root().create_symlink_path("/a/b", "../c/d").unwrap();
        fs.root().create_symlink_path("/a/e", "/c/d").unwrap();

        // Follow the symlink and verify it reaches the target
        let content = fs.root().read_file_path("/a/b").unwrap();
        assert_eq!(content, b"relative symlink target");

        // Open directory "/a" directly
        let wd_a = fs.open_dir_path("/a").unwrap();

        // Attempting to resolve "b" from within "/a" should fail
        // because the symlink target "../c/d" requires backtracking
        let result = wd_a.read_file_path("b");
        assert_eq!(result, Err(FSError::InvalidPath(PathBuf::from("../c/d"))),);

        // Can't read an absolute path except from the root.
        let result = wd_a.read_file_path("e");
        assert_eq!(result, Err(FSError::InvalidPath(PathBuf::from("/c/d"))),);
    }

    #[test]
    fn test_open_dir_path() {
        let fs = FS::new();
        let root = fs.root();

        // Create a directory and a file
        root.create_dir_path("/testdir").unwrap();
        root.create_file_path("/testfile", "content").unwrap();

        // Successfully open a directory
        let wd = fs.open_dir_path("/testdir").unwrap();

        // Create a file inside the opened directory
        wd.create_file("file_in_dir", "inner content").unwrap();

        // Verify we can read the file through the original path
        let content = root.read_file_path("/testdir/file_in_dir").unwrap();
        assert_eq!(content, b"inner content");

        // Trying to open a file as directory should fail
        assert!(matches!(
            root.open_dir_path("/testfile"),
            Err(FSError::NotADirectory(_))
        ));

        // Trying to open a non-existent path should fail
        assert!(matches!(
            root.open_dir_path("/nonexistent"),
            Err(FSError::NotFound(_))
        ));
    }
}
