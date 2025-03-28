use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::iter::Peekable;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

/// Represents errors that can occur in filesystem operations
#[derive(Debug)]
pub enum FSError {
    NotFound(PathBuf),
    NotADirectory(PathBuf),
    NotAFile(PathBuf),
    InvalidPath(PathBuf),
    AlreadyExists(PathBuf),
    SymlinkLoop(),
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

/// Enum representing the result of path resolution
pub enum PathResolution<T> {
    Complete(T),        // Path resolution complete with result
    Backtrack(PathBuf), // Need to backtrack with a new path
}

/// Common interface for both files and directories
pub trait Node {
    fn is_file(&self) -> bool {
        false
    }
    fn is_directory(&self) -> bool {
        false
    }
    fn is_symlink(&self) -> bool {
        false
    }
    fn as_file(&self) -> Option<&File> {
        None
    }
    fn as_directory(&self) -> Option<&Directory> {
        None
    }
    fn as_directory_mut(&mut self) -> Option<&mut Directory> {
        None
    }
    fn as_symlink(&self) -> Option<&Symlink> {
        None
    }
}

/// Represents a file with binary content
pub struct File {
    content: Vec<u8>,
}

impl File {
    pub fn new(content: Vec<u8>) -> Self {
        File { content }
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

impl Node for File {
    fn is_file(&self) -> bool {
        true
    }
    fn as_file(&self) -> Option<&File> {
        Some(self)
    }
}

/// Represents a directory containing named entries
pub struct Directory {
    entries: BTreeMap<String, NodeID>,
}

impl Directory {
    pub fn new() -> Self {
        Directory {
            entries: BTreeMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Option<NodeID> {
        self.entries.get(name).copied()
    }

    pub fn insert(&mut self, name: String, id: NodeID) -> Option<NodeID> {
        self.entries.insert(name, id)
    }
}

impl Node for Directory {
    fn is_directory(&self) -> bool {
        true
    }
    fn as_directory(&self) -> Option<&Directory> {
        Some(self)
    }
    fn as_directory_mut(&mut self) -> Option<&mut Directory> {
        Some(self)
    }
}

/// Represents a symbolic link to another path
pub struct Symlink {
    target: PathBuf,
}

impl Symlink {
    pub fn new(target: PathBuf) -> Self {
        Symlink { target }
    }

    pub fn target(&self) -> &Path {
        &self.target
    }
}

impl Node for Symlink {
    fn is_symlink(&self) -> bool {
        true
    }
    fn as_symlink(&self) -> Option<&Symlink> {
        Some(self)
    }
}

/// Main filesystem structure that owns all nodes
pub struct FS {
    nodes: RefCell<Vec<Rc<RefCell<Box<dyn Node>>>>>,
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
        let root = Box::new(Directory::new()) as Box<dyn Node>;
        let mut nodes = Vec::new();
        nodes.push(Rc::new(RefCell::new(root)));
        FS { nodes: RefCell::new(nodes) }
    }

    /// Returns a working directory context for the root directory
    pub fn root(&self) -> WD {
        WD {
            dir_id: ROOT_DIR,
            fs: self,
        }
    }

    /// Retrieves a node by its ID
    fn get_node(&self, id: NodeID) -> Rc<RefCell<Box<dyn Node>>> {
        self.nodes.borrow()[id.0].clone()
    }

    /// Adds a new node to the filesystem
    fn add_node(&self, node: Box<dyn Node>) -> NodeID {
        let id = NodeID(self.nodes.borrow().len());
        self.nodes.borrow_mut().push(Rc::new(RefCell::new(node)));
        id
    }

    /// Looks up an entry in a directory
    fn dir_get(&self, dir_id: NodeID, name: &str) -> Result<Option<NodeID>> {
        self.get_node(dir_id)
            .borrow()
            .as_directory()
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
        let mut path = path.as_ref().to_path_buf();

        loop {
            let mut components_iter = path.components().peekable();

            // If we are in the root directory, skip leading RootDir components,
            // otherwise a leading RootDir is invalid.
            while components_iter.peek() == Some(&Component::RootDir) {
                if self.dir_id == ROOT_DIR {
                    components_iter.next();
                } else {
                    return Err(FSError::InvalidPath(path));
                }
            }

            if components_iter.peek().is_none() {
                return Err(FSError::InvalidPath(path));
            }
            let resolution = self.resolve_components(self.dir_id, components_iter, 0)?;

            match resolution {
                PathResolution::Complete((dir_id, handle)) => {
                    let wd = WD {
                        dir_id,
                        fs: self.fs,
                    };
                    return op(&wd, handle);
                }
                PathResolution::Backtrack(new_path) => {
                    // If the leading component of new_path is a ParentDir, consume it and continue
                    let mut new_components = new_path.components().peekable();
                    match new_components.peek() {
                        Some(Component::ParentDir) => {
                            new_components.next();
                            path = new_components.collect();
                        }
                        _ => {
                            path = new_components.collect();
                        }
                    };
                }
            };
        }
    }

    /// Recursively resolves path components and calls the operation when done
    fn resolve_components<'p, I>(
        &self,
        dir_id: NodeID,
        mut components: Peekable<I>,
        symlink_depth: usize,
    ) -> Result<PathResolution<(NodeID, Handle)>>
    where
        I: Iterator<Item = Component<'p>>,
    {
        const MAX_SYMLINK_DEPTH: usize = 32;

        if symlink_depth > MAX_SYMLINK_DEPTH {
            return Err(FSError::SymlinkLoop());
        }

        let component = match components.next() {
            Some(comp) => comp,
            None => {
                // No more components: this has to be a single-component path.
                return Ok(PathResolution::Complete((dir_id, Handle::Found(dir_id))));
            }
        };

        match component {
            Component::Normal(name) => {
                let name_str = name.to_str().unwrap().to_string(); // Assuming UTF-8

                let is_final = components.peek().is_none();

                // Get node ID then use map_or_else to handle both cases
                self.fs.dir_get(dir_id, &name_str)?.map_or_else(
                    || {
                        if is_final {
                            // Final component not found - may be creating a new entry
                            Ok(PathResolution::Complete((
                                dir_id,
                                Handle::NotFound(name_str.clone()),
                            )))
                        } else {
                            // Intermediate component not found - path error
                            Err(FSError::NotFound(PathBuf::from(name_str.clone())))
                        }
                    },
                    |node_id| {
                        let node = self.fs.get_node(node_id);
                        let node_borrow = node.borrow();

                        if let Some(symlink) = node_borrow.as_symlink() {
                            let target_path = symlink.target().to_path_buf();

                            // Create a path combining the symlink target with the remaining components
                            let combined_path =
                                Self::append_components_to_path(target_path.clone(), components);

                            // Check the first component of the symlink target to determine how to proceed
                            match symlink.target().components().next() {
                                Some(Component::RootDir) | Some(Component::ParentDir) => {
                                    // For absolute paths or parent dir, use backtrack
                                    Ok(PathResolution::Backtrack(combined_path))
                                }
                                _ => {
                                    // For relative paths (that aren't parent dir), resolve from current directory
                                    self.resolve_relative_symlink(
                                        dir_id,
                                        combined_path,
                                        symlink_depth + 1,
                                    )
                                }
                            }
                        } else if is_final {
                            // Final component found - execute operation
                            Ok(PathResolution::Complete((dir_id, Handle::Found(node_id))))
                        } else {
                            // Create a new PathBuf for error handling to avoid borrowing name_str
                            let path_for_error = PathBuf::from(name_str.clone());

                            // Use and_then to chain the directory check with continued traversal
                            node_borrow
                                .is_directory()
                                .then_some(())
                                .ok_or_else(|| FSError::NotADirectory(path_for_error))
                                .and_then(|_| {
                                    self.resolve_components(node_id, components, symlink_depth)
                                        .and_then(|resolution| match resolution {
                                            PathResolution::Complete(result) => {
                                                Ok(PathResolution::Complete(result))
                                            }
                                            PathResolution::Backtrack(new_path) => {
                                                // Check the first component of the backtrack path
                                                let mut components = new_path.components();
                                                match components.next() {
                                                    Some(Component::RootDir) => {
                                                        // For RootDir, return the backtrack as is
                                                        Ok(PathResolution::Backtrack(new_path))
                                                    }
                                                    Some(Component::ParentDir) => {
                                                        // For ParentDir, consume that component and continue with remaining components
                                                        self.resolve_components(
                                                            dir_id,
                                                            components.peekable(),
                                                            symlink_depth,
                                                        )
                                                    }
                                                    _ => Ok(PathResolution::Backtrack(new_path)),
                                                }
                                            }
                                        })
                                })
                        }
                    },
                )
            }
            Component::RootDir => self.create_backtrack_path(Component::RootDir, components),
            Component::ParentDir => self.create_backtrack_path(Component::ParentDir, components),
            comp => Err(FSError::InvalidPath(PathBuf::from(comp.as_os_str()))),
        }
    }

    /// Helper method to resolve a relative symlink
    fn resolve_relative_symlink(
        &self,
        dir_id: NodeID,
        new_path: PathBuf,
        symlink_depth: usize,
    ) -> Result<PathResolution<(NodeID, Handle)>>
    {
        // Start resolution from the current directory
        let components = new_path.components().peekable();
        self.resolve_components(dir_id, components, symlink_depth)
    }

    /// Helper function to append path components to a base path
    fn append_components_to_path<'p, I>(base_path: PathBuf, components: I) -> PathBuf
    where
        I: Iterator<Item = Component<'p>>,
    {
        components.fold(base_path, |mut path, comp| {
            path.push(comp);
            path
        })
    }

    // Helper method to create a backtrack path
    fn create_backtrack_path<'p, I>(
        &self,
        component: Component<'p>,
        components: I,
    ) -> Result<PathResolution<(NodeID, Handle)>>
    where
        I: Iterator<Item = Component<'p>>,
    {
        let remaining_path = Self::append_components_to_path(
            PathBuf::new(),
            std::iter::once(component).chain(components),
        );
        Ok(PathResolution::Backtrack(remaining_path))
    }

    // Helper method to get directory and validate common conditions
    fn with_directory<F, T>(&self, name: &str, f: F) -> Result<T>
    where
        F: FnOnce(&mut Directory, &FS) -> Result<T>,
    {
        self.fs
            .get_node(self.dir_id)
            .borrow_mut()
            .as_directory_mut()
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
            let file = Box::new(File::new(content.as_bytes().to_vec())) as Box<dyn Node>;
            let id = fs.add_node(file);
            dir.insert(name.to_string(), id);
            Ok(id)
        })
    }

    /// Creates a new symlink in the current working directory
    pub fn create_symlink(&self, name: &str, target: &Path) -> Result<NodeID> {
        self.with_directory(name, |dir, fs| {
            let symlink = Box::new(Symlink::new(target.to_path_buf())) as Box<dyn Node>;
            let id = fs.add_node(symlink);
            dir.insert(name.to_string(), id);
            Ok(id)
        })
    }

    /// Creates a new directory in the current working directory
    pub fn create_dir(&self, name: &str) -> Result<NodeID> {
        self.with_directory(name, |dir, fs| {
            let new_dir = Box::new(Directory::new()) as Box<dyn Node>;
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
                let is_directory = node.borrow().is_directory();
                
                if is_directory {
                    Ok(node_id)
                } else {
                    Err(FSError::NotADirectory(path.to_path_buf()))
                }
            }
            Handle::NotFound(_) => Err(FSError::NotFound(path.to_path_buf())),
        })
    }
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
        fs.root().create_file_path("/targetfile", "target content").unwrap();
        
        // Create a symlink to the file
        fs.root().create_symlink_path("/linkfile", "/targetfile").unwrap();
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
        fs.root().create_file_path("/c/d", "relative symlink target").unwrap();

        // Create a symlink with a relative path
        fs.root().create_symlink_path("/a/b", "../c/d").unwrap();

        // Follow the symlink and verify it reaches the target
        let content = fs.root().read_file_path("/a/b").unwrap();
        assert_eq!(content, b"relative symlink target");

        // Open directory "/a" directly
        let wd_a = fs.open_dir_path("/a").unwrap();
        
        // Attempting to resolve "b" from within "/a" should fail
        // because the symlink target "../c/d" requires backtracking
        let result = wd_a.read_file_path("b");
        assert!(matches!(
            result, 
            Err(FSError::NotFound(_))),
        );
    }

    #[test]
    fn test_open_dir_path() {
        let fs = FS::new();
        
        // Create a directory and a file
        fs.root().create_dir_path("/testdir").unwrap();
        fs.root().create_file_path("/testfile", "content").unwrap();
        
        // Successfully open a directory
        let wd = fs.open_dir_path("/testdir").unwrap();
        
        // Create a file inside the opened directory
        wd.create_file("file_in_dir", "inner content").unwrap();
        
        // Verify we can read the file through the original path
        let content = fs.root().read_file_path("/testdir/file_in_dir").unwrap();
        assert_eq!(content, b"inner content");
        
        // Trying to open a file as directory should fail
        assert!(matches!(
            fs.root().open_dir_path("/testfile"),
            Err(FSError::NotADirectory(_))
        ));
        
        // Trying to open a non-existent path should fail
        assert!(matches!(
            fs.root().open_dir_path("/nonexistent"),
            Err(FSError::NotFound(_))
        ));
    }
}
