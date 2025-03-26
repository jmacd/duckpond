use std::cell::RefCell;
use std::collections::BTreeMap;
use std::iter::Peekable;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

/// Represents errors that can occur in filesystem operations
#[derive(Debug)]
pub enum FSError {
    NotFound(PathBuf),
    NotADirectory(PathBuf),
    InvalidPath(PathBuf),
    AlreadyExists(PathBuf),
    SymlinkLoop(),
}

impl fmt::Display for FSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FSError::NotFound(path) => write!(f, "Path not found: {}", path.display()),
            FSError::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
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
    Complete(T),            // Path resolution complete with result
    Backtrack(PathBuf),     // Need to backtrack with a new path
}

/// Common interface for both files and directories
pub trait Node {
    fn is_file(&self) -> bool;
    fn is_directory(&self) -> bool;
    fn is_symlink(&self) -> bool { false }
    fn as_file(&self) -> Option<&File> { None }
    fn as_directory(&self) -> Option<&Directory> { None }
    fn as_directory_mut(&mut self) -> Option<&mut Directory> { None }
    fn as_symlink(&self) -> Option<&Symlink> { None }
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
    fn is_file(&self) -> bool { true }
    fn is_directory(&self) -> bool { false }
    fn as_file(&self) -> Option<&File> { Some(self) }
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
    fn is_file(&self) -> bool { false }
    fn is_directory(&self) -> bool { true }
    fn as_directory(&self) -> Option<&Directory> { Some(self) }
    fn as_directory_mut(&mut self) -> Option<&mut Directory> { Some(self) }
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
    fn is_file(&self) -> bool { false }
    fn is_directory(&self) -> bool { false }
    fn is_symlink(&self) -> bool { true }
    fn as_symlink(&self) -> Option<&Symlink> { Some(self) }
}

/// Main filesystem structure that owns all nodes
pub struct FS {
    nodes: Vec<Rc<RefCell<Box<dyn Node>>>>,
}

/// Context for operations within a specific directory
pub struct WD<'a> {
    dir_id: NodeID,
    fs: &'a mut FS,
}

/// Result of path resolution
pub enum Handle {
    Found(NodeID),
    NotFound(String),  // Contains the name of the missing component
}

impl Handle {
    pub fn is_none(&self) -> bool {
        match self {
            Handle::NotFound(_) => true,
            Handle::Found(_) => false,
        }
    }
}

impl FS {
    /// Creates a new filesystem with an empty root directory
    pub fn new() -> Self {
        let root = Box::new(Directory::new()) as Box<dyn Node>;
        let mut nodes = Vec::new();
        nodes.push(Rc::new(RefCell::new(root)));
        FS { nodes }
    }
    
    /// Retrieves a node by its ID
    fn get_node(&self, id: NodeID) -> Rc<RefCell<Box<dyn Node>>> {
        self.nodes[id.0].clone()
    }
    
    /// Adds a new node to the filesystem
    fn add_node(&mut self, node: Box<dyn Node>) -> NodeID {
        let id = NodeID(self.nodes.len());
        self.nodes.push(Rc::new(RefCell::new(node)));
        id
    }

    /// Looks up an entry in a directory
    fn dir_get(&mut self, dir_id: NodeID, name: &str) -> Result<Option<NodeID>> {
        let dir_node = self.get_node(dir_id);
        let dir_borrow = dir_node.borrow();
        
        if !dir_borrow.is_directory() {
            return Err(FSError::NotADirectory(PathBuf::from(name)));
        }
        
        Ok(dir_borrow.as_directory().unwrap().get(name))
    }
    
    /// Performs an operation on a path
    pub fn in_path<F, P, T>(&mut self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(&mut WD, Handle) -> Result<T>,
        P: AsRef<Path>,
    {
        let mut components_iter = path.as_ref().components().peekable();
        
        // Strip root dir if present
        if let Some(&Component::RootDir) = components_iter.peek() {
            components_iter.next(); // Consume the root component
        }
        
        // Empty paths not allowed - must have at least one component
        if components_iter.peek().is_none() {
            return Err(FSError::InvalidPath(path.as_ref().to_path_buf()))
        }

        // Resolve the path components starting from root with limit of 0 symlinks followed
        match self.resolve_components(ROOT_DIR, components_iter, 0)? {
            PathResolution::Complete((dir_id, handle)) => {
                let mut wd = WD { dir_id, fs: self };
                op(&mut wd, handle)
            },
            PathResolution::Backtrack(_) => Err(FSError::InvalidPath(path.as_ref().to_path_buf())),
        }
    }
    
    /// Recursively resolves path components and calls the operation when done
    fn resolve_components<'a, I>(
        &mut self, 
        dir_id: NodeID, 
        mut components: Peekable<I>, 
        symlink_depth: usize
    ) -> Result<PathResolution<(NodeID, Handle)>>
    where
        I: Iterator<Item = Component<'a>>,
    {        
        const MAX_SYMLINK_DEPTH: usize = 32;
        
        if symlink_depth > MAX_SYMLINK_DEPTH {
            return Err(FSError::SymlinkLoop());
        }
        
        match components.next() {
            Some(Component::Normal(name)) => {
                let name_str = name.to_str().unwrap().to_string(); // Assuming UTF-8
                
                let is_final = components.peek().is_none();
                
                let node_id = self.dir_get(dir_id, &name_str)?;
                
                // Handle the case where node doesn't exist
                if node_id.is_none() {
                    return if is_final {
                        // Final component not found - may be creating a new entry
                        Ok(PathResolution::Complete((dir_id, Handle::NotFound(name_str))))
                    } else {
                        // Intermediate component not found - path error
                        Err(FSError::NotFound(PathBuf::from(name_str)))
                    };
                }
                
                // At this point, we have a node_id
		        let node_id = node_id.unwrap();
                let node = self.get_node(node_id);
                let node_borrow = node.borrow();
                
                // Check if it's a symlink - common for both final and intermediate components
                if node_borrow.is_symlink() {
                    let symlink = node_borrow.as_symlink().unwrap();
                    let target_path = symlink.target().to_path_buf();
                    
                    // If the target starts with a root, resolve from the root
                    if let Some(Component::RootDir) = symlink.target().components().next() {
			return Ok(PathResolution::Backtrack(symlink.target().to_path_buf()));
                    } else {
                        // Resolve relative to current directory
                        return self.resolve_relative_symlink(dir_id, target_path, components, symlink_depth + 1);
                    }
                }
                
                // Handle non-symlink cases
                if is_final {
                    // Final component found - execute operation
                    Ok(PathResolution::Complete((dir_id, Handle::Found(node_id))))
                } else {
                    // Intermediate directory - continue traversal
                    if !node_borrow.is_directory() {
                        return Err(FSError::NotADirectory(PathBuf::from(name_str)));
                    }
                    
                    // Continue resolution
                    self.resolve_components(node_id, components, symlink_depth)
                }
            },
            Some(Component::RootDir) => {
                self.create_backtrack_path(Component::RootDir, components)
            },
            Some(Component::ParentDir) => {
                self.create_backtrack_path(Component::ParentDir, components)
            },
            Some(comp) => {
                Err(FSError::InvalidPath(PathBuf::from(comp.as_os_str())))
            },
            None => {
                // No more components, we're done
                Ok(PathResolution::Complete((dir_id, Handle::Found(dir_id))))
            }
        }
    }
    
    /// Helper method to resolve a relative symlink
    fn resolve_relative_symlink<'a, I>(
        &mut self,
        dir_id: NodeID,
        target_path: PathBuf,
        mut remaining_components: Peekable<I>,
        symlink_depth: usize
    ) -> Result<PathResolution<(NodeID, Handle)>>
    where
        I: Iterator<Item = Component<'a>>,
    {
        // Combine target path with remaining components
        let mut new_path = target_path.clone();
        for comp in remaining_components.by_ref() {
            new_path.push(comp);
        }
        
        // Start resolution from the current directory
        let components = new_path.components().peekable();
        self.resolve_components(dir_id, components, symlink_depth)
    }

    // Modify this helper method to accept any iterator, not just Peekable
    fn create_backtrack_path<'a, I>(
        &self,
        component: Component<'a>,
        components: I
    ) -> Result<PathResolution<(NodeID, Handle)>>
    where
        I: Iterator<Item = Component<'a>>,
    {
        let mut remaining_path = PathBuf::new();
        remaining_path.push(component);
        for comp in components {
            remaining_path.push(comp);
        }
        Ok(PathResolution::Backtrack(remaining_path))
    }
}

/// Working directory methods
impl<'a> WD<'a> {
    /// Creates a new file in the current working directory
    pub fn create_file(&mut self, name: &str, content: &str) -> Result<NodeID> {
        let dir_node = self.fs.get_node(self.dir_id);
        let mut dir_borrow = dir_node.borrow_mut();
        
        if !dir_borrow.is_directory() {
            return Err(FSError::NotADirectory(PathBuf::from(name)));
        }
        
        let dir = dir_borrow.as_directory_mut().unwrap();
        
        if dir.get(name).is_some() {
            return Err(FSError::AlreadyExists(PathBuf::from(name)));
        }
        
        let file = Box::new(File::new(content.as_bytes().to_vec())) as Box<dyn Node>;
        let id = self.fs.add_node(file);
        dir.insert(name.to_string(), id);
        
        Ok(id)
    }
    
    /// Creates a new symlink in the current working directory
    pub fn create_symlink(&mut self, name: &str, target: &Path) -> Result<NodeID> {
        let dir_node = self.fs.get_node(self.dir_id);
        let mut dir_borrow = dir_node.borrow_mut();
        
        if !dir_borrow.is_directory() {
            return Err(FSError::NotADirectory(PathBuf::from(name)));
        }
        
        let dir = dir_borrow.as_directory_mut().unwrap();
        
        if dir.get(name).is_some() {
            return Err(FSError::AlreadyExists(PathBuf::from(name)));
        }
        
        let symlink = Box::new(Symlink::new(target.to_path_buf())) as Box<dyn Node>;
        let id = self.fs.add_node(symlink);
        dir.insert(name.to_string(), id);
        
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_file() {
        let mut fs = FS::new();
        
        // Create a file in the root directory
        let result = fs.in_path("/newfile", |wd, entry| {
            assert!(entry.is_none());
            match &entry {
                Handle::NotFound(name) => wd.create_file(name, "content"),
                _ => unreachable!()
            }
        });
        
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_create_symlink() {
        let mut fs = FS::new();
        
        // Create a file
        let file_result = fs.in_path("/targetfile", |wd, entry| {
            match &entry {
                Handle::NotFound(name) => wd.create_file(name, "target content"),
                _ => unreachable!()
            }
        });
        assert!(file_result.is_ok());
        
        // Create a symlink to the file
        let symlink_result = fs.in_path("/linkfile", |wd, entry| {
            match &entry {
                Handle::NotFound(name) => wd.create_symlink(name, Path::new("/targetfile")),
                _ => unreachable!()
            }
        });
        assert!(symlink_result.is_ok());
    }
    
    #[test]
    fn test_follow_symlink() {
        let mut fs = FS::new();
        
        // Create a file
        fs.in_path("/targetfile", |wd, entry| {
            match &entry {
                Handle::NotFound(name) => wd.create_file(name, "target content"),
                _ => unreachable!()
            }
        }).unwrap();
        
        // Create a symlink to the file
        fs.in_path("/linkfile", |wd, entry| {
            match &entry {
                Handle::NotFound(name) => wd.create_symlink(name, Path::new("/targetfile")),
                _ => unreachable!()
            }
        }).unwrap();
        
        // Follow the symlink and verify it reaches the target
        let result = fs.in_path("/linkfile", |_wd, entry| {
            match entry {
                Handle::Found(node_id) => {
                    let node = _wd.fs.get_node(node_id);
                    let node_borrow = node.borrow();
                    if node_borrow.is_file() {
                        let file = node_borrow.as_file().unwrap();
                        assert_eq!(file.content(), b"target content");
                        Ok(())
                    } else {
                        panic!("Expected a file");
                    }
                },
                _ => panic!("Expected to find the file")
            }
        });
        
        assert!(result.is_err(), "Symlink should be automatically followed");
    }
}
