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
}

impl fmt::Display for FSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FSError::NotFound(path) => write!(f, "Path not found: {}", path.display()),
            FSError::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
            FSError::InvalidPath(path) => write!(f, "Invalid path: {}", path.display()),
            FSError::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
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
pub trait Node {
    fn is_file(&self) -> bool;
    fn is_directory(&self) -> bool;
    fn as_file(&self) -> Option<&File> { None }
    fn as_directory(&self) -> Option<&Directory> { None }
    fn as_directory_mut(&mut self) -> Option<&mut Directory> { None }
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

        // Resolve the path components starting from root
        self.resolve_components(ROOT_DIR, components_iter, op)
    }
    
    /// Recursively resolves path components and calls the operation when done
    fn resolve_components<'a, F, T, I>(&mut self, dir_id: NodeID, mut components: Peekable<I>, op: F) -> Result<T>
    where
        F: FnOnce(&mut WD, Handle) -> Result<T>,
        I: Iterator<Item = Component<'a>>,
    {        
        match components.next().unwrap() {
            Component::Normal(name) => {
                let name_str = name.to_str().unwrap().to_string(); // Assuming UTF-8
                
                let is_final = components.peek().is_none();
                
                match (self.dir_get(dir_id, &name_str)?, is_final) {
                    (Some(node_id), true) => {
                        // Final component found - execute operation
                        let mut wd = WD { dir_id, fs: self };
                        op(&mut wd, Handle::Found(node_id))
                    },
                    (Some(node_id), false) => {
                        // Intermediate directory - continue traversal
                        self.get_node(node_id)
                            .borrow()
                            .as_directory()
                            .ok_or(FSError::NotADirectory(PathBuf::from(name_str)))
                            .and_then(|_| self.resolve_components(node_id, components, op))
                    },
                    (None, true) => {
                        // Final component not found - may be creating a new entry
                        let mut wd = WD { dir_id, fs: self };
                        op(&mut wd, Handle::NotFound(name_str))
                    },
                    (None, false) => {
                        // Intermediate component not found - path error
                        Err(FSError::NotFound(PathBuf::from(name_str)))
                    }
                }
            },
            _ => Err(FSError::InvalidPath(PathBuf::from(components.collect::<Vec<_>>().iter().map(|c| c.as_os_str().to_str().unwrap()).collect::<Vec<_>>().join("/")))),
        }
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
}
