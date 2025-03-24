use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::path::{Component, Path};
use std::rc::Rc;

#[derive(Debug)]
pub enum FSError {
    NotFound,
    NotADirectory,
    InvalidPath,
    AlreadyExists,
}

impl fmt::Display for FSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FSError::NotFound => write!(f, "Path not found"),
            FSError::NotADirectory => write!(f, "Not a directory"),
            FSError::InvalidPath => write!(f, "Invalid path"),
            FSError::AlreadyExists => write!(f, "Entry already exists"),
        }
    }
}

impl std::error::Error for FSError {}

pub type Result<T> = std::result::Result<T, FSError>;

// NodeID is a reference to a node in the FS
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeID(pub usize);

// Node trait that both File and Directory implement
pub trait Node {
    fn is_file(&self) -> bool;
    fn is_directory(&self) -> bool;
    fn as_file(&self) -> Option<&File> { None }
    fn as_directory(&self) -> Option<&Directory> { None }
    fn as_directory_mut(&mut self) -> Option<&mut Directory> { None }
}

// File implementation
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

// Directory implementation
pub struct Directory {
    entries: HashMap<String, NodeID>,
}

impl Directory {
    pub fn new() -> Self {
        Directory {
            entries: HashMap::new(),
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

// The file system
pub struct FS {
    nodes: Vec<Rc<RefCell<Box<dyn Node>>>>,
}

// Working directory context
pub struct WD<'a> {
    dir_id: NodeID,
    fs: &'a mut FS,
}

// Handle to a resolved path
pub enum Handle {
    Found(NodeID),
    NotFound(String),
}

impl Handle {
    pub fn is_none(&self) -> bool {
        matches!(self, Handle::NotFound(_))
    }
    
    pub fn create_file(&self, content: &str) -> Result<NodeID> {
        match self {
            Handle::Found(_) => Err(FSError::AlreadyExists),
            Handle::NotFound(_) => Err(FSError::NotFound), // This will be handled by WD context in actual implementation
        }
    }
}

impl FS {
    pub fn new() -> Self {
        let root = Box::new(Directory::new()) as Box<dyn Node>;
        let mut nodes = Vec::new();
        nodes.push(Rc::new(RefCell::new(root)));
        FS { nodes }
    }
    
    pub fn root_id(&self) -> NodeID {
        NodeID(0) // Root is always the first node
    }
    
    pub fn get_node(&self, id: NodeID) -> Rc<RefCell<Box<dyn Node>>> {
        self.nodes[id.0].clone()
    }
    
    pub fn add_node(&mut self, node: Box<dyn Node>) -> NodeID {
        let id = NodeID(self.nodes.len());
        self.nodes.push(Rc::new(RefCell::new(node)));
        id
    }
    
    // Main function to operate on paths
    pub fn in_path<F, T>(&mut self, path: &str, op: F) -> Result<T>
    where
        F: FnOnce(&mut WD, Handle) -> Result<T>,
    {
        let path = Path::new(path);
        self.resolve_path(path, op)
    }
    
    fn resolve_path<F, T>(&mut self, path: &Path, op: F) -> Result<T>
    where
        F: FnOnce(&mut WD, Handle) -> Result<T>,
    {
        let mut components: Vec<_> = path.components().collect();
        
        // Strip root dir if present
        if let Some(Component::RootDir) = components.first() {
            components.remove(0);
        }
        
        // Check for invalid components
        for comp in &components {
            match comp {
                Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                    return Err(FSError::InvalidPath);
                }
                _ => {}
            }
        }
        
        // Special case: path with zero elements refers to root
        if components.is_empty() {
            let root_id = self.root_id();
            let mut wd = WD { dir_id: root_id, fs: self };
            return op(&mut wd, Handle::Found(root_id));
        }
        
        // Recursive path resolution
        self.resolve_components(self.root_id(), &components, op)
    }
    
    fn resolve_components<F, T>(&mut self, dir_id: NodeID, components: &[Component], op: F) -> Result<T>
    where
        F: FnOnce(&mut WD, Handle) -> Result<T>,
    {
        if components.is_empty() {
            let mut wd = WD { dir_id, fs: self };
            return op(&mut wd, Handle::Found(dir_id));
        }
        
        let comp = &components[0];
        let remaining = &components[1..];
        
        if let Component::Normal(name) = comp {
            let name_str = name.to_str().unwrap().to_string(); // Assuming UTF-8
            
            // Get the directory node
            let dir_node = self.get_node(dir_id);
            let dir_borrow = dir_node.borrow();
            
            if !dir_borrow.is_directory() {
                return Err(FSError::NotADirectory);
            }
            
            let dir = dir_borrow.as_directory().unwrap();
            
            if let Some(node_id) = dir.get(&name_str) {
                if remaining.is_empty() {
                    // This is the final component
                    let mut wd = WD { dir_id, fs: self };
                    return op(&mut wd, Handle::Found(node_id));
                } else {
                    // More components to process
                    let node = self.get_node(node_id);
                    let node_ref = node.borrow();
                    
                    if node_ref.is_directory() {
                        // Continue resolution with remaining components
                        return self.resolve_components(node_id, remaining, op);
                    } else {
                        return Err(FSError::NotADirectory);
                    }
                }
            } else if remaining.is_empty() {
                // Final component not found
                let mut wd = WD { dir_id, fs: self };
                return op(&mut wd, Handle::NotFound(name_str));
            } else {
                // Intermediate component not found
                return Err(FSError::NotFound);
            }
        } else {
            // Should not happen due to earlier validation
            return Err(FSError::InvalidPath);
        }
    }
}

// Working directory methods
impl<'a> WD<'a> {
    pub fn create_file(&mut self, name: &str, content: &str) -> Result<NodeID> {
        let dir_node = self.fs.get_node(self.dir_id);
        let mut dir_borrow = dir_node.borrow_mut();
        
        if !dir_borrow.is_directory() {
            return Err(FSError::NotADirectory);
        }
        
        let dir = dir_borrow.as_directory_mut().unwrap();
        
        if dir.get(name).is_some() {
            return Err(FSError::AlreadyExists);
        }
        
        let file = Box::new(File::new(content.as_bytes().to_vec())) as Box<dyn Node>;
        let id = self.fs.add_node(file);
        dir.insert(name.to_string(), id);
        
        Ok(id)
    }
}

// Extend Handle to work with the example
impl Handle {
    pub fn create_file(&self, content: &str) -> Result<NodeID> {
        // This is a placeholder that will be modified by the in_path function
        // The actual implementation depends on having access to the WD context
        match self {
            Handle::NotFound(name) => {
                // This will be replaced by proper implementation in the in_path closure
                Ok(NodeID(0))
            },
            Handle::Found(_) => Err(FSError::AlreadyExists),
        }
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
