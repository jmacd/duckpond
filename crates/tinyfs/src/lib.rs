mod dir;
mod error;
mod file;
mod glob;
mod path_utils;
mod symlink;

#[cfg(test)]
mod tests;

use crate::error::Error;
use crate::glob::parse_glob;
use crate::glob::WildcardComponent;
use crate::path_utils::strip_root;
use std::cell::RefCell;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

const ROOT_ID: NodeID = NodeID(0);

pub type Result<T> = error::Result<T>;

/// Unique identifier for a node in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeID(pub usize);

/// Type of node (file, directory, or symlink)
#[derive(Clone)]
pub enum NodeType {
    File(file::Handle),
    Directory(dir::Handle),
    Symlink(symlink::Handle),
}

/// Common interface for both files and directories
#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeID,
    pub node_type: NodeType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeRef(Rc<RefCell<Node>>);

/// Contains a node reference and the path used to reach it
#[derive(Clone, PartialEq)]
pub struct NodePath {
    pub node: NodeRef,
    pub path: PathBuf,
}

pub struct NodePathRef<'a> {
    node: std::cell::Ref<'a, Node>,
    path: &'a PathBuf,
}

struct State {
    nodes: Vec<NodeRef>,
}

/// Main filesystem structure that owns all nodes
#[derive(Clone)]
pub struct FS {
    state: Rc<RefCell<State>>,
}

type DirNode = dir::Pathed<dir::Handle>;
type FileNode = dir::Pathed<file::Handle>;
type SymlinkNode = dir::Pathed<symlink::Handle>;

/// Context for operations within a specific directory
#[derive(Clone)]
pub struct WD {
    np: NodePath,
    dref: DirNode,
    fs: FS,
}

/// Result of path resolution
pub enum NodeHandle {
    Found(NodePath),
    NotFound(PathBuf, String),
}

impl Deref for NodeRef {
    type Target = Rc<RefCell<Node>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl NodePath {
    pub fn basename(&self) -> String {
        path_utils::basename(&self.path).unwrap_or_default()
    }

    pub fn dirname(&self) -> PathBuf {
        path_utils::dirname(&self.path).unwrap_or_default()
    }
    
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn join<P: AsRef<Path>>(&self, p: P) -> PathBuf {
        self.path.clone().join(p)
    }

    pub fn read_file(&self) -> Result<Vec<u8>> {
	self.deref().read_file()
    }

    fn deref(&self) -> NodePathRef {
	NodePathRef{
	    node: self.node.as_ref().borrow(),
	    path: &self.path,
	}
    }
}

impl NodeID {
    pub fn is_root(self) -> bool {
        self == ROOT_ID
    }
}

impl NodePathRef<'_> {
    pub fn as_file(&self) -> Result<FileNode> {
        if let NodeType::File(f) = &self.node.node_type {
            Ok(dir::Pathed::new(self.path, f.clone()))
        } else {
            Err(Error::not_a_file(self.path))
        }
    }

    pub fn as_symlink(&self) -> Result<SymlinkNode> {
        if let NodeType::Symlink(s) = &self.node.node_type {
            Ok(dir::Pathed::new(self.path, s.clone()))
        } else {
            Err(Error::not_a_symlink(self.path))
        }
    }

    pub fn as_dir(&self) -> Result<DirNode> {
        if let NodeType::Directory(d) = &self.node.node_type {
            Ok(dir::Pathed::new(self.path, d.clone()))
        } else {
            Err(Error::not_a_directory(self.path))
        }
    }

    pub fn read_file(&self) -> Result<Vec<u8>> {
	self.as_file()?.read_file()
    }
}

impl FS {
    /// Creates a new filesystem with an empty root directory
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a working directory context for the root directory
    pub fn root(&self) -> WD {
        let root = self.state.deref().borrow().nodes.first().unwrap().clone();
	let node = NodePath {
	    node: root,
	    path: "/".into(),
	};
        self.wd(node).unwrap()
    }

    fn wd(&self, np: NodePath) -> Result<WD> {
        Ok(WD {
	    np: np.clone(),
            dref: np.deref().as_dir()?,
            fs: self.clone(),
        })
    }

    /// Adds a new node to the filesystem
    fn add_node(&self, node_type: NodeType) -> NodeRef {
        let mut state = self.state.borrow_mut();
        let id = NodeID(state.nodes.len());
        let node = NodeRef(Rc::new(RefCell::new(Node { node_type, id })));
        state.nodes.push(node.clone());
        node
    }
}

impl Default for FS {
    /// Creates a new filesystem with an empty root directory
    fn default() -> Self {
        let root = dir::MemoryDirectory::new_handle();
        let node_type = NodeType::Directory(root);
        let nodes = vec![NodeRef(Rc::new(RefCell::new(Node {
            node_type,
            id: ROOT_ID,
        })))];
        FS {
            state: Rc::new(RefCell::new(State { nodes })),
        }
    }
}

impl std::fmt::Debug for FS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FS{{}}")
    }
}

impl PartialEq<FS> for FS {
    fn eq(&self, other: &FS) -> bool {
        std::ptr::eq(self, other)
    }
}

impl std::fmt::Debug for WD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WD{{path:{:?}}}", self.dref.path())
    }
}

impl PartialEq<Node> for Node {
    fn eq(&self, other: &Node) -> bool {
        self.id == other.id
    }
}

impl PartialEq<WD> for WD {
    fn eq(&self, other: &WD) -> bool {
        *self.np.deref().node == *other.np.deref().node
    }
}

impl std::fmt::Debug for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::File(_) => write!(f, "(file)"),
            NodeType::Directory(_) => write!(f, "(directory)"),
            NodeType::Symlink(_) => write!(f, "(symlink)"),
        }
    }
}

impl WD {
    pub fn read_dir(&self) -> error::Result<dir::DuckHandle<'_>> {
	self.dref.read_dir()
    }

    fn is_root(&self) -> bool {
	self.np.node.borrow().id.is_root()
    }
    
    // Generic node creation method for all node types
    fn create_node<T, F>(&self, name: &str, node_creator: F) -> Result<NodePath>
    where
        F: FnOnce() -> T,
        T: Into<NodeType>,
    {
        let node = self.fs.add_node(node_creator().into());
        self.dref.insert(name.to_string(), node.clone())?;
	Ok(NodePath{
	    node,
	    path: self.dref.path().join(name),
	})
    }

    // Generic path-based node creation for all node types
    fn create_node_path<P, T, F>(&self, path: P, node_creator: F) -> Result<NodePath>
    where
        P: AsRef<Path>,
        F: FnOnce() -> T,
        T: Into<NodeType>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            NodeHandle::NotFound(_, name) => wd.create_node(&name, node_creator),
            NodeHandle::Found(_) => Err(Error::already_exists(path.as_ref())),
        })
    }

    pub fn get_node_path<P>(&self, path: P) -> Result<NodePath>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |_wd, entry| match entry {
            NodeHandle::NotFound(_, _) => Err(Error::not_found(path.as_ref())),
            NodeHandle::Found(np) => Ok(np),
        })
    }

    /// Creates a file at the specified path
    pub fn create_file_path<P: AsRef<Path>>(&self, path: P, content: &[u8]) -> Result<NodePath> {
        self.create_node_path(path, || {
            NodeType::File(file::MemoryFile::new_handle(content.to_vec()))
        })
    }

    /// Creates a symlink at the specified path
    pub fn create_symlink_path<P: AsRef<Path>>(&self, path: P, target: P) -> Result<NodePath> {
        let target_path = target.as_ref().to_path_buf();
        self.create_node_path(path, || {
            NodeType::Symlink(symlink::MemorySymlink::new_handle(target_path))
        })
    }

    /// Creates a directory at the specified path
    pub fn create_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let node = self.create_node_path(path, || {
	    NodeType::Directory(dir::MemoryDirectory::new_handle())
	})?;
	self.fs.wd(node)
    }

    /// Reads the content of a file at the specified path
    pub fn read_file_path<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        self.in_path(path.as_ref(), |_, entry| match entry {
            NodeHandle::Found(node) => node.deref().as_file()?.read_file(),
            NodeHandle::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
        })
    }

    /// Opens a directory at the specified path and returns a new working directory for it
    pub fn open_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let path = path.as_ref();
        self.in_path(path, |_, entry| match entry {
            NodeHandle::Found(node) => Ok(self.fs.wd(node)?),
            NodeHandle::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
        })
    }

    /// Performs an operation on a path
    pub fn in_path<F, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(&WD, NodeHandle) -> Result<T>,
        P: AsRef<Path>,
    {
        let stack = vec![self.np.clone()];
        let (node, handle) = self.resolve(&stack, path.as_ref(), 0)?;
        let wd = self.fs.wd(node)?;
        op(&wd, handle)
    }

    fn resolve<P>(
        &self,
        stack_in: &[NodePath],
        path: P,
        depth: u32,
    ) -> Result<(NodePath, NodeHandle)>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let mut stack = stack_in.to_vec();
        let mut components = path.components().peekable();

        // Iterate through the components of the path
        for comp in &mut components {
            match comp {
                Component::Prefix(_) => {
                    return Err(Error::prefix_not_supported(path));
                }
                Component::RootDir => {
                    if !self.np.deref().node.id.is_root() {
                        return Err(Error::root_path_from_non_root(path));
                    }
                    continue;
                }
                Component::CurDir => continue,
                Component::ParentDir => {
                    if stack.len() <= 1 {
                        return Err(Error::parent_path_invalid(path));
                    }
                    stack.pop();
                }
                Component::Normal(name) => {
                    let dnode = stack.last().unwrap().clone();
		    let ddir = dnode.deref().as_dir()?;
                    let name = name.to_string_lossy().to_string();

                    match ddir.get(&name)
                    {
                        None => {
                            // This is OK in the last position
			    if components.peek().is_some() {
                                return Err(Error::not_found(path));
                            } else {
                                return Ok((
                                    dnode,
                                    NodeHandle::NotFound(path.to_path_buf(), name),
                                ));
                            }
                        }
                        Some(child) => {
                            match child.deref().node.node_type {
                                NodeType::Symlink(ref link) => {
                                    let (newsz, relp) = path_utils::normalize(link.readlink()?, &stack)?;
                                    if depth >= symlink::SYMLINK_LOOP_LIMIT {
                                        return Err(Error::symlink_loop(link.readlink()?));
                                    }
                                    let (_, handle) = self.resolve(&stack[0..newsz], relp, depth + 1)?;
				    match handle{
                                        NodeHandle::Found(node) => {
					    stack.push(node);
                                        }
                                        NodeHandle::NotFound(_, _) => {
                                            return Err(Error::not_found(link.readlink()?));
                                        }
                                    }
                                }
                                _ => {
                                    // File or Directory.
				    stack.push(child.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        if stack.len() <= 1 {
            Err(Error::empty_path())
        } else {
            let found = stack.pop().unwrap();
            let dir = stack.pop().unwrap();
            Ok((dir, NodeHandle::Found(found)))
        }
    }

    /// Visits all filesystem entries matching the given wildcard pattern
    pub fn visit<F, P, T, C>(&self, pattern: P, mut callback: F) -> Result<C>
    where
        F: FnMut(NodePath, &Vec<String>) -> Result<T>,
        P: AsRef<Path>,
        C: Extend<T> + IntoIterator<Item = T> + Default,
    {
        let pattern = if self.is_root() {
            strip_root(pattern)
        } else {
            pattern.as_ref().to_path_buf()
        };
        let pattern_components: Vec<_> = parse_glob(pattern)?.collect();

        if pattern_components.is_empty() {
            return Err(Error::empty_path());
        }

        let mut results = C::default();
        let mut captured = Vec::new();

        self.visit_recursive(
            &pattern_components,
            &mut captured,
            &mut results,
            &mut callback,
        )?;

        Ok(results)
    }

    fn visit_match<F, T, C>(
        &self,
        child: NodePath,
        double: bool,
        pattern: &[WildcardComponent],
        captured: &mut Vec<String>,
        results: &mut C,
        callback: &mut F,
    ) -> Result<()>
    where
        F: FnMut(NodePath, &Vec<String>) -> Result<T>,
        C: Extend<T> + IntoIterator<Item = T> + Default,
    {
        if pattern.len() == 1 {
            let result = callback(child, captured)?;
            results.extend(std::iter::once(result)); // Add the result to the collection
        } else if child.deref().as_dir().is_ok() {
            let cd = self.fs.wd(child)?;
            if double {
                cd.visit_recursive(pattern, captured, results, callback)?;
            }
            cd.visit_recursive(&pattern[1..], captured, results, callback)?;
        }

        Ok(())
    }

    fn visit_recursive<F, T, C>(
        &self,
        pattern: &[WildcardComponent],
        captured: &mut Vec<String>,
        results: &mut C,
        callback: &mut F,
    ) -> Result<()>
    where
        F: FnMut(NodePath, &Vec<String>) -> Result<T>,
        C: Extend<T> + IntoIterator<Item = T> + Default,
    {
	if self.np.deref().as_dir().is_err() {
	    return Ok(())
	}
	    
        match &pattern[0] {
            WildcardComponent::Normal(name) => {
                // Direct match with a literal name
                if let Some(child) = self.dref.get(name) {
                    self.visit_match(child, false, pattern, captured, results, callback)?;
                }
            }
            WildcardComponent::Wildcard { .. } => {
                // Match any component that satisfies the wildcard pattern
                for child in self.read_dir()? {
                    // Check if the name matches the wildcard pattern
                    if let Some(captured_match) = pattern[0].match_component(child.basename()) {
                        captured.push(captured_match.unwrap());
                        self.visit_match(
                            child, false, pattern, captured, results, callback,
                        )?;
                        captured.pop();
                    }
                }
            }
            WildcardComponent::DoubleWildcard { .. } => {
                // Match any single component and recurse with the same pattern
                for child in self.read_dir()? {
                    captured.push(child.basename().clone());
                    self.visit_match(child, true, pattern, captured, results, callback)?;
                    captured.pop();
                }
            }
        };

        Ok(())
    }
}
