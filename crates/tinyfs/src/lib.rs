mod dir;
mod error;
mod file;
mod glob;
mod symlink;

use crate::error::Error;
use crate::glob::parse_glob;
use crate::glob::WildcardComponent;
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

impl Deref for NodeRef {
    type Target = Rc<RefCell<Node>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

impl NodePath {
    pub fn basename(&self) -> String {
	// TODO imagine this can be more efficient by saving a ref once?
	self.path.components().last().and_then(|c| {
	    match c {
		Component::Normal(name) => Some(name.to_string_lossy().to_string()),
		_ => None,
	    }
	}).unwrap_or("".to_string())
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
    nn: NodePath,
    fs: FS,
}

/// Result of path resolution
pub enum NodeHandle {
    Found(NodePath),
    NotFound(PathBuf, String),
}

impl NodeID {
    pub fn is_root(self) -> bool {
        self == ROOT_ID
    }
}

impl<'a>  NodePathRef<'a> {
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
        let root = dir::MemoryDirectory::new();
        let node_type = NodeType::Directory(root);
        let nodes = vec![NodeRef(Rc::new(RefCell::new(Node {
            node_type,
            id: ROOT_ID,
        })))];
        FS {
            state: Rc::new(RefCell::new(State { nodes })),
        }
    }

    /// Opens a directory at the specified path
    pub fn open_dir_path<P>(&self, path: P) -> Result<WD>
    where
        P: AsRef<Path>,
    {
        self.root().open_dir_path(path)
    }

    /// Returns a working directory context for the root directory
    pub fn root(&self) -> WD {
        let root = self.state.deref().borrow().nodes.get(0).unwrap().clone();
        self.wd(NodePath {
	    node: root,
	    path: "/".into(),
	})
    }

    fn wd(&self, nn: NodePath) -> WD {
        WD {
            nn,
            fs: self.clone(),
        }
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

impl std::fmt::Debug for FS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FS{{}}")
    }
}

impl PartialEq<FS> for FS {
    fn eq(&self, other: &FS) -> bool {
        // Compare if both references point to the same FS instance
        std::ptr::eq(self, other)
    }
}

impl std::fmt::Debug for WD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WD{{path:{:?}}}", self.nn.path())
    }
}

impl PartialEq<Node> for Node {
    fn eq(&self, other: &Node) -> bool {
        self.id == other.id
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
    fn is_root(&self) -> bool {
	self.nn.deref().node.id.is_root()
    }
    
    fn dnode(&self) -> DirNode {
        self.nn.deref().as_dir().unwrap()
    }

    // Generic node creation method for all node types
    fn create_node<T, F>(&self, name: &str, node_creator: F) -> Result<NodeRef>
    where
        F: FnOnce() -> T,
        T: Into<NodeType>,
    {
        let node = self.fs.add_node(node_creator().into());
        self.dnode().insert(name.to_string(), node.clone())?;
	Ok(node)
    }

    // Generic path-based node creation for all node types
    fn create_node_path<P, T, F>(&self, path: P, node_creator: F) -> Result<NodeRef>
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

    /// Creates a new file in the current working directory
    pub fn create_file(&self, name: &str, content: &str) -> Result<NodeRef> {
        self.create_node(name, || {
            NodeType::File(file::MemoryFile::new(content.as_bytes().to_vec()))
        })
    }

    /// Creates a new symlink in the current working directory
    pub fn create_symlink(&self, name: &str, target: &Path) -> Result<NodeRef> {
        self.create_node(name, || {
            NodeType::Symlink(symlink::MemorySymlink::new(target.to_path_buf()))
        })
    }

    /// Creates a new directory in the current working directory
    pub fn create_dir(&self, name: &str) -> Result<NodeRef> {
        self.create_node(name, || NodeType::Directory(dir::MemoryDirectory::new()))
    }

    /// Creates a file at the specified path
    pub fn create_file_path<P>(&self, path: P, content: &str) -> Result<NodeRef>
    where
        P: AsRef<Path>,
    {
        self.create_node_path(path, || {
            NodeType::File(file::MemoryFile::new(content.as_bytes().to_vec()))
        })
    }

    /// Creates a symlink at the specified path
    pub fn create_symlink_path<P, T>(&self, path: P, target: T) -> Result<NodeRef>
    where
        P: AsRef<Path>,
        T: AsRef<Path>,
    {
        let target_path = target.as_ref().to_path_buf();
        self.create_node_path(path, || {
            NodeType::Symlink(symlink::MemorySymlink::new(target_path))
        })
    }

    /// Creates a directory at the specified path
    pub fn create_dir_path<P>(&self, path: P) -> Result<NodeRef>
    where
        P: AsRef<Path>,
    {
        self.create_node_path(path, || NodeType::Directory(dir::MemoryDirectory::new()))
    }

    /// Reads the content of a file at the specified path
    pub fn read_file_path<P>(&self, path: P) -> Result<Vec<u8>>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |_, entry| match entry {
            NodeHandle::Found(node) => node.deref().as_file()?.read_file(),
            NodeHandle::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
        })
    }

    /// Opens a directory at the specified path and returns a new working directory for it
    pub fn open_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        self.in_path(path, |_, entry| match entry {
            NodeHandle::Found(node) => Ok(self.fs.wd(node)),
            NodeHandle::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
        })
    }

    /// Performs an operation on a path
    pub fn in_path<F, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(&WD, NodeHandle) -> Result<T>,
        P: AsRef<Path>,
    {
        let stack = vec![self.nn.clone()];
        let (node, handle) = self.resolve(&stack, path.as_ref(), 0)?;
        let wd = self.fs.wd(node);
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
                    if !self.nn.deref().node.id.is_root() {
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
		    // @@@ this unwrap requires stack not empty, check?
                    let dnode = stack.last().unwrap().clone();
		    let ddir = dnode.deref().as_dir()?;
                    let name = name.to_string_lossy().to_string();

                    match ddir.get(&name)
                    {
                        None => {
                            // This is OK in the last position
			    if components.peek().is_none() {
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
                                    let (newsz, relp) = normalize(link.readlink()?, &stack)?;
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
        } else {
            let cd = self.fs.wd(child);
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
        match &pattern[0] {
            WildcardComponent::Normal(name) => {
                // Direct match with a literal name
                if let Some(child) = self.dnode().get(name) {
                    self.visit_match(child, false, pattern, captured, results, callback)?;
                }
            }
            WildcardComponent::Wildcard { .. } => {
                // Match any component that satisfies the wildcard pattern
                for child in self.dnode().read()? {
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
                // Then, match any single component and recurse with the same pattern
                for child in self.dnode().read()? {
                    captured.push(child.basename().clone());
                    self.visit_match(child, true, pattern, captured, results, callback)?;
                    captured.pop();
                }
            }
        };

        Ok(())
    }
}

fn strip_root<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref()
        .components()
        .skip_while(|c| matches!(c, Component::RootDir))
        .collect()
}

fn normalize<P>(path: P, stack: &[NodePath]) -> Result<(usize, PathBuf)>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();

    // Process components to normalize the path
    let mut components = Vec::new();

    for component in path.components() {
        match component {
            Component::CurDir => {} // Skip current directory components
            Component::ParentDir => {
                // If the last component is not a parent dir, pop it and continue
                if let Some(Component::Normal(_)) = components.last() {
                    components.pop();
                    continue;
                }
                // Otherwise, keep the parent dir
                components.push(component);
            }
            _ => components.push(component),
        }
    }

    // Check if the path starts with a root component
    if let Some(Component::RootDir) = components.first() {
        return Ok((1, components.into_iter().collect()));
    }

    // Count leading parent directory components
    let parent_count = components
        .iter()
        .take_while(|comp| matches!(comp, Component::ParentDir))
        .count();

    // Check if we have enough parent directories in our stack
    if stack.len() <= parent_count {
        return Err(Error::parent_path_invalid(path));
    }

    // Return the resulting stack size and path, skipping the parent directory components
    // that have already been processed
    Ok((
        stack.len() - parent_count,
        components.into_iter().skip(parent_count).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_file() {
        let fs = FS::new();

        // Create a file in the root directory
        fs.root().create_file_path("/newfile", "content").unwrap();

        let content = fs.root().read_file_path("/newfile").unwrap();

        assert_eq!(content, b"content");
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

    // #[test]
    // fn test_normalize() {
    //     let fs = FS::new();
    //     let root = fs.root();
    //     let a_node = root.create_dir_path("/a").unwrap();
    //     let b_node = root.create_dir_path("/a/b").unwrap();

    //     // Create node stack with actual NodeRefs
    //     let dc = "".to_string();
    //     let node_stack = [
    //         (dc.clone(), root.to_node_ref()),
    //         (dc.clone(), a_node),
    //         (dc.clone(), b_node),
    //     ];

    //     // Test 1: ../a/../b should normalize to "b" with the a_node as parent
    //     let (stacklen, path) = normalize("../a/../b", &node_stack).unwrap();
    //     assert_eq!(stacklen, 2);
    //     assert_eq!(path, PathBuf::from("b"));

    //     // Test 2: Multiple parent dirs
    //     let (stacklen, path) = normalize("../../file.txt", &node_stack).unwrap();
    //     assert_eq!(stacklen, 1);
    //     assert_eq!(path, PathBuf::from("file.txt"));

    //     // Test 3: Current dir components should be ignored
    //     let (stacklen, path) = normalize("./a/./b", &node_stack).unwrap();
    //     assert_eq!(stacklen, 3);
    //     assert_eq!(path, PathBuf::from("a/b"));

    //     // Test 4: Too many parent dirs should fail
    //     let result = normalize("../../../too-far", &node_stack);
    //     assert_eq!(
    //         result,
    //         Err(Error::parent_path_invalid("../../../too-far"))
    //     );

    //     // Test 5: No parent dirs means use current node
    //     let (stacklen, path) = normalize("just/a/path", &node_stack).unwrap();
    //     assert_eq!(stacklen, 3);
    //     assert_eq!(path, PathBuf::from("just/a/path"));
    // }

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
        assert_eq!(result, Err(Error::parent_path_invalid("../c/d")));

        // Can't read an absolute path except from the root.
        let result = wd_a.read_file_path("e");
        assert_eq!(result, Err(Error::root_path_from_non_root("/c/d")));
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
        assert_eq!(
            root.open_dir_path("/testfile"),
            Err(Error::not_a_directory("/testfile"))
        );

        // Trying to open a non-existent path should fail
        assert_eq!(
            root.open_dir_path("/nonexistent"),
            Err(Error::not_found("/nonexistent"))
        );
    }

    #[test]
    fn test_symlink_loop() {
        let fs = FS::new();

        // Create directories to work with
        fs.root().create_dir_path("/dir1").unwrap();
        fs.root().create_dir_path("/dir2").unwrap();

        // Create a circular symlink reference:
        // /dir1/link1 -> /dir2/link2
        // /dir2/link2 -> /dir1/link1
        fs.root()
            .create_symlink_path("/dir1/link1", "../dir2/link2")
            .unwrap();
        fs.root()
            .create_symlink_path("/dir2/link2", "../dir1/link1")
            .unwrap();

        // Attempt to access through the symlink loop
        let result = fs.root().read_file_path("/dir1/link1");

        // Verify we get a SymlinkLoop error
        assert_eq!(result, Err(Error::symlink_loop("../dir2/link2")));

        // Test a more complex loop
        fs.root().create_dir_path("/loop").unwrap();
        fs.root().create_symlink_path("/loop/a", "/loop/b").unwrap();
        fs.root().create_symlink_path("/loop/b", "/loop/c").unwrap();
        fs.root().create_symlink_path("/loop/c", "/loop/d").unwrap();
        fs.root().create_symlink_path("/loop/d", "/loop/e").unwrap();
        fs.root().create_symlink_path("/loop/e", "/loop/f").unwrap();
        fs.root().create_symlink_path("/loop/f", "/loop/g").unwrap();
        fs.root().create_symlink_path("/loop/g", "/loop/h").unwrap();
        fs.root().create_symlink_path("/loop/h", "/loop/i").unwrap();
        fs.root().create_symlink_path("/loop/i", "/loop/j").unwrap();
        fs.root().create_symlink_path("/loop/j", "/loop/a").unwrap();

        // This should exceed the SYMLINK_LOOP_LIMIT (10)
        let result = fs.root().read_file_path("/loop/a");
        assert_eq!(result, Err(Error::symlink_loop("/loop/b")));
    }

    #[test]
    fn test_symlink_to_nonexistent() {
        let fs = FS::new();

        // Create a symlink pointing to a non-existent target
        fs.root()
            .create_symlink_path("/broken_link", "/nonexistent_target")
            .unwrap();

        // Attempt to follow the symlink
        let result = fs.root().read_file_path("/broken_link");

        // Should fail with NotFound error
        assert_eq!(result, Err(Error::not_found("/nonexistent_target")));

        // Test with relative path to non-existent target
        fs.root().create_dir_path("/dir").unwrap();
        fs.root()
            .create_symlink_path("/dir/broken_rel", "../nonexistent_file")
            .unwrap();

        let result = fs.root().read_file_path("/dir/broken_rel");
        assert_eq!(result, Err(Error::not_found("../nonexistent_file")));

        // Test with a chain of symlinks where the last one is broken
        fs.root().create_symlink_path("/link1", "/link2").unwrap();
        fs.root()
            .create_symlink_path("/link2", "/nonexistent_file")
            .unwrap();

        let result = fs.root().read_file_path("/link1");
        assert_eq!(result, Err(Error::not_found("/nonexistent_file")));
    }

    #[test]
    fn test_strip_root() {
        // Test with absolute path
        let path = PathBuf::from("/a/b/c");
        let stripped = strip_root(path);
        assert_eq!(stripped, PathBuf::from("a/b/c"));

        // Test with relative path (should remain unchanged)
        let path = PathBuf::from("a/b/c");
        let stripped = strip_root(path);
        assert_eq!(stripped, PathBuf::from("a/b/c"));

        // Test with multiple root components
        let path = PathBuf::from("//a/b");
        let stripped = strip_root(path);
        assert_eq!(stripped, PathBuf::from("a/b"));

        // Test with just a root component
        let path = PathBuf::from("/");
        let stripped = strip_root(path);
        assert_eq!(stripped, PathBuf::from(""));
    }

    #[test]
    fn test_visit_glob_matching() {
        let fs = FS::new();
        let root = fs.root();

        // Create test directory structure
        root.create_dir_path("/a").unwrap();
        root.create_dir_path("/a/b").unwrap();
        root.create_dir_path("/a/b/c").unwrap();
        root.create_dir_path("/a/d").unwrap();
        root.create_file_path("/a/file1.txt", "content1").unwrap();
        root.create_file_path("/a/file2.txt", "content2").unwrap();
        root.create_file_path("/a/other.dat", "data").unwrap();
        root.create_file_path("/a/b/file3.txt", "content3").unwrap();
        root.create_file_path("/a/b/c/file4.txt", "content4")
            .unwrap();
        root.create_file_path("/a/d/file5.txt", "content5").unwrap();

        // Test case 1: Simple direct match
        let paths: Vec<_> = root
            .visit("/a/file1.txt", |node, _| {
                Ok(node.read_file()?)
            })
            .unwrap();
        assert_eq!(paths, vec![b"content1"]);

        // Test case 2: Multiple match
        let paths: Vec<_> = root
            .visit("/a/file*.txt", |node, _| {
                Ok(node.borrow().read_file()?.to_vec())
            })
            .unwrap();
        assert_eq!(paths, vec![b"content1", b"content2"]);

        // Test case 3: Multiple ** match
        let paths: Vec<_> = root
            .visit("/**/*.txt", |node, _| {
                Ok(node.borrow().read_file()?.to_vec())
            })
            .unwrap();
        assert_eq!(
            paths,
            vec![
                b"content4",
                b"content3",
                b"content5",
                b"content1",
                b"content2"
            ],
        );

        // Test case 4: Single ** match
        let paths: Vec<_> = root
            .visit("/**/file4.txt", |node, _| {
                Ok(node.borrow().read_file()?.to_vec())
            })
            .unwrap();
        assert_eq!(paths, vec![b"content4"]);

        // Test case 5: Single ** match
        let paths: Vec<_> = root
            .visit("/*/*.dat", |node, _| {
                Ok(node.borrow().read_file()?.to_vec())
            })
            .unwrap();
        assert_eq!(paths, vec![b"data"]);
    }
}
