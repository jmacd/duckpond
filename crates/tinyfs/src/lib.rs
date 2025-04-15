use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
use crate::glob::WildcardComponent;
use crate::glob::GlobError;
use crate::glob::parse_glob;
        
// Constants
const ROOT_DIR: NodeID = NodeID(0);
const SYMLINK_LOOP_LIMIT: u32 = 10;

mod glob;

// Type definitions

/// Represents errors that can occur in filesystem operations
#[derive(Debug, PartialEq)]
pub enum FSError {
    NotFound(PathBuf),
    NotADirectory(PathBuf),
    NotAFile(PathBuf),
    PrefixNotSupported(PathBuf),
    RootPathFromNonRoot(PathBuf),
    ParentPathInvalid(PathBuf),
    EmptyPath,
    AlreadyExists(PathBuf),
    SymlinkLoop(PathBuf),
    GlobError(GlobError),
}

impl FSError {
    pub fn not_found<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotFound(path.as_ref().to_path_buf())
    }

    pub fn not_a_directory<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotADirectory(path.as_ref().to_path_buf())
    }

    pub fn not_a_file<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotAFile(path.as_ref().to_path_buf())
    }

    pub fn prefix_not_supported<P: AsRef<Path>>(path: P) -> Self {
        FSError::PrefixNotSupported(path.as_ref().to_path_buf())
    }

    pub fn root_path_from_non_root<P: AsRef<Path>>(path: P) -> Self {
        FSError::RootPathFromNonRoot(path.as_ref().to_path_buf())
    }

    pub fn parent_path_invalid<P: AsRef<Path>>(path: P) -> Self {
        FSError::ParentPathInvalid(path.as_ref().to_path_buf())
    }

    pub fn empty_path() -> Self {
        FSError::EmptyPath
    }

    pub fn already_exists<P: AsRef<Path>>(path: P) -> Self {
        FSError::AlreadyExists(path.as_ref().to_path_buf())
    }

    pub fn symlink_loop<P: AsRef<Path>>(path: P) -> Self {
        FSError::SymlinkLoop(path.as_ref().to_path_buf())
    }
}

impl From<GlobError> for FSError {
    fn from(ge: GlobError) -> FSError {
	FSError::GlobError(ge)
    }
}

pub type Result<T> = std::result::Result<T, FSError>;

/// Unique identifier for a node in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeID(pub usize);

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

/// Main filesystem structure that owns all nodes
pub struct FS {
    nodes: RefCell<Vec<Rc<RefCell<Node>>>>,
}

/// Context for operations within a specific directory
#[derive(Debug, PartialEq)]
pub struct WD<'a> {
    dir_id: NodeID,
    fs: &'a FS,
}

/// Result of path resolution
pub enum Handle {
    Found(String, NodeID),
    NotFound(String), // Contains the name of the missing component
}

// Implementations
impl std::fmt::Display for FSError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FSError::NotFound(path) => write!(f, "Path not found: {}", path.display()),
            FSError::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
            FSError::NotAFile(path) => write!(f, "Not a file: {}", path.display()),
            FSError::PrefixNotSupported(path) => {
                write!(f, "Path prefix not supported: {}", path.display())
            }
            FSError::RootPathFromNonRoot(path) => {
                write!(f, "Can't resolve root path: {}", path.display())
            }
            FSError::ParentPathInvalid(path) => {
                write!(f, "Parent path invalid: {}", path.display())
            }
            FSError::EmptyPath => write!(f, "Path is empty"),
            FSError::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
            FSError::SymlinkLoop(path) => write!(f, "Too many symbolic links: {}", path.display()),
            FSError::GlobError(ge) => write!(f, "Bad glob expression: {:?}", ge),
        }
    }
}

impl std::error::Error for FSError {}

impl Node {
    pub fn as_file(&self) -> Option<&File> {
        match self {
            Node::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn read_file(&self) -> Result<&[u8]> {
	match self {
	    Node::File(f) => Ok(f.content()),
	    _ => Err(FSError::not_a_file("@@@")),
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
        F: FnOnce() -> FSError,
    {
        self.as_dir().ok_or_else(or)
    }
}

impl File {
    pub fn new(content: Vec<u8>) -> Node {
        Node::File(File { content })
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
        Node::Symlink(Symlink { target })
    }

    pub fn target(&self) -> &Path {
        &self.target
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

impl std::fmt::Debug for FS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FS{{}}")
    }
}

impl<'a, 'b> PartialEq<&'b FS> for &'a FS {
    fn eq(&self, other: &&'b FS) -> bool {
        // Compare if both references point to the same FS instance
        std::ptr::eq(*self, *other)
    }
}

impl<'a> WD<'a> {
    // Helper method to get directory and validate common conditions
    fn with_directory<F, T>(&self, name: &str, f: F) -> Result<T>
    where
        F: FnOnce(&mut Directory, &FS) -> Result<T>,
    {
        self.fs
            .get_node(self.dir_id)
            .borrow_mut()
            .as_dir_mut()
            .ok_or_else(|| FSError::not_a_directory(name))
            .and_then(|dir| {
                dir.get(name).map_or(f(dir, self.fs), |_| {
                    Err(FSError::already_exists(name))
                })
            })
    }

    fn child_dir(&self, child_id: NodeID) -> Option<WD> {
	let node = self.fs.get_node(child_id);
	let child = node.borrow();
	child.as_dir().map(|_| WD{
	    dir_id: child_id,
	    fs: self.fs,
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
            Handle::Found(_, _) => Err(FSError::already_exists(path.as_ref())),
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
            Handle::Found(_, _) => Err(FSError::already_exists(path.as_ref())),
        })
    }

    /// Creates a directory at the specified path
    pub fn create_dir_path<P>(&self, path: P) -> Result<NodeID>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Handle::NotFound(name) => wd.create_dir(&name),
            Handle::Found(_, _) => Err(FSError::already_exists(path.as_ref())),
        })
    }

    /// Reads the content of a file at the specified path
    pub fn read_file_path<P>(&self, path: P) -> Result<Vec<u8>>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Handle::Found(_, node_id) => {
                let node = wd.fs.get_node(node_id);
                let node_borrow = node.borrow();
                node_borrow
                    .as_file()
                    .ok_or_else(|| FSError::not_a_file(path.as_ref()))
                    .map(|file| file.content().to_vec())
            }
            Handle::NotFound(_) => Err(FSError::not_found(path.as_ref())),
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
            Handle::Found(_, node_id) => {
                let node = wd.fs.get_node(node_id);
                if node.borrow().as_dir().is_some() {
                    Ok(node_id)
                } else {
                    Err(FSError::not_a_directory(path))
                }
            }
            Handle::NotFound(_) => Err(FSError::not_found(path)),
        })
    }

    /// Performs an operation on a path
    pub fn in_path<F, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(&WD, Handle) -> Result<T>,
        P: AsRef<Path>,
    {
        let stack = vec![(".".to_string(), self.dir_id)];
        let (node, handle) = self.resolve(&stack, path, 0)?;
        let child = self.child_dir(node).unwrap();

        op(&child, handle)
    }

    fn resolve<P>(&self, stack_in: &[(String, NodeID)], path: P, depth: u32) -> Result<(NodeID, Handle)>
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
                    return Err(FSError::prefix_not_supported(path));
                }
                Component::RootDir => {
                    if self.dir_id != ROOT_DIR {
                        return Err(FSError::root_path_from_non_root(path));
                    }
                    continue;
                }
                Component::CurDir => continue,
                Component::ParentDir => {
                    if stack.len() <= 1 {
                        return Err(FSError::parent_path_invalid(path));
                    }
                    stack.pop();
                }
                Component::Normal(name) => {
                    let (_, dirid) = stack.last().unwrap().clone();
                    let dnode = self.fs.get_node(dirid);
                    let dbor = dnode.borrow();
                    let dir = dbor.as_dir_or_else(|| FSError::not_a_directory(path))?;

                    let name = name.to_string_lossy().to_string();

                    match dir.entries.get(&name) {
                        None => {
                            // This is OK in the last position
                            if components.peek().is_some() {
                                return Err(FSError::not_found(path));
                            } else {
                                return Ok((dirid, Handle::NotFound(name)));
                            }
                        }
                        Some(cid) => {
                            let cnode = self.fs.get_node(*cid);
                            let cbor = cnode.borrow();
                            let child = cbor.deref();
                            match child {
                                Node::Symlink(symlink) => {
                                    let (newsz, relp) = normalize(symlink.target(), &stack)?;
                                    if depth >= SYMLINK_LOOP_LIMIT {
                                        return Err(FSError::symlink_loop(symlink.target()));
                                    }
                                    let (_, han) = self.resolve(&stack[0..newsz], relp, depth + 1)?;
                                    match han {
                                        Handle::Found(_, tgtid) => {
                                            stack.push((name, tgtid));
                                        }
                                        Handle::NotFound(_) => {
                                            return Err(FSError::not_found(symlink.target()));
                                        }
                                    }
                                }
                                _ => {
                                    // File or Directory.
                                    stack.push((name, *cid));
                                }
                            }
                        }
                    }
                }
            }
        }

        if stack.len() <= 1 {
            Err(FSError::empty_path())
        } else {
            let (name, found_id) = stack.pop().unwrap();
            let (_, dir_id) = stack.pop().unwrap();
            Ok((dir_id, Handle::Found(name, found_id)))
        }
    }

    /// Visits all filesystem entries matching the given wildcard pattern
    pub fn visit<F, P, T, C>(&self, pattern: P, mut callback: F) -> Result<C>
    where
    // @@@ Have a Node, need a path binding or at least basename?
    // How about to resolve symlinks at the end? Maintain the stack
    // and give an option to the caller to resolve or no?
        F: FnMut(&WD, Rc<RefCell<Node>>, &Vec<String>) -> Result<T>,
        P: AsRef<Path>,
        C: Extend<T> + IntoIterator<Item = T> + Default,
    {
	let pattern = if self.dir_id == ROOT_DIR {
	    strip_root(pattern)
	} else {
	    pattern.as_ref().to_path_buf()
	};
        let pattern_components: Vec<_> = parse_glob(pattern)?.collect();

	if pattern_components.is_empty() {
	    return Err(FSError::empty_path())
	}
        
        let mut results = C::default();
	let mut captured = Vec::new();

        self.visit_recursive(&pattern_components, &mut captured, &mut results, &mut callback)?;

        Ok(results)
    }

    fn visit_match<F, T, C>(
        &self,
	child_id: NodeID,
	double: bool,
        pattern: &[WildcardComponent],
        captured: &mut Vec<String>,
        results: &mut C,
        callback: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&WD, Rc<RefCell<Node>>, &Vec<String>) -> Result<T>,
        C: Extend<T> + IntoIterator<Item = T> + Default,
    {
        if pattern.len() == 1 {
            let result = callback(self, self.fs.get_node(child_id), captured)?;
            results.extend(std::iter::once(result)); // Add the result to the collection
        } else if let Some(child) = self.child_dir(child_id) {
            if double {
		child.visit_recursive(pattern, captured, results, callback)?;
	    }
            child.visit_recursive(&pattern[1..], captured, results, callback)?;
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
        F: FnMut(&WD, Rc<RefCell<Node>>, &Vec<String>) -> Result<T>,
        C: Extend<T> + IntoIterator<Item = T> + Default,
    {
        let node = self.fs.get_node(self.dir_id);
        let dir = node.borrow();
        let dir = dir.as_dir().ok_or_else(|| FSError::not_a_directory("."))?;

        match &pattern[0] {
            WildcardComponent::Normal(name) => {
                // Direct match with a literal name
                if let Some(child_id) = dir.entries.get(name) {
		    self.visit_match(*child_id, false, pattern, captured, results, callback)?;
                }
            },
            WildcardComponent::Wildcard { .. } => {
                // Match any component that satisfies the wildcard pattern
                for (name, &child_id) in &dir.entries {
                    // Check if the name matches the wildcard pattern
                    if let Some(captured_match) = pattern[0].match_component(name) {
                        captured.push(captured_match.unwrap());
			self.visit_match(child_id, false, pattern, captured, results, callback)?;
                        captured.pop();
                    }
                }
            },
            WildcardComponent::DoubleWildcard { .. } => {
                // Then, match any single component and recurse with the same pattern
                for (name, &child_id) in &dir.entries {
                    captured.push(name.clone());
		    self.visit_match(child_id, true, pattern, captured, results, callback)?;
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

fn normalize<P>(path: P, stack: &[(String, NodeID)]) -> Result<(usize, PathBuf)>
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
        return Err(FSError::parent_path_invalid(path));
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
    fn test_normalize() {
        // Create test NodeIDs
	let dc = "".to_string();
        let node_stack = [(dc.clone(), NodeID(1)), (dc.clone(), NodeID(2)), (dc.clone(), NodeID(3))];

        // Test 1: ../a/../b should normalize to "b" with NodeID(2)
        let (node_id, path) = normalize("../a/../b", &node_stack).unwrap();
        assert_eq!(node_id, 2);
        assert_eq!(path, PathBuf::from("b"));

        // Test 2: Multiple parent dirs
        let (node_id, path) = normalize("../../file.txt", &node_stack).unwrap();
        assert_eq!(node_id, 1);
        assert_eq!(path, PathBuf::from("file.txt"));

        // Test 3: Current dir components should be ignored
        let (node_id, path) = normalize("./a/./b", &node_stack).unwrap();
        assert_eq!(node_id, 3);
        assert_eq!(path, PathBuf::from("a/b"));

        // Test 4: Too many parent dirs should fail
        let result = normalize("../../../too-far", &node_stack);
        assert_eq!(
            result,
            Err(FSError::parent_path_invalid("../../../too-far"))
        );

        // Test 5: No parent dirs means use current node
        let (node_id, path) = normalize("just/a/path", &node_stack).unwrap();
        assert_eq!(node_id, 3);
        assert_eq!(path, PathBuf::from("just/a/path"));
    }

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
        assert_eq!(result, Err(FSError::parent_path_invalid("../c/d")));

        // Can't read an absolute path except from the root.
        let result = wd_a.read_file_path("e");
        assert_eq!(result, Err(FSError::root_path_from_non_root("/c/d")));
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
            Err(FSError::not_a_directory("/testfile"))
        );

        // Trying to open a non-existent path should fail
        assert_eq!(
            root.open_dir_path("/nonexistent"),
            Err(FSError::not_found("/nonexistent"))
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
        assert_eq!(result, Err(FSError::symlink_loop("../dir2/link2")));

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
        assert_eq!(result, Err(FSError::symlink_loop("/loop/b")));
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
        assert_eq!(result, Err(FSError::not_found("/nonexistent_target")));

        // Test with relative path to non-existent target
        fs.root().create_dir_path("/dir").unwrap();
        fs.root()
            .create_symlink_path("/dir/broken_rel", "../nonexistent_file")
            .unwrap();

        let result = fs.root().read_file_path("/dir/broken_rel");
        assert_eq!(result, Err(FSError::not_found("../nonexistent_file")));

        // Test with a chain of symlinks where the last one is broken
        fs.root().create_symlink_path("/link1", "/link2").unwrap();
        fs.root().create_symlink_path("/link2", "/nonexistent_file").unwrap();

        let result = fs.root().read_file_path("/link1");
        assert_eq!(result, Err(FSError::not_found("/nonexistent_file")));
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
        root.create_file_path("/a/b/c/file4.txt", "content4").unwrap();
        root.create_file_path("/a/d/file5.txt", "content5").unwrap();
        
        // Test case 1: Simple direct match
        let paths: Vec<_> = root.visit(
	    "/a/file1.txt", |_, node, _| Ok(node.borrow().read_file()?.to_vec()),
	).unwrap();
        assert_eq!(paths, vec![b"content1"]);

        // Test case 2: Multiple match
        let paths: Vec<_> = root.visit(
	    "/a/file*.txt", |_, node, _| Ok(node.borrow().read_file()?.to_vec()),
	).unwrap();
        assert_eq!(paths, vec![b"content1", b"content2"]);

        // Test case 3: Multiple ** match
        let paths: Vec<_> = root.visit(
	    "/**/*.txt", |_, node, _| Ok(node.borrow().read_file()?.to_vec()),
	).unwrap();
        assert_eq!(paths, vec![b"content4", b"content3", b"content5", b"content1", b"content2"]);

        // Test case 4: Single ** match
        let paths: Vec<_> = root.visit(
	    "/**/file4.txt", |_, node, _| Ok(node.borrow().read_file()?.to_vec()),
	).unwrap();
        assert_eq!(paths, vec![b"content4"]);

        // Test case 5: Single ** match
        let paths: Vec<_> = root.visit(
	    "/*/*.dat", |_, node, _| Ok(node.borrow().read_file()?.to_vec()),
	).unwrap();
        assert_eq!(paths, vec![b"data"]);
    }
}
