use crate::error::*;
use crate::node::*;
use crate::file::*;
use crate::dir::*;
use crate::symlink::*;
use crate::fs::*;
use crate::glob::*;
use std::path::Component;
use std::path::PathBuf;
use std::path::Path;

/// Context for operations within a specific directory
#[derive(Clone)]
pub struct WD {
    np: NodePath,
    fs: FS,
    dref: DirNode,
}

/// Result of path resolution
pub enum Lookup {
    Found(NodePath),
    NotFound(PathBuf, String),
}

impl WD {
    pub(crate) fn new(np: NodePath, fs: FS) -> Result<Self> {
	let dref = np.borrow().as_dir()?;
	Ok(Self {
	    np,
            fs,
            dref,
        })
    }

    pub fn read_dir(&self) -> Result<ReadDirHandle<'_>> {
	self.dref.read_dir()
    }

    fn is_root(&self) -> bool {
	self.np.node.borrow().id.is_root()
    }

    pub(crate) fn node_path(&self) -> NodePath {
	self.np.clone()
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
    pub(crate) fn create_node_path<P, T, F>(&self, path: P, node_creator: F) -> Result<NodePath>
    where
        P: AsRef<Path>,
        F: FnOnce() -> T,
        T: Into<NodeType>,
    {
        self.in_path(path.as_ref(), |wd, entry| match entry {
            Lookup::NotFound(_, name) => wd.create_node(&name, node_creator),
            Lookup::Found(_) => Err(Error::already_exists(path.as_ref())),
        })
    }

    pub fn get_node_path<P>(&self, path: P) -> Result<NodePath>
    where
        P: AsRef<Path>,
    {
        self.in_path(path.as_ref(), |_wd, entry| match entry {
            Lookup::NotFound(_, _) => Err(Error::not_found(path.as_ref())),
            Lookup::Found(np) => Ok(np),
        })
    }

    /// Creates a file at the specified path
    pub fn create_file_path<P: AsRef<Path>>(&self, path: P, content: &[u8]) -> Result<NodePath> {
        self.create_node_path(path, || {
            NodeType::File(MemoryFile::new_handle(content.to_vec()))
        })
    }

    /// Creates a symlink at the specified path
    pub fn create_symlink_path<P: AsRef<Path>>(&self, path: P, target: P) -> Result<NodePath> {
        let target_path = target.as_ref().to_path_buf();
        self.create_node_path(path, || {
            NodeType::Symlink(MemorySymlink::new_handle(target_path))
        })
    }

    /// Creates a directory at the specified path
    pub fn create_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let node = self.create_node_path(path, || {
	    NodeType::Directory(MemoryDirectory::new_handle())
	})?;
	self.fs.wd(&node)
    }

    /// Reads the content of a file at the specified path
    pub fn read_file_path<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        self.in_path(path.as_ref(), |_, entry| match entry {
            Lookup::Found(node) => node.borrow().as_file()?.read_file(),
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
        })
    }

    /// Opens a directory at the specified path and returns a new working directory for it
    pub fn open_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let path = path.as_ref();
        self.in_path(path, |_, entry| match entry {
            Lookup::Found(node) => Ok(self.fs.wd(&node)?),
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
        })
    }

    /// Performs an operation on a path
    pub fn in_path<F, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(&WD, Lookup) -> Result<T>,
        P: AsRef<Path>,
    {
        let stack = vec![self.np.clone()];
        let (node, handle) = self.resolve(&stack, path.as_ref(), 0)?;
        let wd = self.fs.wd(&node)?;
        op(&wd, handle)
    }

    fn resolve<P>(
        &self,
        stack_in: &[NodePath],
        path: P,
        depth: u32,
    ) -> Result<(NodePath, Lookup)>
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
                    if !self.np.borrow().is_root() {
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
		    let ddir = dnode.borrow().as_dir()?;
                    let name = name.to_string_lossy().to_string();

                    match ddir.get(&name)?
                    {
                        None => {
                            // This is OK in the last position
			    if components.peek().is_some() {
                                return Err(Error::not_found(path));
                            } else {
                                return Ok((
                                    dnode,
                                    Lookup::NotFound(path.to_path_buf(), name),
                                ));
                            }
                        }
                        Some(child) => {
                            match child.borrow().node_type() {
                                NodeType::Symlink(ref link) => {
                                    let (newsz, relp) = crate::path_utils::normalize(link.readlink()?, &stack)?;
                                    if depth >= crate::symlink::SYMLINK_LOOP_LIMIT {
                                        return Err(Error::symlink_loop(link.readlink()?));
                                    }
                                    let (_, handle) = self.resolve(&stack[0..newsz], relp, depth + 1)?;
				    match handle{
                                        Lookup::Found(node) => {
					    stack.push(node);
                                        }
                                        Lookup::NotFound(_, _) => {
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
            Ok((dir, Lookup::Found(found)))
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
            crate::path_utils::strip_root(pattern)
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
        } else if child.borrow().as_dir().is_ok() {

	    self.fs.enter_node(&child)?;

            let cd = self.fs.wd(&child)?;
            if double {
                cd.visit_recursive(pattern, captured, results, callback)?;
            }
            cd.visit_recursive(&pattern[1..], captured, results, callback)?;

	    self.fs.exit_node(&child);
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
	if self.np.borrow().as_dir().is_err() {
	    return Ok(())
	}
        match &pattern[0] {
            WildcardComponent::Normal(name) => {
                // Direct match with a literal name
                if let Some(child) = self.dref.get(name)? {
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

impl std::fmt::Debug for WD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WD{{path:{:?}}}", self.dref.path())
    }
}

impl PartialEq<WD> for WD {
    fn eq(&self, other: &WD) -> bool {
        self.np.borrow().id() == other.np.borrow().id()
    }
}
