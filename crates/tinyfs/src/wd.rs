use crate::dir::*;
use crate::error::*;
use crate::file::*;
use crate::fs::*;
use crate::glob::*;
use crate::node::*;
use crate::symlink::*;
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::Component;
use std::path::Path;
use std::pin::Pin;
use std::future::Future;
use std::path::PathBuf;

/// Context for operations within a specific directory
#[derive(Clone)]
pub struct WD {
    np: NodePath,
    fs: FS,
    dref: DirNode,
}

/// Result of path resolution
#[derive(Debug)]
pub enum Lookup {
    Found(NodePath),
    NotFound(PathBuf, String),
    Empty(NodePath),
}

/// Copy destination semantics
#[derive(Debug)]
pub enum CopyDestination {
    /// Target is explicitly a directory (trailing slash)
    Directory,
    /// Target is an existing directory (no trailing slash)
    ExistingDirectory,
    /// Target is an existing file (no trailing slash)
    ExistingFile,
    /// Target is a new path (no trailing slash)
    NewPath(String),
}

impl WD {
    pub(crate) async fn new(np: NodePath, fs: FS) -> Result<Self> {
        let dref = np.borrow().await.as_dir()?;
        Ok(Self { np, fs, dref })
    }

    /// Detects if a path has a trailing slash, which indicates directory intent
    fn has_trailing_slash<P: AsRef<Path>>(path: P) -> bool {
        let path_str = path.as_ref().to_string_lossy();
        path_str.ends_with('/') && path_str.len() > 1 // Don't count root "/" as trailing
    }

    /// Strips trailing slash from path for component parsing
    fn strip_trailing_slash<P: AsRef<Path>>(path: P) -> PathBuf {
        let path_str = path.as_ref().to_string_lossy();
        if Self::has_trailing_slash(&path) {
            PathBuf::from(path_str.trim_end_matches('/'))
        } else {
            path.as_ref().to_path_buf()
        }
    }

    pub async fn read_dir(&self) -> Result<DirEntryStream> {
        self.dref.read_dir().await
    }

    fn is_root(&self) -> bool {
        // Use try_lock since this is a sync method
        if let Ok(node_guard) = self.np.node.try_lock() {
            node_guard.id.is_root()
        } else {
            false // If we can't lock, assume not root
        }
    }

    pub(crate) fn node_path(&self) -> NodePath {
        self.np.clone()
    }

    // Generic node creation method for all node types
    async fn create_node<T, F>(&self, name: &str, node_creator: F) -> Result<NodePath>
    where
        F: FnOnce() -> T,
        T: Into<NodeType>,
    {
        let node = self.fs.add_node(node_creator().into()).await?;
        self.dref.insert(name.to_string(), node.clone()).await?;
        Ok(NodePath {
            node,
            path: self.dref.path().join(name),
        })
    }

    // Generic path-based node creation for all node types
    pub async fn create_node_path<P, F>(&self, path: P, node_creator: F) -> Result<NodePath>
    where
        P: AsRef<Path>,
        F: FnOnce() -> Result<NodeType>,
    {
        let path_clone = path.as_ref().to_path_buf();
        self.in_path(path.as_ref(), |wd, entry| async move {
            match entry {
                Lookup::NotFound(_, name) => {
                    let node_type = node_creator()?;
                    wd.create_node(&name, || node_type).await
                },
                Lookup::Found(_) => {
                    // Need to use async block to match the other arm
                    async { Err(Error::already_exists(&path_clone)) }.await
                },
		Lookup::Empty(_) => {
		    Err(Error::empty_path())
		}
            }
        }).await
    }

    pub async fn get_node_path<P>(&self, path: P) -> Result<NodePath>
    where
        P: AsRef<Path>,
    {
        let path_ref = path.as_ref();
        self.in_path(path_ref, |_wd, entry| async move {
            match entry {
                Lookup::NotFound(_, _) => Err(Error::not_found(path_ref)),
                Lookup::Found(np) => Ok(np),
		Lookup::Empty(np) => Ok(np),
            }
        }).await
    }

    /// Check if a path exists in the filesystem
    pub async fn exists<P>(&self, path: P) -> bool
    where
        P: AsRef<Path>,
    {
        self.get_node_path(path).await.is_ok()
    }

    /// Creates a file at the specified path
    pub async fn create_file_path<P: AsRef<Path>>(&self, path: P, content: &[u8]) -> Result<NodePath> {
        let parent_node_id = self.np.id().await.to_hex_string();
        let path_clone = path.as_ref().to_path_buf();
        
        self.in_path(path.as_ref(), |wd, entry| async move {
            match entry {
                Lookup::NotFound(_, name) => {
                    // Create the file node through the filesystem which coordinates with the backend
                    let node = wd.fs.create_file(content, Some(&parent_node_id)).await?;
                    
                    // Insert into the directory and return NodePath
                    wd.dref.insert(name.clone(), node.clone()).await?;
                    Ok(NodePath {
                        node,
                        path: wd.dref.path().join(&name),
                    })
                },
                Lookup::Found(_) => {
                    Err(Error::already_exists(&path_clone))
                },
		Lookup::Empty(_) => {
		    Err(Error::empty_path())
		}
            }
        }).await
    }

    /// Creates a symlink at the specified path
    pub async fn create_symlink_path<P: AsRef<Path>>(&self, path: P, target: P) -> Result<NodePath> {
        let target_str = target.as_ref().to_string_lossy();
        let parent_node_id = self.np.id().await.to_hex_string();
        let path_clone = path.as_ref().to_path_buf();
        
        self.in_path(path.as_ref(), |wd, entry| async move {
            match entry {
                Lookup::NotFound(_, name) => {
                    // Create the symlink node through the filesystem which coordinates with the backend
                    let node = wd.fs.create_symlink(&target_str, Some(&parent_node_id)).await?;
                    
                    // Insert into the directory and return NodePath
                    wd.dref.insert(name.clone(), node.clone()).await?;
                    Ok(NodePath {
                        node,
                        path: wd.dref.path().join(&name),
                    })
                },
                Lookup::Found(_) => {
                    Err(Error::already_exists(&path_clone))
                },
		Lookup::Empty(_) => {
		    Err(Error::empty_path())
		}
            }
        }).await
    }

    /// Creates a directory at the specified path
    pub async fn create_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let path_clone = path.as_ref().to_path_buf();
        
        let node = self.in_path(path.as_ref(), |wd, entry| async move {
            match entry {
                Lookup::NotFound(_, name) => {
                    // Create the directory node through the filesystem which coordinates with the backend
                    let node = wd.fs.create_directory().await?;
                    
                    // Insert into the directory and return NodePath
                    wd.dref.insert(name.clone(), node.clone()).await?;
                    Ok(NodePath {
                        node,
                        path: wd.dref.path().join(&name),
                    })
                },
                Lookup::Found(_) => {
                    Err(Error::already_exists(&path_clone))
                },
		Lookup::Empty(_) => {
		    Err(Error::empty_path())
		}
            }
        }).await?;
        
        self.fs.wd(&node).await
    }

    /// Reads the content of a file at the specified path
    pub async fn read_file_path<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => node.borrow().await.as_file()?.read_file().await,
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
	    Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Opens a directory at the specified path and returns a new working directory for it
    pub async fn open_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => self.fs.wd(&node).await,
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
	    Lookup::Empty(node) => Ok(self.fs.wd(&node).await?),
        }
    }

    /// Resolves a path and returns the working directory and lookup result
    pub async fn resolve_path<P: AsRef<Path>>(&self, path: P) -> Result<(WD, Lookup)> {
        let stack = vec![self.np.clone()];
        let (node, handle) = self.resolve(&stack, path.as_ref(), 0).await?;
        let wd = self.fs.wd(&node).await?;
        Ok((wd, handle))
    }

    /// Performs an operation on a path
    pub async fn in_path<F, Fut, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(WD, Lookup) -> Fut,  // Pass owned WD instead of &WD
        Fut: std::future::Future<Output = Result<T>>,
        P: AsRef<Path>,
    {
        let (wd, lookup) = self.resolve_path(path).await?;
        op(wd, lookup).await  // Pass owned wd instead of &wd
    }

    fn resolve<'a, P>(&'a self, stack_in: &'a [NodePath], path: P, depth: u32) -> Pin<Box<dyn Future<Output = Result<(NodePath, Lookup)>> + Send + 'a>>
    where
        P: AsRef<Path> + Send + 'a,
    {
        Box::pin(async move {
        let path = path.as_ref();
        let path_debug = format!("{:?}", path);
        diagnostics::log_debug!("resolve: starting with path = {path}, depth = {depth}", path: path_debug, depth: depth);
        let mut stack = stack_in.to_vec();
        let mut components = path.components().peekable();

        let components_debug = format!("{:?}", path.components().collect::<Vec<_>>());
        diagnostics::log_debug!("resolve: path components = {components}", components: components_debug);

        // Iterate through the components of the path
        for comp in &mut components {
            let comp_debug = format!("{:?}", comp);
            diagnostics::log_debug!("resolve: processing component = {comp}", comp: comp_debug);
            match comp {
                Component::Prefix(_) => {
                    diagnostics::log_debug!("resolve: Prefix component");
                    return Err(Error::prefix_not_supported(path));
                }
                Component::RootDir => {
                    diagnostics::log_debug!("resolve: RootDir component");
                    if !self.np.borrow().await.is_root() {
                        return Err(Error::root_path_from_non_root(path));
                    }
                    continue;
                }
                Component::CurDir => {
                    diagnostics::log_debug!("resolve: CurDir component");
                    continue;
                }
                Component::ParentDir => {
                    diagnostics::log_debug!("resolve: ParentDir component");
                    if stack.len() <= 1 {
                        return Err(Error::parent_path_invalid(path));
                    }
                    stack.pop();
                }
                Component::Normal(name) => {
                    let name_str = name.to_string_lossy().to_string();
                    diagnostics::log_debug!("resolve: Normal component = '{name}'", name: name_str);
                    let dnode = stack.last().unwrap().clone();
                    let ddir = dnode.borrow().await.as_dir()?;
                    let name = name.to_string_lossy().to_string();

                    let name_bound = &name;
                    diagnostics::log_debug!("resolve: Looking up name '{name}' in directory", name: name_bound);
                    match ddir.get(&name).await? {
                        None => {
                            let name_bound2 = &name;
                            diagnostics::log_debug!("resolve: Name '{name}' not found", name: name_bound2);
                            // This is OK in the last position
                            if components.peek().is_some() {
                                return Err(Error::not_found(path));
                            } else {
                                return Ok((dnode, Lookup::NotFound(path.to_path_buf(), name)));
                            }
                        }
                        Some(child) => {
                            match child.borrow().await.node_type() {
                                NodeType::Symlink(ref link) => {
                                    let (newsz, relp) =
                                        crate::path::normalize(link.readlink().await?, &stack)?;
                                    if depth >= crate::symlink::SYMLINK_LOOP_LIMIT {
                                        return Err(Error::symlink_loop(link.readlink().await?));
                                    }
                                    let (_, handle) =
                                        self.resolve(&stack[0..newsz], relp, depth + 1).await?;
                                    match handle {
                                        Lookup::Found(node) => {
                                            stack.push(node);
                                        }
                                        Lookup::NotFound(_, _) => {
                                            return Err(Error::not_found(link.readlink().await?));
                                        }
					Lookup::Empty(node) => {
                                            stack.push(node);
					},
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

        let stack_len = stack.len();
        diagnostics::log_debug!("resolve: End of component loop, stack.len() = {stack_len}", stack_len: stack_len);
        if stack.len() <= 1 {
            diagnostics::log_debug!("resolve: Returning Empty case");
            let dir = stack.pop().unwrap();
            Ok((dir.clone(), Lookup::Empty(dir)))
        } else {
            diagnostics::log_debug!("resolve: Returning Found case");
            let found = stack.pop().unwrap();
            let dir = stack.pop().unwrap();
            Ok((dir, Lookup::Found(found)))
        }
        }) // Close the Box::pin(async move { block
    }

    /// Visits all filesystem entries matching the given wildcard pattern using a visitor
    pub async fn visit_with_visitor<P, V, T>(&self, pattern: P, visitor: &mut V) -> Result<Vec<T>>
    where
        P: AsRef<Path>,
        V: Visitor<T>,
        T: Send,
    {
        let pattern = if self.is_root() {
            crate::path::strip_root(pattern)
        } else {
            pattern.as_ref().to_path_buf()
        };
        let pattern_components: Vec<_> = parse_glob(pattern)?.collect();

        if pattern_components.is_empty() {
            return Err(Error::empty_path());
        }

        let mut visited = Vec::new();
        let mut captured = Vec::new();
        let mut stack = vec![self.np.clone()];
        let mut results = Vec::new();

        self.visit_recursive_with_visitor(
            &pattern_components,
            &mut visited,
            &mut captured,
            &mut stack,
            &mut results,
            visitor,
        ).await?;

        Ok(results)
    }

    /// Returns all matching entries as a simple collection
    pub async fn collect_matches<P: AsRef<Path>>(&self, pattern: P) -> Result<Vec<(NodePath, Vec<String>)>> {
        let mut visitor = CollectingVisitor::new();
        self.visit_with_visitor(pattern, &mut visitor).await?;
        Ok(visitor.results)
    }

    fn visit_recursive_with_visitor<'a, V, T>(
        &'a self,
        pattern: &'a [WildcardComponent],
        visited: &'a mut Vec<HashSet<NodeID>>,
        captured: &'a mut Vec<String>,
        stack: &'a mut Vec<NodePath>,
        results: &'a mut Vec<T>,
        visitor: &'a mut V,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>
    where
        V: Visitor<T>,
        T: Send,
    {
        Box::pin(async move {
            if self.np.borrow().await.as_dir().is_err() {
                return Ok(());
            }
            
            // Handle empty pattern case
            if pattern.is_empty() {
                return Ok(());
            }
            
            match &pattern[0] {
                WildcardComponent::Normal(name) => {
                    // Direct match with a literal name
                    if let Some(child) = self.dref.get(name).await? {
                        self.visit_match_with_visitor(
                            child, false, pattern, visited, captured, stack, results, visitor,
                        ).await?;
                    }
                }
                WildcardComponent::Wildcard { .. } => {
                    // Match any component that satisfies the wildcard pattern
                    use futures::StreamExt;
                    let mut dir_stream = self.read_dir().await?;
                    let mut children = Vec::new();
                    while let Some(child) = dir_stream.next().await {
                        children.push(child);
                    }
                    
                    for child in children {
                        // Check if the name matches the wildcard pattern
                        if let Some(captured_match) = pattern[0].match_component(child.basename()) {
                            captured.push(captured_match.unwrap());
                            self.visit_match_with_visitor(
                                child, false, pattern, visited, captured, stack, results, visitor,
                            ).await?;
                            captured.pop();
                        }
                    }
                }
                WildcardComponent::DoubleWildcard { .. } => {
                    // For DoubleWildcard, we need to handle two cases:
                    // 1. Match zero directories (current directory) - continue with next pattern component
                    // 2. Match one or more directories - recurse into each child with the same pattern
                    
                    // Case 1: Match zero directories - try the next pattern component in current directory
                    if pattern.len() > 1 {
                        self.visit_recursive_with_visitor(&pattern[1..], visited, captured, stack, results, visitor).await?;
                    }
                    
                    // Case 2: Match one or more directories - recurse into children with same pattern
                    use futures::StreamExt;
                    let mut dir_stream = self.read_dir().await?;
                    let mut children = Vec::new();
                    while let Some(child) = dir_stream.next().await {
                        children.push(child);
                    }
                    
                    for child in children {
                        captured.push(child.basename().clone());
                        self.visit_match_with_visitor(
                            child, true, pattern, visited, captured, stack, results, visitor,
                        ).await?;
                        captured.pop();
                    }
                }
            };

            Ok(())
        })
    }

    async fn visit_match_with_visitor<V, T>(
        &self,
        child: NodePath,
        is_double: bool,
        pattern: &[WildcardComponent],
        visited: &mut Vec<HashSet<NodeID>>,
        captured: &mut Vec<String>,
        stack: &mut Vec<NodePath>,
        results: &mut Vec<T>,
        visitor: &mut V,
    ) -> Result<()>
    where
        V: Visitor<T>,
        T: Send,
    {
        // Ensure the same node is does repeat the scan at the same
        // level in the pattern.
        if visited.len() <= pattern.len() {
            visited.resize(pattern.len() + 1, HashSet::default());
        }
        let set = visited.get_mut(pattern.len()).unwrap();
        let id = child.id().await;
        if set.get(&id).is_some() {
            return Ok(());
        }
        _ = set.insert(id);

        // If we're in the last position, visit the node.
        // For DoubleWildcard, we should also continue recursing into directories.
        let is_double_wildcard = matches!(pattern[0], WildcardComponent::DoubleWildcard { .. });
        if pattern.len() == 1 {
            let result = visitor.visit(child.clone(), captured).await?;
            results.push(result);
            
            // For DoubleWildcard patterns, we need to continue recursing even at the terminal position
            if !is_double_wildcard {
                return Ok(());
            }
            // Continue to the recursion logic below for DoubleWildcard
        }

        // If the component is a symlink, resolve it.
        let mut current = child.clone();
        if let Ok(link) = child.borrow().await.as_symlink() {
            let link_target = link.readlink().await?;
            let (_, handle) = self.resolve(stack, link_target, 0).await?;
            match handle {
                Lookup::Found(np) => current = np,
                Lookup::NotFound(fp, _) => return Err(Error::not_found(fp)),
		Lookup::Empty(np) => { current = np },
            }
        }

        // If the component is a directory, recurse.
        if current.borrow().await.as_dir().is_ok() {
            // Prevent dynamic file expansion from recursing.
            self.fs.enter_node(&current).await?;
            // Ensure correct parent directory for resolve().
            stack.push(child.clone());

            // Recursive visit.
            let cd = self.fs.wd(&current).await?;
            if is_double {
                // If **, there are two recursive branches.
                cd.visit_recursive_with_visitor(pattern, visited, captured, stack, results, visitor).await?;
            }
            cd.visit_recursive_with_visitor(&pattern[1..], visited, captured, stack, results, visitor).await?;

            stack.pop();
            self.fs.exit_node(&current).await;
        }

        Ok(())
    }

    /// Resolves a path for copy destination semantics
    /// - path/ (with trailing slash) means "copy INTO this directory"  
    /// - path (without trailing slash) could be file or directory
    pub async fn resolve_copy_destination<P: AsRef<Path>>(&self, path: P) -> Result<(WD, CopyDestination)> {
        let has_trailing_slash = Self::has_trailing_slash(&path);
        
        if has_trailing_slash {
            // Trailing slash means "must be a directory, copy INTO it"
            let clean_path = Self::strip_trailing_slash(&path);
            let (wd, lookup) = self.resolve_path(clean_path).await?;
            
            match lookup {
                Lookup::Found(found_node) => {
                    // Verify it's actually a directory
                    let node_guard = found_node.node.lock().await;
                    match &node_guard.node_type {
                        NodeType::Directory(_) => {
                            drop(node_guard);
                            // Get the WD for the directory we found
                            let dest_wd = self.fs.wd(&found_node).await?;
                            Ok((dest_wd, CopyDestination::Directory))
                        }
                        _ => {
                            drop(node_guard);
                            Err(Error::not_a_directory(&path))
                        }
                    }
                }
                Lookup::NotFound(_, _) => {
                    Err(Error::not_found(&path))
                }
                Lookup::Empty(_) => {
                    // This shouldn't happen with clean path, but treat as directory
                    Ok((wd, CopyDestination::Directory))
                }
            }
        } else {
            // No trailing slash - ambiguous, check what exists
            let (wd, lookup) = self.resolve_path(&path).await?;
            
            match lookup {
                Lookup::Found(found_node) => {
                    let node_guard = found_node.node.lock().await;
                    match &node_guard.node_type {
                        NodeType::Directory(_) => {
                            drop(node_guard);
                            // Get the WD for the directory we found
                            let dest_wd = self.fs.wd(&found_node).await?;
                            Ok((dest_wd, CopyDestination::ExistingDirectory))
                        }
                        _ => {
                            drop(node_guard);
                            Ok((wd, CopyDestination::ExistingFile))
                        }
                    }
                }
                Lookup::NotFound(_, name) => {
                    // Destination doesn't exist - could create file or directory
                    Ok((wd, CopyDestination::NewPath(name)))
                }
                Lookup::Empty(_) => {
                    // Empty component - treat as directory
                    Ok((wd, CopyDestination::Directory))
                }
            }
        }
    }
}

impl std::fmt::Debug for WD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WD{{path:{:?}}}", self.dref.path())
    }
}

impl PartialEq<WD> for WD {
    fn eq(&self, other: &WD) -> bool {
        // We can't use async in PartialEq, so use try_lock for comparison
        match (self.np.node.try_lock(), other.np.node.try_lock()) {
            (Ok(self_guard), Ok(other_guard)) => self_guard.id == other_guard.id,
            _ => false, // If we can't lock both, assume they're different
        }
    }
}

/// Trait for visiting filesystem nodes during glob traversal
#[async_trait]
pub trait Visitor<T>: Send {
    /// Called for each matching node with the node path and captured wildcard groups
    async fn visit(&mut self, node: NodePath, captured: &[String]) -> Result<T>;
}

/// Simple visitor that collects all matching nodes
pub struct CollectingVisitor {
    pub results: Vec<(NodePath, Vec<String>)>,
}

impl CollectingVisitor {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

#[async_trait]
impl Visitor<()> for CollectingVisitor {
    async fn visit(&mut self, node: NodePath, captured: &[String]) -> Result<()> {
        self.results.push((node, captured.to_vec()));
        Ok(())
    }
}

// ...existing code...
