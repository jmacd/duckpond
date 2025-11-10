use crate::error::Error;
use crate::error::Result;
use crate::node::NodePath;
use std::path::{Component, Path, PathBuf};

/// Strips the root component from a path, if present
pub fn strip_root<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref()
        .components()
        .skip_while(|c| matches!(c, Component::RootDir))
        .collect()
}

/// Extracts the final component of a path as a string, if possible
pub fn basename<P: AsRef<Path>>(path: P) -> Option<String> {
    path.as_ref()
        .components()
        .next_back()
        .and_then(|c| match c {
            Component::Normal(name) => Some(name.to_string_lossy().to_string()),
            _ => None,
        })
}

/// Extracts the directory component of a path as a pathbuf, if possible
pub fn dirname<P: AsRef<Path>>(path: P) -> Option<PathBuf> {
    path.as_ref().parent().map(|x| x.to_path_buf())
}

pub fn normalize<P: AsRef<Path>>(path: P, stack: &[NodePath]) -> Result<(usize, PathBuf)> {
    let path = path.as_ref();

    // Process components to normalize the path
    let mut components = Vec::new();

    for component in path.components() {
        match component {
            Component::CurDir => {} // Skip current directory components
            Component::ParentDir => {
                // If the last component is not a parent dir, pop it and continue
                if let Some(Component::Normal(_)) = components.last() {
                    _ = components.pop();
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
    fn test_basename() {
        // Regular filename
        assert_eq!(basename("/path/to/file.txt"), Some("file.txt".to_string()));

        // Directory with trailing slash
        assert_eq!(basename("/path/to/dir/"), Some("dir".to_string()));

        // Root directory
        assert_eq!(basename("/"), None);

        // Empty path
        assert_eq!(basename(""), None);
    }
}
