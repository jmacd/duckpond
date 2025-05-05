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
    path.as_ref().components().last().and_then(|c| {
        match c {
            Component::Normal(name) => Some(name.to_string_lossy().to_string()),
            _ => None,
        }
    })
}

/// Extracts the directory component of a path as a pathbuf, if possible
pub fn dirname<P: AsRef<Path>>(path: P) -> Option<PathBuf> {
    path.as_ref().parent().map(|x| x.to_path_buf())
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
        assert_eq!(
            basename("/path/to/file.txt"),
            Some("file.txt".to_string())
        );
        
        // Directory with trailing slash
        assert_eq!(
            basename("/path/to/dir/"),
            Some("dir".to_string())
        );
        
        // Root directory
        assert_eq!(basename("/"), None);
        
        // Empty path
        assert_eq!(basename(""), None);
    }
}
