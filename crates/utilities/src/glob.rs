// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Glob pattern parsing and matching utilities
//!
//! This module provides core glob pattern functionality including:
//! - Parsing glob patterns into components
//! - Matching filenames against wildcard patterns
//! - Capturing matched portions from wildcards
//!
//! This is filesystem-agnostic and can be used for both in-memory filesystems
//! (like tinyfs) and host filesystem operations.

use std::path::Path;

/// Error type for glob operations
#[derive(thiserror::Error, Debug)]
pub enum GlobError {
    #[error("Invalid path component: {0}")]
    InvalidComponent(String),
}

pub type Result<T> = std::result::Result<T, GlobError>;

/// Represents a path component that may contain wildcards
#[derive(Debug, Clone, PartialEq)]
pub enum WildcardComponent {
    /// A double wildcard ("**") that matches zero or more path segments
    DoubleWildcard {
        /// Index of this wildcard in the pattern
        index: usize,
    },
    /// A wildcard component supporting multiple wildcards
    ///
    /// Pattern is split by '*' into literal segments that must appear in order.
    /// Examples:
    /// - "*VuLink*" -> ["", "VuLink", ""]
    /// - "file*.txt" -> ["file", ".txt"]
    /// - "file*.*" -> ["file", ".", ""]
    Wildcard {
        /// Literal segments separated by wildcards
        segments: Vec<String>,
        /// Index of this wildcard in the pattern
        index: usize,
    },
    /// A normal path component with no wildcards
    Normal(String),
}

/// Iterator that yields WildcardComponents from a glob pattern
#[derive(Debug, PartialEq)]
pub struct GlobComponentIterator {
    components: Vec<WildcardComponent>,
    position: usize,
}

impl WildcardComponent {
    /// Check if this component matches the given name
    /// Returns Some(captures) if match, where captures is a Vec of all wildcard matches
    pub fn match_component<S: AsRef<str>>(&self, name: S) -> Option<Vec<String>> {
        let name = name.as_ref();

        match self {
            WildcardComponent::DoubleWildcard { .. } => Some(vec![name.to_string()]),
            WildcardComponent::Wildcard { segments, .. } => {
                // Match name against pattern segments
                // Capture ALL parts matched by wildcards

                if segments.is_empty() {
                    // Just "*" - matches everything
                    return Some(vec![name.to_string()]);
                }

                let mut pos = 0;
                let mut captures = Vec::new();

                // For each wildcard (between segments), capture what it matched

                // Handle first segment
                if !segments[0].is_empty() {
                    // Pattern starts with literal (e.g., "file*.txt")
                    if !name.starts_with(&segments[0]) {
                        return None;
                    }
                    pos = segments[0].len();
                } else {
                    // Pattern starts with * (e.g., "*.txt" or "*VuLink*")
                    // Wildcard is before first segment, capture will start from beginning
                }

                // Process remaining segments and capture wildcards between them
                for (i, segment) in segments.iter().enumerate().skip(1) {
                    let capture_start = pos;

                    if !segment.is_empty() {
                        // Find this literal in the remaining name
                        if let Some(found_at) = name[pos..].find(segment) {
                            let capture_end = pos + found_at;
                            // Capture what the wildcard matched
                            captures.push(name[capture_start..capture_end].to_string());
                            pos = capture_end + segment.len();
                        } else {
                            return None;
                        }
                    } else if i == segments.len() - 1 {
                        // Last segment is empty, means pattern ends with * (e.g., "VuLink*")
                        // Capture everything remaining
                        captures.push(name[capture_start..].to_string());
                        pos = name.len();
                    }
                    // else: empty segment in middle means consecutive wildcards - skip
                }

                // Check if we matched the entire name for patterns ending with a literal
                if let Some(last) = segments.last()
                    && !last.is_empty()
                    && pos != name.len()
                {
                    return None; // Didn't consume entire name
                }

                // For segments.len() == 1, there's one wildcard at the end
                if segments.len() == 1 {
                    captures.push(name[pos..].to_string());
                }

                Some(captures)
            }
            WildcardComponent::Normal(pattern) => {
                if name == pattern {
                    Some(vec![]) // No wildcards, no captures
                } else {
                    None
                }
            }
        }
    }
}

impl Iterator for GlobComponentIterator {
    type Item = WildcardComponent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.components.len() {
            let result = self.components[self.position].clone();
            self.position += 1;
            Some(result)
        } else {
            None
        }
    }
}

/// Parse a path into WildcardComponents
///
/// # Examples
/// ```
/// use utilities::glob::parse_glob;
/// use std::path::Path;
///
/// let glob = parse_glob("src/*.rs").unwrap();
/// let components: Vec<_> = glob.collect();
/// assert_eq!(components.len(), 2);
/// ```
pub fn parse_glob<P: AsRef<Path>>(pattern: P) -> Result<GlobComponentIterator> {
    let path = pattern.as_ref();
    let mut components = Vec::new();

    // First collect all component strings
    let component_strings: Vec<String> = path
        .components()
        .map(|c| match c {
            std::path::Component::Normal(os_str) => os_str
                .to_str()
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    GlobError::InvalidComponent(format!("{:?}", os_str))
                }),
            _ => Err(GlobError::InvalidComponent(format!("{:?}", path))),
        })
        .collect::<Result<Vec<String>>>()?;

    // Create components with appropriate wildcard indices
    let mut wildcard_index = 0;
    for component_str in component_strings.iter() {
        if component_str == "**" {
            components.push(WildcardComponent::DoubleWildcard {
                index: wildcard_index,
            });
            wildcard_index += 1;
            continue;
        }

        if component_str.contains('*') {
            // Split by '*' to get literal segments
            let segments: Vec<String> = component_str.split('*').map(|s| s.to_string()).collect();

            components.push(WildcardComponent::Wildcard {
                segments,
                index: wildcard_index,
            });
            wildcard_index += 1;
        } else {
            components.push(WildcardComponent::Normal(component_str.clone()));
        }
    }

    Ok(GlobComponentIterator {
        components,
        position: 0,
    })
}

/// Expand a glob pattern on the host filesystem, returning matching paths with their captures
///
/// This function walks the host filesystem matching the pattern and collecting wildcard
/// captures for each match.
///
/// # Arguments
/// * `pattern` - A glob pattern relative to base_dir (e.g., "src/*.rs", "**/data*.csv")
/// * `base_dir` - The base directory to start from (typically current working directory)
///
/// # Returns
/// A vector of (PathBuf, Vec<String>) tuples where:
/// - PathBuf is the full path to the matching file/directory
/// - Vec<String> contains the captures from wildcards in the pattern
///
/// # Example
/// ```no_run
/// use std::path::Path;
/// use utilities::glob::collect_host_matches;
///
/// # async fn example() -> utilities::glob::Result<()> {
/// // Find all CSV files matching pattern "data*.csv" and capture the numeric part
/// let matches = collect_host_matches("data*.csv", Path::new(".")).await?;
/// for (path, captures) in matches {
///     println!("Found: {:?}, captured: {:?}", path, captures);
///     // e.g., "Found: ./data123.csv, captured: ["123"]"
/// }
/// # Ok(())
/// # }
/// ```
pub async fn collect_host_matches<P: AsRef<Path>, B: AsRef<Path>>(
    pattern: P,
    base_dir: B,
) -> Result<Vec<(std::path::PathBuf, Vec<String>)>> {
    let components = parse_glob(pattern)?;
    let pattern_components: Vec<_> = components.collect();

    let mut results = Vec::new();
    let mut captured = Vec::new();

    // Start traversal from base directory
    let base_path = base_dir.as_ref().to_path_buf();

    visit_host_recursive(
        &base_path,
        &pattern_components,
        &mut captured,
        &mut results,
    )
    .await?;

    Ok(results)
}

/// Recursively visit the host filesystem to match glob patterns
fn visit_host_recursive<'a>(
    current_path: &'a Path,
    pattern: &'a [WildcardComponent],
    captured: &'a mut Vec<String>,
    results: &'a mut Vec<(std::path::PathBuf, Vec<String>)>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
    // Handle empty pattern case
    if pattern.is_empty() {
        return Ok(());
    }

    match &pattern[0] {
        WildcardComponent::Normal(name) => {
            // Direct match with a literal name
            let child_path = current_path.join(name);
            if child_path.exists() {
                visit_host_match(
                    &child_path,
                    false,
                    pattern,
                    captured,
                    results,
                )
                .await?;
            }
        }
        WildcardComponent::Wildcard { .. } => {
            // Read directory entries
            let entries = read_host_dir(current_path)?;

            for entry_name in entries {
                if let Some(wildcard_captures) = pattern[0].match_component(&entry_name) {
                    let child_path = current_path.join(&entry_name);
                    let captures_count = wildcard_captures.len();
                    captured.extend(wildcard_captures);

                    visit_host_match(
                        &child_path,
                        false,
                        pattern,
                        captured,
                        results,
                    )
                    .await?;

                    // Remove captures we added
                    for _ in 0..captures_count {
                        _ = captured.pop();
                    }
                }
            }
        }
        WildcardComponent::DoubleWildcard { .. } => {
            // Case 1: Match zero directories - try the next pattern component in current directory
            if pattern.len() > 1 {
                visit_host_recursive(current_path, &pattern[1..], captured, results)
                    .await?;
            } else {
                // Terminal ** - match all files recursively in current directory
                let entries = read_host_dir(current_path)?;
                for entry_name in entries {
                    let child_path = current_path.join(&entry_name);
                    if child_path.is_file() {
                        captured.push(entry_name.clone());
                        visit_host_match(
                            &child_path,
                            true,
                            pattern,
                            captured,
                            results,
                        )
                        .await?;
                        _ = captured.pop();
                    }
                }
            }

            // Case 2: Recurse into child directories
            let entries = read_host_dir(current_path)?;
            for entry_name in entries {
                let child_path = current_path.join(&entry_name);
                if child_path.is_dir() {
                    captured.push(entry_name.clone());
                    visit_host_match(
                        &child_path,
                        true,
                        pattern,
                        captured,
                        results,
                    )
                    .await?;
                    _ = captured.pop();
                }
            }
        }
    }

    Ok(())
    })
}

/// Visit a matched host path
fn visit_host_match<'a>(
    path: &'a Path,
    is_double_wildcard: bool,
    pattern: &'a [WildcardComponent],
    captured: &'a mut Vec<String>,
    results: &'a mut Vec<(std::path::PathBuf, Vec<String>)>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
    if pattern.is_empty() {
        return Ok(());
    }

    let is_dir = path.is_dir();
    let is_last_component = pattern.len() == 1;

    if is_last_component {
        // Terminal component - add to results if it matches
        if is_double_wildcard {
            // For **, we already checked in the recursive function
            results.push((path.to_path_buf(), captured.clone()));
        } else {
            // For normal wildcard or literal, add if it exists
            results.push((path.to_path_buf(), captured.clone()));
        }
    } else if is_dir {
        // Not the last component - continue recursing if it's a directory
        if is_double_wildcard {
            // For **, keep the same pattern (stay on **)
            visit_host_recursive(path, pattern, captured, results).await?;
        } else {
            // For normal components, advance to next pattern component
            visit_host_recursive(path, &pattern[1..], captured, results).await?;
        }
    }
    // If not a directory and not last component, this path is a dead end

    Ok(())
    })
}

/// Read directory entries from host filesystem
fn read_host_dir(path: &Path) -> Result<Vec<String>> {
    let mut entries = Vec::new();

    let dir_entries = std::fs::read_dir(path)
        .map_err(|e| GlobError::InvalidComponent(format!("Failed to read directory {:?}: {}", path, e)))?;

    for entry in dir_entries {
        let entry = entry
            .map_err(|e| GlobError::InvalidComponent(format!("Failed to read entry in {:?}: {}", path, e)))?;
        if let Some(name) = entry.file_name().to_str() {
            entries.push(name.to_string());
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;    use std::collections::HashSet;
    #[test]
    fn test_match_exact() {
        let comp = WildcardComponent::Normal("file.txt".to_string());
        assert_eq!(comp.match_component("file.txt"), Some(vec![]));
        assert_eq!(comp.match_component("other.txt"), None);
    }

    #[test]
    fn test_match_wildcard() {
        // Pattern: "file*.txt" -> ["file", ".txt"]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["file".to_string(), ".txt".to_string()],
            index: 0,
        };

        // Should capture what's between "file" and ".txt"
        assert_eq!(
            comp.match_component("file1.txt"),
            Some(vec!["1".to_string()])
        );
        assert_eq!(
            comp.match_component("fileabc.txt"),
            Some(vec!["abc".to_string()])
        );
        assert_eq!(comp.match_component("other.txt"), None);
    }

    #[test]
    fn test_match_multiple_wildcards() {
        // Pattern: "*VuLink*" -> ["", "VuLink", ""]
        // Should capture BOTH sides
        let comp = WildcardComponent::Wildcard {
            segments: vec!["".to_string(), "VuLink".to_string(), "".to_string()],
            index: 0,
        };

        assert_eq!(
            comp.match_component("HydroVu_FB-VuLink1.htm"),
            Some(vec!["HydroVu_FB-".to_string(), "1.htm".to_string()])
        );
        assert_eq!(
            comp.match_component("VuLink.txt"),
            Some(vec!["".to_string(), ".txt".to_string()])
        );
        assert_eq!(
            comp.match_component("prefixVuLinksuffix"),
            Some(vec!["prefix".to_string(), "suffix".to_string()])
        );
        assert_eq!(comp.match_component("NoMatch"), None);

        // Pattern: "file*.*" -> ["file", ".", ""]
        // Should capture both the name and extension
        let comp2 = WildcardComponent::Wildcard {
            segments: vec!["file".to_string(), ".".to_string(), "".to_string()],
            index: 0,
        };
        assert_eq!(
            comp2.match_component("file1.txt"),
            Some(vec!["1".to_string(), "txt".to_string()])
        );
        assert_eq!(
            comp2.match_component("filename.ext"),
            Some(vec!["name".to_string(), "ext".to_string()])
        );
        assert_eq!(comp2.match_component("other.txt"), None);
    }

    #[test]
    fn test_match_double_wildcard() {
        let comp = WildcardComponent::DoubleWildcard { index: 0 };
        assert_eq!(
            comp.match_component("anything"),
            Some(vec!["anything".to_string()])
        );
        assert_eq!(comp.match_component(""), Some(vec!["".to_string()]));
    }

    #[test]
    fn test_parse_glob_valid() {
        let glob = parse_glob("src/*.rs").unwrap();
        let components: Vec<_> = glob.collect();
        assert_eq!(components.len(), 2);

        assert!(matches!(components[0], WildcardComponent::Normal(ref s) if s == "src"));

        if let WildcardComponent::Wildcard { segments, index } = &components[1] {
            assert_eq!(segments, &vec!["".to_string(), ".rs".to_string()]);
            assert_eq!(index, &0);
        } else {
            panic!("Expected wildcard component");
        }
    }

    #[test]
    fn test_parse_double_wildcard() {
        let glob = parse_glob("src/**/lib.rs").unwrap();
        let components: Vec<_> = glob.collect();
        assert_eq!(components.len(), 3);

        assert!(matches!(components[0], WildcardComponent::Normal(ref s) if s == "src"));
        assert!(matches!(
            components[1],
            WildcardComponent::DoubleWildcard { index: 0 }
        ));
        assert!(matches!(components[2], WildcardComponent::Normal(ref s) if s == "lib.rs"));
    }

    #[test]
    fn test_parse_glob_multiple_wildcards() {
        // Multiple wildcards now supported
        let glob = parse_glob("src/file*.*").unwrap();
        let components: Vec<_> = glob.collect();
        assert_eq!(components.len(), 2);

        if let WildcardComponent::Wildcard { segments, .. } = &components[1] {
            assert_eq!(
                segments,
                &vec!["file".to_string(), ".".to_string(), "".to_string()]
            );
        } else {
            panic!("Expected wildcard component");
        }
    }

    #[test]
    fn test_parse_glob_invalid() {
        // Leading slash is invalid (not a normal component)
        assert!(matches!(
            parse_glob("/a/b-*"),
            Err(GlobError::InvalidComponent(_))
        ));
    }

    #[test]
    fn test_wildcard_capture_suffix() {
        // Pattern: "*.template" should capture only the part before .template
        // *.template -> ["", ".template"]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["".to_string(), ".template".to_string()],
            index: 0,
        };

        // Should capture "hello", not "hello.template"
        assert_eq!(
            comp.match_component("hello.template"),
            Some(vec!["hello".to_string()])
        );

        // Should capture "world", not "world.template"
        assert_eq!(
            comp.match_component("world.template"),
            Some(vec!["world".to_string()])
        );

        // Should not match files without .template suffix
        assert_eq!(comp.match_component("hello.txt"), None);
    }

    #[test]
    fn test_wildcard_capture_prefix() {
        // Pattern: "file*" should capture only the part after "file"
        // file* -> ["file", ""]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["file".to_string(), "".to_string()],
            index: 0,
        };

        // Should capture "name.txt", not "filename.txt"
        assert_eq!(
            comp.match_component("filename.txt"),
            Some(vec!["name.txt".to_string()])
        );

        // Should capture "123", not "file123"
        assert_eq!(
            comp.match_component("file123"),
            Some(vec!["123".to_string()])
        );

        // Should capture empty string for exact match
        assert_eq!(comp.match_component("file"), Some(vec!["".to_string()]));
    }

    #[test]
    fn test_wildcard_capture_middle() {
        // Pattern: "file*.txt" should capture only the middle part
        // file*.txt -> ["file", ".txt"]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["file".to_string(), ".txt".to_string()],
            index: 0,
        };

        // Should capture "name", not "filename.txt"
        assert_eq!(
            comp.match_component("filename.txt"),
            Some(vec!["name".to_string()])
        );

        // Should capture "123", not "file123.txt"
        assert_eq!(
            comp.match_component("file123.txt"),
            Some(vec!["123".to_string()])
        );

        // Should not match wrong extension
        assert_eq!(comp.match_component("filename.csv"), None);
    }

    #[test]
    fn test_wildcard_capture_just_star() {
        // Pattern: "*" should capture the entire filename
        // * -> [""]
        let comp = WildcardComponent::Wildcard {
            segments: vec![],
            index: 0,
        };

        // Should capture full filename
        assert_eq!(
            comp.match_component("anything.txt"),
            Some(vec!["anything.txt".to_string()])
        );

        assert_eq!(
            comp.match_component("hello.template"),
            Some(vec!["hello.template".to_string()])
        );
    }

    #[test]
    fn test_wildcard_capture_data_pattern() {
        // Pattern: "data*.csv" should capture the number part
        // data*.csv -> ["data", ".csv"]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["data".to_string(), ".csv".to_string()],
            index: 0,
        };

        // Should capture "1", not "data1.csv"
        assert_eq!(
            comp.match_component("data1.csv"),
            Some(vec!["1".to_string()])
        );

        // Should capture "123", not "data123.csv"
        assert_eq!(
            comp.match_component("data123.csv"),
            Some(vec!["123".to_string()])
        );

        // Should not match wrong prefix
        assert_eq!(comp.match_component("file1.csv"), None);
    }

    // Tests for host filesystem glob functionality
    #[tokio::test]
    async fn test_host_glob_basic() {
        // Create a temporary directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path();

        // Create test files
        std::fs::write(base_path.join("file1.txt"), b"test1").unwrap();
        std::fs::write(base_path.join("file2.txt"), b"test2").unwrap();
        std::fs::write(base_path.join("other.rs"), b"test3").unwrap();

        // Test simple wildcard pattern
        let results = collect_host_matches("*.txt", base_path).await.unwrap();
        assert_eq!(results.len(), 2);

        // Verify files are matched
        let mut found_names: Vec<_> = results
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        found_names.sort();
        assert_eq!(found_names, vec!["file1.txt", "file2.txt"]);
    }

    #[tokio::test]
    async fn test_host_glob_captures() {
        // Create a temporary directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path();

        // Create test files with pattern data*.csv
        std::fs::write(base_path.join("data1.csv"), b"test1").unwrap();
        std::fs::write(base_path.join("data123.csv"), b"test2").unwrap();
        std::fs::write(base_path.join("dataABC.csv"), b"test3").unwrap();

        // Test wildcard with capture
        let results = collect_host_matches("data*.csv", base_path)
            .await
            .unwrap();
        assert_eq!(results.len(), 3);

        // Verify captures
        for (path, captures) in &results {
            let filename = path.file_name().unwrap().to_str().unwrap();
            match filename {
                "data1.csv" => assert_eq!(captures, &vec!["1"]),
                "data123.csv" => assert_eq!(captures, &vec!["123"]),
                "dataABC.csv" => assert_eq!(captures, &vec!["ABC"]),
                _ => panic!("Unexpected file: {}", filename),
            }
        }
    }

    #[tokio::test]
    async fn test_host_glob_subdirectories() {
        // Create a temporary directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path();

        // Create subdirectory structure
        let subdir = base_path.join("src");
        std::fs::create_dir(&subdir).unwrap();
        std::fs::write(subdir.join("lib.rs"), b"test1").unwrap();
        std::fs::write(subdir.join("main.rs"), b"test2").unwrap();
        std::fs::write(base_path.join("README.md"), b"test3").unwrap();

        // Test pattern with directory
        let results = collect_host_matches("src/*.rs", base_path).await.unwrap();
        assert_eq!(results.len(), 2);

        // Verify files
        let mut found_names: Vec<_> = results
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        found_names.sort();
        assert_eq!(found_names, vec!["lib.rs", "main.rs"]);
    }

    #[tokio::test]
    async fn test_host_glob_double_wildcard() {
        // Create a temporary directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path();

        // Create nested directory structure
        let dir1 = base_path.join("dir1");
        let dir2 = dir1.join("dir2");
        std::fs::create_dir_all(&dir2).unwrap();

        std::fs::write(base_path.join("root.txt"), b"test1").unwrap();
        std::fs::write(dir1.join("file1.txt"), b"test2").unwrap();
        std::fs::write(dir2.join("file2.txt"), b"test3").unwrap();

        // Verify files were created
        assert!(base_path.join("root.txt").exists());
        assert!(dir1.join("file1.txt").exists());
        assert!(dir2.join("file2.txt").exists());

        // Test ** pattern to match all .txt files recursively
        let results = collect_host_matches("**/*.txt", base_path).await.unwrap();
        
        assert_eq!(results.len(), 3);

        // Verify all files are found
        let found_names: HashSet<_> = results
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert!(found_names.contains("root.txt"));
        assert!(found_names.contains("file1.txt"));
        assert!(found_names.contains("file2.txt"));
    }

    #[tokio::test]
    async fn test_host_glob_multiple_wildcards() {
        // Create a temporary directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path();

        // Create test files matching pattern file*.*
        std::fs::write(base_path.join("file1.txt"), b"test1").unwrap();
        std::fs::write(base_path.join("file2.rs"), b"test2").unwrap();
        std::fs::write(base_path.join("filename.csv"), b"test3").unwrap();
        std::fs::write(base_path.join("other.txt"), b"test4").unwrap();

        // Test pattern with multiple wildcards
        let results = collect_host_matches("file*.*", base_path).await.unwrap();
        assert_eq!(results.len(), 3);

        // Verify captures (should have 2 captures per match: name part and extension)
        for (path, captures) in &results {
            let filename = path.file_name().unwrap().to_str().unwrap();
            assert_eq!(captures.len(), 2, "Expected 2 captures for {}", filename);
            match filename {
                "file1.txt" => assert_eq!(captures, &vec!["1", "txt"]),
                "file2.rs" => assert_eq!(captures, &vec!["2", "rs"]),
                "filename.csv" => assert_eq!(captures, &vec!["name", "csv"]),
                _ => panic!("Unexpected file: {}", filename),
            }
        }
    }
}
