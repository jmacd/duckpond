use std::path::Path;

use crate::error::*;

/// Represents a path component that may contain wildcards
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WildcardComponent {
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
pub(crate) struct GlobComponentIterator {
    components: Vec<WildcardComponent>,
    position: usize,
}

impl WildcardComponent {
    /// Check if this component matches the given name
    /// Returns Some(captures) if match, where captures is a Vec of all wildcard matches
    pub(crate) fn match_component<S: AsRef<str>>(&self, name: S) -> Option<Vec<String>> {
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
pub(crate) fn parse_glob<P: AsRef<Path>>(pattern: P) -> Result<GlobComponentIterator> {
    let path = pattern.as_ref();
    let mut components = Vec::new();

    // First collect all component strings
    let component_strings: Vec<String> = path
        .components()
        .map(|c| match c {
            std::path::Component::Normal(os_str) => os_str
                .to_str()
                .map(|s| s.to_string())
                .ok_or_else(|| Error::invalid_component(os_str)),
            _ => Err(Error::invalid_component(path)),
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

#[cfg(test)]
mod tests {
    use super::*;

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
            Err(Error::InvalidComponent(_))
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
}
