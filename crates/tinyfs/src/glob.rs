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
    pub(crate) fn match_component<S: AsRef<str>>(&self, name: S) -> Option<Option<String>> {
        let name = name.as_ref();

        match self {
            WildcardComponent::DoubleWildcard { .. } => Some(Some(name.to_string())),
            WildcardComponent::Wildcard { segments, .. } => {
                // Match name against pattern segments
                // Each segment must appear in order in the name
                
                if segments.is_empty() {
                    // Just "*" - matches everything
                    return Some(Some(name.to_string()));
                }

                let mut pos = 0;

                for (i, segment) in segments.iter().enumerate() {
                    if segment.is_empty() {
                        // Empty segment means wildcard at start/end or consecutive wildcards
                        if i == 0 {
                            // Wildcard at start - continue to next segment
                            continue;
                        } else if i == segments.len() - 1 {
                            // Wildcard at end - we're done, rest matches
                            break;
                        }
                        // Middle empty segment - consecutive wildcards, continue
                        continue;
                    }

                    // Find this literal segment in the remaining name
                    if let Some(found_at) = name[pos..].find(segment) {
                        pos += found_at + segment.len();
                    } else {
                        // Required segment not found
                        return None;
                    }
                }

                // Check if we matched the entire name for patterns ending with a literal
                if !segments.is_empty() && !segments.last().unwrap().is_empty() {
                    if pos != name.len() {
                        return None; // Didn't consume entire name
                    }
                }

                Some(Some(name.to_string()))
            }
            WildcardComponent::Normal(pattern) => {
                if name == pattern {
                    Some(None)
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
            let segments: Vec<String> = component_str
                .split('*')
                .map(|s| s.to_string())
                .collect();

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
        assert_eq!(comp.match_component("file.txt"), Some(None));
        assert_eq!(comp.match_component("other.txt"), None);
    }

    #[test]
    fn test_match_wildcard() {
        // Pattern: "file*.txt" -> ["file", ".txt"]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["file".to_string(), ".txt".to_string()],
            index: 0,
        };
        assert!(comp.match_component("file1.txt").is_some());
        assert!(comp.match_component("fileabc.txt").is_some());
        assert_eq!(comp.match_component("other.txt"), None);
    }

    #[test]
    fn test_match_multiple_wildcards() {
        // Pattern: "*VuLink*" -> ["", "VuLink", ""]
        let comp = WildcardComponent::Wildcard {
            segments: vec!["".to_string(), "VuLink".to_string(), "".to_string()],
            index: 0,
        };
        assert!(comp.match_component("HydroVu_FB-VuLink1.htm").is_some());
        assert!(comp.match_component("VuLink.txt").is_some());
        assert!(comp.match_component("prefixVuLinksuffix").is_some());
        assert_eq!(comp.match_component("NoMatch"), None);

        // Pattern: "file*.*" -> ["file", ".", ""]
        let comp2 = WildcardComponent::Wildcard {
            segments: vec!["file".to_string(), ".".to_string(), "".to_string()],
            index: 0,
        };
        assert!(comp2.match_component("file1.txt").is_some());
        assert!(comp2.match_component("filename.ext").is_some());
        assert_eq!(comp2.match_component("other.txt"), None);
    }

    #[test]
    fn test_match_double_wildcard() {
        let comp = WildcardComponent::DoubleWildcard { index: 0 };
        assert_eq!(
            comp.match_component("anything"),
            Some(Some("anything".to_string()))
        );
        assert_eq!(comp.match_component(""), Some(Some("".to_string())));
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
            assert_eq!(segments, &vec!["file".to_string(), ".".to_string(), "".to_string()]);
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
}
