use std::path::Path;

use crate::error::*;

/// Represents a path component that may contain a wildcard
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WildcardComponent {
    /// A double wildcard ("**") that matches zero or more path segments
    DoubleWildcard {
        /// Index of this wildcard in the pattern
        index: usize,
    },
    /// A single wildcard component, with optional prefix and suffix
    Wildcard {
        /// Text before the wildcard (if any)
        prefix: Option<String>,
        /// Text after the wildcard (if any)
        suffix: Option<String>,
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
            WildcardComponent::Wildcard { prefix, suffix, .. } => {
                let prefix_str = prefix.as_deref().unwrap_or("");
                let suffix_str = suffix.as_deref().unwrap_or("");

                if name.starts_with(prefix_str) && name.ends_with(suffix_str) {
                    let captured = &name[prefix_str.len()..name.len() - suffix_str.len()];
                    Some(Some(captured.to_string()))
                } else {
                    None
                }
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
            let asterisk_count = component_str.chars().filter(|&c| c == '*').count();

            if asterisk_count > 1 {
                return Err(Error::multiple_wildcards(component_str));
            }

            let wildcard_idx = component_str.find('*').expect("wildcard case");
            let prefix = if wildcard_idx > 0 {
                Some(component_str[..wildcard_idx].to_string())
            } else {
                None
            };

            let suffix = if wildcard_idx < component_str.len() - 1 {
                Some(component_str[wildcard_idx + 1..].to_string())
            } else {
                None
            };

            components.push(WildcardComponent::Wildcard {
                prefix,
                suffix,
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
        let comp = WildcardComponent::Wildcard {
            prefix: Some("file".to_string()),
            suffix: Some(".txt".to_string()),
            index: 0,
        };
        assert_eq!(
            comp.match_component("file1.txt"),
            Some(Some("1".to_string()))
        );
        assert_eq!(
            comp.match_component("fileabc.txt"),
            Some(Some("abc".to_string()))
        );
        assert_eq!(comp.match_component("other.txt"), None);
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

        if let WildcardComponent::Wildcard {
            prefix,
            suffix,
            index,
        } = &components[1]
        {
            assert_eq!(prefix, &None);
            assert_eq!(suffix, &Some(".rs".to_string()));
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
    fn test_parse_glob_invalid() {
        assert!(matches!(
            parse_glob("src/file*.*"),
            Err(Error::MultipleWildcards(s)) if s == "file*.*"
        ));

        assert!(matches!(
            parse_glob("/a/b-*"),
            Err(Error::InvalidComponent(_))
        ));
    }
}
