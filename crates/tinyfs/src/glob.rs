use std::path::{Path, PathBuf};

/// Represents a path component that may contain a wildcard
#[derive(Debug, Clone, PartialEq)]
pub struct WildcardComponent {
    /// The pattern string for this component
    pattern: String,
    /// If this component has a wildcard, its index in the path
    wildcard_index: Option<usize>,
}

impl WildcardComponent {
    /// Create a new component with its pattern and position
    pub fn new(pattern: String, wildcard_index: Option<usize>) -> Self {
        Self {
            pattern,
            wildcard_index,
        }
    }

    /// Check if this component matches the given name
    pub fn match_component<S: AsRef<str>>(&self, name: S) -> Option<Option<String>> {
        let name = name.as_ref();

        // Handle "**" wildcard - matches anything including empty
        if self.pattern == "**" {
            return Some(Some(name.to_string()));
        }

        // Handle "*" wildcard
        if let Some(wildcard_idx) = self.pattern.find('*') {
            let prefix = &self.pattern[..wildcard_idx];
            let suffix = &self.pattern[wildcard_idx + 1..];

            if name.starts_with(prefix) && name.ends_with(suffix) {
                let captured = &name[prefix.len()..name.len() - suffix.len()];
                return Some(Some(captured.to_string()));
            }

            return None;
        }

        // No wildcard, exact match required
        if name == self.pattern {
            Some(None)
        } else {
            None
        }
    }
}

/// Error types for glob pattern parsing
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobError {
    /// Component contains multiple wildcards (only one '*' is allowed)
    MultipleWildcards(String),
    /// Path component could not be converted to string
    InvalidComponent(PathBuf),
}

impl GlobError {
    /// Create a MultipleWildcards error from a string-like value
    pub fn multiple_wildcards<S: AsRef<str>>(s: S) -> Self {
        GlobError::MultipleWildcards(s.as_ref().into())
    }

    /// Create an InvalidComponent error from a path-like value
    pub fn invalid_component<P: AsRef<Path>>(p: P) -> Self {
        GlobError::InvalidComponent(p.as_ref().into())
    }
}

/// Iterator that yields WildcardComponents from a glob pattern
#[derive(Debug, PartialEq)]
pub struct GlobComponentIterator {
    components: Vec<WildcardComponent>,
    position: usize,
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
pub fn parse_glob<P: AsRef<Path>>(pattern: P) -> Result<GlobComponentIterator, GlobError> {
    let path = pattern.as_ref();
    let mut components = Vec::new();

    // First collect all component strings
    let component_strings: Vec<String> = path
        .components()
        .map(|c| match c {
            std::path::Component::Normal(os_str) => os_str
                .to_str()
                .map(|s| s.to_string())
                .ok_or_else(|| GlobError::invalid_component(os_str)),
            _ => Err(GlobError::invalid_component(path)),
        })
        .collect::<Result<Vec<String>, GlobError>>()?;

    // Create components with appropriate wildcard indices
    let mut wildcard_index = 0;
    for component_str in component_strings.iter() {
        // Validate wildcard patterns
        let idx = if component_str.contains('*') {
            let asterisk_count = component_str.chars().filter(|&c| c == '*').count();

            match asterisk_count {
                0 | 1 => (), // 0 or 1 wildcard is always valid
                2 => {
                    // Two asterisks are only valid if they're the entire component ("**")
                    if component_str != "**" {
                        return Err(GlobError::multiple_wildcards(component_str));
                    }
                }
                _ => return Err(GlobError::multiple_wildcards(component_str)),
            }
            let tmp = wildcard_index;
            wildcard_index += 1;
            Some(tmp)
        } else {
            None
        };

        components.push(WildcardComponent::new(
            component_str.clone(),
            idx,
        ));
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
        let comp = WildcardComponent::new("file.txt".to_string(), None);
        assert_eq!(comp.match_component("file.txt"), Some(None));
        assert_eq!(comp.match_component("other.txt"), None);
    }

    #[test]
    fn test_parse_glob_valid() {
        let glob = parse_glob("src/*.rs").unwrap();
        let components: Vec<_> = glob.collect();
        assert_eq!(components.len(), 2);
        assert_eq!(components[0].pattern, "src");
        assert_eq!(components[1].pattern, "*.rs");
    }

    #[test]
    fn test_parse_glob_invalid() {
        // Multiple wildcards in one component
        assert_eq!(
            parse_glob("src/file*.*"),
            Err(GlobError::multiple_wildcards("file*.*")
        ));

        assert_eq!(
            parse_glob("/a/b-*"),
            Err(GlobError::invalid_component("/a/b-*")
        ));
    }
}
