//! Tree formatting utilities for displaying hierarchical structures with proper box-drawing characters.
//!
//! This module provides a general-purpose tree formatter that can display any hierarchical
//! structure with proper visual connections using Unicode box-drawing characters.
//!
//! # Example
//!
//! ```
//! use tinyfs::tree_format::{TreeNode, format_tree};
//!
//! let root = TreeNode::new("root")
//!     .with_child(TreeNode::new("child1"))
//!     .with_child(TreeNode::new("child2")
//!         .with_child(TreeNode::new("grandchild")));
//!
//! let output = format_tree(&root);
//! // Produces:
//! // root
//! // ├── child1
//! // └── child2
//! //     └── grandchild
//! ```

use std::fmt;

/// A node in a tree structure that can be formatted with box-drawing characters.
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// The label for this node (can be multi-line)
    pub label: String,
    /// Child nodes
    pub children: Vec<TreeNode>,
}

impl TreeNode {
    /// Create a new tree node with the given label
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            children: Vec::new(),
        }
    }

    /// Add a child node (builder pattern)
    pub fn with_child(mut self, child: TreeNode) -> Self {
        self.children.push(child);
        self
    }

    /// Add a child node (mutable)
    pub fn add_child(&mut self, child: TreeNode) {
        self.children.push(child);
    }
}

/// Format a tree structure with proper box-drawing characters
///
/// # Arguments
///
/// * `root` - The root node of the tree
/// * `indent_size` - Number of spaces to use for indentation (default: 4)
///
/// # Returns
///
/// A formatted string with proper tree structure using box-drawing characters
pub fn format_tree(root: &TreeNode) -> String {
    format_tree_with_indent(root, 4)
}

/// Format a tree structure with a custom indentation size
pub fn format_tree_with_indent(root: &TreeNode, indent_size: usize) -> String {
    let mut output = String::new();
    output.push_str(&root.label);
    output.push('\n');

    let prefix = String::new();
    format_children(&mut output, &root.children, &prefix, indent_size);

    output
}

/// Format the children of a node with proper prefixes
fn format_children(output: &mut String, children: &[TreeNode], prefix: &str, indent_size: usize) {
    let child_count = children.len();

    for (index, child) in children.iter().enumerate() {
        let is_last = index == child_count - 1;

        // Choose the appropriate connector
        // If this child has children of its own, use a tee connector (├─┬ or └─┬)
        // Otherwise use a simple connector (├── or └──)
        let (connector, continuation_char) = if child.children.is_empty() {
            // Leaf node: simple connector
            if is_last {
                ("└──", ' ')
            } else {
                ("├──", '│')
            }
        } else {
            // Has children: use tee connector
            if is_last {
                ("└─┬", ' ')
            } else {
                ("├─┬", '│')
            }
        };

        // Format the child's label (handle multi-line labels)
        let lines: Vec<&str> = child.label.lines().collect();
        for (line_idx, line) in lines.iter().enumerate() {
            if line_idx == 0 {
                // First line gets the connector
                output.push_str(prefix);
                output.push_str(connector);
                output.push(' ');
                output.push_str(line);
                output.push('\n');
            } else {
                // Subsequent lines get aligned indentation
                output.push_str(prefix);
                output.push(continuation_char);
                output.push_str(&" ".repeat(connector.chars().count()));
                output.push_str(line);
                output.push('\n');
            }
        }

        // Recursively format children with updated prefix
        if !child.children.is_empty() {
            // The new prefix continues with the continuation character (│ or space)
            let new_prefix = format!("{}{} ", prefix, continuation_char);
            format_children(output, &child.children, &new_prefix, indent_size);
        }
    }
}

impl fmt::Display for TreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_tree(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_tree() {
        let root = TreeNode::new("root")
            .with_child(TreeNode::new("child1"))
            .with_child(TreeNode::new("child2"));

        let output = format_tree(&root);
        assert!(output.contains("root"));
        assert!(output.contains("├── child1"));
        assert!(output.contains("└── child2"));
    }

    #[test]
    fn test_nested_tree() {
        let root = TreeNode::new("root")
            .with_child(TreeNode::new("child1").with_child(TreeNode::new("grandchild1")))
            .with_child(TreeNode::new("child2"));

        let output = format_tree(&root);
        assert!(output.contains("root"));
        assert!(output.contains("├─┬ child1"));
        assert!(output.contains("│ └── grandchild1"));
        assert!(output.contains("└── child2"));
    }

    #[test]
    fn test_deep_nesting() {
        let root = TreeNode::new("root").with_child(
            TreeNode::new("a").with_child(TreeNode::new("b").with_child(TreeNode::new("c"))),
        );

        let output = format_tree(&root);
        assert!(output.contains("└─┬ a"));
        assert!(output.contains("  └─┬ b"));
        assert!(output.contains("    └── c"));
    }

    #[test]
    fn test_multiple_levels() {
        let root = TreeNode::new("root")
            .with_child(
                TreeNode::new("a")
                    .with_child(TreeNode::new("a1"))
                    .with_child(TreeNode::new("a2")),
            )
            .with_child(TreeNode::new("b").with_child(TreeNode::new("b1")));

        let output = format_tree(&root);
        println!("Output:\n{}", output);

        // Verify structure with tee connectors
        assert!(output.contains("├─┬ a"));
        assert!(output.contains("│ ├── a1"));
        assert!(output.contains("│ └── a2"));
        assert!(output.contains("└─┬ b"));
        assert!(output.contains("  └── b1"));
    }

    #[test]
    fn test_single_child() {
        let root = TreeNode::new("root").with_child(TreeNode::new("only_child"));

        let output = format_tree(&root);
        assert!(output.contains("└── only_child"));
        // Leaf node should use simple connector, not tee
        assert!(!output.contains("└─┬"));
    }
}
