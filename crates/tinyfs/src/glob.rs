// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Glob pattern matching and expansion for tinyfs and host filesystems
//!
//! This module provides glob functionality using the utilities::glob crate
//! with tinyfs-specific error conversion.

use crate::error::{Error, Result};
use std::path::Path;
pub use utilities::glob::{parse_glob, split_absolute_pattern, GlobComponentIterator, WildcardComponent};

/// Expand a glob pattern on the host filesystem, returning matching paths with their captures
///
/// This is a convenience wrapper around utilities::glob::collect_host_matches that converts
/// errors to tinyfs::Error.
pub async fn collect_host_matches<P: AsRef<Path>, B: AsRef<Path>>(
    pattern: P,
    base_dir: B,
) -> Result<Vec<(std::path::PathBuf, Vec<String>)>> {
    utilities::glob::collect_host_matches(pattern, base_dir)
        .await
        .map_err(|e| Error::Internal(format!("Glob error: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for host filesystem glob functionality - just verify the wrapper works
    #[tokio::test]
    async fn test_host_glob_wrapper() {
        // Create a temporary directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path();

        // Create test files
        std::fs::write(base_path.join("file1.txt"), b"test1").unwrap();
        std::fs::write(base_path.join("file2.txt"), b"test2").unwrap();

        // Test simple wildcard pattern through wrapper
        let results = collect_host_matches("*.txt", base_path).await.unwrap();
        assert_eq!(results.len(), 2);
    }
}
