//! URL-Based Pattern Matching for TinyFS Queryable Files
//!
//! This module provides a unified API for factories to match files using URL patterns
//! that encode format, decompression, and TinyFS glob patterns.
//!
//! ## URL Structure
//!
//! `scheme://[compression]/path/pattern[?options]`
//!
//! - **scheme**: Format type or builtin file type
//!   - Builtin: `series`, `table`, `data` (maps to EntryType)
//!   - Format: `csv`, `excelhtml` (requires FormatProvider)
//! - **authority** (optional): Decompression (gzip, zstd, bzip2)
//! - **path**: TinyFS glob pattern (e.g., `/data/**/*.csv`)
//! - **query** (optional): Format-specific parameters
//!
//! ## Examples
//!
//! ```ignore
//! // Builtin FileSeries (Parquet files)
//! "series:///data/sensors/*.series"
//! "series://gzip/data/sensors/*.series.gz"
//!
//! // Builtin FileTable
//! "table:///locations/*.table"
//!
//! // CSV files with format conversion
//! "csv:///exports/*.csv?delimiter=;"
//! "csv://gzip/exports/*.csv.gz"
//!
//! // HydroVu HTML exports
//! "excelhtml:///hydrovu/*.htm"
//! ```

use crate::{Error, Provider, Result, Url};
use std::path::PathBuf;
use tinyfs::{EntryType, NodePath, ProviderContext};

/// URL-based pattern matcher for queryable files
///
/// Unpacks URL to determine:
/// 1. File type (builtin EntryType or format provider)
/// 2. Decompression requirements
/// 3. TinyFS glob pattern
/// 4. Format-specific options
pub struct UrlPatternMatcher {
    provider: Provider,
}

impl UrlPatternMatcher {
    /// Create a new matcher with a Provider for format handling
    pub fn new(provider: Provider) -> Self {
        Self { provider }
    }

    /// Match files using URL pattern
    ///
    /// Returns list of NodePath instances that match the pattern.
    /// The caller can then convert these to QueryableFiles or TableProviders.
    ///
    /// # Arguments
    ///
    /// * `url_str` - URL pattern (e.g., "series:///data/*.series")
    /// * `context` - Provider context for accessing TinyFS
    ///
    /// # Returns
    ///
    /// Vector of matching NodePath instances with their metadata
    pub async fn match_pattern(
        &self,
        url_str: &str,
        context: &ProviderContext,
    ) -> Result<Vec<MatchedFile>> {
        let url = Url::parse(url_str)?;

        // Determine if this is a builtin type or requires format provider
        let scheme = url.scheme();
        
        match scheme {
            "series" | "table" | "data" | "file" => {
                // Builtin TinyFS types - use direct TinyFS matching
                // "file" scheme uses EntryType to determine actual type
                self.match_builtin_type(&url, context).await
            }
            _ => {
                // Format provider required (csv, excelhtml, etc.)
                self.match_with_format_provider(&url, context).await
            }
        }
    }

    /// Match builtin TinyFS file types (series, table, data)
    async fn match_builtin_type(
        &self,
        url: &Url,
        context: &ProviderContext,
    ) -> Result<Vec<MatchedFile>> {
        // Map scheme to EntryType
        let entry_types = match url.scheme() {
            "series" => vec![
                EntryType::FileSeriesPhysical,
                EntryType::FileSeriesDynamic,
            ],
            "table" => vec![
                EntryType::FileTablePhysical,
                EntryType::FileTableDynamic,
            ],
            "data" => vec![
                EntryType::FileDataPhysical,
                EntryType::FileDataDynamic,
            ],
            scheme => {
                return Err(Error::InvalidUrl(format!("Unknown builtin type: {}", scheme)));
            }
        };

        // Get TinyFS root and expand pattern
        let fs = context.filesystem();
        let root = fs.root().await?;
        let pattern = url.path();

        // Use TinyFS collect_matches
        let matches = root
            .collect_matches(pattern)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Pattern expansion failed: {}", e)))?;

        // Filter by entry_type and collect metadata
        let mut matched_files = Vec::new();
        for (node_path, captures) in matches {
            let file_id = node_path.id();
            let actual_entry_type = file_id.entry_type();

            // Check if this file matches any of the requested entry types
            if entry_types.contains(&actual_entry_type) {
                matched_files.push(MatchedFile {
                    node_path,
                    entry_type: actual_entry_type,
                    captures,
                    url: url.clone(),
                });
            }
        }

        if matched_files.is_empty() {
            return Err(Error::InvalidUrl(format!(
                "No files match pattern: {} with entry types: {:?}",
                pattern, entry_types
            )));
        }

        Ok(matched_files)
    }

    /// Match files that require format provider (csv, excelhtml, etc.)
    async fn match_with_format_provider(
        &self,
        url: &Url,
        context: &ProviderContext,
    ) -> Result<Vec<MatchedFile>> {
        // Get TinyFS root and expand pattern
        let fs = context.filesystem();
        let root = fs.root().await?;
        let pattern = url.path();

        // Use TinyFS collect_matches (any file type - format provider handles content)
        let matches = root
            .collect_matches(pattern)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Pattern expansion failed: {}", e)))?;

        if matches.is_empty() {
            return Err(Error::InvalidUrl(format!(
                "No files match pattern: {}",
                pattern
            )));
        }

        // For format providers, we match any file and let the provider handle the content
        // Entry type is typically DataPhysical or DataDynamic for raw data files
        let matched_files = matches
            .into_iter()
            .map(|(node_path, captures)| {
                let file_id = node_path.id();
                MatchedFile {
                    node_path,
                    entry_type: file_id.entry_type(),
                    captures,
                    url: url.clone(),
                }
            })
            .collect();

        Ok(matched_files)
    }

    /// Get the Provider for creating TableProviders from matched files
    #[must_use]
    pub fn provider(&self) -> &Provider {
        &self.provider
    }
}

/// A file matched by URL pattern with its metadata
#[derive(Clone)]
pub struct MatchedFile {
    /// TinyFS node path
    pub node_path: NodePath,
    /// File entry type (Physical/Dynamic, Series/Table/Data)
    pub entry_type: EntryType,
    /// Captured wildcard groups from pattern
    pub captures: Vec<String>,
    /// Original URL used for matching (contains format/decompression info)
    pub url: Url,
}

impl MatchedFile {
    /// Get the full file path
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.node_path.path()
    }

    /// Get the file ID
    #[must_use]
    pub fn file_id(&self) -> tinyfs::FileID {
        self.node_path.id()
    }

    /// Check if this is a builtin queryable type (series/table)
    #[must_use]
    pub fn is_builtin_queryable(&self) -> bool {
        matches!(
            self.entry_type,
            EntryType::FileSeriesPhysical
                | EntryType::FileSeriesDynamic
                | EntryType::FileTablePhysical
                | EntryType::FileTableDynamic
        )
    }

    /// Check if this requires format provider
    #[must_use]
    pub fn requires_format_provider(&self) -> bool {
        !self.is_builtin_queryable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_pattern_validation() {
        // Valid builtin patterns
        assert!(Url::parse("series:///data/*.series").is_ok());
        assert!(Url::parse("table:///locations/*.table").is_ok());
        assert!(Url::parse("series://gzip/data/*.series.gz").is_ok());

        // Valid format provider patterns
        assert!(Url::parse("csv:///exports/*.csv").is_ok());
        assert!(Url::parse("csv:///exports/*.csv?delimiter=;").is_ok());
        assert!(Url::parse("excelhtml:///hydrovu/*.htm").is_ok());
    }
}
