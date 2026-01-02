// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Timeseries Pivot Factory
//!
//! Creates a series file that pivots specific columns from multiple inputs matched by a pattern.
//! Performs dynamic schema introspection on each query to handle schema changes.
//!
//! Example config:
//! ```yaml
//! factory: "timeseries-pivot"
//! config:
//!   pattern: "series:///combined/*"  # Captures site name
//!   columns:
//!     - "AT500_Surface.DO.mg/L"
//!     - "AT500_Bottom.DO.mg/L"
//! ```

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use crate::register_dynamic_factory;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{EntryType, FileHandle, FileID, NodeMetadata, Result as TinyFSResult};
use tokio::sync::Mutex;

/// Configuration for timeseries-pivot factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeseriesPivotConfig {
    /// Pattern to match input files (e.g., "series:///combined/*")
    pub pattern: crate::Url,

    /// List of column names to pivot across all matched inputs
    pub columns: Vec<String>,

    /// Time column name (default: "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,

    /// Optional list of table transform factory paths to apply to input TableProvider.
    /// Transforms are applied in order before SQL execution.
    /// Each transform is a path like "/etc/hydro_rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,
}

fn default_time_column() -> String {
    "timestamp".to_string()
}

/// File implementation that dynamically generates pivot SQL based on current schemas
pub struct TimeseriesPivotFile {
    config: TimeseriesPivotConfig,
    context: crate::FactoryContext,
}

impl TimeseriesPivotFile {
    #[must_use]
    pub fn new(config: TimeseriesPivotConfig, context: crate::FactoryContext) -> Self {
        Self { config, context }
    }

    /// Resolve pattern to matched inputs, extracting captured site names
    async fn resolve_pattern(&self) -> TinyFSResult<Vec<(String, String)>> {
        // Get filesystem from ProviderContext
        let fs = self.context.context.filesystem();
        let tinyfs_root = fs.root().await?;

        // Extract filesystem path from URL for pattern matching
        let pattern_path = self.config.pattern.path();

        // Use collect_matches to find matching files with captured groups
        let pattern_matches = tinyfs_root.collect_matches(pattern_path).await?;

        if pattern_matches.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "No files matched pattern: {}",
                self.config.pattern
            )));
        }

        // Extract site names from captured groups (first wildcard capture)
        let mut matches = Vec::new();
        for (node_path, captured_groups) in pattern_matches {
            // Get path string
            let path_buf = node_path.path();
            let path_str = path_buf.to_string_lossy().to_string();

            let alias = if !captured_groups.is_empty() {
                // Use first captured group as site name (index 0)
                captured_groups[0].clone()
            } else {
                // If no capture, use the last path component
                path_buf
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string()
            };
            matches.push((alias, path_str));
        }

        Ok(matches)
    }

    /// Generate SQL for pivoting columns across matched inputs.
    /// Each input's columns are selected with site prefix (e.g., Silver_WaterTemp).
    /// Missing columns will be NULL - no need for schema introspection.
    fn generate_pivot_sql(
        &self,
        matched_inputs: &[(String, String)], // (site_alias, pattern_path)
    ) -> (String, HashMap<String, crate::Url>) {
        if matched_inputs.is_empty() {
            return (String::new(), HashMap::new());
        }

        let mut patterns = HashMap::new();

        // Register each matched input with its pattern
        for (alias, path) in matched_inputs {
            // Convert filesystem path to series:// URL
            let url_str = if path.starts_with('/') {
                format!("series://{}", path)
            } else {
                format!("series:///{}", path)
            };
            // Parse path as URL - if it fails, skip this entry
            if let Ok(url) = crate::Url::parse(&url_str) {
                _ = patterns.insert(alias.clone(), url);
            }
        }

        // Build UNION of all timestamps from all inputs
        let timestamp_unions: Vec<String> = matched_inputs
            .iter()
            .map(|(alias, _)| format!("SELECT {} FROM {}", self.config.time_column, alias))
            .collect();

        let timestamps_cte = format!(
            "WITH all_timestamps AS (\n  {}\n)",
            timestamp_unions.join("\n  UNION\n  ")
        );

        // Build column selections: for each column, select from each site
        // Note: scope_prefix wrapper prepends the ORIGINAL alias (not sql_derived_xxx) to column names
        // So after sql_derived replaces "Silver" -> "sql_derived_silver_xxx" in table references,
        // the column names still use the original alias: sql_derived_silver_xxx."Silver.Column"
        let mut column_selections = vec![format!("all_timestamps.{}", self.config.time_column)];

        for (alias, _) in matched_inputs {
            for column in &self.config.columns {
                // Reference format: table."Scope.Column"
                // e.g., Silver."Silver.AT500_Surface.DO.mg/L"
                // After sql_derived replacement: sql_derived_silver_xxx."Silver.AT500_Surface.DO.mg/L"
                column_selections.push(format!(r#"{}."{}.{}""#, alias, alias, column));
            }
        }

        // Build LEFT JOINs for each site
        let join_clauses: Vec<String> = matched_inputs
            .iter()
            .map(|(alias, _)| {
                format!(
                    "LEFT JOIN {} ON all_timestamps.{} = {}.{}",
                    alias, self.config.time_column, alias, self.config.time_column
                )
            })
            .collect();

        // Assemble final query
        let sql = format!(
            "{}\nSELECT\n  {}\nFROM all_timestamps\n{}\nORDER BY all_timestamps.{}",
            timestamps_cte,
            column_selections.join(",\n  "),
            join_clauses.join("\n"),
            self.config.time_column
        );

        (sql, patterns)
    }

    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl tinyfs::File for TimeseriesPivotFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        Err(tinyfs::Error::Other(
            "TimeseriesPivotFile does not support direct reading".to_string(),
        ))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "TimeseriesPivotFile is read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl tinyfs::Metadata for TimeseriesPivotFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            entry_type: EntryType::FileSeriesDynamic,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl tinyfs::QueryableFile for TimeseriesPivotFile {
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        log::debug!(
            "🔍 TIMESERIES-PIVOT: Resolving pattern '{}' for {} columns",
            self.config.pattern,
            self.config.columns.len()
        );

        // Resolve pattern to get current matched inputs
        let matched_inputs = self.resolve_pattern().await?;

        log::debug!(
            "📋 TIMESERIES-PIVOT: Pattern matched {} inputs: {:?}",
            matched_inputs.len(),
            matched_inputs.iter().map(|(a, _)| a).collect::<Vec<_>>()
        );

        if matched_inputs.is_empty() {
            return Err(tinyfs::Error::Other(
                "Timeseries-pivot pattern matched no inputs".to_string(),
            ));
        }

        // Generate SQL - SqlDerivedFile will handle missing columns gracefully
        let (sql, patterns) = self.generate_pivot_sql(&matched_inputs);

        log::debug!("📝 TIMESERIES-PIVOT: Generated SQL:\n{}", sql);

        // Build scope_prefixes map for each table
        let mut scope_prefixes = HashMap::new();
        for (alias, _) in &matched_inputs {
            _ = scope_prefixes.insert(
                alias.clone(),
                (alias.clone(), self.config.time_column.clone()),
            );
        }

        // Build list of expected columns for null padding
        // These are the raw column names that should exist in each source table
        let mut expected_columns = HashMap::new();
        for column in &self.config.columns {
            _ = expected_columns.insert(column.clone(), arrow::datatypes::DataType::Float64);
        }

        // Create SqlDerivedFile config with scope prefixes and null_padding wrapper
        let mut sql_config = SqlDerivedConfig::new_scoped(patterns, Some(sql), scope_prefixes)
            .with_provider_wrapper(move |provider| {
                crate::transform::null_padding::null_padding_table(
                    provider,
                    expected_columns.clone(),
                )
                .map_err(crate::Error::from)
            });

        // Add transforms if configured
        sql_config.transforms = self.config.transforms.clone();

        // Use SqlDerivedSeries factory to create the file
        let sql_file =
            SqlDerivedFile::new(sql_config, self.context.clone(), SqlDerivedMode::Series)?;

        // Delegate to SqlDerivedFile - it handles everything from here
        sql_file.as_table_provider(id, context).await
    }
}

impl std::fmt::Debug for TimeseriesPivotFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeseriesPivotFile")
            .field("pattern", &self.config.pattern)
            .field("columns", &self.config.columns)
            .finish()
    }
}

// Factory registration

fn create_timeseries_pivot_handle(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: TimeseriesPivotConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid timeseries-pivot config: {}", e)))?;

    let pivot_file = TimeseriesPivotFile::new(cfg, context);
    Ok(pivot_file.create_handle())
}

fn validate_timeseries_pivot_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;

    let cfg: TimeseriesPivotConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Validate scheme is recognized (no fallback for unknown schemes)
    let scheme = cfg.pattern.scheme();
    const KNOWN_SCHEMES: &[&str] = &[
        "file",
        "series",
        "table",
        "data",
        "csv",
        "excelhtml",
        "oteljson",
    ];
    if !KNOWN_SCHEMES.contains(&scheme) {
        return Err(tinyfs::Error::Other(format!(
            "Unknown URL scheme '{}' in pattern '{}'. Known schemes: {}",
            scheme,
            cfg.pattern,
            KNOWN_SCHEMES.join(", ")
        )));
    }

    if cfg.columns.is_empty() {
        return Err(tinyfs::Error::Other(
            "At least one column must be specified".to_string(),
        ));
    }

    serde_json::to_value(&cfg)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

register_dynamic_factory!(
    name: "timeseries-pivot",
    description: "Pivot a timeseries by value column",
    file: create_timeseries_pivot_handle,
    validate: validate_timeseries_pivot_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tinyfs::{FileID, MemoryPersistence, ProviderContext};

    /// Helper to create crate::FactoryContext from ProviderContext for tests
    fn test_context(context: &ProviderContext, file_id: FileID) -> crate::FactoryContext {
        crate::FactoryContext {
            context: context.clone(),
            file_id,
            pond_metadata: None,
            txn_seq: 0,
        }
    }

    /// Helper to create test environment with MemoryPersistence
    fn create_test_environment() -> ProviderContext {
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
            .expect("Failed to register TinyFS object store");
        ProviderContext::new(session, HashMap::new(), Arc::new(persistence))
    }

    // Helper to create a TimeseriesPivotFile for SQL generation testing
    fn create_test_pivot_file(config: TimeseriesPivotConfig) -> TimeseriesPivotFile {
        // Create a mock context for testing - we only need it for SQL generation
        let provider_context = create_test_environment();
        let context = test_context(&provider_context, FileID::root());

        TimeseriesPivotFile { config, context }
    }

    #[test]
    fn test_generate_pivot_sql_basic() {
        let config = TimeseriesPivotConfig {
            pattern: crate::Url::parse("series:///combined/*").unwrap(),
            columns: vec!["WaterTemp".to_string(), "DO".to_string()],
            time_column: "time".to_string(),
            transforms: None,
        };

        let matched_inputs = vec![
            ("Silver".to_string(), "/combined/silver".to_string()),
            ("BDock".to_string(), "/combined/bdock".to_string()),
        ];

        let pivot_file = create_test_pivot_file(config);
        let (sql, patterns) = pivot_file.generate_pivot_sql(&matched_inputs);

        // Check patterns mapping
        assert_eq!(patterns.len(), 2);
        assert_eq!(
            patterns.get("Silver"),
            Some(&crate::Url::parse("series:///combined/silver").unwrap())
        );
        assert_eq!(
            patterns.get("BDock"),
            Some(&crate::Url::parse("series:///combined/bdock").unwrap())
        );

        // Check SQL structure - column names have scope prefix from ScopePrefixTableProvider
        assert!(sql.contains("WITH all_timestamps AS"));
        assert!(sql.contains("SELECT time FROM Silver"));
        assert!(sql.contains("SELECT time FROM BDock"));
        assert!(sql.contains("UNION"));
        assert!(sql.contains("all_timestamps.time"));
        // After scope_prefix wrapper, columns are: Silver."Silver.WaterTemp", Silver."Silver.DO", etc.
        assert!(sql.contains("Silver.\"Silver.WaterTemp\""));
        assert!(sql.contains("Silver.\"Silver.DO\""));
        assert!(sql.contains("BDock.\"BDock.WaterTemp\""));
        assert!(sql.contains("BDock.\"BDock.DO\""));
        assert!(sql.contains("LEFT JOIN Silver ON all_timestamps.time = Silver.time"));
        assert!(sql.contains("LEFT JOIN BDock ON all_timestamps.time = BDock.time"));
        assert!(sql.contains("ORDER BY all_timestamps.time"));
    }

    #[test]
    fn test_generate_pivot_sql_single_input() {
        let config = TimeseriesPivotConfig {
            pattern: crate::Url::parse("series:///combined/*").unwrap(),
            columns: vec!["Temp".to_string()],
            time_column: "timestamp".to_string(),
            transforms: None,
        };

        let matched_inputs = vec![("OnlySite".to_string(), "/data/site1".to_string())];

        let pivot_file = create_test_pivot_file(config);
        let (sql, patterns) = pivot_file.generate_pivot_sql(&matched_inputs);

        assert_eq!(patterns.len(), 1);
        // After scope_prefix wrapper, column is: OnlySite."OnlySite.Temp"
        assert!(sql.contains("OnlySite.\"OnlySite.Temp\""));
        assert!(sql.contains("all_timestamps.timestamp"));
    }

    #[test]
    fn test_generate_pivot_sql_empty() {
        let config = TimeseriesPivotConfig {
            pattern: crate::Url::parse("series:///combined/*").unwrap(),
            columns: vec!["WaterTemp".to_string()],
            time_column: "time".to_string(),
            transforms: None,
        };

        let matched_inputs = vec![];

        let pivot_file = create_test_pivot_file(config);
        let (sql, patterns) = pivot_file.generate_pivot_sql(&matched_inputs);

        assert_eq!(patterns.len(), 0);
        assert!(sql.is_empty());
    }

    #[test]
    fn test_config_validation() {
        // Valid config - just the config section as JSON/YAML
        let valid_config = r#"
pattern: "series:///combined/*"
columns:
  - "WaterTemp"
  - "DO"
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(valid_config.as_bytes());
        assert!(result.is_ok(), "Valid config should pass: {:?}", result);

        // Invalid URL pattern should fail
        let invalid_pattern = r#"
pattern: "invalid-scheme://bad/url"
columns:
  - "WaterTemp"
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(invalid_pattern.as_bytes());
        assert!(result.is_err(), "Invalid URL pattern should fail");

        // No columns should fail
        let no_columns = r#"
pattern: "series:///combined/*"
columns: []
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(no_columns.as_bytes());
        assert!(result.is_err(), "Empty columns list should fail");
    }
}
