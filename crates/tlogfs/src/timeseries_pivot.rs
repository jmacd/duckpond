//! Timeseries Pivot Factory
//!
//! Creates a series file that pivots specific columns from multiple inputs matched by a pattern.
//! Performs dynamic schema introspection on each query to handle schema changes.
//!
//! Example config:
//! ```yaml
//! factory: "timeseries-pivot"
//! config:
//!   pattern: "/combined/*"  # Captures site name
//!   columns:
//!     - "AT500_Surface.DO.mg/L"
//!     - "AT500_Bottom.DO.mg/L"
//! ```

use crate::error::TLogFSError;
use crate::factory::FactoryContext;
use crate::persistence::State;
use crate::query::QueryableFile;
use crate::register_dynamic_factory;
use crate::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{EntryType, FileHandle, NodeID, NodeMetadata, Result as TinyFSResult};
use tokio::sync::Mutex;

/// Configuration for timeseries-pivot factory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesPivotConfig {
    /// Pattern to match input files (e.g., "/combined/*")
    pub pattern: String,
    
    /// List of column names to pivot across all matched inputs
    pub columns: Vec<String>,
    
    /// Time column name (default: "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,
}

fn default_time_column() -> String {
    "timestamp".to_string()
}

/// File implementation that dynamically generates pivot SQL based on current schemas
pub struct TimeseriesPivotFile {
    config: TimeseriesPivotConfig,
    context: FactoryContext,
}

impl TimeseriesPivotFile {
    pub fn new(config: TimeseriesPivotConfig, context: FactoryContext) -> Self {
        Self { config, context }
    }

    /// Resolve pattern to matched inputs, extracting captured site names
    async fn resolve_pattern(&self) -> TinyFSResult<Vec<(String, String)>> {
        // Build TinyFS from state
        let fs = tinyfs::FS::new(self.context.state.clone()).await?;
        let tinyfs_root = fs.root().await?;

        // Use collect_matches to find matching files with captured groups
        let pattern_matches = tinyfs_root.collect_matches(&self.config.pattern).await?;

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
                path_buf.file_name()
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
    ) -> (String, HashMap<String, String>) {
        if matched_inputs.is_empty() {
            return (String::new(), HashMap::new());
        }

        let mut patterns = HashMap::new();
        
        // Register each matched input with its pattern
        for (alias, path) in matched_inputs {
            _ = patterns.insert(alias.clone(), path.clone());
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
        let mut column_selections = vec![format!("all_timestamps.{}", self.config.time_column)];
        
        for (alias, _) in matched_inputs {
            for column in &self.config.columns {
                // Output column name: SiteName.ColumnName (e.g., BDock.AT500_Surface.DO.mg/L)
                column_selections.push(format!(
                    "{}.\"{}\" AS \"{}.{}\"", 
                    alias, column, alias, column
                ));
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

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other(
            "TimeseriesPivotFile is read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl tinyfs::Metadata for TimeseriesPivotFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::FileSeriesDynamic,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl QueryableFile for TimeseriesPivotFile {
    async fn as_table_provider(
        &self,
        _node_id: NodeID,
        _part_id: NodeID,
        _state: &State,
    ) -> Result<Arc<dyn TableProvider>, TLogFSError> {
        log::info!(
            "🔍 TIMESERIES-PIVOT: Resolving pattern '{}' for {} columns",
            self.config.pattern,
            self.config.columns.len()
        );

        // Resolve pattern to get current matched inputs
        let matched_inputs = self.resolve_pattern().await
            .map_err(TLogFSError::TinyFS)?;
        
        log::info!(
            "📋 TIMESERIES-PIVOT: Pattern matched {} inputs: {:?}",
            matched_inputs.len(),
            matched_inputs.iter().map(|(a, _)| a).collect::<Vec<_>>()
        );

        if matched_inputs.is_empty() {
            return Err(TLogFSError::Internal(
                "Timeseries-pivot pattern matched no inputs".to_string()
            ));
        }

        // Generate SQL - SqlDerivedFile will handle missing columns gracefully
        let (sql, patterns) = self.generate_pivot_sql(&matched_inputs);
        
        log::info!("📝 TIMESERIES-PIVOT: Generated SQL:\n{}", sql);

        // Create SqlDerivedFile config
        let sql_config = SqlDerivedConfig {
            patterns,
            query: Some(sql),
        };

        // Use SqlDerivedSeries factory to create the file
        let sql_file = SqlDerivedFile::new(
            sql_config,
            self.context.clone(),
            SqlDerivedMode::Series,
        )?;

        // Delegate to SqlDerivedFile - it handles everything from here
        sql_file.as_table_provider(_node_id, _part_id, _state).await
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
    context: FactoryContext,
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
    
    if cfg.pattern.is_empty() {
        return Err(tinyfs::Error::Other("Pattern cannot be empty".to_string()));
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
    description: "Pivot specific columns across multiple time series inputs matched by pattern",
    file: create_timeseries_pivot_handle,
    validate: validate_timeseries_pivot_config,
    try_as_queryable: |file| {
        file.as_any()
            .downcast_ref::<TimeseriesPivotFile>()
            .map(|f| f as &dyn QueryableFile)
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a TimeseriesPivotFile for SQL generation testing
    fn create_test_pivot_file(config: TimeseriesPivotConfig) -> TimeseriesPivotFile {
        // Create a mock state for testing - we only need it for SQL generation
        use tempfile::tempdir;
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().to_str().unwrap();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut persistence = rt.block_on(async {
            crate::persistence::OpLogPersistence::open(store_path).await.unwrap()
        });
        
        let tx_guard = rt.block_on(async {
            persistence.begin_test().await.unwrap()
        });
        
        let state = tx_guard.state().unwrap();
        let context = FactoryContext::new(state.clone(), NodeID::root());
        
        TimeseriesPivotFile { config, context }
    }

    #[test]
    fn test_generate_pivot_sql_basic() {
        let config = TimeseriesPivotConfig {
            pattern: "/combined/*".to_string(),
            columns: vec!["WaterTemp".to_string(), "DO".to_string()],
            time_column: "time".to_string(),
        };

        let matched_inputs = vec![
            ("Silver".to_string(), "/combined/silver".to_string()),
            ("BDock".to_string(), "/combined/bdock".to_string()),
        ];

        let pivot_file = create_test_pivot_file(config);
        let (sql, patterns) = pivot_file.generate_pivot_sql(&matched_inputs);

        // Check patterns mapping
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns.get("Silver"), Some(&"/combined/silver".to_string()));
        assert_eq!(patterns.get("BDock"), Some(&"/combined/bdock".to_string()));

        // Check SQL structure
        assert!(sql.contains("WITH all_timestamps AS"));
        assert!(sql.contains("SELECT time FROM Silver"));
        assert!(sql.contains("SELECT time FROM BDock"));
        assert!(sql.contains("UNION"));
        assert!(sql.contains("all_timestamps.time"));
        assert!(sql.contains("Silver.\"WaterTemp\" AS \"Silver.WaterTemp\""));
        assert!(sql.contains("Silver.\"DO\" AS \"Silver.DO\""));
        assert!(sql.contains("BDock.\"WaterTemp\" AS \"BDock.WaterTemp\""));
        assert!(sql.contains("BDock.\"DO\" AS \"BDock.DO\""));
        assert!(sql.contains("LEFT JOIN Silver ON all_timestamps.time = Silver.time"));
        assert!(sql.contains("LEFT JOIN BDock ON all_timestamps.time = BDock.time"));
        assert!(sql.contains("ORDER BY all_timestamps.time"));
    }

    #[test]
    fn test_generate_pivot_sql_single_input() {
        let config = TimeseriesPivotConfig {
            pattern: "/combined/*".to_string(),
            columns: vec!["Temp".to_string()],
            time_column: "timestamp".to_string(),
        };

        let matched_inputs = vec![
            ("OnlySite".to_string(), "/data/site1".to_string()),
        ];

        let pivot_file = create_test_pivot_file(config);
        let (sql, patterns) = pivot_file.generate_pivot_sql(&matched_inputs);

        assert_eq!(patterns.len(), 1);
        assert!(sql.contains("OnlySite.\"Temp\" AS \"OnlySite.Temp\""));
        assert!(sql.contains("all_timestamps.timestamp"));
    }

    #[test]
    fn test_generate_pivot_sql_empty() {
        let config = TimeseriesPivotConfig {
            pattern: "/combined/*".to_string(),
            columns: vec!["WaterTemp".to_string()],
            time_column: "time".to_string(),
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
pattern: "/combined/*"
columns:
  - "WaterTemp"
  - "DO"
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(valid_config.as_bytes());
        assert!(result.is_ok(), "Valid config should pass: {:?}", result);

        // Empty pattern should fail
        let empty_pattern = r#"
pattern: ""
columns:
  - "WaterTemp"
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(empty_pattern.as_bytes());
        assert!(result.is_err(), "Empty pattern should fail");

        // No columns should fail
        let no_columns = r#"
pattern: "/combined/*"
columns: []
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(no_columns.as_bytes());
        assert!(result.is_err(), "Empty columns list should fail");
    }
}
