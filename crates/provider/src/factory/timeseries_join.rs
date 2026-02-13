// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Timeseries Join Factory for TLogFS
//!
//! This factory simplifies the common pattern of joining multiple time series sources
//! by timestamp, automatically generating the COALESCE + FULL OUTER JOIN + EXCLUDE SQL.

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use crate::register_dynamic_factory;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::catalog::TableProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{EntryType, FileHandle, NodeMetadata, Result as TinyFSResult};

/// Time range bounds for filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    /// Optional start time (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub begin: Option<String>,

    /// Optional end time (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
}

/// Input source with URL pattern and optional time range
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeseriesInput {
    /// URL pattern for matching time series files
    ///
    /// Supported URL schemes:
    /// - `series:///pattern` - Builtin TinyFS FileSeries (Parquet)
    /// - `series+gzip:///pattern` - Compressed FileSeries
    /// - `csv:///pattern` - CSV files (requires format conversion)
    /// - `csv+gzip:///pattern?delimiter=;` - CSV with decompression and options
    /// - `excelhtml:///pattern` - HydroVu HTML exports
    ///
    /// Example: `series:///data/sensors/*.series`
    pub pattern: crate::Url,

    /// Optional time range filter for this specific input
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<TimeRange>,

    /// Optional scope prefix to add to all column names (e.g.,
    /// "temperature" -> "BDock.temperature")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,

    /// Optional list of table transform factory paths to apply to this input's TableProvider.
    /// Transforms are applied in order before scope prefixes and SQL execution.
    /// Each transform is a path like "/etc/hydro_rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,
}

/// Configuration for the timeseries-join factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeseriesJoinConfig {
    /// Name of the timestamp column (defaults to "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,

    /// List of input sources with patterns and optional time ranges
    pub inputs: Vec<TimeseriesInput>,
}

fn default_time_column() -> String {
    "timestamp".to_string()
}

impl TimeseriesJoinConfig {
    /// Validate that all input patterns are valid URLs with appropriate schemes
    pub fn validate(&self) -> TinyFSResult<()> {
        if self.inputs.is_empty() {
            return Err(tinyfs::Error::Other(
                "At least one input must be specified".to_string(),
            ));
        }

        if self.inputs.len() == 1 {
            return Err(tinyfs::Error::Other(
                "Timeseries join requires at least 2 inputs. Use sql-derived-series for single sources.".to_string(),
            ));
        }

        // Validate each input pattern is a valid URL
        for (i, input) in self.inputs.iter().enumerate() {
            // URL already validated during deserialization
            let scheme = input.pattern.scheme();
            match scheme {
                "series" | "csv" | "excelhtml" | "file" => {
                    // Valid timeseries sources (file uses EntryType to determine type)
                }
                _ => {
                    return Err(tinyfs::Error::Other(format!(
                        "Input {} uses unsupported scheme '{}' for timeseries data. Supported: series, csv, excelhtml, file",
                        i, scheme
                    )));
                }
            }

            // Validate time range timestamps if present
            if let Some(range) = &input.range {
                if let Some(begin) = &range.begin {
                    let _ = validate_timestamp(begin)?;
                }
                if let Some(end) = &range.end {
                    let _ = validate_timestamp(end)?;
                }
            }
        }

        Ok(())
    }
}

/// Validate and parse an ISO 8601 timestamp
fn validate_timestamp(ts_str: &str) -> TinyFSResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            tinyfs::Error::Other(format!(
                "Invalid timestamp '{}': {}. Expected ISO 8601/RFC 3339 format (e.g., '2023-11-06T14:00:00Z')",
                ts_str, e
            ))
        })
}

/// Generate SQL query for timeseries join with per-input time ranges
///
/// Strategy: Use FULL OUTER JOIN on timestamp to preserve all unique timestamps.
/// To handle duplicate column names (when multiple inputs have same scope), we use
/// explicit STRUCT() to group each input's columns, then unnest with unique names.
fn generate_timeseries_join_sql(
    config: &TimeseriesJoinConfig,
) -> TinyFSResult<(String, HashMap<String, crate::Url>)> {
    if config.inputs.is_empty() {
        return Err(tinyfs::Error::Other(
            "At least one input must be specified".to_string(),
        ));
    }

    if config.inputs.len() == 1 {
        return Err(tinyfs::Error::Other(
            "Timeseries join requires at least 2 inputs. Use sql-derived-series for single sources.".to_string(),
        ));
    }

    // Generate table aliases and patterns map
    let mut patterns = HashMap::new();
    let table_names: Vec<String> = config
        .inputs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            let alias = format!("input{}", i);
            _ = patterns.insert(alias.clone(), input.pattern.clone());
            alias
        })
        .collect();

    // Build CTEs for each input with optional time range filtering
    let mut ctes = Vec::new();

    for (i, (table_name, input)) in table_names.iter().zip(config.inputs.iter()).enumerate() {
        let cte_name = format!("filtered{}", i);

        if let Some(range) = &input.range {
            let mut conditions = Vec::new();
            if let Some(begin) = &range.begin {
                let _validated = validate_timestamp(begin)?;
                conditions.push(format!("{} >= '{}'", config.time_column, begin));
            }
            if let Some(end) = &range.end {
                let _validated = validate_timestamp(end)?;
                conditions.push(format!("{} <= '{}'", config.time_column, end));
            }
            if !conditions.is_empty() {
                ctes.push(format!(
                    "{} AS (SELECT * FROM {} WHERE {})",
                    cte_name,
                    table_name,
                    conditions.join(" AND ")
                ));
            } else {
                ctes.push(format!("{} AS (SELECT * FROM {})", cte_name, table_name));
            }
        } else {
            ctes.push(format!("{} AS (SELECT * FROM {})", cte_name, table_name));
        }
    }

    // Build FULL OUTER JOIN chain for all inputs
    let first_cte = "filtered0";
    let mut join_sql = format!("FROM {}", first_cte);

    for i in 1..config.inputs.len() {
        let cte_name = format!("filtered{}", i);
        join_sql.push_str(&format!(
            "\nFULL OUTER JOIN {} USING ({})",
            cte_name, config.time_column
        ));
    }

    // Build column selections - use * with proper deduplication via USING clause
    // USING clause merges the join columns, giving us a single timestamp column in the result
    // Select it from the first table to avoid ambiguity, then select other columns from each
    let mut column_selections = vec![format!("filtered0.{}", config.time_column)];
    for i in 0..config.inputs.len() {
        column_selections.push(format!("filtered{}.* EXCLUDE ({})", i, config.time_column));
    }

    let with_clause = if !ctes.is_empty() {
        format!("WITH\n{}\n", ctes.join(",\n"))
    } else {
        String::new()
    };

    let sql = format!(
        "{}SELECT\n  {}\n{}\nORDER BY {}",
        with_clause,
        column_selections.join(",\n  "),
        join_sql,
        config.time_column
    );

    Ok((sql, patterns))
}

/// Timeseries join file implementation
/// Wraps SqlDerivedFile with auto-generated join SQL
pub struct TimeseriesJoinFile {
    config: TimeseriesJoinConfig,
    context: crate::FactoryContext,
    // Lazy-initialized SqlDerivedFile
    inner: Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>,
}

impl TimeseriesJoinFile {
    /// Create a new TimeseriesJoinFile with validated URL patterns
    ///
    /// # Errors
    /// Returns error if config validation fails (invalid URLs, unsupported schemes, etc.)
    pub fn new(config: TimeseriesJoinConfig, context: crate::FactoryContext) -> TinyFSResult<Self> {
        // Validate config immediately on creation
        config.validate()?;

        Ok(Self {
            config,
            context,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    /// Ensure the inner SqlDerivedFile is created
    async fn ensure_inner(&self) -> TinyFSResult<()> {
        let mut inner_guard = self.inner.lock().await;
        if inner_guard.is_none() {
            log::debug!(
                "🔍 TIMESERIES-JOIN: Generating schema-aware SQL for {} inputs",
                self.config.inputs.len()
            );

            // Generate SQL using UNION BY NAME for same-scope inputs, then FULL OUTER JOIN
            let (sql_query, patterns, scope_prefixes, pattern_transforms) =
                self.generate_union_join_sql().await?;

            log::debug!("🔍 Generated SQL:\n{}", sql_query);

            // Create SqlDerivedConfig with scope prefixes and pattern transforms
            let mut sql_config = if scope_prefixes.is_empty() {
                SqlDerivedConfig::new(patterns, Some(sql_query))
            } else {
                SqlDerivedConfig::new_scoped(patterns, Some(sql_query), scope_prefixes)
            };

            // Add pattern transforms if any inputs have them
            if !pattern_transforms.is_empty() {
                sql_config.pattern_transforms = Some(pattern_transforms);
            }

            // Create SqlDerivedFile in Series mode
            log::debug!("🔍 TIMESERIES-JOIN: Creating SqlDerivedFile with SqlDerivedMode::Series");
            let sql_file =
                SqlDerivedFile::new(sql_config, self.context.clone(), SqlDerivedMode::Series)?;
            log::debug!("✅ TIMESERIES-JOIN: Successfully created SqlDerivedFile");
            *inner_guard = Some(sql_file);
        }
        Ok(())
    }

    /// Generate SQL using UNION BY NAME for same-scope inputs, then FULL OUTER JOIN different scopes
    /// Returns: (sql_query, patterns, scope_prefixes, pattern_transforms)
    ///
    /// Strategy:
    /// 1. Group inputs by scope
    /// 2. Create CTEs that UNION BY NAME inputs with the same scope
    /// 3. FULL OUTER JOIN the scope CTEs
    /// 4. Let ScopePrefixTableProvider (via SqlDerivedConfig) apply the scope prefixes to inputN tables
    async fn generate_union_join_sql(
        &self,
    ) -> TinyFSResult<(
        String,
        HashMap<String, crate::Url>,
        HashMap<String, (String, String)>,
        HashMap<String, Vec<String>>,
    )> {
        use std::collections::BTreeMap;

        // Build patterns map - one URL per input (already validated in config)
        // AND build scope_prefixes map - one entry per input that has a scope
        // AND build pattern_transforms map - one entry per input that has transforms
        let mut patterns = HashMap::new();
        let mut scope_prefixes = HashMap::new();
        let mut pattern_transforms = HashMap::new();

        for (i, input) in self.config.inputs.iter().enumerate() {
            let table_name = format!("input{}", i);

            // Pattern is already a Url (validated during deserialization)
            _ = patterns.insert(table_name.clone(), input.pattern.clone());

            // Register scope prefix for this input's table
            if let Some(ref scope) = input.scope {
                let _ = scope_prefixes.insert(
                    table_name.clone(),
                    (scope.clone(), self.config.time_column.clone()),
                );
            }

            // Register transforms for this input's table
            if let Some(ref transforms) = input.transforms {
                let _ = pattern_transforms.insert(table_name.clone(), transforms.clone());
            }
        }

        // Group inputs by scope for UNION BY NAME
        let mut scope_groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();

        for (i, input) in self.config.inputs.iter().enumerate() {
            // Use scope as key, or generate unique key for None scopes
            let scope_key = input
                .scope
                .clone()
                .unwrap_or_else(|| format!("_none_{}", i));
            scope_groups.entry(scope_key).or_default().push(i);
        }

        log::debug!(
            "🔍 TIMESERIES-JOIN: Grouped {} inputs into {} scope groups",
            self.config.inputs.len(),
            scope_groups.len()
        );
        for (scope, indices) in &scope_groups {
            log::debug!("  Scope '{}': inputs {:?}", scope, indices);
        }

        // Build CTEs:
        // 1. Filtered CTEs for each input (with range filters)
        // 2. Combined CTEs for each scope group (UNION BY NAME if multiple inputs)
        let mut ctes = Vec::new();

        // Step 1: Create filtered CTEs for each input
        for (i, input) in self.config.inputs.iter().enumerate() {
            let table_name = format!("input{}", i);
            let filtered_name = format!("filtered{}", i);

            if let Some(range) = &input.range {
                let mut conditions = Vec::new();
                if let Some(begin) = &range.begin {
                    let _validated = validate_timestamp(begin)?;
                    conditions.push(format!("{} >= '{}'", self.config.time_column, begin));
                }
                if let Some(end) = &range.end {
                    let _validated = validate_timestamp(end)?;
                    conditions.push(format!("{} <= '{}'", self.config.time_column, end));
                }
                if !conditions.is_empty() {
                    ctes.push(format!(
                        "{} AS (SELECT * FROM {} WHERE {})",
                        filtered_name,
                        table_name,
                        conditions.join(" AND ")
                    ));
                } else {
                    ctes.push(format!(
                        "{} AS (SELECT * FROM {})",
                        filtered_name, table_name
                    ));
                }
            } else {
                ctes.push(format!(
                    "{} AS (SELECT * FROM {})",
                    filtered_name, table_name
                ));
            }
        }

        // Step 2: Create combined CTEs for each scope group using UNION BY NAME
        let mut scope_table_names: Vec<String> = Vec::new();

        for (scope_idx, (_scope, input_indices)) in scope_groups.iter().enumerate() {
            let scope_table = format!("scope_combined{}", scope_idx);
            scope_table_names.push(scope_table.clone());

            if input_indices.len() == 1 {
                // Single input in this scope - just select from it
                let input_idx = input_indices[0];
                ctes.push(format!(
                    "{} AS (SELECT * FROM filtered{})",
                    scope_table, input_idx
                ));
            } else {
                // Multiple inputs - UNION BY NAME
                let union_parts: Vec<String> = input_indices
                    .iter()
                    .map(|idx| format!("SELECT * FROM filtered{}", idx))
                    .collect();
                ctes.push(format!(
                    "{} AS ({})",
                    scope_table,
                    union_parts.join("\nUNION BY NAME\n")
                ));
            }
        }

        log::debug!(
            "🔍 TIMESERIES-JOIN: Created {} combined scope tables",
            scope_table_names.len()
        );

        // Step 3: FULL OUTER JOIN all scope tables
        let mut join_sql = format!("FROM {}", scope_table_names[0]);
        if scope_table_names.len() > 1 {
            for i in 1..scope_table_names.len() {
                join_sql.push_str(&format!(
                    "\nFULL OUTER JOIN {} ON {}.{} = {}.{}",
                    scope_table_names[i],
                    scope_table_names[0],
                    self.config.time_column,
                    scope_table_names[i],
                    self.config.time_column
                ));
            }
        }

        // Step 4: SELECT all columns
        // Let ScopePrefixTableProvider handle the prefixing for inputN tables
        let mut select_parts = Vec::new();

        // COALESCE timestamp
        if scope_table_names.len() > 1 {
            let timestamp_coalesce: Vec<String> = scope_table_names
                .iter()
                .map(|t| format!("{}.{}", t, self.config.time_column))
                .collect();
            select_parts.push(format!(
                "COALESCE({}) AS {}",
                timestamp_coalesce.join(", "),
                self.config.time_column
            ));

            // Select all other columns from each scope table (excluding timestamp)
            for table_name in &scope_table_names {
                select_parts.push(format!(
                    "{}.* EXCLUDE ({})",
                    table_name, self.config.time_column
                ));
            }
        } else {
            // Only one scope - just select everything
            select_parts.push("*".to_string());
        }

        let with_clause = if !ctes.is_empty() {
            format!("WITH\n{}\n", ctes.join(",\n"))
        } else {
            String::new()
        };

        let sql = format!(
            "{}SELECT\n  {}\n{}\nORDER BY {}",
            with_clause,
            select_parts.join(",\n  "),
            join_sql,
            self.config.time_column
        );

        log::debug!("🔍 TIMESERIES-JOIN: Generated SQL:\n{}", sql);
        log::debug!("🔍 TIMESERIES-JOIN: Scope prefixes: {:?}", scope_prefixes);
        log::debug!(
            "🔍 TIMESERIES-JOIN: Pattern transforms: {:?}",
            pattern_transforms
        );

        Ok((sql, patterns, scope_prefixes, pattern_transforms))
    }

    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl tinyfs::File for TimeseriesJoinFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("inner initialized");
        inner.async_reader().await
    }

    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("inner initialized");
        inner.async_writer().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl tinyfs::Metadata for TimeseriesJoinFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::TableDynamic,
            timestamp: 0, // @@@ Not sure
        })
    }
}

#[async_trait]
impl tinyfs::QueryableFile for TimeseriesJoinFile {
    async fn as_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn TableProvider>> {
        log::debug!("DELEGATING TimeseriesJoinFile to inner SqlDerivedFile: id={id}",);
        self.ensure_inner().await?;

        let inner_guard = self.inner.lock().await;
        let inner = inner_guard
            .as_ref()
            .expect("inner initialized by ensure_inner");
        inner.as_table_provider(id, context).await
    }
}

// Factory functions

fn create_timeseries_join_handle(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: TimeseriesJoinConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid timeseries-join config: {}", e)))?;

    let join_file = TimeseriesJoinFile::new(cfg, context)?;
    Ok(join_file.create_handle())
}

fn validate_timeseries_join_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;

    let config_value: Value = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;

    // Validate by deserializing
    let _cfg: TimeseriesJoinConfig = serde_json::from_value(config_value.clone())
        .map_err(|e| tinyfs::Error::Other(format!("Invalid configuration: {}", e)))?;

    // Additional validation: generate SQL to catch errors early
    let (_sql, _patterns) = generate_timeseries_join_sql(&_cfg)?;

    Ok(config_value)
}

// Register the factory
register_dynamic_factory!(
    name: "timeseries-join",
    description: "Create time series join files with automatic COALESCE and FULL OUTER JOIN",
    file: create_timeseries_join_handle,
    validate: validate_timeseries_join_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryableFile;
    use arrow::array::{Array, Float64Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;
    use tinyfs::{EntryType, FS, FileID, MemoryPersistence, ProviderContext};

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
    /// Returns (FS, ProviderContext) that share the SAME persistence instance
    async fn create_test_environment() -> (FS, ProviderContext) {
        let persistence = MemoryPersistence::default();
        let fs = FS::new(persistence.clone())
            .await
            .expect("Failed to create FS");
        let session = Arc::new(SessionContext::new());
        let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
            .expect("Failed to register TinyFS object store");
        let provider_context = ProviderContext::new(session, HashMap::new(), Arc::new(persistence));
        (fs, provider_context)
    }

    /// Helper to create a parquet file in both FS and persistence
    async fn create_parquet_file(
        fs: &FS,
        path: &str,
        parquet_data: Vec<u8>,
        entry_type: EntryType,
    ) -> Result<FileID, Box<dyn std::error::Error>> {
        let root = fs.root().await?;
        let mut file_writer = root.async_writer_path_with_type(path, entry_type).await?;
        use tokio::io::AsyncWriteExt;
        file_writer.write_all(&parquet_data).await?;
        file_writer.flush().await?;
        file_writer.shutdown().await?;

        // Get the FileID that was created
        let node_path = root.get_node_path(path).await?;
        let file_id = node_path.id();

        // async_writer already stores the version in persistence, no need to duplicate
        Ok(file_id)
    }

    /// Helper to create a text file (e.g., CSV) in both FS and persistence
    async fn create_text_file(
        fs: &FS,
        path: &str,
        content: Vec<u8>,
        entry_type: EntryType,
    ) -> Result<FileID, Box<dyn std::error::Error>> {
        let root = fs.root().await?;
        let mut file_writer = root.async_writer_path_with_type(path, entry_type).await?;
        use tokio::io::AsyncWriteExt;
        file_writer.write_all(&content).await?;
        file_writer.flush().await?;
        file_writer.shutdown().await?;

        // Get the FileID that was created
        let node_path = root.get_node_path(path).await?;
        let file_id = node_path.id();

        // async_writer already stores the version in persistence, no need to duplicate
        Ok(file_id)
    }

    #[test]
    fn test_validation_errors() {
        // Empty inputs
        let config_empty = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![],
        };
        assert!(generate_timeseries_join_sql(&config_empty).is_err());

        // Single input
        let config_single = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![TimeseriesInput {
                pattern: crate::Url::parse("series:///solo.series").unwrap(),
                scope: None,
                range: None,
                transforms: None,
            }],
        };
        assert!(generate_timeseries_join_sql(&config_single).is_err());

        // Invalid timestamp format
        let config_bad_time = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///a.series").unwrap(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: Some("not-a-timestamp".to_string()),
                        end: None,
                    }),
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///b.series").unwrap(),
                    scope: None,
                    range: None,
                    transforms: None,
                },
            ],
        };
        assert!(generate_timeseries_join_sql(&config_bad_time).is_err());
    }

    #[tokio::test]
    async fn test_timeseries_join_factory_integration() {
        let (fs, provider_context) = create_test_environment().await;

        // Create source1.series with timestamps [1, 2, 3] and temp_a column
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp_a", DataType::Float64, false),
        ]));

        let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
        let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(timestamps1), Arc::new(temps1)],
        )
        .unwrap();

        let mut parquet_buffer1 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer1);
            let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
            writer1.write(&batch1).unwrap();
            _ = writer1.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/source1.series",
            parquet_buffer1,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create source2.series with timestamps [2, 3, 4] and temp_b column
        let schema2 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp_b", DataType::Float64, false),
        ]));

        let timestamps2 = TimestampSecondArray::from(vec![2, 3, 4]);
        let temps2 = Float64Array::from(vec![15.0, 25.0, 35.0]);
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(timestamps2), Arc::new(temps2)],
        )
        .unwrap();

        let mut parquet_buffer2 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer2);
            let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
            writer2.write(&batch2).unwrap();
            _ = writer2.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/source2.series",
            parquet_buffer2,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Now create the timeseries join
        let context = test_context(&provider_context, FileID::root());

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source1.series").unwrap(),
                    scope: None,
                    range: None,
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source2.series").unwrap(),
                    scope: None,
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();

        // Test as_table_provider
        let table_provider = join_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Register and query
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("joined", table_provider).unwrap();

        let df = ctx
            .sql("SELECT * FROM joined ORDER BY timestamp")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = &batches[0];

        // Should have timestamps [1, 2, 3, 4] due to FULL OUTER JOIN
        assert_eq!(batch.num_rows(), 4);

        // Should have columns: timestamp, temp_a, temp_b
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_timeseries_join_with_scope_prefixes() {
        let (fs, provider_context) = create_test_environment().await;

        // Create source1.series with temp column
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
        ]));
        let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
        let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(timestamps1), Arc::new(temps1)],
        )
        .unwrap();
        let mut parquet_buffer1 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer1);
            let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
            writer1.write(&batch1).unwrap();
            _ = writer1.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/source1.series",
            parquet_buffer1,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create source2.series with pressure column
        let schema2 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("pressure", DataType::Float64, false),
        ]));
        let timestamps2 = TimestampSecondArray::from(vec![2, 3, 4]);
        let pressures = Float64Array::from(vec![100.0, 101.0, 102.0]);
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(timestamps2), Arc::new(pressures)],
        )
        .unwrap();
        let mut parquet_buffer2 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer2);
            let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
            writer2.write(&batch2).unwrap();
            _ = writer2.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/source2.series",
            parquet_buffer2,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Test with scope prefixes
        let root_id = FileID::root();
        let context = test_context(&provider_context, root_id);

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source1.series").unwrap(),
                    scope: Some("BDock".to_string()),
                    range: None,
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source2.series").unwrap(),
                    scope: Some("ADock".to_string()),
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();
        let table_provider = join_file
            .as_table_provider(root_id, &provider_context)
            .await
            .unwrap();

        // Query to verify scoped column names - use direct scan to avoid SQL optimizer
        let ctx = &provider_context.datafusion_session;
        let df_state = ctx.state();
        let plan = table_provider
            .scan(&df_state, None, &[], None)
            .await
            .unwrap();

        let task_ctx = ctx.task_ctx();
        let stream = plan.execute(0, task_ctx).unwrap();

        use futures::StreamExt;
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert!(!batches.is_empty());
        let batch = &batches[0];
        let schema = batch.schema();

        // Verify column names include scope prefixes
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            column_names.contains(&"timestamp"),
            "Should have timestamp column"
        );
        assert!(
            column_names.contains(&"BDock.temp"),
            "Should have BDock.temp column"
        );
        assert!(
            column_names.contains(&"ADock.pressure"),
            "Should have ADock.pressure column"
        );
    }

    #[tokio::test]
    async fn test_timeseries_join_same_scope_non_overlapping_ranges() {
        // Test the Silver case: two Vulink devices with same scope but non-overlapping time ranges
        // This should use UNION BY NAME and produce proper column names with scope prefix
        let (fs, provider_context) = create_test_environment().await;

        // Create vulink1.series with temp and conductivity columns (timestamps 1-3)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
            Field::new("conductivity", DataType::Float64, false),
        ]));
        let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
        let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let cond1 = Float64Array::from(vec![100.0, 110.0, 120.0]);
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(timestamps1), Arc::new(temps1), Arc::new(cond1)],
        )
        .unwrap();
        let mut parquet_buffer1 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer1);
            let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
            writer1.write(&batch1).unwrap();
            _ = writer1.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/vulink1.series",
            parquet_buffer1,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create vulink2.series with same schema (timestamps 5-7, non-overlapping)
        let schema2 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
            Field::new("conductivity", DataType::Float64, false),
        ]));
        let timestamps2 = TimestampSecondArray::from(vec![5, 6, 7]);
        let temps2 = Float64Array::from(vec![40.0, 50.0, 60.0]);
        let cond2 = Float64Array::from(vec![130.0, 140.0, 150.0]);
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(timestamps2), Arc::new(temps2), Arc::new(cond2)],
        )
        .unwrap();
        let mut parquet_buffer2 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer2);
            let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
            writer2.write(&batch2).unwrap();
            _ = writer2.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/vulink2.series",
            parquet_buffer2,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create at500.series with different columns (timestamps 2-6, overlapping both)
        let schema3 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("pressure", DataType::Float64, false),
        ]));
        let timestamps3 = TimestampSecondArray::from(vec![2, 4, 6]);
        let pressures = Float64Array::from(vec![1000.0, 1010.0, 1020.0]);
        let batch3 = RecordBatch::try_new(
            schema3.clone(),
            vec![Arc::new(timestamps3), Arc::new(pressures)],
        )
        .unwrap();
        let mut parquet_buffer3 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer3);
            let mut writer3 = ArrowWriter::try_new(cursor, schema3, None).unwrap();
            writer3.write(&batch3).unwrap();
            _ = writer3.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/at500.series",
            parquet_buffer3,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Test with same scope for both Vulinks
        let root_id = FileID::root();
        let context = test_context(&provider_context, root_id);

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///vulink1.series").unwrap(),
                    scope: Some("Vulink".to_string()),
                    range: Some(TimeRange {
                        begin: None,
                        end: Some("1970-01-01T00:00:03Z".to_string()),
                    }),
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///vulink2.series").unwrap(),
                    scope: Some("Vulink".to_string()),
                    range: Some(TimeRange {
                        begin: Some("1970-01-01T00:00:05Z".to_string()),
                        end: None,
                    }),
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///at500.series").unwrap(),
                    scope: Some("AT500_Surface".to_string()),
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();
        let table_provider = join_file
            .as_table_provider(root_id, &provider_context)
            .await
            .unwrap();

        // Query to verify scoped column names and UNION BY NAME behavior
        let ctx = &provider_context.datafusion_session;
        let df_state = ctx.state();
        let plan = table_provider
            .scan(&df_state, None, &[], None)
            .await
            .unwrap();

        let task_ctx = ctx.task_ctx();
        let stream = plan.execute(0, task_ctx).unwrap();

        use futures::StreamExt;
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert!(!batches.is_empty());
        let batch = &batches[0];
        let schema = batch.schema();

        // Verify column names
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(
            column_names.contains(&"timestamp"),
            "Should have timestamp column"
        );
        assert!(
            column_names.contains(&"Vulink.temp"),
            "Should have Vulink.temp column"
        );
        assert!(
            column_names.contains(&"Vulink.conductivity"),
            "Should have Vulink.conductivity column"
        );
        assert!(
            column_names.contains(&"AT500_Surface.pressure"),
            "Should have AT500_Surface.pressure column"
        );

        // Verify we have all unique timestamps from all sources (1,2,3,4,5,6,7)
        assert_eq!(batch.num_rows(), 7, "Should have 7 unique timestamps");
    }

    #[tokio::test]
    async fn test_timeseries_join_csv_different_schemas() {
        // Test CSV format provider with multiple files having different schemas
        // This validates the UNION BY NAME logic in sql_derived.rs
        let (fs, provider_context) = create_test_environment().await;

        // Create sensor1.csv with timestamp, temp, humidity
        let csv1_content = "timestamp,temp,humidity\n\
                           2024-01-01T00:00:00Z,20.5,65.0\n\
                           2024-01-01T01:00:00Z,21.0,63.0\n\
                           2024-01-01T02:00:00Z,21.5,62.0\n";

        _ = create_text_file(
            &fs,
            "/sensor1.csv",
            csv1_content.as_bytes().to_vec(),
            EntryType::FilePhysicalVersion,
        )
        .await
        .unwrap();

        // Create sensor2.csv with timestamp, temp, pressure (different schema - no humidity, has pressure)
        let csv2_content = "timestamp,temp,pressure\n\
                           2024-01-01T01:00:00Z,22.0,1013.0\n\
                           2024-01-01T02:00:00Z,22.5,1012.0\n\
                           2024-01-01T03:00:00Z,23.0,1011.0\n";

        _ = create_text_file(
            &fs,
            "/sensor2.csv",
            csv2_content.as_bytes().to_vec(),
            EntryType::FilePhysicalVersion,
        )
        .await
        .unwrap();

        // Create timeseries-join with CSV patterns
        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("csv:///sensor1.csv").unwrap(),
                    range: None,
                    transforms: None,
                    scope: Some("sensor1".to_string()),
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("csv:///sensor2.csv").unwrap(),
                    range: None,
                    transforms: None,
                    scope: Some("sensor2".to_string()),
                },
            ],
        };

        let factory_context = test_context(&provider_context, FileID::root());

        let join_file = TimeseriesJoinFile::new(config, factory_context).unwrap();

        // Create table provider - this should succeed with UNION BY NAME handling schema differences
        let table_provider = join_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .expect("Should create table provider");

        // Query the joined data
        let ctx = SessionContext::new();
        _ = ctx.register_table("joined", table_provider).unwrap();
        let df = ctx
            .sql("SELECT * FROM joined ORDER BY timestamp")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty(), "Should have results");
        let batch = &batches[0];

        // Verify schema includes columns from both sensors (with NULL for missing values)
        let schema = batch.schema();
        assert!(
            schema.column_with_name("timestamp").is_some(),
            "Should have timestamp column"
        );
        assert!(
            schema.column_with_name("sensor1.temp").is_some(),
            "Should have sensor1.temp column"
        );
        assert!(
            schema.column_with_name("sensor1.humidity").is_some(),
            "Should have sensor1.humidity column (NULL for sensor2 rows)"
        );
        assert!(
            schema.column_with_name("sensor2.temp").is_some(),
            "Should have sensor2.temp column"
        );
        assert!(
            schema.column_with_name("sensor2.pressure").is_some(),
            "Should have sensor2.pressure column (NULL for sensor1 rows)"
        );

        // Verify we have all timestamps from both sensors (4 unique: 00:00, 01:00, 02:00, 03:00)
        assert_eq!(batch.num_rows(), 4, "Should have 4 unique timestamps");

        // Verify UNION BY NAME filled NULLs correctly
        // At 00:00: sensor1 has data, sensor2 should be NULL
        // At 01:00: both have data
        // At 02:00: both have data
        // At 03:00: sensor2 has data, sensor1 should be NULL
        use arrow::array::AsArray;
        let sensor1_humidity = batch
            .column_by_name("sensor1.humidity")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>();
        let sensor2_pressure = batch
            .column_by_name("sensor2.pressure")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>();

        // First row (00:00): sensor1.humidity=65.0, sensor2.pressure=NULL
        assert_eq!(sensor1_humidity.value(0), 65.0);
        assert!(
            sensor2_pressure.is_null(0),
            "sensor2.pressure should be NULL at 00:00"
        );

        // Last row (03:00): sensor1.humidity=NULL, sensor2.pressure=1011.0
        assert!(
            sensor1_humidity.is_null(3),
            "sensor1.humidity should be NULL at 03:00"
        );
        assert_eq!(sensor2_pressure.value(3), 1011.0);
    }
}
