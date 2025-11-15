//! Timeseries Join Factory for TLogFS
//!
//! This factory simplifies the common pattern of joining multiple time series sources
//! by timestamp, automatically generating the COALESCE + FULL OUTER JOIN + EXCLUDE SQL.

use crate::factory::FactoryContext;
use crate::query::QueryableFile;
use crate::register_dynamic_factory;
use crate::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
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
pub struct TimeRange {
    /// Optional start time (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub begin: Option<String>,
    
    /// Optional end time (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
}

/// Input source with pattern and optional time range
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesInput {
    /// Glob pattern for matching time series files
    pub pattern: String,
    
    /// Optional time range filter for this specific input
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<TimeRange>,
    
    /// Optional scope prefix to add to all column names (e.g., "BDock" -> "BDock.temperature")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// Configuration for the timeseries-join factory
#[derive(Debug, Clone, Serialize, Deserialize)]
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
fn generate_timeseries_join_sql(config: &TimeseriesJoinConfig) -> TinyFSResult<(String, HashMap<String, String>)> {
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

    // Generate table aliases (input0, input1, etc.) and build patterns map
    let mut patterns = HashMap::new();
    let table_names: Vec<String> = config.inputs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            let alias = format!("input{}", i);
            _ = patterns.insert(alias.clone(), input.pattern.clone());
            alias
        })
        .collect();

    // Build COALESCE clause for timestamp  
    let first_table = &table_names[0];
    let coalesce_parts: Vec<String> = table_names
        .iter()
        .map(|name| format!("{}.{}", name, config.time_column))
        .collect();
    let coalesce_clause = format!(
        "COALESCE({}) AS {}",
        coalesce_parts.join(", "),
        config.time_column
    );

    // Build column selections with EXCLUDE
    let mut column_selections: Vec<String> = vec![];
    
    // Always include all columns from first input
    column_selections.push(format!("{}.* EXCLUDE ({})", first_table, config.time_column));
    
    // For additional inputs, we need to handle potential schema overlap
    // Since we can't introspect schemas here, we use a heuristic:
    // - If there are only 2 inputs, assume different schemas (old behavior)
    // - If there are 3+ inputs, the middle ones likely overlap with first (time-partitioned)
    //   so we skip selecting their columns - the COALESCE timestamp + first input columns suffice
    if table_names.len() == 2 {
        // Two inputs - assume different schemas (e.g., vulink + at500)
        column_selections.push(format!("{}.* EXCLUDE ({})", table_names[1], config.time_column));
    } else {
        // Three+ inputs - only select from first input to avoid duplicates
        // The JOIN will still include all rows, just using first input's schema
        // and selecting from last input if it has unique columns
        column_selections.push(format!("{}.* EXCLUDE ({})", table_names.last().unwrap(), config.time_column));
    }

    // Build JOIN clauses with per-input time filtering
    let mut join_clauses = Vec::new();
    
    // FROM clause with optional WHERE for first input
    if let Some(range) = &config.inputs[0].range {
        let mut conditions = Vec::new();
        if let Some(begin) = &range.begin {
            let _validated = validate_timestamp(begin)?;
            conditions.push(format!("{}.{} >= '{}'", first_table, config.time_column, begin));
        }
        if let Some(end) = &range.end {
            let _validated = validate_timestamp(end)?;
            conditions.push(format!("{}.{} <= '{}'", first_table, config.time_column, end));
        }
        if !conditions.is_empty() {
            join_clauses.push(format!("FROM (SELECT * FROM {} WHERE {}) AS {}", 
                first_table, conditions.join(" AND "), first_table));
        } else {
            join_clauses.push(format!("FROM {}", first_table));
        }
    } else {
        join_clauses.push(format!("FROM {}", first_table));
    }
    
    // FULL OUTER JOIN clauses with per-input time filtering
    for (i, table_name) in table_names[1..].iter().enumerate() {
        let input_idx = i + 1;
        let join_condition = format!("{}.{} = {}.{}", 
            first_table, config.time_column, table_name, config.time_column);
        
        if let Some(range) = &config.inputs[input_idx].range {
            let mut conditions = vec![join_condition];
            if let Some(begin) = &range.begin {
                let _validated = validate_timestamp(begin)?;
                conditions.push(format!("{}.{} >= '{}'", table_name, config.time_column, begin));
            }
            if let Some(end) = &range.end {
                let _validated = validate_timestamp(end)?;
                conditions.push(format!("{}.{} <= '{}'", table_name, config.time_column, end));
            }
            join_clauses.push(format!(
                "FULL OUTER JOIN (SELECT * FROM {} WHERE {}) AS {} ON {}",
                table_name,
                conditions[1..].join(" AND "),
                table_name,
                conditions[0]
            ));
        } else {
            join_clauses.push(format!(
                "FULL OUTER JOIN {} ON {}",
                table_name, join_condition
            ));
        }
    }

    // Assemble the complete SQL
    let select_clause = format!("  {}", coalesce_clause);
    let columns_clause = column_selections
        .iter()
        .map(|col| format!("  {}", col))
        .collect::<Vec<_>>()
        .join(",\n");
    
    let sql = format!(
        "SELECT\n{},\n{}\n{}\nORDER BY {}",
        select_clause,
        columns_clause,
        join_clauses.join("\n"),
        config.time_column
    );
    
    Ok((sql, patterns))
}

/// Timeseries join file implementation
/// Wraps SqlDerivedFile with auto-generated join SQL
pub struct TimeseriesJoinFile {
    config: TimeseriesJoinConfig,
    context: FactoryContext,
    // Lazy-initialized SqlDerivedFile
    inner: Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>,
}

impl TimeseriesJoinFile {
    #[must_use]
    pub fn new(config: TimeseriesJoinConfig, context: FactoryContext) -> Self {
        Self {
            config,
            context,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Ensure the inner SqlDerivedFile is created
    async fn ensure_inner(&self) -> TinyFSResult<()> {
        let mut inner_guard = self.inner.lock().await;
        if inner_guard.is_none() {
            // Generate the SQL query and patterns map
            log::debug!(
                "🔍 TIMESERIES-JOIN: Generating SQL for {} inputs",
                self.config.inputs.len()
            );
            let (sql_query, patterns) = generate_timeseries_join_sql(&self.config)?;
            log::info!(
                "🔍 TIMESERIES-JOIN: Generated SQL:\n{}",
                sql_query
            );

            // Build scope_prefixes map from inputs that have scope set
            let mut scope_prefixes = HashMap::new();
            for (i, input) in self.config.inputs.iter().enumerate() {
                if let Some(ref scope_prefix) = input.scope {
                    let table_name = format!("input{}", i);
                    log::debug!(
                        "🔧 TIMESERIES-JOIN: Adding scope prefix '{}' for table '{}'",
                        scope_prefix, table_name
                    );
                    _ = scope_prefixes.insert(
                        table_name,
                        (scope_prefix.clone(), self.config.time_column.clone()),
                    );
                }
            }

            // Create SqlDerivedConfig with the generated patterns and scope prefixes
            let sql_config = if scope_prefixes.is_empty() {
                SqlDerivedConfig::new(patterns, Some(sql_query))
            } else {
                log::debug!(
                    "🔧 TIMESERIES-JOIN: Configuring {} scope prefixes",
                    scope_prefixes.len()
                );
                SqlDerivedConfig::new_scoped(patterns, Some(sql_query), scope_prefixes)
            };

            // Create SqlDerivedFile in Series mode
            log::debug!("🔍 TIMESERIES-JOIN: Creating SqlDerivedFile with SqlDerivedMode::Series");
            let sql_file = SqlDerivedFile::new(
                sql_config,
                self.context.clone(),
                SqlDerivedMode::Series,
            )?;
            log::debug!("✅ TIMESERIES-JOIN: Successfully created SqlDerivedFile");
            *inner_guard = Some(sql_file);
        }
        Ok(())
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

    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("inner initialized");
        inner.async_writer().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl tinyfs::Metadata for TimeseriesJoinFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::FileSeriesDynamic,
            timestamp: 0, // @@@ Not sure
        })
    }
}

#[async_trait]
impl QueryableFile for TimeseriesJoinFile {
    async fn as_table_provider(
        &self,
        node_id: tinyfs::NodeID,
        part_id: tinyfs::NodeID,
        state: &crate::persistence::State,
    ) -> Result<Arc<dyn TableProvider>, crate::error::TLogFSError> {
        log::info!(
            "📋 DELEGATING TimeseriesJoinFile to inner SqlDerivedFile: node_id={}, part_id={}",
            node_id,
            part_id
        );
        self.ensure_inner()
            .await
            .map_err(crate::error::TLogFSError::TinyFS)?;

        let inner_guard = self.inner.lock().await;
        let inner = inner_guard
            .as_ref()
            .expect("inner initialized by ensure_inner");
        inner.as_table_provider(node_id, part_id, state).await
    }
}

// Factory functions

fn create_timeseries_join_handle(
    config: Value,
    context: FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: TimeseriesJoinConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid timeseries-join config: {}", e)))?;

    let join_file = TimeseriesJoinFile::new(cfg, context);
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
    validate: validate_timeseries_join_config,
    try_as_queryable: |file| {
        file.as_any()
            .downcast_ref::<TimeseriesJoinFile>()
            .map(|f| f as &dyn QueryableFile)
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::OpLogPersistence;
    use arrow::array::{Float64Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tinyfs::{EntryType, NodeID};

    #[test]
    fn test_sql_generation_two_sources() {
        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/path/vulink.series".to_string(),
                    scope: None,
                    range: None,
                },
                TimeseriesInput {
                    pattern: "/path/at500.series".to_string(),
                    scope: None,
                    range: None,
                },
            ],
        };

        let (sql, patterns) = generate_timeseries_join_sql(&config).unwrap();
        
        // Check key components - uses input0, input1 as aliases
        assert!(sql.contains("COALESCE(input0.timestamp, input1.timestamp) AS timestamp"));
        assert!(sql.contains("input0.* EXCLUDE (timestamp)"));
        assert!(sql.contains("input1.* EXCLUDE (timestamp)"));
        assert!(sql.contains("FROM input0"));
        assert!(sql.contains("FULL OUTER JOIN input1 ON input0.timestamp = input1.timestamp"));
        assert!(sql.contains("ORDER BY timestamp"));
        assert!(!sql.contains("WHERE"));
        
        // Check patterns map
        assert_eq!(patterns.get("input0").unwrap(), "/path/vulink.series");
        assert_eq!(patterns.get("input1").unwrap(), "/path/at500.series");
    }

    #[test]
    fn test_sql_generation_with_time_range() {
        let config = TimeseriesJoinConfig {
            time_column: "ts".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/path/s1.series".to_string(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: Some("2023-11-06T14:00:00Z".to_string()),
                        end: Some("2024-04-06T07:00:00Z".to_string()),
                    }),
                },
                TimeseriesInput {
                    pattern: "/path/s2.series".to_string(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: Some("2023-11-06T14:00:00Z".to_string()),
                        end: Some("2024-04-06T07:00:00Z".to_string()),
                    }),
                },
            ],
        };

        let (sql, _) = generate_timeseries_join_sql(&config).unwrap();
        
        // With per-input ranges, the WHERE clauses are in subqueries
        assert!(sql.contains("input0.ts >= '2023-11-06T14:00:00Z' AND input0.ts <= '2024-04-06T07:00:00Z'"));
        assert!(sql.contains("ORDER BY ts"));
    }

    #[test]
    fn test_sql_generation_half_bounded() {
        // Begin only
        let config_begin = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/a.series".to_string(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: Some("2023-01-01T00:00:00Z".to_string()),
                        end: None,
                    }),
                },
                TimeseriesInput {
                    pattern: "/b.series".to_string(),
                    scope: None,
                    range: None,
                },
            ],
        };

        let (sql_begin, _) = generate_timeseries_join_sql(&config_begin).unwrap();
        assert!(sql_begin.contains("input0.timestamp >= '2023-01-01T00:00:00Z'"));

        // End only
        let config_end = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/a.series".to_string(),
                    scope: None,
                    range: None,
                },
                TimeseriesInput {
                    pattern: "/b.series".to_string(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: None,
                        end: Some("2024-01-01T00:00:00Z".to_string()),
                    }),
                },
            ],
        };

        let (sql_end, _) = generate_timeseries_join_sql(&config_end).unwrap();
        assert!(sql_end.contains("input1.timestamp <= '2024-01-01T00:00:00Z'"));
    }

    #[test]
    fn test_sql_generation_three_sources() {
        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/a.series".to_string(),
                    scope: None,
                    range: None,
                },
                TimeseriesInput {
                    pattern: "/b.series".to_string(),
                    scope: None,
                    range: None,
                },
                TimeseriesInput {
                    pattern: "/c.series".to_string(),
                    scope: None,
                    range: None,
                },
            ],
        };

        let (sql, _) = generate_timeseries_join_sql(&config).unwrap();
        
        // Should have 3-way COALESCE with input0, input1, input2
        assert!(sql.contains("COALESCE(input0.timestamp, input1.timestamp, input2.timestamp)"));
        
        // Should have 2 FULL OUTER JOINs
        let join_count = sql.matches("FULL OUTER JOIN").count();
        assert_eq!(join_count, 2);
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
            inputs: vec![
                TimeseriesInput {
                    pattern: "/solo.series".to_string(),
                    scope: None,
                    range: None,
                },
            ],
        };
        assert!(generate_timeseries_join_sql(&config_single).is_err());

        // Invalid timestamp format
        let config_bad_time = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/a.series".to_string(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: Some("not-a-timestamp".to_string()),
                        end: None,
                    }),
                },
                TimeseriesInput {
                    pattern: "/b.series".to_string(),
                    scope: None,
                    range: None,
                },
            ],
        };
        assert!(generate_timeseries_join_sql(&config_bad_time).is_err());
    }

    #[tokio::test]
    async fn test_timeseries_join_factory_integration() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Create test data files
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            // Create source1.series with timestamps [1, 2, 3] and temp_a column
            let schema1 = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("temp_a", DataType::Float64, false),
            ]));

            let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
            let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
            let batch1 = RecordBatch::try_new(
                schema1.clone(),
                vec![Arc::new(timestamps1), Arc::new(temps1)],
            ).unwrap();

            let mut parquet_buffer1 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer1);
                let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
                writer1.write(&batch1).unwrap();
                _ = writer1.close().unwrap();
            }

            let mut file_writer1 = root
                .async_writer_path_with_type("/source1.series", EntryType::FileSeriesPhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            file_writer1.write_all(&parquet_buffer1).await.unwrap();
            file_writer1.flush().await.unwrap();
            file_writer1.shutdown().await.unwrap();

            // Create source2.series with timestamps [2, 3, 4] and temp_b column
            let schema2 = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("temp_b", DataType::Float64, false),
            ]));

            let timestamps2 = TimestampSecondArray::from(vec![2, 3, 4]);
            let temps2 = Float64Array::from(vec![15.0, 25.0, 35.0]);
            let batch2 = RecordBatch::try_new(
                schema2.clone(),
                vec![Arc::new(timestamps2), Arc::new(temps2)],
            ).unwrap();

            let mut parquet_buffer2 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer2);
                let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
                writer2.write(&batch2).unwrap();
                _ = writer2.close().unwrap();
            }

            let mut file_writer2 = root
                .async_writer_path_with_type("/source2.series", EntryType::FileSeriesPhysical)
                .await
                .unwrap();
            file_writer2.write_all(&parquet_buffer2).await.unwrap();
            file_writer2.flush().await.unwrap();
            file_writer2.shutdown().await.unwrap();

            tokio::task::yield_now().await;
            tx_guard.commit_test().await.unwrap();
        }

        // Now create the timeseries join
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();
        let context = FactoryContext::new(state.clone(), NodeID::root());

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/source1.series".to_string(),
                    scope: None,
                    range: None,
                },
                TimeseriesInput {
                    pattern: "/source2.series".to_string(),
                    scope: None,
                    range: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context);
        
        // Test as_table_provider
        let table_provider = join_file
            .as_table_provider(NodeID::root(), NodeID::root(), &state)
            .await
            .unwrap();

        // Register and query
        let ctx = state.session_context().await.unwrap();
        _ = ctx.register_table("joined", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM joined ORDER BY timestamp").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = &batches[0];
        
        // Should have timestamps [1, 2, 3, 4] due to FULL OUTER JOIN
        assert_eq!(batch.num_rows(), 4);
        
        // Should have columns: timestamp, temp_a, temp_b
        assert_eq!(batch.num_columns(), 3);

        tx_guard.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_timeseries_join_with_scope_prefixes() {
        use crate::factory::FactoryContext;
        use crate::persistence::OpLogPersistence;
        use arrow::array::{Float64Array, TimestampSecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        use tinyfs::{EntryType, FS, NodeID};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();
            let fs = FS::new(state.clone()).await.unwrap();
            let root = fs.root().await.unwrap();

            // Create source1.series with temp column
            let schema1 = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("temp", DataType::Float64, false),
            ]));
            let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
            let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
            let batch1 = RecordBatch::try_new(
                schema1.clone(),
                vec![Arc::new(timestamps1), Arc::new(temps1)],
            ).unwrap();
            let mut parquet_buffer1 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer1);
                let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
                writer1.write(&batch1).unwrap();
                _ = writer1.close().unwrap();
            }
            let mut file_writer1 = root
                .async_writer_path_with_type("/source1.series", EntryType::FileSeriesPhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            file_writer1.write_all(&parquet_buffer1).await.unwrap();
            file_writer1.flush().await.unwrap();
            file_writer1.shutdown().await.unwrap();

            // Create source2.series with pressure column
            let schema2 = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("pressure", DataType::Float64, false),
            ]));
            let timestamps2 = TimestampSecondArray::from(vec![2, 3, 4]);
            let pressures = Float64Array::from(vec![100.0, 101.0, 102.0]);
            let batch2 = RecordBatch::try_new(
                schema2.clone(),
                vec![Arc::new(timestamps2), Arc::new(pressures)],
            ).unwrap();
            let mut parquet_buffer2 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer2);
                let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
                writer2.write(&batch2).unwrap();
                _ = writer2.close().unwrap();
            }
            let mut file_writer2 = root
                .async_writer_path_with_type("/source2.series", EntryType::FileSeriesPhysical)
                .await
                .unwrap();
            file_writer2.write_all(&parquet_buffer2).await.unwrap();
            file_writer2.flush().await.unwrap();
            file_writer2.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        // Test with scope prefixes
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();
        let root_id = NodeID::root();
        let context = FactoryContext::new(state.clone(), root_id);

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: "/source1.series".to_string(),
                    scope: Some("BDock".to_string()),
                    range: None,
                },
                TimeseriesInput {
                    pattern: "/source2.series".to_string(),
                    scope: Some("ADock".to_string()),
                    range: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context);
        let table_provider = join_file
            .as_table_provider(root_id, root_id, &state)
            .await
            .unwrap();

        // Query to verify scoped column names - use direct scan to avoid SQL optimizer
        let ctx = state.session_context().await.unwrap();
        let df_state = ctx.state();
        let plan = table_provider.scan(&df_state, None, &[], None).await.unwrap();
        
        let task_ctx = ctx.task_ctx();
        let stream = plan.execute(0, task_ctx).unwrap();
        
        use futures::StreamExt;
        let batches: Vec<_> = stream.collect::<Vec<_>>().await.into_iter().map(|r| r.unwrap()).collect();

        assert!(!batches.is_empty());
        let batch = &batches[0];
        let schema = batch.schema();
        
        // Verify column names include scope prefixes
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(column_names.contains(&"timestamp"), "Should have timestamp column");
        assert!(column_names.contains(&"BDock.temp"), "Should have BDock.temp column");
        assert!(column_names.contains(&"ADock.pressure"), "Should have ADock.pressure column");

        tx_guard.commit_test().await.unwrap();
    }
}
