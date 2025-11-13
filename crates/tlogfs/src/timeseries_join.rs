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

/// Configuration for the timeseries-join factory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesJoinConfig {
    /// Named patterns mapping to time series sources
    /// Keys become table aliases in the generated SQL
    pub patterns: HashMap<String, String>,
    
    /// Name of the timestamp column (defaults to "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,
    
    /// Optional time range filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range: Option<TimeRange>,
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

/// Generate SQL query for timeseries join
fn generate_timeseries_join_sql(config: &TimeseriesJoinConfig) -> TinyFSResult<String> {
    if config.patterns.is_empty() {
        return Err(tinyfs::Error::Other(
            "At least one pattern must be specified".to_string(),
        ));
    }

    if config.patterns.len() == 1 {
        return Err(tinyfs::Error::Other(
            "Timeseries join requires at least 2 patterns. Use sql-derived-series for single sources.".to_string(),
        ));
    }

    // Sort pattern names for deterministic SQL generation
    let mut pattern_names: Vec<&String> = config.patterns.keys().collect();
    pattern_names.sort();

    // Build COALESCE clause for timestamp
    let coalesce_parts: Vec<String> = pattern_names
        .iter()
        .map(|name| format!("{}.{}", name, config.time_column))
        .collect();
    let coalesce_clause = format!(
        "COALESCE({}) AS {}",
        coalesce_parts.join(", "),
        config.time_column
    );

    // Build column selections with EXCLUDE
    let column_selections: Vec<String> = pattern_names
        .iter()
        .map(|name| format!("{}.* EXCLUDE ({})", name, config.time_column))
        .collect();

    // Build JOIN clauses
    let first_table = pattern_names[0];
    let mut join_clauses = vec![format!("FROM {}", first_table)];
    
    for table_name in &pattern_names[1..] {
        join_clauses.push(format!(
            "FULL OUTER JOIN {} ON {}.{} = {}.{}",
            table_name, first_table, config.time_column, table_name, config.time_column
        ));
    }

    // Build WHERE clause for time range
    let where_clause = if let Some(time_range) = &config.time_range {
        let mut conditions = Vec::new();
        
        if let Some(begin) = &time_range.begin {
            // Validate timestamp format
            let _validated = validate_timestamp(begin)?;
            conditions.push(format!("{} >= '{}'", config.time_column, begin));
        }
        
        if let Some(end) = &time_range.end {
            // Validate timestamp format
            let _validated = validate_timestamp(end)?;
            conditions.push(format!("{} <= '{}'", config.time_column, end));
        }
        
        if !conditions.is_empty() {
            format!("WHERE {}", conditions.join(" AND "))
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // Assemble the complete SQL
    let select_clause = format!("  {}", coalesce_clause);
    let columns_clause = column_selections
        .iter()
        .map(|col| format!("  {}", col))
        .collect::<Vec<_>>()
        .join(",\n");
    
    let mut sql = format!(
        "SELECT\n{},\n{}\n{}",
        select_clause,
        columns_clause,
        join_clauses.join("\n")
    );
    
    if !where_clause.is_empty() {
        sql.push('\n');
        sql.push_str(&where_clause);
    }
    
    sql.push_str(&format!("\nORDER BY {}", config.time_column));
    
    Ok(sql)
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
            // Generate the SQL query
            log::debug!(
                "🔍 TIMESERIES-JOIN: Generating SQL for {} patterns",
                self.config.patterns.len()
            );
            let sql_query = generate_timeseries_join_sql(&self.config)?;
            log::info!(
                "🔍 TIMESERIES-JOIN: Generated SQL:\n{}",
                sql_query
            );

            // Create SqlDerivedConfig with the same patterns
            let sql_config = SqlDerivedConfig {
                patterns: self.config.patterns.clone(),
                query: Some(sql_query),
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
    let _sql = generate_timeseries_join_sql(&_cfg)?;

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
        let mut patterns = HashMap::new();
        _ = patterns.insert("vulink".to_string(), "/path/vulink.series".to_string());
        _ = patterns.insert("at500".to_string(), "/path/at500.series".to_string());

        let config = TimeseriesJoinConfig {
            patterns,
            time_column: "timestamp".to_string(),
            time_range: None,
        };

        let sql = generate_timeseries_join_sql(&config).unwrap();
        
        // Check key components
        assert!(sql.contains("COALESCE(at500.timestamp, vulink.timestamp) AS timestamp"));
        assert!(sql.contains("at500.* EXCLUDE (timestamp)"));
        assert!(sql.contains("vulink.* EXCLUDE (timestamp)"));
        assert!(sql.contains("FROM at500"));
        assert!(sql.contains("FULL OUTER JOIN vulink ON at500.timestamp = vulink.timestamp"));
        assert!(sql.contains("ORDER BY timestamp"));
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn test_sql_generation_with_time_range() {
        let mut patterns = HashMap::new();
        _ = patterns.insert("source1".to_string(), "/path/s1.series".to_string());
        _ = patterns.insert("source2".to_string(), "/path/s2.series".to_string());

        let config = TimeseriesJoinConfig {
            patterns,
            time_column: "ts".to_string(),
            time_range: Some(TimeRange {
                begin: Some("2023-11-06T14:00:00Z".to_string()),
                end: Some("2024-04-06T07:00:00Z".to_string()),
            }),
        };

        let sql = generate_timeseries_join_sql(&config).unwrap();
        
        assert!(sql.contains("WHERE ts >= '2023-11-06T14:00:00Z' AND ts <= '2024-04-06T07:00:00Z'"));
        assert!(sql.contains("ORDER BY ts"));
    }

    #[test]
    fn test_sql_generation_half_bounded() {
        let mut patterns = HashMap::new();
        _ = patterns.insert("a".to_string(), "/a.series".to_string());
        _ = patterns.insert("b".to_string(), "/b.series".to_string());

        // Begin only
        let config_begin = TimeseriesJoinConfig {
            patterns: patterns.clone(),
            time_column: "timestamp".to_string(),
            time_range: Some(TimeRange {
                begin: Some("2023-01-01T00:00:00Z".to_string()),
                end: None,
            }),
        };

        let sql_begin = generate_timeseries_join_sql(&config_begin).unwrap();
        assert!(sql_begin.contains("WHERE timestamp >= '2023-01-01T00:00:00Z'"));
        assert!(!sql_begin.contains("<="));

        // End only
        let config_end = TimeseriesJoinConfig {
            patterns,
            time_column: "timestamp".to_string(),
            time_range: Some(TimeRange {
                begin: None,
                end: Some("2024-01-01T00:00:00Z".to_string()),
            }),
        };

        let sql_end = generate_timeseries_join_sql(&config_end).unwrap();
        assert!(sql_end.contains("WHERE timestamp <= '2024-01-01T00:00:00Z'"));
        assert!(!sql_end.contains(">="));
    }

    #[test]
    fn test_sql_generation_three_sources() {
        let mut patterns = HashMap::new();
        _ = patterns.insert("a".to_string(), "/a.series".to_string());
        _ = patterns.insert("b".to_string(), "/b.series".to_string());
        _ = patterns.insert("c".to_string(), "/c.series".to_string());

        let config = TimeseriesJoinConfig {
            patterns,
            time_column: "timestamp".to_string(),
            time_range: None,
        };

        let sql = generate_timeseries_join_sql(&config).unwrap();
        
        // Should have 3-way COALESCE
        assert!(sql.contains("COALESCE(a.timestamp, b.timestamp, c.timestamp)"));
        
        // Should have 2 FULL OUTER JOINs
        let join_count = sql.matches("FULL OUTER JOIN").count();
        assert_eq!(join_count, 2);
    }

    #[test]
    fn test_validation_errors() {
        // Empty patterns
        let config_empty = TimeseriesJoinConfig {
            patterns: HashMap::new(),
            time_column: "timestamp".to_string(),
            time_range: None,
        };
        assert!(generate_timeseries_join_sql(&config_empty).is_err());

        // Single pattern
        let mut patterns = HashMap::new();
        _ = patterns.insert("solo".to_string(), "/solo.series".to_string());
        let config_single = TimeseriesJoinConfig {
            patterns,
            time_column: "timestamp".to_string(),
            time_range: None,
        };
        assert!(generate_timeseries_join_sql(&config_single).is_err());

        // Invalid timestamp format
        let mut patterns = HashMap::new();
        _ = patterns.insert("a".to_string(), "/a.series".to_string());
        _ = patterns.insert("b".to_string(), "/b.series".to_string());
        let config_bad_time = TimeseriesJoinConfig {
            patterns,
            time_column: "timestamp".to_string(),
            time_range: Some(TimeRange {
                begin: Some("not-a-timestamp".to_string()),
                end: None,
            }),
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

        let mut patterns = HashMap::new();
        _ = patterns.insert("s1".to_string(), "/source1.series".to_string());
        _ = patterns.insert("s2".to_string(), "/source2.series".to_string());

        let config = TimeseriesJoinConfig {
            patterns,
            time_column: "timestamp".to_string(),
            time_range: None,
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
}
