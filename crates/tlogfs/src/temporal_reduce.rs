//! Temporal reduce dynamic factory for TLogFS
//!
//! This factory creates temporal downsampling views from existing data sources, 
//! providing time-bucketed aggregations similar to the original reduce module but 
//! using the modern TLogFS dynamic factory system.
//!
//! ## Factory Type: `temporal-reduce`
//!
//! Creates a dynamic directory with resolution-based files, where each resolution
//! represents a different temporal downsampling level (e.g., 1h, 6h, 1d).
//!
//! ## Configuration
//!
//! ```yaml
//! entries:
//!   - name: "BDockDownsampled"
//!     factory: "temporal-reduce"
//!     config:
//!       source: "/hydrovu/BDock"  # Source series or sql-derived node
//!       time_column: "timestamp"   # Column containing timestamp
//!       resolutions:
//!         - "1h"                   # 1 hour buckets
//!         - "6h"                   # 6 hour buckets  
//!         - "1d"                   # 1 day buckets
//!       aggregations:
//!         - type: "avg"
//!           columns: ["temperature", "conductivity"]
//!         - type: "min"
//!           columns: ["temperature"]
//!         - type: "max"
//!           columns: ["temperature"]
//!         - type: "count"
//!           columns: ["*"]
//! ```
//!
//! ## Generated Structure
//! 
//! The factory creates a directory with files named by resolution:
//! - `res=1h.series` - 1 hour aggregated data
//! - `res=6h.series` - 6 hour aggregated data  
//! - `res=1d.series` - 1 day aggregated data
//!
//! Each file contains time-bucketed aggregations using SQL GROUP BY operations.

use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use std::pin::Pin;
use tinyfs::{DirHandle, Result as TinyFSResult, NodeMetadata, EntryType, Directory, NodeRef, Node, NodeType};
use crate::register_dynamic_factory;
use crate::factory::FactoryContext;
use crate::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use async_trait::async_trait;
use futures::stream::{self, Stream};


/// Aggregation types supported by the temporal reduce factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationType {
    Avg,
    Min,
    Max,
    Count,
    Sum,
}

impl AggregationType {
    /// Convert to SQL function name
    fn to_sql(&self) -> &'static str {
        match self {
            AggregationType::Avg => "AVG",
            AggregationType::Min => "MIN", 
            AggregationType::Max => "MAX",
            AggregationType::Count => "COUNT",
            AggregationType::Sum => "SUM",
        }
    }
}

/// Configuration for a single aggregation operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    /// Type of aggregation (avg, min, max, count, sum)
    #[serde(rename = "type")]
    pub agg_type: AggregationType,
    
    /// Columns to apply the aggregation to
    /// Use ["*"] for count operations
    pub columns: Vec<String>,
}

/// Configuration for the temporal-reduce factory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalReduceConfig {
    /// Source path to read data from (can be sql-derived series or regular series)
    pub source: String,
    
    /// Name of the timestamp column
    pub time_column: String,
    
    /// List of temporal resolutions (e.g., "1h", "6h", "1d")
    /// Parsed using humantime::parse_duration
    pub resolutions: Vec<String>,
    
    /// Aggregation operations to perform
    pub aggregations: Vec<AggregationConfig>,
}

/// Convert Duration to SQL interval string compatible with DuckDB
fn duration_to_sql_interval(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    
    // Convert to appropriate SQL interval
    if total_seconds >= 86400 && total_seconds % 86400 == 0 {
        // Days
        format!("INTERVAL {} DAY", total_seconds / 86400)
    } else if total_seconds >= 3600 && total_seconds % 3600 == 0 {
        // Hours
        format!("INTERVAL {} HOUR", total_seconds / 3600)
    } else if total_seconds >= 60 && total_seconds % 60 == 0 {
        // Minutes
        format!("INTERVAL {} MINUTE", total_seconds / 60)
    } else {
        // Seconds
        format!("INTERVAL {} SECOND", total_seconds)
    }
}

/// Generate SQL query for temporal aggregation
fn generate_temporal_sql(
    config: &TemporalReduceConfig,
    resolution: Duration,
) -> String {
    let interval = duration_to_sql_interval(resolution);
    
    // Build aggregation expressions for the CTE and collect aliases for final SELECT
    let mut agg_exprs = Vec::new();
    let mut final_select_exprs = Vec::new();
    
    for agg in &config.aggregations {
        for column in &agg.columns {
            if column == "*" && matches!(agg.agg_type, AggregationType::Count) {
                let alias = "count";
                agg_exprs.push(format!("{}(*) AS {}", agg.agg_type.to_sql(), alias));
                final_select_exprs.push(alias.to_string());
            } else {
                // Sanitize column name for alias: remove quotes and replace special chars with underscores
                let sanitized_column = column.trim_matches('"')
                    .replace('.', "_")
                    .replace(' ', "_")
                    .replace('/', "_")
                    .replace(':', "_")
                    .replace('%', "pct")
                    .replace('²', "2")
                    .replace('₂', "2")
                    .replace('³', "3")
                    .replace('µ', "u")
                    .replace('Ω', "ohm");
                let alias = format!("{}_{}", agg.agg_type.to_sql().to_lowercase(), sanitized_column);
                agg_exprs.push(format!("{}({}) AS {}", agg.agg_type.to_sql(), column, alias));
                final_select_exprs.push(alias);
            }
        }
    }
    
    // Generate SQL with time bucketing
    format!(
        r#"
        WITH time_buckets AS (
          SELECT 
            DATE_TRUNC('{}', {}) AS time_bucket,
            {}
          FROM source
          GROUP BY DATE_TRUNC('{}', {})
        )
        SELECT 
          time_bucket AS {},
          {}
        FROM time_buckets
        ORDER BY time_bucket
        "#,
        // Extract time unit from interval for DATE_TRUNC
        extract_time_unit_from_interval(&interval),
        config.time_column,
        agg_exprs.join(",\n            "),
        extract_time_unit_from_interval(&interval),
        config.time_column,
        config.time_column,
        final_select_exprs.join(",\n          ")
    )
}

/// Extract time unit from SQL interval for DATE_TRUNC
fn extract_time_unit_from_interval(interval: &str) -> &str {
    if interval.contains("DAY") {
        "day"
    } else if interval.contains("HOUR") {
        "hour"
    } else if interval.contains("MINUTE") {
        "minute"
    } else {
        "second"
    }
}

/// Dynamic directory for temporal reduce operations
pub struct TemporalReduceDirectory {
    config: TemporalReduceConfig,
    context: FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceDirectory {
    pub fn new(config: TemporalReduceConfig, context: FactoryContext) -> TinyFSResult<Self> {
        // Parse all resolutions upfront
        let mut parsed_resolutions = Vec::new();
        
        for res_str in &config.resolutions {
            let duration = humantime::parse_duration(res_str)
                .map_err(|e| tinyfs::Error::Other(format!("Invalid resolution '{}': {}", res_str, e)))?;
            parsed_resolutions.push((res_str.clone(), duration));
        }
        
        Ok(Self {
            config,
            context,
            parsed_resolutions,
        })
    }
    
    /// Create a DirHandle from this temporal reduce directory
    pub fn create_handle(self) -> tinyfs::DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for TemporalReduceDirectory {
    async fn get(&self, name: &str) -> TinyFSResult<Option<NodeRef>> {
        // Parse resolution from filename
        if !name.starts_with("res=") || !name.ends_with(".series") {
            return Ok(None);
        }
        
        let res_part = &name[4..name.len()-7]; // Remove "res=" prefix and ".series" suffix
        
        // Find matching resolution
        let duration = match self.parsed_resolutions
            .iter()
            .find(|(res_str, _)| res_str == res_part)
            .map(|(_, duration)| *duration)
        {
            Some(duration) => duration,
            None => return Ok(None),
        };
        
        // Generate SQL query for this resolution
        let sql_query = generate_temporal_sql(&self.config, duration);
        
        // Create a SQL-derived file with a single source path (not a pattern)
        // The sql-derived infrastructure will resolve "/test-locations/BDock" using resolve_path()
        // instead of collect_matches() since we expect exactly one target
        let sql_config = SqlDerivedConfig {
            patterns: {
                let mut patterns = HashMap::new();
                patterns.insert("source".to_string(), self.config.source.clone());
                patterns
            },
            query: Some(sql_query),
        };
        
        let sql_file = SqlDerivedFile::new(
            sql_config, 
            self.context.clone(), 
            SqlDerivedMode::Series
        )?;
        
        // Create deterministic NodeID based on source path and resolution
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(self.config.source.as_bytes());
        id_bytes.extend_from_slice(res_part.as_bytes());
        id_bytes.extend_from_slice(b"temporal-reduce"); // Factory type for uniqueness
        let node_id = tinyfs::NodeID::from_content(&id_bytes);
        
        // Create a NodeRef containing this file
        let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
            id: node_id,
            node_type: NodeType::File(sql_file.create_handle()),
        })));
        
        Ok(Some(node_ref))
    }

    async fn insert(&mut self, _name: String, _id: NodeRef) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other("temporal-reduce directory is read-only".to_string()))
    }

    async fn entries(&self) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<(String, NodeRef)>> + Send>>> {
        let mut entries = vec![];
        
        // Create an entry for each resolution
        for (res_str, duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);
            
            // Generate SQL query for this resolution
            let sql_query = generate_temporal_sql(&self.config, *duration);
            
            // Create a SQL-derived file for this resolution
            let sql_config = SqlDerivedConfig {
                patterns: {
                    let mut patterns = HashMap::new();
                    patterns.insert("source".to_string(), self.config.source.clone());
                    patterns
                },
                query: Some(sql_query),
            };
            
            match SqlDerivedFile::new(sql_config, self.context.clone(), SqlDerivedMode::Series) {
                Ok(sql_file) => {
                    // Create deterministic NodeID for this temporal-reduce entry
                    let mut id_bytes = Vec::new();
                    id_bytes.extend_from_slice(self.config.source.as_bytes());
                    id_bytes.extend_from_slice(res_str.as_bytes());
                    id_bytes.extend_from_slice(filename.as_bytes());
                    id_bytes.extend_from_slice(b"temporal-reduce-entry"); // Factory type for uniqueness
                    let node_id = tinyfs::NodeID::from_content(&id_bytes);
                    
                    let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
                        id: node_id,
                        node_type: NodeType::File(sql_file.create_handle()),
                    })));
                    entries.push(Ok((filename, node_ref)));
                }
                Err(e) => {
                    entries.push(Err(e));
                }
            }
        }
        
        let stream = stream::iter(entries);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::Directory,
            timestamp: 0,
        })
    }
}

/// Create a temporal reduce directory from configuration and context
fn create_temporal_reduce_directory(config: Value, context: &FactoryContext) -> TinyFSResult<DirHandle> {
    let temporal_config: TemporalReduceConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid temporal-reduce config: {}", e)))?;
    
    let directory = TemporalReduceDirectory::new(temporal_config, context.clone())?;
    Ok(directory.create_handle())
}

/// Validate temporal reduce configuration
fn validate_temporal_reduce_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8 in config: {}", e)))?;
    
    let config_value: Value = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML config: {}", e)))?;
    
    // Validate by deserializing to our config struct
    let _temporal_config: TemporalReduceConfig = serde_json::from_value(config_value.clone())
        .map_err(|e| tinyfs::Error::Other(format!("Invalid temporal-reduce config: {}", e)))?;
    
    // Additional validation: check that resolutions can be parsed
    if let Some(resolutions) = config_value.get("resolutions").and_then(|r| r.as_array()) {
        for resolution in resolutions {
            if let Some(res_str) = resolution.as_str() {
                humantime::parse_duration(res_str)
                    .map_err(|e| tinyfs::Error::Other(format!("Invalid resolution '{}': {}", res_str, e)))?;
            }
        }
    }
    
    Ok(config_value)
}

// Register the temporal-reduce factory
register_dynamic_factory!(
    name: "temporal-reduce",
    description: "Create temporal downsampling views with configurable resolutions and aggregations",
    directory_with_context: create_temporal_reduce_directory,
    validate: validate_temporal_reduce_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::FactoryRegistry;

    #[test]
    fn test_duration_to_sql_interval() {
        assert_eq!(duration_to_sql_interval(Duration::from_secs(3600)), "INTERVAL 1 HOUR");
        assert_eq!(duration_to_sql_interval(Duration::from_secs(86400)), "INTERVAL 1 DAY");
        assert_eq!(duration_to_sql_interval(Duration::from_secs(3600 * 6)), "INTERVAL 6 HOUR");
        assert_eq!(duration_to_sql_interval(Duration::from_secs(60)), "INTERVAL 1 MINUTE");
    }

    #[test]
    fn test_extract_time_unit_from_interval() {
        assert_eq!(extract_time_unit_from_interval("INTERVAL 1 HOUR"), "hour");
        assert_eq!(extract_time_unit_from_interval("INTERVAL 6 HOUR"), "hour");
        assert_eq!(extract_time_unit_from_interval("INTERVAL 1 DAY"), "day");
        assert_eq!(extract_time_unit_from_interval("INTERVAL 30 MINUTE"), "minute");
    }

    #[test]
    fn test_aggregation_type_to_sql() {
        assert_eq!(AggregationType::Avg.to_sql(), "AVG");
        assert_eq!(AggregationType::Min.to_sql(), "MIN");
        assert_eq!(AggregationType::Max.to_sql(), "MAX");
        assert_eq!(AggregationType::Count.to_sql(), "COUNT");
        assert_eq!(AggregationType::Sum.to_sql(), "SUM");
    }

    #[test]
    fn test_generate_temporal_sql() {
        let config = TemporalReduceConfig {
            source: "/test/source".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string()],
            aggregations: vec![
                AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: vec!["temperature".to_string()],
                },
                AggregationConfig {
                    agg_type: AggregationType::Count,
                    columns: vec!["*".to_string()],
                },
            ],
        };

        let sql = generate_temporal_sql(&config, Duration::from_secs(3600));
        
        // Check that the SQL contains expected components
        assert!(sql.contains("DATE_TRUNC('hour', timestamp)"));
        assert!(sql.contains("AVG(temperature) AS avg_temperature"));
        assert!(sql.contains("COUNT(*) AS count"));
        assert!(sql.contains("FROM source"));
        assert!(sql.contains("GROUP BY DATE_TRUNC('hour', timestamp)"));
        assert!(sql.contains("ORDER BY time_bucket"));
    }

    #[test]
    fn test_temporal_reduce_config_serialization() {
        let config = TemporalReduceConfig {
            source: "/hydrovu/BDock".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "6h".to_string(), "1d".to_string()],
            aggregations: vec![
                AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: vec!["temperature".to_string(), "conductivity".to_string()],
                },
                AggregationConfig {
                    agg_type: AggregationType::Min,
                    columns: vec!["temperature".to_string()],
                },
                AggregationConfig {
                    agg_type: AggregationType::Max,
                    columns: vec!["temperature".to_string()],
                },
                AggregationConfig {
                    agg_type: AggregationType::Count,
                    columns: vec!["*".to_string()],
                },
            ],
        };

        // Test YAML serialization
        let yaml = serde_yaml::to_string(&config).expect("Failed to serialize to YAML");
        let deserialized: TemporalReduceConfig = serde_yaml::from_str(&yaml).expect("Failed to deserialize from YAML");
        
        assert_eq!(config.source, deserialized.source);
        assert_eq!(config.time_column, deserialized.time_column);
        assert_eq!(config.resolutions, deserialized.resolutions);
        assert_eq!(config.aggregations.len(), deserialized.aggregations.len());

        // Test JSON serialization
        let json = serde_json::to_string(&config).expect("Failed to serialize to JSON");
        let deserialized: TemporalReduceConfig = serde_json::from_str(&json).expect("Failed to deserialize from JSON");
        
        assert_eq!(config.source, deserialized.source);
        assert_eq!(config.time_column, deserialized.time_column);
        assert_eq!(config.resolutions, deserialized.resolutions);
        assert_eq!(config.aggregations.len(), deserialized.aggregations.len());
    }

    #[test]
    fn test_factory_registration() {
        // Test that the temporal-reduce factory is properly registered
        let factory = FactoryRegistry::get_factory("temporal-reduce");
        assert!(factory.is_some());
        
        let factory = factory.unwrap();
        assert_eq!(factory.name, "temporal-reduce");
        assert!(factory.description.contains("temporal downsampling"));
        assert!(factory.create_directory_with_context.is_some());
        assert!(factory.create_file_with_context.is_none());
    }

    #[test]
    fn test_config_validation() {
        let valid_config = r#"
source: "/hydrovu/BDock"
time_column: "timestamp"
resolutions:
  - "1h"
  - "6h" 
  - "1d"
aggregations:
  - type: "avg"
    columns: ["temperature", "conductivity"]
  - type: "count"
    columns: ["*"]
"#;

        // Test valid config validation
        let result = validate_temporal_reduce_config(valid_config.as_bytes());
        assert!(result.is_ok());

        // Test invalid resolution
        let invalid_config = r#"
source: "/hydrovu/BDock"
time_column: "timestamp"
resolutions:
  - "invalid_duration"
aggregations:
  - type: "avg"
    columns: ["temperature"]
"#;

        let result = validate_temporal_reduce_config(invalid_config.as_bytes());
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid resolution"));

        // Test missing required fields
        let incomplete_config = r#"
source: "/hydrovu/BDock"
# missing time_column, resolutions, and aggregations
"#;

        let result = validate_temporal_reduce_config(incomplete_config.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_temporal_reduce_directory_creation() {
        use tempfile::TempDir;
        use crate::persistence::OpLogPersistence;
        
        // Create a test configuration
        let config = TemporalReduceConfig {
            source: "/test/source".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "1d".to_string()],
            aggregations: vec![
                AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: vec!["temperature".to_string()],
                },
            ],
        };

        // Create test persistence and get state
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
    let context = crate::factory::FactoryContext::new(state, tinyfs::NodeID::root());

        // Create the directory
        let directory = TemporalReduceDirectory::new(config, context).unwrap();
        
        // Verify parsed resolutions
        assert_eq!(directory.parsed_resolutions.len(), 2);
        assert_eq!(directory.parsed_resolutions[0].0, "1h");
        assert_eq!(directory.parsed_resolutions[0].1, Duration::from_secs(3600));
        assert_eq!(directory.parsed_resolutions[1].0, "1d");  
        assert_eq!(directory.parsed_resolutions[1].1, Duration::from_secs(86400));

        // Test that we can get entries (though they won't work without actual data)
        let entries_result = directory.entries().await;
        assert!(entries_result.is_ok());
    }
}
