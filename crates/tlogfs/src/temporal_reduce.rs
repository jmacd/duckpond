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
//!   - name: "single_site"
//!     factory: "temporal-reduce"
//!     config:
//!       in_pattern: "/combined/*"  # Glob pattern to match source files
//!       out_pattern: "$0"           # Output pattern using captured groups
//!       time_column: "timestamp"    # Column containing timestamp
//!       resolutions: [1h, 2h, 4h, 12h, 24h]
//!       aggregations:
//!         - type: "avg"
//!           columns: ["Vulink.Temperature.C", "AT500_Surface.Temperature.C"]
//!         - type: "min"
//!           columns: ["Vulink.Temperature.C", "AT500_Surface.Temperature.C"]
//!         - type: "max"  # columns optional - applies to all numeric columns
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
use datafusion::catalog::TableProvider;
use crate::query::QueryableFile;



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
    /// If not specified, applies to all numeric columns
    /// Use ["*"] for count operations
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

/// Configuration for the temporal-reduce factory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalReduceConfig {
    /// Input pattern to match source files (supports glob patterns)
    pub in_pattern: String,
    
    /// Output pattern using captured groups (e.g., "$0", "$1")
    pub out_pattern: String,
    
    /// Name of the timestamp column
    pub time_column: String,
    
    /// List of temporal resolutions (e.g., "1h", "6h", "1d")
    /// Parsed using humantime::parse_duration
    pub resolutions: Vec<String>,
    
    /// Aggregation operations to perform
    pub aggregations: Vec<AggregationConfig>,
}

/// Convert Duration to SQL interval string compatible with DuckDB
/// Deferred SQL file that generates temporal reduction SQL when schema is discovered
/// This avoids the marker-based approach by deferring SQL generation until
/// the source schema can be properly discovered
pub struct TemporalReduceSqlFile {
    config: TemporalReduceConfig,
    duration: Duration,
    source_node: tinyfs::NodeRef,
    source_path: String, // For SQL pattern reference
    context: FactoryContext,
    // Lazy-initialized actual SQL file
    inner: Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>,
}

impl TemporalReduceSqlFile {
    pub fn new(
        config: TemporalReduceConfig,
        duration: Duration,
        source_node: tinyfs::NodeRef,
        source_path: String,
        context: FactoryContext,
    ) -> Self {
        Self {
            config,
            duration,
            source_node,
            source_path,
            context,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Discover source columns by accessing the source node directly
    async fn discover_source_columns(&self) -> TinyFSResult<Vec<String>> {
        log::debug!("TemporalReduceFile::discover_source_columns - accessing source node directly");
        
        let node_id = self.source_node.id().await;
        
        // Get the correct part_id (parent directory's node_id) using TinyFS resolve_path() pattern
        // For files, part_id should be the parent directory's node_id, not the file's node_id
        let fs = tinyfs::FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
        let tinyfs_root = fs.root().await?;
        
        // Parse the source_path to get parent directory
        let source_path_buf = std::path::PathBuf::from(&self.source_path);
        let parent_path = source_path_buf.parent()
            .ok_or_else(|| tinyfs::Error::Other("Source path has no parent directory".to_string()))?;
        
        let parent_node_path = tinyfs_root.resolve_path(parent_path).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to resolve parent path: {}", e)))?;
            
        let part_id = match parent_node_path.1 {
            tinyfs::Lookup::Found(parent_node) => parent_node.id().await,
            _ => {
                log::warn!("Parent directory not found for {}, falling back to root", self.source_path);
                tinyfs::NodeID::root()
            }
        };
        
        log::debug!("TemporalReduceFile: resolved part_id={} for file node_id={}", part_id, node_id);
        
        // Get the file handle from the node and access the file - following CLI pattern
        let node_guard = self.source_node.lock().await;
        let table_provider = match &node_guard.node_type {
            tinyfs::NodeType::File(file_handle) => {
                let file_arc = file_handle.get_file().await;
                let file_guard = file_arc.lock().await;
                
                // In temporal reduce context, source files are always QueryableFile implementations
                if let Some(queryable_file) = crate::sql_derived::try_as_queryable_file(&**file_guard) {
                    queryable_file.as_table_provider(node_id, part_id, &self.context.state).await
                        .map_err(|e| tinyfs::Error::Other(format!("QueryableFile table provider error: {}", e)))?
                } else {
                    return Err(tinyfs::Error::Other("Source file does not implement QueryableFile - temporal reduce requires queryable sources".to_string()));
                }
            }
            _ => {
                return Err(tinyfs::Error::Other("Source path does not point to a file".to_string()));
            }
        };
        
        // Get schema and extract all column names, filtering out only the timestamp column
        // We include all columns (numeric and non-numeric) and let the aggregation functions
        // handle what they can aggregate - SQL will naturally ignore non-aggregatable columns
        let schema = table_provider.schema();
        let columns: Vec<String> = schema.fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name != &self.config.time_column)
            .collect();
        
        // Fail fast if no columns are discovered (following DuckPond's fail-fast architectural principles)
        if columns.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "Schema discovery failed: no columns found in source file '{}'. \
                Table provider returned schema with {} total fields. \
                This indicates a problem with the table provider configuration or partition pruning. \
                Check that the correct part_id ({}) and node_id ({}) are being used.",
                self.source_path,
                schema.fields().len(),
                part_id,
                node_id
            )));
        }
        
        Ok(columns)
    }

    /// Generate SQL with discovered schema
    async fn generate_sql_with_discovered_schema(&self) -> TinyFSResult<String> {
        // Discover available columns
        let discovered_columns = self.discover_source_columns().await?;
        log::debug!("TemporalReduceFile: discovered {} columns: {:?}", discovered_columns.len(), discovered_columns);
        
        // Create a modified config with discovered columns filled in
        let mut modified_config = self.config.clone();
        
        for agg in &mut modified_config.aggregations {
            if agg.columns.is_none() {
                // Use all discovered columns for this aggregation
                agg.columns = Some(discovered_columns.clone());
                log::debug!("TemporalReduceFile: filled {} aggregation with {} columns", agg.agg_type.to_sql(), discovered_columns.len());
            }
        }
        
        // Now call the existing generate_temporal_sql function with filled-in columns
        let sql = generate_temporal_sql(&modified_config, self.duration, &self.source_path, &self.context).await?;
        log::info!("🔍 TEMPORAL-REDUCE SQL for {}: \n{}", &self.source_path, sql);
        Ok(sql)
    }

    /// Ensure the inner SqlDerivedFile is created with discovered schema
    async fn ensure_inner(&self) -> TinyFSResult<()> {
        let mut inner_guard = self.inner.lock().await;
        if inner_guard.is_none() {
            // Generate the SQL query with schema discovery
            log::debug!("🔍 TEMPORAL-REDUCE: Generating SQL query for source path: {}", self.source_path);
            let sql_query = self.generate_sql_with_discovered_schema().await?;
            log::debug!("🔍 TEMPORAL-REDUCE: Generated SQL query: {}", sql_query);
            
            // Create the actual SqlDerivedFile
            log::debug!("🔍 TEMPORAL-REDUCE: Creating SqlDerivedConfig with pattern 'series' -> '{}'", self.source_path);
            let sql_config = SqlDerivedConfig {
                patterns: {
                    let mut patterns = HashMap::new();
                    patterns.insert("series".to_string(), self.source_path.clone());
                    patterns
                },
                query: Some(sql_query.clone()),
            };
            
            log::info!("🔍 TEMPORAL-REDUCE SqlDerivedConfig for '{}': query=\n{}", self.source_path, sql_query);
            
            log::debug!("🔍 TEMPORAL-REDUCE: Creating SqlDerivedFile with SqlDerivedMode::Series");
            let sql_file = SqlDerivedFile::new(sql_config, self.context.clone(), SqlDerivedMode::Series)?;
            log::debug!("✅ TEMPORAL-REDUCE: Successfully created SqlDerivedFile");
            *inner_guard = Some(sql_file);
        }
        Ok(())
    }

    pub fn create_handle(self) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl tinyfs::File for TemporalReduceSqlFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().unwrap();
        inner.async_reader().await
    }
    
    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().unwrap();
        inner.async_writer().await
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceSqlFile {
    async fn metadata(&self) -> tinyfs::Result<tinyfs::NodeMetadata> {
        // Return lightweight metadata without expensive schema discovery
        // This allows list operations to be fast - schema discovery is deferred
        // until actual content access (as_table_provider, async_reader, etc.)
        Ok(tinyfs::NodeMetadata {
            version: 1,
            size: None, // Unknown until SQL is generated and data computed
            sha256: None, // Unknown until SQL is generated and data computed
            entry_type: tinyfs::EntryType::FileSeries, // Temporal reduce always creates series files
            timestamp: 0, // Use epoch time for dynamic content
        })
    }
}

#[async_trait]
impl QueryableFile for TemporalReduceSqlFile {
    async fn as_table_provider(
        &self,
        node_id: tinyfs::NodeID,
        part_id: tinyfs::NodeID,
        state: &crate::persistence::State,
    ) -> Result<Arc<dyn TableProvider>, crate::error::TLogFSError> {
        log::info!("📋 DELEGATING TemporalReduceSqlFile to inner file: node_id={}, part_id={}", node_id, part_id);
        self.ensure_inner().await.map_err(|e| crate::error::TLogFSError::TinyFS(e))?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().unwrap();
        inner.as_table_provider(node_id, part_id, state).await
    }
}

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
async fn generate_temporal_sql(
    config: &TemporalReduceConfig,
    interval: Duration,
    _source_path: &str,
    _context: &FactoryContext,
) -> TinyFSResult<String> {
    let interval = duration_to_sql_interval(interval);
    
    // Build aggregation expressions for the CTE and collect aliases for final SELECT
    let mut agg_exprs = Vec::new();
    let mut final_select_exprs = Vec::new();
    
    for agg in &config.aggregations {
        match &agg.columns {
            Some(columns) => {
                // Specific columns provided
                for column in columns {
                    if column == "*" && matches!(agg.agg_type, AggregationType::Count) {
                        // Special case: count(*) becomes "timestamp.count" (count of distinct timestamps)
                        let alias = "timestamp.count";
                        agg_exprs.push(format!("{}(*) AS \"{}\"", agg.agg_type.to_sql(), alias));
                        final_select_exprs.push(format!("\"{}\"", alias));
                    } else {
                        // Generate alias in format: scope.parameter.unit.agg
                        let alias = format!("{}.{}", column, agg.agg_type.to_sql().to_lowercase());
                        
                        // Insert quotes around column name and alias for SQL (DataFusion needs them for special chars)
                        agg_exprs.push(format!("{}(\"{}\") AS \"{}\"", agg.agg_type.to_sql(), column, alias));
                        final_select_exprs.push(format!("\"{}\"", alias));
                    }
                }
            }
            None => {
                // This should never happen since TemporalReduceSqlFile.generate_sql_with_discovered_schema()
                // fills in None columns before calling this function
                return Err(tinyfs::Error::Other("Internal error: generate_temporal_sql called with None columns".to_string()));
            }
        }
    }
    
    // Generate SQL with time bucketing and explicit non-nullable timestamp using COALESCE
    // COALESCE forces DataFusion to infer non-nullable schema, even though the fallback is never used
    Ok(format!(
        r#"
        WITH time_buckets AS (
          SELECT 
            DATE_TRUNC('{}', {}) AS time_bucket,
            {}
          FROM series
          WHERE {} IS NOT NULL
          GROUP BY DATE_TRUNC('{}', {})
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {},
          {}
        FROM time_buckets
        ORDER BY time_bucket
        "#,
        // Extract time unit from interval for DATE_TRUNC
        extract_time_unit_from_interval(&interval),
        config.time_column,
        agg_exprs.join(",\n            "),
        config.time_column, // WHERE clause to filter out nulls
        extract_time_unit_from_interval(&interval),
        config.time_column,
        config.time_column,
        final_select_exprs.join(",\n          ")
    ))
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
    
    /// Discover source files using the in_pattern and generate output names using out_pattern
    async fn discover_source_files(&self) -> TinyFSResult<Vec<(String, String)>> {
        let pattern = &self.config.in_pattern;
        log::debug!("TemporalReduceDirectory::discover_source_files - scanning pattern {}", pattern);
        
        let mut source_files = Vec::new();
        
        let fs = tinyfs::FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
        
        // Use collect_matches to find source files with the given pattern
        match fs.root().await?.collect_matches(&pattern).await {
            Ok(matches) => {
                for (node_path, captured) in matches {
                    let source_path = node_path.path.to_string_lossy().to_string();
                    
                    // Generate output name using out_pattern and captured groups
                    let output_name = self.substitute_pattern(&self.config.out_pattern, &captured)?;
                    log::debug!("TemporalReduceDirectory::discover_source_files - found match {} -> output {}", source_path, output_name);
                    source_files.push((source_path, output_name));
                }
            }
            Err(e) => {
                log::error!("TemporalReduceDirectory::discover_source_files - failed to match pattern {}: {}", pattern, e);
                return Err(tinyfs::Error::Other(format!("Failed to match source pattern: {}", e)));
            }
        }
        
        let count = source_files.len();
        log::debug!("TemporalReduceDirectory::discover_source_files - discovered {} source files", count);
        Ok(source_files)
    }
    
    /// Substitute pattern placeholders like $0, $1 with captured groups
    fn substitute_pattern(&self, pattern: &str, captured: &[String]) -> TinyFSResult<String> {
        let mut result = pattern.to_string();
        
        for (i, capture) in captured.iter().enumerate() {
            let placeholder = format!("${}", i);
            result = result.replace(&placeholder, capture);
        }
        
        Ok(result)
    }
    
    /// Get source node by path from discovered source files
    async fn get_source_node_by_path(&self, source_path: &str) -> TinyFSResult<tinyfs::NodeRef> {
        let fs = tinyfs::FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
        
        let matches = fs.root().await?.collect_matches(source_path).await?;
        
        if matches.is_empty() {
            return Err(tinyfs::Error::NotFound(std::path::PathBuf::from("Source file not found")));
        }
        
        let (node_path, _) = matches.into_iter().next().unwrap();
        Ok(node_path.node)
    }
    
    /// Create a site directory node with consistent logic (eliminates duplication)
    fn create_site_directory_node(&self, site_name: String, source_path: String, source_node: NodeRef) -> NodeRef {
        let site_directory = TemporalReduceSiteDirectory::new(
            site_name.clone(),
            source_path.clone(),
            source_node,
            self.config.clone(),
            self.context.clone(),
            self.parsed_resolutions.clone(),
        );
        
        // Create deterministic NodeID for this site directory
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(site_name.as_bytes());
        id_bytes.extend_from_slice(source_path.as_bytes());
        id_bytes.extend_from_slice(b"temporal-reduce-site-directory");
        let node_id = tinyfs::NodeID::from_content(&id_bytes);
        
        NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
            id: node_id,
            node_type: NodeType::Directory(site_directory.create_handle()),
        })))
    }
    
    /// Create a DirHandle from this temporal reduce directory
    pub fn create_handle(self) -> tinyfs::DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for TemporalReduceDirectory {
    async fn get(&self, name: &str) -> TinyFSResult<Option<NodeRef>> {
        // Discover all source files first
        let source_files = self.discover_source_files().await?;
        
        // Group source files by output_name - reuse same logic as entries()
        let mut sites = std::collections::HashMap::new();
        for (source_path, output_name) in source_files {
            sites.insert(output_name, source_path);
        }
        
        // Look for the requested site directory name
        if let Some(source_path) = sites.get(name) {
            // Get the source node for this site
            let source_node = self.get_source_node_by_path(&source_path).await?;
            
            // Create the site directory using shared helper
            let node_ref = self.create_site_directory_node(
                name.to_string(),
                source_path.clone(),
                source_node,
            );
            
            return Ok(Some(node_ref));
        }
        
        Ok(None)
    }

    async fn insert(&mut self, _name: String, _id: NodeRef) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other("temporal-reduce directory is read-only".to_string()))
    }

    async fn entries(&self) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<(String, NodeRef)>> + Send>>> {
        let mut entries = vec![];
        
        // Discover all source files first
        let source_files = match self.discover_source_files().await {
            Ok(files) => files,
            Err(e) => {
                entries.push(Err(e));
                return Ok(Box::pin(stream::iter(entries)));
            }
        };
        
        // Group source files by output_name to create directory structure
        let mut sites = std::collections::HashMap::new();
        for (source_path, output_name) in source_files {
            sites.insert(output_name, source_path);
        }
        
        // Create a directory entry for each unique output_name (site)
        for (site_name, source_path) in sites {
            // Get the source node for this site
            let source_node = match self.get_source_node_by_path(&source_path).await {
                Ok(node) => node,
                Err(e) => {
                    entries.push(Err(e));
                    continue;
                }
            };
            
            // Create the site directory using shared helper
            let node_ref = self.create_site_directory_node(
                site_name.clone(),
                source_path.clone(),
                source_node,
            );
            entries.push(Ok((site_name, node_ref)));
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

/// Intermediate directory for a specific site/output in temporal reduce
/// This creates the resolution files (res=1h.series, res=2h.series, etc.) for a single site
pub struct TemporalReduceSiteDirectory {
    site_name: String,
    source_path: String,
    source_node: NodeRef,
    config: TemporalReduceConfig,
    context: FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceSiteDirectory {
    pub fn new(
        site_name: String,
        source_path: String,
        source_node: NodeRef,
        config: TemporalReduceConfig,
        context: FactoryContext,
        parsed_resolutions: Vec<(String, Duration)>,
    ) -> Self {
        Self {
            site_name,
            source_path,
            source_node,
            config,
            context,
            parsed_resolutions,
        }
    }

    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Create a temporal SQL file for this site and resolution
    async fn create_temporal_sql_file(
        &self,
        duration: Duration,
    ) -> TinyFSResult<tinyfs::FileHandle> {
        let sql_file = TemporalReduceSqlFile::new(
            self.config.clone(),
            duration,
            self.source_node.clone(),
            self.source_path.clone(),
            self.context.clone(),
        );
        
        Ok(tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(sql_file)))))
    }
}

#[async_trait]
impl Directory for TemporalReduceSiteDirectory {
    async fn entries(&self) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<(String, NodeRef)>> + Send>>> {
        let mut entries = vec![];
        
        // Create an entry for each resolution
        for (res_str, duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);
            
            let sql_file = match self.create_temporal_sql_file(*duration).await {
                Ok(file) => file,
                Err(e) => {
                    entries.push(Err(e));
                    continue;
                }
            };
            
            // Create deterministic NodeID for this temporal-reduce entry
            let mut id_bytes = Vec::new();
            id_bytes.extend_from_slice(self.site_name.as_bytes());
            id_bytes.extend_from_slice(self.source_path.as_bytes());
            id_bytes.extend_from_slice(res_str.as_bytes());
            id_bytes.extend_from_slice(filename.as_bytes());
            id_bytes.extend_from_slice(b"temporal-reduce-site-entry"); // Factory type for uniqueness
            let node_id = tinyfs::NodeID::from_content(&id_bytes);
            
            let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
                id: node_id,
                node_type: NodeType::File(sql_file),
            })));
            entries.push(Ok((filename, node_ref)));
        }
        
        let stream = stream::iter(entries);
        Ok(Box::pin(stream))
    }

    async fn get(&self, name: &str) -> TinyFSResult<Option<NodeRef>> {
        // Check if the requested name matches any of our resolution files
        for (res_str, duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);
            if filename == name {
                let sql_file = self.create_temporal_sql_file(*duration).await?;
                
                // Create deterministic NodeID for this temporal-reduce entry
                let mut id_bytes = Vec::new();
                id_bytes.extend_from_slice(self.site_name.as_bytes());
                id_bytes.extend_from_slice(self.source_path.as_bytes());
                id_bytes.extend_from_slice(res_str.as_bytes());
                id_bytes.extend_from_slice(filename.as_bytes());
                id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
                let node_id = tinyfs::NodeID::from_content(&id_bytes);
                
                let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
                    id: node_id,
                    node_type: NodeType::File(sql_file),
                })));
                return Ok(Some(node_ref));
            }
        }
        Ok(None)
    }

    async fn insert(&mut self, _name: String, _node: NodeRef) -> TinyFSResult<()> {
        // Temporal reduce site directories are read-only
        Err(tinyfs::Error::Other("Temporal reduce site directories are read-only".to_string()))
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceSiteDirectory {
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
    let temporal_config: TemporalReduceConfig = serde_json::from_value(config.clone())
        .map_err(|e| tinyfs::Error::Other(format!("Invalid temporal-reduce config: {}: {:?}", e, config)))?;
    
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

    // TODO: Add integration test for generate_temporal_sql function
    // The function requires async context and FactoryContext which needs persistence layer
    // This should be tested in the integration tests where the full context is available
    
    // #[test]
    // fn test_generate_temporal_sql() {
    //     // This test is commented out because generate_temporal_sql now requires
    //     // async context and FactoryContext which are not easily available in unit tests
    // }

    #[test]
    fn test_temporal_reduce_config_serialization() {
        let config = TemporalReduceConfig {
            in_pattern: "/hydrovu/*".to_string(),
            out_pattern: "$0".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "6h".to_string(), "1d".to_string()],
            aggregations: vec![
                AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: Some(vec!["temperature".to_string(), "conductivity".to_string()]),
                },
                AggregationConfig {
                    agg_type: AggregationType::Min,
                    columns: Some(vec!["temperature".to_string()]),
                },
                AggregationConfig {
                    agg_type: AggregationType::Max,
                    columns: Some(vec!["temperature".to_string()]),
                },
                AggregationConfig {
                    agg_type: AggregationType::Count,
                    columns: Some(vec!["*".to_string()]),
                },
            ],
        };

        // Test YAML serialization
        let yaml = serde_yaml::to_string(&config).expect("Failed to serialize to YAML");
        let deserialized: TemporalReduceConfig = serde_yaml::from_str(&yaml).expect("Failed to deserialize from YAML");
        
        assert_eq!(config.in_pattern, deserialized.in_pattern);
        assert_eq!(config.out_pattern, deserialized.out_pattern);
        assert_eq!(config.time_column, deserialized.time_column);
        assert_eq!(config.resolutions, deserialized.resolutions);
        assert_eq!(config.aggregations.len(), deserialized.aggregations.len());

        // Test JSON serialization
        let json = serde_json::to_string(&config).expect("Failed to serialize to JSON");
        let deserialized: TemporalReduceConfig = serde_json::from_str(&json).expect("Failed to deserialize from JSON");
        
        assert_eq!(config.in_pattern, deserialized.in_pattern);
        assert_eq!(config.out_pattern, deserialized.out_pattern);
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
in_pattern: "/hydrovu/*"
out_pattern: "$0"
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
in_pattern: "/hydrovu/*"
out_pattern: "$0"
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
in_pattern: "/hydrovu/*"
# missing out_pattern, time_column, resolutions, and aggregations
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
            in_pattern: "/test/source/*".to_string(),
            out_pattern: "$0".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "1d".to_string()],
            aggregations: vec![
                AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: Some(vec!["temperature".to_string()]),
                },
            ],
        };

        // Create test persistence and get state
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        let tx_guard = persistence.begin(1).await.unwrap();
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
