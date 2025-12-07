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
//!           # Glob patterns supported: * matches any characters
//!           # "Vulink*.Temperature.C" matches "Vulink1.Temperature.C", "Vulink2.Temperature.C", etc.
//!           columns: ["Vulink*.Temperature.C", "AT500_Surface.Temperature.C"]
//!         - type: "min"
//!           columns: ["Vulink*.Temperature.C", "AT500_Surface.Temperature.C"]
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

use crate::factory::FactoryContext;

use crate::register_dynamic_factory;
use crate::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tinyfs::{
    DirHandle, Directory, EntryType, Node, NodeMetadata, NodeType, Result as TinyFSResult,
};

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
    source_node: Node,
    source_path: String, // For SQL pattern reference
    context: FactoryContext,
    // Lazy-initialized actual SQL file
    inner: Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>,
    // CACHE: Discovered columns to avoid repeated schema discovery
    // This prevents the O(N) cascade where discover_source_columns() triggers
    // as_table_provider() on each layer for every export operation
    discovered_columns: Arc<tokio::sync::Mutex<Option<Vec<String>>>>,
}

impl TemporalReduceSqlFile {
    #[must_use]
    pub fn new(
        config: TemporalReduceConfig,
        duration: Duration,
        source_node: Node,
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
            discovered_columns: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Discover source columns by accessing the source node directly
    /// CACHED: Only performs discovery once per TemporalReduceSqlFile instance
    async fn discover_source_columns(&self) -> TinyFSResult<Vec<String>> {
        // Check cache first - avoid repeated schema discovery cascade
        let mut columns_guard = self.discovered_columns.lock().await;

        if let Some(cached_columns) = &*columns_guard {
            log::debug!(
                "🚀 CACHE HIT (race): Using cached {} columns for temporal reduce source '{}'",
                cached_columns.len(),
                self.source_path
            );
            return Ok(cached_columns.clone());
        }

        let file_id = self.source_node.id();

        // Get the correct part_id (parent directory's node_id) using TinyFS resolve_path() pattern
        // For files, part_id should be the parent directory's node_id, not the file's node_id
        let fs = tinyfs::FS::new(self.context.state.clone())
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
        let tinyfs_root = fs.root().await?;

        // Parse the source_path to get parent directory
        let source_path_buf = std::path::PathBuf::from(&self.source_path);
        let parent_path = source_path_buf.parent().ok_or_else(|| {
            tinyfs::Error::Other("Source path has no parent directory".to_string())
        })?;

        let parent_node_path = tinyfs_root
            .resolve_path(parent_path)
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to resolve parent path: {}", e)))?;

        let parent_id = match parent_node_path.1 {
            tinyfs::Lookup::Found(parent_node) => parent_node.id(),
            _ => {
                return Err(tinyfs::Error::Other(format!(
                    "Parent directory not found for {}",
                    self.source_path,
                )));
            }
        };

        log::debug!(
            "TemporalReduceFile: file_id={}, parent_id={}",
            file_id,
            parent_id
        );

        // Get the file handle from the node - extract from node_type
        let file_handle = match &self.source_node.node_type {
            NodeType::File(handle) => handle.clone(),
            _ => return Err(tinyfs::Error::Other("Source node is not a file".to_string())),
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;

        // In temporal reduce context, source files are always QueryableFile implementations
        let table_provider = if let Some(queryable_file) =
            crate::sql_derived::try_as_queryable_file(&**file_guard)
        {
            let provider_context = self.context.state.as_provider_context();
            queryable_file
                .as_table_provider(file_id, &provider_context)
                .await
                .map_err(|e| {
                    tinyfs::Error::Other(format!(
                        "QueryableFile table provider error: {}",
                        e
                    ))
                })?
        } else {
            return Err(tinyfs::Error::Other("Source file does not implement QueryableFile - temporal reduce requires queryable sources".to_string()));
        };

        // Get schema and extract all column names, filtering out only the timestamp column
        // We include all columns (numeric and non-numeric) and let the aggregation functions
        // handle what they can aggregate - SQL will naturally ignore non-aggregatable columns
        let schema = table_provider.schema();
        let columns: Vec<String> = schema
            .fields()
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
                Check that the correct file_id ({}) is being used.",
                self.source_path,
                schema.fields().len(),
                file_id
            )));
        }

        // Cache the discovered columns for future calls
        *columns_guard = Some(columns.clone());
        log::debug!(
            "💾 CACHED: Stored {} discovered columns for temporal reduce source '{}'",
            columns.len(),
            self.source_path
        );

        Ok(columns)
    }

    /// Generate SQL with discovered schema
    async fn generate_sql_with_discovered_schema(
        &self,
        pattern_name: &str,
    ) -> TinyFSResult<String> {
        // Discover available columns
        let discovered_columns = self.discover_source_columns().await?;
        log::debug!(
            "TemporalReduceFile: discovered {} columns: {:?}",
            discovered_columns.len(),
            discovered_columns
        );

        // Create a modified config with discovered columns filled in
        let mut modified_config = self.config.clone();

        for agg in &mut modified_config.aggregations {
            match agg.columns.take() {
                None => {
                    // Use all discovered columns for this aggregation
                    agg.columns = Some(discovered_columns.clone());
                    log::debug!(
                        "TemporalReduceFile: filled {} aggregation with {} columns",
                        agg.agg_type.to_sql(),
                        discovered_columns.len()
                    );
                }
                Some(patterns) => {
                    // Apply glob matching to filter discovered columns
                    let mut matched_columns = Vec::new();
                    for pattern in &patterns {
                        for column in &discovered_columns {
                            if match_column_pattern(column, pattern) {
                                // Avoid duplicates
                                if !matched_columns.contains(column) {
                                    matched_columns.push(column.clone());
                                }
                            }
                        }
                    }
                    
                    // Replace the patterns with the matched columns
                    agg.columns = Some(matched_columns.clone());
                    log::debug!(
                        "TemporalReduceFile: {} aggregation matched {} patterns to {} columns",
                        agg.agg_type.to_sql(),
                        patterns.len(),
                        matched_columns.len()
                    );
                }
            }
        }

        // Now call the existing generate_temporal_sql function with filled-in columns
        let sql = generate_temporal_sql(
            &modified_config,
            self.duration,
            &self.source_path,
            &self.context,
            pattern_name,
        )
        .await?;
        log::debug!(
            "🔍 TEMPORAL-REDUCE SQL for {}: \n{}",
            &self.source_path,
            sql
        );
        Ok(sql)
    }

    /// Ensure the inner SqlDerivedFile is created with discovered schema
    async fn ensure_inner(&self) -> TinyFSResult<()> {
        let mut inner_guard = self.inner.lock().await;
        if inner_guard.is_none() {
            // Create unique pattern name based on source path to avoid collisions
            // when multiple temporal reduce files are active in the same session
            // CRITICAL: Lowercase to match DataFusion's case-insensitive table name handling
            let pattern_name =
                format!("source_{}", self.source_path.replace(['/', '.'], "_")).to_lowercase();

            // Generate the SQL query with schema discovery, using the unique pattern name
            log::debug!(
                "🔍 TEMPORAL-REDUCE: Generating SQL query for source path: {}",
                self.source_path
            );
            let sql_query = self
                .generate_sql_with_discovered_schema(&pattern_name)
                .await?;
            log::debug!("🔍 TEMPORAL-REDUCE: Generated SQL query: {}", sql_query);

            // Create the actual SqlDerivedFile
            log::debug!(
                "🔍 TEMPORAL-REDUCE: Creating SqlDerivedConfig with pattern '{}' -> '{}'",
                pattern_name,
                self.source_path
            );
            let sql_config = SqlDerivedConfig {
                patterns: {
                    let mut patterns = HashMap::new();
                    _ = patterns.insert(pattern_name.clone(), self.source_path.clone());
                    patterns
                },
                query: Some(sql_query.clone()),
                scope_prefixes: None,
                provider_wrapper: None,
            };

            log::debug!(
                "🔍 TEMPORAL-REDUCE SqlDerivedConfig for '{}': query=\n{}",
                self.source_path,
                sql_query
            );

            log::debug!("🔍 TEMPORAL-REDUCE: Creating SqlDerivedFile with SqlDerivedMode::Series");
            let sql_file =
                SqlDerivedFile::new(sql_config, self.context.clone(), SqlDerivedMode::Series)?;
            log::debug!("✅ TEMPORAL-REDUCE: Successfully created SqlDerivedFile");
            *inner_guard = Some(sql_file);
        }
        Ok(())
    }

    #[must_use]
    pub fn create_handle(self) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl tinyfs::File for TemporalReduceSqlFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("safelock");
        inner.async_reader().await
    }

    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("safelock");
        inner.async_writer().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceSqlFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // Return lightweight metadata without expensive schema discovery
        // This allows list operations to be fast - schema discovery is deferred
        // until actual content access (as_table_provider, async_reader, etc.)
        Ok(NodeMetadata {
            version: 1,
            size: None,   // Unknown until SQL is generated and data computed
            sha256: None, // Unknown until SQL is generated and data computed
            entry_type: EntryType::FileSeriesDynamic, // Temporal reduce always creates series files
            timestamp: 0, // Use epoch time for dynamic content
        })
    }
}

#[async_trait]
impl provider::QueryableFile for TemporalReduceSqlFile {
    async fn as_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &provider::ProviderContext,
    ) -> Result<Arc<dyn TableProvider>, provider::Error> {
        log::debug!(
            "📋 DELEGATING TemporalReduceSqlFile to inner file: id={id}",
        );
        self.ensure_inner()
            .await
            .map_err(|e| provider::Error::TinyFs(e))?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("safelock");
        inner.as_table_provider(id, context).await
    }
}

fn duration_to_sql_interval(duration: Duration) -> String {
    let total_seconds = duration.as_secs();

    // Convert to appropriate SQL interval
    if total_seconds.is_multiple_of(86400) {
        // Days
        format!("INTERVAL {} DAY", total_seconds / 86400)
    } else if total_seconds.is_multiple_of(3600) {
        // Hours
        format!("INTERVAL {} HOUR", total_seconds / 3600)
    } else if total_seconds.is_multiple_of(60) {
        // Minutes
        format!("INTERVAL {} MINUTE", total_seconds / 60)
    } else {
        // Seconds
        format!("INTERVAL {} SECOND", total_seconds)
    }
}

/// Match a column name against a glob pattern using tinyfs glob matching logic
///
/// Supports simple patterns with a single `*` wildcard:
/// - `"Vulink*.Temperature.C"` matches `"Vulink1.Temperature.C"`, `"Vulink2.Temperature.C"`, etc.
/// - `"Vulink*"` matches any column starting with "Vulink"
/// - `"*.Temperature.C"` matches any column ending with ".Temperature.C"
/// - `"*"` matches any column name
///
/// # Design
/// This reuses the same wildcard matching logic from `tinyfs::glob::WildcardComponent::Wildcard`
/// without requiring the full path parsing infrastructure. Column names aren't paths, so we
/// apply the pattern matching directly to the string.
///
/// # Limitations
/// - Only supports a single `*` wildcard per pattern (no `**` or multiple `*`)
/// - Multiple wildcards fall back to exact string matching
/// - No bracket expressions or other advanced glob features
fn match_column_pattern(column: &str, pattern: &str) -> bool {
    // If no wildcard, do exact match
    if !pattern.contains('*') {
        return column == pattern;
    }

    // Parse the pattern as a single component (not a path)
    // We can reuse the Wildcard variant logic directly
    let asterisk_count = pattern.chars().filter(|&c| c == '*').count();

    // Only support single wildcard patterns for simplicity
    if asterisk_count != 1 {
        return column == pattern; // Fall back to exact match
    }

    let wildcard_idx = pattern.find('*').expect("wildcard checked");
    let prefix = if wildcard_idx > 0 {
        Some(&pattern[..wildcard_idx])
    } else {
        None
    };

    let suffix = if wildcard_idx < pattern.len() - 1 {
        Some(&pattern[wildcard_idx + 1..])
    } else {
        None
    };

    // Match logic from WildcardComponent::Wildcard
    let prefix_str = prefix.unwrap_or("");
    let suffix_str = suffix.unwrap_or("");

    column.starts_with(prefix_str) && column.ends_with(suffix_str)
}

/// Generate SQL query for temporal aggregation
async fn generate_temporal_sql(
    config: &TemporalReduceConfig,
    interval: Duration,
    _source_path: &str,
    _context: &FactoryContext,
    pattern_name: &str,
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
                        agg_exprs.push(format!(
                            "{}(\"{}\") AS \"{}\"",
                            agg.agg_type.to_sql(),
                            column,
                            alias
                        ));
                        final_select_exprs.push(format!("\"{}\"", alias));
                    }
                }
            }
            None => {
                // This should never happen since TemporalReduceSqlFile.generate_sql_with_discovered_schema()
                // fills in None columns before calling this function
                return Err(tinyfs::Error::Other(
                    "Internal error: generate_temporal_sql called with None columns".to_string(),
                ));
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
          FROM {}
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
        pattern_name,       // Use the unique pattern name instead of hardcoded "series"
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
            let duration = humantime::parse_duration(res_str).map_err(|e| {
                tinyfs::Error::Other(format!("Invalid resolution '{}': {}", res_str, e))
            })?;
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
        log::debug!(
            "TemporalReduceDirectory::discover_source_files - scanning pattern {}",
            pattern
        );

        let mut source_files = Vec::new();

        let fs = tinyfs::FS::new(self.context.state.clone())
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        // Use collect_matches to find source files with the given pattern
        match fs.root().await?.collect_matches(&pattern).await {
            Ok(matches) => {
                for (node_path, captured) in matches {
                    let source_path = node_path.path.to_string_lossy().to_string();

                    // Generate output name using out_pattern and captured groups
                    let output_name =
                        self.substitute_pattern(&self.config.out_pattern, &captured)?;
                    log::debug!(
                        "TemporalReduceDirectory::discover_source_files - found match {} -> output {}",
                        source_path,
                        output_name
                    );
                    source_files.push((source_path, output_name));
                }
            }
            Err(e) => {
                log::error!(
                    "TemporalReduceDirectory::discover_source_files - failed to match pattern {}: {}",
                    pattern,
                    e
                );
                return Err(tinyfs::Error::Other(format!(
                    "Failed to match source pattern: {}",
                    e
                )));
            }
        }

        let count = source_files.len();
        log::debug!(
            "TemporalReduceDirectory::discover_source_files - discovered {} source files",
            count
        );
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
    async fn get_source_node_by_path(&self, source_path: &str) -> TinyFSResult<Node> {
        let fs = tinyfs::FS::new(self.context.state.clone())
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        let matches = fs.root().await?.collect_matches(source_path).await?;

        if matches.is_empty() {
            return Err(tinyfs::Error::NotFound(std::path::PathBuf::from(
                "Source file not found",
            )));
        }

        let (node_path, _) = matches.into_iter().next().expect("checked");
        Ok(node_path.node)
    }

    /// Create a site directory node with consistent logic (eliminates duplication)
    fn create_site_directory_node(
        &self,
        site_name: String,
        source_path: String,
        source_node: Node,
    ) -> Node {
        let site_directory = TemporalReduceSiteDirectory::new(
            site_name.clone(),
            source_path.clone(),
            source_node,
            self.config.clone(),
            self.context.clone(),
            self.parsed_resolutions.clone(),
        );

        // Create deterministic FileID for this site directory
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(site_name.as_bytes());
        id_bytes.extend_from_slice(source_path.as_bytes());
        id_bytes.extend_from_slice(b"temporal-reduce-site-directory");
        // Use this temporal reduce directory's NodeID as the PartID for children
        let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
        let file_id = tinyfs::FileID::from_content(parent_part_id, EntryType::DirectoryDynamic, &id_bytes);

        Node::new(file_id, NodeType::Directory(site_directory.create_handle()))
    }

    /// Create a DirHandle from this temporal reduce directory
    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for TemporalReduceDirectory {
    async fn get(&self, name: &str) -> TinyFSResult<Option<Node>> {
        // Discover all source files first
        let source_files = self.discover_source_files().await?;

        // Group source files by output_name - reuse same logic as entries()
        let mut sites = HashMap::new();
        for (source_path, output_name) in source_files {
            _ = sites.insert(output_name, source_path);
        }

        // Look for the requested site directory name
        if let Some(source_path) = sites.get(name) {
            // Get the source node for this site
            let source_node = self.get_source_node_by_path(source_path).await?;

            // Create the site directory using shared helper
            let node_ref =
                self.create_site_directory_node(name.to_string(), source_path.clone(), source_node);

            return Ok(Some(node_ref));
        }

        Ok(None)
    }

    async fn insert(&mut self, _name: String, _id: Node) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other(
            "temporal-reduce directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>> {
        // Discover all source files - fail fast on error
        let source_files = self.discover_source_files().await?;

        // Group source files by output_name to create directory structure
        let mut sites = HashMap::new();
        for (source_path, output_name) in source_files {
            _ = sites.insert(output_name, source_path);
        }

        // Create DirectoryEntry for each unique output_name (site)
        let mut entries = Vec::new();
        for (site_name, _source_path) in sites {
            // Get the actual node to extract its real node_id - fail fast on error
            let node = self.get(&site_name).await?.ok_or_else(|| {
                tinyfs::Error::Other(format!("Failed to create temporal-reduce site '{}'", site_name))
            })?;
            let dir_entry = tinyfs::DirectoryEntry::new(
                site_name.clone(),
                node.id.node_id(),
                EntryType::DirectoryDynamic,
                0,
            );
            entries.push(Ok(dir_entry));
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
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// Intermediate directory for a specific site/output in temporal reduce
/// This creates the resolution files (res=1h.series, res=2h.series, etc.) for a single site
pub struct TemporalReduceSiteDirectory {
    site_name: String,
    source_path: String,
    source_node: Node,
    config: TemporalReduceConfig,
    context: FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceSiteDirectory {
    #[must_use]
    pub fn new(
        site_name: String,
        source_path: String,
        source_node: Node,
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

    #[must_use]
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

        Ok(tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(
            Box::new(sql_file),
        ))))
    }
}

#[async_trait]
impl Directory for TemporalReduceSiteDirectory {
    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>> {
        let mut entries = vec![];

        // Create DirectoryEntry for each resolution without creating the file yet
        for (res_str, _duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);
            
            // Create deterministic FileID for this entry - note: we already have the EntryType from dir_entry below
            let mut id_bytes = Vec::new();
            id_bytes.extend_from_slice(self.site_name.as_bytes());
            id_bytes.extend_from_slice(self.source_path.as_bytes());
            id_bytes.extend_from_slice(res_str.as_bytes());
            id_bytes.extend_from_slice(filename.as_bytes());
            id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
            // Use this temporal reduce site directory's NodeID as the PartID for children
            let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
            let file_id = tinyfs::FileID::from_content(parent_part_id, EntryType::FileSeriesDynamic, &id_bytes);

            let dir_entry = tinyfs::DirectoryEntry::new(
                filename.clone(),
                file_id.node_id(),
                EntryType::FileSeriesDynamic,
                0,
            );
            entries.push(Ok(dir_entry));
        }

        let stream = stream::iter(entries);
        Ok(Box::pin(stream))
    }

    async fn get(&self, name: &str) -> TinyFSResult<Option<Node>> {
        // Check if the requested name matches any of our resolution files
        for (res_str, duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);
            if filename == name {
                let sql_file = self.create_temporal_sql_file(*duration).await?;

                // Create deterministic FileID for this temporal-reduce entry
                let mut id_bytes = Vec::new();
                id_bytes.extend_from_slice(self.site_name.as_bytes());
                id_bytes.extend_from_slice(self.source_path.as_bytes());
                id_bytes.extend_from_slice(res_str.as_bytes());
                id_bytes.extend_from_slice(filename.as_bytes());
                id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
                // Use this temporal reduce site directory's NodeID as the PartID for children
                let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
                let file_id = tinyfs::FileID::from_content(parent_part_id, EntryType::FileSeriesDynamic, &id_bytes);

                let node_ref = Node::new(file_id, NodeType::File(sql_file));
                return Ok(Some(node_ref));
            }
        }
        Ok(None)
    }

    async fn insert(&mut self, _name: String, _node: Node) -> TinyFSResult<()> {
        // Temporal reduce site directories are read-only
        Err(tinyfs::Error::Other(
            "Temporal reduce site directories are read-only".to_string(),
        ))
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceSiteDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// Create a temporal reduce directory from configuration and context
fn create_temporal_reduce_directory(
    config: Value,
    context: provider::FactoryContext,
) -> TinyFSResult<DirHandle> {
    let temporal_config: TemporalReduceConfig =
        serde_json::from_value(config.clone()).map_err(|e| {
            tinyfs::Error::Other(format!(
                "Invalid temporal-reduce config: {}: {:?}",
                e, config
            ))
        })?;

    // Convert to legacy FactoryContext for TemporalReduceDirectory which hasn't migrated yet
    let legacy_ctx = FactoryContext {
        state: crate::factory::extract_state(&context).map_err(|e| tinyfs::Error::Other(e.to_string()))?,
        file_id: context.file_id,
        pond_metadata: context.pond_metadata.clone(),
    };
    let directory = TemporalReduceDirectory::new(temporal_config, legacy_ctx)?;
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
                _ = humantime::parse_duration(res_str).map_err(|e| {
                    tinyfs::Error::Other(format!("Invalid resolution '{}': {}", res_str, e))
                })?;
            }
        }
    }

    Ok(config_value)
}

// Register the temporal-reduce factory
register_dynamic_factory!(
    name: "temporal-reduce",
    description: "Create temporal downsampling views with configurable resolutions and aggregations",
    directory: create_temporal_reduce_directory,
    validate: validate_temporal_reduce_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::{FactoryContext, FactoryRegistry};
    use crate::persistence::OpLogPersistence;
    use arrow::array::{Float64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tinyfs::arrow::SimpleParquetExt;
    use tinyfs::{EntryType, FileID, NodeType};

    /// Comprehensive integration test for temporal-reduce factory
    ///
    /// Creates multiple series files with different names (site1, site2, site3),
    /// each with 3 days of hourly data, then validates that temporal-reduce correctly:
    /// - Discovers all source files matching the pattern
    /// - Creates site subdirectories for each matched file
    /// - Generates daily aggregated files with correct avg/min/max values
    /// - Produces exactly 3 rows (one per day) for each site
    #[tokio::test]
    async fn test_temporal_reduce_multi_site_daily_aggregation() {
        let _ = env_logger::try_init();

        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Transaction 1: Create source series files with 3 days of hourly data
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();
            _ = root.create_dir_path("/sources").await.unwrap();

            // Schema: timestamp + temperature + salinity
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, false),
                Field::new("salinity", DataType::Float64, false),
            ]));

            // Create 3 series files, each with 3 days of hourly data
            // Day boundaries: Day 0 = 0-86399999ms, Day 1 = 86400000-172799999ms, Day 2 = 172800000-259199999ms
            for site_num in 1..=3 {
                let filename = format!("/sources/site{}.series", site_num);
                let base_temp = 15.0 + (site_num as f64) * 5.0; // site1=20, site2=25, site3=30
                let base_salinity = 30.0 + (site_num as f64) * 2.0; // site1=32, site2=34, site3=36

                let mut all_timestamps = Vec::new();
                let mut all_temps = Vec::new();
                let mut all_salinities = Vec::new();

                // 3 days × 24 hours = 72 data points per site
                for day in 0..3 {
                    for hour in 0..24 {
                        let timestamp_ms = (day * 86400 + hour * 3600) * 1000; // milliseconds
                        all_timestamps.push(timestamp_ms);
                        all_temps.push(base_temp + hour as f64); // temperature increases by hour
                        all_salinities.push(base_salinity + hour as f64 * 0.1);
                    }
                }

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(all_timestamps)),
                        Arc::new(Float64Array::from(all_temps)),
                        Arc::new(Float64Array::from(all_salinities)),
                    ],
                )
                .unwrap();

                // Write the series file
                root.write_parquet(&filename, &batch, EntryType::FileSeriesPhysical)
                    .await
                    .unwrap();
            }

            tx_guard.commit_test().await.unwrap();
        }

        // Transaction 2: Create temporal-reduce directory and verify aggregations
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();

            // Create temporal-reduce configuration
            let config = TemporalReduceConfig {
                in_pattern: "/sources/*.series".to_string(),
                out_pattern: "$0".to_string(), // Preserve filename as site directory name
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()], // Daily aggregation
                aggregations: vec![
                    AggregationConfig {
                        agg_type: AggregationType::Avg,
                        columns: Some(vec!["temperature".to_string(), "salinity".to_string()]),
                    },
                    AggregationConfig {
                        agg_type: AggregationType::Min,
                        columns: Some(vec!["temperature".to_string()]),
                    },
                    AggregationConfig {
                        agg_type: AggregationType::Max,
                        columns: Some(vec!["temperature".to_string()]),
                    },
                ],
            };

            let context = FactoryContext::new(state.clone(), FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            // Verify we have 3 site directories
            // Note: The pattern "*.series" wildcard captures only the part matching "*" (e.g., "site1")
            // The out_pattern "$0" uses the first capture group, so directory names are just "site1", "site2", "site3"
            use futures::StreamExt;
            let mut entries_stream = temporal_handle.entries().await.unwrap();
            let mut site_names = Vec::new();
            while let Some(entry_result) = entries_stream.next().await {
                let entry = entry_result.unwrap();
                site_names.push(entry.name.clone());
            }
            site_names.sort();
            
            // The glob pattern "/sources/*.series" captures just the part matching * (without .series)
            // so we expect "site1", "site2", "site3" as directory names
            assert_eq!(
                site_names,
                vec!["site1", "site2", "site3"],
                "Should find all 3 site directories (without .series extension)"
            );

            // Verify each site has correct daily aggregations
            for site_num in 1..=3 {
                let site_name = format!("site{}", site_num);
                let base_temp = 15.0 + (site_num as f64) * 5.0;

                // Get the site directory
                let site_node = temporal_handle.get(&site_name).await.unwrap().unwrap();
                let site_dir = match &site_node.node_type {
                    NodeType::Directory(dir) => dir,
                    _ => panic!("Expected directory for {}", site_name),
                };

                // Get the daily aggregation file
                let daily_file_node = site_dir.get("res=1d.series").await.unwrap().unwrap();
                let file_id = daily_file_node.id;

                // Read and verify the aggregated data using QueryableFile interface
                if let NodeType::File(file_handle) = &daily_file_node.node_type {
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Get table provider to query the data
                    let queryable_file = crate::sql_derived::try_as_queryable_file(&**file_guard)
                        .expect("Temporal-reduce should create QueryableFile");

                    let provider_context = state.as_provider_context();
                    let table_provider = queryable_file
                        .as_table_provider(file_id, &provider_context)
                        .await
                        .unwrap();

                    // Execute query to get results
                    let ctx = state.session_context().await.unwrap();
                    let table_name = format!("temporal_{}", site_num);
                    _ = ctx.register_table(&table_name, table_provider).unwrap();

                    let query = format!(
                        "SELECT * FROM {} ORDER BY timestamp",
                        table_name
                    );
                    let dataframe = ctx.sql(&query).await.unwrap();
                    let batches = dataframe.collect().await.unwrap();

                    // Should have exactly 3 rows (one per day)
                    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert_eq!(
                        total_rows, 3,
                        "Site {} should have exactly 3 daily aggregated rows",
                        site_num
                    );

                    // Verify schema has expected columns
                    assert!(!batches.is_empty(), "Should have at least one batch");
                    let result_schema = &batches[0].schema();
                    assert!(
                        result_schema.column_with_name("timestamp").is_some(),
                        "Should have timestamp column"
                    );
                    assert!(
                        result_schema.column_with_name("temperature.avg").is_some(),
                        "Should have temperature.avg column"
                    );
                    assert!(
                        result_schema.column_with_name("salinity.avg").is_some(),
                        "Should have salinity.avg column"
                    );
                    assert!(
                        result_schema.column_with_name("temperature.min").is_some(),
                        "Should have temperature.min column"
                    );
                    assert!(
                        result_schema.column_with_name("temperature.max").is_some(),
                        "Should have temperature.max column"
                    );

                    // Verify values for first day (day 0)
                    // Each day has 24 hours: hour 0-23
                    // Expected avg temperature: base_temp + (0+1+2+...+23)/24 = base_temp + 11.5
                    // Expected min temperature: base_temp + 0 = base_temp
                    // Expected max temperature: base_temp + 23 = base_temp + 23
                    let first_batch = &batches[0];
                    let temp_avg_col = first_batch
                        .column_by_name("temperature.avg")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let temp_min_col = first_batch
                        .column_by_name("temperature.min")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let temp_max_col = first_batch
                        .column_by_name("temperature.max")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let expected_avg = base_temp + 11.5; // Average of 0..23
                    let expected_min = base_temp;
                    let expected_max = base_temp + 23.0;

                    assert!(
                        (temp_avg_col.value(0) - expected_avg).abs() < 0.01,
                        "Site {} day 0: expected avg={}, got={}",
                        site_num,
                        expected_avg,
                        temp_avg_col.value(0)
                    );
                    assert_eq!(
                        temp_min_col.value(0),
                        expected_min,
                        "Site {} day 0: min temperature mismatch",
                        site_num
                    );
                    assert_eq!(
                        temp_max_col.value(0),
                        expected_max,
                        "Site {} day 0: max temperature mismatch",
                        site_num
                    );

                    log::info!(
                        "✅ Site {} verified: {} rows, avg={:.2}, min={:.2}, max={:.2}",
                        site_num,
                        total_rows,
                        temp_avg_col.value(0),
                        temp_min_col.value(0),
                        temp_max_col.value(0)
                    );
                } else {
                    panic!("Expected file node for {}/res=1d.series", site_name);
                }
            }

            tx_guard.commit_test().await.unwrap();
        }

        log::info!("✅ Temporal-reduce integration test completed successfully");
        log::info!("   - Created 3 series files with 72 hourly records each (3 days)");
        log::info!("   - Verified temporal-reduce discovered all 3 sites");
        log::info!("   - Validated daily aggregations: 3 rows per site");
        log::info!("   - Confirmed avg/min/max calculations are correct");
    }

    #[test]
    fn test_factory_registration() {
        let factory = FactoryRegistry::get_factory("temporal-reduce");
        assert!(factory.is_some());
        let factory = factory.unwrap();
        assert_eq!(factory.name, "temporal-reduce");
        assert!(factory.description.contains("temporal downsampling"));
    }
}
