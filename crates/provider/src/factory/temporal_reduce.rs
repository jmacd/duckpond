// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

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
//!       in_pattern: "series:///combined/*"  # URL pattern to match source files
//!       out_pattern: "$0"           # Output basename pattern using captured groups
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

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use crate::register_dynamic_factory;
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct TemporalReduceConfig {
    /// Input URL pattern to match source files (supports glob patterns)
    pub in_pattern: crate::Url,

    /// Output basename pattern using captured groups (e.g., "$0", "$1") - no URL scheme
    pub out_pattern: String,

    /// Name of the timestamp column
    pub time_column: String,

    /// List of temporal resolutions (e.g., "1h", "6h", "1d")
    /// Parsed using humantime::parse_duration
    pub resolutions: Vec<String>,

    /// Aggregation operations to perform
    pub aggregations: Vec<AggregationConfig>,

    /// Optional list of table transform factory paths to apply to input TableProvider.
    /// Transforms are applied in order before SQL execution.
    /// Each transform is a path like "/etc/hydro_rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,
}

/// Convert Duration to SQL interval string compatible with DuckDB
/// Deferred SQL file that generates temporal reduction SQL when schema is discovered
/// This avoids the marker-based approach by deferring SQL generation until
/// the source schema can be properly discovered
pub struct TemporalReduceSqlFile {
    config: TemporalReduceConfig,
    duration: Duration,
    source_path: String, // For SQL pattern reference
    context: crate::FactoryContext,
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
        _source_node: Node,
        source_path: String,
        context: crate::FactoryContext,
    ) -> Self {
        Self {
            config,
            duration,
            source_path,
            context,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            discovered_columns: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Build the full source URL string, preserving the in_pattern's URL scheme.
    ///
    /// For format providers (oteljson://, csv://, etc.) this returns the original URL.
    /// For builtin schemes (series://, table://, file://) it re-applies the scheme to
    /// the source_path. This ensures that schema discovery and SQL execution both use
    /// the correct format provider for the source data.
    fn source_url(&self) -> String {
        let scheme = self.config.in_pattern.scheme();
        let is_format_provider = matches!(scheme, "csv" | "excelhtml" | "oteljson");

        if is_format_provider {
            // Format providers: reconstruct URL with original scheme + source path
            if self.source_path.starts_with('/') {
                format!("{}://{}", scheme, self.source_path)
            } else {
                format!("{}:///{}", scheme, self.source_path)
            }
        } else {
            // Builtin types: preserve original scheme (series, table, file)
            if self.source_path.starts_with('/') {
                format!("{}://{}", scheme, self.source_path)
            } else {
                format!("{}:///{}", scheme, self.source_path)
            }
        }
    }

    /// Discover source columns by accessing the source node directly
    /// CACHED: Only performs discovery once per TemporalReduceSqlFile instance
    async fn discover_source_columns(&self) -> TinyFSResult<Vec<String>> {
        // Check cache first - avoid repeated schema discovery cascade
        let mut columns_guard = self.discovered_columns.lock().await;

        if let Some(cached_columns) = &*columns_guard {
            log::debug!(
                "[GO] CACHE HIT (race): Using cached {} columns for temporal reduce source '{}'",
                cached_columns.len(),
                self.source_path
            );
            return Ok(cached_columns.clone());
        }

        // Use the Provider API to create a table provider, respecting the in_pattern URL scheme.
        // This is critical for format providers like oteljson://, csv://, excelhtml:// where the
        // raw file bytes need format-specific parsing to produce a queryable table.
        // Previously this went directly through as_queryable() on the raw file node, which
        // assumes Parquet format and fails for non-Parquet data files.
        let source_url = self.source_url();
        log::debug!(
            "TemporalReduceFile: discovering columns via Provider API with URL '{}'",
            source_url
        );

        let fs = self.context.context.filesystem();
        let provider =
            crate::Provider::with_context(Arc::new(fs), Arc::new(self.context.context.clone()));
        let datafusion_ctx = datafusion::prelude::SessionContext::new();

        let table_provider = provider
            .create_table_provider(&source_url, &datafusion_ctx)
            .await
            .map_err(|e| {
                tinyfs::Error::Other(format!(
                    "Failed to create table provider for source '{}': {}",
                    source_url, e
                ))
            })?;

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
                This indicates a problem with the table provider configuration or the source URL scheme.",
                self.source_path,
                schema.fields().len()
            )));
        }

        // Cache the discovered columns for future calls
        *columns_guard = Some(columns.clone());
        log::debug!(
            "[SAVE] CACHED: Stored {} discovered columns for temporal reduce source '{}'",
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
                    // Special case: COUNT with ["*"] means COUNT(*), not glob expansion
                    if matches!(agg.agg_type, AggregationType::Count)
                        && patterns.len() == 1
                        && patterns[0] == "*"
                    {
                        agg.columns = Some(vec!["*".to_string()]);
                        log::debug!("TemporalReduceFile: COUNT aggregation preserved as COUNT(*)");
                        continue;
                    }

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
            "[SEARCH] TEMPORAL-REDUCE SQL for {}: \n{}",
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
            // Replace ALL non-alphanumeric characters with underscore so the name is
            // a valid unquoted SQL identifier (spaces, parens, slashes, etc.).
            let sanitized: String = self
                .source_path
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            let pattern_name = format!("source_{}", sanitized).to_lowercase();

            // Generate the SQL query with schema discovery, using the unique pattern name
            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE: Generating SQL query for source path: {}",
                self.source_path
            );
            let sql_query = self
                .generate_sql_with_discovered_schema(&pattern_name)
                .await?;
            log::debug!("[SEARCH] TEMPORAL-REDUCE: Generated SQL query: {}", sql_query);

            // Create the actual SqlDerivedFile
            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE: Creating SqlDerivedConfig with pattern '{}' -> '{}'",
                pattern_name,
                self.source_path
            );

            // Convert source_path to URL preserving the original in_pattern scheme.
            // Previously this hardcoded series://, which broke format providers like
            // oteljson://, csv://, excelhtml:// -- the SqlDerivedFile needs the correct
            // scheme to route through the format provider instead of assuming Parquet.
            let source_url_str = self.source_url();
            let source_url = crate::Url::parse(&source_url_str).map_err(|e| {
                tinyfs::Error::Other(format!(
                    "Invalid source path URL '{}': {}",
                    source_url_str, e
                ))
            })?;

            let sql_config = SqlDerivedConfig {
                patterns: {
                    let mut patterns = HashMap::new();
                    _ = patterns.insert(pattern_name.clone(), source_url);
                    patterns
                },
                query: Some(sql_query.clone()),
                transforms: self.config.transforms.clone(),
                pattern_transforms: None,
                scope_prefixes: None,
                provider_wrapper: None,
            };

            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE SqlDerivedConfig for '{}': query=\n{}",
                self.source_path,
                sql_query
            );

            log::debug!("[SEARCH] TEMPORAL-REDUCE: Creating SqlDerivedFile with SqlDerivedMode::Series");
            let sql_file =
                SqlDerivedFile::new(sql_config, self.context.clone(), SqlDerivedMode::Series)?;
            log::debug!("[OK] TEMPORAL-REDUCE: Successfully created SqlDerivedFile");
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

    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard.as_ref().expect("safelock");
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
impl tinyfs::Metadata for TemporalReduceSqlFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // Return lightweight metadata without expensive schema discovery
        // This allows list operations to be fast - schema discovery is deferred
        // until actual content access (as_table_provider, async_reader, etc.)
        Ok(NodeMetadata {
            version: 1,
            size: None,   // Unknown until SQL is generated and data computed
            blake3: None, // Unknown until SQL is generated and data computed
            bao_outboard: None,
            entry_type: EntryType::TableDynamic, // Temporal reduce always creates series files
            timestamp: 0,                        // Use epoch time for dynamic content
        })
    }
}

#[async_trait]
impl tinyfs::QueryableFile for TemporalReduceSqlFile {
    async fn as_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn TableProvider>> {
        log::debug!("DELEGATING TemporalReduceSqlFile to inner file: id={id}",);
        self.ensure_inner().await?;
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
    _context: &crate::FactoryContext,
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

    // Generate SQL with time bucketing using date_bin() for arbitrary intervals.
    //
    // DATE_TRUNC only supports single calendar units (hour, day, etc.) and
    // discards the multiplier -- so DATE_TRUNC('hour', ts) is the same whether
    // the config says 1h, 4h, or 12h. date_bin() properly handles multi-unit
    // intervals like INTERVAL '4 HOUR' by binning from an epoch origin.
    //
    // COALESCE forces DataFusion to infer non-nullable schema, even though the
    // fallback is never used.
    Ok(format!(
        r#"
        WITH time_buckets AS (
          SELECT 
            date_bin({interval}, {ts}, TIMESTAMP '1970-01-01T00:00:00') AS time_bucket,
            {agg_exprs}
          FROM {table}
          WHERE {ts} IS NOT NULL
          GROUP BY date_bin({interval}, {ts}, TIMESTAMP '1970-01-01T00:00:00')
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {ts},
          {select_exprs}
        FROM time_buckets
        ORDER BY time_bucket
        "#,
        interval = interval,
        ts = config.time_column,
        agg_exprs = agg_exprs.join(",\n            "),
        table = pattern_name,
        select_exprs = final_select_exprs.join(",\n          ")
    ))
}

/// Dynamic directory for temporal reduce operations
pub struct TemporalReduceDirectory {
    config: TemporalReduceConfig,
    context: crate::FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceDirectory {
    pub fn new(config: TemporalReduceConfig, context: crate::FactoryContext) -> TinyFSResult<Self> {
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
        let pattern = self.config.in_pattern.path();
        log::debug!(
            "TemporalReduceDirectory::discover_source_files - scanning pattern {}",
            pattern
        );

        let mut source_files = Vec::new();

        let fs = self.context.context.filesystem();

        // Use collect_matches to find source files with the given pattern
        match fs.root().await?.collect_matches(pattern).await {
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
        let fs = self.context.context.filesystem();

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
        let file_id =
            tinyfs::FileID::from_content(parent_part_id, EntryType::DirectoryDynamic, &id_bytes);

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

    async fn remove(&mut self, _name: &str) -> TinyFSResult<Option<Node>> {
        Err(tinyfs::Error::Other(
            "temporal-reduce directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>>
    {
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
                tinyfs::Error::Other(format!(
                    "Failed to create temporal-reduce site '{}'",
                    site_name
                ))
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
            blake3: None,
            bao_outboard: None,
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
    context: crate::FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceSiteDirectory {
    #[must_use]
    pub fn new(
        site_name: String,
        source_path: String,
        source_node: Node,
        config: TemporalReduceConfig,
        context: crate::FactoryContext,
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
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>>
    {
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
            let file_id =
                tinyfs::FileID::from_content(parent_part_id, EntryType::TableDynamic, &id_bytes);

            let dir_entry = tinyfs::DirectoryEntry::new(
                filename.clone(),
                file_id.node_id(),
                EntryType::TableDynamic,
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
                let file_id = tinyfs::FileID::from_content(
                    parent_part_id,
                    EntryType::TableDynamic,
                    &id_bytes,
                );

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

    async fn remove(&mut self, _name: &str) -> TinyFSResult<Option<Node>> {
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
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// Create a temporal reduce directory from configuration and context
fn create_temporal_reduce_directory(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<DirHandle> {
    let temporal_config: TemporalReduceConfig =
        serde_json::from_value(config.clone()).map_err(|e| {
            tinyfs::Error::Other(format!(
                "Invalid temporal-reduce config: {}: {:?}",
                e, config
            ))
        })?;

    let directory = TemporalReduceDirectory::new(temporal_config, context)?;
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
    use crate::FactoryRegistry;
    use arrow::array::{Float64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tinyfs::{EntryType, FS, FileID, MemoryPersistence, NodeType, ProviderContext};

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

        let (fs, provider_context) = create_test_environment().await;

        // Create source series files with 3 days of hourly data
        {
            let root = fs.root().await.unwrap();
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

                // 3 days x 24 hours = 72 data points per site
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

                // Write the series file using create_parquet_file helper
                let mut buf = Vec::new();
                {
                    let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                        &mut buf,
                        schema.clone(),
                        None,
                    )
                    .unwrap();
                    writer.write(&batch).unwrap();
                    let _ = writer.close().unwrap();
                }

                let _ = create_parquet_file(&fs, &filename, buf, EntryType::TablePhysicalSeries)
                    .await
                    .unwrap();
            }
        }

        // Create temporal-reduce directory and verify aggregations
        {
            // Create temporal-reduce configuration
            let config = TemporalReduceConfig {
                in_pattern: crate::Url::parse("series:///sources/*.series").unwrap(),
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
                transforms: None,
            };

            let context = test_context(&provider_context, FileID::root());
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
                    let queryable_file = file_guard
                        .as_queryable()
                        .expect("Temporal-reduce should create QueryableFile");

                    let table_provider = queryable_file
                        .as_table_provider(file_id, &provider_context)
                        .await
                        .unwrap();

                    // Execute query to get results
                    let ctx = &provider_context.datafusion_session;
                    let table_name = format!("temporal_{}", site_num);
                    _ = ctx.register_table(&table_name, table_provider).unwrap();

                    let query = format!("SELECT * FROM {} ORDER BY timestamp", table_name);
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
                        "[OK] Site {} verified: {} rows, avg={:.2}, min={:.2}, max={:.2}",
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
        }

        log::info!("[OK] Temporal-reduce integration test completed successfully");
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

    /// Test that source_url() preserves the URL scheme from in_pattern.
    /// This is critical: format providers (oteljson://, csv://) must not be
    /// silently rewritten to series:// or file://.
    #[tokio::test]
    async fn test_source_url_preserves_scheme() {
        let (fs, provider_context) = create_test_environment().await;

        // Create a dummy source node (we only need it for TemporalReduceSqlFile construction)
        let root = fs.root().await.unwrap();
        _ = root.create_dir_path("/ingest").await.unwrap();
        use tokio::io::AsyncWriteExt;
        let mut w = root
            .async_writer_path_with_type("/ingest/test.json", EntryType::FilePhysicalVersion)
            .await
            .unwrap();
        w.write_all(b"test").await.unwrap();
        w.flush().await.unwrap();
        w.shutdown().await.unwrap();
        let node_path = root.get_node_path("/ingest/test.json").await.unwrap();

        let make_file = |scheme: &str, path: &str| {
            let in_pattern = crate::Url::parse(&format!("{}://{}", scheme, path)).unwrap();
            let config = TemporalReduceConfig {
                in_pattern,
                out_pattern: "out".to_string(),
                time_column: "timestamp".to_string(),
                resolutions: vec!["1h".to_string()],
                aggregations: vec![],
                transforms: None,
            };
            TemporalReduceSqlFile::new(
                config,
                Duration::from_secs(3600),
                node_path.node.clone(),
                path.to_string(),
                test_context(&provider_context, FileID::root()),
            )
        };

        // Format providers must preserve their scheme
        let oteljson_file = make_file("oteljson", "/ingest/test.json");
        assert_eq!(
            oteljson_file.source_url(),
            "oteljson:///ingest/test.json",
            "oteljson:// scheme must be preserved"
        );

        let csv_file = make_file("csv", "/data/metrics.csv");
        assert_eq!(
            csv_file.source_url(),
            "csv:///data/metrics.csv",
            "csv:// scheme must be preserved"
        );

        let excelhtml_file = make_file("excelhtml", "/data/report.html");
        assert_eq!(
            excelhtml_file.source_url(),
            "excelhtml:///data/report.html",
            "excelhtml:// scheme must be preserved"
        );

        // Builtin schemes must also be preserved
        let series_file = make_file("series", "/combined/site1.series");
        assert_eq!(
            series_file.source_url(),
            "series:///combined/site1.series",
            "series:// scheme must be preserved"
        );

        let table_file = make_file("table", "/tables/lookup");
        assert_eq!(
            table_file.source_url(),
            "table:///tables/lookup",
            "table:// scheme must be preserved"
        );

        let file_file = make_file("file", "/raw/data.parquet");
        assert_eq!(
            file_file.source_url(),
            "file:///raw/data.parquet",
            "file:// scheme must be preserved"
        );
    }

    /// Integration test: temporal-reduce with CSV format provider (non-Parquet source).
    ///
    /// This validates the fix for the bug where temporal-reduce hardcoded series://
    /// in ensure_inner() and used direct as_queryable() in discover_source_columns(),
    /// causing "Corrupt footer" errors when the source was a non-Parquet format like
    /// CSV, OTelJSON, or ExcelHTML.
    #[tokio::test]
    async fn test_temporal_reduce_with_csv_format_provider() {
        let _ = env_logger::try_init();

        let (fs, provider_context) = create_test_environment().await;

        // Create a raw CSV file (entry type = data, NOT table:series)
        // This is exactly the scenario that broke: raw data files with a format provider URL
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();

            // Generate CSV data: timestamp, temperature, humidity
            // 48 rows: 2 days of hourly readings
            // Use RFC3339 timestamps so CSV parser infers Timestamp type (not Int64)
            // Use fractional values so CSV parser infers Float64 (not Int64)
            let mut csv_data = String::from("timestamp,temperature,humidity\n");
            for day in 0..2 {
                for hour in 0..24 {
                    let total_secs = day * 86400 + hour * 3600;
                    let h = total_secs / 3600;
                    let m = (total_secs % 3600) / 60;
                    let s = total_secs % 60;
                    // Format as ISO 8601: 1970-01-01T00:00:00 + offset
                    let d = day + 1; // 1-indexed day
                    let ts = format!("1970-01-{:02}T{:02}:{:02}:{:02}", d, h % 24, m, s);
                    let temp = 20.5 + hour as f64;
                    let humidity = 50.5 + hour as f64 * 0.5;
                    csv_data.push_str(&format!("{},{},{}\n", ts, temp, humidity));
                }
            }

            // Write as raw data file (FilePhysicalVersion, NOT TablePhysicalSeries)
            use tokio::io::AsyncWriteExt;
            let mut w = root
                .async_writer_path_with_type("/ingest/weather.csv", EntryType::FilePhysicalVersion)
                .await
                .unwrap();
            w.write_all(csv_data.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        // Create temporal-reduce with csv:// in_pattern
        {
            let config = TemporalReduceConfig {
                in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
                out_pattern: "weather".to_string(),
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()],
                aggregations: vec![
                    AggregationConfig {
                        agg_type: AggregationType::Avg,
                        columns: Some(vec!["temperature".to_string(), "humidity".to_string()]),
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
                transforms: None,
            };

            let context = test_context(&provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            // Verify directory structure: should have "weather" subdirectory
            use futures::StreamExt;
            let mut entries_stream = temporal_handle.entries().await.unwrap();
            let mut site_names = Vec::new();
            while let Some(entry_result) = entries_stream.next().await {
                let entry = entry_result.unwrap();
                site_names.push(entry.name.clone());
            }
            assert_eq!(
                site_names,
                vec!["weather"],
                "Should create 'weather' site directory from out_pattern"
            );

            // Navigate to the site directory
            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let weather_dir = match &weather_node.node_type {
                NodeType::Directory(dir) => dir,
                _ => panic!("Expected directory for 'weather'"),
            };

            // Get the daily aggregation file
            let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
            let file_id = daily_node.id;

            // Query the aggregated data -- this is where the bug manifested as "Corrupt footer"
            if let NodeType::File(file_handle) = &daily_node.node_type {
                let file_arc = file_handle.get_file().await;
                let file_guard = file_arc.lock().await;

                let queryable = file_guard.as_queryable().expect("Should be queryable");

                let table_provider = queryable
                    .as_table_provider(file_id, &provider_context)
                    .await
                    .expect("Table provider creation should succeed with csv:// source");

                // Query: verify results
                let ctx = &provider_context.datafusion_session;
                _ = ctx.register_table("csv_reduced", table_provider).unwrap();
                let df = ctx
                    .sql("SELECT * FROM csv_reduced ORDER BY timestamp")
                    .await
                    .unwrap();
                let batches = df.collect().await.unwrap();

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 2, "Should have 2 aggregated rows (2 days)");

                // Debug: print schema and row count
                log::info!(
                    "CSV reduced: {} rows, schema: {:?}",
                    total_rows,
                    batches[0].schema()
                );

                // Verify schema
                let schema = &batches[0].schema();
                assert!(schema.column_with_name("timestamp").is_some());
                assert!(schema.column_with_name("temperature.avg").is_some());
                assert!(schema.column_with_name("humidity.avg").is_some());
                assert!(schema.column_with_name("temperature.min").is_some());
                assert!(schema.column_with_name("temperature.max").is_some());

                // Verify day 0 values: avg temp = 20.5 + avg(0..23) = 20.5 + 11.5 = 32.0
                let temp_avg = batches[0]
                    .column_by_name("temperature.avg")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert!(
                    (temp_avg.value(0) - 32.0).abs() < 0.01,
                    "Day 0 avg temperature: expected 32.0, got {}",
                    temp_avg.value(0)
                );

                let temp_min = batches[0]
                    .column_by_name("temperature.min")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(temp_min.value(0), 20.5, "Day 0 min temperature");

                let temp_max = batches[0]
                    .column_by_name("temperature.max")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(temp_max.value(0), 43.5, "Day 0 max temperature");

                log::info!("[OK] CSV format provider temporal-reduce test passed");
                log::info!("   - Raw CSV data read via csv:// scheme (not Parquet)");
                log::info!("   - 48 hourly rows aggregated to 2 daily rows");
                log::info!("   - avg/min/max values verified correct");
            } else {
                panic!("Expected file node for weather/res=1d.series");
            }
        }
    }
}
