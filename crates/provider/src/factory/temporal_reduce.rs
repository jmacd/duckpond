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
//!
//! ## Multi-file glob patterns
//!
//! When `in_pattern` contains a glob (e.g., `oteljson:///ingest/casparwater*.json`)
//! that matches multiple files all mapping to the same `out_pattern`, the factory
//! delegates to `SqlDerivedFile` which expands the glob and creates a UNION ALL
//! across all matching files.  This allows temporal-reduce to aggregate across
//! many rotated log files or ingested data fragments in a single pass.
//!
//! **Caveat -- schema inference shortcut:** The column schema is discovered from
//! the lexicographically last matching file (the newest for timestamped names).
//! This works well when columns are only added over time (the newest file has
//! the most complete schema).  If files have **incompatible schemas** (e.g.,
//! columns renamed or removed), schema discovery may be incorrect.
//! Use `timeseries-join` for heterogeneous-schema merging, which performs proper
//! per-file schema discovery and alignment.

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile};
use crate::register_dynamic_factory;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tinyfs::ResultExt;
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
    source_path: String, // Representative source file for schema discovery (no wildcards)
    /// URL to use as the SqlDerived pattern source.  When the in_pattern glob
    /// matched multiple files that map to the same output, this is the original
    /// glob URL so that SqlDerivedFile can expand/UNION all matching files.
    /// For single-match cases this equals source_url().
    pattern_url: String,
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
        pattern_url: String,
        context: crate::FactoryContext,
    ) -> Self {
        Self {
            config,
            duration,
            source_path,
            pattern_url,
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
        // Use full_scheme() to preserve compression and entry type suffixes
        // (e.g., "csv+gzip", "file+series", "file+table").
        let scheme = self.config.in_pattern.full_scheme();
        if self.source_path.starts_with('/') {
            format!("{}://{}", scheme, self.source_path)
        } else {
            format!("{}:///{}", scheme, self.source_path)
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
        let mut provider =
            crate::Provider::with_context(Arc::new(fs), Arc::new(self.context.context.clone()));
        if let Ok(root) = self.context.root().await {
            provider = provider.with_root(root);
        }
        let datafusion_ctx = datafusion::prelude::SessionContext::new();

        let table_provider = provider
            .create_table_provider(&source_url, &datafusion_ctx)
            .await
            .map_other_context(format!(
                "Failed to create table provider for source '{}'",
                source_url
            ))?;

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
        let modified_config = self.filled_config().await?;

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

    /// Resolve the config's aggregation column patterns against the discovered
    /// source schema, returning a config whose every aggregation has concrete
    /// column lists. This is the shared input to both the single-pass SQL and
    /// the rollup partial/merge SQL.
    async fn filled_config(&self) -> TinyFSResult<TemporalReduceConfig> {
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

        Ok(modified_config)
    }

    /// Attempt to serve this resolution from the incremental rollup cache,
    /// returning `Ok(None)` to fall back to the single-pass delegate when the
    /// rollup preconditions are not met.
    ///
    /// The rollup computes decomposable partials once per immutable source
    /// version, caches them on disk, and merges them by time bucket. A new
    /// ingest version therefore costs O(new rows) instead of O(history).
    ///
    /// Preconditions for the rollup path:
    /// - a pond cache directory is configured;
    /// - the input has no transforms, so partials can be computed directly from
    ///   the parsed source rows;
    /// - the input scheme is a format provider whose parsed leaves are cached
    ///   per version by the format cache, which the partials read.
    async fn try_rollup_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        let Some(cache_dir) = context.cache_dir().map(std::path::Path::to_path_buf) else {
            return Ok(None);
        };

        if self
            .config
            .transforms
            .as_ref()
            .is_some_and(|t| !t.is_empty())
        {
            return Ok(None);
        }

        let scheme = self.config.in_pattern.scheme();
        let Some(format_provider) = crate::FormatRegistry::get_provider(scheme) else {
            return Ok(None);
        };

        let source_files = self.resolve_source_files().await?;
        if source_files.is_empty() {
            return Ok(None);
        }

        // Partials are stored once at the finest resolution and re-binned to
        // this file's resolution at merge time, so coarser resolutions never
        // rescan raw history. The finest-partial namespace is therefore shared
        // across all resolutions of the same site.
        let resolution_durations = parse_nesting_resolutions(&self.config.resolutions)?;
        let Some(finest_interval) = resolution_durations.first().copied() else {
            return Ok(None);
        };

        let filled = self.filled_config().await?;
        let pieces = AggSqlPieces::build(&filled)?;
        let cfg_hash = crate::rollup_cache::cfg_hash(&rollup_cfg_canonical(
            &filled,
            finest_interval,
            &self.pattern_url,
        ));

        // Key the partials directory by the parent site partition, which is
        // shared by every resolution file of this site, so all resolutions read
        // and write the same finest-resolution partials.
        let site_node_id = id.part_id().to_node_id();
        let glob_dir = crate::rollup_cache::glob_dir(&cache_dir, &cfg_hash, &site_node_id);

        let fs = self.context.context.filesystem();
        let mut provider_api =
            crate::Provider::with_context(Arc::new(fs), Arc::new(self.context.context.clone()));
        if let Ok(root) = self.context.root().await {
            provider_api = provider_api.with_root(root);
        }

        for node_path in &source_files {
            let file_url_str = node_file_url(scheme, node_path);
            let file_url = crate::Url::parse(&file_url_str).map_other()?;

            let (source_node_id, versions) = provider_api
                .ensure_url_cached(&file_url, format_provider.as_ref(), &cache_dir)
                .await
                .map_other()?;

            let mut uncached =
                crate::rollup_cache::find_uncached_members(&glob_dir, &source_node_id, &versions);
            // Process versions oldest-first so the sealed-bucket frontier only
            // ever advances. A new version whose earliest bucket precedes the
            // frontier is a sequentiality violation: its sealed buckets would
            // need partials that are no longer recomputed, silently
            // double-counting on merge. Per the no-fallback policy this is a
            // hard error recovered with --rebuild, never a silent merge.
            uncached.sort_by_key(|v| v.version);
            let mut frontier = crate::rollup_cache::read_frontier(&glob_dir, &source_node_id);
            for version in &uncached {
                let parquet_path = crate::format_cache::cache_version_path(
                    &cache_dir,
                    scheme,
                    &source_node_id,
                    version,
                );
                let span = self
                    .version_bucket_span(finest_interval, &parquet_path)
                    .await?;
                if let Some((lo, _hi)) = span
                    && let Some(f) = frontier
                    && lo < f
                {
                    return Err(tinyfs::Error::Other(format!(
                        "temporal-reduce rollup: source node {} version {} backfills \
                         already-sealed buckets (earliest bucket {} precedes frontier \
                         {}); a non-sequential input breaks incremental partial \
                         summation. Re-run the export with --rebuild to recompute the \
                         rollup cache from scratch.",
                        source_node_id, version.version, lo, f
                    )));
                }
                self.write_version_partial(
                    &pieces,
                    finest_interval,
                    &glob_dir,
                    &source_node_id,
                    version,
                    &parquet_path,
                )
                .await?;
                // Advance and persist the frontier only after the partial is
                // durably written, so a later violation in this loop cannot
                // strand an unrecorded sealed bucket: on retry the persisted
                // frontier still reflects every version already cached.
                if let Some((_lo, hi)) = span {
                    let advanced = frontier.map_or(hi, |f| f.max(hi));
                    if frontier != Some(advanced) {
                        frontier = Some(advanced);
                        crate::rollup_cache::write_frontier(&glob_dir, &source_node_id, advanced)
                            .map_other()?;
                    }
                }
            }
        }

        let ctx = &context.datafusion_session;
        let sanitized_id: String = site_node_id
            .to_string()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let partials_table = format!("__rollup_partials_{}_{}", cfg_hash, sanitized_id);
        if !ctx.table_exist(partials_table.as_str()).unwrap_or(false) {
            let provider = crate::rollup_cache::listing_table_from_dir(&glob_dir, ctx)
                .await
                .map_other()?;
            _ = ctx
                .register_table(partials_table.as_str(), provider)
                .map_other()?;
        }

        let merge_sql = pieces.merge_sql(self.duration, &filled.time_column, &partials_table);
        let logical_plan = ctx
            .sql(&merge_sql)
            .await
            .map_other_context("rollup merge SQL planning failed")?
            .logical_plan()
            .clone();

        use datafusion::catalog::view::ViewTable;
        let view_table = ViewTable::new(logical_plan, Some(merge_sql));
        let table_provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(view_table);

        let cache_key = crate::TableProviderKey::new(id, crate::VersionSelection::LatestVersion)
            .to_cache_string();
        context.set_table_provider_cache(cache_key, table_provider.clone())?;

        Ok(Some(table_provider))
    }

    /// Compute the finest-interval `time_bucket` span [min, max] of one cached
    /// source version, in the timestamp unit cast to `i64`. Returns `None` when
    /// the version contributes no non-null timestamps, in which case it neither
    /// advances nor is checked against the sequentiality frontier. The unit is
    /// consistent across all versions of one source node, so comparing these
    /// spans against a per-node frontier is well defined.
    async fn version_bucket_span(
        &self,
        finest_interval: Duration,
        parquet_path: &std::path::Path,
    ) -> TinyFSResult<Option<(i64, i64)>> {
        let ctx = datafusion::prelude::SessionContext::new();
        let table = "__src_version";
        ctx.register_parquet(
            table,
            parquet_path.to_string_lossy().as_ref(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .map_other_context(format!(
            "register cached parquet '{}'",
            parquet_path.display()
        ))?;

        let interval = duration_to_sql_interval(finest_interval);
        let bin = date_bin_expr(&interval, &self.config.time_column);
        let ts = &self.config.time_column;
        let span_sql = format!(
            "SELECT CAST(MIN({bin}) AS BIGINT) AS lo, CAST(MAX({bin}) AS BIGINT) AS hi \
             FROM {table} WHERE {ts} IS NOT NULL"
        );
        let batches = ctx
            .sql(&span_sql)
            .await
            .map_other_context("rollup span SQL planning failed")?
            .collect()
            .await
            .map_other_context("rollup span SQL execution failed")?;

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let lo = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>();
            let hi = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>();
            if let (Some(lo), Some(hi)) = (lo, hi) {
                use arrow::array::Array;
                if lo.is_null(0) || hi.is_null(0) {
                    return Ok(None);
                }
                return Ok(Some((lo.value(0), hi.value(0))));
            }
        }
        Ok(None)
    }

    /// Compute one immutable source version's partials at the finest resolution
    /// and write them to the site's rollup glob directory. The partials carry
    /// no reconstruction so they stay mergeable across versions and re-bin into
    /// any coarser nesting resolution.
    async fn write_version_partial(
        &self,
        pieces: &AggSqlPieces,
        finest_interval: Duration,
        glob_dir: &std::path::Path,
        source_node_id: &tinyfs::NodeID,
        version: &tinyfs::FileVersionInfo,
        parquet_path: &std::path::Path,
    ) -> TinyFSResult<()> {
        let ctx = datafusion::prelude::SessionContext::new();
        let table = "__src_version";
        ctx.register_parquet(
            table,
            parquet_path.to_string_lossy().as_ref(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .map_other_context(format!(
            "register cached parquet '{}'",
            parquet_path.display()
        ))?;

        let available: std::collections::HashSet<String> = ctx
            .table(table)
            .await
            .map_other_context("read cached version schema")?
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let partial_sql =
            pieces.partial_sql(finest_interval, &self.config.time_column, table, &available);
        let stream = ctx
            .sql(&partial_sql)
            .await
            .map_other_context("rollup partial SQL planning failed")?
            .execute_stream()
            .await
            .map_other_context("rollup partial SQL execution failed")?;

        let schema = stream.schema();
        let mapped = stream.map(|r| r.map_err(|e| crate::error::Error::Arrow(e.to_string())));
        _ = crate::rollup_cache::write_glob_member(
            glob_dir,
            source_node_id,
            version,
            schema,
            Box::pin(mapped),
        )
        .await
        .map_other()?;
        Ok(())
    }

    /// Resolve this partition's `pattern_url` to concrete source file
    /// NodePaths, deduplicated by node id. Directories and non-file nodes are
    /// skipped.
    async fn resolve_source_files(&self) -> TinyFSResult<Vec<tinyfs::NodePath>> {
        // Resolve the partition-scoped pattern_url, not the original in_pattern
        // glob. pattern_url is the concrete file for a 1:1 source->output
        // mapping and the original glob only when several files share one
        // output, exactly matching the non-rollup SqlDerived source in
        // ensure_inner. Using in_pattern here would pull in sibling files from
        // other output partitions whose schemas differ, so the per-version
        // partial SQL would reference columns absent from those siblings.
        let url = crate::Url::parse(&self.pattern_url)
            .map_other_context(format!("Invalid pattern URL '{}'", self.pattern_url))?;
        let tinyfs_path = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .map_other_context(format!("Invalid UTF-8 in URL path '{}'", url.path()))?
            .to_string();

        let tinyfs_root = self
            .context
            .root()
            .await
            .map_other_context("Failed to get root")?;

        let is_exact =
            !tinyfs_path.contains('*') && !tinyfs_path.contains('?') && !tinyfs_path.contains('[');

        let matches: Vec<tinyfs::NodePath> = if is_exact {
            match tinyfs_root.resolve_path(&tinyfs_path).await {
                Ok((_wd, tinyfs::Lookup::Found(np))) => vec![np],
                _ => Vec::new(),
            }
        } else {
            tinyfs_root
                .collect_matches(&tinyfs_path)
                .await
                .map_other_context(format!("Failed to resolve pattern '{}'", tinyfs_path))?
                .into_iter()
                .map(|(np, _captures)| np)
                .collect()
        };

        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut files = Vec::new();
        for np in matches {
            let file_id = np.id();
            if !file_id.entry_type().is_file() {
                continue;
            }
            if seen.insert(file_id.node_id()) {
                files.push(np);
            }
        }
        Ok(files)
    }

    async fn ensure_inner(&self) -> TinyFSResult<()> {
        crate::factory::lazy_sql_file::ensure_inner_series(&self.inner, &self.context, || async {
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
            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE: Generated SQL query: {}",
                sql_query
            );

            // Use the pattern_url for the SqlDerived source.  When the in_pattern
            // glob matched multiple files mapping to the same output, pattern_url
            // retains the glob so SqlDerivedFile can expand and UNION ALL the
            // matching files.  For single-match cases it is the concrete file URL.
            let source_url = crate::Url::parse(&self.pattern_url)
                .map_other_context(format!("Invalid pattern URL '{}'", self.pattern_url))?;

            let sql_config = SqlDerivedConfig::new(
                {
                    let mut patterns = HashMap::new();
                    _ = patterns.insert(pattern_name.clone(), source_url);
                    patterns
                },
                Some(sql_query.clone()),
            )
            .with_transforms(self.config.transforms.clone());

            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE SqlDerivedConfig for '{}': query=\n{}",
                self.source_path,
                sql_query
            );

            Ok(sql_config)
        })
        .await
    }

    #[must_use]
    pub fn create_handle(self) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

crate::factory::lazy_sql_file::impl_lazy_sql_derived_file_metadata!(TemporalReduceSqlFile);

#[async_trait::async_trait]
impl tinyfs::QueryableFile for TemporalReduceSqlFile {
    async fn as_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let cache_key = crate::TableProviderKey::new(id, crate::VersionSelection::LatestVersion)
            .to_cache_string();
        if let Some(cached) = context.get_table_provider_cache(&cache_key) {
            return Ok(cached);
        }

        // Prefer the incremental rollup cache. A rollup error is propagated
        // rather than masked by the single-pass delegate, so any rollup bug
        // surfaces instead of silently degrading to O(history) reads.
        if let Some(provider) = self.try_rollup_table_provider(id, context).await? {
            return Ok(provider);
        }

        log::debug!(
            "DELEGATING TemporalReduceSqlFile to inner SqlDerivedFile: id={}",
            id
        );
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard
            .as_ref()
            .expect("inner initialized by ensure_inner");
        inner.as_table_provider(id, context).await
    }
}

/// Build the full source URL string for a matched source file under a format
/// provider scheme. Mirrors `SqlDerivedFile::node_file_url`.
fn node_file_url(scheme: &str, node_path: &tinyfs::NodePath) -> String {
    format!("{}://{}", scheme, node_path.path().display())
}

/// Canonical string over the parts of a temporal-reduce config that change the
/// meaning of the cached partials: the source pattern, the finest resolution at
/// which partials are stored, the time column, and the schema-filled
/// aggregation set. Feeds [`crate::rollup_cache::cfg_hash`].
///
/// The output resolution is deliberately excluded: partials are stored once at
/// the finest resolution and shared across every coarser resolution of the same
/// site, which re-bin them at merge time.
fn rollup_cfg_canonical(
    filled: &TemporalReduceConfig,
    finest_interval: Duration,
    pattern_url: &str,
) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = write!(
        s,
        "src={};finest={}s;ts={};",
        pattern_url,
        finest_interval.as_secs(),
        filled.time_column
    );
    for agg in &filled.aggregations {
        let _ = write!(s, "agg={}:", agg.agg_type.to_sql());
        if let Some(cols) = &agg.columns {
            for col in cols {
                let _ = write!(s, "{},", col);
            }
        }
        s.push(';');
    }
    s
}

/// Parse the resolution list and validate that it nests: sorted ascending, each
/// coarser interval is an exact integer multiple of the next-finer one. Returns
/// the durations sorted finest-first.
///
/// Nesting guarantees a finer bucket never splits across a coarser boundary, so
/// every coarser resolution can be derived by re-binning the finest partials
/// rather than rescanning raw history.
fn parse_nesting_resolutions(resolutions: &[String]) -> TinyFSResult<Vec<Duration>> {
    let mut durations = Vec::with_capacity(resolutions.len());
    for res_str in resolutions {
        let d = humantime::parse_duration(res_str)
            .map_other_context(format!("Invalid resolution '{}'", res_str))?;
        if d.as_nanos() == 0 {
            return Err(tinyfs::Error::Other(format!(
                "Invalid resolution '{}': must be non-zero",
                res_str
            )));
        }
        durations.push(d);
    }
    durations.sort();
    for win in durations.windows(2) {
        let finer = win[0].as_nanos();
        let coarser = win[1].as_nanos();
        if coarser % finer != 0 {
            return Err(tinyfs::Error::Other(format!(
                "Non-nesting resolutions: {:?} is not an integer multiple of {:?}; \
                 each coarser resolution must be an integer multiple of the next finer one \
                 so cascaded rollups stay exact",
                win[1], win[0]
            )));
        }
    }
    Ok(durations)
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

    column.starts_with(prefix_str)
        && column.ends_with(suffix_str)
        && column.len() >= prefix_str.len() + suffix_str.len()
}

/// Decomposable partial-aggregate kinds stored in the inner CTE.
///
/// Every supported output aggregation is reconstructed from one or more of
/// these associative partials so that the same machinery can later merge
/// partials across input versions and across cascaded resolutions. `Avg` is
/// never stored directly; it is lowered to `Sum` and `Count` and reconstructed
/// as `Sum / Count` in the final SELECT.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PartialKind {
    Sum,
    Count,
    Min,
    Max,
    CountStar,
}

impl PartialKind {
    fn tag(self) -> &'static str {
        match self {
            PartialKind::Sum => "sum",
            PartialKind::Count => "count",
            PartialKind::Min => "min",
            PartialKind::Max => "max",
            PartialKind::CountStar => "cstar",
        }
    }

    /// SQL expression computing this partial over raw source rows, grouped into
    /// a time bucket. Aliased to the partial's internal column name.
    fn raw_expr(self, column: &str, alias: &str) -> String {
        match self {
            PartialKind::Sum => format!("SUM(\"{column}\") AS \"{alias}\""),
            PartialKind::Count => format!("COUNT(\"{column}\") AS \"{alias}\""),
            PartialKind::Min => format!("MIN(\"{column}\") AS \"{alias}\""),
            PartialKind::Max => format!("MAX(\"{column}\") AS \"{alias}\""),
            PartialKind::CountStar => format!("COUNT(*) AS \"{alias}\""),
        }
    }

    /// Like [`raw_expr`], but emits a typed placeholder when the source column
    /// is absent from this input version. An oteljson source builds a per-file
    /// wide schema from the metric names present in that file, so a metric that
    /// appears or is renamed over history is missing from older versions. The
    /// single-pass query never sees this because it reads the unioned glob where
    /// the absent column reads as NULL, but a per-version partial query plans
    /// against one version's schema and would fail on `SUM("missing")`. The
    /// placeholder keeps the partial output schema identical across versions,
    /// which the cross-version merge listing table requires, and contributes
    /// nothing on merge: NULL sums/mins/maxes are ignored and a zero count adds
    /// nothing.
    fn raw_expr_or_absent(self, column: &str, alias: &str, present: bool) -> String {
        if present || matches!(self, PartialKind::CountStar) {
            return self.raw_expr(column, alias);
        }
        match self {
            PartialKind::Sum | PartialKind::Min | PartialKind::Max => {
                format!("CAST(NULL AS DOUBLE) AS \"{alias}\"")
            }
            PartialKind::Count => format!("CAST(0 AS BIGINT) AS \"{alias}\""),
            PartialKind::CountStar => format!("COUNT(*) AS \"{alias}\""),
        }
    }

    /// SQL expression merging an already-computed partial across partitions
    /// (input versions, cascaded levels), grouped by `time_bucket`. The merge
    /// is associative: sums and counts add, mins/maxes extend. The result keeps
    /// the same alias so the reconstruction SELECT is identical whether it runs
    /// over raw-grouped or merged partials.
    fn merge_expr(self, alias: &str) -> String {
        match self {
            // SUM is nullable in DataFusion; COUNT is not. Single-pass COUNT
            // columns are non-nullable, so restore that with COALESCE here to
            // keep the merged output schema identical (counts can never be
            // NULL: a bucket only exists because it had rows).
            PartialKind::Count | PartialKind::CountStar => {
                format!("COALESCE(SUM(\"{alias}\"), 0) AS \"{alias}\"")
            }
            PartialKind::Sum => format!("SUM(\"{alias}\") AS \"{alias}\""),
            PartialKind::Min => format!("MIN(\"{alias}\") AS \"{alias}\""),
            PartialKind::Max => format!("MAX(\"{alias}\") AS \"{alias}\""),
        }
    }
}

/// A single decomposable partial column in the bucketed partials table.
struct PartialDef {
    kind: PartialKind,
    /// Source column name, or `"*"` for `COUNT(*)`.
    column: String,
    /// Internal partial column alias, e.g. `__p_sum_0`.
    alias: String,
}

/// The decomposed form of a temporal aggregation: the set of decomposable
/// partial columns plus the output-column reconstructions that reference them.
///
/// The same pieces drive three SQL forms:
/// - the single-pass query ([`AggSqlPieces::full_sql`]) used when no rollup
///   cache is available (identical to a direct `AVG/MIN/MAX/...` GROUP BY);
/// - the per-version partial query ([`AggSqlPieces::partial_sql`]) whose output
///   is cached once per immutable input version;
/// - the cross-version merge query ([`AggSqlPieces::merge_sql`]) that combines
///   cached partials back into the final output.
struct AggSqlPieces {
    partials: Vec<PartialDef>,
    /// Output column expressions, in the original aggregation/column order,
    /// each referencing one or more partial aliases.
    reconstruct_exprs: Vec<String>,
}

impl AggSqlPieces {
    /// Build the decomposed pieces from the (schema-filled) config.
    fn build(config: &TemporalReduceConfig) -> TinyFSResult<Self> {
        let mut aliases: HashMap<(PartialKind, String), String> = HashMap::new();
        let mut partials: Vec<PartialDef> = Vec::new();
        let mut reconstruct_exprs: Vec<String> = Vec::new();

        // Register a partial for (kind, column), deduplicating so e.g. Avg and
        // Sum on the same column share a single SUM partial. Returns its alias.
        fn register(
            kind: PartialKind,
            column: &str,
            aliases: &mut HashMap<(PartialKind, String), String>,
            partials: &mut Vec<PartialDef>,
        ) -> String {
            let key = (kind, column.to_string());
            if let Some(existing) = aliases.get(&key) {
                return existing.clone();
            }
            let alias = format!("__p_{}_{}", kind.tag(), partials.len());
            partials.push(PartialDef {
                kind,
                column: column.to_string(),
                alias: alias.clone(),
            });
            _ = aliases.insert(key, alias.clone());
            alias
        }

        for agg in &config.aggregations {
            let columns = agg.columns.as_ref().ok_or_else(|| {
                // This should never happen since
                // TemporalReduceSqlFile.generate_sql_with_discovered_schema()
                // fills in None columns before building the pieces.
                tinyfs::Error::Other(
                    "Internal error: AggSqlPieces::build called with None columns".to_string(),
                )
            })?;

            for column in columns {
                if column == "*" && matches!(agg.agg_type, AggregationType::Count) {
                    // Special case: count(*) becomes "timestamp.count".
                    let out_alias = "timestamp.count";
                    let p = register(PartialKind::CountStar, "*", &mut aliases, &mut partials);
                    reconstruct_exprs.push(format!("\"{p}\" AS \"{out_alias}\""));
                    continue;
                }

                // Generate alias in format: scope.parameter.unit.agg
                let out_alias = format!("{}.{}", column, agg.agg_type.to_sql().to_lowercase());
                let expr = match agg.agg_type {
                    AggregationType::Avg => {
                        // Avg is not associative; store Sum and Count partials
                        // and reconstruct Avg = Sum / Count. NULLIF guards the
                        // empty bucket so the result is NULL, matching AVG.
                        let sum = register(PartialKind::Sum, column, &mut aliases, &mut partials);
                        let count =
                            register(PartialKind::Count, column, &mut aliases, &mut partials);
                        format!(
                            "CAST(\"{sum}\" AS DOUBLE) / NULLIF(\"{count}\", 0) AS \"{out_alias}\""
                        )
                    }
                    AggregationType::Sum => {
                        let p = register(PartialKind::Sum, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                    AggregationType::Count => {
                        let p = register(PartialKind::Count, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                    AggregationType::Min => {
                        let p = register(PartialKind::Min, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                    AggregationType::Max => {
                        let p = register(PartialKind::Max, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                };
                reconstruct_exprs.push(expr);
            }
        }

        Ok(Self {
            partials,
            reconstruct_exprs,
        })
    }

    /// Partial expressions computed over raw source rows (one row per bucket).
    fn raw_partial_exprs(&self) -> Vec<String> {
        self.partials
            .iter()
            .map(|p| p.kind.raw_expr(&p.column, &p.alias))
            .collect()
    }

    /// Partial expressions for one input version, substituting typed
    /// placeholders for source columns absent from that version's schema.
    fn raw_partial_exprs_for(&self, available: &std::collections::HashSet<String>) -> Vec<String> {
        self.partials
            .iter()
            .map(|p| {
                let present = p.column == "*" || available.contains(&p.column);
                p.kind.raw_expr_or_absent(&p.column, &p.alias, present)
            })
            .collect()
    }

    /// Partial expressions merging cached partials across partitions.
    fn merge_partial_exprs(&self) -> Vec<String> {
        self.partials
            .iter()
            .map(|p| p.kind.merge_expr(&p.alias))
            .collect()
    }
}

/// Time-bucketing `date_bin` expression for an interval, binning from epoch.
///
/// DATE_TRUNC only supports single calendar units and discards the multiplier,
/// so DATE_TRUNC('hour', ts) is the same whether the config says 1h or 4h.
/// date_bin() properly handles multi-unit intervals like INTERVAL '4 HOUR'.
fn date_bin_expr(interval: &str, ts: &str) -> String {
    format!("date_bin({interval}, {ts}, TIMESTAMP '1970-01-01T00:00:00')")
}

/// Generate the single-pass SQL query for temporal aggregation.
///
/// The query groups the source into time buckets, computes the decomposable
/// partials (`Sum`, `Count`, `Min`, `Max`, `COUNT(*)`), and reconstructs the
/// requested output columns, including `Avg = Sum / Count`. Output column
/// names, ordering, and values are identical to a direct `AVG/MIN/MAX/...`
/// GROUP BY. This form is used when no rollup cache is available.
async fn generate_temporal_sql(
    config: &TemporalReduceConfig,
    interval: Duration,
    _source_path: &str,
    _context: &crate::FactoryContext,
    pattern_name: &str,
) -> TinyFSResult<String> {
    let pieces = AggSqlPieces::build(config)?;
    Ok(pieces.full_sql(interval, &config.time_column, pattern_name))
}

impl AggSqlPieces {
    /// Single-pass query: group raw rows into buckets, compute partials, and
    /// reconstruct the output columns in one statement.
    fn full_sql(&self, interval: Duration, ts: &str, table: &str) -> String {
        let interval = duration_to_sql_interval(interval);
        let bin = date_bin_expr(&interval, ts);
        // COALESCE forces DataFusion to infer a non-nullable timestamp schema,
        // even though the fallback is never used.
        format!(
            r#"
        WITH time_buckets AS (
          SELECT 
            {bin} AS time_bucket,
            {partial_exprs}
          FROM {table}
          WHERE {ts} IS NOT NULL
          GROUP BY {bin}
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {ts},
          {select_exprs}
        FROM time_buckets
        ORDER BY time_bucket
        "#,
            partial_exprs = self.raw_partial_exprs().join(",\n            "),
            select_exprs = self.reconstruct_exprs.join(",\n          "),
        )
    }

    /// Per-version partial query: group one immutable input version into
    /// buckets and emit `time_bucket` plus the partial columns. The output is
    /// cached once per version; it carries no reconstruction so the partials
    /// stay mergeable across versions.
    fn partial_sql(
        &self,
        interval: Duration,
        ts: &str,
        version_table: &str,
        available: &std::collections::HashSet<String>,
    ) -> String {
        let interval = duration_to_sql_interval(interval);
        let bin = date_bin_expr(&interval, ts);
        format!(
            r#"
        SELECT 
          {bin} AS time_bucket,
          {partial_exprs}
        FROM {version_table}
        WHERE {ts} IS NOT NULL
        GROUP BY {bin}
        "#,
            partial_exprs = self.raw_partial_exprs_for(available).join(",\n          "),
        )
    }

    /// Cross-version merge query: merge cached per-version partials by
    /// `time_bucket`, re-binning to `output_interval`, then reconstruct the
    /// output columns. The reconstruction is identical to the single-pass form,
    /// so a straddling boundary bucket present in two versions is recombined
    /// exactly.
    ///
    /// Partials are stored at the finest resolution; `output_interval` may be a
    /// coarser nesting multiple, in which case `date_bin` re-buckets the finer
    /// partials into the coarser bucket. When `output_interval` equals the
    /// partial interval the re-bin is an identity because the partial
    /// `time_bucket` values are already aligned to that interval.
    fn merge_sql(&self, output_interval: Duration, ts: &str, partials_table: &str) -> String {
        let interval = duration_to_sql_interval(output_interval);
        let bin = date_bin_expr(&interval, "time_bucket");
        format!(
            r#"
        WITH merged AS (
          SELECT 
            {bin} AS time_bucket,
            {merge_exprs}
          FROM {partials_table}
          GROUP BY {bin}
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {ts},
          {select_exprs}
        FROM merged
        ORDER BY time_bucket
        "#,
            merge_exprs = self.merge_partial_exprs().join(",\n            "),
            select_exprs = self.reconstruct_exprs.join(",\n          "),
        )
    }
}

/// Dynamic directory for temporal reduce operations
pub struct TemporalReduceDirectory {
    config: TemporalReduceConfig,
    context: crate::FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceDirectory {
    pub fn new(config: TemporalReduceConfig, context: crate::FactoryContext) -> TinyFSResult<Self> {
        // Parse all resolutions upfront, validating that they nest so cascaded
        // rollups stay exact.
        _ = parse_nesting_resolutions(&config.resolutions)?;
        let mut parsed_resolutions = Vec::new();

        for res_str in &config.resolutions {
            let duration = humantime::parse_duration(res_str)
                .map_other_context(format!("Invalid resolution '{}'", res_str))?;
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

        // Use context.root() to respect effective_root for cross-pond imports
        let root = self.context.root().await?;

        // Use collect_matches to find source files with the given pattern
        match root.collect_matches(pattern).await {
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
        // Use context.root() to respect effective_root for cross-pond imports
        let root = self.context.root().await?;

        let matches = root.collect_matches(source_path).await?;

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
        pattern_url: String,
    ) -> Node {
        let site_directory = TemporalReduceSiteDirectory::new(
            site_name.clone(),
            source_path.clone(),
            source_node,
            pattern_url,
            self.config.clone(),
            self.context.clone(),
            self.parsed_resolutions.clone(),
        );

        // Create deterministic FileID for this site directory
        // Use site_name + in_pattern (not source_path) so the ID is stable
        // regardless of which representative file was picked.
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(site_name.as_bytes());
        id_bytes.extend_from_slice(self.config.in_pattern.to_string().as_bytes());
        id_bytes.extend_from_slice(b"temporal-reduce-site-directory");
        // Use this temporal reduce directory's NodeID as the PartID for children
        let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
        let file_id = tinyfs::FileID::from_content(
            parent_part_id,
            EntryType::DirectoryDynamic,
            &id_bytes,
            self.context.file_id.pond_id(),
        );

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

        // Group source files by output_name, collecting all paths per output.
        // The last path (lexicographically) is used as the representative for
        // schema discovery.  For rotating log files with timestamps in the
        // filename this picks the newest file, which has the most complete
        // schema (columns are only ever added over time).
        let mut sites: HashMap<String, Vec<String>> = HashMap::new();
        for (source_path, output_name) in source_files {
            sites.entry(output_name).or_default().push(source_path);
        }

        // Look for the requested site directory name
        if let Some(source_paths) = sites.get(name) {
            // Use the lexicographically last path as the schema representative.
            // For timestamped filenames (casparwater-2022-*.json, ...-2025-*.json)
            // this picks the newest file with the most complete schema.
            let representative_path = source_paths.iter().max().unwrap_or(&source_paths[0]);

            // Get the source node for this site (any representative file works)
            let source_node = self.get_source_node_by_path(representative_path).await?;

            // Build the pattern URL.  When multiple source files map to the
            // same output name, use the original in_pattern glob so that
            // SqlDerivedFile expands and UNIONs all matching files.  When
            // there is a 1:1 mapping, use the concrete source URL.
            let pattern_url = if source_paths.len() > 1 {
                self.config.in_pattern.to_string()
            } else {
                let scheme = self.config.in_pattern.full_scheme();
                if representative_path.starts_with('/') {
                    format!("{}://{}", scheme, representative_path)
                } else {
                    format!("{}:///{}", scheme, representative_path)
                }
            };

            // Create the site directory using shared helper
            let node_ref = self.create_site_directory_node(
                name.to_string(),
                representative_path.clone(),
                source_node,
                pattern_url,
            );

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

        // Group source files by output_name, collecting all paths per output
        let mut sites: HashMap<String, Vec<String>> = HashMap::new();
        for (source_path, output_name) in source_files {
            sites.entry(output_name).or_default().push(source_path);
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
    /// URL to use as the SqlDerived pattern source (may contain glob for multi-file cases).
    pattern_url: String,
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
        pattern_url: String,
        config: TemporalReduceConfig,
        context: crate::FactoryContext,
        parsed_resolutions: Vec<(String, Duration)>,
    ) -> Self {
        Self {
            site_name,
            source_path,
            source_node,
            pattern_url,
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
            self.pattern_url.clone(),
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
            // Use pattern_url (not source_path) so the ID is stable regardless
            // of which representative file was picked from a multi-file glob.
            let mut id_bytes = Vec::new();
            id_bytes.extend_from_slice(self.site_name.as_bytes());
            id_bytes.extend_from_slice(self.pattern_url.as_bytes());
            id_bytes.extend_from_slice(res_str.as_bytes());
            id_bytes.extend_from_slice(filename.as_bytes());
            id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
            // Use this temporal reduce site directory's NodeID as the PartID for children
            let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
            let file_id = tinyfs::FileID::from_content(
                parent_part_id,
                EntryType::TableDynamic,
                &id_bytes,
                self.context.file_id.pond_id(),
            );

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
                // Use pattern_url (not source_path) so the ID is stable regardless
                // of which representative file was picked from a multi-file glob.
                let mut id_bytes = Vec::new();
                id_bytes.extend_from_slice(self.site_name.as_bytes());
                id_bytes.extend_from_slice(self.pattern_url.as_bytes());
                id_bytes.extend_from_slice(res_str.as_bytes());
                id_bytes.extend_from_slice(filename.as_bytes());
                id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
                // Use this temporal reduce site directory's NodeID as the PartID for children
                let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
                let file_id = tinyfs::FileID::from_content(
                    parent_part_id,
                    EntryType::TableDynamic,
                    &id_bytes,
                    self.context.file_id.pond_id(),
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
        crate::factory::config_util::config_from_value(config, "Invalid temporal-reduce config")?;

    let directory = TemporalReduceDirectory::new(temporal_config, context)?;
    Ok(directory.create_handle())
}

/// Validate temporal reduce configuration
fn validate_temporal_reduce_config(config: &[u8]) -> TinyFSResult<Value> {
    let (config_value, temporal_config) = crate::factory::config_util::parse_yaml_config::<
        TemporalReduceConfig,
    >(config, "Invalid temporal-reduce config")?;

    // Additional validation: resolutions must parse and nest (each coarser
    // interval an integer multiple of the next finer) so cascaded rollups stay
    // exact.
    _ = parse_nesting_resolutions(&temporal_config.resolutions)?;

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
    use std::sync::Arc;
    use tinyfs::{EntryType, FileID, NodeType};

    use crate::factory::test_support::{
        create_parquet_file, create_test_environment, test_context,
    };

    fn cfg_with_aggs(aggregations: Vec<AggregationConfig>) -> TemporalReduceConfig {
        TemporalReduceConfig {
            in_pattern: crate::Url::parse("series:///sources/*").unwrap(),
            out_pattern: "$0".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations,
            transforms: None,
        }
    }

    fn agg(t: AggregationType, cols: &[&str]) -> AggregationConfig {
        AggregationConfig {
            agg_type: t,
            columns: Some(cols.iter().map(|c| c.to_string()).collect()),
        }
    }

    fn lowering_context() -> crate::FactoryContext {
        // generate_temporal_sql does not read the context; any valid one works.
        let provider_context = crate::factory::test_support::create_provider_context();
        test_context(&provider_context, FileID::root())
    }

    /// Phase 1: Avg must be lowered to decomposable Sum/Count partials and
    /// reconstructed as Sum/Count in the final SELECT. No bare AVG(...) is
    /// emitted, and the output column alias is preserved.
    #[tokio::test]
    async fn test_avg_lowered_to_sum_over_count() {
        let config = cfg_with_aggs(vec![agg(AggregationType::Avg, &["temperature"])]);
        let ctx = lowering_context();
        let sql = generate_temporal_sql(&config, Duration::from_secs(86400), "x", &ctx, "source_t")
            .await
            .unwrap();

        assert!(
            !sql.contains("AVG("),
            "Avg must not be emitted directly: {sql}"
        );
        assert!(
            sql.contains("SUM(\"temperature\")"),
            "expected Sum partial: {sql}"
        );
        assert!(
            sql.contains("COUNT(\"temperature\")"),
            "expected Count partial: {sql}"
        );
        assert!(
            sql.contains("AS \"temperature.avg\""),
            "expected reconstructed avg alias: {sql}"
        );
        assert!(
            sql.contains("NULLIF("),
            "Avg reconstruction must guard empty buckets: {sql}"
        );
    }

    /// Avg and Sum on the same column share a single SUM partial (dedup).
    #[tokio::test]
    async fn test_partials_deduplicated_across_aggregations() {
        let config = cfg_with_aggs(vec![
            agg(AggregationType::Avg, &["temperature"]),
            agg(AggregationType::Sum, &["temperature"]),
        ]);
        let ctx = lowering_context();
        let sql = generate_temporal_sql(&config, Duration::from_secs(86400), "x", &ctx, "source_t")
            .await
            .unwrap();

        assert_eq!(
            sql.matches("SUM(\"temperature\")").count(),
            1,
            "Sum partial must be computed once and reused: {sql}"
        );
        assert!(sql.contains("AS \"temperature.avg\""), "{sql}");
        assert!(sql.contains("AS \"temperature.sum\""), "{sql}");
    }

    /// Min/Max/Count(*) map to their decomposable partials with preserved aliases.
    #[tokio::test]
    async fn test_min_max_count_partials() {
        let config = cfg_with_aggs(vec![
            agg(AggregationType::Min, &["temperature"]),
            agg(AggregationType::Max, &["temperature"]),
            agg(AggregationType::Count, &["*"]),
        ]);
        let ctx = lowering_context();
        let sql = generate_temporal_sql(&config, Duration::from_secs(86400), "x", &ctx, "source_t")
            .await
            .unwrap();

        assert!(sql.contains("MIN(\"temperature\")"), "{sql}");
        assert!(sql.contains("MAX(\"temperature\")"), "{sql}");
        assert!(sql.contains("COUNT(*)"), "{sql}");
        assert!(sql.contains("AS \"temperature.min\""), "{sql}");
        assert!(sql.contains("AS \"temperature.max\""), "{sql}");
        assert!(sql.contains("AS \"timestamp.count\""), "{sql}");
    }

    /// SQL-split correctness: running the per-version partial query and then the
    /// cross-version merge query over a SINGLE input version must reproduce the
    /// single-pass query's output exactly (byte-for-byte per column). With one
    /// version each bucket has exactly one partial row, so the merge is an
    /// identity over the partials and no float reassociation occurs.
    #[tokio::test]
    async fn test_partial_then_merge_equals_full_single_pass() {
        use arrow::array::{Float64Array, TimestampMillisecondArray};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        // Two daily buckets of sub-daily samples with distinct values so avg,
        // min, max, sum, and count all exercise non-trivial reconstruction.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
            Field::new("salinity", DataType::Float64, true),
        ]));
        let day = 86_400_000i64;
        let ts = vec![0, day / 4, day / 2, day, day + day / 4, day + day / 2];
        let temp = vec![20.0, 21.5, 19.0, 25.0, 24.0, 26.5];
        let sal = vec![30.0, 31.0, 32.0, 33.0, 34.0, 35.0];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(temp)),
                Arc::new(Float64Array::from(sal)),
            ],
        )
        .unwrap();

        let config = cfg_with_aggs(vec![
            agg(AggregationType::Avg, &["temperature"]),
            agg(AggregationType::Min, &["temperature"]),
            agg(AggregationType::Max, &["temperature"]),
            agg(AggregationType::Sum, &["salinity"]),
            agg(AggregationType::Count, &["*"]),
        ]);
        let pieces = AggSqlPieces::build(&config).unwrap();
        let interval = Duration::from_secs(86_400);

        let ctx = SessionContext::new();
        let src = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        _ = ctx.register_table("src", Arc::new(src)).unwrap();

        // Single-pass output.
        let full_sql = pieces.full_sql(interval, "timestamp", "src");
        let full = ctx.sql(&full_sql).await.unwrap().collect().await.unwrap();

        // Per-version partials -> register -> merge.
        let available: std::collections::HashSet<String> =
            ["temperature".to_string(), "salinity".to_string()]
                .into_iter()
                .collect();
        let partial_sql = pieces.partial_sql(interval, "timestamp", "src", &available);
        let partials = ctx
            .sql(&partial_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let partials_schema = partials[0].schema();
        let partials_table = MemTable::try_new(partials_schema, vec![partials]).unwrap();
        _ = ctx
            .register_table("partials", Arc::new(partials_table))
            .unwrap();
        let merge_sql = pieces.merge_sql(interval, "timestamp", "partials");
        let merged = ctx.sql(&merge_sql).await.unwrap().collect().await.unwrap();

        let full = arrow::compute::concat_batches(&full[0].schema(), &full).unwrap();
        let merged = arrow::compute::concat_batches(&merged[0].schema(), &merged).unwrap();

        assert_eq!(
            full.schema().fields(),
            merged.schema().fields(),
            "merge output schema must match single-pass"
        );
        assert_eq!(full.num_rows(), merged.num_rows());
        for col in 0..full.num_columns() {
            assert_eq!(
                full.column(col).to_data(),
                merged.column(col).to_data(),
                "column {} ({}) differs between single-pass and partial+merge",
                col,
                full.schema().field(col).name()
            );
        }
    }

    /// A source column that is absent from an older input version must not break
    /// the per-version partial query: an oteljson source builds a per-file wide
    /// schema from the metric names present in that file, so a metric that is
    /// added or renamed over history is missing from earlier versions. The
    /// partial query for such a version emits typed placeholders for the absent
    /// column, keeping the partial schema identical across versions so the merge
    /// can union them, and the merged output matches a single-pass query over
    /// the union of both versions.
    #[tokio::test]
    async fn test_partial_tolerates_column_absent_in_older_version() {
        use arrow::array::{Float64Array, TimestampMillisecondArray};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let day = 86_400_000i64;

        // Older version: only "temperature" exists; "salinity" had not yet
        // appeared in the source.
        let v0_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
        ]));
        let v0 = RecordBatch::try_new(
            v0_schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, day / 4])),
                Arc::new(Float64Array::from(vec![20.0, 22.0])),
            ],
        )
        .unwrap();

        // Newer version: both columns present.
        let v1_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
            Field::new("salinity", DataType::Float64, true),
        ]));
        let v1 = RecordBatch::try_new(
            v1_schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![day, day + day / 4])),
                Arc::new(Float64Array::from(vec![25.0, 24.0])),
                Arc::new(Float64Array::from(vec![33.0, 34.0])),
            ],
        )
        .unwrap();

        let config = cfg_with_aggs(vec![
            agg(AggregationType::Avg, &["temperature"]),
            agg(AggregationType::Sum, &["salinity"]),
            agg(AggregationType::Min, &["salinity"]),
            agg(AggregationType::Max, &["salinity"]),
            agg(AggregationType::Count, &["*"]),
        ]);
        let pieces = AggSqlPieces::build(&config).unwrap();
        let interval = Duration::from_secs(86_400);
        let ctx = SessionContext::new();

        // Per-version partials, each planned against that version's own schema.
        let mut all_partials = Vec::new();
        for (i, (batch, schema)) in [(v0, v0_schema), (v1, v1_schema)].into_iter().enumerate() {
            let table = format!("v{i}");
            let available: std::collections::HashSet<String> =
                schema.fields().iter().map(|f| f.name().clone()).collect();
            let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            _ = ctx.register_table(table.as_str(), Arc::new(mem)).unwrap();
            let partial_sql = pieces.partial_sql(interval, "timestamp", &table, &available);
            let mut part = ctx
                .sql(&partial_sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            all_partials.append(&mut part);
        }

        // All partial batches must share one schema so the merge can union them.
        let partials_schema = all_partials[0].schema();
        for p in &all_partials {
            assert_eq!(
                p.schema(),
                partials_schema,
                "per-version partial schemas must be identical across versions"
            );
        }
        let partials_table = MemTable::try_new(partials_schema, vec![all_partials]).unwrap();
        _ = ctx
            .register_table("partials", Arc::new(partials_table))
            .unwrap();
        let merge_sql = pieces.merge_sql(interval, "timestamp", "partials");
        let merged = ctx.sql(&merge_sql).await.unwrap().collect().await.unwrap();
        let merged = arrow::compute::concat_batches(&merged[0].schema(), &merged).unwrap();

        // Two daily buckets: day 0 (temperature only) and day 1 (both columns).
        assert_eq!(merged.num_rows(), 2);

        let merged_schema = merged.schema();
        let names: Vec<&str> = merged_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let sal_sum_idx = names.iter().position(|n| *n == "salinity.sum").unwrap();
        let sal_sum = merged
            .column(sal_sum_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        use arrow::array::Array;
        // Day 0 had no salinity column at all -> NULL; day 1 -> 33 + 34 = 67.
        assert!(sal_sum.is_null(0), "day-0 salinity sum must be NULL");
        assert_eq!(sal_sum.value(1), 67.0);
    }

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
            let pattern_url = in_pattern.to_string();
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
                pattern_url,
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

        // Builtin schemes: series and table are now entry type suffixes.
        // series:///path parses to format=file + entry_type=series,
        // so full_scheme() returns "file+series" (the canonical form).
        let series_file = make_file("series", "/combined/site1.series");
        assert_eq!(
            series_file.source_url(),
            "file+series:///combined/site1.series",
            "series entry type must be preserved via full_scheme()"
        );

        let table_file = make_file("table", "/tables/lookup");
        assert_eq!(
            table_file.source_url(),
            "file+table:///tables/lookup",
            "table entry type must be preserved via full_scheme()"
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

    /// Phase 2: the incremental rollup cache must produce the same aggregated
    /// values as the single-pass delegate, and a second read over an unchanged
    /// source must reuse the cached partials without recomputing any.
    #[tokio::test]
    async fn test_rollup_cache_matches_single_pass_and_is_incremental() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        // Two CSV source files matched by a glob, one day of hourly data each.
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();

            for day in 0..2u32 {
                let mut csv_data = String::from("timestamp,temperature,humidity\n");
                for hour in 0..24u32 {
                    let d = day + 1;
                    let ts = format!("1970-01-{:02}T{:02}:00:00", d, hour);
                    let temp = 20.5 + hour as f64;
                    let humidity = 50.5 + hour as f64 * 0.5;
                    csv_data.push_str(&format!("{},{},{}\n", ts, temp, humidity));
                }

                use tokio::io::AsyncWriteExt;
                let path = format!("/ingest/weather-{}.csv", day + 1);
                let mut w = root
                    .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
                    .await
                    .unwrap();
                w.write_all(csv_data.as_bytes()).await.unwrap();
                w.flush().await.unwrap();
                w.shutdown().await.unwrap();
            }
        }

        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather-*.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![
                agg(AggregationType::Avg, &["temperature", "humidity"]),
                agg(AggregationType::Min, &["temperature"]),
                agg(AggregationType::Max, &["temperature"]),
            ],
            transforms: None,
        };

        let make_ctx = |cache: Option<std::path::PathBuf>| {
            let session = Arc::new(datafusion::prelude::SessionContext::new());
            let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
                .expect("register object store");
            let ctx = crate::ProviderContext::new(session, Arc::new(persistence.clone()));
            match cache {
                Some(dir) => ctx.with_cache_dir(dir),
                None => ctx,
            }
        };

        // Collect the daily aggregation under a given provider context. Returns
        // the (avg_temp, avg_humidity, min_temp, max_temp) tuples per day.
        async fn collect_daily(
            fs: &tinyfs::FS,
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
        ) -> Vec<(f64, f64, f64, f64)> {
            let _ = fs;
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let weather_dir = match &weather_node.node_type {
                NodeType::Directory(dir) => dir,
                _ => panic!("expected directory"),
            };
            let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
            let file_id = daily_node.id;

            let NodeType::File(file_handle) = &daily_node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");

            let ctx = &provider_context.datafusion_session;
            _ = ctx.register_table("reduced", table_provider).unwrap();
            let batches = ctx
                .sql("SELECT \"temperature.avg\", \"humidity.avg\", \"temperature.min\", \"temperature.max\" FROM reduced ORDER BY timestamp")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let mut rows = Vec::new();
            for b in &batches {
                let col = |name: &str| {
                    b.column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone()
                };
                let ta = col("temperature.avg");
                let ha = col("humidity.avg");
                let tmin = col("temperature.min");
                let tmax = col("temperature.max");
                for i in 0..b.num_rows() {
                    rows.push((ta.value(i), ha.value(i), tmin.value(i), tmax.value(i)));
                }
            }
            rows
        }

        fn count_rollup_partials(cache_dir: &std::path::Path) -> usize {
            let mut count = 0;
            let mut stack = vec![cache_dir.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let Ok(entries) = std::fs::read_dir(&dir) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if path.extension().is_some_and(|e| e == "parquet")
                        && path
                            .parent()
                            .and_then(|p| p.file_name())
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| n.starts_with("rollup_"))
                    {
                        count += 1;
                    }
                }
            }
            count
        }

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Rollup path (cache enabled).
        let cache_ctx = make_ctx(Some(cache_dir.clone()));
        let rollup_rows = collect_daily(&fs, &cache_ctx, config()).await;

        // Single-pass path (no cache) for the same config.
        let nocache_ctx = make_ctx(None);
        let single_rows = collect_daily(&fs, &nocache_ctx, config()).await;

        assert_eq!(rollup_rows.len(), 2, "two daily rows");
        assert_eq!(
            rollup_rows, single_rows,
            "rollup output must equal single-pass output"
        );

        // Known expected values for day 0 (hours 0..23).
        let (ta0, _ha0, tmin0, tmax0) = rollup_rows[0];
        assert!((ta0 - 32.0).abs() < 1e-9, "day0 avg temp");
        assert_eq!(tmin0, 20.5, "day0 min temp");
        assert_eq!(tmax0, 43.5, "day0 max temp");

        // One partial per source file after the first read.
        let partials_after_first = count_rollup_partials(&cache_dir);
        assert_eq!(partials_after_first, 2, "one partial per source file");

        // A second read over an unchanged source, with a fresh session but the
        // same cache directory, must reuse the cached partials: identical output
        // and no newly-written partials.
        let cache_ctx2 = make_ctx(Some(cache_dir.clone()));
        let rollup_rows2 = collect_daily(&fs, &cache_ctx2, config()).await;
        assert_eq!(rollup_rows2, rollup_rows, "incremental read matches");
        assert_eq!(
            count_rollup_partials(&cache_dir),
            partials_after_first,
            "second read must not recompute any partials"
        );
    }

    /// Phase 3: cascading rollups. Multiple nesting resolutions of one site
    /// share a single set of finest-resolution partials; each resolution
    /// re-bins those partials and matches its single-pass output, and raw is
    /// scanned only once regardless of how many resolutions are read.
    #[tokio::test]
    async fn test_rollup_cascading_resolutions_share_finest_partials() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            for day in 0..2u32 {
                let mut csv_data = String::from("timestamp,temperature\n");
                for hour in 0..24u32 {
                    let d = day + 1;
                    let ts = format!("1970-01-{:02}T{:02}:00:00", d, hour);
                    let temp = 20.5 + hour as f64 + day as f64 * 100.0;
                    csv_data.push_str(&format!("{},{}\n", ts, temp));
                }
                use tokio::io::AsyncWriteExt;
                let path = format!("/ingest/weather-{}.csv", day + 1);
                let mut w = root
                    .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
                    .await
                    .unwrap();
                w.write_all(csv_data.as_bytes()).await.unwrap();
                w.flush().await.unwrap();
                w.shutdown().await.unwrap();
            }
        }

        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather-*.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "6h".to_string(), "1d".to_string()],
            aggregations: vec![
                agg(AggregationType::Avg, &["temperature"]),
                agg(AggregationType::Min, &["temperature"]),
                agg(AggregationType::Max, &["temperature"]),
            ],
            transforms: None,
        };

        let make_ctx = |cache: Option<std::path::PathBuf>| {
            let session = Arc::new(datafusion::prelude::SessionContext::new());
            let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
                .expect("register object store");
            let ctx = crate::ProviderContext::new(session, Arc::new(persistence.clone()));
            match cache {
                Some(dir) => ctx.with_cache_dir(dir),
                None => ctx,
            }
        };

        async fn collect_resolution(
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
            res_file: &str,
            table: &str,
        ) -> Vec<(f64, f64, f64)> {
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let weather_dir = match &weather_node.node_type {
                NodeType::Directory(dir) => dir,
                _ => panic!("expected directory"),
            };
            let node = weather_dir.get(res_file).await.unwrap().unwrap();
            let file_id = node.id;
            let NodeType::File(file_handle) = &node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");

            let ctx = &provider_context.datafusion_session;
            _ = ctx.register_table(table, table_provider).unwrap();
            let batches = ctx
                .sql(&format!(
                    "SELECT \"temperature.avg\", \"temperature.min\", \"temperature.max\" FROM {} ORDER BY timestamp",
                    table
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let mut rows = Vec::new();
            for b in &batches {
                let col = |name: &str| {
                    b.column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone()
                };
                let avg = col("temperature.avg");
                let min = col("temperature.min");
                let max = col("temperature.max");
                for i in 0..b.num_rows() {
                    rows.push((avg.value(i), min.value(i), max.value(i)));
                }
            }
            rows
        }

        fn count_rollup_partials(cache_dir: &std::path::Path) -> usize {
            let mut count = 0;
            let mut stack = vec![cache_dir.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let Ok(entries) = std::fs::read_dir(&dir) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if path.extension().is_some_and(|e| e == "parquet")
                        && path
                            .parent()
                            .and_then(|p| p.file_name())
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| n.starts_with("rollup_"))
                    {
                        count += 1;
                    }
                }
            }
            count
        }

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();
        let cache_ctx = make_ctx(Some(cache_dir.clone()));

        // Read all three resolutions from the cascading rollup path.
        let hourly = collect_resolution(&cache_ctx, config(), "res=1h.series", "r_1h").await;
        let partials_after_first_res = count_rollup_partials(&cache_dir);
        let six_hourly = collect_resolution(&cache_ctx, config(), "res=6h.series", "r_6h").await;
        let daily = collect_resolution(&cache_ctx, config(), "res=1d.series", "r_1d").await;

        // Raw is scanned only once: the finest partials (one per source file)
        // are shared, so reading coarser resolutions adds no new partials.
        assert_eq!(
            partials_after_first_res, 2,
            "one finest partial per source file"
        );
        assert_eq!(
            count_rollup_partials(&cache_dir),
            2,
            "coarser resolutions must reuse the shared finest partials"
        );

        // Each cascaded resolution matches its single-pass output.
        let single_1h =
            collect_resolution(&make_ctx(None), config(), "res=1h.series", "s_1h").await;
        let single_6h =
            collect_resolution(&make_ctx(None), config(), "res=6h.series", "s_6h").await;
        let single_1d =
            collect_resolution(&make_ctx(None), config(), "res=1d.series", "s_1d").await;

        assert_eq!(hourly.len(), 48, "48 hourly buckets");
        assert_eq!(six_hourly.len(), 8, "8 six-hourly buckets");
        assert_eq!(daily.len(), 2, "2 daily buckets");
        assert_eq!(hourly, single_1h, "1h cascade == single-pass");
        assert_eq!(six_hourly, single_6h, "6h cascade == single-pass");
        assert_eq!(daily, single_1d, "1d cascade == single-pass");
    }

    /// Phase 3: non-nesting resolution sets are rejected so cascaded rollups can
    /// never silently miscompute a straddling bucket.
    #[test]
    fn test_non_nesting_resolutions_rejected() {
        // 90m is not an integer multiple of 60m, and 60m does not divide 90m.
        let err = parse_nesting_resolutions(&["1h".to_string(), "90m".to_string()]);
        assert!(err.is_err(), "non-nesting resolutions must be rejected");

        // Nesting sets are accepted and returned finest-first.
        let ok = parse_nesting_resolutions(&["1d".to_string(), "1h".to_string(), "6h".to_string()])
            .unwrap();
        assert_eq!(ok.first().copied(), Some(Duration::from_secs(3600)));
    }

    /// Phase 4: a non-sequential source version, one that backfills buckets
    /// already sealed by an earlier version, is a hard error on the incremental
    /// path rather than a silent double-count. A single source file is written
    /// twice: the first version holds only day 2, the second is a full rewrite
    /// that prepends day 1, regressing the input min-ts below the frontier.
    #[tokio::test]
    async fn test_rollup_rejects_non_sequential_version() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        async fn write_csv(root: &tinyfs::WD, path: &str, days: std::ops::Range<u32>) {
            use tokio::io::AsyncWriteExt;
            let mut csv_data = String::from("timestamp,temperature\n");
            for day in days {
                for hour in 0..24u32 {
                    let ts = format!("1970-01-{:02}T{:02}:00:00", day, hour);
                    let temp = 20.5 + hour as f64;
                    csv_data.push_str(&format!("{},{}\n", ts, temp));
                }
            }
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
                .await
                .unwrap();
            w.write_all(csv_data.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            // Version 1: day 2 only.
            write_csv(&root, "/ingest/weather.csv", 2..3).await;
            // Version 2: full rewrite that prepends day 1, backfilling a sealed
            // bucket below the version-1 frontier.
            write_csv(&root, "/ingest/weather.csv", 1..3).await;
        }

        let config = TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![agg(AggregationType::Avg, &["temperature"])],
            transforms: None,
        };

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        let session = Arc::new(datafusion::prelude::SessionContext::new());
        let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
            .expect("register object store");
        let provider_context = crate::ProviderContext::new(session, Arc::new(persistence.clone()))
            .with_cache_dir(cache_dir.clone());

        let context = test_context(&provider_context, FileID::root());
        let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
        let temporal_handle = temporal_dir.create_handle();

        let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
        let NodeType::Directory(weather_dir) = &weather_node.node_type else {
            panic!("expected directory");
        };
        let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
        let file_id = daily_node.id;
        let NodeType::File(file_handle) = &daily_node.node_type else {
            panic!("expected file node");
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;
        let queryable = file_guard.as_queryable().expect("queryable");

        let result = queryable
            .as_table_provider(file_id, &provider_context)
            .await;

        let err = result.expect_err("non-sequential version must be a hard error");
        let msg = err.to_string();
        assert!(
            msg.contains("backfills") && msg.contains("--rebuild"),
            "error must explain the violation and suggest --rebuild, got: {}",
            msg
        );
    }
}
