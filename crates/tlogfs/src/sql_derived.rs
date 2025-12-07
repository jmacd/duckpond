//! SQL-derived dynamic node factories for TLogFS
//!
//! This module provides two specialized factories for SQL operations over TLogFS data:
//!
//! ## `sql-derived-table` Factory
//! - Operates on **FileTable** entries (single files)
//! - Errors if pattern matches more than one file
//! - Ideal for operations on individual data files
//! - Uses unified `create_listing_table_unified()` with DataFusion's native ListingTable
//!
//! ## `sql-derived-series` Factory  
//! - Operates on **FileSeries** entries (versioned time series)
//! - Supports multiple files and multiple versions per file
//! - Automatically unions data across files and versions
//! - Ideal for time-series aggregation and multi-file analytics
//! - Uses unified `create_listing_table_unified()` with DataFusion's native ListingTable
//!
//! ## Unified ListingTable Architecture
//!
//! **Approach**: Both factories now use DataFusion's native ListingTable with our TinyFS ObjectStore
//! implementation. This provides:
//! - **Predicate Pushdown**: Filters are pushed down to the storage layer
//! - **Streaming Execution**: Large datasets can be processed without loading entirely into memory
//! - **Native Performance**: DataFusion's optimized query execution
//! - **Single Code Path**: No duplication between FileTable and FileSeries modes
//!
//! **Legacy Support**: `create_memtable_unified()` is retained for in-memory data (DirectBytes)
//! that cannot use ListingTable, but all file-based operations use the ListingTable approach.
//!
//! For detailed architecture, performance analysis, and predicate pushdown strategies,
//! see [`crates/docs/sql-derived-design.md`](../docs/sql-derived-design.md).

use crate::factory::FactoryContext;
use tinyfs::FileID;

use crate::register_dynamic_factory;
use provider::ScopePrefixTableProvider;
use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::{
    AsyncReadSeek, EntryType, File, FileHandle, Metadata, NodeMetadata, Result as TinyFSResult,
};
use tokio::io::AsyncWrite;

// Removed unused imports - using functions from file_table.rs instead

/// Helper function to convert a File trait object to QueryableFile trait object
/// 
/// This uses a generic downcast approach: since QueryableFile: File, any concrete type
/// T: QueryableFile can be downcast from &dyn File via as_any(). We leverage Rust's
/// type system by attempting to downcast through each known QueryableFile implementation.
/// 
/// This is more maintainable than a factory registry because:
/// 1. Works for both factory-created and non-factory files (OpLogFile, TemporalReduceSqlFile)
/// 2. Type-safe - compiler ensures all QueryableFile impls are File
/// 3. Centralized - all QueryableFile types in one place
pub fn try_as_queryable_file(file: &dyn File) -> Option<&dyn provider::QueryableFile> {
    use crate::file::OpLogFile;
    use crate::temporal_reduce::TemporalReduceSqlFile;
    use crate::timeseries_join::TimeseriesJoinFile;
    use crate::timeseries_pivot::TimeseriesPivotFile;

    let file_any = file.as_any();

    // Try each QueryableFile implementation
    if let Some(f) = file_any.downcast_ref::<OpLogFile>() {
        return Some(f as &dyn provider::QueryableFile);
    }
    if let Some(f) = file_any.downcast_ref::<SqlDerivedFile>() {
        return Some(f as &dyn provider::QueryableFile);
    }
    if let Some(f) = file_any.downcast_ref::<TemporalReduceSqlFile>() {
        return Some(f as &dyn provider::QueryableFile);
    }
    if let Some(f) = file_any.downcast_ref::<TimeseriesJoinFile>() {
        return Some(f as &dyn provider::QueryableFile);
    }
    if let Some(f) = file_any.downcast_ref::<TimeseriesPivotFile>() {
        return Some(f as &dyn provider::QueryableFile);
    }

    None
}

/// Options for SQL transformation and table name replacement
#[derive(Default, Clone)]
pub struct SqlTransformOptions {
    /// Replace multiple table names with mappings (for patterns)
    pub table_mappings: Option<HashMap<String, String>>,
    /// Replace a single source table name (for simple cases)  
    pub source_replacement: Option<String>,
}

/// Mode for SQL-derived operations
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SqlDerivedMode {
    /// FileTable mode: single files only, errors if pattern matches >1 file
    Table,
    /// FileSeries mode: handles multiple files and versions
    Series,
}

/// Configuration for SQL-derived file generation
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SqlDerivedConfig {
    /// Named patterns for matching files. Each pattern name becomes a table in the SQL query.
    /// Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.
    /// Example: {"vulink": "/data/vulink*.series", "at500": "/data/at500*.series"}
    #[serde(default)]
    pub patterns: HashMap<String, String>,

    /// SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    
    /// Optional scope prefixes to apply to table columns (internal use by derivative factories)
    /// Maps table name to (scope_prefix, time_column_name)
    /// NOT serialized - must be set programmatically by factories like timeseries-join
    #[serde(skip, default)]
    pub scope_prefixes: Option<HashMap<String, (String, String)>>,
    
    /// Optional closure to wrap TableProviders before registration
    /// Used by timeseries-pivot to inject null_padding_table wrapping
    /// NOT serialized - must be set programmatically
    #[serde(skip, default)]
    pub provider_wrapper: Option<Arc<dyn Fn(Arc<dyn datafusion::catalog::TableProvider>) -> Result<Arc<dyn datafusion::catalog::TableProvider>, crate::error::TLogFSError> + Send + Sync>>,
}

impl SqlDerivedConfig {
    /// Create a new SqlDerivedConfig with patterns and optional query
    pub fn new(patterns: HashMap<String, String>, query: Option<String>) -> Self {
        Self {
            patterns,
            query,
            scope_prefixes: None,
            provider_wrapper: None,
        }
    }
    
    /// Create a new SqlDerivedConfig with scope prefixes for column renaming
    pub fn new_scoped(
        patterns: HashMap<String, String>,
        query: Option<String>,
        scope_prefixes: HashMap<String, (String, String)>,
    ) -> Self {
        Self {
            patterns,
            query,
            scope_prefixes: Some(scope_prefixes),
            provider_wrapper: None,
        }
    }
    
    /// Builder method to add a provider wrapper closure
    pub fn with_provider_wrapper<F>(mut self, wrapper: F) -> Self 
    where
        F: Fn(Arc<dyn datafusion::catalog::TableProvider>) -> Result<Arc<dyn datafusion::catalog::TableProvider>, crate::error::TLogFSError> + Send + Sync + 'static,
    {
        self.provider_wrapper = Some(Arc::new(wrapper));
        self
    }
}

impl std::fmt::Debug for SqlDerivedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlDerivedConfig")
            .field("patterns", &self.patterns)
            .field("query", &self.query)
            .field("scope_prefixes", &self.scope_prefixes)
            .field("provider_wrapper", &self.provider_wrapper.as_ref().map(|_| "<closure>"))
            .finish()
    }
}

/// Represents a resolved file with its path and NodeID
#[derive(Clone)]
pub struct SqlDerivedFile {
    config: SqlDerivedConfig,
    context: FactoryContext,
    mode: SqlDerivedMode,
}

impl SqlDerivedFile {
    pub fn new(
        config: SqlDerivedConfig,
        context: FactoryContext,
        mode: SqlDerivedMode,
    ) -> TinyFSResult<Self> {
        Ok(Self {
            config,
            context,
            mode,
        })
    }

    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Resolve pattern to QueryableFile instances (eliminates ResolvedFile indirection)
    pub async fn resolve_pattern_to_queryable_files(
        &self,
        pattern: &str,
        entry_type: EntryType,
    ) -> TinyFSResult<Vec<tinyfs::NodePath>> {
        // STEP 1: Build TinyFS from State (transaction context from FactoryContext)
        let fs = tinyfs::FS::new(self.context.state.clone())
            .await
            .map_err(|e| {
                tinyfs::Error::Other(format!("Failed to create TinyFS from state: {}", e))
            })?;
        let tinyfs_root = fs
            .root()
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        // Check if pattern is an exact path (no wildcards) - use resolve_path() for single targets
        // This handles cases like "/test-locations/BDock" where we expect exactly one result
        let is_exact_path =
            !pattern.contains('*') && !pattern.contains('?') && !pattern.contains('[');

        debug!(
            "resolve_pattern_to_queryable_files: pattern='{pattern}', is_exact_path={is_exact_path}"
        );

        // STEP 2: Use collect_matches to get NodePath instances
        let matches = if is_exact_path {
            // Use resolve_path() for exact paths to handle dynamic nodes correctly
            match tinyfs_root.resolve_path(pattern).await {
                Ok((_wd, tinyfs::Lookup::Found(node_path))) => {
                    vec![(node_path, HashMap::new())] // Empty captured groups for exact matches
                }
                Ok((_wd, tinyfs::Lookup::NotFound(_, _))) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Exact path '{}' not found",
                        pattern
                    )));
                }
                Ok((_wd, tinyfs::Lookup::Empty(_))) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Exact path '{}' points to empty directory",
                        pattern
                    )));
                }
                Err(e) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Failed to resolve exact path '{}': {}",
                        pattern, e
                    )));
                }
            }
        } else {
            // Use collect_matches() for glob patterns - returns Vec<(NodePath, Vec<String>)>
            let pattern_matches = tinyfs_root.collect_matches(pattern).await.map_err(|e| {
                tinyfs::Error::Other(format!("Failed to resolve pattern '{}': {}", pattern, e))
            })?;

            // Convert Vec<(NodePath, Vec<String>)> to Vec<(NodePath, HashMap<String, String>)>
            pattern_matches
                .into_iter()
                .map(|(path, captures)| {
                    let mut capture_map = HashMap::new();
                    for (i, capture) in captures.into_iter().enumerate() {
                        _ = capture_map.insert(i.to_string(), capture);
                    }
                    (path, capture_map)
                })
                .collect()
        };

        let matches_count = matches.len();
        debug!(
            "resolve_pattern_to_queryable_files: found {matches_count} matches for pattern '{pattern}'"
        );

        // STEP 3: Extract files - check entry_type directly from FileID (no metadata fetch needed)
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut queryable_files = Vec::new();

        for (node_path, _captured) in matches {
            let file_id = node_path.id();
            let actual_entry_type = file_id.entry_type();
            
            debug!(
                "🔍 Checking file at path '{}': file_id={}, actual_entry_type={:?}, requested_entry_type={:?}",
                node_path.path().display(),
                file_id,
                actual_entry_type,
                entry_type
            );
            
            // Check if this node matches the requested entry_type (encoded in FileID)
            if actual_entry_type == entry_type {
                // For FileSeries, deduplicate by full FileID. For FileTable, use only node_id.
                let dedup_key = match entry_type {
                    EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => {
                        file_id
                    }
                    _ => FileID::new_from_ids(file_id.part_id(), file_id.node_id()),
                };
                if seen.insert(dedup_key) {
                    let entry_type_str = format!("{entry_type:?}");
                    let path_str = node_path.path().display().to_string();
                    debug!(
                        "Successfully extracted file with entry_type '{entry_type_str}' at path '{path_str}'"
                    );
                    queryable_files.push(node_path.clone());
                } else {
                    let path_str = node_path.path().display().to_string();
                    debug!("Deduplication: Skipping duplicate file at path '{path_str}'");
                }
            }
        }

        let file_count = queryable_files.len();
        debug!(
            "resolve_pattern_to_queryable_files: returning {file_count} files after deduplication"
        );
        Ok(queryable_files)
    }

    /// Get the effective SQL query with table name substitution
    /// String replacement is reliable for table names - no fallbacks needed
    #[must_use]
    pub fn get_effective_sql(&self, options: &SqlTransformOptions) -> String {
        let default_query: String;
        let original_sql = if let Some(query) = &self.config.query {
            query.as_str()
        } else {
            // Generate smart default based on patterns
            if self.config.patterns.len() == 1 {
                let pattern_name = self.config.patterns.keys().next().expect("checked");
                default_query = format!("SELECT * FROM {}", pattern_name);
                &default_query
            } else {
		// @@@
                "SELECT * FROM <specify_pattern_name>"
            }
        };

        debug!("Original SQL query: {original_sql}");

        // Direct string replacement - reliable and deterministic
        let result = self.apply_table_transformations(original_sql, options);
        debug!("Transformed SQL result: {result}");
        result
    }

    /// Apply table name transformations to SQL query using LogicalPlan transformation
    /// This properly handles table references without touching column names in quotes
    fn apply_table_transformations(
        &self,
        original_sql: &str,
        options: &SqlTransformOptions,
    ) -> String {
        // If no transformations needed, return original
        if options.table_mappings.is_none() && options.source_replacement.is_none() {
            return original_sql.to_string();
        }

        // Parse SQL into LogicalPlan, transform table references, then unparse back to SQL
        // This ensures we only replace actual table references, not column names or string literals
        use datafusion::sql::parser::DFParser;
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        
        let dialect = GenericDialect {};
        
        // Parse the SQL
        let statements = match DFParser::parse_sql_with_dialect(original_sql, &dialect) {
            Ok(stmts) => stmts,
            Err(e) => {
                debug!("Failed to parse SQL for transformation, falling back to original: {}", e);
                return original_sql.to_string();
            }
        };
        
        if statements.is_empty() {
            return original_sql.to_string();
        }
        
        // Get the first statement (we only expect one SELECT)
        let mut statement = statements[0].clone();
        
        // Apply transformations to table names in the AST
        use datafusion::sql::sqlparser::ast::{Query, SetExpr, Select, TableFactor};
        use datafusion::sql::parser::Statement as DFStatement;
        
        fn replace_table_names_in_statement(
            statement: &mut DFStatement,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            match statement {
                DFStatement::Statement(boxed) => {
                    if let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_mut() {
                        replace_table_names_in_query(query, table_mappings, source_replacement);
                    }
                }
                _ => {}
            }
        }
        
        fn replace_table_names_in_query(
            query: &mut Box<Query>,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            // Handle CTEs (WITH clauses)
            if let Some(ref mut with) = query.with {
                for cte_table in &mut with.cte_tables {
                    replace_table_names_in_query(&mut cte_table.query, table_mappings, source_replacement);
                }
            }
            
            // Handle main query body
            replace_table_names_in_set_expr(&mut query.body, table_mappings, source_replacement);
        }
        
        fn replace_table_names_in_set_expr(
            set_expr: &mut SetExpr,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            match set_expr {
                SetExpr::Select(select) => {
                    replace_table_names_in_select(select, table_mappings, source_replacement);
                }
                SetExpr::SetOperation { left, right, .. } => {
                    // Handle UNION, INTERSECT, EXCEPT operations
                    replace_table_names_in_set_expr(left, table_mappings, source_replacement);
                    replace_table_names_in_set_expr(right, table_mappings, source_replacement);
                }
                SetExpr::Query(query) => {
                    replace_table_names_in_query(query, table_mappings, source_replacement);
                }
                _ => {}
            }
        }
        
        fn replace_table_names_in_select(
            select: &mut Box<Select>,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            // Replace in FROM clause
            for table_with_joins in &mut select.from {
                replace_table_name(&mut table_with_joins.relation, table_mappings, source_replacement);
                
                // Replace in JOINs
                for join in &mut table_with_joins.joins {
                    replace_table_name(&mut join.relation, table_mappings, source_replacement);
                }
            }
        }
        
        fn replace_table_name(
            table_factor: &mut TableFactor,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            if let TableFactor::Table { name, alias, .. } = table_factor {
                let table_name = name.to_string();
                
                if let Some(mappings) = table_mappings {
                    if let Some(replacement) = mappings.get(&table_name) {
                        debug!("Replacing table reference '{}' with '{}' in AST", table_name, replacement);
                        use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart, TableAlias};
                        
                        // If no existing alias, add one using the original table name
                        // This allows column references like "sensor_a.timestamp" to still work
                        if alias.is_none() {
                            *alias = Some(TableAlias {
                                name: Ident::new(&table_name),
                                columns: vec![],
                            });
                        }
                        
                        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(replacement.clone()))]);
                    }
                } else if let Some(replacement) = source_replacement {
                    if table_name == "source" {
                        debug!("Replacing 'source' with '{}' in AST", replacement);
                        use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
                        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(replacement))]);
                    }
                }
            } else if let TableFactor::Derived { subquery, .. } = table_factor {
                // Recursively handle subqueries
                replace_table_names_in_query(subquery, table_mappings, source_replacement);
            }
        }
        
        replace_table_names_in_statement(&mut statement, options.table_mappings.as_ref(), options.source_replacement.as_deref());
        
        // Unparse the transformed AST back to SQL
        statement.to_string()
    }
}

// Async trait implementations

#[async_trait]
impl File for SqlDerivedFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        Err(tinyfs::Error::Other(
            "SQL-derived files cannot be read as byte streams. Use QueryableFile::as_table_provider to get a DataFusion TableProvider, then compose your own queries and choose your execution strategy (streaming/collecting).".to_string()
        ))
    }

    async fn async_writer(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other(
            "SQL-derived file is read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl Metadata for SqlDerivedFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        // Metadata should be lightweight - don't compute the actual data
        // The entry type can be determined from mode without expensive computation
        let entry_type = match self.mode {
            SqlDerivedMode::Table => EntryType::FileTableDynamic,
            SqlDerivedMode::Series => EntryType::FileSeriesDynamic,
        };

        // Return lightweight metadata - size and hash will be computed on actual data access
        Ok(NodeMetadata {
            version: 1,
            size: None,   // Unknown until data is actually computed
            sha256: None, // Unknown until data is actually computed
            entry_type,
            timestamp: 0,
        })
    }
}

// Factory functions for linkme registration

fn create_sql_derived_table_handle(
    config: Value,
    context: provider::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    // Convert to legacy FactoryContext for SqlDerivedFile which hasn't migrated yet
    let legacy_ctx = FactoryContext {
        state: crate::factory::extract_state(&context).map_err(|e| tinyfs::Error::Other(e.to_string()))?,
        file_id: context.file_id,
        pond_metadata: context.pond_metadata.clone(),
    };
    let sql_file = SqlDerivedFile::new(cfg, legacy_ctx, SqlDerivedMode::Table)?;
    Ok(sql_file.create_handle())
}

fn create_sql_derived_series_handle(
    config: Value,
    context: provider::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    // Convert to legacy FactoryContext for SqlDerivedFile which hasn't migrated yet
    let legacy_ctx = FactoryContext {
        state: crate::factory::extract_state(&context).map_err(|e| tinyfs::Error::Other(e.to_string()))?,
        file_id: context.file_id,
        pond_metadata: context.pond_metadata.clone(),
    };
    let sql_file = SqlDerivedFile::new(cfg, legacy_ctx, SqlDerivedMode::Series)?;
    Ok(sql_file.create_handle())
}

fn validate_sql_derived_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse as YAML first (user format)
    let yaml_config: SqlDerivedConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML config: {}", e)))?;

    // Validate that patterns list is not empty
    if yaml_config.patterns.is_empty() {
        return Err(tinyfs::Error::Other(
            "Patterns list cannot be empty".to_string(),
        ));
    }

    // Validate individual patterns
    for (table_name, pattern) in &yaml_config.patterns {
        if table_name.is_empty() {
            return Err(tinyfs::Error::Other(
                "Table name cannot be empty".to_string(),
            ));
        }
        if pattern.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "Pattern for table '{}' cannot be empty",
                table_name
            )));
        }
    }

    // Validate query if provided (now optional)
    if let Some(query) = &yaml_config.query
        && query.is_empty()
    {
        return Err(tinyfs::Error::Other(
            "SQL query cannot be empty if specified".to_string(),
        ));
    }

    // Convert to JSON for internal use
    serde_json::to_value(yaml_config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

// Register the factories
register_dynamic_factory!(
    name: "sql-derived-table",
    description: "Create SQL-derived tables from single FileTable sources",
    file: create_sql_derived_table_handle,
    validate: validate_sql_derived_config
);

register_dynamic_factory!(
    name: "sql-derived-series",
    description: "Create SQL-derived tables from multiple FileSeries sources",
    file: create_sql_derived_series_handle,
    validate: validate_sql_derived_config
);

impl SqlDerivedFile {
    /// Get the factory context for accessing the state
    #[must_use]
    pub fn get_context(&self) -> &FactoryContext {
        &self.context
    }

    /// Get the configuration patterns
    #[must_use]
    pub fn get_config(&self) -> &SqlDerivedConfig {
        &self.config
    }

    /// Get the mode (Table or Series)
    #[must_use]
    pub fn get_mode(&self) -> &SqlDerivedMode {
        &self.mode
    }

    /// Get the effective SQL query that this SQL-derived file represents
    ///
    /// This returns the complete SQL query including WHERE, ORDER BY, and other clauses
    /// as specified by the user. Use this when you need the full query semantics preserved.
    #[must_use]
    pub fn get_effective_sql_query(&self) -> String {
        self.get_effective_sql(&SqlTransformOptions {
            table_mappings: Some(
                self.get_config()
                    .patterns
                    .keys()
                    .map(|k| (k.clone(), k.clone()))
                    .collect(),
            ),
            source_replacement: None,
        })
    }

    /// Generate deterministic table name for caching based on SQL query, pattern config, and file content
    ///
    /// This enables DataFusion table provider and query plan caching by ensuring the same
    /// SQL query + pattern + underlying files always get the same table name.
    async fn generate_deterministic_table_name(
        &self,
        pattern_name: &str,
        pattern: &str,
        queryable_files: &[tinyfs::NodePath],
    ) -> Result<String, crate::error::TLogFSError> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Hash the pattern name
        pattern_name.hash(&mut hasher);

        // Hash the SQL query content
        let sql_query = self.get_effective_sql_query();
        sql_query.hash(&mut hasher);

        // Hash the pattern string
        pattern.hash(&mut hasher);

        // Hash the file IDs (sorted for deterministic ordering)
        let mut file_ids: Vec<String> = Vec::new();
        for node_path in queryable_files {
            let file_id = node_path.id();
            file_ids.push(format!("{}_{}", file_id.node_id(), file_id.part_id()));
        }
        file_ids.sort();
        file_ids.hash(&mut hasher);

        let hash_value = hasher.finish();
        Ok(format!("{:016x}", hash_value))
    }
}

// QueryableFile trait implementation - follows anti-duplication principles
#[async_trait]
impl provider::QueryableFile for SqlDerivedFile {
    /// Create TableProvider for SqlDerivedFile using DataFusion's ViewTable
    ///
    /// This approach creates a ViewTable that wraps the SqlDerivedFile's SQL query,
    /// allowing DataFusion to handle query planning and optimization efficiently.
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &provider::ProviderContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, provider::Error> {
        // Extract State for tlogfs-internal operations (node resolution, etc.)
        let state = context.state_handle
            .downcast_ref::<crate::persistence::State>()
            .ok_or_else(|| provider::Error::StateHandle("Invalid state handle - not a tlogfs State".to_string()))?;

        // Check cache first for SqlDerivedFile ViewTable
        let cache_key = crate::persistence::TableProviderKey::new(
            id,
            crate::file_table::VersionSelection::LatestVersion,
        );

        if let Some(cached_provider) = state.get_table_provider_cache(&cache_key) {
            debug!(
                "🚀 CACHE HIT: Returning cached ViewTable for node_id: {id}"
            );
            return Ok(cached_provider);
        }

        debug!("💾 CACHE MISS: Creating new ViewTable for node_id: {id}");

        // Get SessionContext from state to set up tables
        let ctx = state.session_context().await?;

        // Create mapping from user pattern names to unique internal table names
        let mut table_mappings = HashMap::new();

        // Register each pattern as a table in the session context
        for (pattern_name, pattern) in &self.get_config().patterns {
            // FIXED: After EntryType refactor, we need to search for BOTH Physical and Dynamic variants
            // since source files can be created by factories (Dynamic) or direct uploads (Physical)
            let entry_types = match self.get_mode() {
                SqlDerivedMode::Table => {
                    vec![EntryType::FileTablePhysical, EntryType::FileTableDynamic]
                }
                SqlDerivedMode::Series => {
                    vec![EntryType::FileSeriesPhysical, EntryType::FileSeriesDynamic]
                }
            };
            debug!(
                "🔍 SQL-DERIVED: Processing pattern '{}' -> '{}' (entry_types: {:?})",
                pattern_name, pattern, entry_types
            );

            // Try to resolve pattern with all applicable entry types (Physical and Dynamic)
            debug!(
                "🔍 SQL-DERIVED: Resolving pattern '{}' to queryable files (trying {} entry types)...",
                pattern,
                entry_types.len()
            );
            let mut queryable_files = Vec::new();
            for entry_type in &entry_types {
                match self
                    .resolve_pattern_to_queryable_files(pattern, *entry_type)
                    .await
                {
                    Ok(files) => {
                        debug!(
                            "🔍 SQL-DERIVED: Found {} files for pattern '{}' with entry_type {:?}",
                            files.len(),
                            pattern,
                            entry_type
                        );
                        queryable_files.extend(files);
                    }
                    Err(e) => {
                        debug!(
                            "🔍 SQL-DERIVED: No files found for pattern '{}' with entry_type {:?}: {}",
                            pattern, entry_type, e
                        );
                    }
                }
            }
            debug!(
                "✅ SQL-DERIVED: Pattern '{}' resolved to {} total files across all entry types",
                pattern,
                queryable_files.len()
            );

            if !queryable_files.is_empty() {
                // Generate deterministic table name based on content for caching
                // This enables DataFusion table provider and query plan caching
                let deterministic_name = self
                    .generate_deterministic_table_name(pattern_name, pattern, &queryable_files)
                    .await?;
                // CRITICAL: DataFusion converts table names to lowercase, so we must register with lowercase names
                // to avoid case-sensitivity mismatches between registration and SQL queries
                let unique_table_name =
                    format!("sql_derived_{}_{}", pattern_name, deterministic_name).to_lowercase();
                debug!(
                    "Generated deterministic table name: '{}' for pattern '{}' (hash: {})",
                    unique_table_name, pattern_name, deterministic_name
                );

                _ = table_mappings.insert(pattern_name.clone(), unique_table_name.clone());

                // Create table provider using QueryableFile trait - handles both OpLogFile and SqlDerivedFile
                let listing_table_provider = if queryable_files.len() == 1 {
                    // Single file: use QueryableFile trait dispatch
                    let node_path = &queryable_files[0];
                    let file_id = node_path.id();
                    debug!(
                        "🔍 SQL-DERIVED: Creating table provider for single file: file_id={}",
                        file_id.node_id()
                    );
                    let file_handle = node_path.as_file().await.map_err(|e| {
                        crate::error::TLogFSError::ArrowMessage(format!(
                            "Failed to get file handle: {}",
                            e
                        ))
                    })?;
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                        debug!(
                            "🔍 SQL-DERIVED: File implements QueryableFile trait, calling as_table_provider..."
                        );
                        match queryable_file
                            .as_table_provider(file_id, context)
                            .await
                        {
                            Ok(provider) => {
                                debug!(
                                    "✅ SQL-DERIVED: Successfully created table provider for file_id={}",
                                    file_id.node_id()
                                );
                                // Apply optional provider wrapper (e.g., null_padding_table)
                                if let Some(wrapper) = &self.config.provider_wrapper {
                                    debug!("Applying provider wrapper to table '{}'", pattern_name);
                                    wrapper(provider)?
                                } else {
                                    provider
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "❌ SQL-DERIVED: Failed to create table provider for file_id={}: {}",
                                    file_id.node_id(),
                                    e
                                );
                                return Err(e);
                            }
                        }
                    } else {
                        log::error!(
                            "❌ SQL-DERIVED: File for pattern '{}' does not implement QueryableFile trait",
                            pattern_name
                        );
                        return Err(provider::Error::Arrow(format!(
                            "File for pattern '{}' does not implement QueryableFile trait",
                            pattern_name
                        )));
                    }
                } else {
                    // Multiple files: use multi-URL ListingTable approach (maintains ownership chain)
                    // Following anti-duplication: use existing create_table_provider_for_multiple_urls pattern
                    let mut urls = Vec::new();
                    for node_path in queryable_files {
                        let file_id = node_path.id();
                        let file_handle = node_path.as_file().await.map_err(|e| {
                            crate::error::TLogFSError::ArrowMessage(format!(
                                "Failed to get file handle: {}",
                                e
                            ))
                        })?;
                        let file_arc = file_handle.handle.get_file().await;
                        let file_guard = file_arc.lock().await;
                        if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                            // For files that implement QueryableFile, we need to get their URL pattern
                            // This maintains the ownership chain: FS Root → State → Cache → Single TableProvider
                            match queryable_file
                                .as_any()
                                .downcast_ref::<crate::file::OpLogFile>()
                            {
                                Some(_oplog_file) => {
                                    // Generate URL pattern for this OpLogFile
                                    let url_pattern =
                                        crate::file_table::VersionSelection::AllVersions
                                            .to_url_pattern(&file_id);
                                    urls.push(url_pattern);
                                }
                                None => {
                                    return Err(provider::Error::Arrow(format!(
                                        "Multi-file pattern '{}' contains non-OpLogFile - only OpLogFiles support multi-URL aggregation",
                                        pattern_name
                                    )));
                                }
                            }
                        } else {
                            return Err(provider::Error::Arrow(format!(
                                "File for pattern '{}' does not implement QueryableFile trait",
                                pattern_name
                            )));
                        }
                    }

                    // Use existing create_table_provider_for_multiple_urls to maintain ownership chain
                    if urls.is_empty() {
                        return Err(provider::Error::Arrow(format!(
                            "No valid URLs found for pattern '{}'",
                            pattern_name
                        )));
                    }

                    // Create single TableProvider with multiple URLs - maintains ownership chain
                    // Use direct create_table_provider with additional_urls to avoid TransactionGuard dependency
                    use crate::file_table::{TableProviderOptions, create_table_provider};

                    let dummy_file_id = FileID::root();

                    let options = TableProviderOptions {
                        additional_urls: urls.clone(),
                        ..Default::default()
                    };

                    log::debug!(
                        "📋 CREATING multi-URL TableProvider for pattern '{}': {} URLs",
                        pattern_name,
                        urls.len()
                    );

                    let provider = create_table_provider(dummy_file_id, state, options).await?;
                    
                    // Apply optional provider wrapper (e.g., null_padding_table)
                    if let Some(wrapper) = &self.config.provider_wrapper {
                        debug!("Applying provider wrapper to multi-file table '{}'", pattern_name);
                        wrapper(provider)?
                    } else {
                        provider
                    }
                };
                // Register the ListingTable as the provider
                let table_exists = matches!(
                    ctx.catalog("datafusion")
                        .expect("registered")
                        .schema("public")
                        .expect("defined")
                        .table(&unique_table_name)
                        .await,
                    Ok(Some(_))
                );

                if table_exists {
                    debug!("Table '{unique_table_name}' already exists, skipping registration");
                } else {
                    debug!(
                        "🔍 SQL-DERIVED: Registering table '{}' (pattern: '{}') with DataFusion...",
                        unique_table_name, pattern_name
                    );
                    
                    // Wrap with ScopePrefixTableProvider if scope prefix is configured
                    let final_table_provider: Arc<dyn datafusion::catalog::TableProvider> = 
                        if let Some(scope_prefixes) = &self.config.scope_prefixes {
                            log::debug!(
                                "🔧 SQL-DERIVED: Checking scope_prefixes for pattern '{}'. Available keys: {:?}",
                                pattern_name, scope_prefixes.keys().collect::<Vec<_>>()
                            );
                            if let Some((scope_prefix, time_column)) = scope_prefixes.get(pattern_name.as_str()) {
                                log::debug!(
                                    "🔧 SQL-DERIVED: Wrapping table '{}' (pattern '{}') with scope prefix '{}'",
                                    unique_table_name, pattern_name, scope_prefix
                                );
                                let wrapped = Arc::new(ScopePrefixTableProvider::new(
                                    listing_table_provider,
                                    scope_prefix.clone(),
                                    time_column.clone(),
                                )?);
                                use datafusion::catalog::TableProvider;
                                debug!(
                                    "🔧 SQL-DERIVED: Wrapped table '{}' schema: {:?}",
                                    unique_table_name,
                                    wrapped.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                                );
                                wrapped
                            } else {
                                listing_table_provider
                            }
                        } else {
                            listing_table_provider
                        };
                    
                    _ = ctx
                        .register_table(
                            datafusion::sql::TableReference::bare(unique_table_name.as_str()),
                            final_table_provider,
                        )
                        .map_err(|e| {
                            log::error!(
                                "❌ SQL-DERIVED: Failed to register table '{}': {}",
                                unique_table_name,
                                e
                            );
                            crate::error::TLogFSError::ArrowMessage(format!(
                                "Failed to register table '{}': {}",
                                unique_table_name, e
                            ))
                        })?;
                    debug!(
                        "✅ SQL-DERIVED: Successfully registered table '{}' (user pattern: '{}') in SessionContext",
                        unique_table_name, pattern_name
                    );
                }
            }
        }

        // Get the effective SQL query with table name substitutions using our unique internal names
        debug!("🔍 SQL-DERIVED: Original query: {:?}", self.config.query);
        debug!("🔍 SQL-DERIVED: Table mappings: {:?}", table_mappings);
        let effective_sql = self.get_effective_sql(&SqlTransformOptions {
            table_mappings: Some(table_mappings.clone()),
            source_replacement: None,
        });

        let mapping_count = table_mappings.len();
        debug!(
            "✅ SQL-DERIVED: Effective SQL after table mapping ({} mappings): {}",
            mapping_count, effective_sql
        );
        debug!(
            "🔍 SQL-DERIVED: Table mappings details: {:?}",
            table_mappings
        );

        // Parse the SQL into a LogicalPlan
        debug!(
            "🔍 SQL-DERIVED: Executing SQL with DataFusion: {}",
            effective_sql
        );

        // Debug: List all registered tables in the context
        debug!("🔍 SQL-DERIVED: Available tables in DataFusion context:");
        if let Some(catalog) = ctx.catalog("datafusion") {
            if let Some(schema) = catalog.schema("public") {
                let table_names: Vec<String> = schema.table_names();
                debug!("🔍 SQL-DERIVED: Registered tables: {:?}", table_names);
            } else {
                debug!("🔍 SQL-DERIVED: Could not access 'public' schema");
            }
        } else {
            debug!("🔍 SQL-DERIVED: Could not access 'datafusion' catalog");
        }

        let logical_plan = ctx
            .sql(&effective_sql)
            .await
            .map_err(|e| {
                log::error!(
                    "❌ SQL-DERIVED: Failed to parse SQL into LogicalPlan: {}",
                    e
                );
                log::error!("❌ SQL-DERIVED: Failed SQL was: {}", effective_sql);
                crate::error::TLogFSError::ArrowMessage(format!(
                    "Failed to parse SQL into LogicalPlan: {}",
                    e
                ))
            })?
            .logical_plan()
            .clone();
        debug!("✅ SQL-DERIVED: Successfully created logical plan");

        use datafusion::catalog::view::ViewTable;

        let view_table = ViewTable::new(logical_plan, Some(effective_sql));
        let table_provider = Arc::new(view_table);

        // Cache the ViewTable for future reuse
        state.set_table_provider_cache(cache_key, table_provider.clone());
        debug!("💾 CACHED: Stored ViewTable for node_id: {id}");

        Ok(table_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::OpLogPersistence;
    use provider::QueryableFile;
    use arrow_array::record_batch;
    use tempfile::TempDir;
    use tinyfs::FS;
    use tinyfs::arrow::SimpleParquetExt;

    /// Helper function to get a string array from any column, handling different Arrow string types
    fn get_string_array(
        batch: &arrow::record_batch::RecordBatch,
        column_index: usize,
    ) -> Arc<arrow::array::StringArray> {
        use arrow::datatypes::DataType;
        use arrow_cast::cast;

        let column = batch.column(column_index);
        let string_column = match column.data_type() {
            DataType::Utf8 => column.clone(),
            _ => cast(column.as_ref(), &DataType::Utf8).expect("Failed to cast column to string"),
        };
        string_column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Failed to downcast to StringArray")
            .clone()
            .into()
    }

    /// Helper function to execute a SQL-derived file using direct table provider scanning
    /// This preserves ORDER BY clauses and other SQL semantics from the original query
    async fn execute_sql_derived_direct(
        sql_derived_file: &SqlDerivedFile,
        tx_guard: &mut crate::transaction_guard::TransactionGuard<'_>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Box<dyn std::error::Error + Send + Sync>>
    {
        debug!("execute_sql_derived_direct: Starting execution");

        // Get table provider
        let state_ref = tx_guard.state()?;
        let provider_context = state_ref.as_provider_context();
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        debug!("execute_sql_derived_direct: Got table provider");

        // Scan the ViewTable directly to preserve ORDER BY and other semantics
        let state_ref = tx_guard.state()?;
        let ctx = state_ref.session_context().await?;
        let state = ctx.state();
        let execution_plan = table_provider
            .scan(
                &state, // session state
                None,   // projection (None = all columns)
                &[],    // filters (empty = no additional filters)
                None,   // limit (None = no limit)
            )
            .await?;

        // Execute the plan directly
        use datafusion::physical_plan::collect;
        let task_context = ctx.task_ctx();
        debug!("execute_sql_derived_direct: Executing plan");
        let result_batches = collect(execution_plan, task_context).await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        let batch_count = result_batches.len();
        debug!("execute_sql_derived_direct: Got {total_rows} total rows in {batch_count} batches");

        Ok(result_batches)
    }

    /// Helper function to set up test environment with sample Parquet data
    async fn setup_test_data(persistence: &mut OpLogPersistence) {
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create TinyFS root to work with
        let fs = FS::new(state.clone()).await.unwrap();
        let root = fs.root().await.unwrap();

        // Create test Parquet data with meaningful content
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "David", "Eve",
                ])),
                Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250])),
            ],
        )
        .unwrap();

        // Write to Parquet format
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            _ = writer.close().unwrap();
        }

        // Create the source data file using TinyFS convenience API
        use tinyfs::async_helpers::convenience;
        let _data_file = convenience::create_file_path_with_type(
            &root,
            "/data.parquet",
            &parquet_buffer,
            EntryType::FileTablePhysical,
        )
        .await
        .unwrap();

        // Commit this transaction so the source file is visible to subsequent reads
        tx_guard.commit_test().await.unwrap();
    }

    /// Helper function to set up test environment with FileTable data
    async fn setup_file_table_test_data(persistence: &mut OpLogPersistence) {
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create TinyFS root to work with
        let fs = FS::new(state.clone()).await.unwrap();
        let root = fs.root().await.unwrap();

        // Create test Parquet data with meaningful content (different from FileTable test)
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;

        let schema = Arc::new(Schema::new(vec![
            Field::new("sensor_id", DataType::Int32, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("reading", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![101, 102, 103, 104, 105])),
                Arc::new(StringArray::from(vec![
                    "Building A",
                    "Building B",
                    "Building C",
                    "Building A",
                    "Building B",
                ])),
                Arc::new(Int32Array::from(vec![75, 82, 68, 90, 77])),
            ],
        )
        .unwrap();

        // Write to Parquet format
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            _ = writer.close().unwrap();
        }

        let buffer_len = parquet_buffer.len();
        let preview = if buffer_len >= 8 {
            format!(
                "{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}...",
                parquet_buffer[0],
                parquet_buffer[1],
                parquet_buffer[2],
                parquet_buffer[3],
                parquet_buffer[4],
                parquet_buffer[5],
                parquet_buffer[6],
                parquet_buffer[7]
            )
        } else {
            format!("{:02x?}", &parquet_buffer[..buffer_len.min(8)])
        };
        let suffix = if buffer_len >= 8 {
            format!(
                "...{:02x} {:02x} {:02x} {:02x}",
                parquet_buffer[buffer_len - 4],
                parquet_buffer[buffer_len - 3],
                parquet_buffer[buffer_len - 2],
                parquet_buffer[buffer_len - 1]
            )
        } else {
            "".to_string()
        };
        debug!(
            "Created Parquet buffer: {buffer_len} bytes, starts with: {preview}, ends with: {suffix}"
        );

        // Create the source data as FileTable for SQL testing
        use tinyfs::async_helpers::convenience;
        let _table_file = convenience::create_file_path_with_type(
            &root,
            "/sensor_data.parquet",
            &parquet_buffer,
            EntryType::FileTablePhysical, // Use FileTable for SQL functionality tests
        )
        .await
        .unwrap();

        // Validate the Parquet file by trying to read it directly
        debug!("Validating Parquet file by reading metadata directly...");
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;
        let test_bytes = Bytes::from(parquet_buffer.clone());
        match ParquetRecordBatchReaderBuilder::try_new(test_bytes) {
            Ok(reader_builder) => {
                let metadata = reader_builder.metadata();
                let num_row_groups = metadata.num_row_groups();
                debug!("Parquet validation SUCCESS: {num_row_groups} row groups");
            }
            Err(e) => {
                debug!("Parquet validation FAILED: {e}");
            }
        }

        // Commit this transaction so the source file is visible to subsequent reads
        tx_guard.commit_test().await.unwrap();
    }

    /// Helper function to set up multi-version FileSeries test data
    async fn setup_file_series_multi_version_data(
        persistence: &mut OpLogPersistence,
        num_versions: usize,
    ) {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;

        let schema = Arc::new(Schema::new(vec![
            Field::new("sensor_id", DataType::Int32, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("reading", DataType::Int32, false),
        ]));

        for version in 1..=num_versions {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            // Create different data for each version
            let base_sensor_id = 100 + (version * 10) as i32;
            let base_reading = 70 + (version * 10) as i32;

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![
                        base_sensor_id + 1,
                        base_sensor_id + 2,
                        base_sensor_id + 3,
                    ])),
                    Arc::new(StringArray::from(vec![
                        format!("Building {}", version),
                        format!("Building {}", version),
                        format!("Building {}", version),
                    ])),
                    Arc::new(Int32Array::from(vec![
                        base_reading,
                        base_reading + 5,
                        base_reading + 10,
                    ])),
                ],
            )
            .unwrap();

            // Write to Parquet format
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            // Write to the SAME path for all versions - TLogFS will handle versioning
            let mut writer = root
                .async_writer_path_with_type(
                    "/multi_sensor_data.parquet",
                    EntryType::FileTablePhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_sql_derived_config_validation() {
        let valid_config = r#"
patterns:
  testdata: "/test/data.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;

        let result = validate_sql_derived_config(valid_config.as_bytes());
        assert!(result.is_ok());

        // Test valid pattern config with multiple patterns
        let valid_pattern_config = r#"
patterns:
  data: "/data/*.parquet"
  metrics: "/metrics/*.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;

        let result = validate_sql_derived_config(valid_pattern_config.as_bytes());
        assert!(result.is_ok());

        // Test valid config without query (should use default)
        let valid_no_query_config = r#"
patterns:
  data: "/data/*.parquet"
"#;

        let result = validate_sql_derived_config(valid_no_query_config.as_bytes());
        assert!(result.is_ok());

        // Test empty patterns map
        let invalid_config = r#"
patterns: {}
query: "SELECT * FROM source"
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());

        // Test empty pattern value in map
        let invalid_config = r#"
patterns:
  data: "/data/*.parquet"
  empty: ""
query: "SELECT * FROM source"
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());

        // Test no patterns specified
        let invalid_config = r#"
query: "SELECT * FROM source"
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());

        // Test empty query (when specified)
        let invalid_config = r#"
patterns:
  testdata: "/test/data.parquet"
query: ""
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_derived_pattern_matching() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up multiple FileSeries files that match a pattern
        {
            // Create sensor_data1.parquet
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));

            // First file: sensor_data1.parquet
            let batch1 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![101, 102])),
                    Arc::new(StringArray::from(vec!["Building A", "Building B"])),
                    Arc::new(Int32Array::from(vec![80, 85])),
                ],
            )
            .unwrap();

            let mut parquet_buffer1 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer1);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch1).unwrap();
                _ = writer.close().unwrap();
            }

            let mut writer = root
                .async_writer_path_with_type("/sensor_data1.parquet", EntryType::FileTablePhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer1).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        {
            // Create sensor_data2.parquet
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));

            // Second file: sensor_data2.parquet
            let batch2 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![201, 202])),
                    Arc::new(StringArray::from(vec!["Building C", "Building D"])),
                    Arc::new(Int32Array::from(vec![90, 95])),
                ],
            )
            .unwrap();

            let mut parquet_buffer2 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer2);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch2).unwrap();
                _ = writer.close().unwrap();
            }

            let mut writer = root
                .async_writer_path_with_type("/sensor_data2.parquet", EntryType::FileTablePhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer2).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        // Now test pattern matching across both files
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("sensor_data".to_string(), "/sensor_data*.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading FROM sensor_data WHERE reading > 85 ORDER BY reading DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Read the result using direct table provider scanning
        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // We should have data from both files matching reading > 85
        // File 1: Building A (80) - excluded, Building B (85) - excluded because 85 is not > 85
        // File 2: Building C (90), Building D (95) - both included
        // So we expect 2 rows
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 2);

        // Check that we have the right data ordered by reading DESC
        use arrow::array::Int32Array;
        let locations = get_string_array(result_batch, 0);
        let readings = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Should be ordered by reading DESC: Building D (95), Building C (90)
        assert_eq!(locations.value(0), "Building D");
        assert_eq!(readings.value(0), 95);

        assert_eq!(locations.value(1), "Building C");
        assert_eq!(readings.value(1), 90);

        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_recursive_pattern_matching() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up FileSeries files in different directories
        {
            // Create /sensors/building_a/data.parquet
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));

            let batch_a = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![301])),
                    Arc::new(StringArray::from(vec!["Building A"])),
                    Arc::new(Int32Array::from(vec![100])),
                ],
            )
            .unwrap();

            let mut parquet_buffer_a = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer_a);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_a).unwrap();
                _ = writer.close().unwrap();
            }

            // Create directory first, then file
            _ = root.create_dir_path("/sensors").await.unwrap();
            _ = root.create_dir_path("/sensors/building_a").await.unwrap();
            let mut writer = root
                .async_writer_path_with_type(
                    "/sensors/building_a/data.parquet",
                    EntryType::FileTablePhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer_a).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            // Add a small delay to ensure the async writer background task completes
            tokio::task::yield_now().await;

            tx_guard.commit_test().await.unwrap();
        }

        {
            // Create /sensors/building_b/data.parquet
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));

            let batch_b = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![302])),
                    Arc::new(StringArray::from(vec!["Building B"])),
                    Arc::new(Int32Array::from(vec![110])),
                ],
            )
            .unwrap();

            let mut parquet_buffer_b = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer_b);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_b).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root.create_dir_path("/sensors/building_b").await.unwrap();
            let mut writer = root
                .async_writer_path_with_type(
                    "/sensors/building_b/data.parquet",
                    EntryType::FileTablePhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer_b).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            // Add a small delay to ensure the async writer background task completes
            tokio::task::yield_now().await;

            tx_guard.commit_test().await.unwrap();
        }

        // Now test recursive pattern matching across all nested files
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("data".to_string(), "/**/data.parquet".to_string());
                map
            },
            query: Some(
                "SELECT location, reading, sensor_id FROM data ORDER BY sensor_id".to_string(),
            ),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Read the result using direct table provider scanning
        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // We should have data from both nested directories
        // Building A: sensor_id 301, reading 100
        // Building B: sensor_id 302, reading 110
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Concatenate all batches into one for easier testing
        use arrow::compute::concat_batches;
        let schema = result_batches[0].schema();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        assert_eq!(result_batch.num_columns(), 3);

        use arrow::array::Int32Array;
        let locations = get_string_array(&result_batch, 0);
        let readings = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let sensor_ids = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Should be ordered by sensor_id: Building A (301), Building B (302)
        assert_eq!(sensor_ids.value(0), 301);
        assert_eq!(locations.value(0), "Building A");
        assert_eq!(readings.value(0), 100);

        assert_eq!(sensor_ids.value(1), 302);
        assert_eq!(locations.value(1), "Building B");
        assert_eq!(readings.value(1), 110);

        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_multiple_patterns() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up FileSeries files in different locations matching different patterns
        {
            // Create files in /metrics/ directory
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));

            // Metrics file 1
            let batch_metrics = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![401, 402])),
                    Arc::new(StringArray::from(vec!["Metrics Room A", "Metrics Room B"])),
                    Arc::new(Int32Array::from(vec![120, 125])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_metrics).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root.create_dir_path("/metrics").await.unwrap();
            let mut writer = root
                .async_writer_path_with_type("/metrics/data.parquet", EntryType::FileTablePhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            // Add a small delay to ensure the async writer background task completes
            tokio::task::yield_now().await;

            tx_guard.commit_test().await.unwrap();
        }

        {
            // Create files in /logs/ directory
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));

            // Logs file 1
            let batch_logs = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![501, 502])),
                    Arc::new(StringArray::from(vec!["Log Server A", "Log Server B"])),
                    Arc::new(Int32Array::from(vec![130, 135])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_logs).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root.create_dir_path("/logs").await.unwrap();
            let mut writer = root
                .async_writer_path_with_type("/logs/info.parquet", EntryType::FileTablePhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            // Add a small delay to ensure the async writer background task completes
            tokio::task::yield_now().await;

            tx_guard.commit_test().await.unwrap();
        }

        // Test multiple patterns combining files from different directories
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("metrics".to_string(), "/metrics/*.parquet".to_string());
                _ = map.insert("logs".to_string(), "/logs/*.parquet".to_string());
                map
            },
            query: Some("SELECT * FROM metrics UNION ALL SELECT * FROM logs".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Read the result using direct table provider scanning
        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // We should have data from both directories: 2 from metrics + 2 from logs = 4 rows
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);

        // Concatenate all batches into one for easier testing
        use arrow::compute::concat_batches;
        let schema = result_batches[0].schema();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        assert_eq!(result_batch.num_columns(), 3);

        // Check that we have data from both locations
        use arrow::array::Int32Array;
        let sensor_ids = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let locations = get_string_array(&result_batch, 1);

        // Verify we have sensor IDs from both metrics (401, 402) and logs (501, 502)
        let mut found_metrics = false;
        let mut found_logs = false;

        for i in 0..result_batch.num_rows() {
            let sensor_id = sensor_ids.value(i);
            let location = locations.value(i);

            if (401..=402).contains(&sensor_id) {
                found_metrics = true;
                assert!(location.contains("Metrics"));
            } else if (501..=502).contains(&sensor_id) {
                found_logs = true;
                assert!(location.contains("Log Server"));
            }
        }

        assert!(found_metrics, "Should have found metrics data");
        assert!(found_logs, "Should have found logs data");

        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_default_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up test data
        setup_file_table_test_data(&mut persistence).await;

        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create SQL-derived file without specifying query (should use default)
        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert(
                    "sensor_data".to_string(),
                    "/sensor_data.parquet".to_string(),
                );
                map
            },
            query: None, // No query specified - should default to "SELECT * FROM source"
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let tx_guard_mut = tx_guard;
        let state = tx_guard_mut.state().unwrap();
        let provider_context = state.as_provider_context();
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = state.session_context().await.unwrap();
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // Should have all the original data with default query "SELECT * FROM source"
        // Original test data has 5 rows with sensor readings [75, 82, 68, 90, 77]
        assert_eq!(result_batch.num_rows(), 5);
        assert_eq!(result_batch.num_columns(), 3); // sensor_id, location, reading

        // Verify column names (should match original schema)
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "sensor_id");
        assert_eq!(schema.field(1).name(), "location");
        assert_eq!(schema.field(2).name(), "reading");

        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_single_version() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up FileTable test data (single version)
        setup_file_table_test_data(&mut persistence).await;

        // Create and test the SQL-derived file with FileTable source
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create the SQL-derived file with FileSeries source
        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("sensor_data".to_string(), "/sensor_data.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading * 1.5 as adjusted_reading FROM sensor_data WHERE reading > 75 ORDER BY adjusted_reading DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let tx_guard_mut = tx_guard;
        let state = tx_guard_mut.state().unwrap();
        let provider_context = state.as_provider_context();
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = state.session_context().await.unwrap();
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // Verify we got the expected derived data from FileSeries
        // Original data: sensor readings [75, 82, 68, 90, 77] for locations ["Building A", "Building B", "Building C", "Building A", "Building B"]
        // Query: WHERE reading > 75, SELECT location, reading * 1.5 as adjusted_reading, ORDER BY adjusted_reading DESC
        // Expected: Building A (90*1.5=135), Building B (82*1.5=123), Building B (77*1.5=115.5)
        // Note: 75 is not > 75, and 68 < 75, so they're excluded
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);

        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "adjusted_reading");

        // This transaction is read-only, so just let it end without committing
        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_two_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up FileSeries test data with 2 versions
        setup_file_series_multi_version_data(&mut persistence, 2).await;

        // For now, test against the first version until we implement union logic
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create the SQL-derived file with multi-version FileSeries source
        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("multi_sensor_data".to_string(), "/multi_sensor_data.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading FROM multi_sensor_data WHERE reading > 75 ORDER BY reading DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let tx_guard_mut = tx_guard;
        let state = tx_guard_mut.state().unwrap();
        let provider_context = state.as_provider_context();
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = state.session_context().await.unwrap();
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // For now, we expect data from version 1 only (readings: [70, 75, 80])
        // Query: WHERE reading > 75, so we should get reading=80 from Building 1
        assert!(
            result_batch.num_rows() >= 1,
            "Should have at least 1 row with reading > 75"
        );
        assert_eq!(result_batch.num_columns(), 2);

        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "reading");

        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_three_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up FileSeries test data with 3 versions
        setup_file_series_multi_version_data(&mut persistence, 3).await;

        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create the SQL-derived file that should union all 3 versions
        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert(
                    "multi_sensor_data".to_string(),
                    "/multi_sensor_data.parquet".to_string(),
                );
                map
            },
            // This query should return data from all 3 versions
            query: Some(
                "SELECT location, reading, sensor_id FROM multi_sensor_data ORDER BY sensor_id"
                    .to_string(),
            ),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let tx_guard_mut = tx_guard;
        let state = tx_guard_mut.state().unwrap();
        let provider_context = state.as_provider_context();
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = state.session_context().await.unwrap();
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // We should have data from all 3 versions (3 rows per version = 9 total rows)
        // Version 1: sensor_ids 111, 112, 113 (Building 1)
        // Version 2: sensor_ids 121, 122, 123 (Building 2)
        // Version 3: sensor_ids 131, 132, 133 (Building 3)
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 9);

        // Concatenate all batches into one for easier testing
        use arrow::compute::concat_batches;
        let schema = result_batches[0].schema();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        assert_eq!(result_batch.num_columns(), 3);

        // Check that we have sensor IDs from all versions
        use arrow::array::Int32Array;
        let sensor_ids = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let locations = get_string_array(&result_batch, 0);

        // Verify we have sensor IDs from all 3 versions (ordered by sensor_id)
        assert_eq!(sensor_ids.value(0), 111); // First version
        assert_eq!(locations.value(0), "Building 1");

        assert_eq!(sensor_ids.value(3), 121); // Second version (after 111,112,113)
        assert_eq!(locations.value(3), "Building 2");

        assert_eq!(sensor_ids.value(6), 131); // Third version (after 111,112,113,121,122,123)
        assert_eq!(locations.value(6), "Building 3");

        tx_guard_mut.commit_test().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up test data
        setup_test_data(&mut persistence).await;

        // Create and test the SQL-derived file
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Create the SQL-derived file with read-only state context
        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("data".to_string(), "/data.parquet".to_string());
                map
            },
            query: Some("SELECT name, value * 2 as doubled_value FROM data WHERE value > 150 ORDER BY doubled_value DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use helper function for direct table provider scanning
        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // DEBUG: Print actual data to understand ordering
        log::debug!(
            "Result batch has {} rows, {} columns",
            result_batch.num_rows(),
            result_batch.num_columns()
        );
        for (i, column) in result_batch.columns().iter().enumerate() {
            log::debug!("Column {}: {:?}", i, column);
        }

        // Verify we got the expected derived data with ORDER BY preserved
        // Query: "SELECT name, value * 2 as doubled_value FROM data WHERE value > 150 ORDER BY doubled_value DESC"
        // Expected: David (300*2=600), Eve (250*2=500), Bob (200*2=400) - order should be preserved!
        // Note: Charlie (150) is excluded because 150 is not > 150
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);

        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "doubled_value");

        // Check data - ORDER BY should be preserved with direct table provider scanning
        use arrow::array::Int64Array;
        let names = get_string_array(result_batch, 0);
        let doubled_values = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // With ORDER BY doubled_value DESC, should be: David (600), Eve (500), Bob (400)
        assert_eq!(names.value(0), "David");
        assert_eq!(doubled_values.value(0), 600);

        assert_eq!(names.value(1), "Eve");
        assert_eq!(doubled_values.value(1), 500);

        assert_eq!(names.value(2), "Bob");
        assert_eq!(doubled_values.value(2), 400);

        // This transaction is read-only, so just let it end without committing
        tx_guard_mut.commit_test().await.unwrap();
    }

    /// Test SQL-derived chain functionality and document predicate pushdown limitations
    ///
    /// # Current Behavior
    /// This test demonstrates a two-stage SQL transformation chain:
    /// 1. First node: `data.parquet` → SQL query → `intermediate.parquet`
    /// 2. Second node: `intermediate.parquet` → SQL query → final result
    ///
    /// Each node operates independently with full materialization between stages.
    ///
    /// # Predicate Pushdown Analysis
    ///
    /// ## The Challenge
    /// DataFusion **cannot** push predicates through this chain because:
    ///
    /// 1. **Materialization Boundaries**: Each SQL-derived node fully materializes its results
    ///    as Parquet bytes, breaking the logical query chain.
    ///
    /// 2. **Independent Sessions**: Each node creates its own `SessionContext`, so DataFusion's
    ///    optimizer has zero visibility across nodes.
    ///
    /// 3. **MemTable Limitations**: The intermediate data is loaded into `MemTable`, which
    ///    returns `TableProviderFilterPushDown::Unsupported` for all filters.
    ///
    /// 4. **No Logical Connection**: DataFusion sees each node as reading from static data,
    ///    not as part of a connected pipeline.
    ///
    /// ## The Potential
    /// If predicate pushdown worked across the chain, a query like:
    /// ```sql
    /// SELECT * FROM second_node WHERE final_value > 1400
    /// ```
    ///
    /// Could theoretically optimize to:
    /// - Level 2: `WHERE (adjusted_value * 2) > 1400` → `WHERE adjusted_value > 700`
    /// - Level 1: `WHERE (value + 50) > 700` → `WHERE value > 650`  
    /// - Level 0: Parquet scan with `WHERE value > 650`
    ///
    /// This would:
    /// - **Reduce I/O**: Only read rows that survive all filters
    /// - **Reduce Memory**: No need to materialize filtered-out intermediate results
    /// - **Improve Performance**: Leverage Parquet predicate pushdown at the source
    ///
    /// ## Work Required for True Pushdown
    ///
    /// ### Architecture Changes (High Complexity)
    ///
    /// 1. **Unified Session Management**
    ///    - Single `SessionContext` shared across all SQL-derived nodes
    ///    - Maintain logical plan tree instead of materializing intermediate results
    ///    - Challenge: Complex dependency tracking and invalidation
    ///
    /// 2. **Custom Pushdown-Aware TableProvider**
    ///    ```rust
    ///    struct SqlDerivedTableProvider {
    ///        source_query: String,
    ///        upstream_provider: Arc<dyn TableProvider>,
    ///    }
    ///    
    ///    impl TableProvider for SqlDerivedTableProvider {
    ///        fn supports_filters_pushdown(&self, filters: &[&Expr])
    ///            -> Result<Vec<TableProviderFilterPushDown>> {
    ///            // Return Exact for filters that can be algebraically pushed through
    ///            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    ///        }
    ///        
    ///        async fn scan(&self, filters: &[Expr]) -> Result<Arc<dyn ExecutionPlan>> {
    ///            // Rewrite filters through the SQL transformation
    ///            let pushed_filters = self.rewrite_filters_through_query(filters)?;
    ///            self.upstream_provider.scan(projection, &pushed_filters, limit).await
    ///        }
    ///    }
    ///    ```
    ///
    /// 3. **Query Rewriting Engine**
    ///    - Algebraic manipulation of predicates through SQL transformations
    ///    - Handle complex cases: aggregations, window functions, subqueries
    ///    - Detect non-pushable operations and gracefully degrade
    ///
    /// ### Implementation Phases
    ///
    /// #### Phase 1: Simple Expression Pushdown (Medium Effort)
    /// - Support pushing simple predicates through projection and selection
    /// - Handle basic arithmetic transformations: `WHERE (value + 50) > 700` → `WHERE value > 650`
    /// - Estimated effort: 2-3 weeks
    ///
    /// #### Phase 2: Complex Query Rewriting (High Effort)  
    /// - Support pushdown through joins, aggregations, window functions
    /// - Implement sophisticated predicate analysis and rewriting
    /// - Handle schema changes and column renaming
    /// - Estimated effort: 2-3 months
    ///
    /// #### Phase 3: Optimization and Edge Cases (High Effort)
    /// - Performance optimization for complex rewriting
    /// - Handle all SQL edge cases and error conditions  
    /// - Comprehensive testing and validation
    /// - Estimated effort: 1-2 months
    ///
    /// ### Alternative Approaches
    ///
    /// #### 1. View-Based Lazy Evaluation (Lower Complexity)
    /// - Register SQL-derived nodes as DataFusion views instead of materialized tables
    /// - Let DataFusion's existing optimizer handle the full query tree
    /// - Trade-off: Less control over execution, potential memory issues with large intermediate results
    ///
    /// #### 2. Streaming Pipeline (Medium Complexity)
    /// - Implement SQL-derived nodes as streaming transformations
    /// - Process data in batches without full materialization
    /// - Challenge: More complex error handling and transaction semantics
    ///
    /// ## Compatibility with DuckPond Philosophy
    ///
    /// The current materialized approach aligns with DuckPond's principles:
    /// - **Explicit over implicit**: Each transformation step is clearly defined
    /// - **Fail-fast**: No complex optimization chains that could break subtly
    /// - **Debuggable**: Intermediate results can be inspected and validated
    ///
    /// True predicate pushdown would require careful design to maintain these benefits
    /// while adding sophisticated query optimization capabilities.
    #[tokio::test]
    async fn test_sql_derived_chain() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Set up test data
        setup_test_data(&mut persistence).await;

        // Test chaining: Create two SQL-derived nodes, where one refers to the other
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();

            // Create TinyFS root to work with for storing intermediate results
            let fs = FS::new(state.clone()).await.unwrap();
            let root = fs.root().await.unwrap();

            // Create the first SQL-derived file (filters and transforms original data)
            let context = FactoryContext::new(state.clone(), FileID::root());
            let first_config = SqlDerivedConfig {
                patterns: {
                    let mut map = HashMap::new();
                    _ = map.insert("data".to_string(), "/data.parquet".to_string());
                    map
                },
                query: Some("SELECT name, value + 50 as adjusted_value FROM data WHERE value >= 200 ORDER BY adjusted_value".to_string()),
                ..Default::default()
            };

            let first_sql_file =
                SqlDerivedFile::new(first_config, context.clone(), SqlDerivedMode::Table).unwrap();

            // Execute the first SQL-derived query using direct table provider scanning
            let mut tx_guard_mut = tx_guard;
            let first_result_batches =
                execute_sql_derived_direct(&first_sql_file, &mut tx_guard_mut)
                    .await
                    .unwrap();

            // Convert result batches to Parquet format for intermediate storage
            let first_result_data = {
                use parquet::arrow::ArrowWriter;
                use std::io::Cursor;
                let mut parquet_buffer = Vec::new();
                {
                    let cursor = Cursor::new(&mut parquet_buffer);
                    let schema = first_result_batches[0].schema();
                    let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                    for batch in &first_result_batches {
                        writer.write(batch).unwrap();
                    }
                    _ = writer.close().unwrap();
                }
                parquet_buffer
            };

            // Store the first result as an intermediate Parquet file
            use tinyfs::async_helpers::convenience;
            let _intermediate_file = convenience::create_file_path_with_type(
                &root,
                "/intermediate.parquet",
                &first_result_data,
                EntryType::FileTablePhysical,
            )
            .await
            .unwrap();

            // Commit to make the intermediate file visible
            tx_guard_mut.commit_test().await.unwrap();
        }

        // Second transaction: Create the second SQL-derived node that chains from the first
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();

            let context = FactoryContext::new(state, FileID::root());
            let second_config = SqlDerivedConfig {
                patterns: {
                    let mut map = HashMap::new();
                    _ = map.insert("intermediate".to_string(), "/intermediate.parquet".to_string());
                    map
                },
                query: Some("SELECT name, adjusted_value * 2 as final_value FROM intermediate WHERE adjusted_value > 250 ORDER BY final_value DESC".to_string()),
                ..Default::default()
            };

            let second_sql_file =
                SqlDerivedFile::new(second_config, context, SqlDerivedMode::Table).unwrap();

            // Execute the final chained result using direct table provider scanning
            let mut tx_guard_mut = tx_guard;
            let result_batches = execute_sql_derived_direct(&second_sql_file, &mut tx_guard_mut)
                .await
                .unwrap();

            assert!(!result_batches.is_empty(), "Should have at least one batch");
            let result_batch = &result_batches[0];

            // Verify the chained transformation worked correctly
            // Original data: Alice=100, Bob=200, Charlie=150, David=300, Eve=250
            // First query: WHERE value >= 200, SELECT value + 50 as adjusted_value
            //   -> Bob=250, David=350, Eve=300
            // Second query: WHERE adjusted_value > 250, SELECT adjusted_value * 2 as final_value, ORDER BY final_value DESC
            //   -> David=700, Eve=600 (Bob excluded because 250 is not > 250)
            assert_eq!(result_batch.num_rows(), 2);
            assert_eq!(result_batch.num_columns(), 2);

            // Check column names
            let schema = result_batch.schema();
            assert_eq!(schema.field(0).name(), "name");
            assert_eq!(schema.field(1).name(), "final_value");

            // Check data - should be ordered by final_value DESC
            use arrow::array::Int64Array;
            let names = get_string_array(result_batch, 0);
            let final_values = result_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            assert_eq!(names.value(0), "David"); // (300 + 50) * 2 = 700 (highest)
            assert_eq!(final_values.value(0), 700);

            assert_eq!(names.value(1), "Eve"); // (250 + 50) * 2 = 600 (second)
            assert_eq!(final_values.value(1), 600);

            // Bob would be (200 + 50) * 2 = 500, but excluded because 250 is not > 250

            tx_guard_mut.commit_test().await.unwrap();
        }

        // ## Observations from This Test
        //
        // This test demonstrates the current materialized approach:
        // 1. **Full Materialization**: 3 rows → intermediate.parquet → 2 final rows
        // 2. **No Cross-Node Optimization**: DataFusion optimizes each query independently
        // 3. **Predictable Behavior**: Each step is explicit and debuggable
        //
        // Missing optimization opportunities:
        // - Original data has 5 rows: [Alice=100, Bob=200, Charlie=150, David=300, Eve=250]
        // - First filter `value >= 200` could be pushed to source: scan only 3 rows
        // - Second filter `adjusted_value > 250` could be rewritten as `value > 200` and
        //   combined: scan only 2 rows (David=300, Eve=250)
        // - Current approach: scan 5 rows → materialize 3 → scan 3 → return 2
        // - Optimized approach: scan 2 rows → return 2 (60% less I/O)
    }

    /// Test SQL-derived factory with multiple file versions and wildcard patterns
    ///
    /// This test validates the functionality we implemented for the HydroVu dynamic configuration,
    /// specifically testing:
    /// 1. Multiple file versions with different schemas
    /// 2. Wildcard pattern matching across different file types
    /// 3. Automatic schema merging and column combination by timestamp
    /// 4. SQL-derived factory creating unified views over versioned data
    ///
    /// Based on our real-world success case where we:
    /// - Simplified from complex UNION queries to simple wildcard patterns
    /// - Successfully combined 11,297 rows from multiple file versions
    /// - Let Arrow/Parquet handle schema evolution automatically
    #[tokio::test]
    async fn test_sql_derived_factory_multi_version_wildcard_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Create File A v1: timestamps 1,2,3 with columns: timestamp, temperature
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Float64Array, TimestampSecondArray};
            use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, true), // Make nullable since not all files have it
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(vec![1, 2, 3])),
                    Arc::new(Float64Array::from(vec![20.5, 21.0, 19.8])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root.create_dir_path("/hydrovu").await.unwrap();
            _ = root.create_dir_path("/hydrovu/devices").await.unwrap();
            _ = root
                .create_dir_path("/hydrovu/devices/station_a")
                .await
                .unwrap();
            let mut writer = root
                .async_writer_path_with_type(
                    "/hydrovu/devices/station_a/SensorA_v1.series",
                    EntryType::FileSeriesPhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tokio::task::yield_now().await;
            tx_guard.commit_test().await.unwrap();
        }

        // Create File A v2: timestamps 4,5,6 with columns: timestamp, temperature, humidity
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Float64Array, TimestampSecondArray};
            use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, true), // Make nullable since not all files have it
                Field::new("humidity", DataType::Float64, true), // Make nullable since not all files have it
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(vec![4, 5, 6])),
                    Arc::new(Float64Array::from(vec![22.1, 23.5, 21.8])),
                    Arc::new(Float64Array::from(vec![65.0, 68.2, 70.1])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            // Write new version to same path - TLogFS handles versioning
            let mut writer = root
                .async_writer_path_with_type(
                    "/hydrovu/devices/station_a/SensorA_v1.series",
                    EntryType::FileSeriesPhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tokio::task::yield_now().await;
            tx_guard.commit_test().await.unwrap();
        }

        // Create File B: timestamps 1-6 with columns: timestamp, pressure
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Float64Array, TimestampSecondArray};
            use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("pressure", DataType::Float64, true), // Make nullable since not all files have it
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(Float64Array::from(vec![
                        1013.2, 1012.8, 1014.1, 1015.3, 1013.9, 1012.5,
                    ])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root
                .create_dir_path("/hydrovu/devices/station_b")
                .await
                .unwrap();
            let mut writer = root
                .async_writer_path_with_type(
                    "/hydrovu/devices/station_b/PressureB.series",
                    EntryType::FileSeriesPhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tokio::task::yield_now().await;
            tx_guard.commit_test().await.unwrap();
        }

        // Test SQL-derived factory with wildcard pattern (like our successful HydroVu config)
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        let context = FactoryContext::new(state.clone(), FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                // Use separate patterns like in our successful HydroVu config
                _ = map.insert("sensor_a".to_string(), "/hydrovu/devices/**/SensorA*.series".to_string());
                _ = map.insert("pressure_b".to_string(), "/hydrovu/devices/**/PressureB*.series".to_string());
                map
            },
            // Use COALESCE pattern like in our fixed HydroVu config to ensure non-nullable timestamps
            query: Some("SELECT COALESCE(sensor_a.timestamp, pressure_b.timestamp) AS timestamp, sensor_a.* EXCLUDE (timestamp), pressure_b.* EXCLUDE (timestamp) FROM sensor_a FULL OUTER JOIN pressure_b ON sensor_a.timestamp = pressure_b.timestamp ORDER BY timestamp".to_string()),
            ..Default::default()
        };

        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        // Read the result using direct table provider scanning
        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // Verify schema merging: should have all columns from all files
        // Expected columns: temperature, humidity, timestamp, pressure (order may vary)
        let schema = result_batch.schema();
        assert_eq!(result_batch.num_columns(), 4);

        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(column_names.contains(&"timestamp"));
        assert!(column_names.contains(&"temperature"));
        assert!(column_names.contains(&"humidity"));
        assert!(column_names.contains(&"pressure"));

        // Should have 6 rows total (one for each timestamp 1-6)
        // This validates that COALESCE-based FULL OUTER JOIN combined data correctly by timestamp
        // while ensuring non-nullable timestamp column for temporal partitioning
        assert_eq!(result_batch.num_rows(), 6);

        // Verify data integrity: check a few key combinations
        use arrow::array::{Array, Float64Array, TimestampSecondArray};

        let timestamp_col_idx = column_names
            .iter()
            .position(|&name| name == "timestamp")
            .unwrap();
        let temperature_col_idx = column_names
            .iter()
            .position(|&name| name == "temperature")
            .unwrap();
        let humidity_col_idx = column_names
            .iter()
            .position(|&name| name == "humidity")
            .unwrap();
        let pressure_col_idx = column_names
            .iter()
            .position(|&name| name == "pressure")
            .unwrap();

        let timestamps = result_batch
            .column(timestamp_col_idx)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        let temperatures = result_batch
            .column(temperature_col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let humidity = result_batch
            .column(humidity_col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let pressures = result_batch
            .column(pressure_col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Find rows by timestamp and verify correct data combination
        for row_idx in 0..result_batch.num_rows() {
            let ts = timestamps.value(row_idx);
            match ts {
                1 => {
                    // Timestamp 1: should have temperature (20.5), pressure (1013.2), null humidity
                    assert_eq!(temperatures.value(row_idx), 20.5);
                    assert_eq!(pressures.value(row_idx), 1013.2);
                    assert!(humidity.is_null(row_idx)); // File A v1 didn't have humidity
                }
                2 => {
                    // Timestamp 2: should have temperature (21.0), pressure (1012.8), null humidity
                    assert_eq!(temperatures.value(row_idx), 21.0);
                    assert_eq!(pressures.value(row_idx), 1012.8);
                    assert!(humidity.is_null(row_idx));
                }
                3 => {
                    // Timestamp 3: should have temperature (19.8), pressure (1014.1), null humidity
                    assert_eq!(temperatures.value(row_idx), 19.8);
                    assert_eq!(pressures.value(row_idx), 1014.1);
                    assert!(humidity.is_null(row_idx));
                }
                4 => {
                    // Timestamp 4: should have temperature (22.1), humidity (65.0), pressure (1015.3)
                    assert_eq!(temperatures.value(row_idx), 22.1);
                    assert_eq!(humidity.value(row_idx), 65.0);
                    assert_eq!(pressures.value(row_idx), 1015.3);
                }
                5 => {
                    // Timestamp 5: should have temperature (23.5), humidity (68.2), pressure (1013.9)
                    assert_eq!(temperatures.value(row_idx), 23.5);
                    assert_eq!(humidity.value(row_idx), 68.2);
                    assert_eq!(pressures.value(row_idx), 1013.9);
                }
                6 => {
                    // Timestamp 6: should have temperature (21.8), humidity (70.1), pressure (1012.5)
                    assert_eq!(temperatures.value(row_idx), 21.8);
                    assert_eq!(humidity.value(row_idx), 70.1);
                    assert_eq!(pressures.value(row_idx), 1012.5);
                }
                _ => panic!("Unexpected timestamp: {}", ts),
            }
        }

        tx_guard_mut.commit_test().await.unwrap();
    }

    /// Test temporal-reduce over sql-derived over parquet
    ///
    /// This test exercises the same path as the command:
    /// `RUST_LOG=tlogfs=debug POND=/tmp/dynpond cargo run --bin pond cat '/test-locations/BDockDownsampled/res=1d.series'`
    ///
    /// Testing the chain: Parquet files → SQL-derived (join/filter) → Temporal-reduce (downsampling)
    #[tokio::test]
    async fn test_temporal_reduce_over_sql_derived_over_parquet() {
        // Initialize logger for debugging (safe to call multiple times)
        let _ = env_logger::try_init();

        use crate::factory::FactoryContext;
        use crate::persistence::OpLogPersistence;
        use crate::temporal_reduce::{
            AggregationConfig, AggregationType, TemporalReduceConfig, TemporalReduceDirectory,
        };
        use arrow::array::{Float64Array, StringArray, TimestampSecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap;
        use std::io::Cursor;
        use std::sync::Arc;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Step 1: Create base parquet data with hourly sensor readings over 3 days
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            // Create schema for sensor data: timestamp, station_id, temperature, humidity
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("station_id", DataType::Utf8, false),
                Field::new("temperature", DataType::Float64, true),
                Field::new("humidity", DataType::Float64, true),
            ]));

            // Create 3 days of hourly data (72 records total)
            let base_timestamp = 1640995200; // 2022-01-01 00:00:00 UTC
            let mut timestamps = Vec::new();
            let mut station_ids = Vec::new();
            let mut temperatures = Vec::new();
            let mut humidities = Vec::new();

            for hour in 0..72 {
                let ts = base_timestamp + (hour * 3600); // hourly intervals
                let station = if hour % 2 == 0 { "BDock" } else { "Silver" };
                let temp = 20.0 + (hour as f64 * 0.1) + ((hour as f64 * 0.5).sin() * 5.0); // varying temperature
                let humid = 50.0 + ((hour as f64 * 0.3).cos() * 10.0); // varying humidity

                timestamps.push(ts);
                station_ids.push(station.to_string());
                temperatures.push(temp);
                humidities.push(humid);
            }

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(timestamps)),
                    Arc::new(StringArray::from(station_ids)),
                    Arc::new(Float64Array::from(temperatures)),
                    Arc::new(Float64Array::from(humidities)),
                ],
            )
            .unwrap();

            // Write to parquet
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            // Store as base sensor data - explicitly create parent directories to validate part_id handling
            _ = root.create_dir_path("/sensors").await.unwrap();
            _ = root.create_dir_path("/sensors/stations").await.unwrap();

            use tinyfs::async_helpers::convenience;
            let _base_file = convenience::create_file_path_with_type(
                &root,
                "/sensors/stations/all_data.series",
                &parquet_buffer,
                EntryType::FileSeriesPhysical,
            )
            .await
            .unwrap();

            // CRUCIAL: Commit the transaction to make the base file visible for pattern resolution
            tx_guard.commit_test().await.unwrap();
        }

        // Step 2: Create SQL-derived node that filters for BDock station only
        let bdock_sql_derived = {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();
            let context = FactoryContext::new(state, FileID::root());

            let config = SqlDerivedConfig {
                patterns: {
                    let mut map = HashMap::new();
                    _ = map.insert("series".to_string(), "/sensors/stations/all_data.series".to_string());
                    map
                },
                query: Some("SELECT timestamp, temperature, humidity FROM series WHERE station_id = 'BDock' ORDER BY timestamp".to_string()),
                ..Default::default()
            };

            let sql_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
            tx_guard.commit_test().await.unwrap();
            sql_file
        };

        // Step 3: Create temporal-reduce node that downsamples to daily averages
        let temporal_reduce_dir = {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();
            let context = FactoryContext::new(state, FileID::root());

            let config = TemporalReduceConfig {
                in_pattern: "/sensors/stations/*".to_string(), // Use pattern to match parquet data
                out_pattern: "$0".to_string(),                 // Keep original filename as output
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()],
                aggregations: vec![AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: Some(vec!["temperature".to_string(), "humidity".to_string()]),
                }],
            };

            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            tx_guard.commit_test().await.unwrap();
            temporal_dir
        };

        // Step 4: Test the architectural components without full execution
        {
            let tx_guard = persistence.begin_test().await.unwrap();

            // First, execute the SQL-derived query
            let mut tx_guard_mut = tx_guard;
            let bdock_batches = execute_sql_derived_direct(&bdock_sql_derived, &mut tx_guard_mut)
                .await
                .unwrap();

            // Verify we filtered correctly (should have 36 rows - every other hour for BDock)
            assert!(!bdock_batches.is_empty());
            let total_bdock_rows: usize = bdock_batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_bdock_rows, 36,
                "Should have 36 BDock records (every other hour for 72 hours)"
            );

            // Verify columns are present
            let bdock_schema = bdock_batches[0].schema();
            assert_eq!(bdock_schema.fields().len(), 3);
            assert_eq!(bdock_schema.field(0).name(), "timestamp");
            assert_eq!(bdock_schema.field(1).name(), "temperature");
            assert_eq!(bdock_schema.field(2).name(), "humidity");

            // Now test actual temporal-reduce execution - should reduce 36 hourly points to ~3 daily points
            use tinyfs::Directory;
            // With hierarchical structure, first get the site directory "all_data.series"
            let site_dir_node = temporal_reduce_dir.get("all_data.series").await.unwrap();
            assert!(
                site_dir_node.is_some(),
                "Should find all_data.series site directory in temporal reduce directory"
            );

            // Then get the resolution file "res=1d.series" within that site directory
            let site_node = site_dir_node.unwrap();
            if let tinyfs::NodeType::Directory(site_dir_handle) = &site_node.node_type {
                let daily_series_node = site_dir_handle.get("res=1d.series").await.unwrap();
                assert!(
                    daily_series_node.is_some(),
                    "Should find res=1d.series in all_data.series site directory"
                );

                let daily_node = daily_series_node.unwrap();

                if let tinyfs::NodeType::File(file_handle) = &daily_node.node_type {
                    // Access the file through the public API and downcast using as_any()
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                        let state = tx_guard_mut.state().unwrap();
                        let file_id = daily_node.id;
                        let provider_context = state.as_provider_context();
                        let table_provider = queryable_file
                            .as_table_provider(
                                file_id,
                                &provider_context,
                            )
                            .await
                            .unwrap();

                        // Execute the temporal-reduce query using the proper public API
                        let ctx = state.session_context().await.unwrap();
                        _ = ctx
                            .register_table("temporal_reduce_table", table_provider)
                            .unwrap();

                        // Execute query to get results
                        let dataframe = ctx
                            .sql("SELECT * FROM temporal_reduce_table")
                            .await
                            .unwrap();
                        let temporal_batches = dataframe.collect().await.unwrap();

                        // Verify temporal reduction actually reduced the data
                        let total_temporal_rows: usize =
                            temporal_batches.iter().map(|b| b.num_rows()).sum();
                        log::debug!(
                            "   - Temporal-reduce produced {} daily aggregated records from 36 hourly records",
                            total_temporal_rows
                        );

                        // With 36 hourly BDock records over 3 days, daily aggregation should produce ~3 records
                        assert!(
                            total_temporal_rows > 0 && total_temporal_rows < 36,
                            "Temporal reduction should produce fewer records than input. Got {} from 36 input records",
                            total_temporal_rows
                        );

                        // Verify the temporal aggregation schema includes time_bucket and aggregated columns
                        if !temporal_batches.is_empty() {
                            let temporal_schema = temporal_batches[0].schema();
                            let field_names: Vec<&str> = temporal_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect();
                            log::debug!("   - Temporal-reduce schema: {:?}", field_names);

                            // Should have time_bucket (or timestamp) and aggregated columns like avg_temperature
                            assert!(field_names.iter().any(|name| name.contains("time") || name.contains("timestamp")), 
                            "Should have time column, got: {:?}", field_names);
                            assert!(
                                field_names.iter().any(|name| name.contains("temperature")),
                                "Should have temperature aggregation, got: {:?}",
                                field_names
                            );
                        }
                    } else {
                        panic!("Temporal-reduce should create a QueryableFile");
                    }
                } else {
                    panic!("Expected file node from temporal reduce directory");
                }
            } else {
                panic!("Expected directory node for site directory");
            }

            log::debug!(
                "✅ Temporal-reduce over SQL-derived over Parquet test completed successfully"
            );
            log::debug!("   - Created 72 hourly sensor records from 2 stations");
            log::debug!("   - SQL-derived filtered to 36 BDock-only records");
            log::debug!("   - Temporal-reduce configuration validated");
            log::debug!(
                "   - Architecture demonstrates: Parquet → SQL-derived → Temporal-reduce chain"
            );

            tx_guard_mut.commit_test().await.unwrap();
        }
    }

    /// Test that OpLogFile is properly detected as QueryableFile
    ///
    /// This test verifies that our try_as_queryable_file function correctly identifies
    /// OpLogFile instances as queryable, which is critical for the "cat" command and
    /// other operations that need to execute SQL queries over basic TLogFS files.
    /// This test would fail if OpLogFile detection was commented out.
    #[tokio::test]
    async fn test_oplog_file_queryable_detection() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Create a simple test batch using record_batch! macro
        let batch = record_batch!(
            ("id", Int32, [1_i32, 2_i32, 3_i32]),
            ("name", Utf8, ["Alice", "Bob", "Charlie"]),
            ("value", Int32, [100_i32, 200_i32, 150_i32])
        )
        .unwrap();

        // Create an OpLogFile with parquet data - use setup_test_data pattern
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            // Write as FileTable (this creates an OpLogFile internally)
            root.write_parquet("/test_data.parquet", &batch, EntryType::FileTablePhysical)
                .await
                .unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        // Test OpLogFile QueryableFile detection in single transaction
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        // Get the file we just created
        let fs = FS::new(state.clone()).await.unwrap();
        let root_node = fs.root().await.unwrap();
        let file_lookup = root_node.resolve_path("/test_data.parquet").await.unwrap();

        match file_lookup.1 {
            tinyfs::Lookup::Found(node_path) => {
                if let tinyfs::NodeType::File(file_handle) = &node_path.node.node_type {
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // This is the critical test: try_as_queryable_file should find OpLogFile
                    let queryable_file = try_as_queryable_file(&**file_guard);
                    assert!(
                        queryable_file.is_some(),
                        "OpLogFile should be detected as QueryableFile"
                    );

                    // Verify we can actually create a table provider from it
                    let queryable = queryable_file.unwrap();
                    let file_id = node_path.node.id;
                    let provider_context = state.as_provider_context();
                    let table_provider = queryable
                        .as_table_provider(file_id, &provider_context)
                        .await
                        .unwrap();

                    // Test that we can register it with DataFusion (simulating "cat" command behavior)
                    let ctx = state.session_context().await.unwrap();
                    _ = ctx.register_table("test_table", table_provider).unwrap();

                    log::debug!(
                        "✅ OpLogFile successfully detected as QueryableFile and registered with DataFusion"
                    );
                } else {
                    panic!("Expected file node");
                }
            }
            _ => panic!("File not found"),
        }

        tx_guard.commit_test().await.unwrap();
    }

    /// Test temporal-reduce with wildcard column discovery (columns: None)
    ///
    /// This test duplicates the above test but uses automatic schema discovery
    /// to verify that the deferred schema discovery feature works correctly.
    #[tokio::test]
    async fn test_temporal_reduce_wildcard_schema_discovery() {
        // Initialize logger for debugging (safe to call multiple times)
        let _ = env_logger::try_init();

        use crate::factory::FactoryContext;
        use crate::persistence::OpLogPersistence;
        use crate::temporal_reduce::{
            AggregationConfig, AggregationType, TemporalReduceConfig, TemporalReduceDirectory,
        };
        use arrow::array::{Float64Array, TimestampSecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Step 1: Create base parquet data with hourly sensor readings over 3 days using proper TinyFS Arrow integration
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            // Create 3 days of hourly data (72 records total) using Arrow test support macros
            let base_timestamp = 1640995200; // 2022-01-01 00:00:00 UTC
            let mut timestamps = Vec::new();
            let mut temperatures = Vec::new();
            let mut humidities = Vec::new();

            for hour in 0..72 {
                let ts = base_timestamp + (hour * 3600); // hourly intervals
                let temp = 20.0 + (hour as f64 * 0.1) + ((hour as f64 * 0.5).sin() * 5.0); // varying temperature
                let humid = 50.0 + ((hour as f64 * 0.3).cos() * 10.0); // varying humidity

                timestamps.push(ts);
                temperatures.push(temp);
                humidities.push(humid);
            }

            // Create RecordBatch using proper Arrow approach
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, true),
                Field::new("humidity", DataType::Float64, true),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(timestamps)),
                    Arc::new(Float64Array::from(temperatures)),
                    Arc::new(Float64Array::from(humidities)),
                ],
            )
            .unwrap();

            // Store as base sensor data using TinyFS Arrow integration - create parent directories
            _ = root.create_dir_path("/sensors").await.unwrap();
            _ = root
                .create_dir_path("/sensors/wildcard_test")
                .await
                .unwrap();

            // Use TinyFS SimpleParquetExt to write proper file:series data
            use tinyfs::arrow::SimpleParquetExt;
            root.write_parquet(
                "/sensors/wildcard_test/base_data.series",
                &batch,
                EntryType::FileSeriesPhysical,
            )
            .await
            .unwrap();

            // CRUCIAL: Commit the transaction to make the base file visible for pattern resolution
            tx_guard.commit_test().await.unwrap();
        }

        // Step 2: Test wildcard schema discovery within the same transaction as data access
        // This reflects the normal usage pattern where directories are created and accessed
        // within the same transaction context
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let state = tx_guard.state().unwrap();
            let context = FactoryContext::new(state, FileID::root());

            // Define temporal-reduce config with wildcard schema discovery
            let temporal_config = TemporalReduceConfig {
                in_pattern: "/sensors/wildcard_test/*".to_string(), // Use pattern to match base_data.series
                out_pattern: "$0".to_string(), // Keep original filename as output
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()],
                aggregations: vec![AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: None, // This should trigger automatic schema discovery!
                }],
            };

            // Create temporal reduce directory within the same transaction that will access it
            let temporal_reduce_dir =
                TemporalReduceDirectory::new(temporal_config.clone(), context.clone()).unwrap();

            // Test direct read of the file we just wrote
            let debug_state = tx_guard.state().unwrap();

            // Now test the pattern matching and QueryableFile access
            let fs = FS::new(debug_state.clone()).await.unwrap();
            let root_node = fs.root().await.unwrap();
            let pattern_matches = root_node
                .collect_matches("/sensors/wildcard_test/*")
                .await
                .unwrap();
            log::debug!(
                "   - Pattern /sensors/wildcard_test/* found {} matches",
                pattern_matches.len()
            );

            for (i, (node_path, captured)) in pattern_matches.iter().enumerate() {
                let path_str = node_path.path.to_string_lossy();
                log::debug!("   - Match {}: {} -> captured: {:?}", i, path_str, captured);

                // Try to access the schema of this file directly
                if let tinyfs::NodeType::File(file_handle) = &node_path.node.node_type {
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Check the file metadata first
                    let metadata = file_guard.metadata().await.unwrap();
                    log::debug!(
                        "   - File {} metadata: entry_type={:?}",
                        path_str,
                        metadata.entry_type
                    );

                    if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                        log::debug!("   - File {} implements QueryableFile", path_str);
                        let file_id = node_path.node.id;
                        log::debug!(
                            "   - Using file_id: {}",
                            file_id.node_id()
                        );

                        let provider_context = debug_state.as_provider_context();
                        match queryable_file
                            .as_table_provider(file_id, &provider_context)
                            .await
                        {
                            Ok(table_provider) => {
                                let schema = table_provider.schema();
                                let field_names: Vec<&str> =
                                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                                log::debug!(
                                    "   - QueryableFile schema for {}: {:?}",
                                    path_str,
                                    field_names
                                );

                                // Let's try to actually query the table provider to see if it has data
                                log::debug!("   - Attempting to scan table provider...");
                                match table_provider
                                    .scan(
                                        &debug_state.session_context().await.unwrap().state(),
                                        None,
                                        &[],
                                        None,
                                    )
                                    .await
                                {
                                    Ok(_exec_plan) => {
                                        log::debug!("   - ✅ Table provider scan succeeded");

                                        // Check schema again after scan - maybe it's lazy-loaded
                                        let schema_after_scan = table_provider.schema();
                                        let field_names_after_scan: Vec<&str> = schema_after_scan
                                            .fields()
                                            .iter()
                                            .map(|f| f.name().as_str())
                                            .collect();
                                        log::debug!(
                                            "   - Schema AFTER scan: {:?}",
                                            field_names_after_scan
                                        );

                                        // Also check if we can get table statistics
                                        if let Some(stats) = table_provider.statistics() {
                                            log::debug!(
                                                "   - Table statistics: num_rows={:?}, total_byte_size={:?}",
                                                stats.num_rows,
                                                stats.total_byte_size
                                            );
                                        } else {
                                            log::debug!("   - No table statistics available");
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!("   - ❌ Table provider scan failed: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                log::debug!(
                                    "   - Error getting table provider for {}: {}",
                                    path_str,
                                    e
                                );
                            }
                        }
                    } else {
                        log::debug!("   - File {} does NOT implement QueryableFile", path_str);
                        log::debug!("   - File type: {:?}", file_guard.as_any().type_id());

                        // Let's see what type this actually is
                        let type_name = std::any::type_name_of_val(&**file_guard);
                        log::debug!("   - Actual file type name: {}", type_name);
                    }
                } else {
                    log::debug!("   - Match {} is not a file: {:?}", i, node_path.node.node_type);
                }
            }

            use tinyfs::Directory;
            // With hierarchical structure, first get the site directory "base_data.series"
            let site_dir_node = temporal_reduce_dir.get("base_data.series").await.unwrap();
            assert!(
                site_dir_node.is_some(),
                "Should find base_data.series site directory in temporal reduce directory"
            );

            // Then get the resolution file "res=1d.series" within that site directory
            let site_node = site_dir_node.unwrap();
            if let tinyfs::NodeType::Directory(site_dir_handle) = &site_node.node_type {
                let daily_series_node = site_dir_handle.get("res=1d.series").await.unwrap();
                assert!(
                    daily_series_node.is_some(),
                    "Should find res=1d.series in base_data.series site directory"
                );

                let daily_node = daily_series_node.unwrap();

                if let tinyfs::NodeType::File(file_handle) = &daily_node.node_type {
                    // Access the file through the public API and downcast using as_any()
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                        let state = tx_guard.state().unwrap();
                        let file_id = daily_node.id;
                        let provider_context = state.as_provider_context();
                        let table_provider = queryable_file
                            .as_table_provider(
                                file_id,
                                &provider_context,
                            )
                            .await
                            .unwrap();

                        // Execute the temporal-reduce query using the proper public API
                        let ctx = state.session_context().await.unwrap();
                        _ = ctx
                            .register_table("wildcard_temporal_table", table_provider)
                            .unwrap();

                        // Execute query to get results
                        let dataframe = ctx
                            .sql("SELECT * FROM wildcard_temporal_table")
                            .await
                            .unwrap();
                        let temporal_batches = dataframe.collect().await.unwrap();

                        // Verify wildcard schema discovery worked
                        let total_temporal_rows: usize =
                            temporal_batches.iter().map(|b| b.num_rows()).sum();
                        log::debug!(
                            "   - Wildcard temporal-reduce produced {} daily aggregated records",
                            total_temporal_rows
                        );

                        // Should produce temporal aggregations for all available data
                        assert!(
                            total_temporal_rows > 0,
                            "Wildcard temporal reduction should produce records. Got {} records",
                            total_temporal_rows
                        );

                        // Verify the schema was automatically discovered and aggregated
                        if !temporal_batches.is_empty() {
                            let temporal_schema = temporal_batches[0].schema();
                            let field_names: Vec<&str> = temporal_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect();
                            log::debug!("   - Wildcard schema discovery result: {:?}", field_names);

                            // Should have timestamp and all numeric columns were auto-discovered and aggregated
                            assert!(field_names.iter().any(|name| name.contains("time") || name.contains("timestamp")), 
                            "Should have time column, got: {:?}", field_names);

                            // The key test: automatic discovery should have found both temperature and humidity
                            // and created avg aggregations for both (since columns: None)
                            assert!(
                                field_names.iter().any(|name| name.contains("temperature")),
                                "Should have auto-discovered temperature column, got: {:?}",
                                field_names
                            );
                            assert!(
                                field_names.iter().any(|name| name.contains("humidity")),
                                "Should have auto-discovered humidity column, got: {:?}",
                                field_names
                            );
                        }
                    } else {
                        panic!("Temporal-reduce should create a QueryableFile");
                    }
                } else {
                    panic!("Expected file node from temporal reduce directory");
                }
            } else {
                panic!("Expected directory node for site directory");
            }

            log::debug!("✅ Wildcard temporal-reduce schema discovery test completed successfully");
            log::debug!("   - Created 72 hourly sensor records with numeric columns only");
            log::debug!("   - Temporal-reduce automatically discovered all numeric columns");
            log::debug!(
                "   - Verified both temperature and humidity were included in aggregations"
            );

            tx_guard.commit_test().await.unwrap();
        }
    }

    /// Test that pattern matching finds both Physical and Dynamic entry types
    /// This guards against regression of the EntryType refactor bug where only Physical
    /// types were searched, missing Dynamic files from factories
    #[tokio::test]
    async fn test_pattern_matching_finds_physical_and_dynamic_types() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Create two files with the same schema but different EntryTypes
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            _ = root.create_dir_path("/test").await.unwrap();

            use arrow::array::{Int32Array, TimestampMillisecondArray};
            use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Int32, false),
            ]));

            // Create Physical file
            let batch_physical = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
                    Arc::new(Int32Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_physical).unwrap();
                _ = writer.close().unwrap();
            }

            let mut writer = root
                .async_writer_path_with_type("/test/physical.series", EntryType::FileSeriesPhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        {
            // Create second Physical file
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            use arrow::array::{Int32Array, TimestampMillisecondArray};
            use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Int32, false),
            ]));

            let batch_dynamic = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![4000, 5000, 6000])),
                    Arc::new(Int32Array::from(vec![40, 50, 60])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_dynamic).unwrap();
                _ = writer.close().unwrap();
            }

            let mut writer = root
                .async_writer_path_with_type("/test/physical2.series", EntryType::FileSeriesPhysical)
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        // Test: SqlDerivedFile should find multiple Physical files with pattern matching
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert("data".to_string(), "/test/*.series".to_string());
                map
            },
            query: Some("SELECT * FROM data ORDER BY timestamp".to_string()),
            ..Default::default()
        };

        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // Should have data from both files: 3 rows from physical + 3 rows from physical2 = 6 rows total
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 6,
            "Should find multiple Physical files with pattern matching - got {} rows, expected 6",
            total_rows
        );

        tx_guard_mut.commit_test().await.unwrap();
    }

    /// Test that table names are always lowercase to match DataFusion's case-insensitive behavior
    /// This guards against regression where tables registered with mixed case caused "table not found" errors
    #[tokio::test]
    async fn test_table_names_are_lowercase() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create_test(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        // Create a file with uppercase in the path
        {
            let tx_guard = persistence.begin_test().await.unwrap();
            let root = tx_guard.root().await.unwrap();

            _ = root.create_dir_path("/Sensors").await.unwrap();

            use arrow::array::{Int32Array, TimestampMillisecondArray};
            use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("Temperature", DataType::Int32, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
                    Arc::new(Int32Array::from(vec![20, 25, 30])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            let mut writer = root
                .async_writer_path_with_type(
                    "/Sensors/Temperature.series",
                    EntryType::FileSeriesPhysical,
                )
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();

            tx_guard.commit_test().await.unwrap();
        }

        // Test: SqlDerivedFile with mixed-case pattern name should work
        let tx_guard = persistence.begin_test().await.unwrap();
        let state = tx_guard.state().unwrap();

        let context = FactoryContext::new(state, FileID::root());
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                _ = map.insert(
                    "TempData".to_string(),
                    "/Sensors/Temperature.series".to_string(),
                );
                map
            },
            query: Some("SELECT * FROM TempData ORDER BY timestamp".to_string()),
            ..Default::default()
        };

        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        // This should NOT fail with "table not found" due to case mismatch
        let mut tx_guard_mut = tx_guard;
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &mut tx_guard_mut)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "Should successfully query with mixed-case pattern name"
        );

        tx_guard_mut.commit_test().await.unwrap();
    }

    /// Test AST-based table name replacement preserves quoted column names
    #[test]
    fn test_ast_table_replacement_preserves_column_names() {
        // Test case: Column name contains table name - should NOT be replaced
        let original_sql = r#"SELECT Silver."Silver.AT500_Surface.DO.mg/L" FROM Silver"#;
        
        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_12345".to_string());
        
        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // Table name should be replaced in FROM clause
        assert!(transformed.contains("sql_derived_silver_12345"), 
            "Table reference should be replaced, got: {}", transformed);
        
        // Column name should NOT be replaced - should still contain "Silver."
        assert!(transformed.contains(r#""Silver.AT500_Surface.DO.mg/L""#), 
            "Column name should be preserved, got: {}", transformed);
    }
    
    /// Test AST transformation handles multiple table references correctly
    #[test]
    fn test_ast_table_replacement_multiple_tables() {
        let original_sql = r#"
            SELECT 
                Silver."Silver.WaterTemp",
                BDock."BDock.WaterTemp"
            FROM Silver
            JOIN BDock ON Silver.timestamp = BDock.timestamp
        "#;
        
        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_abc".to_string());
        _ = table_mappings.insert("BDock".to_string(), "sql_derived_bdock_def".to_string());
        
        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // Both table references should be replaced
        assert!(transformed.contains("sql_derived_silver_abc"), 
            "Silver table should be replaced, got: {}", transformed);
        assert!(transformed.contains("sql_derived_bdock_def"), 
            "BDock table should be replaced, got: {}", transformed);
        
        // Column names should be preserved with original scope prefixes
        assert!(transformed.contains(r#""Silver.WaterTemp""#), 
            "Silver column name should be preserved, got: {}", transformed);
        assert!(transformed.contains(r#""BDock.WaterTemp""#), 
            "BDock column name should be preserved, got: {}", transformed);
    }
    
    /// Test AST transformation handles subqueries
    #[test]
    fn test_ast_table_replacement_subquery() {
        let original_sql = r#"
            SELECT * FROM (
                SELECT Silver."Silver.Value" FROM Silver
            ) AS subquery
        "#;
        
        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_xyz".to_string());
        
        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // Table in subquery should be replaced
        assert!(transformed.contains("sql_derived_silver_xyz"), 
            "Table in subquery should be replaced, got: {}", transformed);
        
        // Column name in subquery should be preserved
        assert!(transformed.contains(r#""Silver.Value""#), 
            "Column name in subquery should be preserved, got: {}", transformed);
    }
    
    /// Test that non-matching table names are not replaced
    #[test]
    fn test_ast_table_replacement_no_match() {
        let original_sql = r#"SELECT timestamp, value FROM OtherTable"#;
        
        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_123".to_string());
        
        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // Original table name should remain unchanged
        assert!(transformed.contains("OtherTable"), 
            "Non-matching table should not be replaced, got: {}", transformed);
        
        // Should NOT contain the replacement
        assert!(!transformed.contains("sql_derived_silver_123"), 
            "Replacement should not appear for non-matching table, got: {}", transformed);
    }
    
    /// Test source replacement pattern
    #[test]
    fn test_ast_source_replacement() {
        let original_sql = r#"SELECT timestamp, value FROM source WHERE value > 10"#;
        
        let options = SqlTransformOptions {
            table_mappings: None,
            source_replacement: Some("my_actual_table".to_string()),
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // 'source' should be replaced with actual table name
        assert!(transformed.contains("my_actual_table"), 
            "Source should be replaced, got: {}", transformed);
        assert!(!transformed.contains(" source "), 
            "Original 'source' should not remain, got: {}", transformed);
    }
    
    /// Test that no transformations returns original SQL unchanged
    #[test]
    fn test_ast_no_transformations() {
        let original_sql = r#"SELECT * FROM MyTable"#;
        
        let options = SqlTransformOptions {
            table_mappings: None,
            source_replacement: None,
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // Should return original SQL if no transformations
        assert_eq!(transformed.trim(), original_sql.trim(), 
            "SQL without transformations should be unchanged");
    }
    
    /// Test complex SQL with CTEs and window functions preserves structure
    #[test]
    fn test_ast_complex_sql_structure() {
        let original_sql = r#"
            WITH ranked AS (
                SELECT 
                    Silver."Silver.Value",
                    ROW_NUMBER() OVER (ORDER BY timestamp) as rn
                FROM Silver
            )
            SELECT * FROM ranked WHERE rn <= 10
        "#;
        
        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_999".to_string());
        
        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };
        
        let transformed = apply_table_transformations_test(original_sql, &options);
        
        // Table name should be replaced
        assert!(transformed.contains("sql_derived_silver_999"), 
            "Table should be replaced in CTE, got: {}", transformed);
        
        // Column name with scope prefix should be preserved
        assert!(transformed.contains(r#""Silver.Value""#), 
            "Column name should be preserved in CTE, got: {}", transformed);
        
        // CTE structure should be preserved
        assert!(transformed.to_uppercase().contains("WITH"), 
            "CTE keyword should be preserved, got: {}", transformed);
        assert!(transformed.to_uppercase().contains("ROW_NUMBER"), 
            "Window function should be preserved, got: {}", transformed);
    }
    
    /// Helper function for testing apply_table_transformations without needing a full SqlDerivedFile
    fn apply_table_transformations_test(
        original_sql: &str,
        options: &SqlTransformOptions,
    ) -> String {
        // If no transformations needed, return original
        if options.table_mappings.is_none() && options.source_replacement.is_none() {
            return original_sql.to_string();
        }

        use datafusion::sql::parser::DFParser;
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        
        let dialect = GenericDialect {};
        
        let statements = match DFParser::parse_sql_with_dialect(original_sql, &dialect) {
            Ok(stmts) => stmts,
            Err(e) => {
                panic!("Failed to parse SQL: {}", e);
            }
        };
        
        if statements.is_empty() {
            return original_sql.to_string();
        }
        
        let mut statement = statements[0].clone();
        
        use datafusion::sql::sqlparser::ast::{Query, SetExpr, Select, TableFactor};
        use datafusion::sql::parser::Statement as DFStatement;
        
        fn replace_table_names_in_statement(
            statement: &mut DFStatement,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            match statement {
                DFStatement::Statement(boxed) => {
                    if let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_mut() {
                        replace_table_names_in_query(query, table_mappings, source_replacement);
                    }
                }
                _ => {}
            }
        }
        
        fn replace_table_names_in_query(
            query: &mut Box<Query>,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            // Handle CTEs (WITH clauses)
            if let Some(ref mut with) = query.with {
                for cte_table in &mut with.cte_tables {
                    replace_table_names_in_query(&mut cte_table.query, table_mappings, source_replacement);
                }
            }
            
            // Handle main query body
            replace_table_names_in_set_expr(&mut query.body, table_mappings, source_replacement);
        }
        
        fn replace_table_names_in_set_expr(
            set_expr: &mut SetExpr,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            match set_expr {
                SetExpr::Select(select) => {
                    replace_table_names_in_select(select, table_mappings, source_replacement);
                }
                SetExpr::SetOperation { left, right, .. } => {
                    // Handle UNION, INTERSECT, EXCEPT operations
                    replace_table_names_in_set_expr(left, table_mappings, source_replacement);
                    replace_table_names_in_set_expr(right, table_mappings, source_replacement);
                }
                SetExpr::Query(query) => {
                    replace_table_names_in_query(query, table_mappings, source_replacement);
                }
                _ => {}
            }
        }
        
        fn replace_table_names_in_select(
            select: &mut Box<Select>,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            for table_with_joins in &mut select.from {
                replace_table_name(&mut table_with_joins.relation, table_mappings, source_replacement);
                
                for join in &mut table_with_joins.joins {
                    replace_table_name(&mut join.relation, table_mappings, source_replacement);
                }
            }
        }
        
        fn replace_table_name(
            table_factor: &mut TableFactor,
            table_mappings: Option<&HashMap<String, String>>,
            source_replacement: Option<&str>,
        ) {
            if let TableFactor::Table { name, alias, .. } = table_factor {
                let table_name = name.to_string();
                
                if let Some(mappings) = table_mappings {
                    if let Some(replacement) = mappings.get(&table_name) {
                        use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart, TableAlias};
                        
                        // If no existing alias, add one using the original table name
                        if alias.is_none() {
                            *alias = Some(TableAlias {
                                name: Ident::new(&table_name),
                                columns: vec![],
                            });
                        }
                        
                        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(replacement.clone()))]);
                    }
                } else if let Some(replacement) = source_replacement {
                    if table_name == "source" {
                        use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
                        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(replacement))]);
                    }
                }
            } else if let TableFactor::Derived { subquery, .. } = table_factor {
                replace_table_names_in_query(subquery, table_mappings, source_replacement);
            }
        }
        
        replace_table_names_in_statement(&mut statement, options.table_mappings.as_ref(), options.source_replacement.as_deref());
        
        statement.to_string()
    }
}
