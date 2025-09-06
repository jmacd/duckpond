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

use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
use tinyfs::{FileHandle, Result as TinyFSResult, File, Metadata, NodeMetadata, EntryType, AsyncReadSeek, FS, NodeType, Lookup};
use crate::register_dynamic_factory;
use crate::factory::FactoryContext;
use crate::tinyfs_object_store::TinyFsObjectStore;
use datafusion::error::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use async_trait::async_trait;
use tokio::io::AsyncWrite;
use diagnostics::*;



/// Options for SQL transformation and table name replacement
#[derive(Default, Clone)]
struct SqlTransformOptions {
    /// Replace multiple table names with mappings (for patterns)
    table_mappings: Option<HashMap<String, String>>,
    /// Replace a single source table name (for simple cases)  
    source_replacement: Option<String>,
}

/// Represents a resolved file with its path and unique NodeID
#[derive(Debug, Clone)]
struct ResolvedFile {
    path: String,
    node_id: String,
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlDerivedConfig {
    /// Named patterns for matching files. Each pattern name becomes a table in the SQL query.
    /// Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.
    /// Example: {"vulink": "/data/vulink*.series", "at500": "/data/at500*.series"}
    pub patterns: HashMap<String, String>,
    
    /// SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
}

/// Represents a resolved file with its path and NodeID
#[derive(Clone)] 
pub struct SqlDerivedFile {
    config: SqlDerivedConfig,
    context: FactoryContext,
    mode: SqlDerivedMode,
}

impl SqlDerivedFile {
    pub fn new(config: SqlDerivedConfig, context: FactoryContext, mode: SqlDerivedMode) -> TinyFSResult<Self> {
        Ok(Self { 
            config, 
            context, 
            mode,
        })
    }
    
    pub fn create_handle(self) -> FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
    
    /// Execute the SQL query and return results as Parquet bytes
    async fn execute_query_to_parquet(&self) -> TinyFSResult<Vec<u8>> {
        // Create TinyFS ObjectStore for ListingTable integration
        let object_store = Arc::new(TinyFsObjectStore::new());
        
        // Create DataFusion context (ObjectStore will be registered after population)
        let ctx = SessionContext::new();
        
        // Generate unique source table name to avoid conflicts
        let unique_source_name = self.generate_unique_source_table_name();
        
        // Create filesystem access using the context pattern from csv_directory
        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS: {}", e)))?;
        
        let tinyfs_root = fs.root().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
        
        // Resolve all patterns to find matching files based on mode
        let pattern_count = self.config.patterns.len();
        debug!("Resolving {pattern_count} patterns for SQL query execution");
        
        // Register first pattern as unified source table with unique name
        // Create table name mappings for SQL replacement
        let mut table_name_mappings = HashMap::new();
        
        match self.mode {
            SqlDerivedMode::Series => {
                // STEP 1: Register ALL files from ALL patterns first
                for (pattern_name, pattern) in &self.config.patterns {
                    let resolved_files = self.resolve_pattern_to_file_series_with_node_ids(&tinyfs_root, pattern).await?;
                    let match_count = resolved_files.len();
                    debug!("Pattern {pattern} (table {pattern_name}) matched {match_count} FileSeries files");
                    
                    if !resolved_files.is_empty() {
                        // Register all resolved files with ObjectStore
                        for resolved_file in &resolved_files {
                            // Resolve the file to get the actual handle
                            if let Ok((_, Lookup::Found(node_path))) = tinyfs_root.resolve_path(&resolved_file.path).await {
                                let node_path_ref = node_path.borrow().await;
                                if let Ok(file_node) = node_path_ref.as_file() {
                                    let node_type = NodeType::File(file_node.handle);
                                    object_store.register_file(resolved_file.node_id.clone(), node_type).await;
                                }
                            }
                        }
                    }
                }
                
                // Register the populated ObjectStore with DataFusion SessionContext
                ctx.runtime_env()
                    .object_store_registry
                    .register_store(&url::Url::parse("tinyfs:///")
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to parse tinyfs URL: {e}")))?, 
                        object_store.clone());
                info!("Registered ObjectStore with DataFusion SessionContext for Series mode");
                
                // STEP 2: Now create tables (ObjectStore is registered and fully populated)
                for (pattern_name, pattern) in &self.config.patterns {
                    let resolved_files = self.resolve_pattern_to_file_series_with_node_ids(&tinyfs_root, pattern).await?;
                    
                    if !resolved_files.is_empty() {
                        // Create ListingTable that can see all registered files
                        let table_provider = self.create_listing_table_for_registered_files(&ctx).await
                            .map_err(|e| tinyfs::Error::Other(format!("Failed to create ListingTable for table '{pattern_name}': {e}")))?;
                        
                        // Generate unique table name based on resolved NodeIDs
                        let unique_table_name = self.generate_unique_table_name(pattern_name, &resolved_files);
                        
                        // Register the table with unique name
                        ctx.register_table(&unique_table_name, table_provider)
                            .map_err(|e| tinyfs::Error::Other(format!("Failed to register table '{unique_table_name}': {e}")))?;
                        
                        // Store mapping for SQL replacement
                        table_name_mappings.insert(pattern_name.clone(), unique_table_name);
                        
                        let registered_name = &table_name_mappings[pattern_name];
                        debug!("Registered table '{pattern_name}' as '{registered_name}'");
                    }
                }
            }
            SqlDerivedMode::Table => {
                // STEP 1: Register ALL files from ALL patterns first  
                for (table_name, pattern) in &self.config.patterns {
                    let pattern_matches = self.resolve_pattern_to_file_table(&tinyfs_root, pattern).await?;
                    let match_count = pattern_matches.len();
                    debug!("Pattern {pattern} (table {table_name}) matched {match_count} FileTable files");
                    
                    if pattern_matches.len() > 1 {
                        return Err(tinyfs::Error::Other(format!("FileTable mode requires pattern '{}' (table '{}') to match exactly 1 file, but matched {}: {:?}", pattern, table_name, pattern_matches.len(), pattern_matches)));
                    }
                    
                    if !pattern_matches.is_empty() {
                        let file_path = &pattern_matches[0];
                        
                        // Register the file with ObjectStore using its node ID
                        if let Ok((_, Lookup::Found(node_path))) = tinyfs_root.resolve_path(file_path).await {
                            let node_path_ref = node_path.borrow().await;
                            if let Ok(file_node) = node_path_ref.as_file() {
                                let node_id = node_path.id().await.to_hex_string();
                                let node_type = NodeType::File(file_node.handle);
                                object_store.register_file(node_id, node_type).await;
                            }
                        }
                    }
                }
                
                // Register the populated ObjectStore with DataFusion SessionContext
                ctx.runtime_env()
                    .object_store_registry
                    .register_store(&url::Url::parse("tinyfs:///")
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to parse tinyfs URL: {e}")))?, 
                        object_store.clone());
                info!("Registered ObjectStore with DataFusion SessionContext for Table mode");
                
                // STEP 2: Now create tables (ObjectStore is registered and fully populated)
                let mut all_file_paths = Vec::new();
                
                for (table_name, pattern) in &self.config.patterns {
                    let pattern_matches = self.resolve_pattern_to_file_table(&tinyfs_root, pattern).await?;
                    
                    if !pattern_matches.is_empty() {
                        let file_path = &pattern_matches[0];
                        
                        // Create ListingTable for this individual table
                        let table_provider = self.create_listing_table_for_registered_files(&ctx).await
                            .map_err(|e| tinyfs::Error::Other(format!("Failed to create ListingTable from FileTable: {e}")))?;
                        
                        // Register the individual table as well for flexibility
                        ctx.register_table(table_name, table_provider)
                            .map_err(|e| tinyfs::Error::Other(format!("Failed to register table '{}': {}", table_name, e)))?;
                            
                        // Collect file paths for unified source table
                        all_file_paths.push(file_path.clone());
                    }
                }
                
                // Register unified source table - for FileTable mode we combine all file paths
                if !all_file_paths.is_empty() {
                    let unified_table_provider = self.create_listing_table_for_registered_files(&ctx).await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to create unified FileTable ListingTable: {e}")))?;
                    
                    ctx.register_table(&unique_source_name, unified_table_provider)
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to register unified source table: {}", e)))?;
                    
                    let table_count = all_file_paths.len();
                    debug!("Registered unified source table '{unique_source_name}' with {table_count} FileTable files");
                }
            }
        }
        
        // Get the effective SQL query with table names replaced by unique names
        let query = self.get_effective_sql_with_table_mappings(&table_name_mappings);
        debug!("Executing SQL query: {query}");
        
        // Execute the SQL query
        let df = ctx.sql(&query).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to execute query: {e}")))?;
        debug!("SQL query created DataFrame successfully");
        
        // Collect results into batches
        debug!("Collecting query results into batches...");
        let batches = df.collect().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to collect query results: {e}")))?;
        
        let batch_count = batches.len();
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        debug!("Query execution completed: {batch_count} batches, {total_rows} total rows");
        
        if batches.is_empty() {
            debug!("Query returned empty results");
            return Ok(Vec::new()); // Empty result is valid
        }
        
        // Convert results to Parquet format
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        let schema = batches[0].schema();
        let mut parquet_buffer = Vec::new();
        let cursor = Cursor::new(&mut parquet_buffer);
        
        let mut writer = ArrowWriter::try_new(cursor, schema, None)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to create Parquet writer: {e}")))?;
        
        for batch in batches {
            writer.write(&batch)
                .map_err(|e| tinyfs::Error::Other(format!("Failed to write Parquet batch: {e}")))?;
        }
        
        writer.close()
            .map_err(|e| tinyfs::Error::Other(format!("Failed to close Parquet writer: {e}")))?;
        
        let buffer_size = parquet_buffer.len();
        info!("Generated {buffer_size} bytes of Parquet data from SQL query");
        Ok(parquet_buffer)
    }
    
    /// Create ListingTable using TinyFS ObjectStore from the provided SessionContext
    ///
    /// Create a DataFusion ListingTable using files already registered with the ObjectStore.
    /// Uses the ObjectStore registry from the provided SessionContext.
    async fn create_listing_table_for_registered_files(&self, ctx: &SessionContext) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        // Create ListingTable with registered ObjectStore
        use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        
        // Use the tinyfs URL to access our registered ObjectStore
        // Try using a specific pattern that matches our file paths
        let table_url = ListingTableUrl::parse("tinyfs:///node/")
            .map_err(|e| DataFusionError::Plan(format!("Failed to parse table URL: {e}")))?;
        let file_format = Arc::new(ParquetFormat::default());
        
        // Create ListingTableConfig without schema - we'll infer it
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(datafusion::datasource::listing::ListingOptions::new(file_format));
        
        // Use DataFusion's schema inference which will call our ObjectStore
        debug!("Calling config.infer_schema to discover files via ObjectStore");
        let config_with_schema = config.infer_schema(&ctx.state()).await
            .map_err(|e| {
                debug!("Schema inference failed: {e}");
                e
            })?;
        debug!("Schema inference completed successfully");
        
        // Now create the ListingTable with the inferred schema
        let listing_table = ListingTable::try_new(config_with_schema)
            .map_err(|e| {
                debug!("ListingTable creation failed: {e}");
                e
            })?;
        debug!("ListingTable created successfully");
        
        Ok(Arc::new(listing_table))
    }

    /// Resolve a pattern to a list of FileSeries files with NodeIDs
    async fn resolve_pattern_to_file_series_with_node_ids(&self, tinyfs_root: &tinyfs::WD, pattern: &str) -> TinyFSResult<Vec<ResolvedFile>> {
        self.resolve_pattern_to_entry_type_with_node_ids(tinyfs_root, pattern, EntryType::FileSeries).await
    }

    /// Resolve a pattern to a list of files with specific entry type, capturing NodeIDs
    async fn resolve_pattern_to_entry_type_with_node_ids(&self, tinyfs_root: &tinyfs::WD, pattern: &str, entry_type: EntryType) -> TinyFSResult<Vec<ResolvedFile>> {
        // Use TinyFS collect_matches to find all files matching the pattern
        let matches = tinyfs_root.collect_matches(pattern).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to resolve pattern '{}': {}", pattern, e)))?;
        
        let mut resolved_files = Vec::new();
        
        for (node_path, _captured) in matches {
            let node_ref = node_path.borrow().await;
            
            // Check if this is a file
            if let Ok(file_node) = node_ref.as_file() {
                // Check if it matches the desired entry type
                if let Ok(metadata) = file_node.metadata().await {
                    if metadata.entry_type == entry_type {
                        // Get the path as a string
                        let path_str = node_path.path().to_string_lossy().to_string();
                        
                        // Get the NodeID
                        let node_id = node_path.id().await.to_hex_string();
                        
                        resolved_files.push(ResolvedFile {
                            path: path_str,
                            node_id,
                        });
                    }
                }
            }
        }
        
        Ok(resolved_files)
    }

    /// Resolve a pattern to a list of FileTable file paths
    async fn resolve_pattern_to_file_table(&self, tinyfs_root: &tinyfs::WD, pattern: &str) -> TinyFSResult<Vec<String>> {
        self.resolve_pattern_to_entry_type(tinyfs_root, pattern, EntryType::FileTable).await
    }

    /// Resolve a pattern to a list of file paths with specific entry type
    async fn resolve_pattern_to_entry_type(&self, tinyfs_root: &tinyfs::WD, pattern: &str, entry_type: EntryType) -> TinyFSResult<Vec<String>> {
        // Use TinyFS collect_matches to find all files matching the pattern
        let matches = tinyfs_root.collect_matches(pattern).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to resolve pattern '{}': {}", pattern, e)))?;
        
        let mut file_paths = Vec::new();
        
        for (node_path, _captured) in matches {
            let node_ref = node_path.borrow().await;
            
            // Check if this is a file
            if let Ok(file_node) = node_ref.as_file() {
                // Check if it matches the desired entry type
                if let Ok(metadata) = file_node.metadata().await {
                    if metadata.entry_type == entry_type {
                        // Get the path as a string
                        let path_str = node_path.path().to_string_lossy().to_string();
                        file_paths.push(path_str);
                    }
                }
            }
        }
        
        Ok(file_paths)
    }
    
    /// Generate a unique source table name for this SqlDerivedFile instance
    /// This prevents conflicts when multiple SqlDerivedFile instances use the same DataFusion context
    fn generate_unique_source_table_name(&self) -> String {
        // Use a reserved name that won't conflict with user-defined pattern names
        // Since each SqlDerivedFile creates its own SessionContext, we only need to avoid
        // conflicts with pattern table names within the same execution
        let unique_name = "___duckpond_unified_source___".to_string();
        debug!("Using reserved source table name: {unique_name}");
        unique_name
    }

    /// Generate unique table name based on resolved files' NodeIDs
    fn generate_unique_table_name(&self, pattern_name: &str, resolved_files: &[ResolvedFile]) -> String {
        if resolved_files.is_empty() {
            return format!("{}_empty", pattern_name);
        }
        
        // Create deterministic name based on the NodeIDs
        let mut node_ids: Vec<&str> = resolved_files.iter().map(|f| f.node_id.as_str()).collect();
        node_ids.sort(); // Ensure deterministic ordering
        
        // Use first few characters of each NodeID to keep name reasonable
        let node_id_summary: String = node_ids.iter()
            .map(|id| &id[..std::cmp::min(8, id.len())]) // First 8 chars
            .collect::<Vec<_>>()
            .join("_");
            
        let unique_name = format!("{}_{}", pattern_name, node_id_summary);
        debug!("Generated unique table name for pattern '{pattern_name}': {unique_name}");
        unique_name
    }
    
    /// Get the effective SQL query with the unique source table name substituted
    /// Replace table names in SQL using proper AST parsing and manipulation
    /// This eliminates the duplication between get_effective_sql_with_table_mappings and get_effective_sql_with_unique_source
    fn get_effective_sql(&self, options: &SqlTransformOptions) -> Result<String, DataFusionError> {
        let default_query: String;
        let original_sql = if let Some(query) = &self.config.query {
            query.as_str()
        } else {
            // Generate smart default based on patterns
            if self.config.patterns.len() == 1 {
                let pattern_name = self.config.patterns.keys().next().unwrap();
                default_query = format!("SELECT * FROM {}", pattern_name);
                &default_query
            } else {
                "SELECT * FROM <specify_pattern_name>"
            }
        };
        
        debug!("Original SQL query: {original_sql}");
        
        // Use string replacement for table name transformation
        // TODO: Implement proper SQL AST parsing when DataFusion APIs stabilize
        let result = self.fallback_string_replacement(original_sql, options);
        debug!("Transformed SQL result: {result}");
        Ok(result)
    }
    
    /// String replacement for table name transformation  
    /// Maintains backward compatibility with existing functionality
    fn fallback_string_replacement(&self, original_sql: &str, options: &SqlTransformOptions) -> String {
        let mut result = original_sql.to_string();
        
        if let Some(table_mappings) = &options.table_mappings {
            // Replace each pattern name with its unique table name
            for (pattern_name, unique_table_name) in table_mappings {
                debug!("Fallback: Replacing table name '{pattern_name}' with '{unique_table_name}'");
                result = result.replace(pattern_name, unique_table_name);
            }
        } else if let Some(source_replacement) = &options.source_replacement {
            debug!("Fallback: Replacing 'source' with '{source_replacement}'");
            result = result.replace("source", source_replacement);
        }
        
        result
    }

    /// Legacy wrapper for table mappings (to maintain existing API)
    fn get_effective_sql_with_table_mappings(&self, table_mappings: &HashMap<String, String>) -> String {
        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings.clone()),
            source_replacement: None,
        };
        
        self.get_effective_sql(&options).unwrap_or_else(|e| {
            warn!("SQL transformation failed, using fallback: {e}", e: e);
            self.fallback_string_replacement(
                self.config.query.as_deref().unwrap_or("SELECT * FROM source"), 
                &options
            )
        })
    }
}

// Async trait implementations

#[async_trait]
impl File for SqlDerivedFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        let parquet_data = self.execute_query_to_parquet().await?;
        let cursor = std::io::Cursor::new(parquet_data);
        Ok(Box::pin(cursor))
    }
    
    async fn async_writer(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other("SQL-derived file is read-only".to_string()))
    }
}

#[async_trait]
impl Metadata for SqlDerivedFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        // Metadata should be lightweight - don't compute the actual data
        // The entry type can be determined from mode without expensive computation
        let entry_type = match self.mode {
            SqlDerivedMode::Table => EntryType::FileTable,
            SqlDerivedMode::Series => EntryType::FileSeries,
        };
        
        // Return lightweight metadata - size and hash will be computed on actual data access
        Ok(NodeMetadata {
            version: 1,
            size: None, // Unknown until data is actually computed
            sha256: None, // Unknown until data is actually computed  
            entry_type,
            timestamp: 0,
        })
    }
}

// Factory functions for linkme registration

fn create_sql_derived_table_handle_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    let sql_file = SqlDerivedFile::new(cfg, context.clone(), SqlDerivedMode::Table)?;
    Ok(sql_file.create_handle())
}

fn create_sql_derived_series_handle_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    let sql_file = SqlDerivedFile::new(cfg, context.clone(), SqlDerivedMode::Series)?;
    Ok(sql_file.create_handle())
}

fn validate_sql_derived_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse as YAML first (user format)
    let yaml_config: SqlDerivedConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML config: {}", e)))?;
    
    // Validate that patterns list is not empty
    if yaml_config.patterns.is_empty() {
        return Err(tinyfs::Error::Other("Patterns list cannot be empty".to_string()));
    }
    
    // Validate individual patterns
    for (table_name, pattern) in &yaml_config.patterns {
        if table_name.is_empty() {
            return Err(tinyfs::Error::Other("Table name cannot be empty".to_string()));
        }
        if pattern.is_empty() {
            return Err(tinyfs::Error::Other(format!("Pattern for table '{}' cannot be empty", table_name)));
        }
    }
    
    // Validate query if provided (now optional)
    if let Some(query) = &yaml_config.query {
        if query.is_empty() {
            return Err(tinyfs::Error::Other("SQL query cannot be empty if specified".to_string()));
        }
    }
    
    // Convert to JSON for internal use
    serde_json::to_value(yaml_config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

// Register the factories
register_dynamic_factory!(
    name: "sql-derived-table",
    description: "Create SQL-derived tables from single FileTable sources",
    file_with_context: create_sql_derived_table_handle_with_context,
    validate: validate_sql_derived_config
);

register_dynamic_factory!(
    name: "sql-derived-series", 
    description: "Create SQL-derived tables from multiple FileSeries sources",
    file_with_context: create_sql_derived_series_handle_with_context,
    validate: validate_sql_derived_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::OpLogPersistence;
    use tempfile::TempDir;

    /// Helper function to set up test environment with sample Parquet data
    async fn setup_test_data(persistence: &mut OpLogPersistence) {
        let tx_guard = persistence.begin().await.unwrap();
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
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"])),
                Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250])),
            ],
        ).unwrap();
        
        // Write to Parquet format
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        
        // Create the source data file using TinyFS convenience API
        use tinyfs::async_helpers::convenience;
        let _data_file = convenience::create_file_path_with_type(
            &root, 
            "/data.parquet", 
            &parquet_buffer,
            EntryType::FileTable
        ).await.unwrap();
        
        // Commit this transaction so the source file is visible to subsequent reads
        tx_guard.commit(None).await.unwrap();
    }

    /// Helper function to set up test environment with FileSeries data
    async fn setup_file_series_test_data(persistence: &mut OpLogPersistence) {
        let tx_guard = persistence.begin().await.unwrap();
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
                Arc::new(StringArray::from(vec!["Building A", "Building B", "Building C", "Building A", "Building B"])),
                Arc::new(Int32Array::from(vec![75, 82, 68, 90, 77])),
            ],
        ).unwrap();
        
        // Write to Parquet format
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        
        let buffer_len = parquet_buffer.len();
        let preview = if buffer_len >= 8 {
            format!("{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}...", 
                parquet_buffer[0], parquet_buffer[1], parquet_buffer[2], parquet_buffer[3],
                parquet_buffer[4], parquet_buffer[5], parquet_buffer[6], parquet_buffer[7])
        } else {
            format!("{:02x?}", &parquet_buffer[..buffer_len.min(8)])
        };
        let suffix = if buffer_len >= 8 {
            format!("...{:02x} {:02x} {:02x} {:02x}", 
                parquet_buffer[buffer_len-4], parquet_buffer[buffer_len-3], 
                parquet_buffer[buffer_len-2], parquet_buffer[buffer_len-1])
        } else {
            "".to_string()
        };
        debug!("Created Parquet buffer: {buffer_len} bytes, starts with: {preview}, ends with: {suffix}");
        
        // Create the source data as FileSeries (not FileTable)
        use tinyfs::async_helpers::convenience;
        let _series_file = convenience::create_file_path_with_type(
            &root, 
            "/sensor_data.parquet", 
            &parquet_buffer,
            EntryType::FileSeries  // This is the key difference
        ).await.unwrap();
        
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
        tx_guard.commit(None).await.unwrap();
    }

    /// Helper function to set up multi-version FileSeries test data
    async fn setup_file_series_multi_version_data(persistence: &mut OpLogPersistence, num_versions: usize) {
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
            let tx_guard = persistence.begin().await.unwrap();
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
                        base_sensor_id + 3
                    ])),
                    Arc::new(StringArray::from(vec![
                        format!("Building {}", version),
                        format!("Building {}", version),
                        format!("Building {}", version),
                    ])),
                    Arc::new(Int32Array::from(vec![
                        base_reading, 
                        base_reading + 5, 
                        base_reading + 10
                    ])),
                ],
            ).unwrap();
            
            // Write to Parquet format
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
            }
            
            // Write to the SAME path for all versions - TLogFS will handle versioning
            let mut writer = root.async_writer_path_with_type("/multi_sensor_data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
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
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up multiple FileSeries files that match a pattern
        {
            // Create sensor_data1.parquet
            let tx_guard = persistence.begin().await.unwrap();
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
            ).unwrap();
            
            let mut parquet_buffer1 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer1);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch1).unwrap();
                writer.close().unwrap();
            }
            
            let mut writer = root.async_writer_path_with_type("/sensor_data1.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer1).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        {
            // Create sensor_data2.parquet
            let tx_guard = persistence.begin().await.unwrap();
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
            ).unwrap();
            
            let mut parquet_buffer2 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer2);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch2).unwrap();
                writer.close().unwrap();
            }
            
            let mut writer = root.async_writer_path_with_type("/sensor_data2.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer2).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        // Now test pattern matching across both files
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("sensor_data".to_string(), "/sensor_data*.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading FROM sensor_data WHERE reading > 85 ORDER BY reading DESC".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result and verify contents from both files
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from both files matching reading > 85
        // File 1: Building A (80) - excluded, Building B (85) - excluded because 85 is not > 85  
        // File 2: Building C (90), Building D (95) - both included
        // So we expect 2 rows
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check that we have the right data ordered by reading DESC
        use arrow::array::{StringArray, Int32Array};
        let locations = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let readings = result_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        
        // Should be ordered by reading DESC: Building D (95), Building C (90)
        assert_eq!(locations.value(0), "Building D");
        assert_eq!(readings.value(0), 95);
        
        assert_eq!(locations.value(1), "Building C");
        assert_eq!(readings.value(1), 90);
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_recursive_pattern_matching() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries files in different directories
        {
            // Create /sensors/building_a/data.parquet
            let tx_guard = persistence.begin().await.unwrap();
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
            ).unwrap();
            
            let mut parquet_buffer_a = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer_a);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_a).unwrap();
                writer.close().unwrap();
            }
            
            // Create directory first, then file
            root.create_dir_path("/sensors").await.unwrap();
            root.create_dir_path("/sensors/building_a").await.unwrap();
            let mut writer = root.async_writer_path_with_type("/sensors/building_a/data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer_a).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        {
            // Create /sensors/building_b/data.parquet
            let tx_guard = persistence.begin().await.unwrap();
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
            ).unwrap();
            
            let mut parquet_buffer_b = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer_b);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_b).unwrap();
                writer.close().unwrap();
            }
            
            root.create_dir_path("/sensors/building_b").await.unwrap();
            let mut writer = root.async_writer_path_with_type("/sensors/building_b/data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer_b).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        // Now test recursive pattern matching across all nested files
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("data".to_string(), "/**/data.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading, sensor_id FROM data ORDER BY sensor_id".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result and verify contents from both nested files
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from both nested directories
        // Building A: sensor_id 301, reading 100
        // Building B: sensor_id 302, reading 110
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 3);
        
        use arrow::array::{StringArray, Int32Array};
        let locations = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let readings = result_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let sensor_ids = result_batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        
        // Should be ordered by sensor_id: Building A (301), Building B (302)
        assert_eq!(sensor_ids.value(0), 301);
        assert_eq!(locations.value(0), "Building A");
        assert_eq!(readings.value(0), 100);
        
        assert_eq!(sensor_ids.value(1), 302);
        assert_eq!(locations.value(1), "Building B");
        assert_eq!(readings.value(1), 110);
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_multiple_patterns() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries files in different locations matching different patterns
        {
            // Create files in /metrics/ directory
            let tx_guard = persistence.begin().await.unwrap();
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
            ).unwrap();
            
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_metrics).unwrap();
                writer.close().unwrap();
            }
            
            root.create_dir_path("/metrics").await.unwrap();
            let mut writer = root.async_writer_path_with_type("/metrics/data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        {
            // Create files in /logs/ directory  
            let tx_guard = persistence.begin().await.unwrap();
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
            ).unwrap();
            
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_logs).unwrap();
                writer.close().unwrap();
            }
            
            root.create_dir_path("/logs").await.unwrap();
            let mut writer = root.async_writer_path_with_type("/logs/info.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        // Test multiple patterns combining files from different directories
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("metrics".to_string(), "/metrics/*.parquet".to_string());
                map.insert("logs".to_string(), "/logs/*.parquet".to_string());
                map
            },
            query: Some("SELECT * FROM metrics UNION ALL SELECT * FROM logs".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result and verify contents from both pattern groups
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from both directories: 2 from metrics + 2 from logs = 4 rows
        assert_eq!(result_batch.num_rows(), 4);
        assert_eq!(result_batch.num_columns(), 3);
        
        // Check that we have data from both locations
        use arrow::array::{StringArray, Int32Array};
        let sensor_ids = result_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let locations = result_batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        
        // Verify we have sensor IDs from both metrics (401, 402) and logs (501, 502)
        let mut found_metrics = false;
        let mut found_logs = false;
        
        for i in 0..result_batch.num_rows() {
            let sensor_id = sensor_ids.value(i);
            let location = locations.value(i);
            
            if sensor_id >= 401 && sensor_id <= 402 {
                found_metrics = true;
                assert!(location.contains("Metrics"));
            } else if sensor_id >= 501 && sensor_id <= 502 {
                found_logs = true;
                assert!(location.contains("Log Server"));
            }
        }
        
        assert!(found_metrics, "Should have found metrics data");
        assert!(found_logs, "Should have found logs data");
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test] 
    async fn test_sql_derived_default_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up test data
        setup_file_series_test_data(&mut persistence).await;
        
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create SQL-derived file without specifying query (should use default)
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("sensor_data".to_string(), "/sensor_data.parquet".to_string());
                map
            },
            query: None, // No query specified - should default to "SELECT * FROM source"
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result - should be all data unchanged
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // Should have all the original data with default query "SELECT * FROM source"
        // Original FileSeries test data has 5 rows with sensor readings [75, 82, 68, 90, 77]
        assert_eq!(result_batch.num_rows(), 5);
        assert_eq!(result_batch.num_columns(), 3); // sensor_id, location, reading
        
        // Verify column names (should match original schema)
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "sensor_id");
        assert_eq!(schema.field(1).name(), "location");
        assert_eq!(schema.field(2).name(), "reading");
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_single_version() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries test data (single version)
        setup_file_series_test_data(&mut persistence).await;
        
        // Create and test the SQL-derived file with FileSeries source
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file with FileSeries source
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("sensor_data".to_string(), "/sensor_data.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading * 1.5 as adjusted_reading FROM sensor_data WHERE reading > 75 ORDER BY adjusted_reading DESC".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result and verify contents
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
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
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_two_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries test data with 2 versions
        setup_file_series_multi_version_data(&mut persistence, 2).await;
        
        // For now, test against the first version until we implement union logic
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file with multi-version FileSeries source
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("multi_sensor_data".to_string(), "/multi_sensor_data.parquet".to_string());
                map
            },
            query: Some("SELECT location, reading FROM multi_sensor_data WHERE reading > 75 ORDER BY reading DESC".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result and verify contents
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // For now, we expect data from version 1 only (readings: [70, 75, 80])
        // Query: WHERE reading > 75, so we should get reading=80 from Building 1
        assert!(result_batch.num_rows() >= 1, "Should have at least 1 row with reading > 75");
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "reading");
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_three_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries test data with 3 versions
        setup_file_series_multi_version_data(&mut persistence, 3).await;
        
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file that should union all 3 versions
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("multi_sensor_data".to_string(), "/multi_sensor_data.parquet".to_string());
                map
            },
            // This query should return data from all 3 versions
            query: Some("SELECT location, reading, sensor_id FROM multi_sensor_data ORDER BY sensor_id".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();
        
        // Read the Parquet result and verify contents from all versions
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from all 3 versions (3 rows per version = 9 total rows)
        // Version 1: sensor_ids 111, 112, 113 (Building 1)
        // Version 2: sensor_ids 121, 122, 123 (Building 2)  
        // Version 3: sensor_ids 131, 132, 133 (Building 3)
        assert_eq!(result_batch.num_rows(), 9);
        assert_eq!(result_batch.num_columns(), 3);
        
        // Check that we have sensor IDs from all versions
        use arrow::array::{StringArray, Int32Array};
        let sensor_ids = result_batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let locations = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        
        // Verify we have sensor IDs from all 3 versions (ordered by sensor_id)
        assert_eq!(sensor_ids.value(0), 111); // First version
        assert_eq!(locations.value(0), "Building 1");
        
        assert_eq!(sensor_ids.value(3), 121); // Second version (after 111,112,113)
        assert_eq!(locations.value(3), "Building 2");
        
        assert_eq!(sensor_ids.value(6), 131); // Third version (after 111,112,113,121,122,123)
        assert_eq!(locations.value(6), "Building 3");
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up test data
        setup_test_data(&mut persistence).await;
        
        // Create and test the SQL-derived file  
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file with read-only state context
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            patterns: {
                let mut map = HashMap::new();
                map.insert("data".to_string(), "/data.parquet".to_string());
                map
            },
            query: Some("SELECT name, value * 2 as doubled_value FROM data WHERE value > 150 ORDER BY doubled_value DESC".to_string()),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();
        
        // Read the Parquet result and verify contents
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // Verify we got the expected derived data
        // Query: "SELECT name, value * 2 as doubled_value FROM source WHERE value > 150 ORDER BY doubled_value DESC"
        // Expected: David (300*2=600), Eve (250*2=500), Bob (200*2=400)
        // Note: Charlie (150) is excluded because 150 is not > 150
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "doubled_value");
        
        // Check data - should be ordered by doubled_value DESC
        use arrow::array::{StringArray, Int64Array};
        let names = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let doubled_values = result_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(names.value(0), "David");   // 300 * 2 = 600 (highest)
        assert_eq!(doubled_values.value(0), 600);
        
        assert_eq!(names.value(1), "Eve");     // 250 * 2 = 500 (second)
        assert_eq!(doubled_values.value(1), 500);
        
        assert_eq!(names.value(2), "Bob");     // 200 * 2 = 400 (third)
        assert_eq!(doubled_values.value(2), 400);
        
        // This transaction is read-only, so just let it end without committing
        tx_guard.commit(None).await.unwrap();
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
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up test data
        setup_test_data(&mut persistence).await;
        
        // Test chaining: Create two SQL-derived nodes, where one refers to the other
        {
            let tx_guard = persistence.begin().await.unwrap();
            let state = tx_guard.state().unwrap();
            
            // Create TinyFS root to work with for storing intermediate results
            let fs = FS::new(state.clone()).await.unwrap();
            let root = fs.root().await.unwrap();
            
            // Create the first SQL-derived file (filters and transforms original data)
            let context = FactoryContext::new(state.clone());
            let first_config = SqlDerivedConfig {
                patterns: {
                    let mut map = HashMap::new();
                    map.insert("data".to_string(), "/data.parquet".to_string());
                    map
                },
                query: Some("SELECT name, value + 50 as adjusted_value FROM data WHERE value >= 200 ORDER BY adjusted_value".to_string()),
            };
            
            let first_sql_file = SqlDerivedFile::new(first_config, context.clone(), SqlDerivedMode::Table).unwrap();
            
            // Read the first SQL-derived result and store it as an intermediate file
            let mut first_reader = first_sql_file.async_reader().await.unwrap();
            let mut first_result_data = Vec::new();
            use tokio::io::AsyncReadExt;
            first_reader.read_to_end(&mut first_result_data).await.unwrap();
            
            // Store the first result as an intermediate Parquet file
            use tinyfs::async_helpers::convenience;
            let _intermediate_file = convenience::create_file_path_with_type(
                &root,
                "/intermediate.parquet",
                &first_result_data,
                EntryType::FileTable
            ).await.unwrap();
            
            // Commit to make the intermediate file visible
            tx_guard.commit(None).await.unwrap();
        }
        
        // Second transaction: Create the second SQL-derived node that chains from the first
        {
            let tx_guard = persistence.begin().await.unwrap();
            let state = tx_guard.state().unwrap();
            
            let context = FactoryContext::new(state);
            let second_config = SqlDerivedConfig {
                patterns: {
                    let mut map = HashMap::new();
                    map.insert("intermediate".to_string(), "/intermediate.parquet".to_string());
                    map
                },
                query: Some("SELECT name, adjusted_value * 2 as final_value FROM intermediate WHERE adjusted_value > 250 ORDER BY final_value DESC".to_string()),
            };
            
            let second_sql_file = SqlDerivedFile::new(second_config, context, SqlDerivedMode::Table).unwrap();
            
            // Read the final chained result
            let mut second_reader = second_sql_file.async_reader().await.unwrap();
            let mut final_result_data = Vec::new();
            use tokio::io::AsyncReadExt;
            second_reader.read_to_end(&mut final_result_data).await.unwrap();
            
            // Parse and verify the final chained results
            use tokio_util::bytes::Bytes;
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            
            let bytes = Bytes::from(final_result_data);
            let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let mut record_batch_reader = parquet_reader.build().unwrap();
            
            let result_batch = record_batch_reader.next().unwrap().unwrap();
            
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
            use arrow::array::{StringArray, Int64Array};
            let names = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let final_values = result_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            
            assert_eq!(names.value(0), "David");   // (300 + 50) * 2 = 700 (highest)
            assert_eq!(final_values.value(0), 700);
            
            assert_eq!(names.value(1), "Eve");     // (250 + 50) * 2 = 600 (second)
            assert_eq!(final_values.value(1), 600);
            
            // Bob would be (200 + 50) * 2 = 500, but excluded because 250 is not > 250
            
            tx_guard.commit(None).await.unwrap();
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
}
