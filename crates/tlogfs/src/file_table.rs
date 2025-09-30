/// File-Table Duality Integration for TinyFS and DataFusion
/// 
/// This module implements the FileTable trait that allows structured files
/// to expose both file-oriented and table-oriented interfaces.

use crate::error::TLogFSError;

// TinyFsObjectStore only used in register function now
use arrow::datatypes::{SchemaRef, DataType, TimeUnit};
use std::sync::Arc;

// DataFusion imports
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result as DataFusionResult, Constraints, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{TableProviderFilterPushDown, Expr};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl, ListingOptions};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::logical_expr::Operator;
use datafusion::execution::context::SessionContext;
use datafusion::common::DataFusionError;
use std::any::Any;
use log::debug;

/// Version selection for ListingTable
#[derive(Clone, Debug)]
pub enum VersionSelection {
    /// All versions (replaces SeriesTable) 
    AllVersions,
    /// Latest version only (replaces TinyFsTableProvider)
    LatestVersion,
    /// Specific version (replaces NodeVersionTable)
    SpecificVersion(u64),
}

impl VersionSelection {
    /// Centralized debug logging for version selection
    /// Eliminates duplicate debug logging patterns throughout the codebase
    pub fn log_debug(&self, node_id: &tinyfs::NodeID) {
        match self {
            VersionSelection::AllVersions => {
                debug!("Version selection: ALL versions for node {node_id}");
            },
            VersionSelection::LatestVersion => {  
                debug!("Version selection: LATEST version for node {node_id}");
            },
            VersionSelection::SpecificVersion(version) => {
                debug!("Version selection: SPECIFIC version {version} for node {node_id}");
            }
        }
    }
    
    /// Generate URL pattern for this version selection
    /// Eliminates duplicate URL pattern generation throughout the codebase
    pub fn to_url_pattern(&self, part_id: &tinyfs::NodeID, node_id: &tinyfs::NodeID) -> String {
        match self {
            VersionSelection::AllVersions | VersionSelection::LatestVersion => {
                crate::tinyfs_object_store::TinyFsPathBuilder::url_all_versions(part_id, node_id)
            },
            VersionSelection::SpecificVersion(version) => {
                crate::tinyfs_object_store::TinyFsPathBuilder::url_specific_version(part_id, node_id, *version)
            }
        }
    }
}

/// Wrapper that applies temporal filtering to a ListingTable
pub struct TemporalFilteredListingTable {
    listing_table: ListingTable,
    min_time: i64,
    max_time: i64,
}

impl TemporalFilteredListingTable {
    pub fn new(listing_table: ListingTable, min_time: i64, max_time: i64) -> Self {
        Self {
            listing_table,
            min_time,
            max_time,
        }
    }
    
    fn apply_temporal_filter_to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        min_seconds: i64,
        max_seconds: i64,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = plan.schema();
        
        // Find timestamp column index
        let timestamp_col_index = schema
            .fields()
            .iter()
            .position(|f| f.name() == "timestamp")
            .ok_or_else(|| {
                DataFusionError::Plan("Timestamp column not found in schema".to_string())
            })?;

        // Check the timestamp column's timezone
        let timestamp_field = &schema.fields()[timestamp_col_index];
        let timestamp_timezone = match timestamp_field.data_type() {
            DataType::Timestamp(TimeUnit::Second, tz) => tz.clone(),
            _ => return Err(DataFusionError::Plan("Expected timestamp column with second precision".to_string())),
        };

        // Create temporal bound expressions with matching timezone
        let min_timestamp = Arc::new(Literal::new(ScalarValue::TimestampSecond(
            Some(min_seconds),
            timestamp_timezone.clone(),
        )));
        
        let max_timestamp = Arc::new(Literal::new(ScalarValue::TimestampSecond(
            Some(max_seconds),
            timestamp_timezone,
        )));

        // Create column reference for timestamp
        let timestamp_col = Arc::new(Column::new("timestamp", timestamp_col_index));

        // Create filter expressions: timestamp >= min_timestamp AND timestamp <= max_timestamp
        let min_filter = Arc::new(BinaryExpr::new(
            timestamp_col.clone(),
            Operator::GtEq,
            min_timestamp,
        ));

        let max_filter = Arc::new(BinaryExpr::new(
            timestamp_col,
            Operator::LtEq,
            max_timestamp,
        ));

        let combined_filter = Arc::new(BinaryExpr::new(
            min_filter,
            Operator::And,
            max_filter,
        ));

        // Apply filter to the plan
        let filter_exec = FilterExec::try_new(combined_filter, plan)?;
        Ok(Arc::new(filter_exec))
    }
}

impl std::fmt::Debug for TemporalFilteredListingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalFilteredListingTable")
            .field("listing_table", &"<ListingTable>")
            .field("min_time", &self.min_time)
            .field("max_time", &self.max_time)
            .field("session_context", &"<SessionContext>")
            .finish()
    }
}

#[async_trait]
impl TableProvider for TemporalFilteredListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = self.listing_table.schema();
        let field_count = schema.fields().len();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let field_names_str = field_names.join(", ");
        debug!("🔍 TemporalFilteredListingTable.schema() called - returning {field_count} fields: [{field_names_str}]");
        schema
    }

    fn table_type(&self) -> TableType {
        self.listing_table.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.listing_table.constraints()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.listing_table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        debug!("🚨 TemporalFilteredListingTable.scan() called - temporal filtering is active!");
        
        // Convert from milliseconds to seconds for HydroVu data
        let min_seconds = self.min_time / 1000;
        let max_seconds = self.max_time / 1000;
        
        debug!("⚡ Temporal filtering range: {min_seconds} to {max_seconds} (seconds)");
        
        // Use the provided session state (not our internal one) to avoid schema mismatches
        debug!("🔍 Using provided SessionState instead of internal one...");
        
        // Check if we actually need to apply temporal filtering
        if self.min_time == i64::MIN && self.max_time == i64::MAX {
            debug!("⚡ No temporal bounds - delegating to base ListingTable");
            return self.listing_table.scan(state, projection, filters, limit).await;
        }
        
        // Check if this is an empty projection (COUNT case)
        let is_empty_projection = projection.as_ref().map_or(false, |p| p.is_empty());
        
        if is_empty_projection {
            debug!("📊 Empty projection detected (COUNT query) - need to include timestamp for filtering");
            
            // For temporal filtering with empty projection, we need to:
            // 1. Scan with timestamp column included
            // 2. Apply temporal filter  
            // 3. Project back to empty schema
            
            // Find timestamp column index in the full schema
            let full_schema = self.listing_table.schema();
            let timestamp_col_index = full_schema
                .fields()
                .iter()
                .position(|f| f.name() == "timestamp")
                .ok_or_else(|| {
                    DataFusionError::Plan("No 'timestamp' field found in schema for temporal filtering".to_string())
                })?;
            
            debug!("🔍 Found timestamp column at index {timestamp_col_index}");
            
            // Scan with timestamp column included
            let timestamp_projection = vec![timestamp_col_index];
            let base_plan = self.listing_table.scan(state, Some(&timestamp_projection), filters, limit).await?;
            
            // Apply temporal filtering
            let filtered_plan = self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;
            
            // Project back to empty schema for COUNT
            let empty_projection: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
            let projection_exec = ProjectionExec::try_new(empty_projection, filtered_plan)?;
            
            debug!("✅ Temporal filtering applied successfully for COUNT query");
            return Ok(Arc::new(projection_exec));
        }
        
        // For non-empty projections, proceed normally but ensure timestamp is included if needed
        let base_plan = self.listing_table.scan(state, projection, filters, limit).await?;
        
        // Apply temporal filtering to the base plan
        let filtered_plan = self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;
        
        debug!("✅ Temporal filtering applied successfully");
        Ok(filtered_plan)
    }
}

/// Configuration options for table provider creation
/// Follows anti-duplication principles: single configurable function instead of multiple variants
#[derive(Default, Clone)]
pub struct TableProviderOptions {
    pub version_selection: VersionSelection,
    /// Multiple file URLs/paths to combine into a single table
    /// If empty, will use the node_id/part_id pattern (existing behavior)
    pub additional_urls: Vec<String>,
    // Future expansion: pub temporal_filter: Option<(i64, i64)>,
    // Future expansion: pub partition_pruning: bool,
}

impl Default for VersionSelection {
    fn default() -> Self {
        VersionSelection::AllVersions
    }
}

/// Single configurable function for creating table providers
/// Replaces create_listing_table_provider and create_listing_table_provider_with_options
/// Following anti-duplication guidelines: options pattern instead of function suffixes
pub async fn create_table_provider(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,  // Used for DeltaLake partition pruning (see deltalake-partition-pruning-fix.md)
    state: &crate::persistence::State,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // ObjectStore should already be registered by the transaction guard's SessionContext
    // Following anti-duplication principles: no duplicate registration
    
    // This is handled by the caller using the transaction guard's object_store() method
    // Following anti-duplication: no duplicate ObjectStore creation or registration needed here
    
    log::debug!("create_table_provider called for node_id: {node_id}");
    
    // Use centralized debug logging to eliminate duplication
    options.version_selection.log_debug(&node_id);
    
    // Create ListingTable URL(s) - either from options.additional_urls or pattern generation
    let (config, debug_info) = if options.additional_urls.is_empty() {
        // Default behavior: single URL from pattern
        let url_pattern = options.version_selection.to_url_pattern(&part_id, &node_id);
        let table_url = ListingTableUrl::parse(&url_pattern)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse table URL: {}", e)))?;
        
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);
        (config, format!("single URL: {}", url_pattern))
    } else {
        // Multiple URLs provided via options - use only the provided URLs, not the default pattern
        let mut table_urls = Vec::new();
        
        // Add only the additional URLs (no default pattern when explicit URLs are provided)
        for url_str in &options.additional_urls {
            table_urls.push(ListingTableUrl::parse(url_str)
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse additional URL '{}': {}", url_str, e)))?);
        }
        
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new_with_multi_paths(table_urls.clone()).with_listing_options(listing_options);
        
        let urls_str: Vec<String> = table_urls.iter().map(|u| u.to_string()).collect();
        (config, format!("multiple URLs: [{}]", urls_str.join(", ")))
    };
    
    debug!("Creating table provider with {debug_info}");
    
    // Use DataFusion's schema inference - this will automatically:
    // 1. Iterate through all versions of the file
    // 2. Skip 0-byte files (temporal override metadata-only versions)
    // 3. Merge schemas from all valid Parquet versions
    // 4. Provide the unified schema
    let ctx = state.session_context().await?;
    let config_with_schema = config.infer_schema(&ctx.state()).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Schema inference failed: {}", e)))?;
    
    // Create the ListingTable - DataFusion handles all the complexity!
    let listing_table = ListingTable::try_new(config_with_schema)
        .map_err(|e| TLogFSError::ArrowMessage(format!("ListingTable creation failed: {}", e)))?;
    
    // Get temporal overrides from the current version of this FileSeries
    let temporal_overrides = get_temporal_overrides_for_node_id(state, &node_id, part_id).await?;
    
    // ALWAYS apply temporal filtering for FileSeries (use i64::MIN/MAX if no overrides)
    let (min_time, max_time) = temporal_overrides.unwrap_or((i64::MIN, i64::MAX));
    debug!("Creating TemporalFilteredListingTable with bounds: {min_time} to {max_time}");
    
    if temporal_overrides.is_some() {
        debug!("⚠️ TEMPORAL OVERRIDES FOUND - creating TemporalFilteredListingTable wrapper");
    } else {
        debug!("⚠️ NO TEMPORAL OVERRIDES - using fallback bounds (i64::MIN, i64::MAX)");
    }
    
    Ok(Arc::new(TemporalFilteredListingTable::new(listing_table, min_time, max_time)))
}

// ✅ Thin convenience wrappers for backward compatibility (no logic duplication)
// Following anti-duplication guidelines: use main function with default options

/// Create a table provider with default options (all versions)
/// Thin wrapper around create_table_provider() with default options
/// Create a listing table provider - core function taking State directly
pub async fn create_listing_table_provider(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,
    state: &crate::persistence::State,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    create_table_provider(node_id, part_id, state, TableProviderOptions::default()).await
}

/// Create a listing table provider from TransactionGuard - convenience wrapper
pub async fn create_listing_table_provider_from_tx<'a>(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,
    tx: &mut crate::transaction_guard::TransactionGuard<'a>,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    let state = tx.state()?;
    create_listing_table_provider(node_id, part_id, &state).await
}

/// Create a table provider with specific version selection
/// Thin wrapper around create_table_provider() with version options
pub async fn create_listing_table_provider_with_options<'a>(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,
    tx: &mut crate::transaction_guard::TransactionGuard<'a>,
    version_selection: VersionSelection,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    let state = tx.state()?;
    create_table_provider(node_id, part_id, &state, TableProviderOptions {
        version_selection,
        additional_urls: Vec::new(),
    }).await
}

/// Register TinyFS ObjectStore with SessionContext - gives access to entire TinyFS
/// Returns the registered ObjectStore instance for further operations (following anti-duplication)
pub(crate) async fn register_tinyfs_object_store_with_context(
    ctx: &SessionContext,
    persistence_state: crate::persistence::State,
) -> Result<Arc<crate::tinyfs_object_store::TinyFsObjectStore>, TLogFSError> {
    // Create ONE ObjectStore with access to entire TinyFS via transaction
    let object_store = Arc::new(crate::tinyfs_object_store::TinyFsObjectStore::new(persistence_state));
    
    // Register with SessionContext
    let url = url::Url::parse("tinyfs:///")
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse tinyfs URL: {}", e)))?;
    ctx.runtime_env()
        .object_store_registry
        .register_store(&url, object_store.clone());
    
    debug!("Registered TinyFS ObjectStore with SessionContext - ready for any TinyFS path");
    Ok(object_store)
}



/// Get temporal overrides from the latest version of a FileSeries by node_id and part_id
/// This enables automatic temporal filtering for series queries with proper partition pruning
/// Note: Temporal overrides always come from the LATEST version and apply to all versions of the FileSeries
async fn get_temporal_overrides_for_node_id(
    persistence_state: &crate::persistence::State,
    node_id: &tinyfs::NodeID,
    part_id: tinyfs::NodeID,
) -> Result<Option<(i64, i64)>, TLogFSError> {
    use crate::query::NodeTable;
    use tinyfs::EntryType;
    
    // Create a metadata table to query all versions
    let table = persistence_state.table().await?
        .ok_or_else(|| TLogFSError::ArrowMessage("No Delta table available".to_string()))?;
    let node_table = NodeTable::new(table);
    
    // Query for all versions of this FileSeries using partition-aware query
    debug!("Looking up temporal overrides for node_id: {node_id}, part_id: {part_id}");
    
    let all_records = node_table.query_records_for_node(node_id, &part_id, EntryType::FileSeries).await?;
    let record_count = all_records.len();
    debug!("Found {record_count} records for node_id {node_id}");
    
    // Always look for temporal overrides in the LATEST version (highest version number)
    // This is correct because temporal overrides apply to the entire FileSeries, not individual versions
    if let Some(latest_version) = all_records.iter().max_by_key(|r| r.version) {
        let version = latest_version.version;
        let temporal_overrides = latest_version.temporal_overrides();
        let has_overrides = temporal_overrides.is_some();
        debug!("Latest version {version} has temporal overrides: {has_overrides}");
        
        if let Some((min_time, max_time)) = temporal_overrides {
            debug!("✅ Found temporal overrides in latest version {version}: {min_time} to {max_time}");
            return Ok(Some((min_time, max_time)));
        } else {
            debug!("⚠️ Latest version {version} has no temporal overrides");
        }
    } else {
        debug!("No records found for node_id {node_id}");
    }
    
    Ok(None)
}
