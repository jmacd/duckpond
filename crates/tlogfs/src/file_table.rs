/// File-Table Duality Integration for TinyFS and DataFusion
/// 
/// This module implements the FileTable trait that allows structured files
/// to expose both file-oriented and table-oriented interfaces.

use crate::error::TLogFSError;

use crate::tinyfs_object_store::TinyFsObjectStore;
use arrow::datatypes::{SchemaRef, DataType, TimeUnit};
use std::sync::Arc;
use std::any::Any;

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
        diagnostics::log_debug!("🔍 TemporalFilteredListingTable.schema() called - returning {count} fields: [{names}]", 
            count: field_count, names: field_names_str);
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
        diagnostics::log_debug!("🚨 TemporalFilteredListingTable.scan() called - temporal filtering is active!");
        
        // Convert from milliseconds to seconds for HydroVu data
        let min_seconds = self.min_time / 1000;
        let max_seconds = self.max_time / 1000;
        
        diagnostics::log_debug!("⚡ Temporal filtering range: {min_seconds} to {max_seconds} (seconds)", min_seconds: min_seconds, max_seconds: max_seconds);
        
        // Use the provided session state (not our internal one) to avoid schema mismatches
        diagnostics::log_debug!("🔍 Using provided SessionState instead of internal one...");
        
        // Check if we actually need to apply temporal filtering
        if self.min_time == i64::MIN && self.max_time == i64::MAX {
            diagnostics::log_debug!("⚡ No temporal bounds - delegating to base ListingTable");
            return self.listing_table.scan(state, projection, filters, limit).await;
        }
        
        // Check if this is an empty projection (COUNT case)
        let is_empty_projection = projection.as_ref().map_or(false, |p| p.is_empty());
        
        if is_empty_projection {
            diagnostics::log_debug!("📊 Empty projection detected (COUNT query) - need to include timestamp for filtering");
            
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
            
            diagnostics::log_debug!("🔍 Found timestamp column at index {index}", index: timestamp_col_index);
            
            // Scan with timestamp column included
            let timestamp_projection = vec![timestamp_col_index];
            let base_plan = self.listing_table.scan(state, Some(&timestamp_projection), filters, limit).await?;
            
            // Apply temporal filtering
            let filtered_plan = self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;
            
            // Project back to empty schema for COUNT
            let empty_projection: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
            let projection_exec = ProjectionExec::try_new(empty_projection, filtered_plan)?;
            
            diagnostics::log_debug!("✅ Temporal filtering applied successfully for COUNT query");
            return Ok(Arc::new(projection_exec));
        }
        
        // For non-empty projections, proceed normally but ensure timestamp is included if needed
        let base_plan = self.listing_table.scan(state, projection, filters, limit).await?;
        
        // Apply temporal filtering to the base plan
        let filtered_plan = self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;
        
        diagnostics::log_debug!("✅ Temporal filtering applied successfully");
        Ok(filtered_plan)
    }
}

// Helper function to create a table provider from a TinyFS path using DataFusion ListingTable  
pub async fn create_table_provider_from_path(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    persistence_state: crate::persistence::State,
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    use tinyfs::Lookup;
    
    // Resolve the path to get the actual file
    let (_, lookup_result) = tinyfs_wd.resolve_path(path).await.map_err(TLogFSError::TinyFS)?;
    
    match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            
            // Try to get the file from the node - this returns Pathed<FileHandle>
            let file_handle = node_guard.as_file().map_err(|e| {
                TLogFSError::ArrowMessage(format!("Path {} does not point to a file: {}", path, e))
            })?;
            
            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;
            
            match metadata.entry_type {
                tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                    // Extract node_id and part_id from the file_handle
                    use crate::file::OpLogFile;
                    let file_arc = file_handle.handle.get_file().await;
                    let (node_id, part_id) = {
                        let file_guard = file_arc.lock().await;
                        let file_any = file_guard.as_any();
                        let oplog_file = file_any.downcast_ref::<OpLogFile>()
                            .ok_or_else(|| TLogFSError::ArrowMessage("FileHandle is not an OpLogFile".to_string()))?;
                        (oplog_file.get_node_id(), oplog_file.get_part_id())
                    };
                    
                    // Use DataFusion ListingTable approach - no more GenericFileTable duplication!
                    create_listing_table_provider(node_id, part_id, persistence_state, ctx).await
                },
                _ => {
                    return Err(TLogFSError::ArrowMessage(
                        format!("Path {} points to unsupported entry type for table operations: {:?}", path, metadata.entry_type)
                    ))
                }
            }
        },
        Lookup::NotFound(full_path, _) => {
            Err(TLogFSError::ArrowMessage(format!("File not found: {}", full_path.display())))
        },
        Lookup::Empty(_) => {
            Err(TLogFSError::ArrowMessage("Empty path provided".to_string()))
        }
    }
}

// Create a ListingTable provider with just the necessary parameters
pub async fn create_listing_table_provider(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,
    persistence_state: crate::persistence::State,
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    create_listing_table_provider_with_options(
        node_id,
        part_id,
        persistence_state,
        ctx,
        VersionSelection::AllVersions, // Default behavior
    ).await
}

// Enhanced ListingTable provider creation with configurable options
async fn create_listing_table_provider_with_options(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,
    persistence_state: crate::persistence::State,
    ctx: &SessionContext,
    version_selection: VersionSelection,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // Use the provided SessionContext and persistence state - no recreation!
    diagnostics::log_debug!("create_listing_table_provider called");
    
        // Create TinyFS ObjectStore using the provided persistence state
    let object_store = Arc::new(TinyFsObjectStore::new(persistence_state.clone()));
    
    // Register ObjectStore with the provided DataFusion context
    let url = url::Url::parse("tinyfs:///")
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse tinyfs URL: {}", e)))?;
    ctx.runtime_env()
        .object_store_registry
        .register_store(&url, object_store.clone());
    
    // Register file versions with ObjectStore - node_id and part_id provided directly
    // TODO: Enhance TinyFsObjectStore to support selective version registration
    object_store.register_file_versions(node_id, part_id).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register file versions: {}", e)))?;
    
    // Log the version selection strategy for debugging  
    match &version_selection {
        VersionSelection::AllVersions => {
            diagnostics::log_debug!("Version selection: ALL versions for node {node_id}", node_id: node_id);
        },
        VersionSelection::LatestVersion => {  
            diagnostics::log_debug!("Version selection: LATEST version for node {node_id}", node_id: node_id);
        },
        VersionSelection::SpecificVersion(version) => {
            diagnostics::log_debug!("Version selection: SPECIFIC version {version} for node {node_id}", version: version, node_id: node_id);
        }
    }
    
    // Create ListingTable URL - same pattern for all, ObjectStore will handle version filtering in future
    let url_pattern = format!("tinyfs:///node/{}/version/", node_id);
    
    let table_url = ListingTableUrl::parse(&url_pattern)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse table URL: {}", e)))?;
    
    // Create ListingTable configuration with Parquet format
    let file_format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(file_format);
    let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);
    
    // Use DataFusion's schema inference - this will automatically:
    // 1. Iterate through all versions of the file
    // 2. Skip 0-byte files (temporal override metadata-only versions)
    // 3. Merge schemas from all valid Parquet versions
    // 4. Provide the unified schema
    let config_with_schema = config.infer_schema(&ctx.state()).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Schema inference failed: {}", e)))?;
    
    // Create the ListingTable - DataFusion handles all the complexity!
    let listing_table = ListingTable::try_new(config_with_schema)
        .map_err(|e| TLogFSError::ArrowMessage(format!("ListingTable creation failed: {}", e)))?;
    
    // Get temporal overrides from the current version of this FileSeries
    let temporal_overrides = get_temporal_overrides_for_node_id(&persistence_state, &node_id).await?;
    
    // ALWAYS apply temporal filtering for FileSeries (use i64::MIN/MAX if no overrides)
    let (min_time, max_time) = temporal_overrides.unwrap_or((i64::MIN, i64::MAX));
    diagnostics::log_debug!("Creating TemporalFilteredListingTable with bounds: {min_time} to {max_time}", min_time: min_time, max_time: max_time);
    
    if temporal_overrides.is_some() {
        diagnostics::log_debug!("⚠️ TEMPORAL OVERRIDES FOUND - creating TemporalFilteredListingTable wrapper");
    } else {
        diagnostics::log_debug!("⚠️ NO TEMPORAL OVERRIDES - using fallback bounds (i64::MIN, i64::MAX)");
    }
    
    Ok(Arc::new(TemporalFilteredListingTable::new(listing_table, min_time, max_time)))
}

/// Get temporal overrides from the current version of a FileSeries by node_id
/// This enables automatic temporal filtering for series queries
async fn get_temporal_overrides_for_node_id(
    persistence_state: &crate::persistence::State,
    node_id: &tinyfs::NodeID,
) -> Result<Option<(i64, i64)>, TLogFSError> {
    use crate::query::NodeTable;
    use tinyfs::EntryType;
    
    // Create a metadata table to query the current version
    let table = persistence_state.table().await?
        .ok_or_else(|| TLogFSError::ArrowMessage("No Delta table available".to_string()))?;
    let node_table = NodeTable::new(table);
    
    // Query for all versions of this FileSeries
    let node_id_str = node_id.to_hex_string();
    let all_records = node_table.query_records_for_node(&node_id_str, EntryType::FileSeries).await?;
    
    // Find the current version (highest version number)
    if let Some(current_version) = all_records.iter().max_by_key(|r| r.version) {
        return Ok(current_version.temporal_overrides());
    }
    
    Ok(None)
}
