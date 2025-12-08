/// File-Table Duality Integration for TinyFS and DataFusion
///
/// This module implements the FileTable trait that allows structured files
/// to expose both file-oriented and table-oriented interfaces.
use crate::error::TLogFSError;

use std::sync::Arc;
use tinyfs::PersistenceLayer;

// DataFusion imports
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use log::debug;

// Re-export types from provider
pub use provider::{TemporalFilteredListingTable, VersionSelection};

/// Configuration options for table provider creation
/// Follows anti-duplication principles: single configurable function instead of multiple variants
#[derive(Default, Clone)]
pub struct TableProviderOptions {
    pub version_selection: VersionSelection,
    /// Multiple file URLs/paths to combine into a single table
    /// If empty, will use the node_id/part_id pattern (existing behavior)
    pub additional_urls: Vec<String>,
}

/// Single configurable function for creating table providers
/// Replaces create_listing_table_provider and create_listing_table_provider_with_options
/// Following anti-duplication guidelines: options pattern instead of function suffixes
pub async fn create_table_provider(
    file_id: tinyfs::FileID, // FileID contains both node_id and part_id for DeltaLake partition pruning
    state: &crate::persistence::State,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // ObjectStore should already be registered by the transaction guard's SessionContext
    // Following anti-duplication principles: no duplicate registration

    // This is handled by the caller using the transaction guard's object_store() method
    // Following anti-duplication: no duplicate ObjectStore creation or registration needed here

    log::debug!("create_table_provider called for file_id: {}", file_id.node_id());

    // Use centralized debug logging to eliminate duplication
    options.version_selection.log_debug(&file_id.node_id());

    // Check cache first (only for simple cases without additional_urls)
    if options.additional_urls.is_empty() {
        let cache_key = crate::persistence::TableProviderKey::new(
            file_id,
            options.version_selection.clone(),
        );

        if let Some(cached_provider) = state.get_table_provider_cache(&cache_key) {
            debug!(
                "🚀 CACHE HIT: Returning cached TableProvider for file_id: {}", file_id.node_id()
            );
            return Ok(cached_provider);
        } else {
            debug!(
                "💾 CACHE MISS: Creating new TableProvider for file_id: {}", file_id.node_id()
            );
        }
    } else {
        debug!("⚠️ CACHE BYPASS: additional_urls present, creating fresh TableProvider");
    }

    // Create ListingTable URL(s) - either from options.additional_urls or pattern generation
    let (config, debug_info) = if options.additional_urls.is_empty() {
        // Default behavior: single URL from pattern
        let url_pattern = options.version_selection.to_url_pattern(&file_id);
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
            table_urls.push(ListingTableUrl::parse(url_str).map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to parse additional URL '{}': {}",
                    url_str, e
                ))
            })?);
        }

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new_with_multi_paths(table_urls.clone())
            .with_listing_options(listing_options);

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
    let config_with_schema = config
        .infer_schema(&ctx.state())
        .await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Schema inference failed: {}", e)))?;

    let listing_table = ListingTable::try_new(config_with_schema)
        .map_err(|e| TLogFSError::ArrowMessage(format!("ListingTable creation failed: {}", e)))?;

    // Determine temporal bounds for TemporalFilteredListingTable wrapper
    // 
    // NOTE: Temporal bounds are per-FileSeries metadata for data quality filtering
    // (set via `pond set-temporal-bounds`). They filter out garbage data outside the
    // file's valid time range.
    //
    // CURRENT: Only applied to single-file queries via TemporalFilteredListingTable
    // FUTURE: Multi-file queries should have per-file bounds enforced at Parquet scan level
    let (min_time, max_time) = if options.additional_urls.is_empty() {
        // Single-file case: query temporal bounds from persistence layer
        debug!("Querying temporal bounds from persistence for file_id={}", file_id);
        let temporal_overrides = state.get_temporal_bounds(file_id).await
            .map_err(|e| TLogFSError::Transaction { message: e.to_string() })?;

        temporal_overrides.unwrap_or_else(|| {
            debug!(
                "No temporal bounds for FileSeries {} - using unbounded (no data quality filtering)",
                file_id
            );
            (i64::MIN, i64::MAX)
        })
    } else {
        // Multi-file case: no temporal filtering at this level
        // TODO: Implement per-file temporal bounds at Parquet reader level
        debug!(
            "Multi-file query ({} URLs) - temporal bounds enforcement deferred to Parquet reader (not yet implemented)",
            options.additional_urls.len()
        );
        (i64::MIN, i64::MAX)
    };
    
    debug!("Creating TemporalFilteredListingTable with bounds: {min_time} to {max_time}");

    let table_provider = Arc::new(TemporalFilteredListingTable::new(
        listing_table,
        min_time,
        max_time,
    ));

    log::debug!(
        "📋 CREATED TableProvider: file_id={}, temporal_bounds=({}, {}), urls={}",
        file_id,
        min_time,
        max_time,
        debug_info
    );

    // Cache the result (only for simple cases without additional_urls)
    if options.additional_urls.is_empty() {
        let cache_key = crate::persistence::TableProviderKey::new(
            file_id,
            options.version_selection.clone(),
        );
        state.set_table_provider_cache(cache_key, table_provider.clone());
        debug!("💾 CACHED: Stored TableProvider for file_id: {file_id}");
    }

    Ok(table_provider)
}

// ✅ Thin convenience wrappers for backward compatibility (no logic duplication)
// Following anti-duplication guidelines: use main function with default options

/// Create a table provider with default options (all versions)
/// Thin wrapper around create_table_provider() with default options
/// Create a listing table provider - core function taking State directly
pub async fn create_listing_table_provider(
    file_id: tinyfs::FileID,
    state: &crate::persistence::State,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    create_table_provider(file_id, state, TableProviderOptions::default()).await
}

/// Create a listing table provider from TransactionGuard - convenience wrapper
pub async fn create_listing_table_provider_from_tx<'a>(
    file_id: tinyfs::FileID,
    tx: &mut crate::transaction_guard::TransactionGuard<'a>,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    let state = tx.state()?;
    create_listing_table_provider(file_id, &state).await
}

/// Create a table provider with specific version selection
/// Thin wrapper around create_table_provider() with version options
pub async fn create_listing_table_provider_with_options<'a>(
    file_id: tinyfs::FileID,
    tx: &mut crate::transaction_guard::TransactionGuard<'a>,
    version_selection: VersionSelection,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    let state = tx.state()?;
    create_table_provider(
        file_id,
        &state,
        TableProviderOptions {
            version_selection,
            additional_urls: Vec::new(),
        },
    )
    .await
}

/// Register TinyFS ObjectStore with SessionContext - gives access to entire TinyFS
/// Returns the registered ObjectStore instance for further operations (following anti-duplication)
pub(crate) async fn register_tinyfs_object_store(
    ctx: &SessionContext,
    persistence_state: crate::persistence::State,
) -> Result<Arc<crate::TinyFsObjectStore<crate::persistence::State>>, TLogFSError> {
    // Create ONE ObjectStore with access to entire TinyFS via transaction
    let object_store = Arc::new(crate::TinyFsObjectStore::new(
        persistence_state,
    ));

    // Register with SessionContext
    let url = url::Url::parse("tinyfs:///")
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to parse tinyfs URL: {}", e)))?;
    _ = ctx
        .runtime_env()
        .object_store_registry
        .register_store(&url, object_store.clone());

    debug!("Registered TinyFS ObjectStore with SessionContext - ready for any TinyFS path");
    Ok(object_store)
}
