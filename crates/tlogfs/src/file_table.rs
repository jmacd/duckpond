/// File-Table Duality Integration for TinyFS and DataFusion
///
/// This module implements the FileTable trait that allows structured files
/// to expose both file-oriented and table-oriented interfaces.
use crate::error::TLogFSError;

use std::sync::Arc;
use log::debug;

// DataFusion imports
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;

// Re-export types from provider
pub use provider::{
    TableProviderKey, TableProviderOptions, TemporalFilteredListingTable, VersionSelection,
};

/// Single configurable function for creating table providers (TEMPORARY tlogfs wrapper)
///
/// **TEMPORARY**: This wrapper exists only during migration (Phase B).
/// Will be removed in Phase D when sql_derived moves to provider crate.
///
/// Converts tlogfs State to ProviderContext and delegates to provider::create_table_provider().
/// Following anti-duplication guidelines: options pattern instead of function suffixes
pub async fn create_table_provider(
    file_id: tinyfs::FileID,
    state: &crate::persistence::State,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    log::debug!("tlogfs::create_table_provider (temporary wrapper) called for file_id: {}", file_id.node_id());

    // Convert State to ProviderContext and delegate to provider crate
    let context = state.as_provider_context();
    
    provider::create_table_provider(file_id, &context, options)
        .await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Provider error: {}", e)))
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
