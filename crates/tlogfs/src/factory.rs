//! Factory system re-exports and backward compatibility layer
//!
//! This module provides backward compatibility for code that imports from tlogfs::factory.
//! The actual factory infrastructure has moved to the provider crate.

use crate::persistence::State;
use crate::TLogFSError;
use tinyfs::FileID;

// Re-export everything from provider::registry
// Note: QueryableFile is NOT re-exported here because tlogfs has its own QueryableFile trait
// that uses State instead of ProviderContext. This will be migrated later.
pub use provider::registry::{
    ConfigFile, DynamicFactory, ExecutionContext, ExecutionMode,
    FactoryCommand, FactoryRegistry, DYNAMIC_FACTORIES,
};

// Re-export PondMetadata from provider
pub use provider::PondMetadata;

// NOTE: We do NOT re-export provider's registration macros because they expect
// provider::FactoryContext. Instead, we define our own macros below that wrap
// tlogfs functions (using tlogfs::FactoryContext) and convert to provider types.

/// Legacy FactoryContext that uses State directly
/// 
/// This maintains backward compatibility with existing factories that expect State.
/// New code should use provider::FactoryContext with provider::ProviderContext.
#[derive(Clone)]
pub struct FactoryContext {
    /// Access to the persistence layer for resolving pond nodes
    pub state: State,
    /// FileID for context-aware factories (provides both node and partition info)
    pub file_id: FileID,
    /// Pond identity metadata (pond_id, birth_timestamp, etc.)
    /// Provided by Steward when creating factory contexts
    pub pond_metadata: Option<PondMetadata>,
}

impl FactoryContext {
    /// Create a new factory context with the given state and file_id
    #[must_use]
    pub fn new(state: State, file_id: FileID) -> Self {
        Self {
            state,
            file_id,
            pond_metadata: None,
        }
    }

    /// Create a factory context with pond metadata
    #[must_use]
    pub fn with_metadata(
        state: State,
        file_id: FileID,
        pond_metadata: PondMetadata,
    ) -> Self {
        Self {
            state,
            file_id,
            pond_metadata: Some(pond_metadata),
        }
    }

    /// Convert to provider::FactoryContext for use with new provider-based factories
    pub fn to_provider_context(&self) -> provider::FactoryContext {
        provider::FactoryContext {
            context: self.state.as_provider_context(),
            file_id: self.file_id,
            pond_metadata: self.pond_metadata.clone(),
        }
    }
}

// Wrapper functions for backward compatibility with tlogfs::FactoryContext

/// Initialize a factory using legacy tlogfs::FactoryContext
pub async fn factory_initialize(
    factory_name: &str,
    config: &[u8],
    context: FactoryContext,
) -> Result<(), TLogFSError> {
    FactoryRegistry::initialize(factory_name, config, context.to_provider_context())
        .await
        .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
            TLogFSError::Internal(format!("Factory initialization failed: {}", e))
        })
}

/// Execute a factory using legacy tlogfs::FactoryContext
pub async fn factory_execute(
    factory_name: &str,
    config: &[u8],
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), TLogFSError> {
    FactoryRegistry::execute(factory_name, config, context.to_provider_context(), ctx)
        .await
        .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
            TLogFSError::Internal(format!("Factory execution failed: {}", e))
        })
}

/// Extract State from provider::FactoryContext
///
/// Simple helper to downcast the state_handle back to concrete State.
/// This is the mechanical bridge - allows factories to access State directly.
pub fn extract_state(pctx: &provider::FactoryContext) -> Result<State, TLogFSError> {
    // Downcast the opaque state_handle to State
    let state_ref = pctx.context.state_handle
        .downcast_ref::<State>()
        .ok_or_else(|| TLogFSError::Internal(
            "ProviderContext was not created from tlogfs::State - cannot extract".to_string()
        ))?;
    
    // Clone the State (it's cheap - Arc-based internally)
    Ok(state_ref.clone())
}

// Simple re-export macros that just pass through to provider
// Factory functions need to use provider::FactoryContext

/// Register a directory factory (synchronous)
#[macro_export]
macro_rules! register_directory_factory {
    (
        name: $name:expr,
        description: $desc:expr,
        directory: $dir_fn:expr,
        validate: $validate_fn:expr
    ) => {
        provider::register_dynamic_factory!(
            name: $name,
            description: $desc,
            directory: $dir_fn,
            validate: $validate_fn
        );
    };
}

/// Register a file factory (async, no QueryableFile support)
#[macro_export]
macro_rules! register_file_factory {
    (
        name: $name:expr,
        description: $desc:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        provider::register_dynamic_factory!(
            name: $name,
            description: $desc,
            file: $file_fn,
            validate: $validate_fn
        );
    };
}

/// Register a queryable file factory (async file with QueryableFile trait support)
#[macro_export]
macro_rules! register_queryable_file_factory {
    (
        name: $name:expr,
        description: $desc:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        try_as_queryable: $downcast_fn:expr
    ) => {
        provider::register_dynamic_factory!(
            name: $name,
            description: $desc,
            file: $file_fn,
            validate: $validate_fn,
            try_as_queryable: $downcast_fn
        );
    };
}

/// Register an executable factory (for command-line invocation)
/// 
/// This macro wraps provider::register_executable_factory! and converts
/// provider::FactoryContext to tlogfs::FactoryContext for backward compatibility.
#[macro_export]
macro_rules! register_executable_factory {
    (
        name: $name:expr,
        description: $desc:expr,
        validate: $validate_fn:expr,
        initialize: $init_fn:expr,
        execute: $exec_fn:expr
    ) => {
        // Create wrapper functions for context conversion
        async fn __tlogfs_init_wrapper(
            config: serde_json::Value,
            provider_ctx: provider::FactoryContext,
        ) -> Result<(), tinyfs::Error> {
            // Convert provider::FactoryContext to tlogfs::FactoryContext
            let legacy_ctx = $crate::factory::FactoryContext {
                state: $crate::factory::extract_state(&provider_ctx)
                    .map_err(|e| tinyfs::Error::Other(e.to_string()))?,
                file_id: provider_ctx.file_id,
                pond_metadata: provider_ctx.pond_metadata.clone(),
            };
            $init_fn(config, legacy_ctx)
                .await
                .map_err(|e| tinyfs::Error::Other(e.to_string()))
        }

        async fn __tlogfs_exec_wrapper(
            config: serde_json::Value,
            provider_ctx: provider::FactoryContext,
            exec_ctx: provider::ExecutionContext,
        ) -> Result<(), tinyfs::Error> {
            // Convert provider::FactoryContext to tlogfs::FactoryContext
            let legacy_ctx = $crate::factory::FactoryContext {
                state: $crate::factory::extract_state(&provider_ctx)
                    .map_err(|e| tinyfs::Error::Other(e.to_string()))?,
                file_id: provider_ctx.file_id,
                pond_metadata: provider_ctx.pond_metadata.clone(),
            };
            $exec_fn(config, legacy_ctx, exec_ctx)
                .await
                .map_err(|e: $crate::TLogFSError| tinyfs::Error::Other(e.to_string()))
        }

        provider::register_executable_factory!(
            name: $name,
            description: $desc,
            validate: $validate_fn,
            initialize: __tlogfs_init_wrapper,
            execute: __tlogfs_exec_wrapper
        );
    };
}

/// Backward compatibility alias - dispatches to specific factory type macros
#[macro_export]
macro_rules! register_dynamic_factory {
    (
        name: $name:expr,
        description: $desc:expr,
        directory: $dir_fn:expr,
        validate: $validate_fn:expr
    ) => {
        $crate::register_directory_factory!(
            name: $name,
            description: $desc,
            directory: $dir_fn,
            validate: $validate_fn
        );
    };
    (
        name: $name:expr,
        description: $desc:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        $crate::register_file_factory!(
            name: $name,
            description: $desc,
            file: $file_fn,
            validate: $validate_fn
        );
    };
    (
        name: $name:expr,
        description: $desc:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        try_as_queryable: $downcast_fn:expr
    ) => {
        $crate::register_queryable_file_factory!(
            name: $name,
            description: $desc,
            file: $file_fn,
            validate: $validate_fn,
            try_as_queryable: $downcast_fn
        );
    };
}
