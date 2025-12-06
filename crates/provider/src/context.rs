//! Provider context and factory context for dynamic node creation
//!
//! This module defines the abstraction layer between factories and persistence implementations.
//! ProviderContext is a concrete struct that holds trait objects for implementation injection,
//! while FactoryContext provides the complete context for factory operations.

use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::FileID;

/// Result type for provider operations
pub type Result<T> = std::result::Result<T, crate::Error>;

/// Internal trait for DataFusion session access
/// 
/// Implemented by persistence layers (tlogfs::State) to provide
/// SessionContext to factories for SQL execution.
#[async_trait]
pub trait SessionProvider: Send + Sync {
    /// Get shared DataFusion session for SQL execution
    async fn session_context(&self) -> Result<Arc<SessionContext>>;
}

/// Internal trait for template variable management
///
/// Implemented by persistence layers to provide CLI variable expansion
/// for Tera templates used in export stages.
pub trait TemplateVariableProvider: Send + Sync {
    /// Get current template variables snapshot
    fn get_template_variables(&self) -> Result<HashMap<String, serde_json::Value>>;
    
    /// Update template variables
    fn set_template_variables(&self, vars: HashMap<String, serde_json::Value>) -> Result<()>;
}

/// Internal trait for TableProvider caching
///
/// Implemented by persistence layers to cache expensive TableProvider instances
/// and avoid repeated schema inference and file scanning.
pub trait TableProviderCache: Send + Sync {
    /// Get cached TableProvider by key
    fn get(&self, key: &dyn std::any::Any) -> Option<Arc<dyn datafusion::catalog::TableProvider>>;
    
    /// Set cached TableProvider for a key
    fn set(&self, key: Box<dyn std::any::Any + Send + Sync>, provider: Arc<dyn datafusion::catalog::TableProvider>) -> Result<()>;
}

/// Provider context - concrete struct holding implementation details
///
/// This struct provides the essential operations that factories need from a persistence layer:
/// - Access to DataFusion SessionContext for SQL execution
/// - Template variable management for CLI expansion
/// - Table provider caching for performance optimization
///
/// Implementation is injected via trait objects:
/// - SessionProvider - Provides DataFusion SessionContext
/// - TemplateVariableProvider - Manages template variables
/// - TableProviderCache - Caches TableProvider instances
///
/// This design avoids `Arc<dyn ProviderContext>` and allows clean injection
/// of tlogfs::State or test implementations.
#[derive(Clone)]
pub struct ProviderContext {
    session: Arc<dyn SessionProvider>,
    template_vars: Arc<dyn TemplateVariableProvider>,
    table_cache: Arc<dyn TableProviderCache>,
}

impl ProviderContext {
    /// Create a new provider context with injected implementations
    pub fn new(
        session: Arc<dyn SessionProvider>,
        template_vars: Arc<dyn TemplateVariableProvider>,
        table_cache: Arc<dyn TableProviderCache>,
    ) -> Self {
        Self {
            session,
            template_vars,
            table_cache,
        }
    }
    
    /// Get shared DataFusion session for SQL execution
    ///
    /// Returns Arc<SessionContext> which is already thread-safe by design.
    /// The SessionContext handles internal synchronization for concurrent
    /// table provider registration and query execution.
    pub async fn session_context(&self) -> Result<Arc<SessionContext>> {
        self.session.session_context().await
    }
    
    /// Get template variables for CLI expansion (read-only access)
    pub fn get_template_variables(&self) -> Result<HashMap<String, serde_json::Value>> {
        self.template_vars.get_template_variables()
    }
    
    /// Set template variables (controlled mutation)
    pub fn set_template_variables(&self, vars: HashMap<String, serde_json::Value>) -> Result<()> {
        self.template_vars.set_template_variables(vars)
    }
    
    /// Get cached TableProvider by key (performance optimization)
    pub fn get_table_provider_cache(
        &self,
        key: &dyn std::any::Any,
    ) -> Option<Arc<dyn datafusion::catalog::TableProvider>> {
        self.table_cache.get(key)
    }
    
    /// Set cached TableProvider for a key
    pub fn set_table_provider_cache(
        &self,
        key: Box<dyn std::any::Any + Send + Sync>,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<()> {
        self.table_cache.set(key, provider)
    }
}

/// Factory context for creating dynamic nodes
///
/// This struct provides the complete context needed by factories:
/// - Access to persistence layer via ProviderContext
/// - FileID providing node and partition identity
/// - Optional pond metadata (pond_id, birth_timestamp, etc.)
#[derive(Clone)]
pub struct FactoryContext {
    /// Access to persistence layer operations
    pub context: ProviderContext,
    /// FileID for context-aware factories (provides both node and partition info)
    pub file_id: FileID,
    /// Pond identity metadata (pond_id, birth_timestamp, etc.)
    /// Provided by Steward when creating factory contexts
    pub pond_metadata: Option<PondMetadata>,
}

impl FactoryContext {
    /// Create a new factory context with the given provider context and file_id
    #[must_use]
    pub fn new(context: ProviderContext, file_id: FileID) -> Self {
        Self {
            context,
            file_id,
            pond_metadata: None,
        }
    }

    /// Create a factory context with pond metadata
    #[must_use]
    pub fn with_metadata(
        context: ProviderContext,
        file_id: FileID,
        pond_metadata: PondMetadata,
    ) -> Self {
        Self {
            context,
            file_id,
            pond_metadata: Some(pond_metadata),
        }
    }
}

/// Pond identity metadata - immutable information about the pond's origin
///
/// This metadata is created once when the pond is initialized and preserved across replicas.
/// It provides traceability and identity for distributed pond systems.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PondMetadata {
    /// Unique identifier for this pond (UUID v7)
    pub pond_id: uuid7::Uuid,
    /// Timestamp when this pond was originally created (microseconds since epoch)
    pub birth_timestamp: i64,
    /// Hostname where the pond was originally created
    pub birth_hostname: String,
    /// Username who originally created the pond
    pub birth_username: String,
}

impl Default for PondMetadata {
    /// Create new pond metadata for a freshly initialized pond
    fn default() -> Self {
        let pond_id = uuid7::uuid7();
        let birth_timestamp = chrono::Utc::now().timestamp_micros();

        // Note: std::net::hostname() is unstable, using placeholder
        let birth_hostname = "unknown".into();

        let birth_username = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or("unknown".into());

        Self {
            pond_id,
            birth_timestamp,
            birth_hostname,
            birth_username,
        }
    }
}
