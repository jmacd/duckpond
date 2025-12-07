//! Provider context and factory context for dynamic node creation
//!
//! This module defines the abstraction layer between factories and persistence implementations.
//! ProviderContext is a concrete struct with direct access to DataFusion session and State handle.

use datafusion::execution::context::SessionContext;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::FileID;

/// Result type for provider operations
pub type Result<T> = std::result::Result<T, crate::Error>;

/// Provider context - concrete struct with direct access to session and state
///
/// This struct provides factories with:
/// - Direct access to DataFusion SessionContext for SQL execution
/// - Template variable management for CLI expansion  
/// - Table provider caching for performance optimization
/// - Opaque handle back to originating State for persistence operations
///
/// All fields are concrete - no trait objects to downcast.
#[derive(Clone)]
pub struct ProviderContext {
    /// DataFusion session for SQL execution (direct access, no async needed)
    pub datafusion_session: Arc<SessionContext>,
    
    /// Template variables for Tera CLI expansion
    pub template_variables: Arc<std::sync::Mutex<HashMap<String, serde_json::Value>>>,
    
    /// Table provider cache for performance
    pub table_provider_cache: Arc<std::sync::Mutex<HashMap<String, Arc<dyn datafusion::catalog::TableProvider>>>>,
    
    /// Opaque handle to originating State (tlogfs can downcast this)
    pub state_handle: Arc<dyn Any + Send + Sync>,
}

impl ProviderContext {
    /// Create a new provider context from concrete values
    pub fn new(
        datafusion_session: Arc<SessionContext>,
        template_variables: HashMap<String, serde_json::Value>,
        state_handle: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self {
            datafusion_session,
            template_variables: Arc::new(std::sync::Mutex::new(template_variables)),
            table_provider_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            state_handle,
        }
    }
    
    /// Get template variables snapshot
    pub fn get_template_variables(&self) -> Result<HashMap<String, serde_json::Value>> {
        self.template_variables
            .lock()
            .map(|guard| guard.clone())
            .map_err(|e| crate::Error::MutexPoisoned(e.to_string()))
    }
    
    /// Set template variables
    pub fn set_template_variables(&self, vars: HashMap<String, serde_json::Value>) -> Result<()> {
        *self.template_variables
            .lock()
            .map_err(|e| crate::Error::MutexPoisoned(e.to_string()))? = vars;
        Ok(())
    }
    
    /// Get cached TableProvider by cache key
    pub fn get_table_provider_cache(&self, key: &str) -> Option<Arc<dyn datafusion::catalog::TableProvider>> {
        self.table_provider_cache
            .lock()
            .ok()?
            .get(key)
            .cloned()
    }
    
    /// Set cached TableProvider
    pub fn set_table_provider_cache(&self, key: String, provider: Arc<dyn datafusion::catalog::TableProvider>) -> Result<()> {
        _ = self.table_provider_cache
            .lock()
            .map_err(|e| crate::Error::MutexPoisoned(e.to_string()))?
            .insert(key, provider);
        Ok(())
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
