// Dynamic factory registration system using linkme
use linkme::distributed_slice;
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
use tinyfs::{DirHandle, FileHandle, Result as TinyFSResult, NodeID};
use crate::persistence::State;

#[derive(Clone)]
pub struct FactoryContext {
    /// Access to the persistence layer for resolving pond nodes
    pub state: State,
    /// Parent node id for context-aware factories
    pub parent_node_id: NodeID,
    /// Template variables from CLI (-v key=value flags)
    pub template_variables: HashMap<String, serde_json::Value>,
    /// Export data from previous export stage (for template context)
    pub export_data: Option<serde_json::Value>,
}

impl FactoryContext {
    /// Create a new factory context with the given state and parent_node_id
    /// Automatically extracts template variables and export data from the state
    pub fn new(state: State, parent_node_id: NodeID) -> Self {
        let state_variables = state.get_template_variables();
        
        // Extract template variables, excluding the special "export" key
        // This preserves structured keys like "vars" from CLI processing
        let mut template_variables = HashMap::new();
        for (key, value) in state_variables.iter() {
            if key != "export" {
                template_variables.insert(key.clone(), value.clone());
            }
        }
        
        // Extract export data if present
        let export_data = state_variables.get("export").cloned();
        
        Self { 
            state, 
            parent_node_id,
            template_variables,
            export_data,
        }
    }

    /// Create a new factory context with template variables
    pub fn with_variables(state: State, parent_node_id: NodeID, template_variables: HashMap<String, serde_json::Value>) -> Self {
        let state_variables = state.get_template_variables();
        
        // Extract export data from state if present
        let export_data = state_variables.get("export").cloned();
        
        Self { 
            state, 
            parent_node_id,
            template_variables,
            export_data,
        }
    }
    
    /// Create a new factory context with template variables and export data
    pub fn with_variables_and_export(
        state: State, 
        parent_node_id: NodeID, 
        template_variables: HashMap<String, serde_json::Value>,
        export_data: serde_json::Value,
    ) -> Self {
        Self { 
            state, 
            parent_node_id,
            template_variables,
            export_data: Some(export_data),
        }
    }
    
    /// Create a cache key for dynamic directory factory
    pub async fn create_cache_key(&self, entry_name: &str) -> Result<crate::persistence::DynamicNodeKey, crate::TLogFSError> {
        let part_id = self.state.get_part_id().await?;
        Ok(crate::persistence::DynamicNodeKey::new(part_id, self.parent_node_id, entry_name.to_string()))
    }
    
    /// Resolve a source path to a node reference within the current transaction context
    /// This method requires a TransactionGuard to access the filesystem
    pub async fn resolve_source_path_with_tx(&self, source_path: &str, tx: &crate::transaction_guard::TransactionGuard<'_>) -> TinyFSResult<Arc<tinyfs::NodePath>> {
        // Get the TinyFS root from the transaction guard
        let tinyfs_root = tx.root().await?;
        
        // Resolve the path using TinyFS to get a node reference
        let node_path = tinyfs_root.get_node_path(source_path).await
            .map_err(|e| tinyfs::Error::Other(format!("Source path '{}' not found: {}", source_path, e)))?;
        
        Ok(Arc::new(node_path))
    }
}

/// A factory descriptor that can create dynamic nodes
pub struct DynamicFactory {
    /// The name of the factory (e.g., "hostmount", "sql-derived-series")
    pub name: &'static str,
    
    /// Human-readable description of what this factory does
    pub description: &'static str,
    
    /// Context-aware function that creates a directory handle from configuration and context
    pub create_directory_with_context: Option<fn(config: Value, context: &FactoryContext) -> TinyFSResult<DirHandle>>,
    
    /// Context-aware function that creates a file handle from configuration and context
    pub create_file_with_context: Option<fn(config: Value, context: &FactoryContext) -> TinyFSResult<FileHandle>>,
    
    /// Function to validate configuration before creating nodes
    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,
}

/// Distributed slice containing all registered factories
#[distributed_slice]
pub static DYNAMIC_FACTORIES: [DynamicFactory];

/// Factory registry for looking up and creating dynamic nodes
pub struct FactoryRegistry;

impl FactoryRegistry {
    /// Get a factory by name
    pub fn get_factory(name: &str) -> Option<&'static DynamicFactory> {
        DYNAMIC_FACTORIES.iter().find(|factory| factory.name == name)
    }
    
    /// List all available factories
    pub fn list_factories() -> &'static [DynamicFactory] {
        &DYNAMIC_FACTORIES
    }
    
    /// Create a dynamic directory using the specified factory
    pub fn create_directory_with_context(factory_name: &str, config: &[u8], context: &FactoryContext) -> TinyFSResult<DirHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;
        
        let config_value = (factory.validate_config)(config)?;
        
        if let Some(create_fn) = factory.create_directory_with_context {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!("Factory '{}' does not support directories", factory_name)))
        }
    }
    
    /// Create a dynamic file using the specified factory
    pub fn create_file_with_context(factory_name: &str, config: &[u8], context: &FactoryContext) -> TinyFSResult<FileHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;
        
        let config_value = (factory.validate_config)(config)?;
        
        if let Some(create_fn) = factory.create_file_with_context {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!("Factory '{}' does not support files", factory_name)))
        }
    }
    
    /// Legacy create directory method (deprecated - use create_directory_with_context)
    pub fn create_directory(_factory_name: &str, _config: &[u8]) -> TinyFSResult<DirHandle> {
        Err(tinyfs::Error::Other("All factories now require context - use create_directory_with_context".to_string()))
    }
    
    /// Legacy create file method (deprecated - use create_file_with_context)
    pub fn create_file(_factory_name: &str, _config: &[u8]) -> TinyFSResult<FileHandle> {
        Err(tinyfs::Error::Other("All factories now require context - use create_file_with_context".to_string()))
    }
    
    /// Validate configuration for a specific factory
    pub fn validate_config(factory_name: &str, config: &[u8]) -> TinyFSResult<Value> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;
        
        (factory.validate_config)(config)
    }
}

/// Convenience macro for registering a dynamic factory
#[macro_export]
macro_rules! register_dynamic_factory {
    // Legacy pattern - deprecated, all factories should use context
    (
        name: $name:expr,
        description: $description:expr,
        directory: $_dir_fn:expr,
        validate: $validate_fn:expr
    ) => {
        compile_error!("Legacy factory pattern - use directory_with_context instead");
    };
    
    // Legacy pattern - deprecated, all factories should use context
    (
        name: $name:expr,
        description: $description:expr,
        file: $_file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        compile_error!("Legacy factory pattern - use file_with_context instead");
    };
    
    // Legacy pattern - deprecated, all factories should use context
    (
        name: $name:expr,
        description: $description:expr,
        directory: $_dir_fn:expr,
        file: $_file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        compile_error!("Legacy factory pattern - use directory_with_context and file_with_context instead");
    };
    
    (
        name: $name:expr,
        description: $description:expr,
        directory_with_context: $dir_fn:expr,
        validate: $validate_fn:expr
    ) => {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static FACTORY: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory_with_context: Some($dir_fn),
            create_file_with_context: None,
            validate_config: $validate_fn,
        };
    };
    
    (
        name: $name:expr,
        description: $description:expr,
        file_with_context: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        // Create a unique identifier from the factory name
        paste::paste! {
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory_with_context: None,
                create_file_with_context: Some($file_fn),
                validate_config: $validate_fn,
            };
        }
    };

    (
        factory_var: $factory_var:ident,
        name: $name:expr,
        description: $description:expr,
        file_with_context: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static $factory_var: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory_with_context: None,
            create_file_with_context: Some($file_fn),
            validate_config: $validate_fn,
        };
    };

    (
        name: $name:expr,
        description: $description:expr,
        directory_with_context: $dir_fn:expr,
        file_with_context: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static FACTORY: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory_with_context: Some($dir_fn),
            create_file_with_context: Some($file_fn),
            validate_config: $validate_fn,
        };
    };
}
