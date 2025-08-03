// Dynamic factory registration system using linkme
use linkme::distributed_slice;
use serde_json::Value;
use std::sync::Arc;
use tinyfs::{DirHandle, FileHandle, Result as TinyFSResult};

// Forward declaration for context
use crate::persistence::OpLogPersistence;

/// Factory context providing access to the pond for resolving source nodes
pub struct FactoryContext {
    /// Access to the persistence layer for resolving pond nodes
    pub persistence: Arc<OpLogPersistence>,
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
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static FACTORY: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
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
