// Dynamic factory registration system using linkme
use linkme::distributed_slice;
use serde_json::Value;
use tinyfs::{DirHandle, FileHandle, Result as TinyFSResult};

/// A factory descriptor that can create dynamic nodes
pub struct DynamicFactory {
    /// The name of the factory (e.g., "hostmount", "sql-derived-series")
    pub name: &'static str,
    
    /// Human-readable description of what this factory does
    pub description: &'static str,
    
    /// Function that creates a directory handle from configuration
    pub create_directory: Option<fn(config: Value) -> TinyFSResult<DirHandle>>,
    
    /// Function that creates a file handle from configuration
    pub create_file: Option<fn(config: Value) -> TinyFSResult<FileHandle>>,
    
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
    pub fn create_directory(factory_name: &str, config: &[u8]) -> TinyFSResult<DirHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;
        
        let create_fn = factory.create_directory
            .ok_or_else(|| tinyfs::Error::Other(format!("Factory '{}' does not support directories", factory_name)))?;
        
        let config_value = (factory.validate_config)(config)?;
        create_fn(config_value)
    }
    
    /// Create a dynamic file using the specified factory
    pub fn create_file(factory_name: &str, config: &[u8]) -> TinyFSResult<FileHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;
        
        let create_fn = factory.create_file
            .ok_or_else(|| tinyfs::Error::Other(format!("Factory '{}' does not support files", factory_name)))?;
        
        let config_value = (factory.validate_config)(config)?;
        create_fn(config_value)
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
    (
        name: $name:expr,
        description: $description:expr,
        directory: $dir_fn:expr,
        validate: $validate_fn:expr
    ) => {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static FACTORY: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory: Some($dir_fn),
            create_file: None,
            validate_config: $validate_fn,
        };
    };
    
    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static FACTORY: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory: None,
            create_file: Some($file_fn),
            validate_config: $validate_fn,
        };
    };
    
    (
        name: $name:expr,
        description: $description:expr,
        directory: $dir_fn:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static FACTORY: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory: Some($dir_fn),
            create_file: Some($file_fn),
            validate_config: $validate_fn,
        };
    };
}
