// Dynamic factory registration system using linkme
use crate::TLogFSError;
use crate::persistence::State;
use crate::query::queryable_file::QueryableFile;
use linkme::distributed_slice;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{DirHandle, FileHandle, NodeID, Result as TinyFSResult};
use tokio::sync::Mutex;
use async_trait::async_trait;
use std::any::Any;
use tinyfs::{AsyncReadSeek, File, NodeMetadata, EntryType, Metadata};

#[derive(Clone)]
pub struct FactoryContext {
    /// Access to the persistence layer for resolving pond nodes
    pub state: State,
    /// Parent node id for context-aware factories
    pub parent_node_id: NodeID,
}

impl FactoryContext {
    /// Create a new factory context with the given state and parent_node_id
    /// Automatically extracts template variables and export data from the state
    pub fn new(state: State, parent_node_id: NodeID) -> Self {
        Self {
            state,
            parent_node_id,
        }
    }

    /// Create a cache key for dynamic directory factory
    pub async fn create_cache_key(
        &self,
        entry_name: &str,
    ) -> Result<crate::persistence::DynamicNodeKey, crate::TLogFSError> {
        Ok(crate::persistence::DynamicNodeKey::new(
            self.parent_node_id,
            entry_name.to_string(),
        ))
    }
}

/// A factory descriptor that can create dynamic nodes
pub struct DynamicFactory {
    /// The name of the factory (e.g., "hostmount", "sql-derived-series")
    pub name: &'static str,

    /// Human-readable description of what this factory does
    pub description: &'static str,

    /// Context-aware function that creates a directory handle from configuration and context
    pub create_directory:
        Option<fn(config: Value, context: FactoryContext) -> TinyFSResult<DirHandle>>,

    /// Context-aware function that creates a file handle from configuration and context
    pub create_file:
        Option<fn(config: Value, context: FactoryContext) -> TinyFSResult<FileHandle>>,

    /// Function to validate configuration before creating nodes
    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,

    /// Optional function to check if a file is queryable and downcast it
    pub try_as_queryable: Option<fn(&dyn tinyfs::File) -> Option<&dyn QueryableFile>>,

    /// Optional function to execute a run configuration
    /// Used for factories that represent executable configurations (e.g., hydrovu collector)
    /// The transaction is managed by the caller, state is available via context
    /// Takes ownership of FactoryContext since the runner has exclusive control
    pub execute: Option<
        fn(
            config: Value,
            context: FactoryContext,
        ) -> Pin<Box<dyn Future<Output = Result<(), TLogFSError>> + Send>>,
    >,
}

/// Distributed slice containing all registered factories
#[distributed_slice]
pub static DYNAMIC_FACTORIES: [DynamicFactory];

/// Factory registry for looking up and creating dynamic nodes
pub struct FactoryRegistry;

impl FactoryRegistry {
    /// Get a factory by name
    pub fn get_factory(name: &str) -> Option<&'static DynamicFactory> {
        DYNAMIC_FACTORIES
            .iter()
            .find(|factory| factory.name == name)
    }

    /// List all available factories
    pub fn list_factories() -> &'static [DynamicFactory] {
        &DYNAMIC_FACTORIES
    }

    /// Create a dynamic directory using the specified factory
    pub fn create_directory(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> TinyFSResult<DirHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        let config_value = (factory.validate_config)(config)?;

        if let Some(create_fn) = factory.create_directory {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!(
                "Factory '{}' does not support directories",
                factory_name
            )))
        }
    }

    /// Create a dynamic file using the specified factory
    pub fn create_file(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> TinyFSResult<FileHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        let config_value = (factory.validate_config)(config)?;

        if let Some(create_fn) = factory.create_file {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!(
                "Factory '{}' does not support files",
                factory_name
            )))
        }
    }

    /// Validate configuration for a specific factory
    pub fn validate_config(factory_name: &str, config: &[u8]) -> TinyFSResult<Value> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        (factory.validate_config)(config)
    }

    /// Execute a run configuration using the specified factory
    /// The caller manages the transaction; state is available via context
    pub async fn execute(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> Result<(), TLogFSError> {
        let factory = Self::get_factory(factory_name).ok_or_else(|| {
            TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Unknown factory: {}",
                factory_name
            )))
        })?;

        let config_value = (factory.validate_config)(config).map_err(|e| TLogFSError::TinyFS(e))?;

        if let Some(execute_fn) = factory.execute {
            execute_fn(config_value, context).await
        } else {
            Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Factory '{}' does not support execution",
                factory_name
            ))))
        }
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
        paste::paste! {
        #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
        static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
            name: $name,
            description: $description,
            create_directory: Some($dir_fn),
            create_file: None,
            validate_config: $validate_fn,
            try_as_queryable: None,
            execute: None,
        };
	}
    };

    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        paste::paste! {
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some($file_fn),
                validate_config: $validate_fn,
                try_as_queryable: None,
                execute: None,
            };
        }
    };

    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        try_as_queryable: $queryable_fn:expr
    ) => {
        paste::paste! {
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some($file_fn),
                validate_config: $validate_fn,
                try_as_queryable: Some($queryable_fn),
                execute: None,
            };
        }
    };
}

/// Register an executable factory (for run configurations like hydrovu)
/// 
/// The execute function should be an async function with signature:
/// `async fn(ConfigType, FactoryContext) -> Result<(), TLogFSError>`
/// 
/// The macro will automatically wrap it to return Pin<Box<dyn Future>>
#[macro_export]
macro_rules! register_executable_factory {
    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        execute: $execute_fn:expr
    ) => {
        paste::paste! {
            // Generate a wrapper function that returns Pin<Box<dyn Future>>
            fn [<execute_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), $crate::TLogFSError>> + Send>> {
                Box::pin($execute_fn(config, context))
            }

            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some($file_fn),
                validate_config: $validate_fn,
                try_as_queryable: None,
                execute: Some([<execute_wrapper_ $name:snake>]),
            };
        }
    };
}

/// Config file that stores configuration as YAML, supports an
/// associated run factory.
pub struct ConfigFile {
    config_yaml: Vec<u8>,
}

impl ConfigFile {
    pub fn new(config_yaml: Vec<u8>) -> Self {
        Self {
            config_yaml,
        }
    }

    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self) as Box<dyn File>)))
    }
}

#[async_trait]
impl File for ConfigFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn AsyncReadSeek>>> {
        use std::io::Cursor;
        let cursor = Cursor::new(self.config_yaml.clone());
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other(
            "Config files are read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl Metadata for ConfigFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: Some(self.config_yaml.len() as u64),
            sha256: None,
            entry_type: EntryType::FileDataDynamic,
            timestamp: 0,
        })
    }
}
