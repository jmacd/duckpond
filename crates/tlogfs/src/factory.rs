// Dynamic factory registration system using linkme
use linkme::distributed_slice;
use serde_json::Value;
use async_trait::async_trait;
use tinyfs::{DirHandle, FileHandle, Result as TinyFSResult, NodeID, EntryType};
use crate::persistence::State;
use crate::query::queryable_file::QueryableFile;
use crate::TLogFSError;

#[derive(Clone)]
pub struct FactoryContext {
    /// Access to the persistence layer for resolving pond nodes
    pub state: State,

    /// Parent node id for context-aware factories
    /// @@@
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
    pub async fn create_cache_key(&self, entry_name: &str) -> Result<crate::persistence::DynamicNodeKey, crate::TLogFSError> {
        Ok(crate::persistence::DynamicNodeKey::new(self.parent_node_id, entry_name.to_string()))
    }
}

/// A factory descriptor that can create dynamic nodes
pub struct DynamicFactory {
    /// The name of the factory (e.g., "hostmount", "sql-derived-series")
    pub name: &'static str,

    /// Human-readable description of what this factory does
    pub description: &'static str,

    /// The entry this factory manages
    pub entry_type: EntryType,

    /// Context-aware function that creates a directory handle from configuration and context
    pub create_directory: Option<fn(config: Value, context: FactoryContext) -> TinyFSResult<DirHandle>>,

    /// Context-aware function that creates a file handle from configuration and context
    pub create_file: Option<fn(config: Value, context: FactoryContext) -> TinyFSResult<FileHandle>>,

    /// Function to validate configuration before creating nodes
    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,

    /// Optional function to check if a file is queryable and downcast it
    pub(crate) try_as_queryable: Option<fn(&dyn tinyfs::File) -> Option<&dyn QueryableFile>>,

    /// Optional function to check if a file is a runner.
    pub execute: Option<fn(&dyn tinyfs::File, FactoryContext) -> TinyFSResult<&dyn Runner>>,
}

#[async_trait]
pub trait Runner {
    async fn run(&self) -> Result<(), TLogFSError>;
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
    pub fn create_directory(factory_name: &str, config: &[u8], context: FactoryContext) -> TinyFSResult<DirHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        let config_value = (factory.validate_config)(config)?;

        if let Some(create_fn) = factory.create_directory {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!("Factory '{}' does not support directories", factory_name)))
        }
    }

    /// Create a dynamic file using the specified factory
    pub fn create_file(factory_name: &str, config: &[u8], context: FactoryContext) -> TinyFSResult<FileHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        let config_value = (factory.validate_config)(config)?;

        if let Some(create_fn) = factory.create_file {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!("Factory '{}' does not support files", factory_name)))
        }
    }

    /// Validate configuration for a specific factory
    pub fn validate_config(factory_name: &str, config: &[u8]) -> TinyFSResult<Value> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        (factory.validate_config)(config)
    }

    /// Execute a run configuration using the specified factory
    pub async fn execute(
        factory_name: &str,
	target_file: &dyn tinyfs::File,
        context: FactoryContext,
    ) -> Result<(), TLogFSError> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| TLogFSError::TinyFS(
                tinyfs::Error::Other(format!("Unknown factory: {}", factory_name))
            ))?;

        if let Some(execute_fn) = factory.execute {
            execute_fn(target_file, context)?.run().await
        } else {
            Err(TLogFSError::TinyFS(tinyfs::Error::Other(
                format!("Factory '{}' does not support execution", factory_name)
            )))
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
	    entry_type: EntryType::DirectoryDynamic,
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
	entry_type: $entry_type:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        paste::paste! {
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
		entry_type: $entry_type,
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
	entry_type: $entry_type:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        try_as_queryable: $queryable_fn:expr
    ) => {
        paste::paste! {
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
		entry_type: $entry_type,
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
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
		entry_type: EntryType::FileDataDynamic,
                create_directory: None,
                create_file: Some($file_fn),
                validate_config: $validate_fn,
                try_as_queryable: None,
                execute: Some($execute_fn),
            };
        }
    };
}
