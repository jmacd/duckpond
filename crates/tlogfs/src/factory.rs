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
use clap::Parser;

/// Execution mode for factory operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    PondReadWriter,
    ControlWriter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    mode: ExecutionMode,
    args: Vec<String>,
}

impl ExecutionContext {
    pub fn control_writer(args: Vec<String>) -> Self {
	Self {
	    mode: ExecutionMode::ControlWriter,
	    args,
	}
    }

    pub fn pond_readwriter(args: Vec<String>) -> Self {
	Self {
	    mode: ExecutionMode::PondReadWriter,
	    args,
	}
    }
}

pub trait FactoryCommand {
    fn allowed(&self) -> ExecutionMode;
}

impl ExecutionContext {
    pub fn to_command<T>(self) -> Result<T, TLogFSError>
    where T: Parser + FactoryCommand
    {
	// Clap expects the first argument to be the program name
	// Prepend a dummy program name if args is non-empty
	let args_with_prog_name = if self.args.is_empty() {
	    vec!["factory".to_string()]
	} else {
	    std::iter::once("factory".to_string())
		.chain(self.args.into_iter())
		.collect()
	};

	let cmd = T::try_parse_from(args_with_prog_name).map_err(|e| {
	    // Print Clap's helpful error message immediately (includes usage, available subcommands, etc.)
	    eprintln!("{}", e);
	    // Then convert to our error type
	    TLogFSError::Clap(e)
	})?;

	let allowed = cmd.allowed();
	if allowed == self.mode {
	    Ok(cmd)
	} else {
	    Err(TLogFSError::Internal(format!("incorrect execution mode: {:?} != {:?}", allowed, self.mode)))
	} 
    }
}

/// Pond identity metadata - immutable information about the pond's origin
/// This metadata is created once when the pond is initialized and preserved across replicas
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

impl PondMetadata {
    /// Create new pond metaedata for a freshly initialized pond
    pub fn new() -> Self {
        let pond_id = uuid7::uuid7();
        let birth_timestamp = chrono::Utc::now().timestamp_micros();
        
        let birth_hostname = std::net::hostname()
            .unwrap_or("localhost".into())
            .to_string_lossy()
            .to_string();
        
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

#[derive(Clone)]
pub struct FactoryContext {
    /// Access to the persistence layer for resolving pond nodes
    pub state: State,
    /// Parent node id for context-aware factories
    pub parent_node_id: NodeID,
    /// Pond identity metadata (pond_id, birth_timestamp, etc.)
    /// Provided by Steward when creating factory contexts
    pub pond_metadata: Option<PondMetadata>,
}

impl FactoryContext {
    /// Create a new factory context with the given state and parent_node_id
    /// Automatically extracts template variables and export data from the state
    /// NOTE! should be #[cfg(test)]
    pub fn new(state: State, parent_node_id: NodeID) -> Self {
        Self {
            state,
            parent_node_id,
            pond_metadata: None,
        }
    }
    
    /// Create a factory context with pond metadata
    pub fn with_metadata(
        state: State,
        parent_node_id: NodeID,
        pond_metadata: PondMetadata,
    ) -> Self {
        Self {
            state,
            parent_node_id,
            pond_metadata: Some(pond_metadata),
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
    pub name: &'static str,

    pub description: &'static str,

    pub create_directory:
        Option<fn(config: Value, context: FactoryContext) -> TinyFSResult<DirHandle>>,

    pub create_file: Option<
        fn(
            config: Value,
            context: FactoryContext,
        ) -> Pin<Box<dyn Future<Output = TinyFSResult<FileHandle>> + Send>>,
    >,

    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,

    pub try_as_queryable: Option<fn(&dyn tinyfs::File) -> Option<&dyn QueryableFile>>,

    pub initialize: Option<
        fn(
            config: Value,
            context: FactoryContext,
        ) -> Pin<Box<dyn Future<Output = Result<(), TLogFSError>> + Send>>,
    >,

    pub execute: Option<
        fn(
            config: Value,
            context: FactoryContext,
	    ctx: ExecutionContext,
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
    pub async fn create_file(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> TinyFSResult<FileHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        let config_value = (factory.validate_config)(config)?;

        if let Some(create_fn) = factory.create_file {
            create_fn(config_value, context).await
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

    /// Initialize a factory after node creation (runs outside lock)
    /// The caller manages the transaction; state is available via context
    pub async fn initialize(
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

        if let Some(initialize_fn) = factory.initialize {
            initialize_fn(config_value, context).await
        } else {
            // No initialization needed - this is fine
            Ok(())
        }
    }

    /// Execute a run configuration using the specified factory
    /// The caller manages the transaction; state is available via context
    /// 
    /// Args: Command arguments passed to the factory (e.g., ["push"] or ["pull"])
    pub async fn execute(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
        ctx: ExecutionContext,
    ) -> Result<(), TLogFSError> {
        let factory = Self::get_factory(factory_name).ok_or_else(|| {
            TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Unknown factory: {}",
                factory_name
            )))
        })?;

        let config_value = (factory.validate_config)(config).map_err(|e| TLogFSError::TinyFS(e))?;

        if let Some(execute_fn) = factory.execute {
            execute_fn(config_value, context, ctx).await
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
            initialize: None,
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
            // Generate a wrapper for file creation that returns Pin<Box<dyn Future>>
            fn [<file_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tinyfs::Result<tinyfs::FileHandle>> + Send>> {
                // Wrap synchronous function in async block
                Box::pin(async move { $file_fn(config, context) })
            }

            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some([<file_wrapper_ $name:snake>]),
                validate_config: $validate_fn,
                try_as_queryable: None,
                initialize: None,
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
            // Generate a wrapper for file creation that returns Pin<Box<dyn Future>>
            fn [<file_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tinyfs::Result<tinyfs::FileHandle>> + Send>> {
                // Wrap synchronous function in async block
                Box::pin(async move { $file_fn(config, context) })
            }

            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some([<file_wrapper_ $name:snake>]),
                validate_config: $validate_fn,
                try_as_queryable: Some($queryable_fn),
                initialize: None,
                execute: None,
            };
        }
    };
}

/// Register an executable factory (for run configurations like hydrovu)
/// 
/// The file function should be an async function with signature:
/// `async fn(Value, FactoryContext) -> TinyFSResult<FileHandle>`
/// 
/// The initialize function (optional) should be an async function with signature:
/// `async fn(Value, FactoryContext) -> Result<(), TLogFSError>`
/// 
/// The execute function should be an async function with signature:
/// `async fn(Value, FactoryContext, ExecutionMode, Vec<String>) -> Result<(), TLogFSError>`
/// 
/// The macro will automatically wrap all to return Pin<Box<dyn Future>>
#[macro_export]
macro_rules! register_executable_factory {
    (
        name: $name:expr,
        description: $description:expr,
        validate: $validate_fn:expr,
        initialize: $init_fn:expr,
        execute: $execute_fn:expr
    ) => {
        paste::paste! {
            // Generate a wrapper for initialization that returns Pin<Box<dyn Future>>
            fn [<initialize_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), $crate::TLogFSError>> + Send>> {
                Box::pin($init_fn(config, context))
            }

            // Generate a wrapper for execution that returns Pin<Box<dyn Future>>
            fn [<execute_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
                ctx: $crate::factory::ExecutionContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), $crate::TLogFSError>> + Send>> {
                Box::pin($execute_fn(config, context, ctx))
            }

            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                // Executable factories don't need create_file - the config bytes ARE the file content
                create_file: None,
                validate_config: $validate_fn,
                try_as_queryable: None,
                initialize: Some([<initialize_wrapper_ $name:snake>]),
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
