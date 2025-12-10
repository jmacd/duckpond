//! Provider: URL-Based file access and factory infrastructure

pub mod factory;

#[cfg(test)]
mod context_tests;
#[cfg(test)]
mod csv_timeseries_integration_test;
mod compression;
mod csv;
mod error;
mod format;
mod format_registry;
mod null_padding;
mod provider_api;
pub mod registry;
mod scope_prefix;
mod sql_transform;
mod table_creation;
mod table_provider_options;
mod temporal_filter;
mod tinyfs_object_store;
mod tinyfs_path;
mod version_selection;

// Re-export context types from tinyfs (they moved there to break circular dependency)
pub use compression::decompress;
pub use csv::{CsvOptions, CsvProvider};
pub use factory::dynamic_dir::{DynamicDirConfig, DynamicDirDirectory, DynamicDirEntry};
pub use error::{Error, Result};
pub use format::FormatProvider;
pub use format_registry::{FormatRegistry, FormatProviderEntry, FORMAT_PROVIDERS};
pub use null_padding::null_padding_table;
pub use provider_api::Provider;
pub use registry::{
    ConfigFile, DYNAMIC_FACTORIES, DynamicFactory, ExecutionContext, ExecutionMode, FactoryCommand,
    FactoryRegistry, QueryableFile,
};
pub use scope_prefix::ScopePrefixTableProvider;
pub use factory::sql_derived::SqlDerivedConfig;
pub use sql_transform::transform_sql;
pub use table_creation::{
    create_latest_table_provider, create_listing_table_provider, create_table_provider,
};
pub use table_provider_options::{TableProviderKey, TableProviderOptions};
pub use temporal_filter::TemporalFilteredListingTable;
pub use tinyfs::{FactoryContext, PondMetadata, ProviderContext};
pub use tinyfs_object_store::TinyFsObjectStore;
pub use tinyfs_path::TinyFsPathBuilder;
pub use version_selection::VersionSelection;

use std::pin::Pin;
use tokio::io::AsyncRead;

/// URL for accessing files with format conversion and optional compression
/// 
/// Format: `scheme://[compression]/path/pattern?query_params`
/// 
/// - `scheme`: Format name (csv, oteljson, etc.) - indicates FormatProvider
/// - `host`: Optional compression (zstd, gzip, bzip2)
/// - `path`: TinyFS path or glob pattern
/// - `query`: Format-specific options (delimiter, batch_size, etc.)
/// 
/// Examples:
/// - `csv:///data/file.csv?delimiter=;`
/// - `oteljson://zstd/logs/**/*.json.zstd?batch_size=2048`
/// - `csv://gzip/metrics/*.csv.gz?has_header=false`
#[derive(Debug, Clone)]
pub struct Url {
    inner: url::Url,
}

impl Url {
    /// Parse URL from string using standard url crate
    /// Returns error if fragment, port, username, or password are present
    pub fn parse(url_str: &str) -> Result<Self> {
        let inner = url::Url::parse(url_str)?;

        if inner.fragment().is_some() {
            return Err(Error::InvalidUrl("fragment not allowed".into()));
        }
        if inner.port().is_some() {
            return Err(Error::InvalidUrl("port not allowed".into()));
        }
        if !inner.username().is_empty() {
            return Err(Error::InvalidUrl("username not allowed".into()));
        }
        if inner.password().is_some() {
            return Err(Error::InvalidUrl("password not allowed".into()));
        }

        Ok(Self { inner })
    }

    /// Get format scheme (e.g., "csv", "oteljson")
    pub fn scheme(&self) -> &str {
        self.inner.scheme()
    }

    /// Get optional compression from host (e.g., "zstd", "gzip")
    pub fn compression(&self) -> Option<&str> {
        let host = self.inner.host_str()?;
        if host.is_empty() {
            None
        } else {
            Some(host)
        }
    }

    /// Get TinyFS path or pattern
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Parse query parameters into strongly-typed struct using serde_qs
    /// 
    /// Example:
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct CsvOptions {
    ///     delimiter: char,
    ///     has_header: bool,
    /// }
    /// 
    /// let url = Url::parse("csv:///file.csv?delimiter=;&has_header=false")?;
    /// let options: CsvOptions = url.query_params()?;
    /// ```
    pub fn query_params<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        match self.inner.query() {
            None => serde_qs::from_str("").map_err(|e| {
                Error::InvalidUrl(format!("query parameter parsing failed: {}", e))
            }),
            Some(query) => serde_qs::from_str(query).map_err(|e| {
                Error::InvalidUrl(format!("query parameter parsing failed: {}", e))
            }),
        }
    }
}

/// Trait for URL access to Tinyfs.
#[async_trait::async_trait]
pub trait FileProvider {
    /// Open a URL with optional decompression
    async fn open_url(&self, url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>>;
}

#[async_trait::async_trait]
impl FileProvider for tinyfs::FS {
    /// Open a file for reading with optional decompression (Layer 1)
    async fn open_url(&self, url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Get reader from TinyFS
        let reader = self.root().await?.async_reader_path(url.path()).await?;

        // Cast AsyncReadSeek to AsyncRead before decompression
        let reader: Pin<Box<dyn AsyncRead + Send>> = reader as Pin<Box<dyn AsyncRead + Send>>;

        // Wrap with decompression if needed (uses Layer 1 compression module)
        decompress(reader, url.compression())
    }
}
