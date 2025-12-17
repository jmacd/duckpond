//! Provider: URL-Based file access and factory infrastructure

pub mod factory;
pub mod registry;
pub mod transform;

mod error;
mod format;
mod format_registry;
mod provider_api;
mod sql_transform;
mod table_creation;
mod table_provider_options;
mod tinyfs_object_store;
mod tinyfs_path;
mod url_pattern_matcher;
mod version_selection;

pub use error::{Error, Result};
pub use factory::dynamic_dir::{DynamicDirConfig, DynamicDirDirectory, DynamicDirEntry};
pub use factory::sql_derived::SqlDerivedConfig;
pub use format::FormatProvider;
pub use format_registry::{FORMAT_PROVIDERS, FormatProviderEntry, FormatRegistry};
pub use provider_api::Provider;
pub use registry::{
    ConfigFile, DYNAMIC_FACTORIES, DynamicFactory, ExecutionContext, ExecutionMode, FactoryCommand,
    FactoryRegistry, QueryableFile,
};
pub use sql_transform::transform_sql;
pub use table_creation::{
    create_latest_table_provider, create_listing_table_provider, create_table_provider,
};
pub use table_provider_options::{TableProviderKey, TableProviderOptions};
pub use tinyfs::{FactoryContext, PondMetadata, ProviderContext};
pub use tinyfs_object_store::{TinyFsObjectStore, register_tinyfs_object_store};
pub use tinyfs_path::TinyFsPathBuilder;
pub use url_pattern_matcher::{MatchedFile, UrlPatternMatcher};
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

impl PartialEq for Url {
    fn eq(&self, other: &Self) -> bool {
        self.inner.as_str() == other.inner.as_str()
    }
}

impl Eq for Url {}

impl serde::Serialize for Url {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.inner.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Url {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Url::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.as_str())
    }
}

impl Url {
    /// Parse URL from string using standard url crate
    /// Returns error if fragment, port, username, or password are present
    ///
    /// If the string doesn't contain "://", defaults to "file://" scheme
    /// The actual file type is determined by EntryType when the file is accessed
    pub fn parse(url_str: &str) -> Result<Self> {
        // If no scheme is present, default to "file" scheme
        let normalized_url = if !url_str.contains("://") {
            format!("file://{}", url_str)
        } else {
            url_str.to_string()
        };

        let inner = url::Url::parse(&normalized_url)?;

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
        if host.is_empty() { None } else { Some(host) }
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
            None => serde_qs::from_str("")
                .map_err(|e| Error::InvalidUrl(format!("query parameter parsing failed: {}", e))),
            Some(query) => serde_qs::from_str(query)
                .map_err(|e| Error::InvalidUrl(format!("query parameter parsing failed: {}", e))),
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
        format::compression::decompress(reader, url.compression())
    }
}
