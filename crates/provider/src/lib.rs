// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Provider: URL-Based file access and factory infrastructure

pub mod export;
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

use futures::StreamExt;
use std::pin::Pin;
use tokio::io::AsyncRead;

/// URL for accessing files with format conversion and optional compression
///
/// Format: `[host+]scheme[+compression]:///path/pattern[?query_params]`
///
/// - `host+`: Optional prefix indicating the path refers to the host filesystem
///   rather than a pond-internal path. When present, no pond/POND env is needed.
/// - `scheme`: Format name (csv, oteljson, etc.) - indicates FormatProvider
///   Compression is encoded as a `+suffix` on the scheme (e.g., `csv+gzip`)
///   Supported compressions: zstd, gzip, bzip2
/// - `path`: Filesystem path (host) or TinyFS path/glob pattern (pond)
/// - `query`: Format-specific options (delimiter, batch_size, etc.)
///
/// The URL host component (authority) must be empty.
///
/// Examples:
/// - `csv:///data/file.csv?delimiter=;`           — pond CSV file
/// - `host+oteljson:///tmp/metrics.json`           — host OtelJSON file
/// - `host+csv+gzip:///tmp/data.csv.gz`            — host gzipped CSV file
/// - `oteljson+zstd:///logs/**/*.json.zstd`        — pond compressed OtelJSON
#[derive(Debug, Clone)]
pub struct Url {
    inner: url::Url,
    /// The base format scheme (e.g., "csv", "oteljson"), without host prefix or compression suffix
    format_scheme: String,
    /// Optional compression extracted from scheme suffix (e.g., "gzip", "zstd")
    compression: Option<String>,
    /// Whether the URL targets the host filesystem (leading `host+` prefix)
    is_host: bool,
}

impl PartialEq for Url {
    fn eq(&self, other: &Self) -> bool {
        self.format_scheme == other.format_scheme
            && self.compression == other.compression
            && self.is_host == other.is_host
            && self.inner.path() == other.inner.path()
            && self.inner.query() == other.inner.query()
    }
}

impl Eq for Url {}

impl serde::Serialize for Url {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
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
        // Reconstruct canonical form: [host+]scheme[+compression]:///path[?query]
        if self.is_host {
            write!(f, "host+")?;
        }
        write!(f, "{}://", self.full_scheme())?;
        write!(f, "{}", self.inner.path())?;
        if let Some(query) = self.inner.query() {
            write!(f, "?{}", query)?;
        }
        Ok(())
    }
}

impl Url {
    /// Known compression suffixes that can appear after `+` in the scheme
    const KNOWN_COMPRESSIONS: &[&str] = &["zstd", "gzip", "bzip2"];

    /// Parse URL from string using standard url crate
    ///
    /// Scheme segments are parsed left-to-right:
    ///   `host+oteljson+gzip:///path`
    ///    │     │         │
    ///    │     │         └── compression (known: zstd, gzip, bzip2)
    ///    │     └──────────── format provider (csv, oteljson, etc.)
    ///    └────────────────── source: host filesystem
    ///
    /// Returns error if fragment, port, username, password, or non-empty host are present.
    ///
    /// If the string doesn't contain "://", defaults to "file://" scheme.
    /// The actual file type is determined by EntryType when the file is accessed.
    pub fn parse(url_str: &str) -> Result<Self> {
        // Strip leading `host+` before handing to the url crate.
        // We do this at the string level so the url crate never sees `host+oteljson`
        // as an opaque compound scheme.
        let (is_host, effective_url) = if let Some(rest) = url_str.strip_prefix("host+") {
            (true, rest.to_string())
        } else {
            (false, url_str.to_string())
        };

        // If no scheme is present, default to "file" scheme
        let normalized_url = if !effective_url.contains("://") {
            format!("file://{}", effective_url)
        } else {
            effective_url
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

        // Reject non-empty host — the host position is reserved
        if let Some(host) = inner.host_str().filter(|h| !h.is_empty()) {
            return Err(Error::InvalidUrl(format!(
                "non-empty host '{}' is not allowed in provider URLs; \
                 use scheme+compression syntax instead (e.g., csv+gzip:///path)",
                host
            )));
        }

        // Extract compression from scheme suffix (e.g., "csv+gzip" → ("csv", Some("gzip")))
        let raw_scheme = inner.scheme().to_string();
        let (format_scheme, compression) = if let Some(pos) = raw_scheme.rfind('+') {
            let suffix = &raw_scheme[pos + 1..];
            if Self::KNOWN_COMPRESSIONS.contains(&suffix) {
                (raw_scheme[..pos].to_string(), Some(suffix.to_string()))
            } else {
                // Unknown suffix — treat the whole thing as the scheme
                (raw_scheme, None)
            }
        } else {
            (raw_scheme, None)
        };

        Ok(Self {
            inner,
            format_scheme,
            compression,
            is_host,
        })
    }

    /// Get the base format scheme (e.g., "csv", "oteljson")
    ///
    /// This returns only the format portion, without any compression suffix.
    /// For `csv+gzip:///path`, this returns `"csv"`.
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.format_scheme
    }

    /// Get the full scheme including compression suffix (e.g., "csv+gzip")
    ///
    /// This is the raw scheme as it appears in the URL.
    #[must_use]
    pub fn full_scheme(&self) -> &str {
        self.inner.scheme()
    }

    /// Get optional compression from scheme suffix (e.g., "gzip", "zstd")
    ///
    /// Compression is encoded as `+suffix` on the scheme:
    /// - `csv+gzip:///path` → `Some("gzip")`
    /// - `csv:///path` → `None`
    #[must_use]
    pub fn compression(&self) -> Option<&str> {
        self.compression.as_deref()
    }

    /// Whether this URL targets the host filesystem (has `host+` prefix)
    #[must_use]
    pub fn is_host(&self) -> bool {
        self.is_host
    }

    /// Get TinyFS path or pattern (also used for host filesystem paths)
    #[must_use]
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

/// Open a host filesystem file as an AsyncRead with optional decompression.
///
/// This does NOT require a pond — it reads directly from the local filesystem.
/// Used by `pond cat host+format:///path` to process local files through
/// format providers (e.g., oteljson, csv).
pub async fn open_host_url(url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
    if !url.is_host() {
        return Err(Error::InvalidUrl(
            "open_host_url requires a host+ URL".into(),
        ));
    }

    let path = url.path();
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| Error::InvalidUrl(format!("Failed to open host file '{}': {}", path, e)))?;

    let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(file);

    // Wrap with decompression if needed
    format::compression::decompress(reader, url.compression())
}

/// Create a DataFusion MemTable from a host filesystem URL.
///
/// This is the standalone pipeline for `pond cat host+format:///path`:
///   host file → decompress → format_provider → MemTable
///
/// No pond or TinyFS required.
pub async fn create_memtable_from_host_url(
    url: &Url,
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
    let format_provider = FormatRegistry::get_provider(url.scheme()).ok_or_else(|| {
        Error::InvalidUrl(format!(
            "Unknown format '{}' in host URL. Known formats: csv, oteljson, excelhtml",
            url.scheme()
        ))
    })?;

    let reader = open_host_url(url).await?;

    let (schema, mut stream) = format_provider.open_stream(reader, url).await?;

    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result?);
    }

    let table =
        datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_err(|e| {
            Error::InvalidUrl(format!("Failed to create MemTable from host file: {}", e))
        })?;

    Ok(std::sync::Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_oteljson_url() {
        let url = Url::parse("host+oteljson:///tmp/traces.json").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "oteljson");
        assert_eq!(url.path(), "/tmp/traces.json");
        assert_eq!(url.compression(), None);
        assert_eq!(url.to_string(), "host+oteljson:///tmp/traces.json");
    }

    #[test]
    fn test_host_csv_gzip_url() {
        let url = Url::parse("host+csv+gzip:///data/logs.csv.gz").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.compression(), Some("gzip"));
        assert_eq!(url.path(), "/data/logs.csv.gz");
        assert_eq!(url.to_string(), "host+csv+gzip:///data/logs.csv.gz");
    }

    #[test]
    fn test_host_csv_no_compression() {
        let url = Url::parse("host+csv:///tmp/data.csv").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.compression(), None);
        assert_eq!(url.path(), "/tmp/data.csv");
    }

    #[test]
    fn test_non_host_url_unchanged() {
        let url = Url::parse("csv:///pond/data.csv").unwrap();
        assert!(!url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.path(), "/pond/data.csv");
        assert_eq!(url.to_string(), "csv:///pond/data.csv");
    }

    #[test]
    fn test_bare_path_not_host() {
        let url = Url::parse("/some/path").unwrap();
        assert!(!url.is_host());
        assert_eq!(url.scheme(), "file");
    }

    #[test]
    fn test_host_url_equality() {
        let a = Url::parse("host+oteljson:///tmp/a.json").unwrap();
        let b = Url::parse("host+oteljson:///tmp/a.json").unwrap();
        let c = Url::parse("oteljson:///tmp/a.json").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c); // host vs non-host differ
    }

    #[test]
    fn test_host_url_with_query_params() {
        let url = Url::parse("host+csv:///tmp/data.csv?delimiter=;").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.path(), "/tmp/data.csv");
    }
}
