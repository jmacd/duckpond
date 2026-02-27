// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Layer 3: Provider API - URL-based TableProvider creation
//!
//! Bridges FormatProviders (Layer 2) with DataFusion TableProviders.
//! Handles:
//! - URL pattern expansion (glob support)
//! - Format provider lookup by scheme
//! - Multi-file union for patterns
//! - Table registration in DataFusion
//!
//! This enables using `csv:///path/*.csv` in factory configs like timeseries_join.

use crate::{Error, FileProvider, FormatProvider, FormatRegistry, Result, Url};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::future::Future;
use std::sync::Arc;

/// Provider for creating DataFusion TableProviders from URL patterns
///
/// Supports:
/// - Single file: `csv:///data/file.csv`
/// - Glob patterns: `csv:///data/**/*.csv`
/// - Compression: `csv+gzip:///data/*.csv.gz`
/// - Format options: `csv:///data/file.csv?delimiter=;`
/// - Builtin types: `file:///path`, `series:///path`, `table:///path`
pub struct Provider {
    /// TinyFS for file access
    fs: Arc<tinyfs::FS>,
    /// Optional ProviderContext for builtin type support (requires active transaction)
    provider_context: Option<Arc<tinyfs::ProviderContext>>,
}

impl Provider {
    /// Create a new Provider with TinyFS (without ProviderContext)
    /// This will only support external format providers (csv, oteljson, etc.)
    #[must_use]
    pub fn new(fs: Arc<tinyfs::FS>) -> Self {
        Self {
            fs,
            provider_context: None,
        }
    }

    /// Create a new Provider with ProviderContext (supports all types)
    /// Use this when you have an active transaction and want to query builtin types
    #[must_use]
    pub fn with_context(
        fs: Arc<tinyfs::FS>,
        provider_context: Arc<tinyfs::ProviderContext>,
    ) -> Self {
        Self {
            fs,
            provider_context: Some(provider_context),
        }
    }

    /// Create a TableProvider from a URL pattern
    ///single-file URL
    ///
    /// Uses global format registry to look up format providers by scheme.
    /// Streams data into MemTable.
    ///
    /// NOTE: Does NOT support wildcard patterns. Callers must expand patterns
    /// using TinyFS collect_matches() and call this method for each matched file.
    ///
    /// # Arguments
    ///
    /// * `url_str` - URL like `csv:///data/file.csv` (NO wildcards)
    /// * `ctx` - DataFusion session context for registration
    ///
    /// # Returns
    ///
    /// TableProvider that can be registered and queried
    pub async fn create_table_provider(
        &self,
        url_str: &str,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let url = Url::parse(url_str)?;

        // Check for wildcards - caller should expand patterns
        let path = url.path();
        if path.contains('*') || path.contains('?') {
            return Err(Error::InvalidUrl(format!(
                "URL '{}' contains wildcards. Caller must expand patterns using TinyFS collect_matches() and call create_table_provider() for each file.",
                url_str
            )));
        }

        let scheme = url.scheme();

        // Check if this is a builtin TinyFS type (file, series, table, data)
        if matches!(scheme, "file" | "series" | "table" | "data") {
            let provider_context = self.provider_context.as_ref().ok_or_else(|| {
                Error::InvalidUrl(format!(
                    "Builtin type '{}' requires ProviderContext. Use Provider::with_context()",
                    scheme
                ))
            })?;
            return self
                .create_builtin_table_provider(&url, provider_context)
                .await;
        }

        // External format providers (csv, oteljson, excelhtml, etc.)
        let format_provider = FormatRegistry::get_provider(scheme)
            .ok_or_else(|| Error::InvalidUrl(format!("Unknown format: {}", scheme)))?;

        // Try cache path if ProviderContext with cache_dir is available
        if let Some(ref provider_context) = self.provider_context
            && let Some(cache_dir) = provider_context.cache_dir()
        {
            return self
                .create_cached_table_from_url(
                    &url,
                    format_provider.as_ref(),
                    cache_dir,
                    &provider_context.datafusion_session,
                )
                .await;
        }

        // No cache available -- fall back to MemTable
        self.create_memtable_from_url(&url, format_provider.as_ref())
            .await
    }

    /// Create TableProvider from builtin TinyFS file using QueryableFile trait
    async fn create_builtin_table_provider(
        &self,
        url: &Url,
        provider_context: &tinyfs::ProviderContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use tinyfs::Lookup;

        let root = self.fs.root().await?;
        let path = url.path();

        let (_, lookup_result) = root
            .resolve_path(path)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to resolve path '{}': {}", path, e)))?;

        let node_path = match lookup_result {
            Lookup::Found(np) => np,
            _ => return Err(Error::InvalidUrl(format!("File not found: {}", path))),
        };

        let file_handle = node_path
            .as_file()
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to get file handle: {}", e)))?;

        let file_arc = file_handle.handle.get_file().await;
        let file_guard = file_arc.lock().await;

        let queryable_file = file_guard.as_queryable().ok_or_else(|| {
            Error::InvalidUrl(format!(
                "File '{}' is not queryable (type: {:?})",
                path,
                node_path.id().entry_type()
            ))
        })?;

        let table_provider = queryable_file
            .as_table_provider(node_path.id(), provider_context)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to create table provider: {}", e)))?;

        drop(file_guard);
        Ok(table_provider)
    }

    /// Expand pattern and invoke callback for each matched file
    ///
    /// For patterns with wildcards, expands using TinyFS and creates a TableProvider
    /// for each match, calling the callback with the provider and file path.
    /// For single files, calls callback once.
    ///
    /// # Arguments
    ///
    /// * `url_str` - URL pattern (e.g., `excelhtml:///data/*.htm` or `csv:///file.csv`)
    /// * `callback` - Called for each match with (TableProvider, file_path)
    ///
    /// # Returns
    ///
    /// Number of files matched
    pub async fn for_each_match<F, Fut>(&self, url_str: &str, mut callback: F) -> Result<usize>
    where
        F: FnMut(Arc<dyn datafusion::catalog::TableProvider>, String) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let url = Url::parse(url_str)?;
        let path = url.path();
        let has_wildcards = path.contains('*') || path.contains('?');

        if has_wildcards {
            // Expand pattern
            let root = self.fs.root().await?;
            let matches = root.collect_matches(path).await.map_err(|e| {
                Error::InvalidUrl(format!("Pattern expansion failed for '{}': {}", path, e))
            })?;

            if matches.is_empty() {
                return Err(Error::InvalidUrl(format!(
                    "No files match pattern: {}",
                    url_str
                )));
            }

            let ctx = SessionContext::new();
            for (node_path, _captures) in &matches {
                let file_path = node_path.path();
                let file_url_str = format!("{}://{}", url.scheme(), file_path.display());

                let table_provider = self.create_table_provider(&file_url_str, &ctx).await?;
                callback(table_provider, file_path.display().to_string()).await?;
            }

            Ok(matches.len())
        } else {
            // Single file
            let ctx = SessionContext::new();
            let table_provider = self.create_table_provider(url_str, &ctx).await?;
            callback(table_provider, path.to_string()).await?;
            Ok(1)
        }
    }

    /// Create a MemTable by streaming a single file
    async fn create_memtable_from_url(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        // Open file with decompression
        let reader = self.fs.open_url(url).await?;

        // Stream data with format provider
        let (schema, mut stream) = format_provider.open_stream(reader, url).await?;

        // Collect all batches
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

        // Create MemTable
        let table = MemTable::try_new(schema, vec![batches])?;
        Ok(Arc::new(table))
    }

    /// Create a cached table from a format URL.
    ///
    /// For each version of the source file, checks if a cached Parquet file
    /// exists.  Uncached versions are parsed via the format provider and
    /// streamed to cache Parquet files.  Returns a `ListingTable` over the
    /// cache directory.
    async fn create_cached_table_from_url(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
        cache_dir: &std::path::Path,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let scheme = url.scheme();
        let (node_id, _versions) = self
            .ensure_url_cached(url, format_provider, cache_dir)
            .await?;

        // Return ListingTable over all cached version Parquet files
        crate::format_cache::listing_table_from_cache(cache_dir, scheme, &node_id, ctx).await
    }

    /// Populate the per-node cache for a URL, returning the `NodeID` and its
    /// version list so callers can build glob symlinks or listing tables.
    ///
    /// Resolves the URL to a `FileID`, lists all versions, and caches any
    /// uncached versions by streaming them through the format provider.
    pub(crate) async fn ensure_url_cached(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
        cache_dir: &std::path::Path,
    ) -> Result<(tinyfs::NodeID, Vec<tinyfs::FileVersionInfo>)> {
        use tinyfs::Lookup;

        let scheme = url.scheme();
        let path = url.path();

        // Resolve URL path to FileID
        let root = self.fs.root().await?;
        let (_, lookup_result) = root
            .resolve_path(path)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to resolve path '{}': {}", path, e)))?;

        let node_path = match lookup_result {
            Lookup::Found(np) => np,
            _ => return Err(Error::InvalidUrl(format!("File not found: {}", path))),
        };

        let file_id = node_path.id();
        let node_id = file_id.node_id();

        let persistence = self
            .provider_context
            .as_ref()
            .expect("cache path requires ProviderContext");

        // List all versions
        let versions = persistence
            .persistence
            .list_file_versions(file_id)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to list versions: {}", e)))?;

        if versions.is_empty() {
            return Err(Error::InvalidUrl(format!(
                "No versions found for file: {}",
                path
            )));
        }

        // Find uncached versions
        let uncached =
            crate::format_cache::find_uncached_versions(cache_dir, scheme, &node_id, &versions);

        if uncached.is_empty() {
            log::debug!(
                "[GO] Format cache: all {} versions cached for {}://{}",
                versions.len(),
                scheme,
                path
            );
        } else {
            log::info!(
                "[SAVE] Format cache: writing {} of {} versions for {}://{}",
                uncached.len(),
                versions.len(),
                scheme,
                path
            );

            for version in &uncached {
                // Read this specific version's raw bytes
                let bytes = persistence
                    .persistence
                    .read_file_version(file_id, version.version)
                    .await
                    .map_err(|e| {
                        Error::InvalidUrl(format!(
                            "Failed to read version {}: {}",
                            version.version, e
                        ))
                    })?;

                // Wrap bytes as AsyncRead for format provider
                let reader: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> =
                    Box::pin(std::io::Cursor::new(bytes));

                // Apply decompression if needed
                let reader = crate::format::compression::decompress(reader, url.compression())?;

                // Parse through format provider
                let (schema, stream) = format_provider.open_stream(reader, url).await?;

                // Stream to cache Parquet -- not collected into memory
                let _ = crate::format_cache::cache_write_version(
                    cache_dir, scheme, &node_id, version, schema, stream,
                )
                .await?;
            }
        }

        Ok((node_id, versions))
    }

    /// Create a single `TableProvider` for a URL that may contain wildcards.
    ///
    /// For external format schemes (`csv`, `oteljson`, ...) with an available
    /// format cache, individual file versions are cached as Parquet and a
    /// single streaming `ListingTable` is returned -- avoiding full
    /// materialization.
    ///
    /// For builtin types (`file`/`series`/`table`) or when no cache is
    /// available, falls back to UNION ALL BY NAME (which materializes).
    pub async fn create_provider_for_url(
        &self,
        url_str: &str,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let url = Url::parse(url_str)?;
        let path = url.path();
        let has_wildcards = path.contains('*') || path.contains('?');

        if !has_wildcards {
            // Single file -- delegate directly
            return self.create_table_provider(url_str, ctx).await;
        }

        let scheme = url.scheme();

        // For external formats with cache: glob-cache path
        if !matches!(scheme, "file" | "series" | "table" | "data")
            && let Some(ref provider_context) = self.provider_context
            && let Some(cache_dir) = provider_context.cache_dir()
        {
            let format_provider = FormatRegistry::get_provider(scheme)
                .ok_or_else(|| Error::InvalidUrl(format!("Unknown format: {}", scheme)))?;

            // Expand pattern via TinyFS
            let root = self.fs.root().await?;
            let matches = root.collect_matches(path).await.map_err(|e| {
                Error::InvalidUrl(format!("Pattern expansion failed for '{}': {}", path, e))
            })?;

            if matches.is_empty() {
                return Err(Error::InvalidUrl(format!(
                    "No files match pattern: {}",
                    url_str
                )));
            }

            if matches.len() == 1 {
                // Single match -- per-node ListingTable
                let (node_path, _) = &matches[0];
                let file_url_str = format!("{}://{}", scheme, node_path.path().display());
                return self.create_table_provider(&file_url_str, ctx).await;
            }

            // Multi-file: cache each, symlink into glob dir, ListingTable
            let pat_hash = crate::format_cache::pattern_hash(url_str);
            let glob_dir = crate::format_cache::cache_glob_dir(cache_dir, scheme, &pat_hash);
            crate::format_cache::reset_glob_dir(&glob_dir)?;

            for (node_path, _) in &matches {
                let file_url_str = format!("{}://{}", scheme, node_path.path().display());
                let file_url = Url::parse(&file_url_str)?;

                let (node_id, versions) = self
                    .ensure_url_cached(&file_url, format_provider.as_ref(), cache_dir)
                    .await?;

                let _ = crate::format_cache::ensure_glob_symlinks(
                    cache_dir, scheme, &node_id, &versions, &glob_dir,
                )?;
            }

            let provider =
                crate::format_cache::listing_table_from_glob_cache(&glob_dir, ctx).await?;

            log::debug!(
                "[OK] Glob cache ListingTable for {} files (pattern '{}')",
                matches.len(),
                url_str,
            );

            return Ok(provider);
        }

        // Fallback: individual providers + UNION ALL BY NAME
        let mut table_providers = Vec::new();
        let _ = self
            .for_each_match(url_str, |tp, file_path| {
                log::debug!("Matched file: {}", file_path);
                table_providers.push(tp);
                async { Ok(()) }
            })
            .await?;

        if table_providers.len() == 1 {
            return Ok(table_providers.into_iter().next().expect("len == 1"));
        }

        // Multiple files without cache -- materialize via UNION ALL BY NAME
        let temp_ctx = SessionContext::new();
        for (i, tp) in table_providers.iter().enumerate() {
            let _ = temp_ctx
                .register_table(format!("t{}", i), tp.clone())
                .map_err(|e| {
                    Error::SessionContext(format!("Failed to register table t{}: {}", i, e))
                })?;
        }

        let union_sql = (0..table_providers.len())
            .map(|i| format!("SELECT * FROM t{}", i))
            .collect::<Vec<_>>()
            .join(" UNION ALL BY NAME ");

        log::debug!("Executing fallback union query: {}", union_sql);

        let df = temp_ctx.sql(&union_sql).await?;
        let batches = df.collect().await?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| Error::SessionContext("No batches in union result".to_string()))?;
        let mem_table = MemTable::try_new(schema, vec![batches])?;
        Ok(Arc::new(mem_table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::test_factory::{InfiniteCsvConfig, InfiniteCsvFile};
    use tinyfs::{File, MemoryPersistence};

    async fn create_test_fs() -> Arc<tinyfs::FS> {
        let persistence = MemoryPersistence::default();
        Arc::new(tinyfs::FS::new(persistence).await.unwrap())
    }

    async fn create_csv_file(fs: &tinyfs::FS, path: &str, config: InfiniteCsvConfig) -> Result<()> {
        let root = fs.root().await?;

        // Create parent directories
        if let Some(parent) = std::path::Path::new(path).parent()
            && parent.to_str() != Some("/")
            && !parent.as_os_str().is_empty()
        {
            let path_parts: Vec<_> = parent
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(name) => name.to_str(),
                    _ => None,
                })
                .collect();

            let mut current_path = String::from("/");
            for part in path_parts {
                if !current_path.ends_with('/') {
                    current_path.push('/');
                }
                current_path.push_str(part);
                let _ = root.create_dir_path(&current_path).await;
            }
        }

        // Generate CSV data
        let infinite_file = InfiniteCsvFile::new(config);
        let mut reader = infinite_file.async_reader().await?;

        use tokio::io::AsyncReadExt;
        let mut csv_data = Vec::new();
        let _ = reader.read_to_end(&mut csv_data).await?;

        // Write to TinyFS
        let mut file_writer = root
            .async_writer_path_with_type(path, tinyfs::EntryType::FileDynamic)
            .await?;
        use tokio::io::AsyncWriteExt;
        file_writer.write_all(&csv_data).await?;
        file_writer.flush().await?;
        file_writer.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_provider_create_table_single_file() {
        let fs = create_test_fs().await;

        // Create CSV file
        let config = InfiniteCsvConfig {
            row_count: 50,
            batch_size: 1024,
            columns: vec!["id".to_string(), "value".to_string()],
        };
        create_csv_file(&fs, "/data/test.csv", config)
            .await
            .unwrap();

        // Create provider (uses global registry with CSV pre-registered)
        let provider = Provider::new(fs.clone());

        // Create TableProvider from URL
        let ctx = SessionContext::new();
        let table = provider
            .create_table_provider("csv:///data/test.csv", &ctx)
            .await
            .unwrap();

        // Register and query
        let _ = ctx.register_table("test", table).unwrap();
        let df = ctx.sql("SELECT COUNT(*) as count FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_provider_with_compression() {
        let fs = create_test_fs().await;

        // Create CSV file (we'll simulate compression in URL)
        let config = InfiniteCsvConfig {
            row_count: 30,
            batch_size: 1024,
            columns: vec!["timestamp".to_string(), "temperature".to_string()],
        };
        create_csv_file(&fs, "/sensors/temp.csv", config)
            .await
            .unwrap();

        let provider = Provider::new(fs.clone());

        let ctx = SessionContext::new();
        let table = provider
            .create_table_provider("csv:///sensors/temp.csv?batch_size=10", &ctx)
            .await
            .unwrap();

        let _ = ctx.register_table("sensors", table).unwrap();
        let df = ctx.sql("SELECT COUNT(*) FROM sensors").await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
    }

    #[test]
    fn test_provider_uses_linkme_registry() {
        // Test that Provider uses linkme format registry
        let csv_provider = FormatRegistry::get_provider("csv");

        // CSV should be registered via linkme
        assert!(csv_provider.is_some());
        assert_eq!(csv_provider.unwrap().name(), "csv");
    }

    #[tokio::test]
    async fn test_layer_3_complete() -> Result<()> {
        // Full Layer 3 integration test: URL -> Provider -> FormatProvider -> TableProvider
        let fs = create_test_fs().await;

        // Create test CSV file in memory
        create_csv_file(
            &fs,
            "data.csv",
            InfiniteCsvConfig {
                row_count: 2,
                batch_size: 1024,
                columns: vec!["id".to_string(), "name".to_string()],
            },
        )
        .await?;

        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        // Create table from CSV URL
        let table = provider
            .create_table_provider("csv:///data.csv", &ctx)
            .await?;

        // Register and query
        let _ = ctx.register_table("users", table)?;
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM users").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_glob_pattern_expansion() -> Result<()> {
        // Test that Provider correctly rejects glob patterns
        // Glob expansion should be handled by the factory layer
        let fs = create_test_fs().await;

        // Create multiple CSV files
        for i in 1..=3 {
            create_csv_file(
                &fs,
                &format!("data{}.csv", i),
                InfiniteCsvConfig {
                    row_count: 5,
                    batch_size: 100,
                    columns: vec!["id".to_string(), "value".to_string()],
                },
            )
            .await?;
        }

        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        // Provider should reject glob patterns - factory handles iteration
        let result = provider
            .create_table_provider("csv:///data*.csv", &ctx)
            .await;
        assert!(result.is_err(), "Provider should reject glob patterns");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("contains wildcards"),
            "Error should explain wildcards are not allowed"
        );
        assert!(
            err_msg.contains("collect_matches()"),
            "Error should guide to use collect_matches"
        );

        Ok(())
    }
}
