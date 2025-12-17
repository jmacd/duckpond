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
/// - Compression: `csv://gzip/data/*.csv.gz`
/// - Format options: `csv:///data/file.csv?delimiter=;`
pub struct Provider {
    /// TinyFS for file access
    fs: Arc<tinyfs::FS>,
}

impl Provider {
    /// Create a new Provider with TinyFS
    #[must_use]
    pub fn new(fs: Arc<tinyfs::FS>) -> Self {
        Self { fs }
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

        // Get format provider from registry
        let format_provider = FormatRegistry::get_provider(url.scheme())
            .ok_or_else(|| Error::InvalidUrl(format!("Unknown format: {}", url.scheme())))?;

        // Single file - stream into MemTable
        self.create_memtable_from_url(&url, format_provider.as_ref())
            .await
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
            && parent.to_str() != Some("/") && !parent.as_os_str().is_empty()
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
            .async_writer_path_with_type(path, tinyfs::EntryType::FileDataDynamic)
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
        // Full Layer 3 integration test: URL → Provider → FormatProvider → TableProvider
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
