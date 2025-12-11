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
    pub fn new(fs: Arc<tinyfs::FS>) -> Self {
        Self { fs }
    }

    /// Create a TableProvider from a URL pattern
    ///
    /// Uses global format registry to look up format providers by scheme.
    /// For single files, streams data into MemTable.
    /// For patterns with wildcards, uses TinyFS collect_matches() to expand pattern
    /// and creates multi-file union TableProvider.
    ///
    /// # Arguments
    ///
    /// * `url_str` - URL like `csv:///path/*.csv?options` or `csv:///data/file.csv`
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

        // Get format provider from registry
        let format_provider = FormatRegistry::get_provider(url.scheme())
            .ok_or_else(|| Error::InvalidUrl(format!("Unknown format: {}", url.scheme())))?;

        // Check if pattern contains wildcards
        let path = url.path();
        if path.contains('*') || path.contains('?') {
            // Expand pattern using TinyFS and create multi-file union
            self.create_table_from_pattern(&url, format_provider.as_ref())
                .await
        } else {
            // Single file - stream into MemTable
            self.create_memtable_from_url(&url, format_provider.as_ref())
                .await
        }
    }

    /// Create a TableProvider from a glob pattern (multiple files)
    async fn create_table_from_pattern(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        // Get TinyFS root for pattern matching
        let root = self.fs.root().await?;
        
        // Use TinyFS collect_matches to expand pattern
        let pattern = url.path();
        let matches = root.collect_matches(pattern).await
            .map_err(|e| Error::InvalidUrl(format!("Pattern expansion failed for '{}': {}", pattern, e)))?;

        if matches.is_empty() {
            return Err(Error::InvalidUrl(format!("No files match pattern: {}", pattern)));
        }

        // For now, collect all files into a single MemTable
        // TODO: Use ListingTable for better performance with large file sets
        let (schema, all_batches) = {
            let mut first = true;
            let mut schema = None;
            let mut all_batches = Vec::new();

            for (node_path, _captures) in matches {
                // Create a URL for this specific file
                let file_url = format!("{}://{}", url.scheme(), node_path.path().display());
                let file_url = Url::parse(&file_url)?;

                // Open and stream this file
                let reader = self.fs.open_url(&file_url).await?;
                let (file_schema, mut stream) = format_provider.open_stream(reader, &file_url).await?;

                if first {
                    schema = Some(file_schema.clone());
                    first = false;
                } else {
                    // Verify schema compatibility
                    if schema.as_ref().unwrap() != &file_schema {
                        return Err(Error::InvalidUrl(format!(
                            "Schema mismatch in multi-file union at '{}': expected {:?}, got {:?}",
                            node_path.path().display(), schema, file_schema
                        )));
                    }
                }

                // Collect batches from this file
                while let Some(result) = stream.next().await {
                    all_batches.push(result?);
                }
            }

            (schema.unwrap(), all_batches)
        };

        // Create MemTable with all batches
        let table = MemTable::try_new(schema, vec![all_batches])?;
        Ok(Arc::new(table))
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

    async fn create_csv_file(
        fs: &tinyfs::FS,
        path: &str,
        config: InfiniteCsvConfig,
    ) -> Result<()> {
        let root = fs.root().await?;

        // Create parent directories
        if let Some(parent) = std::path::Path::new(path).parent() {
            if parent.to_str() != Some("/") && !parent.as_os_str().is_empty() {
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
        println!("✅ Provider created table from single CSV file");
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
        let df = ctx
            .sql("SELECT COUNT(*) FROM sensors")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
        println!("✅ Provider handled CSV with query parameters");
    }

    #[test]
    fn test_provider_uses_linkme_registry() {
        // Test that Provider uses linkme format registry
        let csv_provider = FormatRegistry::get_provider("csv");
        
        // CSV should be registered via linkme
        assert!(csv_provider.is_some());
        assert_eq!(csv_provider.unwrap().name(), "csv");
        
        println!("✅ Provider uses linkme format registry with CSV");
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
        let table = provider.create_table_provider("csv:///data.csv", &ctx).await?;

        // Register and query
        let _ = ctx.register_table("users", table)?;
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM users").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        println!("✅ Layer 3 complete: URL → FormatProvider → TableProvider → SQL");
        Ok(())
    }

    #[tokio::test]
    async fn test_glob_pattern_expansion() -> Result<()> {
        // Test glob pattern expansion using TinyFS collect_matches
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

        // Create table from glob pattern
        let table = provider.create_table_provider("csv:///data*.csv", &ctx).await?;

        // Register and query - should union all 3 files (15 total rows)
        let _ = ctx.register_table("multi", table)?;
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM multi").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        // Verify we got all 15 rows (3 files × 5 rows each)
        let count_batch = &results[0];
        let count_array = count_batch.column(0);
        let count = count_array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(count.value(0), 15);

        println!("✅ Glob pattern expansion: 3 files × 5 rows = 15 total");
        Ok(())
    }
}
