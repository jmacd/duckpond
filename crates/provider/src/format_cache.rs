// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Format provider cache -- per-version Parquet cache for format providers.
//!
//! Format providers (oteljson, csv, excelhtml) parse raw file bytes into Arrow
//! RecordBatches on every read.  This module caches the parsed output of each
//! individual file version as a Parquet file on disk in `{POND}/cache/`.
//!
//! Key properties:
//! - Per-version caching: each version is independently immutable (blake3 hash),
//!   so there is nothing to invalidate.
//! - Incremental: only uncached versions are parsed; cached versions are free.
//! - Throwaway: `rm -rf {POND}/cache/` is always safe.
//! - Returns `ListingTable` over cached Parquet files for full DataFusion pushdown.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use parquet::arrow::AsyncArrowWriter;
use futures::stream::Stream;
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::FileVersionInfo;

/// Result type for format cache operations
type Result<T> = std::result::Result<T, crate::error::Error>;

/// Directory for a file's cached format conversions.
///
/// Returns `{cache_dir}/{scheme}_{node_id}/`
pub fn cache_node_dir(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
) -> PathBuf {
    cache_dir.join(format!("{}_{}", scheme, node_id))
}

/// Path for a single version's cached Parquet file.
///
/// Returns `{cache_dir}/{scheme}_{node_id}/v{version}_{blake3}.parquet`
///
/// # Panics
/// Panics if `version.blake3` is `None` -- format providers only operate on
/// file data, which always has a blake3 hash.
pub fn cache_version_path(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    version: &FileVersionInfo,
) -> PathBuf {
    let blake3 = version
        .blake3
        .as_deref()
        .expect("blake3 must be Some for file data versions -- format cache requires content hash");
    cache_node_dir(cache_dir, scheme, node_id).join(format!(
        "v{}_{}.parquet",
        version.version, blake3
    ))
}

/// Check which versions are missing from the cache.
///
/// Returns the subset of versions whose cached Parquet files do not exist on disk.
pub fn find_uncached_versions(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    versions: &[FileVersionInfo],
) -> Vec<FileVersionInfo> {
    versions
        .iter()
        .filter(|v| {
            let path = cache_version_path(cache_dir, scheme, node_id, v);
            !path.exists()
        })
        .cloned()
        .collect()
}

/// Write a single version's format output to cache as Parquet.
///
/// Streams batches from the format provider through `AsyncArrowWriter` to
/// disk.  Does NOT collect into memory.  Uses atomic write (write to `.tmp`
/// then rename) to prevent partial files on crash.
pub async fn cache_write_version(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    version: &FileVersionInfo,
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>>,
) -> Result<PathBuf> {
    let dir = cache_node_dir(cache_dir, scheme, node_id);
    tokio::fs::create_dir_all(&dir).await?;

    let final_path = cache_version_path(cache_dir, scheme, node_id, version);

    // Atomic write: write to .tmp then rename to prevent partial files on crash
    let tmp_path = final_path.with_extension("parquet.tmp");

    let file = tokio::fs::File::create(&tmp_path).await?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .build();
    let mut writer = AsyncArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| crate::error::Error::Arrow(e.to_string()))?;

    let mut stream = stream;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        writer
            .write(&batch)
            .await
            .map_err(|e| crate::error::Error::Arrow(e.to_string()))?;
    }
    let _metadata = writer
        .close()
        .await
        .map_err(|e| crate::error::Error::Arrow(e.to_string()))?;

    // Atomic rename: .tmp -> final
    tokio::fs::rename(&tmp_path, &final_path).await?;

    log::debug!(
        "[SAVE] Format cache: wrote {}",
        final_path.display()
    );

    Ok(final_path)
}

/// Build a `ListingTable` over all cached version Parquet files for a node.
///
/// Assumes all versions are already cached (call `cache_write_version` for any
/// uncached versions first).
pub async fn listing_table_from_cache(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    _ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let dir = cache_node_dir(cache_dir, scheme, node_id);
    let dir_url = format!("file://{}/", dir.display());

    let table_url = ListingTableUrl::parse(&dir_url)?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");

    // Merge schemas from all cached parquet versions.  Format providers like
    // oteljson produce variable schemas (columns appear/disappear across
    // lines), so even a single file's cached versions can differ.
    let merged_schema = merge_parquet_schemas_in_dir(&dir).await?;

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(merged_schema);

    let table = ListingTable::try_new(config)?;
    Ok(Arc::new(table))
}

/// Directory for a glob-scoped unified cache (all files matching a pattern).
///
/// Returns `{cache_dir}/{scheme}_glob_{pattern_hash}/`
pub fn cache_glob_dir(cache_dir: &Path, scheme: &str, pattern_hash: &str) -> PathBuf {
    cache_dir.join(format!("{}_glob_{}", scheme, pattern_hash))
}

/// Compute a deterministic hash for a glob pattern string.
pub fn pattern_hash(pattern: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    pattern.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Ensure symlinks exist in the glob directory for all cached versions of a node.
///
/// Creates symlinks from `{glob_dir}/{node_id}_v{version}_{blake3}.parquet`
/// to the per-node cache directory files.
///
/// Returns the number of new symlinks created.
pub fn ensure_glob_symlinks(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    versions: &[FileVersionInfo],
    glob_dir: &Path,
) -> Result<usize> {
    let mut created = 0;
    for version in versions {
        let source = cache_version_path(cache_dir, scheme, node_id, version);
        if !source.exists() {
            continue; // Skip versions not yet cached
        }

        let blake3 = version
            .blake3
            .as_deref()
            .expect("blake3 must be Some for file data versions");
        let link_name = glob_dir.join(format!(
            "{}_v{}_{}.parquet",
            node_id, version.version, blake3
        ));

        if !link_name.exists() {
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(&source, &link_name)
                    .map_err(crate::error::Error::Io)?;
            }
            #[cfg(not(unix))]
            {
                // On non-Unix, fall back to hard link or copy
                std::fs::hard_link(&source, &link_name)
                    .or_else(|_| std::fs::copy(&source, &link_name).map(|_| ()))
                    .map_err(crate::error::Error::Io)?;
            }
            created += 1;
        }
    }
    Ok(created)
}

/// Clean a glob directory by removing all existing symlinks, then recreating it.
///
/// This is used before populating with fresh symlinks each query to ensure
/// deleted versions or changed file sets are not stale.
pub fn reset_glob_dir(glob_dir: &Path) -> Result<()> {
    if glob_dir.exists() {
        std::fs::remove_dir_all(glob_dir).map_err(crate::error::Error::Io)?;
    }
    std::fs::create_dir_all(glob_dir).map_err(crate::error::Error::Io)?;
    Ok(())
}

/// Build a `ListingTable` over a glob directory containing symlinks to
/// per-node cached Parquet files from multiple source files.
///
/// This replaces the UNION ALL BY NAME pattern: instead of N separate
/// MemTables unioned via SQL, a single ListingTable scans all files
/// in the glob directory. Schema evolution (different columns per file)
/// is handled by `ListingTable`'s schema adapter.
pub async fn listing_table_from_glob_cache(
    glob_dir: &Path,
    _ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let dir_url = format!("file://{}/", glob_dir.display());

    let table_url = ListingTableUrl::parse(&dir_url)?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");

    // Merge schemas from ALL parquet files in the glob directory.
    // DataFusion's infer_schema only samples a subset of files, which loses
    // columns that appear in later files (e.g., sensors added over time).
    // This is the glob-cache equivalent of UNION ALL BY NAME's schema merge.
    let merged_schema = merge_parquet_schemas_in_dir(glob_dir).await?;

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(merged_schema);

    let table = ListingTable::try_new(config)?;
    Ok(Arc::new(table))
}

/// Read the Arrow schema from every `.parquet` file under `dir` and merge
/// them via `Schema::try_merge`.  This gives UNION-ALL-BY-NAME semantics:
/// columns that appear in any file are present in the result, and files
/// missing a column will produce NULLs when read through the ListingTable.
async fn merge_parquet_schemas_in_dir(dir: &Path) -> Result<SchemaRef> {
    use arrow::datatypes::Schema;

    let mut schemas = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "parquet") {
            let file = tokio::fs::File::open(&path).await?;
            let reader =
                parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(
                    file,
                )
                .await
                .map_err(|e| {
                    crate::error::Error::Arrow(format!(
                        "Failed to read parquet metadata from '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
            schemas.push(reader.schema().as_ref().clone());
        }
    }

    if schemas.is_empty() {
        return Err(crate::error::Error::Arrow(format!(
            "No parquet files found in glob cache dir '{}'",
            dir.display()
        )));
    }

    let merged = Schema::try_merge(schemas).map_err(|e| {
        crate::error::Error::Arrow(format!("Failed to merge parquet schemas: {}", e))
    })?;

    Ok(Arc::new(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn test_batch(schema: &SchemaRef, timestamps: &[i64], values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(
                    values.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn test_version(version: u64, blake3: &str) -> FileVersionInfo {
        FileVersionInfo {
            version,
            timestamp: 0,
            size: 100,
            blake3: Some(blake3.to_string()),
            entry_type: tinyfs::EntryType::FilePhysicalVersion,
            extended_metadata: None,
        }
    }

    fn test_node_id() -> tinyfs::NodeID {
        tinyfs::NodeID::new(uuid7::uuid7().to_string())
    }

    #[test]
    fn test_cache_node_dir() {
        let cache_dir = Path::new("/tmp/pond/cache");
        let node_id = test_node_id();
        let dir = cache_node_dir(cache_dir, "oteljson", &node_id);
        let dir_str = dir.to_str().unwrap();
        assert!(dir_str.starts_with("/tmp/pond/cache/oteljson_"));
        assert!(dir_str.contains(&node_id.to_string()));
    }

    #[test]
    fn test_cache_version_path() {
        let cache_dir = Path::new("/tmp/pond/cache");
        let node_id = test_node_id();
        let version = test_version(3, "abcdef1234567890");
        let path = cache_version_path(cache_dir, "csv", &node_id, &version);
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("v3_abcdef1234567890"));
        assert!(filename.ends_with(".parquet"));
    }

    #[test]
    fn test_find_uncached_versions_all_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let versions = vec![test_version(1, "aaa"), test_version(2, "bbb")];
        let uncached = find_uncached_versions(cache_dir, "oteljson", &node_id, &versions);
        assert_eq!(uncached.len(), 2);
    }

    #[test]
    fn test_find_uncached_versions_some_cached() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let versions = vec![test_version(1, "aaa"), test_version(2, "bbb")];

        // Pre-create the cache dir and v1's parquet file
        let v1_path = cache_version_path(cache_dir, "oteljson", &node_id, &versions[0]);
        std::fs::create_dir_all(v1_path.parent().unwrap()).unwrap();
        std::fs::write(&v1_path, b"fake parquet").unwrap();

        let uncached = find_uncached_versions(cache_dir, "oteljson", &node_id, &versions);
        assert_eq!(uncached.len(), 1);
        assert_eq!(uncached[0].version, 2);
    }

    #[tokio::test]
    async fn test_cache_write_version() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let version = test_version(1, "deadbeef");

        let schema = test_schema();
        let batch = test_batch(&schema, &[1000, 2000, 3000], &["a", "b", "c"]);

        let stream: Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>> =
            Box::pin(futures::stream::once(async move { Ok(batch) }));

        let path = cache_write_version(cache_dir, "oteljson", &node_id, &version, schema, stream)
            .await
            .unwrap();

        assert!(path.exists());
        assert!(path.to_str().unwrap().ends_with("v1_deadbeef.parquet"));

        // Verify the tmp file was cleaned up
        let tmp_path = path.with_extension("parquet.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_listing_table_from_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();

        let schema = test_schema();

        // Write two versions
        for i in 1_i64..=2 {
            let version = test_version(i as u64, &format!("hash{}", i));
            let batch = test_batch(&schema, &[i * 1000], &[&format!("val{}", i)]);
            let stream: Pin<
                Box<
                    dyn Stream<
                            Item = std::result::Result<RecordBatch, crate::error::Error>,
                        > + Send,
                >,
            > = Box::pin(futures::stream::once({
                let batch = batch;
                async move { Ok(batch) }
            }));
            let _ = cache_write_version(cache_dir, "csv", &node_id, &version, schema.clone(), stream)
                .await
                .unwrap();
        }

        // Build ListingTable and verify
        let ctx = SessionContext::new();
        let table = listing_table_from_cache(cache_dir, "csv", &node_id, &ctx)
            .await
            .unwrap();

        // Should have the correct schema
        let table_schema = table.schema();
        assert_eq!(table_schema.fields().len(), 2);
        assert_eq!(table_schema.field(0).name(), "timestamp");
        assert_eq!(table_schema.field(1).name(), "value");
    }

    #[test]
    fn test_pattern_hash_deterministic() {
        let h1 = pattern_hash("/sensors/**/*.json");
        let h2 = pattern_hash("/sensors/**/*.json");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16); // 16 hex chars

        // Different pattern => different hash
        let h3 = pattern_hash("/other/**/*.csv");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_cache_glob_dir() {
        let dir = cache_glob_dir(Path::new("/tmp/cache"), "oteljson", "abc123");
        assert_eq!(dir, Path::new("/tmp/cache/oteljson_glob_abc123"));
    }

    #[test]
    fn test_reset_glob_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let glob_dir = tmp.path().join("test_glob");

        // Create dir with a file in it
        std::fs::create_dir_all(&glob_dir).unwrap();
        std::fs::write(glob_dir.join("old.parquet"), b"old data").unwrap();
        assert!(glob_dir.join("old.parquet").exists());

        // Reset should remove old contents
        reset_glob_dir(&glob_dir).unwrap();
        assert!(glob_dir.exists());
        assert!(!glob_dir.join("old.parquet").exists());
    }

    #[tokio::test]
    async fn test_ensure_glob_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let versions = vec![test_version(1, "aaa"), test_version(2, "bbb")];

        let schema = test_schema();

        // Write both versions to per-node cache
        for v in &versions {
            let batch = test_batch(&schema, &[1000], &["x"]);
            let stream: Pin<
                Box<
                    dyn Stream<
                            Item = std::result::Result<RecordBatch, crate::error::Error>,
                        > + Send,
                >,
            > = Box::pin(futures::stream::once({
                let batch = batch;
                async move { Ok(batch) }
            }));
            let _ = cache_write_version(cache_dir, "oteljson", &node_id, v, schema.clone(), stream)
                .await
                .unwrap();
        }

        // Create glob dir and build symlinks
        let glob_dir = tmp.path().join("glob_test");
        std::fs::create_dir_all(&glob_dir).unwrap();
        let created =
            ensure_glob_symlinks(cache_dir, "oteljson", &node_id, &versions, &glob_dir).unwrap();
        assert_eq!(created, 2);

        // Verify symlinks exist
        let entries: Vec<_> = std::fs::read_dir(&glob_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 2);

        // Calling again should create 0 new symlinks (idempotent)
        let created2 =
            ensure_glob_symlinks(cache_dir, "oteljson", &node_id, &versions, &glob_dir).unwrap();
        assert_eq!(created2, 0);
    }

    #[tokio::test]
    async fn test_listing_table_from_glob_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();

        let schema = test_schema();

        // Simulate two different nodes (different files matched by a glob)
        let node1 = test_node_id();
        let node2 = test_node_id();
        let v1 = test_version(1, "hash_a");
        let v2 = test_version(1, "hash_b");

        for (node_id, version, ts, val) in [
            (&node1, &v1, vec![100], vec!["foo"]),
            (&node2, &v2, vec![200], vec!["bar"]),
        ] {
            let batch = test_batch(&schema, &ts, &val);
            let stream: Pin<
                Box<
                    dyn Stream<
                            Item = std::result::Result<RecordBatch, crate::error::Error>,
                        > + Send,
                >,
            > = Box::pin(futures::stream::once({
                let batch = batch;
                async move { Ok(batch) }
            }));
            let _ = cache_write_version(
                cache_dir, "csv", node_id, version, schema.clone(), stream,
            )
            .await
            .unwrap();
        }

        // Create glob dir, symlink both nodes' versions in
        let glob_dir = tmp.path().join("my_glob");
        std::fs::create_dir_all(&glob_dir).unwrap();
        let _ = ensure_glob_symlinks(cache_dir, "csv", &node1, &[v1.clone()], &glob_dir).unwrap();
        let _ = ensure_glob_symlinks(cache_dir, "csv", &node2, &[v2.clone()], &glob_dir).unwrap();

        // Build listing table over glob dir
        let ctx = SessionContext::new();
        let table = listing_table_from_glob_cache(&glob_dir, &ctx).await.unwrap();

        let table_schema = table.schema();
        assert_eq!(table_schema.fields().len(), 2);

        // Execute a query to confirm both files are read
        let _ = ctx.register_table("source", table).unwrap();
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM source").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2); // One row from each node
    }

    /// Verify that listing_table_from_glob_cache merges schemas across files
    /// with different columns (UNION ALL BY NAME semantics).  This catches the
    /// bug where schema inference from a single file drops columns that only
    /// exist in later files (e.g., sensors added over time).
    #[tokio::test]
    async fn test_listing_table_glob_cache_schema_merge() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();

        // File 1: schema has (timestamp, sensor_a)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("sensor_a", DataType::Utf8, true),
        ]));

        // File 2: schema has (timestamp, sensor_a, sensor_b)
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("sensor_a", DataType::Utf8, true),
            Field::new("sensor_b", DataType::Utf8, true),
        ]));

        let node1 = test_node_id();
        let node2 = test_node_id();
        let v1 = test_version(1, "hash_merge_a");
        let v2 = test_version(1, "hash_merge_b");

        // Write file 1 with schema1
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(StringArray::from(vec![Some("a1")])),
            ],
        )
        .unwrap();
        let stream1: Pin<
            Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>,
        > = Box::pin(futures::stream::once({
            let b = batch1;
            async move { Ok(b) }
        }));
        let _ = cache_write_version(cache_dir, "csv", &node1, &v1, schema1.clone(), stream1)
            .await
            .unwrap();

        // Write file 2 with schema2
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(StringArray::from(vec![Some("a2")])),
                Arc::new(StringArray::from(vec![Some("b2")])),
            ],
        )
        .unwrap();
        let stream2: Pin<
            Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>,
        > = Box::pin(futures::stream::once({
            let b = batch2;
            async move { Ok(b) }
        }));
        let _ = cache_write_version(cache_dir, "csv", &node2, &v2, schema2.clone(), stream2)
            .await
            .unwrap();

        // Create glob dir with symlinks to both nodes
        let glob_dir = tmp.path().join("merge_glob");
        std::fs::create_dir_all(&glob_dir).unwrap();
        let _ =
            ensure_glob_symlinks(cache_dir, "csv", &node1, &[v1.clone()], &glob_dir).unwrap();
        let _ =
            ensure_glob_symlinks(cache_dir, "csv", &node2, &[v2.clone()], &glob_dir).unwrap();

        // Build listing table -- should have merged schema with all 3 columns
        let ctx = SessionContext::new();
        let table = listing_table_from_glob_cache(&glob_dir, &ctx)
            .await
            .unwrap();

        let table_schema = table.schema();
        let field_names: Vec<&str> = table_schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"timestamp"),
            "merged schema must contain timestamp"
        );
        assert!(
            field_names.contains(&"sensor_a"),
            "merged schema must contain sensor_a"
        );
        assert!(
            field_names.contains(&"sensor_b"),
            "merged schema must contain sensor_b (from second file)"
        );

        // Query the data -- sensor_b should be NULL for file 1's row
        let _ = ctx.register_table("source", table).unwrap();
        let df = ctx
            .sql("SELECT timestamp, sensor_a, sensor_b FROM source ORDER BY timestamp")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        use arrow::array::Array;

        let ts_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(0), 100);
        assert_eq!(ts_col.value(1), 200);

        let sb_col = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(sb_col.is_null(0), "sensor_b should be NULL for file 1");
        assert_eq!(sb_col.value(1), "b2");
    }
}
