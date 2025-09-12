//! TinyFS-backed ObjectStore implementation for DataFusion ListingTable integration.
//! 
//! This module implements the object_store::ObjectStore trait to bridge TinyFS 
//! storage with DataFusion's ListingTable. It maps ObjectStore paths to TinyFS 
//! node IDs, enabling native predicate pushdown and streaming execution.
//!
//! Path format: "/node/{node_id}" maps to TinyFS node ID
//! 
//! Example usage:
//! ```no_run
//! use std::sync::Arc;
//! use tlogfs::tinyfs_object_store::TinyFsObjectStore;
//! use datafusion::datasource::listing::{ListingTableUrl, ListingTableConfig, ListingOptions, ListingTable};
//! use datafusion::datasource::file_format::parquet::ParquetFormat;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # use tlogfs::persistence::State;
//! # let persistence: State = todo!();
//! let tinyfs_store = TinyFsObjectStore::new(persistence);
//! let table_url = ListingTableUrl::parse("tinyfs:///node/some_node_id")?;
//! let config = ListingTableConfig::new(table_url)
//!     .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())));
//! let table = ListingTable::try_new(config)?;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::sync::Arc;
use std::collections::HashMap;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, 
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult
};
use tokio::sync::RwLock;
use tinyfs::PersistenceLayer;

use diagnostics::*;

/// File series information for ObjectStore registry
#[derive(Debug, Clone)]
struct FileSeriesInfo {
    /// Node ID for the file series
    node_id: tinyfs::NodeID,
    /// Part ID for the file series  
    part_id: tinyfs::NodeID,
    /// Version information for all versions in the series
    versions: Vec<tinyfs::FileVersionInfo>,
}

/// TinyFS-backed ObjectStore implementation.
/// 
/// This store maps ObjectStore paths to TinyFS file handles:
/// - Path format: "/node/{node_id}" 
/// - Uses file handles created by factories (which have State access)
/// - Provides file discovery and streaming access for DataFusion ListingTable
pub struct TinyFsObjectStore {
    /// Registry of file series information for discovery and access.
    /// Maps base series path -> FileSeriesInfo with versions
    file_registry: Arc<RwLock<HashMap<String, FileSeriesInfo>>>,
    /// Persistence layer for direct version access
    persistence: crate::persistence::State,
}

impl TinyFsObjectStore {
    /// Create a new TinyFS ObjectStore
    pub fn new(persistence: crate::persistence::State) -> Self {
        Self {
            file_registry: Arc::new(RwLock::new(HashMap::new())),
            persistence,
        }
    }

    /// Register file versions for a FileSeries
    /// Register a file series in the ObjectStore using node_id and part_id
    pub async fn register_file_versions(&self, node_id: tinyfs::NodeID, part_id: tinyfs::NodeID) -> Result<(), String> {
        // Get version information using persistence layer
        let versions = self.persistence.list_file_versions(node_id, part_id).await
            .map_err(|e| format!("Failed to list file versions: {}", e))?;
        
        let version_count = versions.len();
        debug!("ObjectStore registering {version_count} versions for node {node_id}");
        
        // FAIL FAST: A FileSeries with 0 versions is architecturally impossible
        if versions.is_empty() {
            return Err(format!(
                "FileSeries with node_id={} part_id={} has 0 versions. This is impossible - a FileSeries must have at least one version. Check part_id correctness.",
                node_id, part_id
            ));
        }
        
        for version in &versions {
            let ver = version.version;
            let size = version.size;
            debug!("ObjectStore version {ver}: size={size}");
        }

        // Create FileSeriesInfo and store in registry
        let series_info = FileSeriesInfo {
            node_id,
            part_id, 
            versions,
        };
        
        let base_key = format!("node/{}", node_id);
        self.file_registry.write().await.insert(base_key, series_info);
        
        Ok(())
    }

    /// Create ObjectMeta for a specific version
    fn create_object_meta_for_version(&self, location: &ObjectPath, series_info: &FileSeriesInfo, version_num: Option<u64>) -> ObjectStoreResult<ObjectMeta> {
        match version_num {
            Some(version) => {
                // Find the specific version info
                let version_info = series_info.versions.iter()
                    .find(|v| v.version == version)
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(), 
                        source: format!("Version {} not found", version).into(),
                    })?;
                
                Ok(ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(), // TODO: use version timestamp
                    size: version_info.size,
                    e_tag: None,
                    version: None,
                })
            }
            None => {
                // For non-versioned access, use the latest version
                let latest_version = series_info.versions.iter()
                    .max_by_key(|v| v.version)
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "No versions found".into(),
                    })?;
                
                Ok(ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(),
                    size: latest_version.size,
                    e_tag: None,
                    version: None,
                })
            }
        }
    }

    /// Extract node ID from ObjectStore path
    /// 
    /// Expected formats: 
    /// - "node/{node_id}.parquet" -> "node/{node_id}"
    /// - "node/{node_id}/version/{version_num}.parquet" -> "node/{node_id}/version/{version_num}.parquet"
    fn path_to_node_id(&self, path: &ObjectPath) -> ObjectStoreResult<String> {
        let path_str = path.as_ref();
        
        if let Some(stripped) = path_str.strip_prefix("node/") {
            // Remove .parquet extension if present
            let node_part = if let Some(without_ext) = stripped.strip_suffix(".parquet") {
                without_ext
            } else {
                stripped
            };
            
            if node_part.is_empty() {
                return Err(object_store::Error::Generic {
                    store: "TinyFS",
                    source: "Empty node ID path".into(),
                });
            }
            
            // Handle both formats:
            // 1. "{node_id}" -> "node/{node_id}"
            // 2. "{node_id}/version/{version_num}" -> "node/{node_id}/version/{version_num}.parquet"
            if node_part.contains('/') {
                // Check if it's a valid versioned path
                if node_part.matches('/').count() == 2 && node_part.contains("/version/") {
                    Ok(format!("node/{}.parquet", node_part))
                } else {
                    Err(object_store::Error::Generic {
                        store: "TinyFS",
                        source: "Invalid versioned path format. Expected: node/{series_id}/version/{version_num}.parquet".into(),
                    })
                }
            } else {
                // Single node ID format
                Ok(format!("node/{}", node_part))
            }
        } else {
            Err(object_store::Error::Generic {
                store: "TinyFS", 
                source: "Path must start with node/".into(),
            })
        }
    }

    /// Parse versioned path to extract series_id and version number
    /// Returns (series_key, version_number) where series_key is for registry lookup
    fn parse_versioned_path(&self, path: &ObjectPath) -> ObjectStoreResult<(String, Option<u64>)> {
        let node_id = self.path_to_node_id(path)?;
        
        // Check if this is a versioned path like "node/series_id/version/123.parquet"
        if let Some(version_part) = node_id.strip_prefix("node/") {
            if let Some(version_index) = version_part.find("/version/") {
                let series_id = &version_part[..version_index];
                let version_str_with_ext = &version_part[version_index + 9..]; // "/version/".len() = 9
                
                // Remove .parquet extension if present
                let version_str = if let Some(without_ext) = version_str_with_ext.strip_suffix(".parquet") {
                    without_ext
                } else {
                    version_str_with_ext
                };
                
                let version_num = version_str.parse::<u64>().map_err(|_| object_store::Error::Generic {
                    store: "TinyFS", 
                    source: format!("Invalid version number: {}", version_str).into(),
                })?;
                
                let series_key = format!("node/{}", series_id);
                return Ok((series_key, Some(version_num)));
            }
        }
        
        // Not a versioned path, return as-is
        Ok((node_id, None))
    }
}

impl fmt::Display for TinyFsObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TinyFsObjectStore")
    }
}

impl fmt::Debug for TinyFsObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TinyFsObjectStore")
            .field("persistence", &"<Persistence>")
            .field("node_registry", &"<NodeRegistry>")
            .finish()
    }
}

#[async_trait]
impl ObjectStore for TinyFsObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        debug!("ObjectStore put_opts called for location: {_location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data.".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn object_store::MultipartUpload>> {
        debug!("ObjectStore put_multipart_opts called for location: {_location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data.".into(),
        })
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let path = location.as_ref();
        debug!("ObjectStore get_opts called for path: {path}");
        let (series_key, version_num) = self.parse_versioned_path(location)?;
        debug!("ObjectStore get_opts called for location: {location}, series_key: {series_key}, version: {#[emit::as_debug] version_num}");
        let head = options.head;
        let range = format!("{:?}", options.range);
        let if_match = format!("{:?}", options.if_match);
        let if_none_match = format!("{:?}", options.if_none_match);
        debug!("ObjectStore get_opts options: head={head}, range={range}, if_match={if_match}, if_none_match={if_none_match}");
        
        // Get file series info from registry
        let series_info = {
            let registry = self.file_registry.read().await;
            let count = registry.len();
            debug!("ObjectStore registry has {count} file series");
            registry.get(&series_key).cloned()
        };
        
        let series_info = series_info.ok_or_else(|| object_store::Error::NotFound {
            path: location.to_string(),
            source: "File series not found in registry".into(),
        })?;
        debug!("ObjectStore found file series for series_key: {series_key}");

        // Get version-specific metadata
        let object_meta = self.create_object_meta_for_version(location, &series_info, version_num)?;
        let size = object_meta.size;
        debug!("ObjectStore file metadata - size: {size}");

        // If this is a head request, return metadata only
        if options.head {
            return Ok(GetResult {
                meta: object_meta.clone(),
                payload: object_store::GetResultPayload::Stream(
                    futures::stream::empty().boxed()
                ),
                range: 0..object_meta.size,
                attributes: Default::default(),
            });
        }

        // Read the specific version using persistence layer
        let version_to_read = match version_num {
            Some(v) => v,
            None => {
                // If no version specified, use the latest available version
                series_info.versions.iter()
                    .map(|v| v.version)
                    .max()
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "No versions available for file series".into(),
                    })?
            }
        };
        
        // Get version-specific content using read_file_version (which returns Vec<u8>)
        let version_data = self.persistence.read_file_version(series_info.node_id, series_info.part_id, Some(version_to_read)).await
            .map_err(|e| object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Failed to read version {}: {}", version_to_read, e).into(),
            })?;
        
        // Return the version data directly - no buffering needed since read_file_version handles efficiency
        let byte_count = version_data.len();
        debug!("ObjectStore read version {version_to_read} directly, got {byte_count} bytes");

        // Create a stream from the version data
        let stream = async_stream::stream! {
            debug!("ObjectStore starting to stream file content for series_key: {series_key}");
            let data = version_data;
            let total_bytes = data.len();
            let chunk_size = 8192; // 8KB chunks
            let mut offset = 0;
            let mut chunk_count = 0;
            
            while offset < total_bytes {
                let end = std::cmp::min(offset + chunk_size, total_bytes);
                let chunk_data = &data[offset..end];
                let chunk_len = chunk_data.len();
                
                chunk_count += 1;
                
                // Log first few bytes of first chunk for diagnostics
                if chunk_count == 1 {
                    let preview = if chunk_len >= 8 {
                        format!("{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}...", 
                            chunk_data[0], chunk_data[1], chunk_data[2], chunk_data[3],
                            chunk_data[4], chunk_data[5], chunk_data[6], chunk_data[7])
                    } else {
                        format!("{:02x?}", &chunk_data[..chunk_len.min(8)])
                    };
                    debug!("ObjectStore chunk {chunk_count}: {chunk_len} bytes, starts with: {preview}");
                } else {
                    debug!("ObjectStore chunk {chunk_count}: {chunk_len} bytes (offset: {offset})");
                }
                
                yield Ok(Bytes::copy_from_slice(chunk_data));
                offset = end;
            }
            
            debug!("ObjectStore stream complete after {total_bytes} bytes in {chunk_count} chunks for series_key: {series_key}");
        };

        let range_end = object_meta.size;
        let meta_size = object_meta.size;
        debug!("ObjectStore returning GetResult with range 0..{range_end}, meta.size: {meta_size}");

        Ok(GetResult {
            meta: object_meta.clone(),
            payload: object_store::GetResultPayload::Stream(stream.boxed()),
            range: 0..object_meta.size,
            attributes: Default::default(),
        })
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        let path = location.as_ref();
        debug!("🔍 ObjectStore get_range called for path: {path}, range: {#[emit::as_debug] range}");
        debug!("🔍 ObjectStore get_range called - this means DataFusion is trying to read Parquet metadata");
        
        let (series_key, version_num) = match self.parse_versioned_path(location) {
            Ok(result) => {
                let series_key = &result.0;
                let version_num = &result.1;
                debug!("✅ ObjectStore get_range parsed path - series_key: {series_key}, version: {#[emit::as_debug] version_num}");
                result
            }
            Err(e) => {
                debug!("❌ ObjectStore get_range failed to parse path {path}: {e}");
                return Err(e);
            }
        };
        
        // Get file series info from registry
        let series_info = {
            let registry = self.file_registry.read().await;
            let count = registry.len();
            debug!("🔍 ObjectStore get_range registry has {count} entries");
            registry.get(&series_key).cloned()
        };
        
        let series_info = match series_info {
            Some(info) => {
                let node_id = info.node_id;
                let part_id = info.part_id;
                let version_count = info.versions.len();
                debug!("✅ ObjectStore get_range found series info: node_id={node_id}, part_id={part_id}, {version_count} versions");
                info
            }
            None => {
                debug!("❌ ObjectStore get_range: series '{series_key}' not found in registry");
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: "File series not found in registry".into(),
                });
            }
        };

        // Read the specific version using persistence layer
        let version_to_read = match version_num {
            Some(v) => v,
            None => {
                // If no version specified, use the latest available version
                let latest = series_info.versions.iter()
                    .map(|v| v.version)
                    .max()
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "No versions available for file series".into(),
                    })?;
                debug!("🔍 ObjectStore get_range using latest version: {latest}");
                latest
            }
        };
        
        debug!("🔍 ObjectStore get_range reading version {version_to_read} for DataFusion schema inference");
        
        // Get version-specific content using read_file_version
        let version_data = match self.persistence.read_file_version(series_info.node_id, series_info.part_id, Some(version_to_read)).await {
            Ok(data) => {
                let len = data.len();
                debug!("✅ ObjectStore get_range successfully read {len} bytes from persistence");
                data
            }
            Err(e) => {
                debug!("❌ ObjectStore get_range failed to read version {version_to_read}: {e}");
                return Err(object_store::Error::Generic {
                    store: "TinyFS",
                    source: format!("Failed to read version {}: {}", version_to_read, e).into(),
                });
            }
        };
        
        let total_size = version_data.len() as u64;
        debug!("🔍 ObjectStore get_range: file has {total_size} bytes total, requested range: {#[emit::as_debug] range}");
        
        // Validate range bounds
        if range.start >= total_size {
            let start = range.start;
            debug!("❌ ObjectStore get_range: range start {start} exceeds file size {total_size}");
            return Err(object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Range start {} exceeds file size {}", range.start, total_size).into(),
            });
        }
        
        let end = std::cmp::min(range.end, total_size);
        let start_usize = range.start as usize;
        let end_usize = end as usize;
        
        if start_usize >= version_data.len() || end_usize > version_data.len() {
            let data_len = version_data.len();
            debug!("❌ ObjectStore get_range: invalid slice bounds start={start_usize}, end={end_usize}, data_len={data_len}");
            return Err(object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Invalid range bounds").into(),
            });
        }
        
        let range_data = &version_data[start_usize..end_usize];
        let range_size = range_data.len();
        debug!("🔍 ObjectStore get_range returning {range_size} bytes from range {#[emit::as_debug] range}");
        
        // Log first few bytes for debugging DataFusion schema inference
        if range.start == 0 && range_size >= 16 {
            let preview = format!("{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}",
                range_data[0], range_data[1], range_data[2], range_data[3],
                range_data[4], range_data[5], range_data[6], range_data[7],
                range_data[8], range_data[9], range_data[10], range_data[11],
                range_data[12], range_data[13], range_data[14], range_data[15]);
            debug!("🔍 ObjectStore get_range (file start): {preview}");
        }
        
        // Log last few bytes for Parquet footer detection (DataFusion reads footer first)
        if range.end == total_size && range_size >= 16 {
            let start_idx = range_size.saturating_sub(16);
            let preview = format!("{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}",
                range_data[start_idx], range_data[start_idx+1], range_data[start_idx+2], range_data[start_idx+3],
                range_data[start_idx+4], range_data[start_idx+5], range_data[start_idx+6], range_data[start_idx+7],
                range_data[start_idx+8], range_data[start_idx+9], range_data[start_idx+10], range_data[start_idx+11],
                range_data[start_idx+12], range_data[start_idx+13], range_data[start_idx+14], range_data[start_idx+15]);
            debug!("🔍 ObjectStore get_range (file end): {preview}");
        }
        
        // Check if this looks like a Parquet file
        if range.start == 0 && range_size >= 4 {
            if &range_data[0..4] == b"PAR1" {
                debug!("✅ ObjectStore get_range: File starts with PAR1 - valid Parquet magic number");
            } else {
                debug!("❌ ObjectStore get_range: File does NOT start with PAR1 - may not be valid Parquet");
            }
        }
        
        // Check for Parquet footer magic number (PAR1 at end)
        if range.end == total_size && range_size >= 4 {
            let footer_start = range_size.saturating_sub(4);
            if &range_data[footer_start..] == b"PAR1" {
                debug!("✅ ObjectStore get_range: File ends with PAR1 - valid Parquet footer");
            } else {
                debug!("❌ ObjectStore get_range: File does NOT end with PAR1 - may not be valid Parquet");
            }
        }
        
        debug!("✅ ObjectStore get_range successfully returning {range_size} bytes");
        Ok(Bytes::copy_from_slice(range_data))
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        debug!("ObjectStore delete called for location: {location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to delete data.".into(),
        })
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let registry = Arc::clone(&self.file_registry);
        let prefix = prefix.map(|p| p.as_ref().to_string());
        
        let prefix_str = prefix.as_ref().map(|p| p.as_ref()).unwrap_or("None");
        debug!("ObjectStore list called with prefix: {prefix_str}");
        
        let stream = async_stream::stream! {
            let registry = registry.read().await;
            let file_count = registry.len();
            debug!("ObjectStore has {file_count} registered file series");
            
            for (series_key, file_info) in registry.iter() {
                debug!("ObjectStore checking file series: {series_key}");
                
                // List all versions for this file series and check if any match the prefix
                for version_info in &file_info.versions {
                    let version_path = format!("{}/version/{}.parquet", series_key, version_info.version);
                    debug!("ObjectStore listing version: {version_path}");
                    
                    // Filter by prefix if specified - check the actual version path, not the series key
                    if let Some(ref prefix_str) = prefix {
                        if !version_path.starts_with(prefix_str) {
                            debug!("ObjectStore skipping version {version_path} (doesn't match prefix {prefix_str})");
                            continue;
                        }
                    }
                    
                    // CRITICAL: Skip 0-byte files during listing to prevent DataFusion schema inference issues
                    // 0-byte files are temporal override metadata only, not actual data files
                    if version_info.size == 0 {
                        debug!("ObjectStore skipping 0-byte file: {version_path} (temporal override metadata only)");
                        continue;
                    }
                    
                    let object_meta = ObjectMeta {
                        location: ObjectPath::from(version_path.clone()),
                        last_modified: chrono::Utc::now(), // TODO: use actual timestamp from version_info
                        size: version_info.size,
                        e_tag: None,
                        version: None,
                    };
                    let size = version_info.size;
                    debug!("ObjectStore yielding ObjectMeta: path={version_path}, size={size}");
                    yield Ok(object_meta);
                }
            }
        };

        stream.boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectStoreResult<ListResult> {
        let prefix_str = prefix.map(|p| p.as_ref()).unwrap_or("None");
        debug!("ObjectStore list_with_delimiter called with prefix: {prefix_str}");
        // For simplicity, treat this the same as regular list since TinyFS
        // doesn't have a natural directory structure
        let objects: Vec<ObjectMeta> = self.list(prefix).try_collect().await?;
        
        let object_count = objects.len();
        debug!("ObjectStore list_with_delimiter returning {object_count} objects");
        Ok(ListResult {
            common_prefixes: vec![], // No directory structure in TinyFS
            objects,
        })
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        debug!("ObjectStore copy called from: {from} to: {to}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to copy data.".into(),
        })
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        debug!("ObjectStore copy_if_not_exists called from: {from} to: {to}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to copy data.".into(),
        })
    }
}
