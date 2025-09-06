//! TinyFS-backed ObjectStore implementation for DataFusion ListingTable integration.
//! 
//! This module implements the object_store::ObjectStore trait to bridge TinyFS 
//! storage with DataFusion's ListingTable. It maps ObjectStore paths to TinyFS 
//! node IDs, enabling native predicate pushdown and streaming execution.
//!
//! Path format: "/node/{node_id}" maps to TinyFS node ID
//! 
//! Example usage:
//! ```
//! let tinyfs_store = TinyFsObjectStore::new(persistence.clone());
//! let table_url = ListingTableUrl::parse("tinyfs:///node/some_node_id")?;
//! let config = ListingTableConfig::new(table_url)
//!     .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())));
//! let table = ListingTable::try_new(config)?;
//! ```

use std::fmt;
use std::sync::Arc;
use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, 
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult
};
use tokio::sync::RwLock;
use tinyfs::NodeType;

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
    persistence: Arc<dyn tinyfs::PersistenceLayer + Send + Sync>,
}

impl TinyFsObjectStore {
    /// Create a new TinyFS ObjectStore
    pub fn new(persistence: Arc<dyn tinyfs::PersistenceLayer + Send + Sync>) -> Self {
        Self {
            file_registry: Arc::new(RwLock::new(HashMap::new())),
            persistence,
        }
    }

    /// Register a TinyFS file handle for discovery by ListingTable.
    /// 
    /// This makes the file available for listing operations. ListingTable
    /// will discover this file when scanning for files matching a prefix.
    pub async fn register_file(&self, node_id: String, file_handle: NodeType) {
        self.file_registry.write().await.insert(node_id, file_handle);
    }

    /// Remove a file from the registry
    pub async fn unregister_file(&self, node_id: &str) {
        self.file_registry.write().await.remove(node_id);
    }

    /// Register file versions for a FileSeries
    pub async fn register_file_versions(&self, series_node_id: &str, wd: &tinyfs::WD) -> Result<(), String> {
        // Resolve the path to get node_id and part_id
        let (_, lookup) = wd.resolve_path(series_node_id).await
            .map_err(|e| format!("Failed to resolve path {}: {}", series_node_id, e))?;
        
        let (node_id, part_id) = match lookup {
            tinyfs::Lookup::Found(node_path) => {
                let node_ref = node_path.borrow().await;
                // Extract node_id and part_id - for now assume part_id = node_id
                let node_id = node_path.id().await;
                let part_id = node_id; // TODO: how to get the actual part_id?
                (node_id, part_id)
            }
            _ => return Err(format!("Path not found: {}", series_node_id))
        };
        
        // Get version information using persistence layer
        let versions = self.persistence.list_file_versions(node_id, part_id).await
            .map_err(|e| format!("Failed to list file versions: {}", e))?;
        
        // Create FileSeriesInfo and store in registry
        let series_info = FileSeriesInfo {
            node_id,
            part_id, 
            versions,
        };
        
        let base_key = format!("node/{}", series_node_id);
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
    /// - "node/{node_id}/version/{version_num}" -> "node/{node_id}/version/{version_num}"
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
            // 2. "{node_id}/version/{version_num}" -> "node/{node_id}/version/{version_num}"
            if node_part.contains('/') {
                // Check if it's a valid versioned path
                if node_part.matches('/').count() == 2 && node_part.contains("/version/") {
                    Ok(format!("node/{}", node_part))
                } else {
                    Err(object_store::Error::Generic {
                        store: "TinyFS",
                        source: "Invalid versioned path format. Expected: node/{series_id}/version/{version_num}".into(),
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

    /// Convert file handle to ObjectMeta for ObjectStore interface
    async fn file_handle_to_object_meta(&self, node_id: &str, file_handle: &NodeType) -> ObjectStoreResult<ObjectMeta> {
        match file_handle {
            NodeType::File(file) => {
                let metadata = file.metadata().await.map_err(|e| object_store::Error::Generic {
                    store: "TinyFS",
                    source: format!("Failed to get file metadata: {}", e).into(),
                })?;
                let mdsize = metadata.size.unwrap();
                debug!("TinyFS metadata for {node_id}: size={mdsize}");
                
                let size = metadata.size.unwrap_or(0);
                if metadata.size.is_none() {
                    panic!("WARNING: TinyFS metadata.size is None for {node_id}, defaulting to 0");
                }
                
                Ok(ObjectMeta {
                    location: ObjectPath::from(format!("node/{}.parquet", node_id)),
                    last_modified: chrono::Utc::now(), // TODO: use actual last_modified from metadata
                    size,
                    e_tag: None,
                    version: None,
                })
            }
            _ => Err(object_store::Error::Generic {
                store: "TinyFS",
                source: "Expected file handle, got non-file NodeType".into(),
            })
        }
    }

    /// Convert file handle to ObjectMeta with version-specific metadata
    async fn file_handle_to_object_meta_versioned(&self, location: &ObjectPath, series_id: &str, file_handle: &NodeType, version_num: Option<u64>, wd: &tinyfs::WD) -> ObjectStoreResult<ObjectMeta> {
        match file_handle {
            NodeType::File(_file) => {
                let size = if let Some(version) = version_num {
                    // Get version-specific size from TinyFS
                    let versions = wd.list_file_versions(series_id).await.map_err(|e| object_store::Error::Generic {
                        store: "TinyFS",
                        source: format!("Failed to list file versions for {}: {}", series_id, e).into(),
                    })?;
                    
                    // Find the specific version
                    versions.iter()
                        .find(|v| v.version == version)
                        .map(|v| v.size)
                        .ok_or_else(|| object_store::Error::NotFound {
                            path: location.to_string(),
                            source: format!("Version {} not found for {}", version, series_id).into(),
                        })?
                } else {
                    // For non-versioned paths, use regular file metadata
                    let metadata = _file.metadata().await.map_err(|e| object_store::Error::Generic {
                        store: "TinyFS",
                        source: format!("Failed to get file metadata: {}", e).into(),
                    })?;
                    metadata.size.unwrap_or(0)
                };
                
                debug!("TinyFS metadata for {series_id} version {#[emit::as_debug] version_num}: size={size}");
                
                Ok(ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(), // TODO: use actual last_modified from metadata
                    size,
                    e_tag: None,
                    version: None,
                })
            }
            _ => Err(object_store::Error::Generic {
                store: "TinyFS",
                source: "Expected file handle, got non-file NodeType".into(),
            })
        }
    }

    /// Parse versioned path to extract series_id and version number
    /// Returns (series_key, version_number) where series_key is for registry lookup
    fn parse_versioned_path(&self, path: &ObjectPath) -> ObjectStoreResult<(String, Option<u64>)> {
        let node_id = self.path_to_node_id(path)?;
        
        // Check if this is a versioned path like "node/series_id/version/123"
        if let Some(version_part) = node_id.strip_prefix("node/") {
            if let Some(version_index) = version_part.find("/version/") {
                let series_id = &version_part[..version_index];
                let version_str = &version_part[version_index + 9..]; // "/version/".len() = 9
                
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
        let (series_key, version_num) = self.parse_versioned_path(location)?;
        debug!("ObjectStore get_opts called for location: {location}, series_key: {series_key}, version: {#[emit::as_debug] version_num}");
        let head = options.head;
        let range = format!("{:?}", options.range);
        let if_match = format!("{:?}", options.if_match);
        let if_none_match = format!("{:?}", options.if_none_match);
        debug!("ObjectStore get_opts options: head={head}, range={range}, if_match={if_match}, if_none_match={if_none_match}");
        
        // Get file handle from registry using series key
        let file_handle = {
            let registry = self.file_registry.read().await;
            let count = registry.len();
            debug!("ObjectStore registry has {count} files");
            registry.get(&series_key).cloned()
        };
        
        let file_handle = file_handle.ok_or_else(|| object_store::Error::NotFound {
            path: location.to_string(),
            source: "File not found in registry".into(),
        })?;
        debug!("ObjectStore found file handle for series_key: {series_key}");

        // Get metadata for the file (version-specific if needed)
        let series_id = series_key.strip_prefix("node/").unwrap_or(&series_key);
        let object_meta = self.file_handle_to_object_meta_versioned(location, series_id, &file_handle, version_num, &self.wd).await?;
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

        // Get async reader from file handle
        let reader = match &file_handle {
            NodeType::File(file) => {
                file.async_reader().await.map_err(|e| object_store::Error::Generic {
                    store: "TinyFS",
                    source: format!("Failed to create file reader: {}", e).into(),
                })?
            }
            _ => return Err(object_store::Error::Generic {
                store: "TinyFS",
                source: "Expected file handle, got non-file NodeType".into(),
            })
        };

        // For now, we'll create a simple stream from the reader
        // DataFusion/Parquet will handle seeking and range requests
        let stream = async_stream::stream! {
            debug!("ObjectStore starting to stream file content for series_key: {series_key}");
            let mut reader = reader;
            let mut buffer = vec![0u8; 8192]; // 8KB chunks
            let mut total_bytes_read = 0;
            let mut chunk_count = 0;
            
            loop {
                match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                    Ok(0) => {
                        debug!("ObjectStore stream EOF after {total_bytes_read} bytes in {chunk_count} chunks for series_key: {series_key}");
                        break; // EOF
                    }
                    Ok(n) => {
                        total_bytes_read += n;
                        chunk_count += 1;
                        let chunk_data = &buffer[..n];
                        
                        // Log first few bytes of first chunk for diagnostics
                        if chunk_count == 1 {
                            let preview = if n >= 8 {
                                format!("{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}...", 
                                    chunk_data[0], chunk_data[1], chunk_data[2], chunk_data[3],
                                    chunk_data[4], chunk_data[5], chunk_data[6], chunk_data[7])
                            } else {
                                format!("{:02x?}", &chunk_data[..n.min(8)])
                            };
                            debug!("ObjectStore chunk {chunk_count}: {n} bytes, starts with: {preview}");
                        } else {
                            debug!("ObjectStore chunk {chunk_count}: {n} bytes (total: {total_bytes_read})");
                        }
                        
                        yield Ok(Bytes::copy_from_slice(chunk_data));
                    }
                    Err(e) => {
                        debug!("ObjectStore stream error after {total_bytes_read} bytes: {e}");
                        yield Err(object_store::Error::Generic {
                            store: "TinyFS",
                            source: format!("Failed to read file data: {}", e).into(),
                        });
                        break;
                    }
                }
            }
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
            debug!("ObjectStore has {file_count} registered files");
            
            for (node_id, file_handle) in registry.iter() {
                let node_path = format!("node/{}.parquet", node_id);
                debug!("ObjectStore checking file: {node_path}");
                
                // Filter by prefix if specified
                if let Some(ref prefix_str) = prefix {
                    if !node_path.starts_with(prefix_str) {
                        debug!("ObjectStore skipping {node_path} (doesn't match prefix {prefix_str})");
                        continue;
                    }
                }
                
                // Create ObjectMeta from file handle
                match file_handle {
                    NodeType::File(file) => {
                        match file.metadata().await {
                            Ok(metadata) => {
                                let size = metadata.size.unwrap_or(0);
                                debug!("ObjectStore listing file {node_path} with size {size}");
                                let object_meta = ObjectMeta {
                                    location: ObjectPath::from(node_path),
                                    last_modified: chrono::Utc::now(), // TODO: use actual timestamp
                                    size,
                                    e_tag: None,
                                    version: None,
                                };
                                yield Ok(object_meta);
                            }
                            Err(e) => {
                                debug!("ObjectStore error getting metadata for {node_id}: {e}");
                                yield Err(object_store::Error::Generic {
                                    store: "TinyFS",
                                    source: format!("Failed to get metadata for {}: {}", node_id, e).into(),
                                });
                            }
                        }
                    }
                    _ => {
                        // Skip non-file entries
                        debug!("ObjectStore skipping non-file entry: {node_id}");
                        continue;
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_node_id() {
        let store = TinyFsObjectStore::new();
        
        // Valid node ID path
        let path = ObjectPath::from("/node/test_node_123");
        assert_eq!(store.path_to_node_id(&path).unwrap(), "test_node_123");
        
        // Invalid paths
        let invalid_paths = vec![
            "/invalid/path",
            "/node/",
            "/node/nested/path",
            "node/missing_slash",
        ];
        
        for invalid_path in invalid_paths {
            let path = ObjectPath::from(invalid_path);
            assert!(store.path_to_node_id(&path).is_err());
        }
    }

    #[tokio::test]
    async fn test_file_registration() {
        let store = TinyFsObjectStore::new();
        
        // For testing, we'll need to create a mock NodeType::File
        // This test will need to be completed when we have access to 
        // the actual file creation methods
        
        // Verify initial state - no files
        let objects: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
        assert_eq!(objects.len(), 0);
        
        // TODO: Add actual file registration test when file creation methods are available
    }
}
