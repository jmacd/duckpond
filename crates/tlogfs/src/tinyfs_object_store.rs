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

/// TinyFS-backed ObjectStore implementation.
/// 
/// This store maps ObjectStore paths to TinyFS file handles:
/// - Path format: "/node/{node_id}" 
/// - Uses file handles created by factories (which have State access)
/// - Provides file discovery and streaming access for DataFusion ListingTable
pub struct TinyFsObjectStore {
    /// Registry of TinyFS file handles created by factories.
    /// Maps stringified node_id -> actual TinyFS file handle
    file_registry: Arc<RwLock<HashMap<String, NodeType>>>,
}

impl TinyFsObjectStore {
    /// Create a new TinyFS ObjectStore
    pub fn new() -> Self {
        Self {
            file_registry: Arc::new(RwLock::new(HashMap::new())),
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

    /// Extract node ID from ObjectStore path
    /// 
    /// Expected format: "/node/{node_id}"
    fn path_to_node_id(&self, path: &ObjectPath) -> ObjectStoreResult<String> {
        let path_str = path.as_ref();
        
        if let Some(stripped) = path_str.strip_prefix("/node/") {
            if !stripped.is_empty() && !stripped.contains('/') {
                Ok(stripped.to_string())
            } else {
                Err(object_store::Error::Generic {
                    store: "TinyFS",
                    source: "Invalid node ID path format".into(),
                })
            }
        } else {
            Err(object_store::Error::Generic {
                store: "TinyFS", 
                source: "Path must start with /node/".into(),
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
                
                Ok(ObjectMeta {
                    location: ObjectPath::from(format!("/node/{}", node_id)),
                    last_modified: chrono::Utc::now(), // TODO: use actual last_modified from metadata
                    size: metadata.size.unwrap_or(0),
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
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data.".into(),
        })
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let node_id = self.path_to_node_id(location)?;
        
        // Get file handle from registry
        let file_handle = {
            let registry = self.file_registry.read().await;
            registry.get(&node_id).cloned()
        };
        
        let file_handle = file_handle.ok_or_else(|| object_store::Error::NotFound {
            path: location.to_string(),
            source: "File not found in registry".into(),
        })?;

        // Get metadata for the file
        let object_meta = self.file_handle_to_object_meta(&node_id, &file_handle).await?;

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
            // TODO: This is a simplified implementation
            // In a real implementation, we might want to chunk the reading
            // or handle range requests here, but since DataFusion/Parquet 
            // will handle the actual data access, we can keep this simple
            let mut reader = reader;
            let mut buffer = vec![0u8; 8192]; // 8KB chunks
            
            loop {
                match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        yield Ok(Bytes::copy_from_slice(&buffer[..n]));
                    }
                    Err(e) => {
                        yield Err(object_store::Error::Generic {
                            store: "TinyFS",
                            source: format!("Failed to read file data: {}", e).into(),
                        });
                        break;
                    }
                }
            }
        };

        Ok(GetResult {
            meta: object_meta.clone(),
            payload: object_store::GetResultPayload::Stream(stream.boxed()),
            range: 0..object_meta.size,
            attributes: Default::default(),
        })
    }

    async fn delete(&self, _location: &ObjectPath) -> ObjectStoreResult<()> {
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to delete data.".into(),
        })
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let registry = Arc::clone(&self.file_registry);
        let prefix = prefix.map(|p| p.as_ref().to_string());
        
        let stream = async_stream::stream! {
            let registry = registry.read().await;
            
            for (node_id, file_handle) in registry.iter() {
                let node_path = format!("/node/{}", node_id);
                
                // Filter by prefix if specified
                if let Some(ref prefix_str) = prefix {
                    if !node_path.starts_with(prefix_str) {
                        continue;
                    }
                }
                
                // Create ObjectMeta from file handle
                match file_handle {
                    NodeType::File(file) => {
                        match file.metadata().await {
                            Ok(metadata) => {
                                let object_meta = ObjectMeta {
                                    location: ObjectPath::from(node_path),
                                    last_modified: chrono::Utc::now(), // TODO: use actual timestamp
                                    size: metadata.size.unwrap_or(0),
                                    e_tag: None,
                                    version: None,
                                };
                                yield Ok(object_meta);
                            }
                            Err(e) => {
                                yield Err(object_store::Error::Generic {
                                    store: "TinyFS",
                                    source: format!("Failed to get metadata for {}: {}", node_id, e).into(),
                                });
                            }
                        }
                    }
                    _ => {
                        // Skip non-file entries
                        continue;
                    }
                }
            }
        };

        stream.boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectStoreResult<ListResult> {
        // For simplicity, treat this the same as regular list since TinyFS
        // doesn't have a natural directory structure
        let objects: Vec<ObjectMeta> = self.list(prefix).try_collect().await?;
        
        Ok(ListResult {
            common_prefixes: vec![], // No directory structure in TinyFS
            objects,
        })
    }

    async fn copy(&self, _from: &ObjectPath, _to: &ObjectPath) -> ObjectStoreResult<()> {
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to copy data.".into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &ObjectPath, _to: &ObjectPath) -> ObjectStoreResult<()> {
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
