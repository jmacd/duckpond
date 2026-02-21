// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! TinyFS-backed ObjectStore implementation for DataFusion ListingTable integration.
//!
//! This module implements the object_store::ObjectStore trait to bridge TinyFS
//! storage with DataFusion's ListingTable. It maps ObjectStore paths to TinyFS
//! node IDs, enabling native predicate pushdown and streaming execution.
//!
//! Path format: "/node/{node_id}" maps to TinyFS node ID

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use crate::tinyfs_path::TinyFsPathBuilder;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use log::debug;
use object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult, path::Path as ObjectPath,
};
use tinyfs::PersistenceLayer;

/// File series information for ObjectStore registry
#[derive(Debug, Clone)]
struct FileSeriesInfo {
    /// FileID for the file series
    file_id: tinyfs::FileID,
    /// Version information for all versions in the series
    versions: Vec<tinyfs::FileVersionInfo>,
}

/// Metadata for a file version (cached from list() call)
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are cached for future use; currently only presence check matters
struct CachedVersionMeta {
    size: u64,
    sha256: Option<String>,
}

/// TinyFS-backed ObjectStore implementation.
///
/// This store maps ObjectStore paths to TinyFS file handles:
/// - Path format: "/node/{node_id}"
/// - Generic over any PersistenceLayer implementation
/// - Provides file discovery and streaming access for DataFusion ListingTable
pub struct TinyFsObjectStore<P: PersistenceLayer> {
    /// Persistence layer for dynamic file discovery and version access
    persistence: P,
    /// Metadata cache: maps clean path (without metadata) to version metadata
    /// Populated during list() calls, consumed by get_range() to avoid redundant queries
    metadata_cache: Arc<Mutex<HashMap<String, CachedVersionMeta>>>,
}

impl<P: PersistenceLayer> TinyFsObjectStore<P> {
    /// Create a new TinyFS ObjectStore from any PersistenceLayer
    #[must_use]
    pub fn new(persistence: P) -> Self {
        Self {
            persistence,
            metadata_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create ObjectMeta for a specific version
    fn create_object_meta_for_version(
        &self,
        location: &ObjectPath,
        series_info: &FileSeriesInfo,
        version_num: Option<u64>,
    ) -> ObjectStoreResult<ObjectMeta> {
        match version_num {
            Some(version) => {
                // Find the specific version info
                let version_info = series_info
                    .versions
                    .iter()
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
                let latest_version = series_info
                    .versions
                    .iter()
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
    /// Parse versioned path to extract series_id and version number using canonical parser
    /// Returns (series_key, version_number) where series_key is for registry lookup
    fn parse_versioned_path(&self, path: &ObjectPath) -> ObjectStoreResult<(String, Option<u64>)> {
        let path_str = path.as_ref();

        // Use canonical parser for consistency
        match parse_tinyfs_path(path_str) {
            Ok(parsed) => {
                // Create series key using FileID components
                let series_key = format!(
                    "node/{}/part/{}",
                    parsed.file_id.node_id(),
                    parsed.file_id.part_id()
                );
                Ok((series_key, parsed.version))
            }
            Err(err) => Err(object_store::Error::Generic {
                store: "TinyFS",
                source: err.into(),
            }),
        }
    }
}

impl<P: PersistenceLayer> fmt::Display for TinyFsObjectStore<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TinyFsObjectStore")
    }
}

impl<P: PersistenceLayer> fmt::Debug for TinyFsObjectStore<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TinyFsObjectStore")
            .field("persistence", &"<Persistence>")
            .field("node_registry", &"<NodeRegistry>")
            .finish()
    }
}

#[async_trait]
impl<P: PersistenceLayer + Clone + 'static> ObjectStore for TinyFsObjectStore<P> {
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
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data."
                .into(),
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
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data."
                .into(),
        })
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let path = location.as_ref();
        debug!("ObjectStore get_opts called for path: {path}");
        let (series_key, version_num) = self.parse_versioned_path(location)?;
        debug!(
            "ObjectStore get_opts called for location: {location}, series_key: {series_key}, version: {version_num:?}"
        );
        let head = options.head;
        let range = format!("{:?}", options.range);
        let if_match = format!("{:?}", options.if_match);
        let if_none_match = format!("{:?}", options.if_none_match);
        debug!(
            "ObjectStore get_opts options: head={head}, range={range}, if_match={if_match}, if_none_match={if_none_match}"
        );

        // Parse the path to get node_id and part_id for dynamic discovery
        let parsed_path =
            parse_tinyfs_path(location.as_ref()).map_err(|err| object_store::Error::Generic {
                store: "TinyFS",
                source: err.into(),
            })?;

        // Query persistence layer dynamically for file versions
        let versions = self
            .persistence
            .list_file_versions(parsed_path.file_id)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Failed to list file versions: {}", e).into(),
            })?;

        if versions.is_empty() {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "No file versions found".into(),
            });
        }

        // Create FileSeriesInfo dynamically
        let series_info = FileSeriesInfo {
            file_id: parsed_path.file_id,
            versions,
        };
        debug!("ObjectStore dynamically discovered file series for series_key: {series_key}");

        // Get version-specific metadata
        let object_meta =
            self.create_object_meta_for_version(location, &series_info, version_num)?;
        let size = object_meta.size;
        debug!("ObjectStore file metadata - size: {size}");

        // If this is a head request, return metadata only
        if options.head {
            return Ok(GetResult {
                meta: object_meta.clone(),
                payload: object_store::GetResultPayload::Stream(futures::stream::empty().boxed()),
                range: 0..object_meta.size,
                attributes: Default::default(),
            });
        }

        // Read the specific version using persistence layer
        let version_to_read = match version_num {
            Some(v) => v,
            None => {
                // If no version specified, use the latest available version
                series_info
                    .versions
                    .iter()
                    .map(|v| v.version)
                    .max()
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "No versions available for file series".into(),
                    })?
            }
        };

        // Get version-specific content using read_file_version (which returns Vec<u8>)
        let version_data = self
            .persistence
            .read_file_version(series_info.file_id, version_to_read)
            .await
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

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: Range<u64>,
    ) -> ObjectStoreResult<Bytes> {
        let path = location.as_ref();
        debug!("[SEARCH] ObjectStore get_range called for path: {path}, range: {range:?}");

        let (_series_key, version_num) = match self.parse_versioned_path(location) {
            Ok(result) => {
                let _series_key = &result.0;
                let version_num = &result.1;
                debug!(
                    "[OK] ObjectStore get_range parsed path - series_key: {_series_key}, version: {version_num:?}"
                );
                result
            }
            Err(e) => {
                debug!("[ERR] ObjectStore get_range failed to parse path {path}: {e}");
                return Err(e);
            }
        };

        // OPTIMIZATION: Check cache first to avoid database query
        let location_str = location.as_ref();

        // Parse the path to get node_id and part_id
        let parsed_path =
            parse_tinyfs_path(location_str).map_err(|err| object_store::Error::Generic {
                store: "TinyFS",
                source: err.into(),
            })?;

        // Try to get metadata from cache
        let cached_metadata = if let Ok(cache) = self.metadata_cache.lock() {
            cache.get(location_str).cloned()
        } else {
            None
        };

        let has_cached_metadata = cached_metadata.is_some();

        if has_cached_metadata {
            debug!("[OK] ObjectStore get_range: metadata found in cache, skipping version query");
        } else {
            debug!(
                "[WARN] ObjectStore get_range: no cached metadata, will query for latest version if needed"
            );
        }

        // FAIL-FAST: Version must be specified in path for file series
        let version_to_read = version_num.ok_or_else(|| object_store::Error::Generic {
            store: "TinyFS",
            source: format!(
                "File series path '{}' missing version number. Paths must include version like 'tinyfs:///.../file.series?version=1'",
                location
            ).into(),
        })?;
        debug!("[SEARCH] ObjectStore get_range using version: {version_to_read}");

        debug!(
            "[SEARCH] ObjectStore get_range reading version {version_to_read} for DataFusion schema inference"
        );

        // Get version-specific content using read_file_version
        let version_data = match self
            .persistence
            .read_file_version(parsed_path.file_id, version_to_read)
            .await
        {
            Ok(data) => {
                let len = data.len();
                debug!("[OK] ObjectStore get_range successfully read {len} bytes from persistence");
                data
            }
            Err(e) => {
                debug!("[ERR] ObjectStore get_range failed to read version {version_to_read}: {e}");
                return Err(object_store::Error::Generic {
                    store: "TinyFS",
                    source: format!("Failed to read version {}: {}", version_to_read, e).into(),
                });
            }
        };

        let total_size = version_data.len() as u64;
        debug!(
            "[SEARCH] ObjectStore get_range: file has {total_size} bytes total, requested range: {range:?}"
        );

        // Validate range bounds
        if range.start >= total_size {
            let start = range.start;
            debug!(
                "[ERR] ObjectStore get_range: range start {start} exceeds file size {total_size}"
            );
            return Err(object_store::Error::Generic {
                store: "TinyFS",
                source: format!(
                    "Range start {} exceeds file size {}",
                    range.start, total_size
                )
                .into(),
            });
        }

        let end = std::cmp::min(range.end, total_size);
        let start_usize = range.start as usize;
        let end_usize = end as usize;

        if start_usize >= version_data.len() || end_usize > version_data.len() {
            let data_len = version_data.len();
            debug!(
                "[ERR] ObjectStore get_range: invalid slice bounds start={start_usize}, end={end_usize}, data_len={data_len}"
            );
            return Err(object_store::Error::Generic {
                store: "TinyFS", // @@@ ugh
                source: "Invalid range bounds".into(),
            });
        }

        let range_data = &version_data[start_usize..end_usize];
        let range_size = range_data.len();
        debug!("[SEARCH] ObjectStore get_range returning {range_size} bytes from range {range:?}");

        // Log first few bytes for debugging DataFusion schema inference
        if range.start == 0 && range_size >= 16 {
            let preview = format!(
                "{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}",
                range_data[0],
                range_data[1],
                range_data[2],
                range_data[3],
                range_data[4],
                range_data[5],
                range_data[6],
                range_data[7],
                range_data[8],
                range_data[9],
                range_data[10],
                range_data[11],
                range_data[12],
                range_data[13],
                range_data[14],
                range_data[15]
            );
            debug!("[SEARCH] ObjectStore get_range (file start): {preview}");
        }

        // Log last few bytes for Parquet footer detection (DataFusion reads footer first)
        if range.end == total_size && range_size >= 16 {
            let start_idx = range_size.saturating_sub(16);
            let preview = format!(
                "{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}",
                range_data[start_idx],
                range_data[start_idx + 1],
                range_data[start_idx + 2],
                range_data[start_idx + 3],
                range_data[start_idx + 4],
                range_data[start_idx + 5],
                range_data[start_idx + 6],
                range_data[start_idx + 7],
                range_data[start_idx + 8],
                range_data[start_idx + 9],
                range_data[start_idx + 10],
                range_data[start_idx + 11],
                range_data[start_idx + 12],
                range_data[start_idx + 13],
                range_data[start_idx + 14],
                range_data[start_idx + 15]
            );
            debug!("[SEARCH] ObjectStore get_range (file end): {preview}");
        }

        // Check if this looks like a Parquet file
        if range.start == 0 && range_size >= 4 {
            if &range_data[0..4] == b"PAR1" {
                debug!(
                    "[OK] ObjectStore get_range: File starts with PAR1 - valid Parquet magic number"
                );
            } else {
                debug!(
                    "[ERR] ObjectStore get_range: File does NOT start with PAR1 - may not be valid Parquet"
                );
            }
        }

        // Check for Parquet footer magic number (PAR1 at end)
        if range.end == total_size && range_size >= 4 {
            let footer_start = range_size.saturating_sub(4);
            if &range_data[footer_start..] == b"PAR1" {
                debug!("[OK] ObjectStore get_range: File ends with PAR1 - valid Parquet footer");
            } else {
                debug!(
                    "[ERR] ObjectStore get_range: File does NOT end with PAR1 - may not be valid Parquet"
                );
            }
        }

        debug!("[OK] ObjectStore get_range successfully returning {range_size} bytes");
        Ok(Bytes::copy_from_slice(range_data))
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        debug!("ObjectStore delete called for location: {location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to delete data."
                .into(),
        })
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let persistence = self.persistence.clone();
        let metadata_cache = self.metadata_cache.clone();
        let prefix = prefix.map(|p| p.as_ref().to_string());

        let prefix_str = prefix.as_ref().map(|p| p.as_ref()).unwrap_or("None");
        debug!("ObjectStore list called with prefix: {prefix_str}");

        let stream = async_stream::stream! {
            // Parse the prefix to extract both node_id and part_id for dynamic discovery
            if let Some(ref prefix_str) = prefix {
                if let Some(file_id) = extract_node_and_part_ids_from_path(prefix_str) {
                    debug!("ObjectStore extracting file versions for file_id: {file_id}");

                    // Query persistence layer directly with FileID - no pre-registration needed!
                    // file_id already constructed by extract_node_and_part_ids_from_path
                    match persistence.list_file_versions(file_id).await {
                        Ok(versions) => {
                            let version_count = versions.len();
                            debug!("ObjectStore discovered {version_count} versions for {file_id}");

                            for version_info in versions {
                                // Filter by prefix before creating path
                                let version_path = TinyFsPathBuilder::specific_version(&file_id, version_info.version);
                                if !version_path.starts_with(prefix_str) {
                                    debug!("ObjectStore skipping version {version_path} (doesn't match prefix {prefix_str})");
                                    continue;
                                }

                                // CRITICAL: Skip 0-byte files during listing to prevent DataFusion schema inference issues
                                // 0-byte files are temporal override metadata only, not actual data files
                                if version_info.size == 0 {
                                    debug!("ObjectStore skipping 0-byte file: {version_path} (temporal override metadata only)");
                                    continue;
                                }

                                // Use clean path (DataFusion-compatible format)
                                let clean_path = version_path.clone();

                                // OPTIMIZATION: Cache metadata to avoid re-querying in get_range()
                                // Store metadata keyed by clean path for later retrieval
                                let cache_key = clean_path.clone();
                                let cached_meta = CachedVersionMeta {
                                    size: version_info.size,
                                    sha256: version_info.blake3.clone(),
                                };

                                // Insert into cache (lock briefly, then release)
                                if let Ok(mut cache) = metadata_cache.lock() {
                                    let _ = cache.insert(cache_key, cached_meta);
                                }

                                debug!("ObjectStore discovered version: {version_path}");

                                let object_meta = ObjectMeta {
                                    location: ObjectPath::from(clean_path.clone()),
                                    last_modified: chrono::Utc::now(), // TODO: use actual timestamp from version_info
                                    size: version_info.size,
                                    e_tag: None,
                                    version: None,
                                };
                                let size = version_info.size;
                                debug!("ObjectStore yielding ObjectMeta: path={clean_path}, size={size}");
                                yield Ok(object_meta);
                            }
                        }
                        Err(e) => {
                            debug!("ObjectStore failed to list file versions for {file_id}: {e}");
                            yield Err(object_store::Error::Generic {
                                store: "TinyFS",
                                source: format!("Failed to list file versions for {}: {}", file_id, e).into(),
                            });
                        }
                    }
                } else {
                    debug!("ObjectStore could not extract node_id from prefix: {prefix_str}");
                }
            } else {
                debug!("ObjectStore list called with no prefix - no files to return");
            }
        };

        stream.boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
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

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        debug!("ObjectStore copy_if_not_exists called from: {from} to: {to}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to copy data.".into(),
        })
    }
}

/// Canonical TinyFS path parsing result
#[derive(Debug, Clone)]
struct TinyFsPath {
    file_id: tinyfs::FileID,
    version: Option<u64>, // None = all versions, Some(n) = specific version n
}

/// Single canonical method to parse all TinyFS path formats
/// This eliminates duplication and ensures consistency across all path parsing
fn parse_tinyfs_path(path: &str) -> Result<TinyFsPath, String> {
    let parts: Vec<&str> = path.split('/').collect();

    // Handle directory paths: "directory/{node_id}"
    if parts.len() == 2 && parts[0] == "directory" {
        let node_id = parts[1]
            .parse::<uuid7::Uuid>()
            .map_err(|_| format!("Invalid directory node_id UUID: {}", parts[1]))
            .map(|uuid| tinyfs::NodeID::new(uuid.to_string()))?;

        // For directories, node_id == part_id
        let file_id = tinyfs::FileID::new_from_ids(tinyfs::PartID::from_node_id(node_id), node_id);
        return Ok(TinyFsPath {
            file_id,
            version: None, // Directories don't have explicit versions in the path
        });
    }

    // Handle file paths (following partition -> node -> version hierarchy):
    // - "part/{part_id}/node/{node_id}/version/"
    // - "part/{part_id}/node/{node_id}/version/{version}.parquet"

    // Minimum: ["part", part_id, "node", node_id, "version"]
    if parts.len() < 5 || parts[0] != "part" || parts[2] != "node" || parts[4] != "version" {
        return Err(format!(
            "Invalid TinyFS path format. Expected: part/{{part_id}}/node/{{node_id}}/version/[{{version}}.parquet] or directory/{{node_id}}, got: {}",
            path
        ));
    }

    // Parse part_id and node_id (following correct hierarchy)
    let part_id = parts[1]
        .parse::<uuid7::Uuid>()
        .map_err(|_| format!("Invalid part_id UUID: {}", parts[1]))
        .map(|uuid| tinyfs::PartID::new(uuid.to_string()))?;

    let node_id = parts[3]
        .parse::<uuid7::Uuid>()
        .map_err(|_| format!("Invalid node_id UUID: {}", parts[3]))
        .map(|uuid| tinyfs::NodeID::new(uuid.to_string()))?;

    // Determine version from path format
    let version = if parts.len() == 5 {
        // Directory format: ends with "version/" -> all versions
        None
    } else if parts.len() == 6 {
        // Specific version format: "version/{version}.parquet"
        let version_str = parts[5]
            .strip_suffix(".parquet")
            .ok_or_else(|| format!("Version file must end with .parquet: {}", parts[5]))?;
        let version_num = version_str
            .parse::<u64>()
            .map_err(|_| format!("Invalid version number: {}", version_str))?;
        Some(version_num)
    } else {
        return Err(format!("Invalid TinyFS path length: {}", path));
    };

    let file_id = tinyfs::FileID::new_from_ids(part_id, node_id);
    Ok(TinyFsPath { file_id, version })
}

/// Extract FileID from a tinyfs:// path using canonical parser
/// Examples:
/// - "part/987fcdeb-51a2-4321-8765-432109876543/node/019945f3-031b-7e54-863d-895392f16dac/version" -> Some(file_id)
/// - "part/987fcdeb-51a2-4321-8765-432109876543/node/019945f3-031b-7e54-863d-895392f16dac/version/1.parquet" -> Some(file_id)
fn extract_node_and_part_ids_from_path(path: &str) -> Option<tinyfs::FileID> {
    parse_tinyfs_path(path).ok().map(|parsed| parsed.file_id)
}

/// Register TinyFsObjectStore with a SessionContext
///
/// This is a helper function to register a TinyFsObjectStore with DataFusion's SessionContext,
/// making TinyFS files accessible via the `tinyfs:///` URL scheme.
///
/// Returns the registered ObjectStore for further use if needed.
pub fn register_tinyfs_object_store<P: PersistenceLayer + Clone + 'static>(
    ctx: &datafusion::execution::context::SessionContext,
    persistence: P,
) -> Result<Arc<TinyFsObjectStore<P>>, Box<dyn std::error::Error + Send + Sync>> {
    let object_store = Arc::new(TinyFsObjectStore::new(persistence));

    let url =
        url::Url::parse("tinyfs:///").map_err(|e| format!("Failed to parse tinyfs URL: {}", e))?;

    _ = ctx
        .runtime_env()
        .register_object_store(&url, object_store.clone());

    debug!("Registered TinyFS ObjectStore with SessionContext - ready for any TinyFS path");
    Ok(object_store)
}
