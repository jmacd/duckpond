// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Read-only "remote-read pond": a [`PersistenceLayer`] backed directly by a
//! content-addressed S3 remote ([`ContentRemote`]), so a pond can be queried
//! (and eventually rendered) without a full local pull.
//!
//! The structure of the pond (its directory tree and node identities) is
//! reconstructed at open time from just two remote objects -- the tip commit
//! and its node manifest -- so opening a remote pond is cheap and downloads no
//! bulk content.  A node's ordered version blob hashes are resolved lazily (a
//! series object is fetched and decoded only on first access), and the *bulk*
//! content -- the per-version parquet blobs -- is fetched lazily too, only for
//! the nodes a query actually reads, and cached in memory by content hash.
//!
//! Because the DataFusion read path in this workspace goes through the
//! persistence-generic `tinyfs://` object store
//! (`provider::register_tinyfs_object_store`), a query over a
//! [`RemotePersistence`] resolves each series' versions via
//! [`PersistenceLayer::list_file_versions`] and reads their bytes via
//! [`PersistenceLayer::read_file_version`] -- both served here from the remote.
//!
//! This layer is **read-only**: every mutating [`PersistenceLayer`] method
//! returns an error.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex as TokioMutex;

use provider::{ConfigFile, FactoryContext, FactoryRegistry, register_tinyfs_object_store};
use sync_store::content::{Commit, ObjectHash, decode_manifest, decode_recipe, decode_series};
use sync_store::{ContentRemote, register_s3_handlers};

use tinyfs::memory::{MemoryDirectory, MemorySymlink};
use tinyfs::transaction_guard::TransactionState;
use tinyfs::{
    AsyncReadSeek, EntryType, Error as TinyError, File, FileHandle, FileID, FileMetadataWriter,
    FileVersionInfo, Metadata, Node, NodeID, NodeMetadata, NodeType, PartID, PersistenceLayer,
    ProviderContext, QueryableFile, Result as TinyResult, ResultExt,
};

use crate::StewardError;

/// Filename of the on-disk sidecar (under `cache_dir`) mapping content-object
/// hashes to their byte sizes, one `"<hex> <size>\n"` line per entry, so a
/// fresh warm process enumerates version sizes without re-downloading blobs.
const VERSION_SIZES_SIDECAR: &str = "version_sizes.json";

/// Per-node information distilled from the tip commit's node manifest. This is
/// the *only* information fetched eagerly at open time (one manifest object for
/// the whole pond); a node's version blob hashes and bytes are resolved lazily,
/// on first access, so opening a remote pond never downloads bulk content.
struct NodeRecord {
    entry_type: EntryType,
    /// The node's content address, exactly as the manifest records it:
    ///   - series (`FilePhysicalSeries`/`TablePhysicalSeries`): a *series
    ///     object* hash listing the ordered version blob hashes;
    ///   - single-version file: the single version blob hash;
    ///   - dynamic node: the recipe (factory config) blob hash;
    ///   - symlink: the target-path blob hash;
    ///   - directory: its tree hash (unused -- children come from the manifest).
    child_hash: ObjectHash,
}

/// Shared, read-only state behind a [`RemotePersistence`].
struct RemoteState {
    remote: ContentRemote,
    pond_id: uuid7::Uuid,
    txn_state: Arc<TransactionState>,
    /// DataFusion session used by dynamic-node factories to resolve and query
    /// pond paths. The `tinyfs://` object store is registered on it at open
    /// time so factory reads resolve through this same remote persistence.
    session: Arc<SessionContext>,
    /// Local directory for the temporal-reduce rollup cache and format cache
    /// (`~/.watertown/cache/remote/<pond_uuid>/` by default). `None` disables
    /// caching, forcing every derived-series query to recompute from source.
    cache_dir: Option<PathBuf>,
    /// Node records keyed by the source `NodeID` (which encodes entry type).
    records: HashMap<NodeID, NodeRecord>,
    /// Lazily fetched content bytes (small inline objects and large external
    /// blobs alike), keyed by content hash.
    blob_cache: TokioMutex<HashMap<ObjectHash, Arc<Vec<u8>>>>,
    /// Content-object byte sizes, resolved cheaply (HEAD / inline-row length)
    /// without downloading blob bodies. Persisted to a `version_sizes.json`
    /// sidecar in `cache_dir` so a fresh warm process enumerates versions
    /// without re-fetching the whole ingest history. Keyed by content hash.
    sizes: TokioMutex<HashMap<ObjectHash, u64>>,
    /// Lazily resolved, ordered version blob hashes per node (series objects are
    /// decoded once, on first access), keyed by source `NodeID`.
    versions: TokioMutex<HashMap<NodeID, Arc<Vec<ObjectHash>>>>,
    /// The fully materialized node tree (directories populated with children),
    /// keyed by `FileID`, for `load_node` and path resolution. Set once after
    /// construction.
    nodes: OnceLock<HashMap<FileID, Node>>,
    /// name-indexed children: parent `NodeID` -> [(name, child `NodeID`)].
    children: HashMap<NodeID, Vec<(String, NodeID)>>,
    /// The root node's `FileID`.
    root_id: FileID,
}

/// A read-only [`PersistenceLayer`] backed by a content-addressed S3 remote.
#[derive(Clone)]
pub struct RemotePersistence(Arc<RemoteState>);

fn readonly_err(op: &str) -> TinyError {
    TinyError::Other(format!("remote-read pond is read-only: {op} not supported"))
}

impl RemotePersistence {
    /// Resolve a slash-separated absolute path (e.g. `/reduced/system_pressure`)
    /// to a node, walking the eagerly built tree. Returns `None` if any segment
    /// is missing.
    #[must_use]
    pub fn find_node(&self, path: &str) -> Option<Node> {
        let st = &self.0;
        let nodes = st.nodes.get()?;
        // Start at root.
        let mut current = st.root_id.node_id();
        for seg in path.split('/').filter(|s| !s.is_empty()) {
            let kids = st.children.get(&current)?;
            let (_, child) = kids.iter().find(|(name, _)| name == seg)?;
            current = *child;
        }
        // Reconstruct the FileID for `current` and look it up.
        nodes
            .values()
            .find(|n| n.id().node_id() == current)
            .cloned()
    }

    /// The pond's root `FileID`.
    #[must_use]
    pub fn root_id(&self) -> FileID {
        self.0.root_id
    }

    /// The DataFusion session this pond's factories query through (the
    /// `tinyfs://` object store is registered on it).
    #[must_use]
    pub fn session(&self) -> Arc<SessionContext> {
        self.0.session.clone()
    }

    /// A [`ProviderContext`] over this remote persistence, suitable for building
    /// a [`provider::Provider`] and resolving/querying pond paths (including
    /// dynamic directories such as `reduced`/`analysis`). When a cache dir is
    /// configured, it is attached so temporal-reduce serves derived series from
    /// the incremental rollup cache instead of recomputing from source.
    #[must_use]
    pub fn provider_context(&self) -> ProviderContext {
        let mut ctx = ProviderContext::new(self.0.session.clone(), Arc::new(self.clone()));
        if let Some(dir) = &self.0.cache_dir {
            ctx = ctx.with_cache_dir(dir.clone());
        }
        ctx
    }

    /// The local cache directory backing this pond's derived-series queries, if
    /// caching is enabled.
    #[must_use]
    pub fn cache_dir(&self) -> Option<&std::path::Path> {
        self.0.cache_dir.as_deref()
    }

    /// Return an indented listing of the node tree (name + entry type), capped
    /// at `max_depth`, for discovery/debugging.
    #[must_use]
    pub fn tree_lines(&self, max_depth: usize) -> Vec<String> {
        fn walk(
            st: &RemoteState,
            nid: NodeID,
            depth: usize,
            max_depth: usize,
            out: &mut Vec<String>,
        ) {
            if depth > max_depth {
                return;
            }
            let Some(kids) = st.children.get(&nid) else {
                return;
            };
            let mut kids = kids.clone();
            kids.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, cnid) in kids {
                let et = st.records.get(&cnid).map(|r| r.entry_type);
                out.push(format!("{}{name} [{et:?}]", "  ".repeat(depth)));
                walk(st, cnid, depth + 1, max_depth, out);
            }
        }
        let mut out = Vec::new();
        walk(&self.0, self.0.root_id.node_id(), 0, max_depth, &mut out);
        out
    }

    /// Fetch the bytes of a content object by hash, lazily. Tries the in-memory
    /// cache, then an inline `objects` row, then the external `_blobs` store.
    /// The result is cached so a version blob is downloaded at most once.
    async fn blob_bytes(&self, hash: ObjectHash) -> TinyResult<Arc<Vec<u8>>> {
        let st = &self.0;
        {
            let cache = st.blob_cache.lock().await;
            if let Some(bytes) = cache.get(&hash) {
                return Ok(bytes.clone());
            }
        }
        // Small objects live inline as an `objects` Delta row; large blobs live
        // out-of-row in the `_blobs` store.  Try inline first, then the blob
        // store, so either representation resolves transparently.
        let bytes = if let Some(inline) = st
            .remote
            .get_object(hash)
            .await
            .map_err(|e| TinyError::Other(format!("remote get_object: {e}")))?
        {
            inline
        } else {
            let mut reader = st
                .remote
                .get_blob_reader(hash)
                .await
                .map_err(|e| TinyError::Other(format!("remote blob get: {e}")))?
                .ok_or_else(|| {
                    TinyError::Other(format!("object {} absent from remote", hash.to_hex()))
                })?;
            let mut buf = Vec::new();
            let _ = reader
                .read_to_end(&mut buf)
                .await
                .map_err(|e| TinyError::Other(format!("remote blob read: {e}")))?;
            buf
        };
        let arc = Arc::new(bytes);
        let mut cache = st.blob_cache.lock().await;
        let _ = cache.insert(hash, arc.clone());
        Ok(arc)
    }

    /// Resolve the byte size of a content object cheaply, without downloading
    /// its body. Tries the in-memory size map (seeded from the on-disk
    /// `version_sizes.json` sidecar and from any already-downloaded blob), then
    /// a HEAD/inline-length probe on the remote. Newly resolved sizes are cached
    /// in memory and appended to the sidecar so a fresh warm process enumerates
    /// versions without re-fetching the whole ingest history.
    async fn blob_size(&self, hash: ObjectHash) -> TinyResult<u64> {
        let st = &self.0;
        {
            let sizes = st.sizes.lock().await;
            if let Some(sz) = sizes.get(&hash) {
                return Ok(*sz);
            }
        }
        // If the blob happens to be resident already, take its length for free.
        if let Some(bytes) = st.blob_cache.lock().await.get(&hash) {
            let sz = bytes.len() as u64;
            self.record_size(hash, sz).await;
            return Ok(sz);
        }
        let sz = st
            .remote
            .object_size(hash)
            .await
            .map_err(|e| TinyError::Other(format!("remote object_size: {e}")))?
            .ok_or_else(|| {
                TinyError::Other(format!("object {} absent from remote", hash.to_hex()))
            })?;
        self.record_size(hash, sz).await;
        Ok(sz)
    }

    /// Cache a resolved size in memory and append it to the on-disk sidecar
    /// (best-effort; a write failure just means a future process re-HEADs it).
    async fn record_size(&self, hash: ObjectHash, size: u64) {
        let st = &self.0;
        {
            let mut sizes = st.sizes.lock().await;
            if sizes.insert(hash, size).is_some() {
                return;
            }
        }
        if let Some(dir) = &st.cache_dir {
            let path = dir.join(VERSION_SIZES_SIDECAR);
            let line = format!("{} {}\n", hash.to_hex(), size);
            use std::io::Write as _;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
            {
                let _ = f.write_all(line.as_bytes());
            }
        }
    }

    /// Resolve a node's ordered version blob hashes, lazily and once. For a
    /// series node this fetches and decodes the series object; for a
    /// single-version file it is just the record's `child_hash`; directories,
    /// symlinks, and dynamic nodes have no versions.
    async fn versions(&self, id: FileID) -> TinyResult<Arc<Vec<ObjectHash>>> {
        let nid = id.node_id();
        {
            let cache = self.0.versions.lock().await;
            if let Some(v) = cache.get(&nid) {
                return Ok(v.clone());
            }
        }
        let rec = self.record(id)?;
        let et = rec.entry_type;
        let child_hash = rec.child_hash;
        let versions = match et {
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                let bytes = self.blob_bytes(child_hash).await?;
                decode_series(&bytes)
                    .map_err(|e| TinyError::Other(format!("decode series: {e}")))?
            }
            EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
                vec![child_hash]
            }
            // Directories, symlinks, and dynamic nodes carry no version blobs.
            _ => Vec::new(),
        };
        let arc = Arc::new(versions);
        let mut cache = self.0.versions.lock().await;
        let _ = cache.insert(nid, arc.clone());
        Ok(arc)
    }

    fn record(&self, id: FileID) -> TinyResult<&NodeRecord> {
        self.0
            .records
            .get(&id.node_id())
            .ok_or(TinyError::IDNotFound(id))
    }
}

#[async_trait]
impl PersistenceLayer for RemotePersistence {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn transaction_state(&self) -> Arc<TransactionState> {
        self.0.txn_state.clone()
    }

    fn pond_uuid(&self) -> uuid7::Uuid {
        self.0.pond_id
    }

    async fn load_node(&self, id: FileID) -> TinyResult<Node> {
        self.0
            .nodes
            .get()
            .and_then(|m| m.get(&id).cloned())
            .ok_or(TinyError::IDNotFound(id))
    }

    async fn store_node(&self, _node: &Node) -> TinyResult<()> {
        Err(readonly_err("store_node"))
    }

    async fn create_file_node(&self, _id: FileID) -> TinyResult<Node> {
        Err(readonly_err("create_file_node"))
    }

    async fn create_directory_node(&self, _id: FileID) -> TinyResult<Node> {
        Err(readonly_err("create_directory_node"))
    }

    async fn create_symlink_node(
        &self,
        _id: FileID,
        _target: &std::path::Path,
    ) -> TinyResult<Node> {
        Err(readonly_err("create_symlink_node"))
    }

    async fn create_dynamic_node(
        &self,
        _id: FileID,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> TinyResult<Node> {
        Err(readonly_err("create_dynamic_node"))
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> TinyResult<Option<(String, Vec<u8>)>> {
        let rec = self.record(id)?;
        if !rec.entry_type.is_dynamic() {
            return Ok(None);
        }
        let bytes = self.blob_bytes(rec.child_hash).await?;
        let (factory_type, config) =
            decode_recipe(&bytes).map_err(|e| TinyError::Other(format!("decode recipe: {e}")))?;
        Ok(Some((factory_type, config)))
    }

    async fn update_dynamic_node_config(
        &self,
        _id: FileID,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> TinyResult<()> {
        Err(readonly_err("update_dynamic_node_config"))
    }

    async fn metadata(&self, id: FileID) -> TinyResult<NodeMetadata> {
        let rec = self.record(id)?;
        // Report metadata without downloading any content: `size = None` keeps
        // path traversal and node inspection cheap. The object store's listing
        // path (which needs true per-version sizes to honor the skip-0-byte
        // contract) goes through `list_file_versions`, not here.
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: rec.entry_type,
            timestamp: 0,
        })
    }

    async fn list_file_versions(&self, id: FileID) -> TinyResult<Vec<FileVersionInfo>> {
        let entry_type = self.record(id)?.entry_type;
        let hashes = self.versions(id).await?;
        let mut out = Vec::with_capacity(hashes.len());
        for (i, hash) in hashes.iter().enumerate() {
            let size = self.blob_size(*hash).await?;
            out.push(FileVersionInfo {
                version: (i + 1) as u64,
                timestamp: 0,
                size,
                blake3: None,
                entry_type,
                extended_metadata: None,
            });
        }
        Ok(out)
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> TinyResult<Vec<u8>> {
        let hashes = self.versions(id).await?;
        let idx = (version as usize)
            .checked_sub(1)
            .ok_or_else(|| TinyError::Other(format!("invalid version {version} (1-based)")))?;
        let hash = hashes
            .get(idx)
            .ok_or_else(|| TinyError::Other(format!("version {version} out of range")))?;
        Ok((*self.blob_bytes(*hash).await?).clone())
    }

    async fn set_extended_attributes(
        &self,
        _id: FileID,
        _attributes: HashMap<String, String>,
    ) -> TinyResult<()> {
        Err(readonly_err("set_extended_attributes"))
    }

    async fn get_temporal_bounds(&self, _id: FileID) -> TinyResult<Option<(i64, i64)>> {
        Ok(None)
    }
}

/// A read-only [`File`] handle backed by a [`RemotePersistence`]. Mirrors the
/// queryable behavior of `tinyfs::memory::MemoryFile`: SQL queries resolve
/// through the registered `tinyfs://` object store, which reads versions from
/// the persistence layer (and thus lazily from the remote).
struct RemoteFile {
    id: FileID,
    persistence: RemotePersistence,
    entry_type: EntryType,
}

impl RemoteFile {
    fn new_handle(id: FileID, persistence: RemotePersistence, entry_type: EntryType) -> FileHandle {
        FileHandle::new(Arc::new(TokioMutex::new(Box::new(RemoteFile {
            id,
            persistence,
            entry_type,
        }))))
    }
}

#[async_trait]
impl Metadata for RemoteFile {
    async fn metadata(&self) -> TinyResult<NodeMetadata> {
        self.persistence.metadata(self.id).await
    }
}

#[async_trait]
impl File for RemoteFile {
    async fn async_reader(&self) -> TinyResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        // Concatenate all versions (no cumulative bao/blake3 validation here: the
        // content-addressed fetch already verified inline objects, and external
        // blobs are hash-addressed).
        let versions = self.persistence.list_file_versions(self.id).await?;
        let mut buf = Vec::new();
        for v in &versions {
            let chunk = self
                .persistence
                .read_file_version(self.id, v.version)
                .await?;
            buf.extend_from_slice(&chunk);
        }
        Ok(Box::pin(std::io::Cursor::new(buf)))
    }

    async fn async_writer(&self) -> TinyResult<std::pin::Pin<Box<dyn FileMetadataWriter>>> {
        Err(readonly_err("async_writer"))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn QueryableFile> {
        if self.entry_type.is_parquet_file() {
            Some(self)
        } else {
            None
        }
    }
}

#[async_trait]
impl QueryableFile for RemoteFile {
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &ProviderContext,
    ) -> TinyResult<Arc<dyn datafusion::catalog::TableProvider>> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        // Same URL contract as MemoryFile/tlogfs: the registered TinyFsObjectStore
        // resolves versions from the persistence layer.
        let url_pattern = format!(
            "tinyfs:///pond/{}/part/{}/node/{}/version/",
            id.pond_id(),
            id.part_id(),
            id.node_id()
        );
        let table_url =
            ListingTableUrl::parse(&url_pattern).map_other_context("Failed to parse table URL")?;
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);
        let config_with_schema = config
            .infer_schema(&context.datafusion_session.state())
            .await
            .map_other_context("Schema inference failed")?;
        let listing_table = ListingTable::try_new(config_with_schema)
            .map_other_context("ListingTable creation failed")?;
        Ok(Arc::new(listing_table))
    }
}

/// Open a read-only pond over the content-addressed remote at `url` (e.g.
/// `s3://water-staging`), using `storage_options` (endpoint / credentials).
///
/// This registers the S3-compatible object-store handlers, opens the remote,
/// walks the object graph reachable from `refs/main`, and materializes the node
/// tree. Only small content objects are fetched here; series version parquet
/// blobs are read lazily on query.
pub async fn open_remote_pond(
    url: &str,
    storage_options: HashMap<String, String>,
) -> Result<RemotePersistence, StewardError> {
    register_s3_handlers();
    let remote = ContentRemote::open_at_url(url, storage_options)
        .await
        .map_err(|e| StewardError::Content(format!("open remote {url}: {e}")))?;

    // Open with just two objects: the tip commit and its node manifest. The
    // manifest is the whole node table (one entry per node) -- no trees or bulk
    // blobs are fetched here.
    let tip = remote
        .get_tip("main")
        .await
        .map_err(|e| StewardError::Content(format!("get tip: {e}")))?
        .ok_or_else(|| StewardError::Content(format!("remote {url} has no 'main' ref")))?;
    let commit_bytes = remote
        .get_object(tip)
        .await
        .map_err(|e| StewardError::Content(format!("get tip commit: {e}")))?
        .ok_or_else(|| StewardError::Content(format!("tip commit {} absent", tip.to_hex())))?;
    let commit = Commit::decode(&commit_bytes)
        .map_err(|e| StewardError::Content(format!("decode commit: {e}")))?;
    let manifest_bytes = remote
        .get_object(commit.node_manifest_hash)
        .await
        .map_err(|e| StewardError::Content(format!("get manifest: {e}")))?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "node manifest {} absent",
                commit.node_manifest_hash.to_hex()
            ))
        })?;
    let manifest = decode_manifest(&manifest_bytes)
        .map_err(|e| StewardError::Content(format!("decode manifest: {e}")))?;
    if manifest.is_empty() {
        return Err(StewardError::Content(format!(
            "remote {url} has an empty node manifest"
        )));
    }

    let pond_id = remote.pond_id();
    let pond_id7 = uuid7_from_uuid(pond_id);

    // Distill per-node records and the name-indexed children map.
    let mut records: HashMap<NodeID, NodeRecord> = HashMap::new();
    let mut children: HashMap<NodeID, Vec<(String, NodeID)>> = HashMap::new();
    let mut file_ids: HashMap<NodeID, FileID> = HashMap::new();
    let mut root_id: Option<FileID> = None;

    for entry in &manifest {
        let nid = NodeID::from_hex_string(&entry.node_id)
            .map_err(|e| StewardError::Content(format!("manifest node_id: {e}")))?;
        let et = entry.entry_type;

        // Compute this node's FileID.
        let fid = if entry.parent_node_id.is_empty() {
            let r = FileID::root_for(pond_id7);
            root_id = Some(r);
            r
        } else if et == EntryType::DirectoryPhysical {
            FileID::from_physical_dir_node_id(nid, pond_id7)
        } else {
            let parent_nid = NodeID::from_hex_string(&entry.parent_node_id)
                .map_err(|e| StewardError::Content(format!("manifest parent_node_id: {e}")))?;
            FileID::new_from_ids(PartID::from_node_id(parent_nid), nid, pond_id7)
        };
        let _ = file_ids.insert(nid, fid);

        let _ = records.insert(
            nid,
            NodeRecord {
                entry_type: et,
                child_hash: entry.child_hash,
            },
        );

        if !entry.parent_node_id.is_empty() {
            let parent_nid = NodeID::from_hex_string(&entry.parent_node_id)
                .map_err(|e| StewardError::Content(format!("manifest parent_node_id: {e}")))?;
            children
                .entry(parent_nid)
                .or_default()
                .push((entry.name.clone(), nid));
        }
    }

    let root_id = root_id.ok_or_else(|| {
        StewardError::Content("manifest has no root entry (empty parent)".to_string())
    })?;

    // Resolve the local cache directory for derived-series queries. Errors here
    // are non-fatal: a missing cache just means every query recomputes.
    let cache_dir = resolve_remote_cache_dir(pond_id);

    // Seed the version-size map from the on-disk sidecar so version enumeration
    // avoids re-HEADing (and, before this cache existed, re-downloading) blobs.
    let sizes = load_version_sizes(cache_dir.as_deref());

    // Build the shared state (without the node tree yet). The session's
    // `tinyfs://` object store is registered just below, once the persistence
    // wrapper exists, so dynamic-node factories query through this pond.
    let state = Arc::new(RemoteState {
        remote,
        pond_id: pond_id7,
        txn_state: Arc::new(TransactionState::new()),
        session: Arc::new(SessionContext::new()),
        cache_dir,
        records,
        blob_cache: TokioMutex::new(HashMap::new()),
        sizes: TokioMutex::new(sizes),
        versions: TokioMutex::new(HashMap::new()),
        nodes: OnceLock::new(),
        children,
        root_id,
    });
    let persistence = RemotePersistence(state.clone());
    let _os = register_tinyfs_object_store(&state.session, persistence.clone())
        .map_err(|e| StewardError::Content(format!("register object store: {e}")))?;

    // Materialize the node tree: create every node, then populate physical
    // directories. Dynamic nodes are instantiated through the factory registry
    // so their generated children/tables resolve lazily over this remote.
    let mut nodes: HashMap<FileID, Node> = HashMap::new();
    let mut dir_handles: HashMap<NodeID, tinyfs::DirHandle> = HashMap::new();
    for entry in &manifest {
        let nid = NodeID::from_hex_string(&entry.node_id).expect("validated above");
        let fid = *file_ids.get(&nid).expect("file id computed above");
        let et = entry.entry_type;
        let node = if et.is_dynamic() {
            // Dynamic node: its content object is a recipe (factory type +
            // config). Build the node through the factory registry so its
            // generated children (dynamic dirs) or query output (dynamic
            // files/tables) are produced on demand.
            let recipe = persistence.blob_bytes(entry.child_hash).await?;
            let (factory_type, config) = decode_recipe(&recipe)
                .map_err(|e| StewardError::Content(format!("decode recipe: {e}")))?;
            let fctx = FactoryContext::new(persistence.provider_context(), fid);
            if et.is_directory() {
                let handle = FactoryRegistry::create_directory(&factory_type, &config, fctx)
                    .map_err(|e| {
                        StewardError::Content(format!(
                            "factory '{factory_type}' create_directory for '{}': {e}",
                            entry.name
                        ))
                    })?;
                Node::new(fid, NodeType::Directory(handle))
            } else {
                // A dynamic file backed by a *queryable* factory is built via
                // `create_file`; a file backed by an *executable* factory (e.g.
                // `logfile-ingest`, run by `pond run`) has no `create_file`, so
                // its recipe config is exposed as the file's content, exactly as
                // the on-disk persistence does.
                let has_create_file = FactoryRegistry::get_factory(&factory_type)
                    .map(|f| f.create_file.is_some())
                    .unwrap_or(false);
                let handle = if has_create_file {
                    FactoryRegistry::create_file(&factory_type, &config, fctx)
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "factory '{factory_type}' create_file for '{}': {e}",
                                entry.name
                            ))
                        })?
                } else {
                    ConfigFile::new(config).create_handle()
                };
                Node::new(fid, NodeType::File(handle))
            }
        } else if et.is_directory() {
            let handle = MemoryDirectory::new_handle();
            let _ = dir_handles.insert(nid, handle.clone());
            Node::new(fid, NodeType::Directory(handle))
        } else if et == EntryType::Symlink {
            // Symlink targets are tiny leaf blobs; fetch (and cache) lazily now.
            let target = persistence
                .blob_bytes(entry.child_hash)
                .await
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .unwrap_or_default();
            Node::new(
                fid,
                NodeType::Symlink(MemorySymlink::new_handle(PathBuf::from(target))),
            )
        } else {
            Node::new(
                fid,
                NodeType::File(RemoteFile::new_handle(fid, persistence.clone(), et)),
            )
        };
        let _ = nodes.insert(fid, node);
    }

    // Populate directory handles with their children.
    for entry in &manifest {
        if entry.parent_node_id.is_empty() {
            continue;
        }
        let parent_nid = NodeID::from_hex_string(&entry.parent_node_id).expect("validated above");
        let child_nid = NodeID::from_hex_string(&entry.node_id).expect("validated above");
        let child_fid = *file_ids.get(&child_nid).expect("file id");
        let Some(child_node) = nodes.get(&child_fid).cloned() else {
            continue;
        };
        if let Some(handle) = dir_handles.get(&parent_nid) {
            handle
                .insert(entry.name.clone(), child_node)
                .await
                .map_err(|e| StewardError::Content(format!("populate dir: {e}")))?;
        }
    }

    state
        .nodes
        .set(nodes)
        .map_err(|_| StewardError::Content("node tree already set".to_string()))?;

    Ok(persistence)
}

/// Convert a `uuid::Uuid` (as returned by `ContentRemote::pond_id`) into the
/// `uuid7::Uuid` used by tinyfs `FileID`s (same 128 bits).
fn uuid7_from_uuid(u: uuid::Uuid) -> uuid7::Uuid {
    uuid7::Uuid::from(*u.as_bytes())
}

/// Resolve the local cache directory for a remote pond's derived-series
/// queries. The rollup/format caches live at
/// `<base>/remote/<pond_uuid>/`, where `<base>` is `$WATERTOWN_CACHE_DIR` if
/// set, otherwise `~/.watertown/cache`. Setting `WATERTOWN_REMOTE_NO_CACHE`
/// disables caching (returns `None`), forcing recompute-from-source.
///
/// The directory is created eagerly; on any failure (no `$HOME`, mkdir error)
/// this returns `None` and queries simply recompute rather than fail.
fn resolve_remote_cache_dir(pond_id: uuid::Uuid) -> Option<PathBuf> {
    if std::env::var_os("WATERTOWN_REMOTE_NO_CACHE").is_some() {
        return None;
    }
    let base = match std::env::var_os("WATERTOWN_CACHE_DIR") {
        Some(v) => PathBuf::from(v),
        None => {
            let home = std::env::var_os("HOME")?;
            PathBuf::from(home).join(".watertown").join("cache")
        }
    };
    let dir = base.join("remote").join(pond_id.to_string());
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

/// Load the `version_sizes.json` sidecar (if present) into a hash->size map.
/// Each line is `"<hex> <size>"`; malformed lines are skipped. A missing file
/// or read error yields an empty map (sizes are then resolved lazily).
fn load_version_sizes(cache_dir: Option<&std::path::Path>) -> HashMap<ObjectHash, u64> {
    let mut map = HashMap::new();
    let Some(dir) = cache_dir else {
        return map;
    };
    let Ok(text) = std::fs::read_to_string(dir.join(VERSION_SIZES_SIDECAR)) else {
        return map;
    };
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(hex), Some(size)) = (it.next(), it.next()) else {
            continue;
        };
        if let (Ok(hash), Ok(sz)) = (ObjectHash::from_hex(hex), size.parse::<u64>()) {
            let _ = map.insert(hash, sz);
        }
    }
    map
}
