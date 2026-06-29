// SPDX-License-Identifier: Apache-2.0

//! [`ContentRemote`]: the delta-managed content-addressed remote.
//!
//! This is the single replication backend described in the design doc
//! Section 8 (Decision D6).  It replaces the bundle/frontier remote: there
//! is no `(pond_id, seq)` frontier, no per-bundle manifest, and no
//! per-partition checksum list.
//!
//! The remote is one Delta table (a [`Store`]) whose rows are content
//! objects keyed by their content hash, plus a distinguished ref row holding
//! the tip commit hash:
//!
//! ```text
//! partition = "objects", item_key = <hex object hash>, value = object bytes
//! partition = "refs",    item_key = <ref name>,        value = 32-byte tip hash
//! ```
//!
//! Because object `value` is the exact object bytes, the store's own
//! `value_blake3` column equals the object hash -- the storage key and the
//! integrity digest agree.
//!
//! **Atomicity comes from Delta, not from object ordering.**  A push is one
//! [`Store::apply_batch`] -- a single Delta commit -- that writes the new
//! object rows *and* advances the tip ref together.  The tip can therefore
//! never point at an incomplete object closure, with no "objects-before-ref"
//! two-phase write and no separate compare-and-swap: delta-rs over
//! `object_store` provides the commit atomicity the Delta protocol already
//! requires on S3.

use std::path::Path;

use uuid::Uuid;

use crate::content::ObjectHash;
use crate::error::{Result, StoreError};
use crate::store::{Op, Store};

/// Partition holding content objects, keyed by hex object hash.
const OBJECTS_PARTITION: &str = "objects";
/// Partition holding refs, keyed by ref name; value is the 32-byte tip hash.
const REFS_PARTITION: &str = "refs";
/// Partition holding remote metadata; the source pond_id is stored here under
/// the nil pond partition so a consumer can discover it without knowing it.
const META_PARTITION: &str = "meta";
const POND_ID_KEY: &str = "pond_id";

/// The delta-managed content-addressed remote for one source pond.
///
/// All rows are written under the source pond's `pond_id`, matching the
/// store's per-`pond_id` physical partitioning.  Object hashes are
/// content-only and lineage-independent, so two ponds with identical content
/// produce identical object bytes under identical keys.
pub struct ContentRemote {
    store: Store,
    pond_id: Uuid,
}

impl ContentRemote {
    /// Create a fresh remote at `path`.  Errors if a Delta table already
    /// exists there.
    pub async fn create_at(path: impl AsRef<Path>, pond_id: Uuid) -> Result<Self> {
        let store = Store::create(path).await?;
        let mut me = Self { store, pond_id };
        me.write_pond_id().await?;
        Ok(me)
    }

    /// Open an existing remote at `path`.
    pub async fn open_at(path: impl AsRef<Path>, pond_id: Uuid) -> Result<Self> {
        let store = Store::open(path).await?;
        Ok(Self { store, pond_id })
    }

    /// Create a fresh remote at `url` with `storage_options` (e.g. S3 creds),
    /// recording `pond_id`.  Errors if a table already exists.
    pub async fn create_at_url(
        url: &str,
        pond_id: Uuid,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let store = Store::create_at_url(url, storage_options).await?;
        let mut me = Self { store, pond_id };
        me.write_pond_id().await?;
        Ok(me)
    }

    /// Open an existing remote at `url`, discovering its source pond_id from
    /// the recorded metadata.
    pub async fn open_at_url(
        url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let store = Store::open_at_url(url, storage_options).await?;
        let bytes = store
            .get(Uuid::nil(), META_PARTITION, POND_ID_KEY)
            .await?
            .ok_or_else(|| StoreError::Invariant("remote has no recorded pond_id".to_string()))?;
        let s = String::from_utf8(bytes)
            .map_err(|e| StoreError::Invariant(format!("pond_id not utf8: {e}")))?;
        let pond_id =
            Uuid::parse_str(&s).map_err(|e| StoreError::Invariant(format!("bad pond_id: {e}")))?;
        Ok(Self { store, pond_id })
    }

    async fn write_pond_id(&mut self) -> Result<()> {
        let _ = self
            .store
            .put(
                Uuid::nil(),
                META_PARTITION,
                POND_ID_KEY,
                self.pond_id.to_string().into_bytes(),
            )
            .await?;
        Ok(())
    }

    /// The pond whose objects this remote holds.
    pub fn pond_id(&self) -> Uuid {
        self.pond_id
    }

    /// Push a commit: write `objects` and advance `ref_name` to `tip` in a
    /// single atomic Delta commit.  `objects` should be the closure the
    /// remote lacks (typically the producer's `missing_from` set); already
    /// present objects may be included harmlessly, since a re-put of an
    /// identical hash is idempotent.
    ///
    /// Returns the `txn_seq` allocated for the commit.
    pub async fn push_commit(
        &mut self,
        objects: &[(ObjectHash, Vec<u8>)],
        ref_name: &str,
        tip: ObjectHash,
    ) -> Result<i64> {
        let mut ops: Vec<Op> = Vec::with_capacity(objects.len() + 1);
        for (hash, bytes) in objects {
            ops.push(Op::Put {
                partition: OBJECTS_PARTITION.to_string(),
                key: hash.to_hex(),
                value: bytes.clone(),
            });
        }
        ops.push(Op::Put {
            partition: REFS_PARTITION.to_string(),
            key: ref_name.to_string(),
            value: tip.as_bytes().to_vec(),
        });

        let txn_seq = self.store.last_txn_seq(self.pond_id).await? + 1;
        let ts = chrono::Utc::now().timestamp_micros();
        self.store
            .apply_batch(self.pond_id, txn_seq, ts, ops)
            .await?;
        Ok(txn_seq)
    }

    /// Read the tip commit hash for `ref_name`, or `None` if the ref does not
    /// exist.
    pub async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>> {
        let Some(bytes) = self
            .store
            .get(self.pond_id, REFS_PARTITION, ref_name)
            .await?
        else {
            return Ok(None);
        };
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            StoreError::Invariant(format!(
                "ref '{}' value is {} bytes, expected 32",
                ref_name,
                bytes.len()
            ))
        })?;
        Ok(Some(ObjectHash::from_bytes(arr)))
    }

    /// Read the bytes of the object with the given hash, or `None` if absent.
    pub async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>> {
        self.store
            .get(self.pond_id, OBJECTS_PARTITION, &hash.to_hex())
            .await
    }

    /// True if the object with the given hash is present on the remote.
    pub async fn has_object(&self, hash: ObjectHash) -> Result<bool> {
        Ok(self.get_object(hash).await?.is_some())
    }

    /// Object-store key for an external blob, sibling to the Delta log.  Large
    /// blobs (>64KB) live here rather than as inline `objects` rows so a
    /// multi-gigabyte value never lands in the Delta table (Decision D7).
    fn blob_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("_blobs/blob={}", hash.to_hex()))
    }

    /// True if the external blob `hash` is already present in the remote blob
    /// store, so a producer can skip re-uploading it.
    pub async fn has_blob(&self, hash: ObjectHash) -> Result<bool> {
        match self.store.object_store().head(&Self::blob_path(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StoreError::Invariant(format!("blob head: {e}"))),
        }
    }

    /// Stream a large blob's raw bytes from `reader` into the remote blob store,
    /// keyed by `hash`.  Chunks flow through a bounded buffer to a multipart
    /// upload; the bytes are hashed as they pass so a value can never be stored
    /// under a key it does not equal.  Never collects the whole blob in memory.
    pub async fn put_blob<R>(&self, hash: ObjectHash, mut reader: R) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        use tokio::io::AsyncReadExt;
        let path = Self::blob_path(hash);
        let upload = self
            .store
            .object_store()
            .put_multipart(&path)
            .await
            .map_err(|e| StoreError::Invariant(format!("blob put_multipart: {e}")))?;
        let mut writer = object_store::WriteMultipart::new(upload);
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 8 * 1024 * 1024];
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|e| StoreError::Invariant(format!("blob read: {e}")))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            writer.write(&buf[..n]);
        }
        writer
            .finish()
            .await
            .map_err(|e| StoreError::Invariant(format!("blob finish: {e}")))?;
        let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
        if computed != hash {
            return Err(StoreError::Invariant(format!(
                "blob bytes hash to {} but were stored under {}",
                computed.to_hex(),
                hash.to_hex()
            )));
        }
        Ok(())
    }

    /// Fetch a large blob's raw bytes by hash, verifying integrity, or `None`
    /// if absent.  The body is streamed from object storage.
    pub async fn get_blob(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>> {
        let path = Self::blob_path(hash);
        let res = match self.store.object_store().get(&path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(StoreError::Invariant(format!("blob get: {e}"))),
        };
        let bytes = res
            .bytes()
            .await
            .map_err(|e| StoreError::Invariant(format!("blob body: {e}")))?;
        let computed = ObjectHash::of_bytes(&bytes);
        if computed != hash {
            return Err(StoreError::Invariant(format!(
                "blob bytes hash to {} but were fetched as {}",
                computed.to_hex(),
                hash.to_hex()
            )));
        }
        Ok(Some(bytes.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn url_remote_persists_and_discovers_pond_id() {
        let dir = tempdir().unwrap();
        let url = format!("file://{}/remote", dir.path().display());
        let pond = Uuid::new_v4();
        let _ = ContentRemote::create_at_url(&url, pond, Default::default())
            .await
            .unwrap();
        let opened = ContentRemote::open_at_url(&url, Default::default())
            .await
            .unwrap();
        assert_eq!(opened.pond_id(), pond);
    }
}
