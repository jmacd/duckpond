// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-graph push: send a pond's reachable object closure plus its tip
//! commit to a [`ContentRemote`] (design Section 8, Decisions D6 and D7).
//!
//! This is the producer side of the single delta-managed content-addressed
//! remote.  A push is one atomic Delta commit on the remote that writes the
//! new object rows and advances the tip ref together; this module assembles
//! the objects to send and the tip to point at.
//!
//! The objects are: the inline tree closure from
//! [`materialize_content_objects`], the node manifest that commit references,
//! plus the tip commit object reproduced verbatim from the persisted commit
//! spine.  Large blobs do not travel here; they transfer by hash through the
//! external `_large_files` path, so a pond that references any large blob is
//! rejected until that path is wired rather than pushed with a silently
//! incomplete closure.

use std::collections::BTreeSet;

use sync_store::ContentRemote;
use sync_store::content::ObjectHash;

use crate::content_tree::materialize_content_objects;
use crate::{Ship, StewardError};

/// The result of a successful content-graph push.
#[derive(Debug, Clone)]
pub struct ContentPushOutcome {
    /// The ref advanced on the remote.
    pub ref_name: String,
    /// The tip commit hash the ref now points at.
    pub tip: ObjectHash,
    /// Number of objects written to the remote in this push.
    pub objects_pushed: usize,
    /// The remote `txn_seq` allocated for this push.
    pub remote_txn_seq: i64,
}

/// Push the pond's current content closure and tip commit to `remote` under
/// `ref_name`.
///
/// The tip is the commit recorded at the pond's latest write seq.  Its encoded
/// object bytes are taken from the persisted commit spine and verified to hash
/// to the recorded commit hash before being sent, so the remote tip can never
/// disagree with the object it names.
///
/// The full inline closure is sent every time.  A re-put of an object the
/// remote already holds is idempotent, so this is correct though not minimal;
/// the local missing-set optimization against the last-pushed tip is a later
/// refinement.
///
/// # Errors
///
/// Returns an error if the pond has no commit spine at its latest write seq,
/// if the persisted commit object does not hash to the recorded commit hash,
/// if the pond references any large blob whose external transfer is not yet
/// wired, or if reading the content tree or writing to the remote fails.
pub async fn push_content_to_remote(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
) -> Result<ContentPushOutcome, StewardError> {
    let seq = ship.last_write_seq();

    let commit_hash_hex = ship
        .control_table()
        .commit_hash_at(seq)
        .await?
        .ok_or_else(|| {
            StewardError::Content(format!("no commit spine recorded at write seq {seq}"))
        })?;
    let commit_object_hex = ship
        .control_table()
        .commit_object_at(seq)
        .await?
        .ok_or_else(|| {
            StewardError::Content(format!("no commit object recorded at write seq {seq}"))
        })?;

    let tip = ObjectHash::from_hex(&commit_hash_hex)
        .map_err(|e| StewardError::Content(format!("invalid commit hash: {e}")))?;
    let commit_bytes = hex::decode(&commit_object_hex)
        .map_err(|e| StewardError::Content(format!("invalid commit object hex: {e}")))?;

    let recomputed = ObjectHash::of_bytes(&commit_bytes);
    if recomputed != tip {
        return Err(StewardError::Content(format!(
            "commit object hashes to {} but the spine records tip {}",
            recomputed.to_hex(),
            tip.to_hex()
        )));
    }

    let materialized = materialize_content_objects(ship).await?;

    if !materialized.external_blobs.is_empty() {
        let listed: BTreeSet<String> = materialized
            .external_blobs
            .iter()
            .map(ObjectHash::to_hex)
            .collect();
        return Err(StewardError::Content(format!(
            "pond references {} large blob(s) whose external transfer is not yet wired: {:?}",
            listed.len(),
            listed
        )));
    }

    let mut objects: Vec<(ObjectHash, Vec<u8>)> = Vec::with_capacity(materialized.inline.len() + 2);
    for (hash, bytes) in materialized.inline {
        objects.push((hash, bytes));
    }
    // The node manifest the commit references (Section 4.5); a consumer fetches
    // it to adopt the source's node_ids.  Verify it hashes to the commit's
    // recorded manifest hash so the tip can never name a manifest the remote
    // lacks or disagrees with.
    let (manifest_hash, manifest_bytes) = materialized.manifest.ok_or_else(|| {
        StewardError::Content("materialized objects carry no node manifest".to_string())
    })?;
    let commit = sync_store::content::Commit::decode(&commit_bytes)
        .map_err(|e| StewardError::Content(format!("decode commit object: {e}")))?;
    if commit.node_manifest_hash != manifest_hash {
        return Err(StewardError::Content(format!(
            "node manifest hashes to {} but the commit names {}",
            manifest_hash.to_hex(),
            commit.node_manifest_hash.to_hex()
        )));
    }
    objects.push((manifest_hash, manifest_bytes));
    objects.push((tip, commit_bytes));

    let remote_txn_seq = remote
        .push_commit(&objects, ref_name, tip)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?;

    Ok(ContentPushOutcome {
        ref_name: ref_name.to_string(),
        tip,
        objects_pushed: objects.len(),
        remote_txn_seq,
    })
}
