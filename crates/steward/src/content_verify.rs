// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-graph verify: compare a pond's current content tip against what a
//! [`ContentRemote`] has published under a ref (design Section 8, Decision D6).
//!
//! The bundle/frontier `verify` compared per-partition checksums against a
//! remote's `(pond_id, seq)` bundle history.  The content-addressed remote has
//! no frontier: it holds one object closure and a single tip ref.  So verify
//! reduces to a commit-graph relationship between the local tip and the
//! remote's tip:
//!
//! * **UpToDate** -- the remote's tip equals the local tip; the published data
//!   is exactly the live data.
//! * **RemoteBehind** -- the remote's tip is an ancestor of the local tip; the
//!   producer has local commits it has not pushed yet (lag, not corruption).
//! * **RemoteEmpty** -- the remote holds no ref under this name.
//! * **Diverged** -- the remote's tip is not in the local commit history, so
//!   the remote published commits this pond does not have (a real mismatch).
//!
//! `ok` is true for UpToDate, RemoteBehind, and RemoteEmpty-with-no-local-data:
//! in each case the remote faithfully holds a prefix of this pond's history.

use sync_store::ContentRemote;
use sync_store::content::{Commit, ObjectHash};

use crate::{Ship, StewardError};

/// The relationship between the local content tip and the remote's published
/// tip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentVerifyState {
    /// Neither side has any commit (a fresh pond against an empty remote).
    BothEmpty,
    /// The remote holds no ref, but the local pond has commits to publish.
    RemoteEmpty,
    /// The remote tip equals the local tip: published == live.
    UpToDate,
    /// The remote tip is an ancestor of the local tip; the producer has
    /// `local_unpushed` commits not yet pushed.
    RemoteBehind { local_unpushed: i64 },
    /// The remote tip is not in the local commit history -- the remote holds
    /// commits this pond does not, so the two have diverged.
    Diverged,
}

/// The outcome of comparing a pond's content tip against a remote ref.
#[derive(Debug, Clone)]
pub struct ContentVerifyReport {
    /// True when the remote faithfully holds a prefix of this pond's history.
    pub ok: bool,
    /// The local pond's current tip commit hash, or `None` if it has no commits.
    pub local_tip: Option<ObjectHash>,
    /// The remote's published tip commit hash, or `None` if the ref is absent.
    pub remote_tip: Option<ObjectHash>,
    /// The seq the remote's tip commit records, if a tip is present.
    pub remote_seq: Option<i64>,
    /// The relationship between the two tips.
    pub state: ContentVerifyState,
}

/// Verify the pond's current content tip against `remote`'s `ref_name` tip.
///
/// This reads the local commit spine and the remote ref; it does not rewrite
/// anything.  It walks the local commit history (newest seq downward) to place
/// the remote tip, so the relationship is exact rather than a seq comparison.
///
/// # Errors
///
/// Returns an error if the local commit spine cannot be read, if the remote ref
/// cannot be read, or if the remote tip commit object is present but does not
/// decode.
pub async fn verify_content_against_remote(
    ship: &Ship,
    remote: &ContentRemote,
    ref_name: &str,
) -> Result<ContentVerifyReport, StewardError> {
    let last_write_seq = ship
        .control_table()
        .get_last_write_sequence()
        .await
        .map_err(|e| StewardError::Content(format!("read last write sequence: {e}")))?;
    let local_tip = if last_write_seq > 0 {
        ship.control_table()
            .commit_hash_at(last_write_seq)
            .await?
            .map(|hex| ObjectHash::from_hex(&hex))
            .transpose()
            .map_err(|e| StewardError::Content(format!("invalid local tip hash: {e}")))?
    } else {
        None
    };

    let remote_tip = remote
        .get_tip(ref_name)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?;

    // The remote tip's recorded seq (decoded from its commit object) is useful
    // operator context: it names the source transaction the publish reflects.
    let remote_seq = if let Some(rt) = remote_tip {
        match remote
            .get_object(rt)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))?
        {
            Some(bytes) => Some(
                Commit::decode(&bytes)
                    .map_err(|e| StewardError::Content(format!("decode remote tip commit: {e}")))?
                    .provenance
                    .seq,
            ),
            None => None,
        }
    } else {
        None
    };

    let state = match (local_tip, remote_tip) {
        (None, None) => ContentVerifyState::BothEmpty,
        (Some(_), None) => ContentVerifyState::RemoteEmpty,
        (None, Some(_)) => ContentVerifyState::Diverged,
        (Some(local), Some(remote_t)) => {
            if local == remote_t {
                ContentVerifyState::UpToDate
            } else if let Some(found_seq) =
                find_in_local_history(ship, remote_t, last_write_seq).await?
            {
                ContentVerifyState::RemoteBehind {
                    local_unpushed: last_write_seq - found_seq,
                }
            } else {
                ContentVerifyState::Diverged
            }
        }
    };

    let ok = matches!(
        state,
        ContentVerifyState::BothEmpty
            | ContentVerifyState::RemoteEmpty
            | ContentVerifyState::UpToDate
            | ContentVerifyState::RemoteBehind { .. }
    );

    Ok(ContentVerifyReport {
        ok,
        local_tip,
        remote_tip,
        remote_seq,
        state,
    })
}

/// Find the seq at which the local commit spine recorded `target`, scanning
/// from `last_write_seq` downward.  Returns `None` if no local commit matches,
/// which means `target` is not in this pond's history.
async fn find_in_local_history(
    ship: &Ship,
    target: ObjectHash,
    last_write_seq: i64,
) -> Result<Option<i64>, StewardError> {
    let mut seq = last_write_seq;
    while seq > 0 {
        if let Some(hex) = ship.control_table().commit_hash_at(seq).await? {
            let hash = ObjectHash::from_hex(&hex)
                .map_err(|e| StewardError::Content(format!("invalid commit hash at {seq}: {e}")))?;
            if hash == target {
                return Ok(Some(seq));
            }
        }
        seq -= 1;
    }
    Ok(None)
}
