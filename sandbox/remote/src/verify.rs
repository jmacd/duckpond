// SPDX-License-Identifier: Apache-2.0

//! `verify_against_remote`: compare a consumer steward's CURRENT
//! data against the remote's recorded `partition_checksums` at the
//! remote's latest bundle.  Walks back through manifest history to
//! find the divergence boundary if a mismatch is detected.

use std::collections::BTreeSet;

use sandbox_steward::{PartitionChecksums, Steward};
use sandbox_store::checksum::Checksum;

use crate::error::Result;
use crate::remote::Remote;

/// Result of [`verify_against_remote`].
#[derive(Debug, Clone)]
pub struct RemoteVerifyReport {
    /// `true` iff the consumer's CURRENT live partition checksums
    /// equal the remote's recorded checksums at `remote_latest_seq`
    /// (same set of partition keys AND byte-equal checksums per key).
    /// Vacuously `true` when both sides are empty.
    pub ok: bool,
    /// The remote's latest manifest seq at verification time, or
    /// `None` if the remote has no bundles.
    pub remote_latest_seq: Option<i64>,
    /// Per-partition mismatches found at `remote_latest_seq`.
    /// Empty when [`Self::ok`] is `true`.
    pub mismatches: Vec<RemoteVerifyMismatch>,
    /// If a mismatch was found at `remote_latest_seq`, the seq of
    /// the most recent prior bundle whose recorded checksums all
    /// match the consumer's current state.  This is the "divergence
    /// boundary": the consumer's data agreed with the source up to
    /// (and including) this seq, and diverged after.  `None` means
    /// either no walk was needed (`ok = true`) or no agreeing
    /// bundle was found (consumer never agreed in the walked range).
    pub divergence_boundary: Option<i64>,
}

/// One partition's mismatch found by [`verify_against_remote`].
#[derive(Debug, Clone)]
pub struct RemoteVerifyMismatch {
    /// Partition key.
    pub partition: String,
    /// What the remote recorded for this partition at
    /// `remote_latest_seq`.  `None` means the remote does not have
    /// a checksum row for this partition at the latest seq (consumer
    /// has it locally, source dropped it or never had it).
    pub remote_recorded: Option<Checksum>,
    /// What the consumer's current data hashes to.  `None` means the
    /// consumer's data store has no rows for this partition (remote
    /// has it, consumer doesn't).
    pub consumer_live: Option<Checksum>,
}

/// Compare `steward`'s CURRENT data for the pond owned by `remote`
/// (i.e., partitions in the consumer's local table whose pond_id
/// equals `remote.store_id()`) against the latest recorded checksums
/// on `remote`.
///
/// Supports both mirror and cross-pond import modes:
/// - **Mirror**: `remote.store_id == steward.store_id`.  Verifies the
///   consumer's own data matches its own remote.
/// - **Cross-pond import**: `remote.store_id != steward.store_id`.
///   Verifies the foreign pond's mirrored data on the consumer
///   matches the foreign pond's remote.
///
/// In both cases the partition set examined on the consumer side is
/// `consumer.compute_live_checksums(remote.store_id())`.
///
/// If the remote has no bundles, returns a report with
/// `remote_latest_seq = None` and `ok` true iff the consumer has no
/// data for `remote.store_id()` either (nothing to verify against).
///
/// If a mismatch is found at the remote's latest seq, walks back
/// through manifest history (descending seq) to identify the most
/// recent prior bundle whose recorded checksums match the consumer
/// exactly.  This is reported as `divergence_boundary` so the
/// operator can identify when drift began.
pub async fn verify_against_remote(
    remote: &Remote,
    steward: &Steward,
) -> Result<RemoteVerifyReport> {
    // Note: no equality check on store_ids.  Verify is scoped to the
    // remote's pond_id; the steward's own pond_id is irrelevant
    // (cross-pond consumer holds many pond_ids of data).

    let live: PartitionChecksums = steward.compute_live_checksums(remote.store_id()).await?;

    let bundles = remote.list_bundles().await?;
    let Some(latest) = bundles.iter().max_by_key(|b| b.txn_seq) else {
        return Ok(RemoteVerifyReport {
            ok: live.is_empty(),
            remote_latest_seq: None,
            mismatches: Vec::new(),
            divergence_boundary: None,
        });
    };
    let remote_latest_seq = latest.txn_seq;

    let recorded_at_latest = remote.read_checksums_for_bundle(remote_latest_seq).await?;
    let mismatches = compare(&live, &recorded_at_latest);
    let ok = mismatches.is_empty();

    let divergence_boundary = if ok {
        None
    } else {
        let mut sorted: Vec<&crate::remote::BundleHeader> = bundles.iter().collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.txn_seq));
        let mut found = None;
        for b in sorted {
            if b.txn_seq >= remote_latest_seq {
                continue;
            }
            let recorded = remote.read_checksums_for_bundle(b.txn_seq).await?;
            if compare(&live, &recorded).is_empty() {
                found = Some(b.txn_seq);
                break;
            }
        }
        found
    };

    Ok(RemoteVerifyReport {
        ok,
        remote_latest_seq: Some(remote_latest_seq),
        mismatches,
        divergence_boundary,
    })
}

fn compare(live: &PartitionChecksums, recorded: &PartitionChecksums) -> Vec<RemoteVerifyMismatch> {
    let mut all_keys: BTreeSet<&str> = BTreeSet::new();
    for k in live.keys() {
        let _ = all_keys.insert(k.as_str());
    }
    for k in recorded.keys() {
        let _ = all_keys.insert(k.as_str());
    }

    let mut out = Vec::new();
    for key in all_keys {
        let l = live.get(key);
        let r = recorded.get(key);
        match (l, r) {
            (Some(a), Some(b)) if a == b => {}
            _ => out.push(RemoteVerifyMismatch {
                partition: key.to_string(),
                remote_recorded: r.cloned(),
                consumer_live: l.cloned(),
            }),
        }
    }
    out
}
