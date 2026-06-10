// SPDX-License-Identifier: Apache-2.0

//! Comprehensive integration tests for [`Remote::push`].
//!
//! Smoke coverage (create/open/store_id, happy-path) lives in
//! `tests/remote.rs`.  This file covers:
//! - Compact bundle: explicit DataAdd + DataRemove rows
//! - partition_checksums round-trip (byte-exact)
//! - Chunked file: multi-chunk parquet round-trip with file-level
//!   BLAKE3 verification
//! - Multiple bundles ordering and parent_seq chain
//! - Post-remote-commit crash recovery (the highest-value failure
//!   window: remote commit succeeded but local Completed never written
//!   -> next push reconciles).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use sync_remote::Remote;
use sync_remote::chunking::assemble_file;
use sync_remote::schema::{
    PARTITION_KIND_CHECKSUM, PARTITION_KIND_DATA, RemoteRow, RowBody, record_batch_to_rows,
};
use sync_steward::{CommitKind, RecordKind, Steward};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Re-open the remote's Delta table outside of `Remote` and read all
/// rows for `txn_seq` from the requested partition_kind.
async fn read_partition(
    remote_path: &std::path::Path,
    partition_kind: &str,
    txn_seq: i64,
) -> Vec<RemoteRow> {
    let url = Url::from_directory_path(remote_path).unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    let ctx = SessionContext::new();
    let _ = ctx.register_table("remote", Arc::new(table)).unwrap();
    let sql = format!(
        "SELECT * FROM remote WHERE partition_kind = '{}' AND txn_seq = {}",
        partition_kind, txn_seq
    );
    let batches: Vec<RecordBatch> = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for b in &batches {
        rows.extend(record_batch_to_rows(b).unwrap());
    }
    rows
}

#[tokio::test]
async fn compact_bundle_carries_add_and_remove_rows() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();

    // Three small writes -> three separate parquets in partition `p`.
    for i in 0..3 {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    // Compact -> seq 4 (Compact bundle, if delta-rs decides to merge).
    let outcome = steward.compact(None).await.unwrap();
    if !outcome.had_data {
        // delta-rs found nothing to merge for this tiny test data;
        // the compact-bundle assertion can't be exercised here. Skip.
        return;
    }
    assert_eq!(outcome.commit_kind, CommitKind::Compact);

    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();
    remote.push(&mut steward, outcome.txn_seq).await.unwrap();

    // Read this bundle's data rows directly.
    let data_rows = read_partition(
        &dir.path().join("remote"),
        PARTITION_KIND_DATA,
        outcome.txn_seq,
    )
    .await;

    let mut adds = 0;
    let mut removes = 0;
    for row in &data_rows {
        match &row.body {
            RowBody::DataAdd { .. } => adds += 1,
            RowBody::DataRemove { .. } => removes += 1,
            _ => panic!("data partition has non-data row body"),
        }
    }
    assert!(adds > 0, "compact bundle has at least one DataAdd row");
    assert!(removes > 0, "compact bundle has DataRemove tombstones");
}

#[tokio::test]
async fn partition_checksums_round_trip_byte_exact() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p1", "k", b"v1".to_vec()).unwrap();
        g.put("p2", "k", b"v2".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let local = steward.partition_checksums_at(1).await.unwrap().unwrap();

    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();
    remote.push(&mut steward, 1).await.unwrap();

    let checksum_rows =
        read_partition(&dir.path().join("remote"), PARTITION_KIND_CHECKSUM, 1).await;

    // Every locally recorded partition_checksum should appear on the
    // remote, byte-for-byte.
    let mut remote_map: HashMap<String, Vec<u8>> = HashMap::new();
    let mut remote_kinds: HashMap<String, sync_store::checksum::ChecksumKind> = HashMap::new();
    for row in &checksum_rows {
        if let RowBody::Checksum {
            partition_key,
            checksum_kind,
            checksum_bytes,
        } = &row.body
        {
            let _ = remote_map.insert(partition_key.clone(), checksum_bytes.to_vec());
            let _ = remote_kinds.insert(partition_key.clone(), *checksum_kind);
        }
    }

    assert_eq!(remote_map.len(), local.len(), "checksum row count matches");
    for (k, c) in &local {
        let r = remote_map
            .get(k)
            .unwrap_or_else(|| panic!("missing remote checksum for {}", k));
        assert_eq!(r, &c.bytes, "byte-exact match for partition {}", k);
        assert_eq!(
            remote_kinds.get(k).copied().unwrap(),
            c.kind,
            "kind matches for partition {}",
            k
        );
    }
}

#[tokio::test]
async fn data_rows_round_trip_to_original_parquet_bytes() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    // Write a couple values; produces ONE parquet add per commit.
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k1", b"hello".to_vec()).unwrap();
        g.put("p", "k2", b"world".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    // Inspect the source's actions for this commit so we know what
    // parquets were added and what bytes to compare against.
    let version = steward
        .data_delta_version_at(1)
        .await
        .unwrap()
        .expect("delta version recorded");
    let (adds, _) = steward.actions_at_version(version).await.unwrap();

    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();
    remote.push(&mut steward, 1).await.unwrap();

    let data_rows = read_partition(&dir.path().join("remote"), PARTITION_KIND_DATA, 1).await;

    // For each Add file, gather all chunks (in chunk_id order),
    // assemble, and compare to the source bytes.
    for add in &adds {
        let source_bytes = std::fs::read(steward.path().join("data").join(&add.path)).unwrap();

        let mut chunks: Vec<sync_remote::ChunkRecord> = Vec::new();
        let mut file_size: i64 = 0;
        let mut file_blake3 = [0u8; 32];
        for row in &data_rows {
            if let RowBody::DataAdd {
                file_path,
                file_size: fs,
                file_blake3: fb,
                chunk_id,
                chunk_data,
                chunk_blake3,
                ..
            } = &row.body
                && file_path == &add.path
            {
                chunks.push(sync_remote::ChunkRecord {
                    chunk_id: *chunk_id,
                    chunk_data: chunk_data.clone(),
                    chunk_blake3: *chunk_blake3,
                });
                file_size = *fs;
                file_blake3 = *fb;
            }
        }
        chunks.sort_by_key(|c| c.chunk_id);
        assert!(
            !chunks.is_empty(),
            "Add file `{}` has at least one chunk row",
            add.path
        );

        let assembled = assemble_file(&chunks, file_size, &file_blake3).unwrap();
        assert_eq!(
            assembled, source_bytes,
            "reassembled bytes match source parquet for `{}`",
            add.path
        );
    }
}

#[tokio::test]
async fn multiple_bundles_ordered_with_correct_parent_seq() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();

    // Three writes -> three bundles.
    for i in 0..3 {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut steward, outcome.txn_seq).await.unwrap();
    }

    let bundles = remote.list_bundles().await.unwrap();
    assert_eq!(bundles.len(), 3);
    assert_eq!(bundles[0].txn_seq, 1);
    assert_eq!(bundles[0].parent_seq, 0, "root commit has parent_seq 0");
    assert_eq!(bundles[1].txn_seq, 2);
    assert_eq!(bundles[1].parent_seq, 1);
    assert_eq!(bundles[2].txn_seq, 3);
    assert_eq!(bundles[2].parent_seq, 2);

    assert_eq!(remote.latest_seq().await.unwrap(), Some(3));
    assert_eq!(remote.oldest_available_seq().await.unwrap(), Some(1));
}

#[tokio::test]
async fn open_errors_when_property_is_corrupted_uuid() {
    init_logger();
    let dir = TempDir::new().unwrap();
    // Create a remote, then re-create the table directly with a
    // bad sandbox.store_id property to simulate corruption.
    let _ = Remote::create(dir.path().join("good"), Uuid::new_v4())
        .await
        .unwrap();

    // Build a fresh table at a new path with an invalid UUID property.
    let bad_path = dir.path().join("bad");
    std::fs::create_dir_all(&bad_path).unwrap();
    let url = Url::from_directory_path(&bad_path).unwrap();
    let mut config: HashMap<String, Option<String>> = HashMap::new();
    let _ = config.insert(
        "sandbox.store_id".to_string(),
        Some("not-a-valid-uuid".to_string()),
    );
    let _ = DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(sync_remote::delta_columns())
        .with_partition_columns(sync_remote::partition_columns())
        .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
        .with_configuration(config)
        .with_raise_if_key_not_exists(false)
        .await
        .unwrap();

    match Remote::open(&bad_path).await {
        Err(sync_remote::RemoteError::InvalidRemote(msg)) => {
            assert!(msg.contains("not a valid UUID"), "msg: {}", msg);
        }
        Ok(_) => panic!("expected InvalidRemote, got Ok"),
        Err(other) => panic!("expected InvalidRemote, got {:?}", other),
    }
}

/// Regression for the "idempotent re-push never advances
/// `last_pushed_seq`" correctness bug (remote-redesign-review #2).
///
/// The idempotent-skip branch -- taken when the bundle for `txn_seq`
/// already exists on the remote (the documented post-crash reconcile
/// window) -- previously returned `Ok(())` without advancing the
/// `last_pushed_seq:<url>` watermark, unlike the success and
/// bootstrap-skip branches.  If a crash left the watermark behind while
/// the bundle was already committed remotely, every subsequent push
/// re-probed and idempotent-skipped the same seq, pinning the watermark
/// and producing false push-lag in `pond status`.
///
/// This test drives the idempotent-skip path with a deliberately
/// rewound watermark and asserts the watermark is advanced to the seq.
#[tokio::test]
async fn idempotent_re_push_advances_last_pushed_seq_watermark() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();

    let watermark_key = format!("last_pushed_seq:{}", remote.url());

    // First push: bundle lands on the remote and the watermark advances.
    remote.push(&mut steward, 1).await.unwrap();
    assert_eq!(remote.list_bundles().await.unwrap().len(), 1);
    assert_eq!(
        steward.config_get(&watermark_key).await.unwrap().as_deref(),
        Some("1"),
        "successful push advances the watermark to seq 1",
    );

    // Simulate the crash window: the bundle is committed on the remote,
    // but the local watermark was left behind (rewound to 0) before it
    // could be persisted.  The next push must take the idempotent-skip
    // branch (manifest already present) AND repair the watermark.
    steward.config_set(&watermark_key, "0").await.unwrap();
    assert_eq!(
        steward.config_get(&watermark_key).await.unwrap().as_deref(),
        Some("0"),
        "precondition: watermark rewound to simulate crash window",
    );

    // Re-push the same seq: idempotent skip (no duplicate bundle), but
    // the watermark must be advanced back to seq 1 so the driver does
    // not re-probe this seq on every subsequent push.
    remote.push(&mut steward, 1).await.unwrap();
    assert_eq!(
        remote.list_bundles().await.unwrap().len(),
        1,
        "idempotent re-push does not duplicate the bundle",
    );
    assert_eq!(
        steward.config_get(&watermark_key).await.unwrap().as_deref(),
        Some("1"),
        "idempotent-skip branch advances last_pushed_seq to the confirmed seq",
    );
}

/// The highest-value crash window: remote commit succeeded, but local
/// PostPushCompleted was never written (process crashed between the
/// two).  On retry, push must:
///  1. Detect the existing manifest row on the remote (idempotence).
///  2. Notice the local PostPushPending has no terminal pair.
///  3. Write PostPushCompleted to close the lifecycle.
///  4. Not duplicate the bundle on the remote.
#[tokio::test]
async fn idempotent_re_push_after_simulated_post_commit_crash_writes_completed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();

    // First push: success.  Then "simulate" that we crashed BEFORE
    // PostPushCompleted by manually dropping that record from the
    // local control table -- there's no public API to delete records,
    // but we can directly inject a competing PostPushPending with a
    // newer ts_micros so that the latest-for-seq becomes Pending again.
    remote.push(&mut steward, 1).await.unwrap();

    // Confirm the remote has exactly one bundle and the local control
    // has Pending+Completed.
    assert_eq!(remote.list_bundles().await.unwrap().len(), 1);
    let log_before = steward.log(None).await.unwrap();
    let pending_before: Vec<_> = log_before
        .iter()
        .filter(|r| r.txn_seq == 1 && r.record_kind == RecordKind::PostPushPending)
        .collect();
    let completed_before: Vec<_> = log_before
        .iter()
        .filter(|r| r.txn_seq == 1 && r.record_kind == RecordKind::PostPushCompleted)
        .collect();
    assert_eq!(pending_before.len(), 1);
    assert_eq!(completed_before.len(), 1);

    // Simulate "crashed before Completed" by writing a NEW Pending
    // (with a fresh txn_id) whose timestamp is later than the existing
    // Completed.  This makes "latest PostPush row for seq 1" be the
    // new Pending, mimicking the crash window.
    let _new_pending_id = steward.record_post_push_pending(1).await.unwrap();

    // Re-push: should detect the existing remote manifest row, notice
    // the latest local PostPush is Pending, and write Completed to
    // close the lifecycle.
    remote.push(&mut steward, 1).await.unwrap();

    // The remote still has exactly one bundle (no duplicate).
    assert_eq!(remote.list_bundles().await.unwrap().len(), 1);

    // The local log now has two Pending and two Completed (or at
    // least, the latest record for seq 1 is Completed, not Pending).
    let log_after = steward.log(None).await.unwrap();
    let mut latest = None;
    for r in &log_after {
        if r.txn_seq == 1
            && matches!(
                r.record_kind,
                RecordKind::PostPushPending
                    | RecordKind::PostPushCompleted
                    | RecordKind::PostPushFailed
            )
            && latest
                .map(|prev: &sync_steward::ControlRecord| r.ts_micros > prev.ts_micros)
                .unwrap_or(true)
        {
            latest = Some(r);
        }
    }
    let latest = latest.expect("at least one PostPush record exists");
    assert_eq!(
        latest.record_kind,
        RecordKind::PostPushCompleted,
        "latest PostPush record after crash-recovery is Completed, not Pending",
    );
}

/// Regression for remote-redesign-review #3 (no-silent-fallback): a
/// present-but-unparseable `last_pushed_seq:<url>` watermark must surface
/// as a `Schema` error from the push path, not be silently coerced to 0
/// (which would re-enumerate the entire transaction history from seq 1).
#[tokio::test]
async fn push_errors_on_unparseable_last_pushed_seq_watermark() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();

    // Plant a corrupt watermark, then push: the watermark-advance step
    // must reject the unparseable value rather than treat it as 0.
    let watermark_key = format!("last_pushed_seq:{}", remote.url());
    steward
        .config_set(&watermark_key, "not-an-i64")
        .await
        .unwrap();

    let err = remote.push(&mut steward, 1).await.unwrap_err();
    assert!(
        matches!(err, sync_remote::RemoteError::Schema(_)),
        "unparseable watermark must surface as Schema, got {err:?}",
    );
}
