// SPDX-License-Identifier: Apache-2.0

//! Tests for [`Steward::apply_pulled_bundle`] -- the steward-side
//! helper that lets `Remote::pull` apply pre-built parquet bytes to
//! the consumer's data store and mirror the source's DataCommitted
//! metadata on the consumer's control table.

use std::collections::HashMap;
use std::sync::Arc;

use sandbox_steward::control_table::ChecksumValue;
use sandbox_steward::{
    CommitKind, PartitionChecksums, RecordKind, Steward, StewardError, StewardOptions,
};
use sandbox_store::checksum::{Checksum, ChecksumKind, Merkle};
use tempfile::TempDir;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn opts() -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(Merkle::new()),
        ..Default::default()
    }
}

/// Build a synthetic add: a tiny parquet file emitted by another
/// steward that we can hand to apply_pulled_bundle.
async fn make_synthetic_add(partition: &str, key: &str, value: &[u8]) -> (String, Vec<u8>) {
    // The cleanest way to get a "parquet file the sandbox-store
    // accepts" is to run a real steward, write a single value,
    // grab the file from disk.  This makes the "add bytes" round-
    // trippable through Store::get later.
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(dir.path(), opts())
        .await
        .unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put(partition, key, value.to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    // The single-write commit produced exactly one parquet file.
    let version = s.data_delta_version_at(1).await.unwrap().unwrap();
    let (adds, _) = s.actions_at_version(version).await.unwrap();
    assert_eq!(adds.len(), 1, "synthetic write produces one add file");
    let bytes = std::fs::read(dir.path().join("data").join(&adds[0].path)).unwrap();
    (adds[0].path.clone(), bytes)
}

fn empty_checksums() -> PartitionChecksums {
    PartitionChecksums::new()
}

fn checksums_for(partition: &str) -> PartitionChecksums {
    let mut out = PartitionChecksums::new();
    let _ = out.insert(
        partition.to_string(),
        Checksum::new(ChecksumKind::Merkle, vec![0u8; 32]),
    );
    out
}

#[tokio::test]
async fn apply_pulled_bundle_writes_data_committed_record() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let (path, bytes) = make_synthetic_add("p1", "k", b"hello").await;
    let checksums = checksums_for("p1");
    consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![(path.clone(), bytes)],
            vec![],
            checksums.clone(),
        )
        .await
        .unwrap();

    // DataCommitted record present at txn_seq 1 with the right metadata.
    let dc = consumer.data_committed_record(1).await.unwrap().unwrap();
    assert_eq!(dc.record_kind, RecordKind::DataCommitted);
    assert_eq!(dc.commit_kind, Some(CommitKind::Write));
    assert_eq!(dc.parent_seq, None, "first bundle has no parent");

    // last_committed_seq advanced.
    assert_eq!(consumer.last_committed_seq().await.unwrap(), 1);

    // Consumer's data file is present on disk.
    let absolute = consumer_dir.path().join("data").join(&path);
    assert!(
        absolute.exists(),
        "synthetic parquet at {} was written",
        absolute.display()
    );

    // partition_checksums_at returns the recorded checksums.
    let recorded = consumer.partition_checksums_at(1).await.unwrap().unwrap();
    assert_eq!(recorded.get("p1"), checksums.get("p1"));
}

#[tokio::test]
async fn apply_pulled_bundle_is_idempotent_on_repeat() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let (path, bytes) = make_synthetic_add("p1", "k", b"v").await;
    let cs = checksums_for("p1");

    consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![(path.clone(), bytes.clone())],
            vec![],
            cs.clone(),
        )
        .await
        .unwrap();
    let first_log_len = consumer.log(None).await.unwrap().len();

    // Re-apply: should be a no-op.
    consumer
        .apply_pulled_bundle(1, CommitKind::Write, 0, vec![(path, bytes)], vec![], cs)
        .await
        .unwrap();
    let second_log_len = consumer.log(None).await.unwrap().len();

    assert_eq!(
        first_log_len, second_log_len,
        "no new control records after idempotent re-apply"
    );
    assert_eq!(consumer.last_committed_seq().await.unwrap(), 1);
}

#[tokio::test]
async fn apply_pulled_bundle_advances_last_write_seq_for_future_writes() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let (path, bytes) = make_synthetic_add("p1", "k", b"v").await;
    consumer
        .apply_pulled_bundle(
            5,
            CommitKind::Write,
            0,
            vec![(path, bytes)],
            vec![],
            checksums_for("p1"),
        )
        .await
        .unwrap();

    // last_write_seq is now at least 5.
    assert!(
        consumer.last_write_seq() >= 5,
        "last_write_seq advanced to >= 5, got {}",
        consumer.last_write_seq()
    );

    // A subsequent local write would allocate seq 6 (not 1).
    let g = consumer.begin_write().await.unwrap();
    assert_eq!(g.txn_seq(), 6);
    let _ = g.commit().await.unwrap();
}

#[tokio::test]
async fn apply_pulled_bundle_chains_parent_seq_across_multiple_bundles() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    for seq in 1i64..=3 {
        let (path, bytes) = make_synthetic_add("p", &format!("k{}", seq), b"v").await;
        let parent = if seq == 1 { 0 } else { seq - 1 };
        consumer
            .apply_pulled_bundle(
                seq,
                CommitKind::Write,
                parent,
                vec![(path, bytes)],
                vec![],
                checksums_for("p"),
            )
            .await
            .unwrap();
    }

    let log = consumer.log(None).await.unwrap();
    let dcs: Vec<_> = log
        .iter()
        .filter(|r| r.record_kind == RecordKind::DataCommitted)
        .collect();
    assert_eq!(dcs.len(), 3);
    assert_eq!(dcs[0].txn_seq, 1);
    assert_eq!(dcs[0].parent_seq, None);
    assert_eq!(dcs[1].txn_seq, 2);
    assert_eq!(dcs[1].parent_seq, Some(1));
    assert_eq!(dcs[2].txn_seq, 3);
    assert_eq!(dcs[2].parent_seq, Some(2));
    assert_eq!(consumer.last_committed_seq().await.unwrap(), 3);
}

#[tokio::test]
async fn apply_pulled_bundle_rejects_absolute_path() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let result = consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![("/etc/passwd".to_string(), vec![0u8; 4])],
            vec![],
            empty_checksums(),
        )
        .await;
    match result {
        Err(StewardError::Invariant(msg)) => assert!(msg.contains("absolute"), "msg: {}", msg),
        other => panic!("expected Invariant, got {:?}", other.err()),
    }
}

#[tokio::test]
async fn apply_pulled_bundle_rejects_dotdot_path() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let result = consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![(
                "partition_key=p/../escape.parquet".to_string(),
                vec![0u8; 4],
            )],
            vec![],
            empty_checksums(),
        )
        .await;
    match result {
        Err(StewardError::Invariant(msg)) => assert!(msg.contains(".."), "msg: {}", msg),
        other => panic!("expected Invariant, got {:?}", other.err()),
    }
}

#[tokio::test]
async fn apply_pulled_bundle_rejects_missing_partition_key_segment() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let result = consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![("part-001.parquet".to_string(), vec![0u8; 4])],
            vec![],
            empty_checksums(),
        )
        .await;
    match result {
        Err(StewardError::Invariant(msg)) => {
            assert!(msg.contains("partition_key"), "msg: {}", msg)
        }
        other => panic!("expected Invariant, got {:?}", other.err()),
    }
}

#[tokio::test]
async fn apply_pulled_bundle_records_partition_checksums_byte_exact() {
    init_logger();
    let consumer_dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(consumer_dir.path(), opts())
        .await
        .unwrap();

    let (path, bytes) = make_synthetic_add("p1", "k", b"v").await;
    let mut input_cs = PartitionChecksums::new();
    let _ = input_cs.insert(
        "p1".to_string(),
        Checksum::new(ChecksumKind::Merkle, (0..32u8).collect()),
    );
    let _ = input_cs.insert(
        "p2".to_string(),
        Checksum::new(ChecksumKind::Homomorphic, (32..64u8).collect()),
    );

    consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![(path, bytes)],
            vec![],
            input_cs.clone(),
        )
        .await
        .unwrap();

    let recorded = consumer.partition_checksums_at(1).await.unwrap().unwrap();
    assert_eq!(recorded.len(), input_cs.len());
    for (k, v) in &input_cs {
        let r = recorded.get(k).unwrap_or_else(|| panic!("missing {}", k));
        assert_eq!(r.kind, v.kind, "kind matches for {}", k);
        assert_eq!(r.bytes, v.bytes, "bytes match for {}", k);
        // Round-trip through ChecksumValue (the on-disk JSON form).
        let cv: ChecksumValue = ChecksumValue::from(v);
        let recovered = Checksum::from(&cv);
        assert_eq!(recovered.bytes, v.bytes);
    }
    let _ = HashMap::<String, ()>::new();
}

// ---- library-api-coverage gap-filling tests ----

#[tokio::test]
async fn apply_pulled_bundle_with_removes_drops_files_from_active_set() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(dir.path(), opts())
        .await
        .unwrap();

    // Bundle 1: Add a single file containing key "k".
    let (path, bytes) = make_synthetic_add("p", "k", b"v").await;
    consumer
        .apply_pulled_bundle(
            1,
            CommitKind::Write,
            0,
            vec![(path.clone(), bytes)],
            vec![],
            checksums_for("p"),
        )
        .await
        .unwrap();
    {
        let r = consumer.begin_read().await.unwrap();
        assert_eq!(r.get("p", "k").await.unwrap(), Some(b"v".to_vec()));
    }

    // Bundle 2: Compact-style remove (no Adds) to tombstone the file.
    consumer
        .apply_pulled_bundle(
            2,
            CommitKind::Compact,
            1,
            vec![],
            vec![path],
            checksums_for("p"),
        )
        .await
        .unwrap();

    // After remove, "k" is no longer accessible (no other file has
    // an active row for it).
    let r = consumer.begin_read().await.unwrap();
    assert_eq!(r.get("p", "k").await.unwrap(), None);
}

#[tokio::test]
async fn read_data_file_errors_on_missing_path() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let s = Steward::create(dir.path()).await.unwrap();
    let result = s.read_data_file("partition_key=nope/missing.parquet");
    assert!(result.is_err(), "missing file should error");
    match result {
        Err(StewardError::Io(_)) => {}
        other => panic!("expected Io error, got {:?}", other.err()),
    }
}
