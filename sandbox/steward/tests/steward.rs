// SPDX-License-Identifier: Apache-2.0

//! Tests for the [`Steward`] transaction lifecycle.

use std::sync::Arc;

use sandbox_steward::{
    CommitKind, RecordKind, Steward, StewardError, StewardOptions, verify_local,
};
use tempfile::TempDir;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn opts_merkle() -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(sandbox_store::checksum::Merkle::new()),
        ..Default::default()
    }
}

fn opts_homomorphic() -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(sandbox_store::checksum::Homomorphic::new()),
        ..Default::default()
    }
}

#[tokio::test]
async fn create_then_open_loads_last_write_seq() {
    init_logger();
    let dir = TempDir::new().unwrap();
    {
        let mut s = Steward::create(dir.path()).await.unwrap();
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
        assert_eq!(s.last_write_seq(), 1);
    }
    let s = Steward::open(dir.path()).await.unwrap();
    assert_eq!(s.last_write_seq(), 1);
    assert_eq!(s.last_committed_seq().await.unwrap(), 1);
}

#[tokio::test]
async fn begin_write_emits_begin_record_and_advances_seq() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    assert_eq!(s.last_write_seq(), 0);
    let g = s.begin_write().await.unwrap();
    let allocated = g.txn_seq();
    let _ = g.abort("not really").await.unwrap();
    assert_eq!(allocated, 1);
    assert_eq!(s.last_write_seq(), 1, "seq advances even on abort");

    let log = s.log(None).await.unwrap();
    let begins: Vec<_> = log
        .iter()
        .filter(|r| r.record_kind == RecordKind::Begin)
        .collect();
    assert_eq!(begins.len(), 1);
    assert_eq!(begins[0].txn_seq, 1);
}

#[tokio::test]
async fn commit_emits_data_committed_record_with_partition_checksums() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(dir.path(), opts_merkle())
        .await
        .unwrap();

    let mut g = s.begin_write().await.unwrap();
    g.put("p1", "a", b"A".to_vec()).unwrap();
    g.put("p2", "x", b"X".to_vec()).unwrap();
    let outcome = g.commit().await.unwrap();
    assert_eq!(outcome.txn_seq, 1);
    assert!(outcome.had_data);
    assert_eq!(outcome.commit_kind, CommitKind::Write);
    assert_eq!(outcome.partition_checksums.len(), 2);

    let recs = s.log(None).await.unwrap();
    let committed: Vec<_> = recs
        .iter()
        .filter(|r| r.record_kind == RecordKind::DataCommitted)
        .collect();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].txn_seq, 1);
    assert_eq!(committed[0].commit_kind, Some(CommitKind::Write));
    assert!(
        committed[0].parent_seq.is_none(),
        "first commit has no parent"
    );

    let snap = s.partition_checksums_at(1).await.unwrap().unwrap();
    assert_eq!(snap.len(), 2);
    assert_eq!(snap.get("p1"), outcome.partition_checksums.get("p1"));
}

#[tokio::test]
async fn commit_records_parent_seq_for_subsequent_commits() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    let mut g1 = s.begin_write().await.unwrap();
    g1.put("p", "a", b"v".to_vec()).unwrap();
    let _ = g1.commit().await.unwrap();

    let mut g2 = s.begin_write().await.unwrap();
    g2.put("p", "b", b"v".to_vec()).unwrap();
    let _ = g2.commit().await.unwrap();

    let recs = s.log(None).await.unwrap();
    let committed: Vec<_> = recs
        .iter()
        .filter(|r| r.record_kind == RecordKind::DataCommitted)
        .collect();
    assert_eq!(committed.len(), 2);
    assert!(committed[0].parent_seq.is_none());
    assert_eq!(committed[1].parent_seq, Some(1));
}

#[tokio::test]
async fn empty_commit_records_completed_not_data_committed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    let g = s.begin_write().await.unwrap();
    let outcome = g.commit().await.unwrap();
    assert!(!outcome.had_data);
    assert_eq!(outcome.txn_seq, 1);
    assert_eq!(s.last_committed_seq().await.unwrap(), 0);

    let recs = s.log(None).await.unwrap();
    assert!(
        recs.iter()
            .any(|r| r.record_kind == RecordKind::Completed && r.txn_seq == 1)
    );
    assert!(
        !recs
            .iter()
            .any(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == 1)
    );
}

#[tokio::test]
async fn abort_records_failed_record() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    let mut g = s.begin_write().await.unwrap();
    g.put("p", "k", b"unwritten".to_vec()).unwrap();
    let _ = g.abort("intentional rollback").await.unwrap();

    let recs = s.log(None).await.unwrap();
    let failed: Vec<_> = recs
        .iter()
        .filter(|r| r.record_kind == RecordKind::Failed)
        .collect();
    assert_eq!(failed.len(), 1);
    assert!(failed[0].metadata_json.contains("intentional rollback"));

    assert_eq!(s.last_write_seq(), 1);
    assert_eq!(s.last_committed_seq().await.unwrap(), 0);
}

#[tokio::test]
async fn read_guards_emit_no_control_records() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let before = s.log(None).await.unwrap().len();

    let r = s.begin_read().await.unwrap();
    let v = r.get("p", "k").await.unwrap();
    assert_eq!(v, Some(b"v".to_vec()));
    let listed = r.list("p").await.unwrap();
    assert_eq!(listed.len(), 1);
    drop(r);

    let after = s.log(None).await.unwrap().len();
    assert_eq!(before, after, "read guard must not write control records");
}

#[tokio::test]
async fn txn_seq_is_strictly_monotonic_across_commits_aborts_and_noops() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "a", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "b", b"v".to_vec()).unwrap();
        let _ = g.abort("nope").await.unwrap();
    }
    {
        let g = s.begin_write().await.unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "c", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    assert_eq!(s.last_write_seq(), 4);
    assert_eq!(s.last_committed_seq().await.unwrap(), 4);
}

#[tokio::test]
async fn partition_checksums_carry_forward_unaffected_partitions() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p1", "a", b"A".to_vec()).unwrap();
        g.put("p2", "x", b"X".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let snap1 = s.partition_checksums_at(1).await.unwrap().unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p1", "b", b"B".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let snap2 = s.partition_checksums_at(2).await.unwrap().unwrap();
    assert_eq!(
        snap1.get("p2"),
        snap2.get("p2"),
        "p2 unchanged commit-over-commit; checksum must match"
    );
    assert_ne!(
        snap1.get("p1"),
        snap2.get("p1"),
        "p1 changed; checksum must differ"
    );
}

#[tokio::test]
async fn dropping_guard_without_commit_or_abort_leaves_incomplete_record() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
    }
    let incomplete = s.incomplete_transactions().await.unwrap();
    assert_eq!(incomplete.len(), 1);
    assert_eq!(incomplete[0].txn_seq, 1);
}

#[tokio::test]
async fn open_after_orphaned_begin_advances_seq_past_orphan() {
    init_logger();
    let dir = TempDir::new().unwrap();
    {
        let mut s = Steward::create(dir.path()).await.unwrap();
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        drop(g);
    }
    let mut s = Steward::open(dir.path()).await.unwrap();
    assert_eq!(s.last_write_seq(), 1);
    let mut g = s.begin_write().await.unwrap();
    g.put("p", "k", b"v".to_vec()).unwrap();
    let outcome = g.commit().await.unwrap();
    assert_eq!(outcome.txn_seq, 2, "next allocation skips orphan");
}

#[tokio::test]
async fn config_set_get_list() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    s.config_set("auto_compact", "true").await.unwrap();
    s.config_set("retention", "5").await.unwrap();
    s.config_set("auto_compact", "false").await.unwrap();

    assert_eq!(
        s.config_get("auto_compact").await.unwrap(),
        Some("false".to_string()),
        "latest write wins"
    );
    assert_eq!(
        s.config_get("retention").await.unwrap(),
        Some("5".to_string())
    );
    assert_eq!(s.config_get("missing").await.unwrap(), None);

    let all = s.config_list().await.unwrap();
    assert_eq!(all.get("auto_compact"), Some(&"false".to_string()));
    assert_eq!(all.get("retention"), Some(&"5".to_string()));
    // `store_id` is now stored under a well-known bootstrap pond_id
    // (Uuid::nil()) so it's readable BEFORE the local pond_id is known,
    // and does NOT appear in the local pond's config_list.
    assert!(
        !all.contains_key("store_id"),
        "store_id setting is bootstrap-scoped, not local-pond-scoped"
    );
}

#[tokio::test]
async fn config_persists_across_reopen() {
    init_logger();
    let dir = TempDir::new().unwrap();
    {
        let mut s = Steward::create(dir.path()).await.unwrap();
        s.config_set("k", "v").await.unwrap();
    }
    let s = Steward::open(dir.path()).await.unwrap();
    assert_eq!(s.config_get("k").await.unwrap(), Some("v".to_string()));
}

#[tokio::test]
async fn log_with_limit_returns_most_recent() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    for i in 0..5 {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let last_three = s.log(Some(3)).await.unwrap();
    assert_eq!(last_three.len(), 3);
    let txn_seqs: Vec<_> = last_three.iter().map(|r| r.txn_seq).collect();
    assert!(txn_seqs.iter().all(|&s| (1..=5).contains(&s)));
}

#[tokio::test]
async fn verify_local_passes_on_clean_state() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    let mut g = s.begin_write().await.unwrap();
    g.put("p", "a", b"A".to_vec()).unwrap();
    g.put("q", "x", b"X".to_vec()).unwrap();
    let _ = g.commit().await.unwrap();

    let report = verify_local(&s).await.unwrap();
    assert!(report.ok, "verify failed: {:?}", report.mismatches);
    assert_eq!(report.recomputed_seq, 1);
}

#[tokio::test]
async fn verify_works_under_homomorphic_strategy() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(dir.path(), opts_homomorphic())
        .await
        .unwrap();
    let mut g = s.begin_write().await.unwrap();
    g.put("p", "a", b"A".to_vec()).unwrap();
    let _ = g.commit().await.unwrap();
    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}

#[tokio::test]
async fn verify_on_empty_pond_passes_trivially() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let s = Steward::create(dir.path()).await.unwrap();
    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
    assert_eq!(report.recomputed_seq, 0);
}

#[tokio::test]
async fn put_after_delete_then_verify() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v1".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = s.begin_write().await.unwrap();
        g.delete("p", "k").unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v2".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let r = s.begin_read().await.unwrap();
    assert_eq!(r.get("p", "k").await.unwrap(), Some(b"v2".to_vec()));
    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}

#[tokio::test]
async fn store_id_minted_on_create() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let s = Steward::create(dir.path()).await.unwrap();
    let id = s.store_id();
    assert_ne!(id, uuid::Uuid::nil(), "store_id is non-nil");
    assert_eq!(id.get_version_num(), 4, "store_id is UUIDv4");
}

#[tokio::test]
async fn store_id_persists_across_reopen() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let id_at_create = {
        let s = Steward::create(dir.path()).await.unwrap();
        s.store_id()
    };
    let s = Steward::open(dir.path()).await.unwrap();
    assert_eq!(s.store_id(), id_at_create, "store_id stable across reopen");
}

#[tokio::test]
async fn store_id_override_via_options() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let chosen = uuid::Uuid::new_v4();
    let opts = StewardOptions {
        store_id: Some(chosen),
        ..Default::default()
    };
    let s = Steward::create_with_options(dir.path(), opts)
        .await
        .unwrap();
    assert_eq!(s.store_id(), chosen);
    drop(s);
    // And it persists.
    let s2 = Steward::open(dir.path()).await.unwrap();
    assert_eq!(s2.store_id(), chosen);
}

#[tokio::test]
async fn legacy_pond_without_store_id_errors_on_open() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let pond = dir.path();
    // Construct a "legacy" pond by creating the underlying Store and
    // ControlTable directly, bypassing Steward::create's `store_id`
    // minting.  This simulates a pond produced before the identity
    // invariant existed.
    std::fs::create_dir_all(pond).unwrap();
    let _ = sandbox_store::Store::create(&pond.join("data"))
        .await
        .unwrap();
    let _ = sandbox_steward::control_table::ControlTable::create(&pond.join("control"))
        .await
        .unwrap();

    match Steward::open(pond).await {
        Err(StewardError::LegacyPond(msg)) => {
            assert!(msg.contains("store_id"), "error mentions the missing key");
        }
        other => panic!("expected LegacyPond error, got {:?}", other.err()),
    }
}

#[tokio::test]
async fn data_delta_version_recorded_on_write() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let v = s.data_delta_version_at(1).await.unwrap();
    assert!(v.is_some(), "DataCommitted has a recorded delta version");
    assert!(v.unwrap() > 0, "delta version advanced past initial");
}

#[tokio::test]
async fn data_delta_version_not_recorded_for_failed_or_completed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    // Aborted txn -> Failed -> no version.
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.abort("nope").await.unwrap();
    }
    assert_eq!(s.data_delta_version_at(1).await.unwrap(), None);

    // No-op write -> Completed -> no version.
    {
        let g = s.begin_write().await.unwrap();
        let _ = g.commit().await.unwrap();
    }
    assert_eq!(s.data_delta_version_at(2).await.unwrap(), None);
}

#[tokio::test]
async fn data_delta_version_monotonic_across_writes() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    let mut last_version = 0i64;
    for i in 0..3 {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        let v = s
            .data_delta_version_at(outcome.txn_seq)
            .await
            .unwrap()
            .unwrap();
        assert!(
            v > last_version,
            "version {} > last {} at seq {}",
            v,
            last_version,
            outcome.txn_seq
        );
        last_version = v;
    }
}

#[tokio::test]
async fn data_delta_version_recorded_on_real_compact() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    // Two writes to give compact something to merge.
    for i in 0..2 {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let pre = s.data_delta_version_at(2).await.unwrap().unwrap();

    let outcome = s.compact(None).await.unwrap();
    if outcome.had_data {
        let post = s.data_delta_version_at(outcome.txn_seq).await.unwrap();
        assert!(post.is_some(), "real-work compact records a version");
        assert!(
            post.unwrap() > pre,
            "compact's version is greater than prior write's version"
        );
    }
    // No-op compact path is exercised separately in tests/compact.rs.
}

#[tokio::test]
async fn data_delta_version_not_recorded_for_noop_compact() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    // No writes => nothing to compact => no-op => Completed (not DataCommitted).
    let outcome = s.compact(None).await.unwrap();
    assert!(!outcome.had_data, "empty pond compact is a no-op");
    assert_eq!(
        s.data_delta_version_at(outcome.txn_seq).await.unwrap(),
        None
    );
}

// ---- library-api-coverage gap-filling tests ----

#[tokio::test]
async fn verify_local_detects_data_drift() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let pond_id = {
        let mut s = Steward::create(dir.path()).await.unwrap();
        let pond_id = s.store_id();
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k", b"v1".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
        pond_id
    };
    // Tamper with the underlying data store directly, bypassing
    // the steward's lifecycle.  The steward's recorded checksums
    // at txn_seq=1 capture the pre-tamper state.
    {
        let mut store = sandbox_store::Store::open(dir.path().join("data"))
            .await
            .unwrap();
        let next_seq = store.last_txn_seq(pond_id).await.unwrap() + 1;
        let now = chrono::Utc::now().timestamp_micros();
        store
            .apply_batch(
                pond_id,
                next_seq,
                now,
                vec![sandbox_store::Op::Put {
                    partition: "p".into(),
                    key: "tamper".into(),
                    value: b"injected".to_vec(),
                }],
            )
            .await
            .unwrap();
    }
    let s = Steward::open(dir.path()).await.unwrap();
    let report = verify_local(&s).await.unwrap();
    assert!(!report.ok, "drifted store fails verify_local");
    assert!(
        !report.mismatches.is_empty(),
        "at least one partition mismatch reported"
    );
}

#[tokio::test]
async fn log_with_limit_returns_most_recent_n() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    for i in 0..5 {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let all = s.log(None).await.unwrap();
    let limited = s.log(Some(3)).await.unwrap();
    assert_eq!(limited.len(), 3, "limit honored");
    assert!(all.len() > limited.len());
    // The limited slice equals the last 3 records.
    let last_three: Vec<&_> = all.iter().rev().take(3).rev().collect();
    let limited_refs: Vec<&_> = limited.iter().collect();
    assert_eq!(last_three, limited_refs);
}
