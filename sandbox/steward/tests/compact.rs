// SPDX-License-Identifier: Apache-2.0

//! Tests for [`Steward::compact`] (the `compact-op` todo).
//!
//! Per `DESIGN.md` §2.4 and §5.1, compaction is a Delta-level merge of
//! small parquets that MUST NOT change the per-partition content
//! checksum.  The lifecycle has three branches:
//!
//! - error from `Store::compact` -> `Failed`
//! - no-op (no files added or removed) -> `Completed`,
//!   `last_committed_seq` does not advance
//! - real work -> `DataCommitted` with `commit_kind = Compact`,
//!   per-partition checksums must equal the pre-compaction snapshot.

use std::sync::Arc;

use sandbox_steward::{CommitKind, RecordKind, Steward, StewardOptions, verify_local};
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

/// Write `n` keys to `partition`, each in its own commit, so that the
/// underlying Delta table accumulates `n` separate parquet files for
/// that partition.  Use this to set up a state where compaction will
/// actually have work to do.
async fn write_n_separate_commits(steward: &mut Steward, partition: &str, n: usize) {
    for i in 0..n {
        let mut g = steward.begin_write().await.unwrap();
        g.put(
            partition,
            &format!("k{}", i),
            format!("v{}", i).into_bytes(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
    }
}

#[tokio::test]
async fn compact_does_not_change_get_or_list() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(dir.path(), opts_merkle())
        .await
        .unwrap();
    write_n_separate_commits(&mut s, "p", 5).await;

    let r = s.begin_read().await.unwrap();
    let pre_list = r.list("p").await.unwrap();
    let pre_get = r.get("p", "k2").await.unwrap();
    drop(r);

    let outcome = s.compact(None).await.unwrap();
    assert_eq!(outcome.commit_kind, CommitKind::Compact);

    let r = s.begin_read().await.unwrap();
    let post_list = r.list("p").await.unwrap();
    let post_get = r.get("p", "k2").await.unwrap();
    assert_eq!(pre_list, post_list, "list must be byte-identical");
    assert_eq!(pre_get, post_get, "get must be byte-identical");

    let report = verify_local(&s).await.unwrap();
    assert!(
        report.ok,
        "verify_local must pass after compact: {:?}",
        report.mismatches
    );
}

#[tokio::test]
async fn compact_records_data_committed_with_compact_kind_when_files_merge() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    write_n_separate_commits(&mut s, "p", 4).await;
    let recs_before = s.log(None).await.unwrap();
    let pre_committed = s.last_committed_seq().await.unwrap();
    assert_eq!(pre_committed, 4);

    let outcome = s.compact(None).await.unwrap();
    assert_eq!(outcome.commit_kind, CommitKind::Compact);
    assert!(
        outcome.had_data,
        "compaction over many small files is not a no-op"
    );
    assert_eq!(outcome.txn_seq, 5);
    assert_eq!(s.last_committed_seq().await.unwrap(), 5);

    let recs_after = s.log(None).await.unwrap();
    let new_recs: Vec<_> = recs_after.iter().skip(recs_before.len()).collect();
    let begins: Vec<_> = new_recs
        .iter()
        .filter(|r| r.record_kind == RecordKind::Begin && r.txn_seq == 5)
        .collect();
    let committed: Vec<_> = new_recs
        .iter()
        .filter(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == 5)
        .collect();
    assert_eq!(begins.len(), 1, "exactly one Begin appended for compact");
    assert_eq!(
        committed.len(),
        1,
        "exactly one DataCommitted appended for compact"
    );
    assert_eq!(committed[0].commit_kind, Some(CommitKind::Compact));
    assert_eq!(committed[0].parent_seq, Some(4), "parent is last write");
}

#[tokio::test]
async fn compact_preserves_partition_checksums() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(dir.path(), opts_merkle())
        .await
        .unwrap();
    write_n_separate_commits(&mut s, "p1", 3).await;
    write_n_separate_commits(&mut s, "p2", 2).await;
    let pre_seq = s.last_committed_seq().await.unwrap();
    let pre = s.partition_checksums_at(pre_seq).await.unwrap().unwrap();

    let outcome = s.compact(None).await.unwrap();
    assert!(outcome.had_data);
    let post = s
        .partition_checksums_at(outcome.txn_seq)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        pre, post,
        "compaction must not change any partition checksum"
    );
}

#[tokio::test]
async fn compact_with_partition_filter_only_touches_that_partition() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    write_n_separate_commits(&mut s, "p1", 3).await;
    write_n_separate_commits(&mut s, "p2", 3).await;

    // Capture file lists per-partition.  Since each commit writes one
    // parquet, we can identify a file's owning partition by its path
    // (Delta partitions data files under `partition_key=<value>/...`).
    let files_before = list_data_files(&s).await;
    let p1_before: Vec<_> = files_before
        .iter()
        .filter(|p| p.contains("partition_key=p1"))
        .cloned()
        .collect();
    let p2_before: Vec<_> = files_before
        .iter()
        .filter(|p| p.contains("partition_key=p2"))
        .cloned()
        .collect();
    assert_eq!(p1_before.len(), 3);
    assert_eq!(p2_before.len(), 3);

    let outcome = s.compact(Some("p1")).await.unwrap();
    assert!(outcome.had_data);

    let files_after = list_data_files(&s).await;
    let p1_after: Vec<_> = files_after
        .iter()
        .filter(|p| p.contains("partition_key=p1"))
        .cloned()
        .collect();
    let p2_after: Vec<_> = files_after
        .iter()
        .filter(|p| p.contains("partition_key=p2"))
        .cloned()
        .collect();
    assert!(
        p1_after.len() < p1_before.len(),
        "p1 must have fewer files after compact"
    );
    assert_eq!(
        p2_after, p2_before,
        "p2 file set must be unchanged by compact(p1)"
    );

    // Logical content is unchanged.
    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}

/// Helper: list the parquet data files referenced by the current Delta
/// version of the data store.  Re-opens the store at `<pond>/data` and
/// reads the file URIs.  Sorted for deterministic comparison.
async fn list_data_files(s: &Steward) -> Vec<String> {
    let path = s.path().join("data");
    let store = sandbox_store::Store::open(&path).await.unwrap();
    store.data_files().unwrap()
}

#[tokio::test]
async fn compact_on_empty_pond_is_noop_completed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    let outcome = s.compact(None).await.unwrap();
    assert_eq!(outcome.commit_kind, CommitKind::Compact);
    assert!(!outcome.had_data, "empty-pond compact is a no-op");
    assert_eq!(outcome.txn_seq, 1);
    assert_eq!(s.last_write_seq(), 1);
    assert_eq!(
        s.last_committed_seq().await.unwrap(),
        0,
        "no-op compact must not advance last_committed_seq"
    );

    let recs = s.log(None).await.unwrap();
    assert!(
        recs.iter()
            .any(|r| r.record_kind == RecordKind::Completed && r.txn_seq == 1),
        "no-op compact records Completed"
    );
    assert!(
        !recs
            .iter()
            .any(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == 1),
        "no-op compact must NOT record DataCommitted"
    );
}

#[tokio::test]
async fn compact_on_nonexistent_filter_is_noop_completed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    write_n_separate_commits(&mut s, "p", 3).await;
    let committed_before = s.last_committed_seq().await.unwrap();

    let outcome = s.compact(Some("nonexistent")).await.unwrap();
    assert!(!outcome.had_data, "filter-misses-everything is a no-op");
    assert_eq!(
        s.last_committed_seq().await.unwrap(),
        committed_before,
        "no-op compact must not advance last_committed_seq"
    );
}

#[tokio::test]
async fn compact_advances_parent_seq() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    write_n_separate_commits(&mut s, "p", 3).await;
    let outcome = s.compact(None).await.unwrap();
    assert!(outcome.had_data);

    let recs = s.log(None).await.unwrap();
    let compact_committed = recs
        .iter()
        .find(|r| {
            r.record_kind == RecordKind::DataCommitted && r.commit_kind == Some(CommitKind::Compact)
        })
        .expect("DataCommitted(Compact) must exist");
    assert_eq!(
        compact_committed.parent_seq,
        Some(3),
        "parent_seq points at most recent prior committed write"
    );
}

#[tokio::test]
async fn compact_under_homomorphic_strategy() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(dir.path(), opts_homomorphic())
        .await
        .unwrap();
    write_n_separate_commits(&mut s, "p", 3).await;
    let pre_seq = s.last_committed_seq().await.unwrap();
    let pre = s.partition_checksums_at(pre_seq).await.unwrap().unwrap();

    let outcome = s.compact(None).await.unwrap();
    assert!(outcome.had_data);
    let post = s
        .partition_checksums_at(outcome.txn_seq)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pre, post, "Homomorphic invariance under compaction");

    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}

#[tokio::test]
async fn tombstones_survive_compaction() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k1", b"v1".to_vec()).unwrap();
        g.put("p", "k2", b"v2".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = s.begin_write().await.unwrap();
        g.delete("p", "k2").unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p", "k3", b"v3".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let _ = s.compact(None).await.unwrap();

    let r = s.begin_read().await.unwrap();
    assert_eq!(r.get("p", "k1").await.unwrap(), Some(b"v1".to_vec()));
    assert_eq!(
        r.get("p", "k2").await.unwrap(),
        None,
        "tombstoned key must remain absent after compaction"
    );
    assert_eq!(r.get("p", "k3").await.unwrap(), Some(b"v3".to_vec()));
    let listed = r.list("p").await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|(k, _)| k == "k1"));
    assert!(listed.iter().any(|(k, _)| k == "k3"));

    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}

#[tokio::test]
async fn interleaved_writes_and_compactions() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create(dir.path()).await.unwrap();

    write_n_separate_commits(&mut s, "p", 3).await; // seqs 1..=3
    let c1 = s.compact(None).await.unwrap(); // seq 4
    assert!(c1.had_data);
    write_n_separate_commits(&mut s, "p", 2).await; // seqs 5..=6
    let c2 = s.compact(None).await.unwrap(); // seq 7
    assert!(c2.had_data);

    assert_eq!(s.last_write_seq(), 7);
    assert_eq!(s.last_committed_seq().await.unwrap(), 7);

    let recs = s.log(None).await.unwrap();
    let writes: Vec<_> = recs
        .iter()
        .filter(|r| {
            r.record_kind == RecordKind::DataCommitted && r.commit_kind == Some(CommitKind::Write)
        })
        .collect();
    let compacts: Vec<_> = recs
        .iter()
        .filter(|r| {
            r.record_kind == RecordKind::DataCommitted && r.commit_kind == Some(CommitKind::Compact)
        })
        .collect();
    assert_eq!(writes.len(), 5, "five user writes");
    assert_eq!(compacts.len(), 2, "two compactions");

    // parent_seq chain: each commit references the previous one.
    let mut prev = 0i64;
    for r in recs
        .iter()
        .filter(|r| r.record_kind == RecordKind::DataCommitted)
    {
        let want_parent = if prev == 0 { None } else { Some(prev) };
        assert_eq!(
            r.parent_seq, want_parent,
            "parent_seq broken at {}",
            r.txn_seq
        );
        prev = r.txn_seq;
    }

    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}

#[tokio::test]
async fn compact_persists_across_reopen() {
    init_logger();
    let dir = TempDir::new().unwrap();
    {
        let mut s = Steward::create(dir.path()).await.unwrap();
        write_n_separate_commits(&mut s, "p", 3).await;
        let outcome = s.compact(None).await.unwrap();
        assert!(outcome.had_data);
        assert_eq!(s.last_write_seq(), 4);
        assert_eq!(s.last_committed_seq().await.unwrap(), 4);
    }
    let s = Steward::open(dir.path()).await.unwrap();
    assert_eq!(s.last_write_seq(), 4);
    assert_eq!(s.last_committed_seq().await.unwrap(), 4);

    let recs = s.log(None).await.unwrap();
    assert!(
        recs.iter()
            .any(|r| r.record_kind == RecordKind::DataCommitted
                && r.commit_kind == Some(CommitKind::Compact)
                && r.txn_seq == 4),
        "compact DataCommitted persists"
    );

    let report = verify_local(&s).await.unwrap();
    assert!(report.ok);
}
