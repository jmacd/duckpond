// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`Remote::maintain`] (remote retention).

use std::sync::Arc;

use sandbox_remote::{MaintainOptions, Remote, RemoteError};
use sandbox_steward::{Steward, StewardOptions};
use sandbox_store::checksum::Merkle;
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

/// Set up a source steward that has pushed some bundles to a fresh remote.
/// `compacts_after_each_n_writes`: write N values then compact, repeat.
async fn make_source_with_pattern(
    dir: &std::path::Path,
    write_compact_pattern: &[(usize, bool)],
) -> (Steward, Remote) {
    let mut source = Steward::create_with_options(dir.join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.join("remote"), source.store_id())
        .await
        .unwrap();

    let mut keyno = 0u64;
    for (writes, do_compact) in write_compact_pattern {
        for _ in 0..*writes {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", keyno), b"v".to_vec()).unwrap();
            let outcome = g.commit().await.unwrap();
            remote.push(&mut source, outcome.txn_seq).await.unwrap();
            keyno += 1;
        }
        if *do_compact {
            let outcome = source.compact(None).await.unwrap();
            if outcome.had_data {
                remote.push(&mut source, outcome.txn_seq).await.unwrap();
            }
        }
    }
    (source, remote)
}

#[tokio::test]
async fn maintain_refuses_keep_zero() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(2, true)]).await;
    let result = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 0,
            vacuum_after: false,
        })
        .await;
    match result {
        Err(RemoteError::InvalidRetention(0)) => {}
        Ok(_) => panic!("expected InvalidRetention, got Ok"),
        Err(other) => panic!("expected InvalidRetention, got {:?}", other),
    }
}

#[tokio::test]
async fn maintain_refuses_when_fewer_compact_bundles_than_keep() {
    init_logger();
    let dir = TempDir::new().unwrap();
    // Two writes, no compact -> 0 compact bundles.
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(2, false)]).await;
    let result = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await;
    match result {
        Err(RemoteError::InsufficientCompactBundles { have, need }) => {
            assert_eq!(have, 0);
            assert_eq!(need, 1);
        }
        Ok(_) => panic!("expected InsufficientCompactBundles, got Ok"),
        Err(other) => panic!("expected InsufficientCompactBundles, got {:?}", other),
    }
}

#[tokio::test]
async fn maintain_keep_one_prunes_writes_keeps_compact() {
    init_logger();
    let dir = TempDir::new().unwrap();
    // 3 writes + 1 compact => bundles 1..3 (Write) + 4 (Compact, if delta-rs merges).
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(3, true)]).await;
    let bundles_before = remote.list_bundles().await.unwrap();
    let compact_count = bundles_before
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .count();
    if compact_count == 0 {
        // delta-rs found nothing to merge for these tiny files; the
        // retention path can't be exercised. Skip.
        return;
    }

    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();

    let bundles_after = remote.list_bundles().await.unwrap();
    assert!(
        bundles_after.len() < bundles_before.len(),
        "some bundles were pruned"
    );
    // The single compact bundle remains.
    let post_compacts: Vec<_> = bundles_after
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .collect();
    assert_eq!(post_compacts.len(), 1, "exactly one compact bundle remains");

    // horizon equals that compact's seq.
    assert_eq!(report.horizon, post_compacts[0].txn_seq);

    // oldest_available_seq advanced.
    let oldest = remote.oldest_available_seq().await.unwrap().unwrap();
    assert_eq!(oldest, post_compacts[0].txn_seq);
}

#[tokio::test]
async fn maintain_keep_two_with_two_compacts_keeps_both() {
    init_logger();
    let dir = TempDir::new().unwrap();
    // 2 writes + compact, then 2 writes + compact -> ideally 2 compacts in the bundle list.
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(2, true), (2, true)]).await;
    let bundles_before = remote.list_bundles().await.unwrap();
    let compact_count = bundles_before
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .count();
    if compact_count < 2 {
        // delta-rs decided not to merge in one or both rounds; skip.
        return;
    }

    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 2,
            vacuum_after: false,
        })
        .await
        .unwrap();

    let bundles_after = remote.list_bundles().await.unwrap();
    let post_compacts: Vec<_> = bundles_after
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .collect();
    assert_eq!(post_compacts.len(), 2, "both compact bundles remain");

    // horizon is the older of the two compacts.
    let oldest_compact_seq = post_compacts.iter().map(|b| b.txn_seq).min().unwrap();
    assert_eq!(report.horizon, oldest_compact_seq);

    // Everything before the oldest compact was pruned.
    for b in &bundles_after {
        assert!(
            b.txn_seq >= oldest_compact_seq,
            "bundle seq {} survived but should have been pruned",
            b.txn_seq
        );
    }
}

#[tokio::test]
async fn maintain_idempotent_re_call() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(3, true)]).await;
    let compact_count = remote
        .list_bundles()
        .await
        .unwrap()
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .count();
    if compact_count == 0 {
        return;
    }

    let r1 = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();
    let bundles_after_first = remote.list_bundles().await.unwrap();

    // Second call: no rows deleted (already at horizon).
    let r2 = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();
    let bundles_after_second = remote.list_bundles().await.unwrap();

    assert_eq!(r1.horizon, r2.horizon, "horizon stable across re-call");
    assert_eq!(
        bundles_after_first.len(),
        bundles_after_second.len(),
        "no additional bundles pruned on re-call"
    );
    assert_eq!(r2.rows_deleted, 0, "second call deletes nothing");
}

#[tokio::test]
async fn maintain_with_vacuum_disabled_reports_zero_files_vacuumed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(3, true)]).await;
    let compact_count = remote
        .list_bundles()
        .await
        .unwrap()
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .count();
    if compact_count == 0 {
        return;
    }

    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();
    assert_eq!(report.files_vacuumed, 0);
}

#[tokio::test]
async fn maintain_with_vacuum_reclaims_files() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (_source, mut remote) = make_source_with_pattern(dir.path(), &[(3, true)]).await;
    let compact_count = remote
        .list_bundles()
        .await
        .unwrap()
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .count();
    if compact_count == 0 {
        return;
    }

    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: true,
        })
        .await
        .unwrap();
    // Some files should have been reclaimed (the parquets that
    // contained the deleted rows are now unreferenced).
    assert!(
        report.files_vacuumed > 0,
        "vacuum reclaimed at least one file (got {})",
        report.files_vacuumed
    );
}

/// After retention prunes bundles below a consumer's last_pulled_seq,
/// the consumer's next pull must error with BehindRetention pointing
/// at the recovery path.
#[tokio::test]
async fn consumer_below_horizon_gets_behind_retention_on_pull() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (source, mut remote) = make_source_with_pattern(dir.path(), &[(3, true)]).await;
    let bundles = remote.list_bundles().await.unwrap();
    let compact_count = bundles
        .iter()
        .filter(|b| matches!(b.commit_kind, sandbox_steward::CommitKind::Compact))
        .count();
    if compact_count == 0 {
        return;
    }

    // Consumer is created BEFORE retention; never pulls.  Its
    // last_pulled_seq is implicitly 0.
    let mut consumer = Steward::create_with_options(
        dir.path().join("consumer"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            store_id: Some(source.store_id()),
        },
    )
    .await
    .unwrap();
    let _ = source; // unused after maintain

    // Maintain: prune all bundles below the single compact.
    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();
    if report.horizon <= 1 {
        // Nothing was pruned (e.g., compact at seq 1 means horizon=1
        // and all writes had seq < 1 was empty).  Skip.
        return;
    }

    // Now pull from the consumer (last_pulled_seq=0).  Since the
    // remote's oldest_available is now at horizon, and horizon > 1,
    // the gap (oldest > last_pulled + 1) triggers BehindRetention.
    match remote.pull(&mut consumer).await {
        Err(RemoteError::BehindRetention {
            last_pulled,
            oldest_available,
        }) => {
            assert_eq!(last_pulled, 0);
            assert_eq!(oldest_available, report.horizon);
        }
        Ok(rep) => panic!(
            "expected BehindRetention, got Ok(bundles_applied: {})",
            rep.bundles_applied.len()
        ),
        Err(other) => panic!("expected BehindRetention, got {:?}", other),
    }
}
