// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`verify_against_remote`].

use std::sync::Arc;

use sandbox_remote::{Remote, RemoteError, verify_against_remote};
use sandbox_steward::{Steward, StewardOptions};
use sandbox_store::checksum::{Merkle, PartitionChecksum};
use tempfile::TempDir;
use uuid::Uuid;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn opts_with_id(id: Uuid) -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(Merkle::new()),
        store_id: Some(id),
    }
}

/// Build a triple where the consumer has pulled all of source's
/// pushed bundles.  Returns (source, remote, consumer).
async fn synced_triple(dir: &std::path::Path) -> (Steward, Remote, Steward) {
    let mut source = Steward::create_with_options(
        dir.join("source"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut remote = Remote::create(dir.join("remote"), source.store_id())
        .await
        .unwrap();
    let mut consumer =
        Steward::create_with_options(dir.join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut source, outcome.txn_seq).await.unwrap();
    }
    let _ = remote.pull(&mut consumer).await.unwrap();
    (source, remote, consumer)
}

#[tokio::test]
async fn verify_passes_when_consumer_is_current() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (_source, remote, consumer) = synced_triple(dir.path()).await;

    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(
        report.ok,
        "synced consumer verifies, got {:?}",
        report.mismatches
    );
    assert!(report.mismatches.is_empty());
    assert!(report.divergence_boundary.is_none());
    assert_eq!(report.remote_latest_seq, Some(3));
}

#[tokio::test]
async fn verify_returns_empty_report_when_remote_has_no_bundles() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let consumer = Steward::create_with_options(
        dir.path().join("consumer"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let remote = Remote::create(dir.path().join("remote"), consumer.store_id())
        .await
        .unwrap();

    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(report.ok, "empty consumer + empty remote = vacuously ok");
    assert_eq!(report.remote_latest_seq, None);
    assert!(report.mismatches.is_empty());
}

#[tokio::test]
async fn verify_returns_not_ok_when_empty_remote_but_consumer_has_data() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut consumer = Steward::create_with_options(
        dir.path().join("consumer"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let remote = Remote::create(dir.path().join("remote"), consumer.store_id())
        .await
        .unwrap();

    {
        let mut g = consumer.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(!report.ok, "consumer has data, remote is empty: not ok");
    assert_eq!(report.remote_latest_seq, None);
}

#[tokio::test]
async fn verify_errors_on_store_id_mismatch() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(
        dir.path().join("source"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote.push(&mut source, 1).await.unwrap();

    let other_id = Uuid::new_v4();
    let other = Steward::create_with_options(dir.path().join("other"), opts_with_id(other_id))
        .await
        .unwrap();

    match verify_against_remote(&remote, &other).await {
        Err(RemoteError::StoreIdMismatch {
            remote: r,
            steward: s,
        }) => {
            assert_eq!(r, source.store_id());
            assert_eq!(s, other_id);
        }
        Ok(rep) => panic!("expected StoreIdMismatch, got Ok: {:?}", rep),
        Err(other) => panic!("expected StoreIdMismatch, got {:?}", other),
    }
}

#[tokio::test]
async fn verify_detects_local_data_drift_via_partition_mismatch() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let (_source, remote, mut consumer) = synced_triple(dir.path()).await;

    // Tamper with the consumer's local data: write a value for a NEW
    // key on a partition the consumer already has.  This breaks the
    // recorded partition_checksum at remote.latest_seq because the
    // consumer's partition now contains additional logical content.
    {
        let mut g = consumer.begin_write().await.unwrap();
        g.put("p", "tamper", b"local-only".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(!report.ok, "tampered consumer fails verification");
    assert!(!report.mismatches.is_empty());
    let m = report
        .mismatches
        .iter()
        .find(|m| m.partition == "p")
        .expect("partition `p` mismatched");
    assert!(m.consumer_live.is_some());
    assert!(m.remote_recorded.is_some());
    assert_ne!(
        m.consumer_live.as_ref().map(|c| &c.bytes),
        m.remote_recorded.as_ref().map(|c| &c.bytes),
    );
}

#[tokio::test]
async fn verify_walks_back_to_find_divergence_boundary() {
    init_logger();
    let dir = TempDir::new().unwrap();

    // Build a setup where:
    //   - Source pushes bundles 1, 2, 3 (each adds a key under "p").
    //   - Consumer pulls all of them.
    //   - Source pushes bundle 4 (adds another key).  Consumer
    //     stays behind.
    //   - On verify, consumer's state should match the recorded
    //     checksum at seq 3 (the divergence boundary), not seq 4.
    let mut source = Steward::create_with_options(
        dir.path().join("source"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut source, outcome.txn_seq).await.unwrap();
    }
    let _ = remote.pull(&mut consumer).await.unwrap();

    // Source diverges: pushes bundle 4 with another write.
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k_new", b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut source, outcome.txn_seq).await.unwrap();
    }
    assert_eq!(remote.latest_seq().await.unwrap(), Some(4));

    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(!report.ok, "consumer at seq 3 differs from remote latest 4");
    assert_eq!(report.remote_latest_seq, Some(4));
    assert_eq!(
        report.divergence_boundary,
        Some(3),
        "consumer's state matches what was recorded at seq 3"
    );
}

#[tokio::test]
async fn verify_returns_divergence_none_when_no_prior_match_found() {
    init_logger();
    let dir = TempDir::new().unwrap();

    // Source pushes 1 bundle.  Consumer is bootstrapped fresh
    // (no pull) and writes its OWN data locally.  Verify should
    // find no agreeing bundle in the walk.
    let mut source = Steward::create_with_options(
        dir.path().join("source"),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "src", b"src-value".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote.push(&mut source, 1).await.unwrap();

    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    {
        let mut g = consumer.begin_write().await.unwrap();
        g.put("p", "consumer-only", b"different".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(!report.ok);
    assert_eq!(
        report.divergence_boundary, None,
        "no prior bundle agrees with consumer's diverged state"
    );
}

#[tokio::test]
async fn verify_consumer_with_compute_live_checksums_matches_verify_local_path() {
    init_logger();
    // Cross-check: Steward::compute_live_checksums (used by
    // verify_against_remote) and the strategy used by verify_local
    // (in steward) compute the same values for the same data.  Both
    // call into Store::compute_partition_checksum, but documenting
    // the equivalence as a regression guard is cheap.
    let dir = TempDir::new().unwrap();
    let mut s = Steward::create_with_options(
        dir.path(),
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    {
        let mut g = s.begin_write().await.unwrap();
        g.put("p1", "k1", b"v1".to_vec()).unwrap();
        g.put("p2", "k2", b"v2".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let live = s.compute_live_checksums().await.unwrap();
    let recorded = s.partition_checksums_at(1).await.unwrap().unwrap();
    assert_eq!(live, recorded, "live == recorded right after commit");
    let _ = Merkle::new().kind(); // silence import
}
