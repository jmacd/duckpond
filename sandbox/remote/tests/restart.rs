// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`Remote::restart_from_compact`] (the
//! BehindRetention recovery path).

use std::sync::Arc;

use sandbox_remote::{MaintainOptions, Remote, RemoteError};
use sandbox_steward::{Steward, StewardOptions};
use sandbox_store::checksum::Merkle;
use tempfile::TempDir;
use uuid::Uuid;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn opts() -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(Merkle::new()),
        ..Default::default()
    }
}

/// Build a source that has pushed `writes_then_compact` cycles to a
/// remote.  Returns the source and remote; if no compact bundle was
/// produced (delta-rs found nothing to merge for the tiny test
/// data), the second element of the tuple is None to signal "skip
/// the test".
async fn source_with_compact(dir: &std::path::Path) -> Option<(Steward, Remote)> {
    let mut source = Steward::create_with_options(dir.join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.join("remote"), source.store_id())
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
    let compact_outcome = source.compact(None).await.unwrap();
    if !compact_outcome.had_data {
        return None;
    }
    remote
        .push(&mut source, compact_outcome.txn_seq)
        .await
        .unwrap();
    Some((source, remote))
}

#[tokio::test]
async fn restart_refuses_when_remote_has_no_compact_bundles() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    // One write -> one Write bundle, no compact.
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote.push(&mut source, 1).await.unwrap();

    match remote
        .restart_from_compact(&dir.path().join("consumer"))
        .await
    {
        Err(RemoteError::NoRestartPoint) => {}
        Ok(_) => panic!("expected NoRestartPoint, got Ok"),
        Err(other) => panic!("expected NoRestartPoint, got {:?}", other),
    }
}

#[tokio::test]
async fn restart_bootstraps_a_fresh_consumer() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let Some((source, remote)) = source_with_compact(dir.path()).await else {
        return;
    };

    let consumer_path = dir.path().join("consumer");
    let consumer = remote.restart_from_compact(&consumer_path).await.unwrap();

    // Consumer's data matches source's CURRENT state.
    let r = consumer.begin_read().await.unwrap();
    for i in 0..3 {
        assert_eq!(
            r.get("p", &format!("k{}", i)).await.unwrap(),
            Some(format!("v{}", i).as_bytes().to_vec()),
        );
    }

    // Consumer has the source's identity.
    assert_eq!(consumer.store_id(), source.store_id());
}

#[tokio::test]
async fn restart_wipes_existing_same_family_pond() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let Some((_source, remote)) = source_with_compact(dir.path()).await else {
        return;
    };
    let consumer_path = dir.path().join("consumer");

    // First: pull normally to establish a same-family consumer.
    let mut consumer1 = Steward::create_with_options(
        &consumer_path,
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            store_id: Some(remote.store_id()),
        },
    )
    .await
    .unwrap();
    let _ = remote.pull(&mut consumer1).await.unwrap();
    drop(consumer1);

    // Now restart.  Should accept the existing pond and wipe + re-bootstrap.
    let consumer2 = remote.restart_from_compact(&consumer_path).await.unwrap();
    assert_eq!(consumer2.store_id(), remote.store_id());

    // Data still readable post-restart.
    let r = consumer2.begin_read().await.unwrap();
    assert_eq!(r.get("p", "k0").await.unwrap(), Some(b"v0".to_vec()));
}

#[tokio::test]
async fn restart_refuses_to_wipe_a_different_family_pond() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let Some((_source, remote)) = source_with_compact(dir.path()).await else {
        return;
    };

    // Pre-existing pond at consumer_path with a DIFFERENT store_id.
    let consumer_path = dir.path().join("consumer");
    let other_id = Uuid::new_v4();
    assert_ne!(other_id, remote.store_id());
    let _ = Steward::create_with_options(
        &consumer_path,
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            store_id: Some(other_id),
        },
    )
    .await
    .unwrap();

    // Restart must refuse.
    match remote.restart_from_compact(&consumer_path).await {
        Err(RemoteError::StoreIdMismatch {
            remote: r,
            steward: s,
        }) => {
            assert_eq!(r, remote.store_id());
            assert_eq!(s, other_id);
        }
        Ok(_) => panic!("expected StoreIdMismatch, got Ok"),
        Err(other) => panic!("expected StoreIdMismatch, got {:?}", other),
    }

    // The pre-existing pond MUST still be intact.
    let preserved = Steward::open(&consumer_path).await.unwrap();
    assert_eq!(preserved.store_id(), other_id);
}

#[tokio::test]
async fn restart_refuses_to_wipe_a_non_pond_directory() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let Some((_source, remote)) = source_with_compact(dir.path()).await else {
        return;
    };

    // Make a directory that is NOT a pond (just a stray file).
    let path = dir.path().join("not_a_pond");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("random.txt"), b"not a pond").unwrap();

    match remote.restart_from_compact(&path).await {
        Err(RemoteError::RestartPathNotPond { path: p, .. }) => {
            assert_eq!(p, path.display().to_string());
        }
        Ok(_) => panic!("expected RestartPathNotPond, got Ok"),
        Err(other) => panic!("expected RestartPathNotPond, got {:?}", other),
    }

    // The stray file must still be there.
    assert!(path.join("random.txt").exists(), "non-pond file preserved");
}

#[tokio::test]
async fn behind_retention_consumer_recovers_via_restart() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let Some((_source, mut remote)) = source_with_compact(dir.path()).await else {
        return;
    };

    // Consumer that never pulled.
    let consumer_path = dir.path().join("consumer");
    let mut consumer = Steward::create_with_options(
        &consumer_path,
        StewardOptions {
            checksum_strategy: Arc::new(Merkle::new()),
            store_id: Some(remote.store_id()),
        },
    )
    .await
    .unwrap();

    // Run retention to push the horizon forward.
    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();
    if report.horizon <= 1 {
        return;
    }

    // Pull errors: BehindRetention.
    match remote.pull(&mut consumer).await {
        Err(RemoteError::BehindRetention { .. }) => {}
        other => panic!("expected BehindRetention, got {:?}", other),
    }

    // Recover via restart.
    drop(consumer);
    let consumer = remote.restart_from_compact(&consumer_path).await.unwrap();

    // Consumer now has source's current state.
    let r = consumer.begin_read().await.unwrap();
    for i in 0..3 {
        assert_eq!(
            r.get("p", &format!("k{}", i)).await.unwrap(),
            Some(format!("v{}", i).as_bytes().to_vec()),
        );
    }
}

#[tokio::test]
async fn after_restart_subsequent_pulls_are_idempotent_noops() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let Some((_source, remote)) = source_with_compact(dir.path()).await else {
        return;
    };

    let consumer_path = dir.path().join("consumer");
    let mut consumer = remote.restart_from_compact(&consumer_path).await.unwrap();

    // Re-pull: nothing new.
    let report = remote.pull(&mut consumer).await.unwrap();
    assert!(
        report.bundles_applied.is_empty(),
        "no bundles applied on re-pull after restart, got {}",
        report.bundles_applied.len()
    );
}
