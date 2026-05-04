// SPDX-License-Identifier: Apache-2.0

//! Cross-component integration tests for the sandbox prototype.
//!
//! Where the per-crate tests focus on a single component's contract,
//! these tests exercise full multi-component flows and confirm the
//! design's invariants hold end-to-end.

use std::sync::Arc;

use sandbox_remote::{MaintainOptions, Remote, RemoteError, verify_against_remote};
use sandbox_steward::{CommitKind, PartitionChecksums, Steward, StewardOptions};
use sandbox_store::checksum::{Checksum, Merkle};
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

fn opts_with_id(id: Uuid) -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(Merkle::new()),
        store_id: Some(id),
    }
}

async fn push_all_pending(source: &mut Steward, remote: &mut Remote) {
    let last_committed = source.last_committed_seq().await.unwrap();
    let pushed = remote.latest_seq().await.unwrap().unwrap_or(0);
    for seq in (pushed + 1)..=last_committed {
        if source.data_committed_record(seq).await.unwrap().is_some() {
            remote.push(source, seq).await.unwrap();
        }
    }
}

fn checksums_equal(a: &PartitionChecksums, b: &PartitionChecksums) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (k, v) in a {
        match b.get(k) {
            Some(b_v) if a_byte_equal(v, b_v) => {}
            _ => return false,
        }
    }
    true
}

fn a_byte_equal(a: &Checksum, b: &Checksum) -> bool {
    a.kind == b.kind && a.bytes == b.bytes
}

#[tokio::test]
async fn full_roundtrip_repeated_push_pull_cycles() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();

    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k1", b"v1".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(report.ok, "after cycle 1, consumer matches remote");

    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k2", b"v2".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k3", b"v3".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    let report = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(report.ok, "after cycle 2, consumer still matches remote");

    let r = consumer.begin_read().await.unwrap();
    for i in 1..=3 {
        let v = r.get("p", &format!("k{}", i)).await.unwrap();
        assert_eq!(v, Some(format!("v{}", i).as_bytes().to_vec()));
    }
}

#[tokio::test]
async fn two_consumers_have_byte_equal_partition_checksums_after_pull() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    let mut c1 =
        Steward::create_with_options(dir.path().join("c1"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let mut c2 =
        Steward::create_with_options(dir.path().join("c2"), opts_with_id(source.store_id()))
            .await
            .unwrap();

    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put(&format!("p{}", i % 2), &format!("k{}", i), b"v".to_vec())
            .unwrap();
        let _ = g.commit().await.unwrap();
    }
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut c1).await.unwrap();
    let _ = remote.pull(&mut c2).await.unwrap();

    let cs1 = c1.compute_live_checksums().await.unwrap();
    let cs2 = c2.compute_live_checksums().await.unwrap();
    assert!(
        checksums_equal(&cs1, &cs2),
        "two consumers reach byte-equal partition checksums",
    );
    let cs_source = source.compute_live_checksums().await.unwrap();
    assert!(
        checksums_equal(&cs1, &cs_source),
        "consumers also match source's live checksums",
    );
}

#[tokio::test]
async fn long_mixed_write_compact_sequence_propagates_correctly() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();

    let mut keyno = 0u64;
    for _cycle in 0..3 {
        for _ in 0..2 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", keyno), b"v".to_vec()).unwrap();
            let _ = g.commit().await.unwrap();
            keyno += 1;
        }
        let _ = source.compact(None).await.unwrap();
    }

    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();

    let cs_consumer = consumer.compute_live_checksums().await.unwrap();
    let cs_source = source.compute_live_checksums().await.unwrap();
    assert!(
        checksums_equal(&cs_consumer, &cs_source),
        "consumer mirrors source after long mixed sequence",
    );

    let r = consumer.begin_read().await.unwrap();
    for k in 0..keyno {
        assert_eq!(
            r.get("p", &format!("k{}", k)).await.unwrap(),
            Some(b"v".to_vec()),
        );
    }
}

#[tokio::test]
async fn multi_remote_push_keeps_both_in_sync() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote_a = Remote::create(dir.path().join("remote_a"), source.store_id())
        .await
        .unwrap();
    let mut remote_b = Remote::create(dir.path().join("remote_b"), source.store_id())
        .await
        .unwrap();

    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        remote_a.push(&mut source, outcome.txn_seq).await.unwrap();
        remote_b.push(&mut source, outcome.txn_seq).await.unwrap();
    }

    let mut c_a =
        Steward::create_with_options(dir.path().join("c_a"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let mut c_b =
        Steward::create_with_options(dir.path().join("c_b"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let _ = remote_a.pull(&mut c_a).await.unwrap();
    let _ = remote_b.pull(&mut c_b).await.unwrap();

    let cs_a = c_a.compute_live_checksums().await.unwrap();
    let cs_b = c_b.compute_live_checksums().await.unwrap();
    assert!(
        checksums_equal(&cs_a, &cs_b),
        "consumers from two remotes have equal live checksums",
    );

    let key_a = format!("last_pushed_seq:{}", dir.path().join("remote_a").display());
    let key_b = format!("last_pushed_seq:{}", dir.path().join("remote_b").display());
    assert!(
        source.config_get(&key_a).await.unwrap().is_some(),
        "source recorded last_pushed_seq for remote_a",
    );
    assert!(
        source.config_get(&key_b).await.unwrap().is_some(),
        "source recorded last_pushed_seq for remote_b",
    );
}

#[tokio::test]
async fn full_retention_and_restart_recovery_loop() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();

    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let outcome1 = source.compact(None).await.unwrap();
    if !outcome1.had_data {
        return;
    }
    push_all_pending(&mut source, &mut remote).await;

    let consumer_path = dir.path().join("consumer");
    let mut consumer =
        Steward::create_with_options(&consumer_path, opts_with_id(source.store_id()))
            .await
            .unwrap();
    let _ = remote.pull(&mut consumer).await.unwrap();

    for i in 3..6 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let outcome2 = source.compact(None).await.unwrap();
    if !outcome2.had_data {
        return;
    }
    push_all_pending(&mut source, &mut remote).await;

    let report = remote
        .maintain(MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: false,
        })
        .await
        .unwrap();

    if report.horizon <= consumer.last_committed_seq().await.unwrap() {
        let _ = remote.pull(&mut consumer).await.unwrap();
        let v = verify_against_remote(&remote, &consumer).await.unwrap();
        assert!(v.ok, "no-recovery path: consumer matches");
        return;
    }

    match remote.pull(&mut consumer).await {
        Err(RemoteError::BehindRetention { .. }) => {}
        other => panic!("expected BehindRetention, got {:?}", other),
    }

    drop(consumer);
    let consumer = remote.restart_from_compact(&consumer_path).await.unwrap();

    let v = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(
        v.ok,
        "after recovery, verify_against_remote passes: {:?}",
        v.mismatches
    );

    let cs_source = source.compute_live_checksums().await.unwrap();
    let cs_consumer = consumer.compute_live_checksums().await.unwrap();
    assert!(
        checksums_equal(&cs_consumer, &cs_source),
        "consumer's live checksums match source's after full recovery loop",
    );

    let r = consumer.begin_read().await.unwrap();
    for i in 0..6 {
        assert_eq!(
            r.get("p", &format!("k{}", i)).await.unwrap(),
            Some(b"v".to_vec()),
            "consumer has source's k{}",
            i
        );
    }

    let dc = consumer
        .data_committed_record(consumer.last_committed_seq().await.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dc.commit_kind, Some(CommitKind::Compact));
}
