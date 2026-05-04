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
        if source
            .data_committed_record(source.store_id(), seq)
            .await
            .unwrap()
            .is_some()
        {
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

    let cs1 = c1.compute_live_checksums(c1.store_id()).await.unwrap();
    let cs2 = c2.compute_live_checksums(c2.store_id()).await.unwrap();
    assert!(
        checksums_equal(&cs1, &cs2),
        "two consumers reach byte-equal partition checksums",
    );
    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
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

    let cs_consumer = consumer
        .compute_live_checksums(consumer.store_id())
        .await
        .unwrap();
    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
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

    let cs_a = c_a.compute_live_checksums(c_a.store_id()).await.unwrap();
    let cs_b = c_b.compute_live_checksums(c_b.store_id()).await.unwrap();
    assert!(
        checksums_equal(&cs_a, &cs_b),
        "consumers from two remotes have equal live checksums",
    );

    let key_a = format!("last_pushed_seq:{}", remote_a.url());
    let key_b = format!("last_pushed_seq:{}", remote_b.url());
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

    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
    let cs_consumer = consumer
        .compute_live_checksums(consumer.store_id())
        .await
        .unwrap();
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
        .data_committed_record(
            consumer.store_id(),
            consumer.last_committed_seq().await.unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dc.commit_kind, Some(CommitKind::Compact));
}

// ============================================================================
// Group A: Scaling
// ============================================================================

#[tokio::test]
async fn five_consumers_pull_and_have_equal_checksums() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();

    for i in 0..5 {
        let mut g = source.begin_write().await.unwrap();
        g.put(&format!("p{}", i % 3), &format!("k{}", i), b"v".to_vec())
            .unwrap();
        let _ = g.commit().await.unwrap();
    }
    push_all_pending(&mut source, &mut remote).await;

    let mut consumers: Vec<Steward> = Vec::new();
    for i in 0..5 {
        let mut c = Steward::create_with_options(
            dir.path().join(format!("c{}", i)),
            opts_with_id(source.store_id()),
        )
        .await
        .unwrap();
        let _ = remote.pull(&mut c).await.unwrap();
        consumers.push(c);
    }

    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
    for (i, c) in consumers.iter().enumerate() {
        let cs = c.compute_live_checksums(c.store_id()).await.unwrap();
        assert!(
            checksums_equal(&cs, &cs_source),
            "consumer {} matches source",
            i
        );
        let v = verify_against_remote(&remote, c).await.unwrap();
        assert!(v.ok, "consumer {} verifies against remote", i);
    }
}

#[tokio::test]
async fn many_writes_then_consumer_catches_up_in_one_pull() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();

    const N: usize = 30;
    for i in 0..N {
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

    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let report = remote.pull(&mut consumer).await.unwrap();
    assert_eq!(report.bundles_applied.len(), N);
    assert_eq!(report.last_pulled_seq, N as i64);

    let r = consumer.begin_read().await.unwrap();
    for i in 0..N {
        assert_eq!(
            r.get("p", &format!("k{}", i)).await.unwrap(),
            Some(format!("v{}", i).as_bytes().to_vec()),
        );
    }
}

// ============================================================================
// Group B: Operation-order equivalence
// ============================================================================

#[tokio::test]
async fn push_per_write_vs_push_batched_yield_same_consumer_state() {
    init_logger();
    let dir = TempDir::new().unwrap();

    // Source A: push after every write.
    let mut src_a = Steward::create_with_options(dir.path().join("src_a"), opts())
        .await
        .unwrap();
    let mut rem_a = Remote::create(dir.path().join("rem_a"), src_a.store_id())
        .await
        .unwrap();
    for i in 0..5 {
        let mut g = src_a.begin_write().await.unwrap();
        g.put(
            "p",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let outcome = g.commit().await.unwrap();
        rem_a.push(&mut src_a, outcome.txn_seq).await.unwrap();
    }

    // Source B: write 5 then push all.  Same store_id as A so we can
    // verify checksum equality cleanly.
    let mut src_b =
        Steward::create_with_options(dir.path().join("src_b"), opts_with_id(src_a.store_id()))
            .await
            .unwrap();
    let mut rem_b = Remote::create(dir.path().join("rem_b"), src_b.store_id())
        .await
        .unwrap();
    for i in 0..5 {
        let mut g = src_b.begin_write().await.unwrap();
        g.put(
            "p",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
    }
    push_all_pending(&mut src_b, &mut rem_b).await;

    // Consumers from each.
    let mut c_a =
        Steward::create_with_options(dir.path().join("c_a"), opts_with_id(src_a.store_id()))
            .await
            .unwrap();
    let mut c_b =
        Steward::create_with_options(dir.path().join("c_b"), opts_with_id(src_b.store_id()))
            .await
            .unwrap();
    let _ = rem_a.pull(&mut c_a).await.unwrap();
    let _ = rem_b.pull(&mut c_b).await.unwrap();

    let cs_a = c_a.compute_live_checksums(c_a.store_id()).await.unwrap();
    let cs_b = c_b.compute_live_checksums(c_b.store_id()).await.unwrap();
    assert!(
        checksums_equal(&cs_a, &cs_b),
        "push timing is irrelevant to final consumer state"
    );
}

#[tokio::test]
async fn consumer_pulls_each_vs_pulls_batched_yield_same_state() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    let mut c_each =
        Steward::create_with_options(dir.path().join("c_each"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let mut c_batch =
        Steward::create_with_options(dir.path().join("c_batch"), opts_with_id(source.store_id()))
            .await
            .unwrap();

    for i in 0..5 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut source, outcome.txn_seq).await.unwrap();
        // c_each pulls after every push.
        let _ = remote.pull(&mut c_each).await.unwrap();
    }
    // c_batch pulls only at the end.
    let _ = remote.pull(&mut c_batch).await.unwrap();

    let cs_each = c_each
        .compute_live_checksums(c_each.store_id())
        .await
        .unwrap();
    let cs_batch = c_batch
        .compute_live_checksums(c_batch.store_id())
        .await
        .unwrap();
    assert!(
        checksums_equal(&cs_each, &cs_batch),
        "pull timing is irrelevant to final consumer state",
    );
}

// ============================================================================
// Group C: Retention + restart cycles
// ============================================================================

#[tokio::test]
async fn multiple_retention_rounds_with_pulls_between_each() {
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
    for round in 0..3 {
        // Each round: 2 writes + compact + push, then consumer pulls.
        for _ in 0..2 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", keyno), b"v".to_vec()).unwrap();
            let _ = g.commit().await.unwrap();
            keyno += 1;
        }
        let _ = source.compact(None).await.unwrap();
        push_all_pending(&mut source, &mut remote).await;
        let _ = remote.pull(&mut consumer).await.unwrap();

        let v = verify_against_remote(&remote, &consumer).await.unwrap();
        assert!(v.ok, "round {}: consumer matches", round);

        // Run retention every round; consumer is current so no
        // BehindRetention.
        let compacts_so_far = remote
            .list_bundles()
            .await
            .unwrap()
            .iter()
            .filter(|b| matches!(b.commit_kind, CommitKind::Compact))
            .count();
        if compacts_so_far >= 1 {
            let _ = remote
                .maintain(MaintainOptions {
                    keep_compact_bundles: 1,
                    vacuum_after: false,
                })
                .await
                .unwrap();
        }
    }

    // Final state: consumer matches source.
    let cs_consumer = consumer
        .compute_live_checksums(consumer.store_id())
        .await
        .unwrap();
    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
    assert!(checksums_equal(&cs_consumer, &cs_source));
}

#[tokio::test]
async fn restart_twice_is_idempotent_in_final_state() {
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
    let outcome = source.compact(None).await.unwrap();
    if !outcome.had_data {
        return;
    }
    push_all_pending(&mut source, &mut remote).await;

    let consumer_path = dir.path().join("consumer");
    // First restart from a non-existent path.
    let consumer = remote.restart_from_compact(&consumer_path).await.unwrap();
    let cs_first = consumer
        .compute_live_checksums(consumer.store_id())
        .await
        .unwrap();
    drop(consumer);

    // Second restart from the same path.  Should wipe + re-bootstrap;
    // final state must be byte-identical (modulo timestamps).
    let consumer = remote.restart_from_compact(&consumer_path).await.unwrap();
    let cs_second = consumer
        .compute_live_checksums(consumer.store_id())
        .await
        .unwrap();

    assert!(
        checksums_equal(&cs_first, &cs_second),
        "two restarts produce equal final checksums",
    );
}

#[tokio::test]
async fn consumer_can_continue_pulling_after_restart() {
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
    let outcome = source.compact(None).await.unwrap();
    if !outcome.had_data {
        return;
    }
    push_all_pending(&mut source, &mut remote).await;

    let consumer_path = dir.path().join("consumer");
    let mut consumer = remote.restart_from_compact(&consumer_path).await.unwrap();

    // Source pushes more after consumer's restart.
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "after_restart", b"v_new".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut source, outcome.txn_seq).await.unwrap();
    }
    // Consumer pulls normally.
    let report = remote.pull(&mut consumer).await.unwrap();
    assert_eq!(report.bundles_applied.len(), 1);

    let r = consumer.begin_read().await.unwrap();
    assert_eq!(
        r.get("p", "after_restart").await.unwrap(),
        Some(b"v_new".to_vec()),
        "consumer can resume pulling after restart"
    );
}

// ============================================================================
// Group D: Multi-remote
// ============================================================================

#[tokio::test]
async fn divergent_retention_per_remote_both_consumers_correct() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut rem_a = Remote::create(dir.path().join("rem_a"), source.store_id())
        .await
        .unwrap();
    let mut rem_b = Remote::create(dir.path().join("rem_b"), source.store_id())
        .await
        .unwrap();
    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        rem_a.push(&mut source, outcome.txn_seq).await.unwrap();
        rem_b.push(&mut source, outcome.txn_seq).await.unwrap();
    }
    let outcome = source.compact(None).await.unwrap();
    if outcome.had_data {
        rem_a.push(&mut source, outcome.txn_seq).await.unwrap();
        rem_b.push(&mut source, outcome.txn_seq).await.unwrap();
    }

    // Apply retention to A only.
    if outcome.had_data {
        let _ = rem_a
            .maintain(MaintainOptions {
                keep_compact_bundles: 1,
                vacuum_after: false,
            })
            .await
            .unwrap();
    }

    // Consumer A: pull from the pruned rem_a -> BehindRetention ->
    // restart_from_compact recovers.
    let c_a_path = dir.path().join("c_a");
    let mut c_a = Steward::create_with_options(&c_a_path, opts_with_id(source.store_id()))
        .await
        .unwrap();
    let mut c_a = match rem_a.pull(&mut c_a).await {
        Ok(_) => c_a, // (no retention triggered)
        Err(RemoteError::BehindRetention { .. }) => {
            drop(c_a);
            rem_a.restart_from_compact(&c_a_path).await.unwrap()
        }
        Err(other) => panic!("unexpected pull error: {:?}", other),
    };
    let _ = rem_a.pull(&mut c_a).await.unwrap(); // catch up after restart, no-op if already.

    // Consumer B: pull from rem_b normally (no retention).
    let mut c_b =
        Steward::create_with_options(dir.path().join("c_b"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let _ = rem_b.pull(&mut c_b).await.unwrap();

    let cs_a = c_a.compute_live_checksums(c_a.store_id()).await.unwrap();
    let cs_b = c_b.compute_live_checksums(c_b.store_id()).await.unwrap();
    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
    assert!(
        checksums_equal(&cs_a, &cs_source),
        "consumer A (post-restart from rem_a) matches source"
    );
    assert!(
        checksums_equal(&cs_b, &cs_source),
        "consumer B (from rem_b) matches source"
    );
    assert!(
        checksums_equal(&cs_a, &cs_b),
        "both consumers byte-equal despite divergent remote retention"
    );
}

#[tokio::test]
async fn consumer_can_pull_from_second_remote_with_same_store_id() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut rem_a = Remote::create(dir.path().join("rem_a"), source.store_id())
        .await
        .unwrap();
    let mut rem_b = Remote::create(dir.path().join("rem_b"), source.store_id())
        .await
        .unwrap();

    // Push different bundles to each remote (but using the same source).
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k1", b"v1".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        rem_a.push(&mut source, outcome.txn_seq).await.unwrap();
        rem_b.push(&mut source, outcome.txn_seq).await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k2", b"v2".to_vec()).unwrap();
        let outcome = g.commit().await.unwrap();
        rem_a.push(&mut source, outcome.txn_seq).await.unwrap();
        // intentionally NOT pushed to rem_b yet.
    }

    // Consumer pulls from rem_a (gets both bundles).
    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let _ = rem_a.pull(&mut consumer).await.unwrap();
    assert_eq!(consumer.last_committed_seq().await.unwrap(), 2);

    // Now rem_b catches up.
    rem_b.push(&mut source, 2).await.unwrap();

    // Consumer pulls from rem_b.  rem_b's last_pulled_seq tracking
    // is independent (different setting key), so consumer thinks
    // rem_b is fresh -- but apply_pulled_bundle's idempotence on
    // existing DataCommitted records means the same bundles aren't
    // re-applied; only any net-new bundles would land.
    let report = remote_pull_safe(&rem_b, &mut consumer).await;
    // Either ok (no new bundles applied because consumer already
    // has them) or BehindRetention (depending on remote state).
    // In this scenario, rem_b has bundles 1, 2; consumer's local
    // last_pulled_seq:rem_b is 0; pull will see bundles 1 and 2 as
    // pending; for each, apply_pulled_bundle short-circuits because
    // DataCommitted at seq 1 and 2 already exist on consumer.
    assert!(report.is_ok(), "pull from second remote: {:?}", report);

    // Final state: consumer still has source's data.
    let r = consumer.begin_read().await.unwrap();
    assert_eq!(r.get("p", "k1").await.unwrap(), Some(b"v1".to_vec()));
    assert_eq!(r.get("p", "k2").await.unwrap(), Some(b"v2".to_vec()));
}

async fn remote_pull_safe(
    remote: &Remote,
    consumer: &mut Steward,
) -> std::result::Result<sandbox_remote::PullReport, sandbox_remote::RemoteError> {
    remote.pull(consumer).await
}

// ============================================================================
// Group E: Edge mixtures
// ============================================================================

#[tokio::test]
async fn abort_and_noop_writes_interleaved_with_real_writes() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();

    // Mix: real, abort, noop, real, real, abort, real.
    // seqs: 1=real, 2=abort, 3=noop, 4=real, 5=real, 6=abort, 7=real
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k1", b"v1".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k_abort", b"x".to_vec()).unwrap();
        let _ = g.abort("intentional").await.unwrap();
    }
    {
        let g = source.begin_write().await.unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k4", b"v4".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k5", b"v5".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k_abort2", b"x".to_vec()).unwrap();
        let _ = g.abort("intentional").await.unwrap();
    }
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k7", b"v7".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    // Push all DataCommitted seqs (push_all_pending skips Failed/Completed).
    push_all_pending(&mut source, &mut remote).await;

    // Consumer pulls.
    let mut consumer =
        Steward::create_with_options(dir.path().join("consumer"), opts_with_id(source.store_id()))
            .await
            .unwrap();
    let _ = remote.pull(&mut consumer).await.unwrap();

    // Consumer has only the real values; the aborted/noop seqs are absent.
    let r = consumer.begin_read().await.unwrap();
    assert_eq!(r.get("p", "k1").await.unwrap(), Some(b"v1".to_vec()));
    assert_eq!(r.get("p", "k4").await.unwrap(), Some(b"v4".to_vec()));
    assert_eq!(r.get("p", "k5").await.unwrap(), Some(b"v5".to_vec()));
    assert_eq!(r.get("p", "k7").await.unwrap(), Some(b"v7".to_vec()));
    assert_eq!(r.get("p", "k_abort").await.unwrap(), None);
    assert_eq!(r.get("p", "k_abort2").await.unwrap(), None);
    drop(r);

    let v = verify_against_remote(&remote, &consumer).await.unwrap();
    assert!(
        v.ok,
        "consumer matches remote despite abort/noop interleaved"
    );
}

#[tokio::test]
async fn source_verify_local_passes_after_each_op_in_a_long_sequence() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();

    // Write, verify_local, compact, verify_local, write, verify_local...
    for cycle in 0..5 {
        for i in 0..2 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}_{}", cycle, i), b"v".to_vec())
                .unwrap();
            let _ = g.commit().await.unwrap();
            let report = sandbox_steward::verify_local(&source).await.unwrap();
            assert!(
                report.ok,
                "verify_local fails after write cycle {}.{}",
                cycle, i
            );
        }
        let _ = source.compact(None).await.unwrap();
        let report = sandbox_steward::verify_local(&source).await.unwrap();
        assert!(
            report.ok,
            "verify_local fails after compact cycle {}",
            cycle
        );
    }
}

// ============================================================================
// Group F: Per-checkpoint invariants
// ============================================================================

#[tokio::test]
async fn verify_invariants_at_every_step_of_a_long_sequence() {
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

    // 5 cycles: write, push, pull, verify (both kinds).
    for i in 0..5 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let outcome = g.commit().await.unwrap();
        remote.push(&mut source, outcome.txn_seq).await.unwrap();
        let _ = remote.pull(&mut consumer).await.unwrap();

        let local_src = sandbox_steward::verify_local(&source).await.unwrap();
        assert!(local_src.ok, "step {}: source verify_local", i);

        let local_cons = sandbox_steward::verify_local(&consumer).await.unwrap();
        assert!(local_cons.ok, "step {}: consumer verify_local", i);

        let against = verify_against_remote(&remote, &consumer).await.unwrap();
        assert!(
            against.ok,
            "step {}: consumer verify_against_remote: {:?}",
            i, against.mismatches
        );
    }
}

// ============================================================================
// Group G: Deterministic stress
// ============================================================================

/// Run a scripted ~25-operation sequence covering the full action
/// matrix.  At every checkpoint, assert end-to-end invariants:
///   - source verify_local ok
///   - consumer verify_local ok (when consumer exists)
///   - consumer verify_against_remote ok (when consumer is current)
///
/// This is the broadest single test in the suite.  If any future
/// change breaks the design's contract under this sequence, it will
/// fail visibly.
#[tokio::test]
async fn scripted_25_operation_stress_invariants_hold_throughout() {
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

    // Check helper.
    async fn check(source: &Steward, consumer: &Steward, remote: &Remote, step: usize) {
        let r = sandbox_steward::verify_local(source).await.unwrap();
        assert!(r.ok, "step {}: source verify_local", step);
        let r = sandbox_steward::verify_local(consumer).await.unwrap();
        assert!(r.ok, "step {}: consumer verify_local", step);
        let v = verify_against_remote(remote, consumer).await.unwrap();
        assert!(
            v.ok,
            "step {}: consumer verify_against_remote: {:?}",
            step, v.mismatches
        );
    }

    let mut step = 0usize;

    // 1-3: writes.
    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p1",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
        step += 1;
    }
    // 4: push all + pull + check.
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 5-7: more writes on a different partition.
    for i in 0..3 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p2",
            &format!("k{}", i),
            format!("u{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
        step += 1;
    }
    // 8: push individual seqs + pull.
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 9: aborted write.
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p1", "trash", b"trash".to_vec()).unwrap();
        let _ = g.abort("intentional").await.unwrap();
    }
    step += 1;
    // No state should change.
    check(&source, &consumer, &remote, step).await;

    // 10-11: more writes on p1.
    for i in 3..5 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p1",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
        step += 1;
    }
    // 12: compact.
    let outcome = source.compact(None).await.unwrap();
    step += 1;
    let _ = sandbox_steward::verify_local(&source).await.unwrap();
    // 13: push + pull + check.
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 14-15: more writes.
    for i in 5..7 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p1",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
        step += 1;
    }
    // 16: another compact.
    let _ = source.compact(None).await.unwrap();
    step += 1;
    // 17: push + pull + check.
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 18-19: a no-op write and a write.
    {
        let g = source.begin_write().await.unwrap();
        let _ = g.commit().await.unwrap(); // Completed (no-op)
    }
    step += 1;
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p2", "final", b"final".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    step += 1;
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 20: maintain (if there are >= 2 compacts; otherwise N=1 is safe).
    let compacts_so_far = remote
        .list_bundles()
        .await
        .unwrap()
        .iter()
        .filter(|b| matches!(b.commit_kind, CommitKind::Compact))
        .count();
    if compacts_so_far >= 1 {
        let _ = remote
            .maintain(MaintainOptions {
                keep_compact_bundles: 1,
                vacuum_after: false,
            })
            .await
            .unwrap();
    }
    step += 1;

    // 21: another consumer pull (idempotent at this point).
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 22-23: a couple more writes after maintain.
    for i in 7..9 {
        let mut g = source.begin_write().await.unwrap();
        g.put(
            "p1",
            &format!("k{}", i),
            format!("v{}", i).as_bytes().to_vec(),
        )
        .unwrap();
        let _ = g.commit().await.unwrap();
        step += 1;
    }
    // 24: push + pull + final check.
    push_all_pending(&mut source, &mut remote).await;
    let _ = remote.pull(&mut consumer).await.unwrap();
    step += 1;
    check(&source, &consumer, &remote, step).await;

    // 25: Spot-check final reads.
    let r = consumer.begin_read().await.unwrap();
    assert_eq!(r.get("p1", "k0").await.unwrap(), Some(b"v0".to_vec()));
    assert_eq!(r.get("p1", "k8").await.unwrap(), Some(b"v8".to_vec()));
    assert_eq!(r.get("p2", "k0").await.unwrap(), Some(b"u0".to_vec()));
    assert_eq!(r.get("p2", "final").await.unwrap(), Some(b"final".to_vec()));
    assert_eq!(r.get("p1", "trash").await.unwrap(), None);

    // Suppress unused warnings.
    let _ = outcome;
}
