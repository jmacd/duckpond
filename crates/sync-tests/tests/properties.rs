// SPDX-License-Identifier: Apache-2.0

//! Property-based tests over random sequences of operations.
//!
//! The deterministic stress test in `integration.rs` exercises ONE
//! scripted long sequence.  These tests use proptest to sweep the
//! space of sequences and assert the design's invariants hold for
//! ANY well-formed input.
//!
//! Each test is heavy because it constructs Delta tables per case;
//! configured with a small `cases` count to keep wall-clock time
//! reasonable.
//!
//! KNOWN DESIGN SUBTLETY: every test case prepends a Write op so
//! `txn_seq=1` is always a pushable DataCommitted.  Without this,
//! a sequence starting with Abort or Noop would leave `txn_seq=1`
//! with no bundle on the remote (because Failed/Completed records
//! are not pushable), and a fresh consumer's pull would trigger
//! `BehindRetention` even though no actual retention pruning has
//! happened.  This is an over-conservative aspect of the current
//! BehindRetention check (it cannot distinguish "seq N never had a
//! bundle" from "seq N's bundle was pruned by retention").  The
//! prototype documents this as a known limitation; the prepended
//! Write keeps the property tests focused on behavior other than
//! this fresh-consumer-bootstrap edge case.

use std::sync::Arc;

use proptest::prelude::*;
use sync_remote::Remote;
use sync_steward::{CommitKind, RecordKind, Steward, StewardOptions};
use sync_store::checksum::Merkle;
use tempfile::TempDir;
use uuid::Uuid;

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

#[derive(Debug, Clone)]
enum Op {
    Write { key: u8, value: u8 },
    AbortWrite { key: u8 },
    NoopWrite,
    Compact,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u8..10, 0u8..255).prop_map(|(k, v)| Op::Write { key: k, value: v }),
        1 => (0u8..10).prop_map(|k| Op::AbortWrite { key: k }),
        1 => Just(Op::NoopWrite),
        1 => Just(Op::Compact),
    ]
}

async fn apply_op(source: &mut Steward, op: &Op) {
    match op {
        Op::Write { key, value } => {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", key), vec![*value]).unwrap();
            let _ = g.commit().await.unwrap();
        }
        Op::AbortWrite { key } => {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", key), b"x".to_vec()).unwrap();
            let _ = g.abort("intentional").await.unwrap();
        }
        Op::NoopWrite => {
            let g = source.begin_write().await.unwrap();
            let _ = g.commit().await.unwrap();
        }
        Op::Compact => {
            let _ = source.compact(None).await.unwrap();
        }
    }
}

async fn push_pending(source: &mut Steward, remote: &mut Remote) {
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

/// Read every key that has ever been written to source and compare
/// to consumer.  Source is the ground truth.
async fn consumer_matches_source_for_every_key(
    source: &Steward,
    consumer: &Steward,
) -> Result<(), String> {
    let cs_source = source
        .compute_live_checksums(source.store_id())
        .await
        .unwrap();
    let cs_consumer = consumer
        .compute_live_checksums(consumer.store_id())
        .await
        .unwrap();
    if cs_source != cs_consumer {
        return Err(format!(
            "live checksums differ: source = {:?}, consumer = {:?}",
            cs_source, cs_consumer
        ));
    }
    // Spot-check by reading every key 0..10.
    let r_s = source.begin_read().await.unwrap();
    let r_c = consumer.begin_read().await.unwrap();
    for k in 0..10u8 {
        let key = format!("k{}", k);
        let from_source = r_s.get("p", &key).await.unwrap();
        let from_consumer = r_c.get("p", &key).await.unwrap();
        if from_source != from_consumer {
            return Err(format!(
                "key `{}`: source = {:?}, consumer = {:?}",
                key, from_source, from_consumer
            ));
        }
    }
    Ok(())
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

/// Prepend a fixed Write so seq 1 is always a pushable DataCommitted.
/// See module docs for why.
fn with_seed(rest: Vec<Op>) -> Vec<Op> {
    let mut out = vec![Op::Write { key: 0, value: 0 }];
    out.extend(rest);
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        max_shrink_iters: 32,
        ..ProptestConfig::default()
    })]

    /// PROPERTY: for any sequence of operations applied to the source,
    /// after pushing all DataCommitted seqs and pulling on the
    /// consumer, the consumer's logical state matches the source's.
    #[test]
    fn prop_consumer_matches_source_after_push_pull(
        rest in proptest::collection::vec(op_strategy(), 0..15)
    ) {
        let ops = with_seed(rest);
        block_on(async move {
            let dir = TempDir::new().unwrap();
            let mut source = Steward::create_with_options(dir.path().join("source"), opts())
                .await
                .unwrap();
            let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
                .await
                .unwrap();
            let mut consumer = Steward::create_with_options(
                dir.path().join("consumer"),
                opts_with_id(source.store_id()),
            )
            .await
            .unwrap();

            for op in &ops {
                apply_op(&mut source, op).await;
                push_pending(&mut source, &mut remote).await;
            }

            let _ = remote.pull(&mut consumer).await.unwrap();
            consumer_matches_source_for_every_key(&source, &consumer)
                .await
                .unwrap();
        });
    }

    /// PROPERTY: compaction preserves logical content.  After any
    /// sequence of writes + compact, every key returns the same
    /// value as before the compact.
    #[test]
    fn prop_compact_preserves_logical_content(
        writes in proptest::collection::vec(
            (0u8..10, 0u8..255).prop_map(|(k, v)| Op::Write { key: k, value: v }),
            1..15,
        )
    ) {
        block_on(async move {
            let dir = TempDir::new().unwrap();
            let mut source = Steward::create_with_options(dir.path(), opts())
                .await
                .unwrap();

            for op in &writes {
                apply_op(&mut source, op).await;
            }

            let mut pre: Vec<(u8, Option<Vec<u8>>)> = Vec::new();
            {
                let r = source.begin_read().await.unwrap();
                for k in 0..10u8 {
                    pre.push((k, r.get("p", &format!("k{}", k)).await.unwrap()));
                }
            }

            let _ = source.compact(None).await.unwrap();

            let r = source.begin_read().await.unwrap();
            for (k, expected) in &pre {
                let actual = r.get("p", &format!("k{}", k)).await.unwrap();
                prop_assert_eq!(
                    actual,
                    expected.clone(),
                    "compact changed value for key k{}",
                    k
                );
            }
            Ok(())
        }).unwrap();
    }

    /// PROPERTY: a txn_seq is pushable iff a DataCommitted record
    /// exists for it.  Failed (aborted), Completed (no-op write),
    /// no-op compact -- all of these allocate a seq but produce no
    /// DataCommitted, so push must reject them with NoSuchCommit.
    #[test]
    fn prop_only_data_committed_seqs_are_pushable(
        ops in proptest::collection::vec(op_strategy(), 1..12)
    ) {
        block_on(async move {
            let dir = TempDir::new().unwrap();
            let mut source = Steward::create_with_options(dir.path().join("source"), opts())
                .await
                .unwrap();
            let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
                .await
                .unwrap();

            for op in &ops {
                apply_op(&mut source, op).await;
            }

            let last = source.last_write_seq();
            for seq in 1..=last {
                let has_dc = source
                    .data_committed_record(source.store_id(), seq)
                    .await
                    .unwrap()
                    .is_some();
                let result = remote.push(&mut source, seq).await;
                if has_dc {
                    prop_assert!(
                        result.is_ok(),
                        "push of seq {} (DataCommitted) failed: {:?}",
                        seq,
                        result
                    );
                } else {
                    match result {
                        Err(sync_remote::RemoteError::NoSuchCommit(s)) => {
                            prop_assert_eq!(s, seq);
                        }
                        other => prop_assert!(
                            false,
                            "push of seq {} (not DataCommitted) returned {:?}, expected NoSuchCommit",
                            seq,
                            other
                        ),
                    }
                }
            }
            Ok(())
        }).unwrap();
    }

    /// PROPERTY: pull is idempotent.  A second pull immediately after
    /// the first applies no new bundles.
    #[test]
    fn prop_pull_is_idempotent(
        rest in proptest::collection::vec(op_strategy(), 0..12)
    ) {
        let ops = with_seed(rest);
        block_on(async move {
            let dir = TempDir::new().unwrap();
            let mut source = Steward::create_with_options(dir.path().join("source"), opts())
                .await
                .unwrap();
            let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
                .await
                .unwrap();
            let mut consumer = Steward::create_with_options(
                dir.path().join("consumer"),
                opts_with_id(source.store_id()),
            )
            .await
            .unwrap();

            for op in &ops {
                apply_op(&mut source, op).await;
                push_pending(&mut source, &mut remote).await;
            }

            let _r1 = remote.pull(&mut consumer).await.unwrap();
            let r2 = remote.pull(&mut consumer).await.unwrap();
            prop_assert!(
                r2.bundles_applied.is_empty(),
                "second pull applied {} bundles, expected 0",
                r2.bundles_applied.len()
            );
            Ok(())
        }).unwrap();
    }

    /// PROPERTY: at every prefix of any random sequence, source's
    /// verify_local passes; and after pushing+pulling the entire
    /// sequence, consumer's verify_local AND consumer matches source.
    #[test]
    fn prop_invariants_hold_throughout_random_long_sequence(
        rest in proptest::collection::vec(op_strategy(), 4..19)
    ) {
        let ops = with_seed(rest);
        block_on(async move {
            let dir = TempDir::new().unwrap();
            let mut source = Steward::create_with_options(dir.path().join("source"), opts())
                .await
                .unwrap();
            let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
                .await
                .unwrap();
            let mut consumer = Steward::create_with_options(
                dir.path().join("consumer"),
                opts_with_id(source.store_id()),
            )
            .await
            .unwrap();

            for (i, op) in ops.iter().enumerate() {
                apply_op(&mut source, op).await;
                let report = sync_steward::verify_local(&source).await.unwrap();
                prop_assert!(
                    report.ok,
                    "source verify_local failed after op {}: {:?}",
                    i,
                    report.mismatches
                );
                push_pending(&mut source, &mut remote).await;
            }

            let _ = remote.pull(&mut consumer).await.unwrap();
            let report = sync_steward::verify_local(&consumer).await.unwrap();
            prop_assert!(report.ok, "consumer verify_local failed after pull");

            consumer_matches_source_for_every_key(&source, &consumer)
                .await
                .map_err(|e| TestCaseError::fail(e))?;
            Ok(())
        }).unwrap();
    }
}

#[cfg(test)]
mod silence_unused {
    use super::*;
    #[allow(dead_code)]
    fn _silence() {
        let _ = RecordKind::Begin;
        let _ = CommitKind::Write;
    }
}
