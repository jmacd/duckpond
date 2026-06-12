// SPDX-License-Identifier: Apache-2.0

//! Bounded-growth property tests.
//!
//! The single most important correctness property of this design:
//! with periodic `compact + vacuum` on the source AND `maintain` on
//! the remote, every component reaches a bounded steady-state on
//! disk.  No ever-increasing parquet counts, no ever-increasing data
//! sizes, no commit log that grows without limit.
//!
//! These tests measure on-disk footprint at every cycle of a long
//! sequence and assert that the steady-state size is bounded.
//!
//! IMPORTANT ORDERING CONSTRAINT (DESIGN.md sec 2.6 rule #4): push
//! must complete for a given txn_seq BEFORE the source vacuums the
//! parquet files referenced by that commit.  Tests below enforce this
//! by always pushing immediately after every commit, then vacuuming
//! only after pushes have captured the data.

use std::path::Path;
use std::sync::Arc;

use sync_remote::{MaintainOptions, Remote};
use sync_steward::{Steward, StewardOptions};
use sync_store::checksum::Merkle;
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

fn dir_size(p: &Path) -> u64 {
    if !p.exists() {
        return 0;
    }
    if p.is_file() {
        return std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    }
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(p) {
        for entry in entries.flatten() {
            total += dir_size(&entry.path());
        }
    }
    total
}

fn parquet_count(p: &Path) -> usize {
    if !p.exists() {
        return 0;
    }
    if p.is_file() {
        return if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            1
        } else {
            0
        };
    }
    let mut total = 0usize;
    if let Ok(entries) = std::fs::read_dir(p) {
        for entry in entries.flatten() {
            total += parquet_count(&entry.path());
        }
    }
    total
}

fn big_value(seed: u8) -> Vec<u8> {
    (0..8192u32)
        .map(|i| ((i + seed as u32) % 256) as u8)
        .collect()
}

#[tokio::test]
async fn source_data_dir_size_steady_state_under_periodic_compact_and_vacuum() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let data_dir = dir.path().join("source").join("data");

    const K: usize = 10;
    const CYCLES: usize = 15;

    let mut sizes_per_cycle: Vec<u64> = Vec::with_capacity(CYCLES);
    for cycle in 0..CYCLES {
        for k in 0..K {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", k), big_value(cycle as u8))
                .unwrap();
            let _ = g.commit().await.unwrap();
        }
        if cycle % 3 == 2 {
            let _ = source.compact(None).await.unwrap();
            let _ = source.vacuum().await.unwrap();
        }
        sizes_per_cycle.push(dir_size(&data_dir));
    }

    let early_max = *sizes_per_cycle[..5].iter().max().unwrap();
    let late_max = *sizes_per_cycle[10..].iter().max().unwrap();
    assert!(
        late_max <= early_max.saturating_mul(4),
        "data dir size did not reach steady state: early_max = {} bytes, \
         late_max = {} bytes, ratio = {:.2}.  Sizes per cycle: {:?}",
        early_max,
        late_max,
        late_max as f64 / early_max as f64,
        sizes_per_cycle,
    );
}

#[tokio::test]
async fn source_data_dir_grows_significantly_without_compact_and_vacuum_control() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let data_dir = dir.path().join("source").join("data");

    const K: usize = 10;
    const CYCLES: usize = 25;

    let mut sizes_per_cycle: Vec<u64> = Vec::with_capacity(CYCLES);
    for cycle in 0..CYCLES {
        for k in 0..K {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", k), big_value(cycle as u8))
                .unwrap();
            let _ = g.commit().await.unwrap();
        }
        sizes_per_cycle.push(dir_size(&data_dir));
    }

    let early_max = *sizes_per_cycle[..5].iter().max().unwrap();
    let late_max = *sizes_per_cycle[20..].iter().max().unwrap();
    // Without maintenance, growth is roughly linear in the number of
    // commits.  25 cycles vs 5 cycles -> ~5x more commits -> ratio
    // close to 5.  Set the assertion at 4x to be reliable.
    assert!(
        late_max > early_max.saturating_mul(4),
        "expected significant unbounded growth without compact+vacuum, but \
         late_max = {} only {:.2}x larger than early_max = {}",
        late_max,
        late_max as f64 / early_max as f64,
        early_max,
    );
}

#[tokio::test]
async fn parquet_count_bounded_after_periodic_compact_and_vacuum() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let data_dir = dir.path().join("source").join("data");

    const CYCLES: usize = 12;

    let mut counts: Vec<usize> = Vec::with_capacity(CYCLES);
    for cycle in 0..CYCLES {
        for k in 0..5 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", k), big_value(cycle as u8))
                .unwrap();
            let _ = g.commit().await.unwrap();
        }
        if cycle % 2 == 1 {
            let _ = source.compact(None).await.unwrap();
            let _ = source.vacuum().await.unwrap();
        }
        counts.push(parquet_count(&data_dir));
    }

    let late_max = *counts[8..].iter().max().unwrap();
    assert!(
        late_max < 30,
        "parquet count not bounded: counts per cycle = {:?}",
        counts
    );
}

#[tokio::test]
async fn remote_dir_size_steady_state_under_periodic_maintain() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), source.store_id())
        .await
        .unwrap();
    let remote_dir = dir.path().join("remote");

    const CYCLES: usize = 10;

    let mut sizes_per_cycle: Vec<u64> = Vec::with_capacity(CYCLES);
    for cycle in 0..CYCLES {
        // Each cycle: 3 writes, push each immediately (preserves
        // DESIGN.md sec 2.6 rule #4 ordering).
        for k in 0..3 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", k), big_value(cycle as u8))
                .unwrap();
            let outcome = g.commit().await.unwrap();
            remote.push(&mut source, outcome.txn_seq).await.unwrap();
        }
        // Compact + push the compact bundle BEFORE vacuum.
        let outcome = source.compact(None).await.unwrap();
        if outcome.had_data {
            remote.push(&mut source, outcome.txn_seq).await.unwrap();
        }
        // Now safe to vacuum source.
        let _ = source.vacuum().await.unwrap();
        // Remote maintenance.
        let compacts = remote
            .list_bundles()
            .await
            .unwrap()
            .iter()
            .filter(|b| matches!(b.commit_kind, sync_steward::CommitKind::Compact))
            .count();
        if compacts >= 1 {
            let _ = remote
                .maintain(MaintainOptions {
                    keep_compact_bundles: 1,
                    vacuum_after: true,
                })
                .await
                .unwrap();
        }
        sizes_per_cycle.push(dir_size(&remote_dir));
    }

    let early_max = *sizes_per_cycle[..3].iter().max().unwrap();
    let late_max = *sizes_per_cycle[7..].iter().max().unwrap();
    assert!(
        late_max <= early_max.saturating_mul(5),
        "remote dir did not reach steady state: early_max = {}, \
         late_max = {}, ratio = {:.2}.  Sizes: {:?}",
        early_max,
        late_max,
        late_max as f64 / early_max as f64,
        sizes_per_cycle,
    );
}

#[tokio::test]
async fn end_to_end_bounded_long_run_keeps_all_components_bounded() {
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

    let source_dir = dir.path().join("source").join("data");
    let remote_dir = dir.path().join("remote");
    let consumer_dir = dir.path().join("consumer").join("data");

    const K: usize = 8;
    const CYCLES: usize = 12;

    let mut source_sizes: Vec<u64> = Vec::new();
    let mut remote_sizes: Vec<u64> = Vec::new();
    let mut consumer_sizes: Vec<u64> = Vec::new();

    for cycle in 0..CYCLES {
        // Writes + immediate pushes (preserves the ordering invariant).
        for k in 0..K {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}", k), big_value(cycle as u8))
                .unwrap();
            let outcome = g.commit().await.unwrap();
            remote.push(&mut source, outcome.txn_seq).await.unwrap();
        }

        // Source maintenance every 2 cycles.
        if cycle % 2 == 1 {
            let outcome = source.compact(None).await.unwrap();
            if outcome.had_data {
                remote.push(&mut source, outcome.txn_seq).await.unwrap();
            }
            let _ = source.vacuum().await.unwrap();
        }

        // Remote maintenance every 3 cycles.
        if cycle % 3 == 2 {
            let compacts = remote
                .list_bundles()
                .await
                .unwrap()
                .iter()
                .filter(|b| matches!(b.commit_kind, sync_steward::CommitKind::Compact))
                .count();
            if compacts >= 1 {
                let _ = remote
                    .maintain(MaintainOptions {
                        keep_compact_bundles: 1,
                        vacuum_after: true,
                    })
                    .await
                    .unwrap();
            }
        }

        // Consumer pull (handle BehindRetention via restart if needed).
        match remote.pull(&mut consumer).await {
            Ok(_) => {}
            Err(sync_remote::RemoteError::BehindRetention { .. }) => {
                let path = dir.path().join("consumer");
                drop(consumer);
                consumer = remote.restart_from_compact(&path).await.unwrap();
            }
            Err(other) => panic!("unexpected pull error at cycle {}: {:?}", cycle, other),
        }

        {
            let r = consumer.begin_read().await.unwrap();
            for k in 0..K {
                assert_eq!(
                    r.get("p", &format!("k{}", k)).await.unwrap(),
                    Some(big_value(cycle as u8)),
                    "consumer at cycle {} has correct k{}",
                    cycle,
                    k
                );
            }
        }

        source_sizes.push(dir_size(&source_dir));
        remote_sizes.push(dir_size(&remote_dir));
        consumer_sizes.push(dir_size(&consumer_dir));
    }

    let bounded = |name: &str, sizes: &[u64]| {
        let early_max = *sizes[..3].iter().max().unwrap();
        let late_max = *sizes[8..].iter().max().unwrap();
        assert!(
            late_max <= early_max.saturating_mul(5),
            "{} did not reach steady state: early_max = {}, \
             late_max = {}, ratio = {:.2}.  Sizes: {:?}",
            name,
            early_max,
            late_max,
            late_max as f64 / early_max as f64,
            sizes,
        );
    };
    bounded("source data dir", &source_sizes);
    bounded("remote dir", &remote_sizes);
    bounded("consumer data dir", &consumer_sizes);
}

// ============================================================================
// Control-table bounded growth (the third leg).
//
// Every begin_write/commit/abort/compact/push/config_set adds one
// or more records to the steward's control table.  Without periodic
// compact_control + vacuum_control, the control table grows
// unboundedly even if the data store is well-maintained.
// ============================================================================

#[tokio::test]
async fn control_table_size_steady_state_under_periodic_compact_and_vacuum() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let control_dir = dir.path().join("source").join("control");

    // Each cycle: 5 writes plus a config_set bump.  Don't touch the
    // data store's compact/vacuum so the control table is the only
    // moving part.
    const CYCLES: usize = 20;
    let mut sizes_per_cycle: Vec<u64> = Vec::with_capacity(CYCLES);

    for cycle in 0..CYCLES {
        for k in 0..5 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}_{}", cycle, k), b"v".to_vec())
                .unwrap();
            let _ = g.commit().await.unwrap();
        }
        // Bump a setting too (latest-write-wins; old setting records
        // are pure history once superseded).
        source
            .config_set("counter", &cycle.to_string())
            .await
            .unwrap();

        // Compact + vacuum the control table every 3 cycles.
        if cycle % 3 == 2 {
            let _ = source.compact_control().await.unwrap();
            let _ = source.vacuum_control().await.unwrap();
        }
        sizes_per_cycle.push(dir_size(&control_dir));
    }

    let early_max = *sizes_per_cycle[..5].iter().max().unwrap();
    let late_max = *sizes_per_cycle[15..].iter().max().unwrap();
    // The control table grows because each commit writes a
    // DataCommitted record; compact merges parquets, vacuum
    // reclaims tombstoned files.  delta-rs's compact has minimum-
    // size heuristics that may leave some growth visible for tiny
    // test data, so we use a generous 4x bound -- matches the
    // data-dir test.
    assert!(
        late_max <= early_max.saturating_mul(4),
        "control table did not reach steady state: early_max = {} bytes, \
         late_max = {} bytes, ratio = {:.2}.  Sizes per cycle: {:?}",
        early_max,
        late_max,
        late_max as f64 / early_max as f64,
        sizes_per_cycle,
    );
}

#[tokio::test]
async fn control_table_grows_significantly_without_compact_and_vacuum_control() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let control_dir = dir.path().join("source").join("control");

    const CYCLES: usize = 25;
    let mut sizes_per_cycle: Vec<u64> = Vec::with_capacity(CYCLES);
    for cycle in 0..CYCLES {
        for k in 0..5 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}_{}", cycle, k), b"v".to_vec())
                .unwrap();
            let _ = g.commit().await.unwrap();
        }
        source
            .config_set("counter", &cycle.to_string())
            .await
            .unwrap();
        sizes_per_cycle.push(dir_size(&control_dir));
    }

    let early_max = *sizes_per_cycle[..5].iter().max().unwrap();
    let late_max = *sizes_per_cycle[20..].iter().max().unwrap();
    assert!(
        late_max > early_max.saturating_mul(4),
        "expected significant unbounded control-table growth without \
         compact_control + vacuum_control, but late_max = {} only \
         {:.2}x larger than early_max = {}",
        late_max,
        late_max as f64 / early_max as f64,
        early_max,
    );
}

#[tokio::test]
async fn control_table_parquet_count_bounded_under_periodic_maintenance() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("source"), opts())
        .await
        .unwrap();
    let control_dir = dir.path().join("source").join("control");

    const CYCLES: usize = 12;
    let mut counts: Vec<usize> = Vec::with_capacity(CYCLES);

    for cycle in 0..CYCLES {
        for k in 0..3 {
            let mut g = source.begin_write().await.unwrap();
            g.put("p", &format!("k{}_{}", cycle, k), b"v".to_vec())
                .unwrap();
            let _ = g.commit().await.unwrap();
        }
        if cycle % 2 == 1 {
            let _ = source.compact_control().await.unwrap();
            let _ = source.vacuum_control().await.unwrap();
        }
        counts.push(parquet_count(&control_dir));
    }

    let late_max = *counts[8..].iter().max().unwrap();
    // 12 cycles * 3 commits + a few infrastructure records.  Without
    // maintenance, ~36 parquets would accumulate.  With it, capped.
    assert!(
        late_max < 30,
        "control table parquet count not bounded: counts = {:?}",
        counts
    );
}
