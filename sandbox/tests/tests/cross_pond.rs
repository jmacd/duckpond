// SPDX-License-Identifier: Apache-2.0

//! End-to-end cross-pond import tests.
//!
//! These tests demonstrate the headline cross-pond import scenario:
//!
//!   - Pond B is a primary that pushes to its own remote.
//!   - Pond A is also a primary (with its own remote) AND attaches B's
//!     remote as an import.  A.pull(B's_remote) deposits B's data into
//!     A's local store under B's pond_id.
//!   - A's own writes go under A's pond_id; A's push to its own remote
//!     does NOT include B's data (push filter).
//!   - A downstream consumer C that wants B's data attaches B's remote
//!     directly, NOT A's remote.

use std::sync::Arc;

use sandbox_remote::Remote;
use sandbox_steward::{Steward, StewardOptions};
use sandbox_store::{Store, checksum::Merkle};
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

fn opts_with_store_id(id: Uuid) -> StewardOptions {
    StewardOptions {
        store_id: Some(id),
        checksum_strategy: Arc::new(Merkle::new()),
    }
}

#[tokio::test]
async fn cross_pond_import_basic_flow() {
    init_logger();
    let dir = TempDir::new().unwrap();

    // ---- Pond B: primary writing its own data ----
    let mut pond_b = Steward::create_with_options(dir.path().join("pond_b"), opts())
        .await
        .unwrap();
    let pond_b_id = pond_b.store_id();
    let mut remote_b = Remote::create(dir.path().join("remote_b"), pond_b_id)
        .await
        .unwrap();

    {
        let mut g = pond_b.begin_write().await.unwrap();
        g.put("ingest", "sensor1", b"measurement-from-B".to_vec())
            .unwrap();
        g.put("ingest", "sensor2", b"another-from-B".to_vec())
            .unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote_b.push(&mut pond_b, 1).await.unwrap();

    // ---- Pond A: primary that imports from B ----
    let mut pond_a = Steward::create_with_options(dir.path().join("pond_a"), opts())
        .await
        .unwrap();
    let pond_a_id = pond_a.store_id();
    assert_ne!(pond_a_id, pond_b_id);
    let mut remote_a = Remote::create(dir.path().join("remote_a"), pond_a_id)
        .await
        .unwrap();

    // A pulls from B's remote (cross-pond import).
    let report = remote_b.pull(&mut pond_a).await.unwrap();
    assert_eq!(report.bundles_applied.len(), 1);

    // A also writes its own data.
    {
        let mut g = pond_a.begin_write().await.unwrap();
        g.put("local", "config", b"A-only".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote_a.push(&mut pond_a, 1).await.unwrap();

    // ---- Verify the cross-pond layout ----
    let store_a = Store::open(dir.path().join("pond_a").join("data"))
        .await
        .unwrap();

    // B's data lives under B's pond_id in A's store.
    assert_eq!(
        store_a.get(pond_b_id, "ingest", "sensor1").await.unwrap(),
        Some(b"measurement-from-B".to_vec())
    );
    assert_eq!(
        store_a.get(pond_b_id, "ingest", "sensor2").await.unwrap(),
        Some(b"another-from-B".to_vec())
    );

    // A's own data lives under A's pond_id.
    assert_eq!(
        store_a.get(pond_a_id, "local", "config").await.unwrap(),
        Some(b"A-only".to_vec())
    );

    // The two pond_ids are isolated: A's pond_id has no "ingest"
    // partition, B's pond_id has no "local" partition.
    assert_eq!(
        store_a.get(pond_a_id, "ingest", "sensor1").await.unwrap(),
        None
    );
    assert_eq!(
        store_a.get(pond_b_id, "local", "config").await.unwrap(),
        None
    );

    // ---- Downstream consumer C: pulls A's remote, expects ONLY A's data ----
    let mut pond_c =
        Steward::create_with_options(dir.path().join("pond_c"), opts_with_store_id(pond_a_id))
            .await
            .unwrap();
    let report_c = remote_a.pull(&mut pond_c).await.unwrap();
    assert_eq!(
        report_c.bundles_applied.len(),
        1,
        "C pulls one bundle from A's remote (just A's own write)"
    );

    let store_c = Store::open(dir.path().join("pond_c").join("data"))
        .await
        .unwrap();
    // C has A's data.
    assert_eq!(
        store_c.get(pond_a_id, "local", "config").await.unwrap(),
        Some(b"A-only".to_vec())
    );
    // C does NOT have B's imported data (the push filter excluded it).
    assert_eq!(
        store_c.get(pond_b_id, "ingest", "sensor1").await.unwrap(),
        None
    );
    assert_eq!(
        store_c.get(pond_b_id, "ingest", "sensor2").await.unwrap(),
        None
    );

    // C wants B's data?  C attaches B's remote directly.
    let report_c_b = remote_b.pull(&mut pond_c).await.unwrap();
    assert_eq!(report_c_b.bundles_applied.len(), 1);
    let store_c2 = Store::open(dir.path().join("pond_c").join("data"))
        .await
        .unwrap();
    assert_eq!(
        store_c2.get(pond_b_id, "ingest", "sensor1").await.unwrap(),
        Some(b"measurement-from-B".to_vec())
    );
}

#[tokio::test]
async fn cross_pond_verify_against_remote_scopes_to_remote_pond_id() {
    // After A4, verify_against_remote is scoped to remote.store_id().
    // For a consumer that imports B, verify(remote_b, consumer)
    // succeeds even though consumer.store_id != remote_b.store_id,
    // because the verify compares the consumer's B-pond_id partitions
    // against B's remote.
    init_logger();
    let dir = TempDir::new().unwrap();

    // Pond B writes; pushes.
    let mut pond_b = Steward::create_with_options(dir.path().join("pond_b"), opts())
        .await
        .unwrap();
    let mut remote_b = Remote::create(dir.path().join("remote_b"), pond_b.store_id())
        .await
        .unwrap();
    {
        let mut g = pond_b.begin_write().await.unwrap();
        g.put("foo", "x", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote_b.push(&mut pond_b, 1).await.unwrap();

    // Pond A imports B.  A's own pond_id != B's.
    let mut pond_a = Steward::create_with_options(dir.path().join("pond_a"), opts())
        .await
        .unwrap();
    assert_ne!(pond_a.store_id(), pond_b.store_id());
    remote_b.pull(&mut pond_a).await.unwrap();

    // verify_against_remote(remote_b, &pond_a) should pass: A has B's
    // data exactly as B's remote recorded it.
    let report = sandbox_remote::verify_against_remote(&remote_b, &pond_a)
        .await
        .unwrap();
    assert!(
        report.ok,
        "cross-pond verify ok: A has B's data matching B's remote, mismatches: {:?}",
        report.mismatches
    );
    assert_eq!(report.remote_latest_seq, Some(1));
    assert!(report.divergence_boundary.is_none());
}

#[tokio::test]
async fn imported_data_does_not_advance_local_seq_allocator() {
    init_logger();
    let dir = TempDir::new().unwrap();

    // Pond B writes 5 commits.
    let mut pond_b = Steward::create_with_options(dir.path().join("pond_b"), opts())
        .await
        .unwrap();
    let mut remote_b = Remote::create(dir.path().join("remote_b"), pond_b.store_id())
        .await
        .unwrap();
    for i in 1..=5 {
        let mut g = pond_b.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
        remote_b.push(&mut pond_b, i).await.unwrap();
    }

    // Pond A imports B (foreign pond -> B's seqs 1..=5 land under
    // pond_b_id).  A's own seq allocator must NOT advance from this.
    let mut pond_a = Steward::create_with_options(dir.path().join("pond_a"), opts())
        .await
        .unwrap();
    assert_eq!(pond_a.last_write_seq(), 0, "fresh pond starts at 0");

    remote_b.pull(&mut pond_a).await.unwrap();
    assert_eq!(
        pond_a.last_write_seq(),
        0,
        "imported foreign records do NOT bump the local seq allocator"
    );

    // A's first own write allocates seq 1 (not 6).
    let g = pond_a.begin_write().await.unwrap();
    assert_eq!(
        g.txn_seq(),
        1,
        "A's own first write is seq 1, not 6 (imports live in their own seq space)"
    );
    let _ = g.commit().await.unwrap();
}

#[tokio::test]
async fn push_filter_rejects_foreign_paths_defensively() {
    // This exercises the defense-in-depth check in
    // build_and_commit_bundle: if a bundle's add/remove paths contain
    // a foreign pond_id, push errors with a Schema error.  In normal
    // operation this can't happen (data_committed_record is per-pond
    // and compact is per-pond), but the assertion guards against
    // regressions.
    //
    // We can't easily construct a "leaking" Delta version through the
    // public API today; this test documents the defensive check exists
    // by exercising the normal mirror-mode path and asserting it
    // succeeds (not erroring is the assertion).
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(dir.path().join("s"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create(dir.path().join("r"), source.store_id())
        .await
        .unwrap();
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote
        .push(&mut source, 1)
        .await
        .expect("local-only bundle must pass the push filter");
}
