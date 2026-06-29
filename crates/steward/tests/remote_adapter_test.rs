// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `ShipRemoteSteward`: drive a source duckpond
//! through `sync_remote::Remote::push` into a `file://` remote, then
//! drive a destination duckpond through `Remote::pull`, and verify the
//! pulled file matches.
//!
//! These tests are end-to-end smoke tests for D4.2.  They cover the
//! happy path (push N transactions, pull into a fresh pond, read the
//! file) and the idempotent-push case (pushing the same seq twice is
//! a no-op).
//!
//! They use `file://` URLs (not S3), so no external services or
//! `deltalake_aws::register_handlers` setup are required.

use std::sync::Once;

use steward::{Ship, ShipRemoteSteward};
use sync_remote::Remote;
use tempfile::tempdir;
use tlogfs::PondUserMetadata;

static INIT_LOG: Once = Once::new();
fn init_log() {
    INIT_LOG.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

/// Push one write, pull into a fresh pond, verify the file appears.
#[tokio::test]
async fn ship_remote_push_pull_roundtrip() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // 1. Source pond + one write transaction.
    let mut src = Ship::create_pond(&src_path, "test-host")
        .await
        .expect("create src");
    let src_pond_id = src.control_table().pond_id_uuid();
    let src_pond_meta = src.control_table().pond_metadata().clone();

    src.write_transaction(&meta("write_test_file"), async |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(
            &root,
            "/hello.txt",
            b"hello from source",
        )
        .await?;
        Ok(())
    })
    .await
    .expect("write transaction");

    let txn_seq = src.last_write_seq();
    assert!(
        txn_seq >= 1,
        "write_transaction should advance last_write_seq"
    );

    // 2. Create a fresh file:// remote with the same store_id as source.
    let mut remote = Remote::create(&remote_path, src_pond_id)
        .await
        .expect("create remote");

    // 3. Push the source pond's txn_seq to the remote.
    {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote.push(&mut adapter, txn_seq).await.expect("push 1");
    }

    // 4. Idempotent push: pushing the same seq again is a no-op success.
    {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote
            .push(&mut adapter, txn_seq)
            .await
            .expect("push 2 (idempotent)");
    }

    // 5. Destination pond with the same identity as source.
    let mut dst = Ship::create_pond_for_restoration(&dst_path, src_pond_meta)
        .await
        .expect("create dst");

    // Bootstrap dst's last_pulled_seq watermark to skip the source's
    // unpushable pond_init transaction (seq=1, data_delta_version=0).
    // In production this is handled by `restart_from_compact`; the
    // smoke test seeds it manually.
    let setting_key = format!("last_pulled_seq:{}", remote.url());
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        sync_remote::RemoteSteward::config_set(&mut adapter, &setting_key, "1")
            .await
            .expect("seed last_pulled_seq");
    }

    // 6. Pull all bundles from the remote into dst.
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        let report = remote.pull(&mut adapter).await.expect("pull");
        assert_eq!(
            report.bundles_applied.len(),
            1,
            "pull should apply exactly the one pushed bundle"
        );
    }

    // 7. Read the file out of the destination pond.
    let tx = dst.begin_read(&meta("verify")).await.expect("begin_read");
    let root = tx.root().await.expect("dst root");
    let bytes = root
        .read_file_path_to_vec("/hello.txt")
        .await
        .expect("read /hello.txt from dst");
    assert_eq!(bytes, b"hello from source");
    let _ = tx.commit().await.expect("commit read");

    // 8. Cross-pond import is now PERMITTED (D5.7b.2).  Mount
    //    materialization is the responsibility of the CLI's
    //    `pond pull` flow; the adapter itself simply applies the
    //    foreign bundle into the local Delta table.  Positive
    //    cross-pond coverage lives in `crates/cmd/tests/test_remote_cli.rs::
    //    cross_pond_pull_materializes_mount_entry`.
}

/// Push two transactions, pull both, verify both files appear in the
/// destination in order.
#[tokio::test]
async fn ship_remote_push_pull_two_transactions() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    let mut src = Ship::create_pond(&src_path, "test-host")
        .await
        .expect("create src");
    let src_pond_id = src.control_table().pond_id_uuid();
    let src_pond_meta = src.control_table().pond_metadata().clone();

    // First transaction.
    src.write_transaction(&meta("write_one"), async |fs| {
        let root = fs.root().await?;
        let _ =
            tinyfs::async_helpers::convenience::create_file_path(&root, "/one.txt", b"one").await?;
        Ok(())
    })
    .await
    .expect("write one");
    let seq_one = src.last_write_seq();

    // Second transaction.
    src.write_transaction(&meta("write_two"), async |fs| {
        let root = fs.root().await?;
        let _ =
            tinyfs::async_helpers::convenience::create_file_path(&root, "/two.txt", b"two").await?;
        Ok(())
    })
    .await
    .expect("write two");
    let seq_two = src.last_write_seq();
    assert!(seq_two > seq_one, "second write should advance seq");

    // Push both.
    let mut remote = Remote::create(&remote_path, src_pond_id)
        .await
        .expect("create remote");
    {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote.push(&mut adapter, seq_one).await.expect("push one");
        remote.push(&mut adapter, seq_two).await.expect("push two");
    }

    // Pull both into a fresh destination pond.
    let mut dst = Ship::create_pond_for_restoration(&dst_path, src_pond_meta)
        .await
        .expect("create dst");
    let setting_key = format!("last_pulled_seq:{}", remote.url());
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        sync_remote::RemoteSteward::config_set(&mut adapter, &setting_key, "1")
            .await
            .expect("seed last_pulled_seq");
    }
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        let report = remote.pull(&mut adapter).await.expect("pull");
        assert_eq!(
            report.bundles_applied.len(),
            2,
            "pull should apply both bundles"
        );
    }

    // Verify both files are readable.
    let tx = dst.begin_read(&meta("verify")).await.expect("begin_read");
    let root = tx.root().await.expect("dst root");
    let one = root
        .read_file_path_to_vec("/one.txt")
        .await
        .expect("read /one.txt");
    let two = root
        .read_file_path_to_vec("/two.txt")
        .await
        .expect("read /two.txt");
    assert_eq!(one, b"one");
    assert_eq!(two, b"two");
    let _ = tx.commit().await.expect("commit read");

    // Verify dst's last_write_seq advanced via sync_last_write_seq.
    assert!(
        dst.last_write_seq() >= seq_two,
        "dst.last_write_seq should be advanced by apply_pulled_bundle (got {}, expected >= {})",
        dst.last_write_seq(),
        seq_two
    );
}

/// Pull when the remote is empty returns zero applied and the
/// destination pond remains pristine.
#[tokio::test]
async fn ship_remote_pull_empty_remote_is_noop() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // Create dst pond first so we have a pond_id to reuse for the remote.
    let mut dst = Ship::create_pond(&dst_path, "test-host")
        .await
        .expect("create dst");
    let dst_pond_id = dst.control_table().pond_id_uuid();

    let remote = Remote::create(&remote_path, dst_pond_id)
        .await
        .expect("create empty remote");

    let mut adapter = ShipRemoteSteward::new(&mut dst);
    let report = remote.pull(&mut adapter).await.expect("pull empty");
    assert_eq!(
        report.bundles_applied.len(),
        0,
        "empty remote should apply zero bundles"
    );
}

/// D5.3: `actions_at_version` filters Add/Remove file actions by the
/// requested pond_id, so cross-pond Delta commits (D5.7) only
/// contribute their local-pond rows to push bundles.  Today every
/// commit is single-pond, so we test the filter contract by asking for
/// a *different* (foreign) pond_id: the adapter must return empty
/// adds/removes even though the commit contains real local-pond files.
#[tokio::test]
async fn ship_remote_actions_at_version_filters_by_pond_id() {
    use sync_remote::RemoteSteward;
    use uuid::Uuid;

    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    // Create a pond and commit one write so a Delta commit exists.
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create");
    let local_pond_id = ship.control_table().pond_id_uuid();
    ship.write_transaction(&meta("seed_write"), async |fs| {
        let root = fs.root().await?;
        let _ =
            tinyfs::async_helpers::convenience::create_file_path(&root, "/d53.txt", b"hi").await?;
        Ok(())
    })
    .await
    .expect("write txn");

    let adapter = ShipRemoteSteward::new(&mut ship);

    // Find the Delta commit version of the just-finished transaction
    // (txn_seq == 1 is `pond init`; the write is txn_seq == 2).
    let dc = adapter
        .data_committed_record(local_pond_id, 2)
        .await
        .expect("data_committed_record")
        .expect("expected DataCommitted at txn_seq 2");
    let dc_meta: sync_steward::DataCommittedMetadata =
        serde_json::from_str(&dc.metadata_json).expect("metadata_json");
    let version = dc_meta.data_delta_version;
    assert!(version > 0, "expected non-zero data_delta_version");

    // Sanity: asking for the local pond_id returns the file(s) just
    // added in this commit.
    let (adds_local, _removes_local) = adapter
        .actions_at_version(local_pond_id, version)
        .await
        .expect("local-pond actions");
    assert!(
        !adds_local.is_empty(),
        "expected at least one Add for local pond at version {}",
        version
    );
    for add in &adds_local {
        let expected_prefix = format!("pond_id={}/", local_pond_id);
        assert!(
            add.path.starts_with(&expected_prefix),
            "local-pond add `{}` should start with `{}`",
            add.path,
            expected_prefix
        );
    }

    // Filter contract: asking for a foreign pond_id at the SAME
    // commit returns empty -- this is the D5.3 invariant that lets
    // push enumerate only the requested pond's files even when
    // commits become multi-pond in D5.7.
    let foreign_pond_id = Uuid::new_v4();
    assert_ne!(foreign_pond_id, local_pond_id);
    let (adds_foreign, removes_foreign) = adapter
        .actions_at_version(foreign_pond_id, version)
        .await
        .expect("foreign-pond actions");
    assert!(
        adds_foreign.is_empty(),
        "expected zero adds for foreign pond at version {} (got {:?})",
        version,
        adds_foreign
    );
    assert!(
        removes_foreign.is_empty(),
        "expected zero removes for foreign pond at version {} (got {:?})",
        version,
        removes_foreign
    );
}

/// D5.4: `Ship::create_replica` yields a pond whose `store_id` matches
/// the requested `pond_id` (bit-for-bit identity preservation across
/// the uuid::Uuid -> uuid7::Uuid conversion).
#[tokio::test]
async fn ship_create_replica_yields_matching_store_id() {
    use uuid::Uuid;

    init_log();
    let tmp = tempdir().expect("tempdir");
    let replica_path = tmp.path().join("replica");

    let target_pond_id = Uuid::new_v4();
    let replica = Ship::create_replica(&replica_path, target_pond_id)
        .await
        .expect("create_replica");
    let replica_pond_id = replica.control_table().pond_id_uuid();

    assert_eq!(
        replica_pond_id, target_pond_id,
        "replica's pond_id ({}) must match the requested ({})",
        replica_pond_id, target_pond_id,
    );
}

/// D5.4: `Remote::bootstrap_consumer` against a remote with only
/// Write bundles seeds `last_pulled_seq=1` (skipping the producer's
/// pond_init) and then pulls all bundles -- the freshly-created
/// duckpond replica ends up with the pushed file visible.
///
/// This is the public-API equivalent of the old manual workaround
/// (`create_pond_for_restoration` + `raw_config_set(last_pulled_seq,
/// "1")` + `remote.pull`) that callers used pre-D5.4.
#[tokio::test]
#[ignore = "bundle bootstrap_consumer; create_replica now writes a root v1 for content mirror (D6)"]
async fn ship_remote_bootstrap_consumer_no_compact() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // 1) Source pond + one write.
    let mut src = Ship::create_pond(&src_path, "test-host")
        .await
        .expect("create src");
    let src_pond_id = src.control_table().pond_id_uuid();
    src.write_transaction(&meta("seed"), async |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(
            &root,
            "/d54.txt",
            b"bootstrap consumer",
        )
        .await?;
        Ok(())
    })
    .await
    .expect("write txn");
    let src_seq = src.last_write_seq();

    // 2) Push to file:// remote.
    let mut remote = Remote::create(&remote_path, src_pond_id)
        .await
        .expect("create remote");
    {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote.push(&mut adapter, src_seq).await.expect("push");
    }

    // 3) Bootstrap a fresh dst replica via the new D5.4 APIs.
    let remote_url = remote.url().to_string();
    let remote_for_bootstrap = Remote::open_at_url(&remote_url, Default::default())
        .await
        .expect("open remote for bootstrap");
    let mut dst = Ship::create_replica(&dst_path, src_pond_id)
        .await
        .expect("create_replica");
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        remote_for_bootstrap
            .bootstrap_consumer(&mut adapter)
            .await
            .expect("bootstrap_consumer");
    }

    // 4) The pushed file should be readable in dst (i.e., the
    //    bootstrap actually pulled the writes, not just seeded the
    //    setting).
    let tx = dst.begin_read(&meta("verify")).await.expect("begin_read");
    let root = tx.root().await.expect("dst root");
    let bytes = root
        .read_file_path_to_vec("/d54.txt")
        .await
        .expect("read /d54.txt");
    assert_eq!(bytes, b"bootstrap consumer");
    let _ = tx.commit().await.expect("commit read");

    // 5) A second bootstrap_consumer call is a no-op (idempotent):
    //    last_pulled_seq is already at the latest, so pull applies
    //    zero bundles.  We can't directly observe this through
    //    bootstrap_consumer's return (it returns ()), but a follow-up
    //    pull should report zero bundles applied.
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        let report = remote_for_bootstrap
            .pull(&mut adapter)
            .await
            .expect("idempotent pull");
        assert_eq!(
            report.bundles_applied.len(),
            0,
            "second pull should be a no-op after bootstrap_consumer caught up"
        );
    }
}

/// D5.4: `Remote::bootstrap_consumer` refuses a consumer whose
/// `store_id` does not match the remote's.  Replicas are always
/// same-identity by construction; a mismatch is a programmer error
/// (e.g., passing the wrong remote handle).
#[tokio::test]
async fn ship_remote_bootstrap_consumer_rejects_store_id_mismatch() {
    use sync_remote::RemoteError;
    use uuid::Uuid;

    init_log();
    let tmp = tempdir().expect("tempdir");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // Remote has its OWN pond_id.
    let remote_pond_id = Uuid::new_v4();
    let remote = Remote::create(&remote_path, remote_pond_id)
        .await
        .expect("create remote");

    // Consumer has a DIFFERENT pond_id.
    let consumer_pond_id = Uuid::new_v4();
    assert_ne!(consumer_pond_id, remote_pond_id);
    let mut dst = Ship::create_replica(&dst_path, consumer_pond_id)
        .await
        .expect("create_replica");

    let mut adapter = ShipRemoteSteward::new(&mut dst);
    match remote.bootstrap_consumer(&mut adapter).await {
        Err(RemoteError::StoreIdMismatch { remote: r, steward }) => {
            assert_eq!(r, remote_pond_id);
            assert_eq!(steward, consumer_pond_id);
        }
        Ok(()) => panic!("expected StoreIdMismatch, got Ok"),
        Err(other) => panic!("expected StoreIdMismatch, got {:?}", other),
    }
}

// ---------------------------------------------------------------------
// D5.5: compute_live_checksums for duckpond (tlogfs row schema).
//
// `RemoteSteward::compute_live_checksums(pond_id)` returns the live
// per-partition BLAKE3-of-Merkle checksums for every part_id under
// `pond_id`.  These tests exercise the freshly-implemented adapter
// method directly (no full sync_remote::Remote roundtrip needed).
// ---------------------------------------------------------------------

/// Two back-to-back invocations of `compute_live_checksums` on the
/// same pond -- with no writes in between -- must return identical
/// maps.  Determinism is the verify path's foundational guarantee.
#[tokio::test]
async fn ship_compute_live_checksums_is_deterministic() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");
    let pond_id = ship.control_table().pond_id_uuid();

    ship.write_transaction(&meta("seed"), async |fs| {
        let root = fs.root().await?;
        let _ =
            tinyfs::async_helpers::convenience::create_file_path(&root, "/a.txt", b"alpha").await?;
        let _ =
            tinyfs::async_helpers::convenience::create_file_path(&root, "/b.txt", b"beta").await?;
        Ok(())
    })
    .await
    .expect("write txn");

    let adapter = ShipRemoteSteward::new(&mut ship);
    let first = sync_remote::RemoteSteward::compute_live_checksums(&adapter, pond_id)
        .await
        .expect("first compute");
    let second = sync_remote::RemoteSteward::compute_live_checksums(&adapter, pond_id)
        .await
        .expect("second compute");
    assert_eq!(
        first, second,
        "compute_live_checksums must be deterministic across calls"
    );

    assert!(
        !first.is_empty(),
        "a pond with writes should have at least one partition checksum"
    );
    for (partition, checksum) in &first {
        assert_eq!(
            checksum.kind,
            sync_store::checksum::ChecksumKind::Merkle,
            "partition {} should use the Merkle strategy",
            partition
        );
        assert_eq!(
            checksum.bytes.len(),
            32,
            "Merkle output is BLAKE3 (32 bytes), got {} for partition {}",
            checksum.bytes.len(),
            partition,
        );
    }
}

/// A write that actually changes content must cause the per-partition
/// checksum to change -- otherwise verify is blind to drift.
#[tokio::test]
async fn ship_compute_live_checksums_changes_with_content() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");
    let pond_id = ship.control_table().pond_id_uuid();

    // Initial write.
    ship.write_transaction(&meta("v1"), async |fs| {
        let root = fs.root().await?;
        let _ =
            tinyfs::async_helpers::convenience::create_file_path(&root, "/file.txt", b"v1").await?;
        Ok(())
    })
    .await
    .expect("write v1");
    let before = {
        let adapter = ShipRemoteSteward::new(&mut ship);
        sync_remote::RemoteSteward::compute_live_checksums(&adapter, pond_id)
            .await
            .expect("checksums before")
    };
    assert!(!before.is_empty(), "expected at least one partition");

    // Second write: append a NEW version of /file.txt.  This creates
    // a new OplogEntry row with a different `content`/`blake3`, so the
    // partition's checksum must shift.
    ship.write_transaction(&meta("v2"), async |fs| {
        let root = fs.root().await?;
        root.write_file_path_from_slice("/file.txt", b"v2").await?;
        Ok(())
    })
    .await
    .expect("write v2");
    let after = {
        let adapter = ShipRemoteSteward::new(&mut ship);
        sync_remote::RemoteSteward::compute_live_checksums(&adapter, pond_id)
            .await
            .expect("checksums after")
    };

    assert_ne!(
        before, after,
        "writing a new version of an existing file must shift the partition's checksum"
    );
    assert_eq!(
        before.keys().collect::<std::collections::BTreeSet<_>>(),
        after.keys().collect::<std::collections::BTreeSet<_>>(),
        "partitions should not appear/disappear from a same-file overwrite"
    );
}

/// A different pond_id (foreign or absent) returns an empty map.
/// This is the contract verify relies on to skip foreign-pond data
/// when the local pond has no cross-pond imports.
#[tokio::test]
async fn ship_compute_live_checksums_unknown_pond_id_is_empty() {
    use uuid::Uuid;

    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    ship.write_transaction(&meta("seed"), async |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(
            &root,
            "/local.txt",
            b"local content",
        )
        .await?;
        Ok(())
    })
    .await
    .expect("write txn");

    let foreign = Uuid::new_v4();
    let adapter = ShipRemoteSteward::new(&mut ship);
    let result = sync_remote::RemoteSteward::compute_live_checksums(&adapter, foreign)
        .await
        .expect("compute for unknown pond_id");
    assert!(
        result.is_empty(),
        "compute_live_checksums for an unknown pond_id must be empty, got {} partitions",
        result.len(),
    );
}

/// P2-VERIFY-BOOTSTRAP-DRIFT: `Remote::push` replicates the `pond_init`
/// txn (txn_seq=1) as a normal bundle.  The root directory's OplogEntry
/// lands at a real Delta version (>= 1), so the producer records that
/// version and push builds a real bundle -- the bundle a replica needs to
/// hold the identity-bearing root rows.
///
/// This test calls `Remote::push(seq=1)` directly on a freshly-created
/// pond (only the pond_init txn exists) and asserts:
/// - it returns Ok(())
/// - PostPush records WERE written locally (a real push happened)
/// - the `last_pushed_seq:<url>` setting was advanced to 1
/// - a bundle for seq=1 now exists on the remote
#[tokio::test]
async fn ship_remote_push_replicates_pond_init() {
    use sync_remote::{Remote, RemoteSteward};
    use sync_steward::RecordKind;

    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let remote_path = tmp.path().join("remote");

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");
    let pond_id = ship.control_table().pond_id_uuid();

    // No data writes -- only the pond_init txn (txn_seq=1) exists, whose
    // root row sits at a real Delta version.
    assert_eq!(
        ship.last_write_seq(),
        1,
        "freshly-created pond has only the pond_init txn at seq 1",
    );

    let mut remote = Remote::create(&remote_path, pond_id)
        .await
        .expect("create remote");
    let remote_url = remote.url().to_string();

    {
        let mut adapter = ShipRemoteSteward::new(&mut ship);
        remote
            .push(&mut adapter, 1)
            .await
            .expect("push of pond_init must succeed");
    }

    // PostPush records ARE written now that pond_init is a real bundle.
    let records = ship
        .control_table()
        .inner()
        .all_records_for(pond_id)
        .await
        .expect("read control records");
    let post_push_completed_seq_1 = records
        .iter()
        .any(|r| r.txn_seq == 1 && matches!(r.record_kind, RecordKind::PostPushCompleted));
    assert!(
        post_push_completed_seq_1,
        "pushing pond_init must write a PostPushCompleted record for seq 1",
    );

    // last_pushed_seq:<url> advanced to 1.
    let setting_key = format!("last_pushed_seq:{}", remote_url);
    let adapter = ShipRemoteSteward::new(&mut ship);
    let stored = adapter
        .config_get(&setting_key)
        .await
        .expect("config_get")
        .expect("setting should exist after push");
    assert_eq!(stored, "1", "last_pushed_seq must be advanced to 1");

    // A bundle for seq=1 now exists on the remote.
    assert_eq!(
        remote.oldest_available_seq().await.expect("oldest"),
        Some(1),
        "pond_init must be replicated as the oldest bundle",
    );
}

/// Regression for remote-redesign-review #3 (no-silent-fallback): when
/// the source's `last_pushed_seq:<url>` watermark holds a corrupt,
/// unparseable value, `push_pending_to_remote` must surface it as an
/// error instead of silently coercing it to 0 (which would re-enumerate
/// the entire transaction history from seq 1).  Before the fix the read
/// was `config_get(...).ok().flatten().and_then(parse).unwrap_or(0)`,
/// swallowing both a control-table read error and a parse error.
#[tokio::test]
async fn push_pending_to_remote_errors_on_corrupt_watermark() {
    use steward::{RemoteAttachment, push_pending_to_remote};

    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let remote_path = tmp.path().join("remote");

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");
    let pond_id = ship.control_table().pond_id_uuid();

    let remote = Remote::create(&remote_path, pond_id)
        .await
        .expect("create remote");
    let remote_url = remote.url().to_string();

    // Plant a corrupt watermark for this remote URL.
    let setting_key = format!("last_pushed_seq:{}", remote_url);
    ship.control_table_mut()
        .raw_config_set(&setting_key, "not-an-i64")
        .await
        .expect("raw_config_set");

    let attachment = RemoteAttachment {
        url: remote_url,
        region: String::new(),
        access_key_id: String::new(),
        secret_access_key: String::new(),
        endpoint: String::new(),
        allow_http: false,
    };

    let err = push_pending_to_remote(&mut ship, &attachment)
        .await
        .expect_err("corrupt watermark must surface as an error");
    assert!(
        matches!(err, sync_remote::RemoteError::Schema(_)),
        "corrupt watermark must surface as Schema, got {err:?}",
    );
}

/// D5.7a regression: after a native write transaction commits, the
/// `DataCommitted` record must carry partition checksums that match
/// the live state of the data filesystem.  Before D5.7a,
/// `record_data_committed` always stored `partition_checksums: {}`,
/// causing `verify_against_remote(remote, src)` to report drift on
/// every native write -- the bundle's recorded checksums were empty
/// while the live checksums (computed by the steward) had real
/// digests, so `compare(live, recorded)` always returned mismatches.
///
/// This test reproduces the failing path:
/// 1. Source pond writes data and pushes to a `file://` remote.
/// 2. `verify_against_remote(remote, src)` must report `ok = true`.
///
/// Cross-pond verify symmetry (dst pulls, dst verifies against
/// remote) is a separate concern not addressed by D5.7a: the live
/// checksum on dst is computed over only the bundles pulled, whereas
/// src's recorded checksum is computed over its full local data
/// table (including the unpushable `pond_init` rows at seq=1).
/// Reconciling those two views is a follow-up.
#[tokio::test]
async fn ship_remote_native_write_then_verify_passes() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let remote_path = tmp.path().join("remote");

    // 1. Source pond with a real write transaction.
    let mut src = Ship::create_pond(&src_path, "test-host")
        .await
        .expect("create src");
    let src_pond_id = src.control_table().pond_id_uuid();

    src.write_transaction(&meta("write_for_verify"), async |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(
            &root,
            "/verify_me.txt",
            b"payload for verify",
        )
        .await?;
        Ok(())
    })
    .await
    .expect("write transaction");
    let txn_seq = src.last_write_seq();
    assert!(txn_seq >= 2, "expected seq >= 2 after one write");

    // 2. Fresh file:// remote with src's store_id.
    let mut remote = Remote::create(&remote_path, src_pond_id)
        .await
        .expect("create remote");

    // 3. Push.
    {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote.push(&mut adapter, txn_seq).await.expect("push");
    }

    // 4. Verify src against remote.  Pre-D5.7a, this would fail
    //    because recorded was empty but live had real checksums.
    let adapter = ShipRemoteSteward::new(&mut src);
    let report = sync_remote::verify_against_remote(&remote, &adapter)
        .await
        .expect("verify src");
    assert!(
        report.ok,
        "src verify must pass after D5.7a: live and recorded should match.  \
         mismatches={:?}, divergence_boundary={:?}",
        report.mismatches, report.divergence_boundary
    );
    assert_eq!(report.remote_latest_seq, Some(txn_seq));
}

/// Write `content` to `path` in its own write transaction and return the
/// allocated txn_seq.
async fn write_one(src: &mut Ship, path: &str, content: &[u8]) -> i64 {
    let owned = content.to_vec();
    src.write_transaction(&meta("compact_write"), async move |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(&root, path, &owned).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
    src.last_write_seq()
}

/// P2-PRODUCER-COMPACT-BUNDLES: `Ship::compact` records a pushable
/// Compact transaction.  Several writes accumulate small parquet files in
/// the root partition; compaction merges them and records a
/// `DataCommitted(commit_kind=Compact)` at the post-optimize Delta
/// version.  A second compaction is a no-op.
#[tokio::test]
async fn ship_compact_records_pushable_compact_transaction() {
    use sync_steward::{CommitKind, RecordKind};

    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");
    let pond_id = ship.control_table().pond_id_uuid();

    // Several writes -> several small parquet files in the root partition.
    for i in 0..4 {
        let _ = write_one(
            &mut ship,
            &format!("/f{}.txt", i),
            format!("v{}", i).as_bytes(),
        )
        .await;
    }
    let last_write_seq = ship.last_write_seq();

    // Compaction merges them as a recorded transaction.
    let outcome = ship.compact().await.expect("compact");
    assert!(outcome.had_data, "expected real compaction, got no-op");
    assert!(
        outcome.files_removed > 0 && outcome.files_added > 0,
        "expected files merged, got +{}/-{}",
        outcome.files_added,
        outcome.files_removed
    );
    assert_eq!(
        outcome.txn_seq,
        last_write_seq + 1,
        "compaction takes the next seq"
    );
    assert_eq!(
        ship.last_write_seq(),
        outcome.txn_seq,
        "in-memory last_write_seq advances to the compaction seq"
    );

    // The control table has a DataCommitted(Compact) at the compaction seq
    // whose data_delta_version matches the outcome.
    let records = ship
        .control_table()
        .inner()
        .all_records_for(pond_id)
        .await
        .expect("read control records");
    let dc = records
        .iter()
        .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == outcome.txn_seq)
        .expect("DataCommitted record for the compaction seq");
    assert_eq!(
        dc.commit_kind,
        Some(CommitKind::Compact),
        "the compaction commit must be marked Compact (so push emits a Compact bundle)"
    );
    let dc_meta: sync_steward::DataCommittedMetadata =
        serde_json::from_str(&dc.metadata_json).expect("parse DataCommitted metadata");
    assert_eq!(
        dc_meta.data_delta_version, outcome.data_delta_version,
        "recorded data_delta_version must point at the optimize commit"
    );
    assert!(
        !dc_meta.partition_checksums.is_empty(),
        "compaction records the post-compaction partition checksums"
    );

    // get_last_write_sequence (reads DataCommitted from disk) reflects it.
    assert_eq!(
        ship.control_table()
            .get_last_write_sequence()
            .await
            .expect("last committed seq"),
        outcome.txn_seq
    );

    // Data is intact (the invariant check would have aborted otherwise).
    let tx = ship.begin_read(&meta("verify")).await.expect("begin_read");
    let root = tx.root().await.expect("root");
    for i in 0..4 {
        let bytes = root
            .read_file_path_to_vec(&format!("/f{}.txt", i))
            .await
            .expect("read file after compaction");
        assert_eq!(bytes, format!("v{}", i).as_bytes());
    }
    let _ = tx.commit().await.expect("commit read");

    // A second compaction has nothing to merge -> clean no-op.
    let noop = ship.compact().await.expect("second compact");
    assert!(!noop.had_data, "second compaction should be a no-op");

    // Regression (no-op compaction must not desync the seq allocators): a
    // no-op compaction commits no data, so it must leave `last_write_seq` in
    // lockstep with `data_persistence.last_txn_seq` (i.e. un-consumed at the
    // last real committed seq).  Before the fix, `last_write_seq` advanced past
    // the data allocator and the next transaction failed the strict +1 check.
    assert_eq!(
        ship.last_write_seq(),
        outcome.txn_seq,
        "no-op compaction must not advance last_write_seq past the last committed seq"
    );
    // A subsequent read transaction (strict `== last_txn_seq` check) succeeds.
    let tx = ship
        .begin_read(&meta("verify after noop"))
        .await
        .expect("begin_read after no-op compaction");
    let _ = tx
        .commit()
        .await
        .expect("commit read after no-op compaction");
    // A subsequent write transaction (strict `== last_txn_seq + 1` check)
    // succeeds and consumes the very seq the no-op compaction had reserved.
    let next_seq = write_one(&mut ship, "/after_noop.txt", b"ok").await;
    assert_eq!(
        next_seq,
        outcome.txn_seq + 1,
        "the write after a no-op compaction reuses the next seq"
    );
}

/// P2-PRODUCER-COMPACT-BUNDLES end-to-end: a producer compaction pushed
/// to a remote becomes a Compact bundle that a fresh consumer can restart
/// from.  This exercises the full mirror retention-recovery loop that was
/// previously unreachable from duckpond alone.
#[tokio::test]
async fn ship_compact_bundle_drives_restart_from_compact() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // 1. Source pond with several writes, all under the root partition.
    let mut src = Ship::create_pond(&src_path, "test-host")
        .await
        .expect("create src");
    let src_pond_id = src.control_table().pond_id_uuid();
    let src_pond_meta = src.control_table().pond_metadata().clone();

    let mut write_seqs = Vec::new();
    for i in 0..4 {
        write_seqs.push(
            write_one(
                &mut src,
                &format!("/f{}.txt", i),
                format!("v{}", i).as_bytes(),
            )
            .await,
        );
    }

    // 2. Remote with the source's identity (mirror).
    let mut remote = Remote::create(&remote_path, src_pond_id)
        .await
        .expect("create remote");

    // 3. Push every write bundle.
    for seq in &write_seqs {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote.push(&mut adapter, *seq).await.expect("push write");
    }

    // 4. Compact and push the resulting Compact bundle.
    let outcome = src.compact().await.expect("compact");
    assert!(outcome.had_data, "expected a real compaction for the test");
    {
        let mut adapter = ShipRemoteSteward::new(&mut src);
        remote
            .push(&mut adapter, outcome.txn_seq)
            .await
            .expect("push compact bundle");
    }

    // The remote now carries a Compact bundle.
    let has_compact = remote
        .list_bundles()
        .await
        .expect("list bundles")
        .iter()
        .any(|b| matches!(b.commit_kind, sync_steward::CommitKind::Compact));
    assert!(has_compact, "remote must carry a Compact bundle after push");

    // 5. Fresh consumer (same identity) restarts from the compact baseline.
    let mut dst = Ship::create_pond_for_restoration(&dst_path, src_pond_meta)
        .await
        .expect("create dst");
    {
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        remote
            .restart_pond_from_compact(&mut adapter)
            .await
            .expect("restart_pond_from_compact");
    }

    // 6. The consumer has the full dataset reconstructed from the baseline.
    let tx = dst
        .begin_read(&meta("verify_dst"))
        .await
        .expect("begin_read");
    let root = tx.root().await.expect("dst root");
    for i in 0..4 {
        let bytes = root
            .read_file_path_to_vec(&format!("/f{}.txt", i))
            .await
            .expect("read file from restarted consumer");
        assert_eq!(bytes, format!("v{}", i).as_bytes());
    }
    let _ = tx.commit().await.expect("commit read");
}

/// P2-PRODUCER-COMPACT-BUNDLES regression: after a compaction, reopening
/// the pond recovers the right `last_txn_seq`.  The optimize commit
/// becomes the most recent Delta commit, so it MUST carry `pond_txn`
/// metadata or `OpLogPersistence::open` would reset the sequence to 0.
#[tokio::test]
async fn ship_compact_survives_reopen() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    let compact_seq = {
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("create pond");
        for i in 0..4 {
            let _ = write_one(
                &mut ship,
                &format!("/f{}.txt", i),
                format!("v{}", i).as_bytes(),
            )
            .await;
        }
        let outcome = ship.compact().await.expect("compact");
        assert!(outcome.had_data);
        outcome.txn_seq
    };

    // Reopen: last_write_seq must reflect the compaction, data intact, and
    // a subsequent write must take the next sequence.
    let mut ship = Ship::open_pond(&pond_path).await.expect("reopen pond");
    assert_eq!(
        ship.last_write_seq(),
        compact_seq,
        "reopened pond must recover the compaction seq, not reset to 0"
    );

    let tx = ship.begin_read(&meta("verify")).await.expect("begin_read");
    let root = tx.root().await.expect("root");
    for i in 0..4 {
        let bytes = root
            .read_file_path_to_vec(&format!("/f{}.txt", i))
            .await
            .expect("read file after reopen");
        assert_eq!(bytes, format!("v{}", i).as_bytes());
    }
    let _ = tx.commit().await.expect("commit read");

    let next_seq = write_one(&mut ship, "/after.txt", b"after").await;
    assert_eq!(
        next_seq,
        compact_seq + 1,
        "a write after compaction takes the next sequence"
    );
}

/// P2-PRODUCER-COMPACT-BUNDLES regression: the full `maintain(true, true)`
/// flow (compact THEN checkpoint/vacuum) must leave the data table's
/// `last_txn_seq` recoverable on reopen, so `pond push` sees the
/// compaction.  Reproduces the CLI `pond maintain --compact` path.
#[tokio::test]
async fn ship_maintain_compact_survives_reopen() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");

    let expected_seq = {
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("create pond");
        for i in 0..4 {
            let _ = write_one(
                &mut ship,
                &format!("/f{}.txt", i),
                format!("v{}", i).as_bytes(),
            )
            .await;
        }
        let report = ship.maintain(true, true).await;
        assert!(
            report.data.as_ref().map(|d| d.compacted).unwrap_or(false),
            "maintain --compact should have compacted the data table"
        );
        ship.last_write_seq()
    };

    let ship = Ship::open_pond(&pond_path).await.expect("reopen pond");
    assert_eq!(
        ship.last_write_seq(),
        expected_seq,
        "reopen after maintain --compact must recover the compaction seq from the data table"
    );
}

/// P2-VERIFY-BOOTSTRAP-DRIFT regression: a replica bootstrapped from a
/// remote must verify cleanly against that remote.  Before the fix the
/// producer's pond_init root rows (Delta version 1, mis-recorded as
/// data_delta_version=0 and skipped by push) were never replicated, so
/// the replica's root partition diverged.  Now seq=1 is a normal bundle.
#[tokio::test]
#[ignore = "bundle bootstrap+verify; create_replica now writes a root v1 for content mirror (D6)"]
async fn ship_replica_verify_matches_after_bootstrap() {
    use sync_remote::Remote;

    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // Producer with a few writes.
    let mut src = Ship::create_pond(&src_path, "test-host")
        .await
        .expect("create src");
    let pond_id = src.control_table().pond_id_uuid();
    for i in 0..3 {
        let _ = write_one(
            &mut src,
            &format!("/f{}.txt", i),
            format!("v{}", i).as_bytes(),
        )
        .await;
    }

    // Push every seq (1..=upper); seq=1 (pond_init) is now a real bundle.
    let mut remote = Remote::create(&remote_path, pond_id).await.expect("remote");
    let upper = src.last_write_seq();
    for seq in 1..=upper {
        let mut a = ShipRemoteSteward::new(&mut src);
        match remote.push(&mut a, seq).await {
            Ok(()) => {}
            Err(sync_remote::RemoteError::NoSuchCommit(_)) => {}
            Err(e) => panic!("push {}: {}", seq, e),
        }
    }

    // The pond_init bundle (seq=1) must now exist on the remote.
    let oldest = remote.oldest_available_seq().await.expect("oldest");
    assert_eq!(
        oldest,
        Some(1),
        "pond_init (seq=1) must be replicated as a bundle"
    );

    // Producer verifies against its own backup.
    {
        let a = ShipRemoteSteward::new(&mut src);
        let r = sync_remote::verify_against_remote(&remote, &a)
            .await
            .expect("verify src");
        assert!(
            r.ok,
            "producer verify must pass: mismatches={:?}",
            r.mismatches
        );
    }

    // Bootstrap a fresh mirror replica and verify it against the remote.
    let mut dst = Ship::create_replica(&dst_path, uuid::Uuid::from_bytes(*pond_id.as_bytes()))
        .await
        .expect("replica");
    {
        let mut a = ShipRemoteSteward::new(&mut dst);
        remote
            .bootstrap_consumer(&mut a)
            .await
            .expect("bootstrap_consumer");
    }
    {
        let a = ShipRemoteSteward::new(&mut dst);
        let r = sync_remote::verify_against_remote(&remote, &a)
            .await
            .expect("verify dst");
        assert!(
            r.ok,
            "replica verify must pass after bootstrap: mismatches={:?}",
            r.mismatches
        );
    }

    // And the replica holds the data.
    let tx = dst
        .begin_read(&meta("verify_dst"))
        .await
        .expect("begin_read");
    let root = tx.root().await.expect("root");
    for i in 0..3 {
        let bytes = root
            .read_file_path_to_vec(&format!("/f{}.txt", i))
            .await
            .expect("read replica file");
        assert_eq!(bytes, format!("v{}", i).as_bytes());
    }
    let _ = tx.commit().await.expect("commit read");
}
