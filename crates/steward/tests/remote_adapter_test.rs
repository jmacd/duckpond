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
    let mut src = Ship::create_pond(&src_path).await.expect("create src");
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

    // 8. Cross-pond import is rejected in D4.  Build a synthetic
    //    PulledBundle with a foreign pond_id and confirm
    //    apply_pulled_bundle errors.
    {
        let mut other = Ship::create_pond(tmp.path().join("other"))
            .await
            .expect("create other");
        let bogus_bundle = sync_steward::PulledBundle {
            pond_id: uuid::Uuid::new_v4(), // not other's pond_id
            txn_seq: 1,
            commit_kind: sync_steward::CommitKind::Write,
            parent_seq: 0,
            adds: vec![],
            removes: vec![],
            partition_checksums: Default::default(),
        };
        let mut adapter = ShipRemoteSteward::new(&mut other);
        let err = sync_remote::RemoteSteward::apply_pulled_bundle(&mut adapter, bogus_bundle)
            .await
            .expect_err("cross-pond import should be rejected in D4");
        let msg = format!("{}", err);
        assert!(
            msg.contains("cross-pond import"),
            "expected cross-pond error, got: {}",
            msg
        );
    }
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

    let mut src = Ship::create_pond(&src_path).await.expect("create src");
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
    let mut dst = Ship::create_pond(&dst_path).await.expect("create dst");
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
    let mut ship = Ship::create_pond(&pond_path).await.expect("create");
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
async fn ship_remote_bootstrap_consumer_no_compact() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let src_path = tmp.path().join("src");
    let dst_path = tmp.path().join("dst");
    let remote_path = tmp.path().join("remote");

    // 1) Source pond + one write.
    let mut src = Ship::create_pond(&src_path).await.expect("create src");
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
