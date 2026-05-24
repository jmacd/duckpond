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
