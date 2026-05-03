// SPDX-License-Identifier: Apache-2.0

//! Smoke tests for Remote::create / open / store_id / push happy path.
//!
//! Comprehensive push test coverage lives in `push.rs`.

use sandbox_remote::Remote;
use sandbox_steward::{CommitKind, Steward};
use tempfile::TempDir;
use uuid::Uuid;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[tokio::test]
async fn create_then_open_round_trips_store_id() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let id = Uuid::new_v4();
    let _ = Remote::create(dir.path().join("remote"), id).await.unwrap();
    let r = Remote::open(dir.path().join("remote")).await.unwrap();
    assert_eq!(r.store_id(), id);
}

#[tokio::test]
async fn open_errors_on_missing_remote() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let result = Remote::open(dir.path().join("nonexistent")).await;
    assert!(result.is_err(), "open of nonexistent path should error");
}

#[tokio::test]
async fn list_and_latest_seq_on_empty_remote() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let id = Uuid::new_v4();
    let r = Remote::create(dir.path().join("remote"), id).await.unwrap();
    let bundles = r.list_bundles().await.unwrap();
    assert!(bundles.is_empty());
    assert_eq!(r.latest_seq().await.unwrap(), None);
    assert_eq!(r.oldest_available_seq().await.unwrap(), None);
}

#[tokio::test]
async fn push_happy_path_one_bundle() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p1", "k1", b"v1".to_vec()).unwrap();
        g.put("p1", "k2", b"v2".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }

    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();
    remote.push(&mut steward, 1).await.unwrap();

    let bundles = remote.list_bundles().await.unwrap();
    assert_eq!(bundles.len(), 1, "exactly one bundle on the remote");
    assert_eq!(bundles[0].txn_seq, 1);
    assert_eq!(bundles[0].commit_kind, CommitKind::Write);
    assert_eq!(bundles[0].parent_seq, 0, "first commit -> parent_seq 0");

    assert_eq!(remote.latest_seq().await.unwrap(), Some(1));
    assert_eq!(remote.oldest_available_seq().await.unwrap(), Some(1));
}

#[tokio::test]
async fn push_records_post_push_pending_then_completed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();
    remote.push(&mut steward, 1).await.unwrap();

    let log = steward.log(None).await.unwrap();
    let pending: Vec<_> = log
        .iter()
        .filter(|r| r.txn_seq == 1 && r.record_kind == sandbox_steward::RecordKind::PostPushPending)
        .collect();
    let completed: Vec<_> = log
        .iter()
        .filter(|r| {
            r.txn_seq == 1 && r.record_kind == sandbox_steward::RecordKind::PostPushCompleted
        })
        .collect();
    assert_eq!(pending.len(), 1, "one PostPushPending after push");
    assert_eq!(completed.len(), 1, "one PostPushCompleted after push");
    assert_eq!(
        pending[0].txn_id, completed[0].txn_id,
        "pending and completed share txn_id"
    );
}

#[tokio::test]
async fn push_idempotent_re_push_is_a_noop() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();

    remote.push(&mut steward, 1).await.unwrap();
    let bundles_after_first = remote.list_bundles().await.unwrap();
    assert_eq!(bundles_after_first.len(), 1);

    // Second push for the same seq should be a no-op success.
    remote.push(&mut steward, 1).await.unwrap();
    let bundles_after_second = remote.list_bundles().await.unwrap();
    assert_eq!(
        bundles_after_second.len(),
        1,
        "idempotent re-push does not create a duplicate bundle"
    );
}

#[tokio::test]
async fn push_errors_on_store_id_mismatch() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    {
        let mut g = steward.begin_write().await.unwrap();
        g.put("p", "k", b"v".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    // Remote with a DIFFERENT store_id.
    let other_id = Uuid::new_v4();
    assert_ne!(other_id, steward.store_id());
    let mut remote = Remote::create(dir.path().join("remote"), other_id)
        .await
        .unwrap();

    match remote.push(&mut steward, 1).await {
        Err(sandbox_remote::RemoteError::StoreIdMismatch {
            remote: r,
            steward: s,
        }) => {
            assert_eq!(r, other_id);
            assert_eq!(s, steward.store_id());
        }
        other => panic!("expected StoreIdMismatch, got {:?}", other),
    }

    // No bundle should have been written.
    assert!(remote.list_bundles().await.unwrap().is_empty());
}

#[tokio::test]
async fn push_errors_on_missing_data_committed() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut steward = Steward::create(dir.path().join("pond")).await.unwrap();
    let mut remote = Remote::create(dir.path().join("remote"), steward.store_id())
        .await
        .unwrap();
    // Empty pond -> no DataCommitted at txn_seq 1.
    match remote.push(&mut steward, 1).await {
        Err(sandbox_remote::RemoteError::NoSuchCommit(seq)) => assert_eq!(seq, 1),
        other => panic!("expected NoSuchCommit, got {:?}", other),
    }
}
