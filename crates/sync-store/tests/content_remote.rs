// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`ContentRemote`]: the delta-managed
//! content-addressed remote (design Section 8, Decision D6).

use sync_store::ContentRemote;
use sync_store::content::ObjectHash;
use tempfile::TempDir;
use uuid::Uuid;

fn pid() -> Uuid {
    Uuid::from_u128(0xc0_0000_0000_0000_0000_0000_0000_0000)
}

fn obj(bytes: &[u8]) -> (ObjectHash, Vec<u8>) {
    (ObjectHash::of_bytes(bytes), bytes.to_vec())
}

/// A pushed object reads back by hash, and the tip ref reads back the tip.
#[tokio::test]
async fn push_then_read_objects_and_tip() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (h_blob, blob) = obj(b"hello blob");
    let (h_tree, tree) = obj(b"tree bytes");
    let (h_commit, commit) = obj(b"commit bytes");

    let seq = remote
        .push_commit(
            &[
                (h_blob, blob.clone()),
                (h_tree, tree.clone()),
                (h_commit, commit.clone()),
            ],
            "main",
            h_commit,
        )
        .await
        .unwrap();
    assert_eq!(seq, 1, "first push allocates txn_seq 1");

    assert_eq!(remote.get_object(h_blob).await.unwrap(), Some(blob));
    assert_eq!(remote.get_object(h_tree).await.unwrap(), Some(tree));
    assert_eq!(remote.get_object(h_commit).await.unwrap(), Some(commit));
    assert!(remote.has_object(h_blob).await.unwrap());
    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_commit));
}

/// A hash that was never pushed is absent; an unknown ref has no tip.
#[tokio::test]
async fn absent_object_and_ref_return_none() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let missing = ObjectHash::of_bytes(b"never pushed");
    assert_eq!(remote.get_object(missing).await.unwrap(), None);
    assert!(!remote.has_object(missing).await.unwrap());
    assert_eq!(remote.get_tip("main").await.unwrap(), None);
}

/// The tip ref advances in the SAME commit that adds the new objects: after
/// a second push the tip moves and the new object is present, while earlier
/// objects remain. Re-putting an already-present object hash is idempotent.
#[tokio::test]
async fn second_push_advances_tip_atomically() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (h_c1, c1) = obj(b"commit-1");
    let (h_shared, shared) = obj(b"shared-blob");
    let s1 = remote
        .push_commit(&[(h_c1, c1), (h_shared, shared.clone())], "main", h_c1)
        .await
        .unwrap();

    let (h_c2, c2) = obj(b"commit-2");
    let (h_new, new) = obj(b"new-blob");
    // Re-include the shared blob; it must remain readable and not corrupt.
    let s2 = remote
        .push_commit(
            &[(h_c2, c2), (h_new, new.clone()), (h_shared, shared.clone())],
            "main",
            h_c2,
        )
        .await
        .unwrap();
    assert_eq!(s2, s1 + 1, "txn_seq is monotonic");

    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_c2));
    assert_eq!(remote.get_object(h_new).await.unwrap(), Some(new));
    assert_eq!(remote.get_object(h_shared).await.unwrap(), Some(shared));
    assert!(
        remote.has_object(h_c1).await.unwrap(),
        "old commit retained"
    );
}

/// A remote survives close/reopen: objects and tip persist via Delta.
#[tokio::test]
async fn reopen_preserves_objects_and_tip() {
    let dir = TempDir::new().unwrap();
    let (h_commit, commit) = obj(b"durable commit");
    {
        let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();
        remote
            .push_commit(&[(h_commit, commit.clone())], "main", h_commit)
            .await
            .unwrap();
    }
    let remote = ContentRemote::open_at(dir.path(), pid()).await.unwrap();
    assert_eq!(remote.get_object(h_commit).await.unwrap(), Some(commit));
    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_commit));
}
