// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the [`Store`] type.

use sandbox_store::{Op, Store};
use tempfile::TempDir;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[tokio::test]
async fn create_then_open_roundtrip() {
    init_logger();
    let dir = TempDir::new().unwrap();
    {
        let _store = Store::create(dir.path()).await.expect("create");
    }
    let store = Store::open(dir.path()).await.expect("open");
    assert_eq!(store.last_txn_seq().await.unwrap(), 0);
    assert!(store.partitions().await.unwrap().is_empty());
}

#[tokio::test]
async fn create_fails_if_already_exists() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let _ = Store::create(dir.path()).await.unwrap();
    let err = Store::create(dir.path()).await;
    assert!(err.is_err(), "second create must fail");
}

#[tokio::test]
async fn put_then_get_returns_value() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let seq = store.put("p1", "k1", b"hello".to_vec()).await.unwrap();
    assert_eq!(seq, 1);
    let v = store.get("p1", "k1").await.unwrap();
    assert_eq!(v, Some(b"hello".to_vec()));
}

#[tokio::test]
async fn get_returns_none_for_missing_partition() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    let v = store.get("nope", "nope").await.unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn get_returns_none_for_missing_key() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p1", "k1", b"v1".to_vec()).await.unwrap();
    let v = store.get("p1", "k2").await.unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn put_overwrites_returns_latest() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p1", "k1", b"v1".to_vec()).await.unwrap();
    let _ = store.put("p1", "k1", b"v2".to_vec()).await.unwrap();
    let _ = store.put("p1", "k1", b"v3".to_vec()).await.unwrap();
    let v = store.get("p1", "k1").await.unwrap();
    assert_eq!(v, Some(b"v3".to_vec()));
    assert_eq!(store.last_txn_seq().await.unwrap(), 3);
}

#[tokio::test]
async fn delete_makes_get_return_none() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p1", "k1", b"v1".to_vec()).await.unwrap();
    let _ = store.delete("p1", "k1").await.unwrap();
    assert_eq!(store.get("p1", "k1").await.unwrap(), None);
}

#[tokio::test]
async fn put_after_delete_resurrects_item() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p1", "k1", b"v1".to_vec()).await.unwrap();
    let _ = store.delete("p1", "k1").await.unwrap();
    let _ = store.put("p1", "k1", b"v2".to_vec()).await.unwrap();
    assert_eq!(store.get("p1", "k1").await.unwrap(), Some(b"v2".to_vec()));
}

#[tokio::test]
async fn delete_of_missing_key_is_a_tombstone_with_no_observable_effect() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.delete("p1", "ghost").await.unwrap();
    assert_eq!(store.get("p1", "ghost").await.unwrap(), None);
}

#[tokio::test]
async fn list_returns_live_items_sorted_excluding_tombstones() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p1", "b", b"B".to_vec()).await.unwrap();
    let _ = store.put("p1", "a", b"A".to_vec()).await.unwrap();
    let _ = store.put("p1", "c", b"C".to_vec()).await.unwrap();
    let _ = store.delete("p1", "b").await.unwrap();
    let listed = store.list("p1").await.unwrap();
    assert_eq!(
        listed,
        vec![
            ("a".to_string(), b"A".to_vec()),
            ("c".to_string(), b"C".to_vec()),
        ]
    );
}

#[tokio::test]
async fn list_empty_partition_returns_empty_vec() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    assert!(store.list("nope").await.unwrap().is_empty());
}

#[tokio::test]
async fn partitions_lists_all_keys_used() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("alpha", "k", b"v".to_vec()).await.unwrap();
    let _ = store.put("beta", "k", b"v".to_vec()).await.unwrap();
    let _ = store.put("alpha", "k2", b"v".to_vec()).await.unwrap();
    let parts = store.partitions().await.unwrap();
    assert_eq!(parts, vec!["alpha".to_string(), "beta".to_string()]);
}

#[tokio::test]
async fn partitions_includes_partitions_with_only_tombstones() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("ghosts", "k", b"v".to_vec()).await.unwrap();
    let _ = store.delete("ghosts", "k").await.unwrap();
    assert_eq!(
        store.partitions().await.unwrap(),
        vec!["ghosts".to_string()]
    );
    assert!(store.list("ghosts").await.unwrap().is_empty());
}

#[tokio::test]
async fn apply_batch_writes_one_commit_for_many_ops() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();

    let v0 = store.delta_version();
    store
        .apply_batch(
            42,
            1_700_000_000_000_000,
            vec![
                Op::Put {
                    partition: "p".into(),
                    key: "a".into(),
                    value: b"A".to_vec(),
                },
                Op::Put {
                    partition: "p".into(),
                    key: "b".into(),
                    value: b"B".to_vec(),
                },
                Op::Put {
                    partition: "q".into(),
                    key: "x".into(),
                    value: b"X".to_vec(),
                },
            ],
        )
        .await
        .unwrap();
    let v1 = store.delta_version();
    assert_eq!(v1, v0 + 1, "exactly one Delta commit for the batch");
    assert_eq!(store.last_txn_seq().await.unwrap(), 42);
    assert_eq!(store.get("p", "a").await.unwrap(), Some(b"A".to_vec()));
    assert_eq!(store.get("p", "b").await.unwrap(), Some(b"B".to_vec()));
    assert_eq!(store.get("q", "x").await.unwrap(), Some(b"X".to_vec()));
}

#[tokio::test]
async fn apply_batch_coalesces_repeated_ops_on_same_key() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();

    store
        .apply_batch(
            1,
            0,
            vec![
                Op::Put {
                    partition: "p".into(),
                    key: "k".into(),
                    value: b"first".to_vec(),
                },
                Op::Put {
                    partition: "p".into(),
                    key: "k".into(),
                    value: b"middle".to_vec(),
                },
                Op::Delete {
                    partition: "p".into(),
                    key: "k".into(),
                },
                Op::Put {
                    partition: "p".into(),
                    key: "k".into(),
                    value: b"last".to_vec(),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(store.get("p", "k").await.unwrap(), Some(b"last".to_vec()));
}

#[tokio::test]
async fn apply_batch_empty_is_a_noop() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let v_before = store.delta_version();
    store.apply_batch(99, 0, vec![]).await.unwrap();
    let v_after = store.delta_version();
    assert_eq!(v_before, v_after, "empty batch must NOT produce a commit");
    assert_eq!(store.last_txn_seq().await.unwrap(), 0);
}

#[tokio::test]
async fn cross_partition_writes_are_independent() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p1", "k", b"v_p1".to_vec()).await.unwrap();
    let _ = store.put("p2", "k", b"v_p2".to_vec()).await.unwrap();
    assert_eq!(store.get("p1", "k").await.unwrap(), Some(b"v_p1".to_vec()));
    assert_eq!(store.get("p2", "k").await.unwrap(), Some(b"v_p2".to_vec()));
    assert_eq!(store.list("p1").await.unwrap().len(), 1);
    assert_eq!(store.list("p2").await.unwrap().len(), 1);
}

#[tokio::test]
async fn empty_value_is_distinguishable_from_tombstone() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p", "k", Vec::new()).await.unwrap();
    assert_eq!(
        store.get("p", "k").await.unwrap(),
        Some(Vec::new()),
        "empty-bytes value is a real value, not absence"
    );
    let _ = store.delete("p", "k").await.unwrap();
    assert_eq!(store.get("p", "k").await.unwrap(), None);
}

#[tokio::test]
async fn keys_with_sql_metacharacters_roundtrip() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let weird = "weird'key;--";
    let _ = store.put("p", weird, b"v".to_vec()).await.unwrap();
    assert_eq!(store.get("p", weird).await.unwrap(), Some(b"v".to_vec()));
    let listed = store.list("p").await.unwrap();
    assert_eq!(listed, vec![(weird.to_string(), b"v".to_vec())]);
}

#[tokio::test]
async fn actions_at_version_returns_adds_for_a_write() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put("p", "k", b"v".to_vec()).await.unwrap();
    let v = store.delta_version();
    assert!(v > 0, "version advanced past initial");

    let (adds, removes) = store.actions_at_version(v).await.unwrap();
    assert!(!adds.is_empty(), "write produces at least one Add action");
    assert!(removes.is_empty(), "first write has no Remove actions");
    for add in &adds {
        assert!(add.size > 0, "Add reports a positive file size");
        assert!(!add.path.is_empty(), "Add has a path");
    }

    // Add paths must be a subset of the table's current data files
    // (they were just added; nothing's been removed).
    let current: std::collections::HashSet<_> = store
        .data_files()
        .unwrap()
        .into_iter()
        // Strip any Url scheme prefix; data_files returns absolute
        // file:// URIs while Add paths are relative.
        .map(|p| {
            p.rsplit_once('/')
                .map(|(_, name)| name.to_string())
                .unwrap_or(p)
        })
        .collect();
    for add in &adds {
        let leaf = add
            .path
            .rsplit_once('/')
            .map(|(_, name)| name.to_string())
            .unwrap_or_else(|| add.path.clone());
        assert!(
            current.contains(&leaf),
            "Add file leaf `{}` (full `{}`) appears in current data_files",
            leaf,
            add.path,
        );
    }
}

#[tokio::test]
async fn actions_at_version_returns_adds_and_removes_for_a_compact() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    // Three small writes -> three small parquets in partition `p`.
    for i in 0..3 {
        let _ = store
            .put("p", &format!("k{}", i), b"v".to_vec())
            .await
            .unwrap();
    }
    let pre_version = store.delta_version();
    let metrics = store.compact(None).await.unwrap();

    if metrics.is_noop() {
        // delta-rs decided nothing to merge for these tiny files; can't
        // exercise the Adds-and-Removes assertion. Still verify the
        // pre-compact write's actions are reachable.
        let (adds, removes) = store.actions_at_version(pre_version).await.unwrap();
        assert!(!adds.is_empty());
        assert!(removes.is_empty());
        return;
    }

    let post_version = store.delta_version();
    assert!(post_version > pre_version, "compact advanced version");

    let (adds, removes) = store.actions_at_version(post_version).await.unwrap();
    assert!(
        !adds.is_empty(),
        "compact recorded at least one Add (merged)"
    );
    assert!(
        !removes.is_empty(),
        "compact recorded Removes for the small parquets it merged",
    );
    assert_eq!(
        adds.len() as u64,
        metrics.num_files_added,
        "Add count matches metrics.num_files_added",
    );
    assert_eq!(
        removes.len() as u64,
        metrics.num_files_removed,
        "Remove count matches metrics.num_files_removed",
    );
}

#[tokio::test]
async fn actions_at_version_errors_on_missing_version() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    // No writes; delta-rs only has the initial schema commit at version 0.
    let result = store.actions_at_version(99).await;
    assert!(result.is_err(), "missing version errors");
}
