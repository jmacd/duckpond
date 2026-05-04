// SPDX-License-Identifier: Apache-2.0

//! Tests bridging the [`Store`] schema to the [`PartitionChecksum`]
//! trait.
//!
//! Each property is tested against both candidate strategies.  This is
//! the core correctness contract for the eventual remote-sync design:
//! two stores that hold the same logical content must produce the same
//! per-partition checksum, regardless of write order, regardless of
//! intervening compactions, regardless of any future Delta-level
//! re-layout.

use sandbox_store::Store;
use sandbox_store::checksum::{Checksum, Homomorphic, Merkle, PartitionChecksum};
use tempfile::TempDir;
use uuid::Uuid;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn pid() -> Uuid {
    Uuid::from_u128(0xa1_0000_0000_0000_0000_0000_0000_0000)
}

async fn populate_alpha(store: &mut Store) {
    let _ = store.put(pid(), "p", "a", b"A".to_vec()).await.unwrap();
    let _ = store.put(pid(), "p", "b", b"B".to_vec()).await.unwrap();
    let _ = store.put(pid(), "p", "c", b"C".to_vec()).await.unwrap();
}

/// Two stores reach the same logical state via different write orders.
/// Their checksums must match under any strategy.
async fn order_independent<C: PartitionChecksum>(c: &C) -> (Checksum, Checksum) {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let mut s1 = Store::create(dir1.path()).await.unwrap();
    let _ = s1.put(pid(), "p", "a", b"A".to_vec()).await.unwrap();
    let _ = s1.put(pid(), "p", "b", b"B".to_vec()).await.unwrap();
    let _ = s1.put(pid(), "p", "c", b"C".to_vec()).await.unwrap();

    let mut s2 = Store::create(dir2.path()).await.unwrap();
    // Different order, plus extra writes that get overwritten.
    let _ = s2.put(pid(), "p", "c", b"WRONG".to_vec()).await.unwrap();
    let _ = s2.put(pid(), "p", "a", b"A".to_vec()).await.unwrap();
    let _ = s2.put(pid(), "p", "c", b"C".to_vec()).await.unwrap();
    let _ = s2.put(pid(), "p", "b", b"OLD".to_vec()).await.unwrap();
    let _ = s2.put(pid(), "p", "b", b"B".to_vec()).await.unwrap();

    let cs1 = s1.compute_partition_checksum(pid(), "p", c).await.unwrap();
    let cs2 = s2.compute_partition_checksum(pid(), "p", c).await.unwrap();
    (cs1, cs2)
}

#[tokio::test]
async fn merkle_is_order_independent_across_stores() {
    init_logger();
    let (a, b) = order_independent(&Merkle::new()).await;
    assert_eq!(a, b);
}

#[tokio::test]
async fn homomorphic_is_order_independent_across_stores() {
    init_logger();
    let (a, b) = order_independent(&Homomorphic::new()).await;
    assert_eq!(a, b);
}

/// Tombstones must NOT contribute to the partition checksum.
async fn tombstone_neutral<C: PartitionChecksum>(c: &C) {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let mut s1 = Store::create(dir1.path()).await.unwrap();
    let _ = s1.put(pid(), "p", "a", b"A".to_vec()).await.unwrap();

    let mut s2 = Store::create(dir2.path()).await.unwrap();
    let _ = s2.put(pid(), "p", "a", b"A".to_vec()).await.unwrap();
    let _ = s2.put(pid(), "p", "b", b"B".to_vec()).await.unwrap();
    let _ = s2.delete(pid(), "p", "b").await.unwrap();

    let cs1 = s1.compute_partition_checksum(pid(), "p", c).await.unwrap();
    let cs2 = s2.compute_partition_checksum(pid(), "p", c).await.unwrap();
    assert_eq!(cs1, cs2, "deleted item must not influence checksum");
}

#[tokio::test]
async fn merkle_tombstone_neutral() {
    init_logger();
    tombstone_neutral(&Merkle::new()).await;
}

#[tokio::test]
async fn homomorphic_tombstone_neutral() {
    init_logger();
    tombstone_neutral(&Homomorphic::new()).await;
}

/// Cross-partition checksums are independent: writes to partition p2
/// must not change p1's checksum.
async fn cross_partition_isolation<C: PartitionChecksum>(c: &C) {
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    populate_alpha(&mut store).await;
    let p_before = store
        .compute_partition_checksum(pid(), "p", c)
        .await
        .unwrap();

    let _ = store.put(pid(), "q", "x", b"XXX".to_vec()).await.unwrap();
    let _ = store.put(pid(), "q", "y", b"YYY".to_vec()).await.unwrap();

    let p_after = store
        .compute_partition_checksum(pid(), "p", c)
        .await
        .unwrap();
    assert_eq!(
        p_before, p_after,
        "partition p must be unaffected by writes to partition q"
    );
}

#[tokio::test]
async fn merkle_cross_partition_isolation() {
    init_logger();
    cross_partition_isolation(&Merkle::new()).await;
}

#[tokio::test]
async fn homomorphic_cross_partition_isolation() {
    init_logger();
    cross_partition_isolation(&Homomorphic::new()).await;
}

/// A change to any value flips the checksum.
async fn value_change_detected<C: PartitionChecksum>(c: &C) {
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    populate_alpha(&mut store).await;
    let before = store
        .compute_partition_checksum(pid(), "p", c)
        .await
        .unwrap();

    let _ = store
        .put(pid(), "p", "b", b"B-changed".to_vec())
        .await
        .unwrap();
    let after = store
        .compute_partition_checksum(pid(), "p", c)
        .await
        .unwrap();
    assert_ne!(before, after);
}

#[tokio::test]
async fn merkle_value_change_detected() {
    init_logger();
    value_change_detected(&Merkle::new()).await;
}

#[tokio::test]
async fn homomorphic_value_change_detected() {
    init_logger();
    value_change_detected(&Homomorphic::new()).await;
}

/// Empty partition has a stable, well-defined checksum.
async fn empty_partition_stable<C: PartitionChecksum>(c: &C) {
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    let cs1 = store
        .compute_partition_checksum(pid(), "never", c)
        .await
        .unwrap();
    let cs2 = store
        .compute_partition_checksum(pid(), "never", c)
        .await
        .unwrap();
    assert_eq!(cs1, cs2);
    assert_eq!(cs1.kind, c.kind());
}

#[tokio::test]
async fn merkle_empty_partition_stable() {
    init_logger();
    empty_partition_stable(&Merkle::new()).await;
}

#[tokio::test]
async fn homomorphic_empty_partition_stable() {
    init_logger();
    empty_partition_stable(&Homomorphic::new()).await;
}

/// Two strategies disagree (different `kind`, generally different
/// bytes) on the same logical state.  Tags travel with bytes.
#[tokio::test]
async fn two_strategies_produce_distinct_checksums_for_same_state() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    populate_alpha(&mut store).await;
    let m = store
        .compute_partition_checksum(pid(), "p", &Merkle::new())
        .await
        .unwrap();
    let h = store
        .compute_partition_checksum(pid(), "p", &Homomorphic::new())
        .await
        .unwrap();
    assert_ne!(m, h);
    assert_ne!(m.bytes, h.bytes);
}
