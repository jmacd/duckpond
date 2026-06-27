// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::compute_content_tree`: the read-side
//! content-tree fold that produces a pond's `root_tree_hash` from live state.

use steward::Ship;
use tempfile::tempdir;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = create_file_path(&root, path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

async fn mkdir_and_file(ship: &mut Ship, dir: &str, file: &str, bytes: &[u8]) {
    let dir = dir.to_string();
    let file = file.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("mkdir"), async move |fs| {
        let root = fs.root().await?;
        let _ = root.create_dir_all(&dir).await?;
        let _ = create_file_path(&root, &file, &bytes).await?;
        Ok(())
    })
    .await
    .expect("mkdir transaction");
}

/// The root tree hash is stable across repeated computations and changes when
/// file content changes.
#[tokio::test]
async fn content_tree_root_is_deterministic_and_data_sensitive() {
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "content-test")
        .await
        .expect("create pond");

    write_file(&mut ship, "/a.txt", b"hello").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"world").await;

    let r1 = steward::compute_content_tree(&ship)
        .await
        .expect("compute 1");
    let r2 = steward::compute_content_tree(&ship)
        .await
        .expect("compute 2");
    assert_eq!(
        r1.root_tree_hash, r2.root_tree_hash,
        "root tree hash must be deterministic"
    );
    assert!(r1.nodes_hashed >= 2, "root plus /sub are directories");

    // Adding a new file changes the root tree hash.
    write_file(&mut ship, "/c.txt", b"new file").await;
    let r3 = steward::compute_content_tree(&ship)
        .await
        .expect("compute 3");
    assert_ne!(
        r1.root_tree_hash, r3.root_tree_hash,
        "root tree hash must change when the tree changes"
    );
}

/// Two ponds built with identical content produce identical root tree hashes,
/// independent of pond identity and lineage (Goal 2: comparison).
#[tokio::test]
async fn identical_content_yields_identical_root_across_ponds() {
    let tmp_a = tempdir().expect("tempdir a");
    let tmp_b = tempdir().expect("tempdir b");
    let mut ship_a = Ship::create_pond(tmp_a.path().join("pond"), "pond-a")
        .await
        .expect("create pond a");
    let mut ship_b = Ship::create_pond(tmp_b.path().join("pond"), "pond-b")
        .await
        .expect("create pond b");

    for ship in [&mut ship_a, &mut ship_b] {
        write_file(ship, "/a.txt", b"same content").await;
        mkdir_and_file(ship, "/d", "/d/c.txt", b"more").await;
    }

    let ra = steward::compute_content_tree(&ship_a).await.expect("a");
    let rb = steward::compute_content_tree(&ship_b).await.expect("b");
    assert_eq!(
        ra.root_tree_hash, rb.root_tree_hash,
        "identical content must hash identically across ponds with different identities"
    );
}

/// Two ponds differing in one file produce different root tree hashes.
#[tokio::test]
async fn differing_content_yields_differing_root() {
    let tmp_a = tempdir().expect("tempdir a");
    let tmp_b = tempdir().expect("tempdir b");
    let mut ship_a = Ship::create_pond(tmp_a.path().join("pond"), "pond-a")
        .await
        .expect("create pond a");
    let mut ship_b = Ship::create_pond(tmp_b.path().join("pond"), "pond-b")
        .await
        .expect("create pond b");

    write_file(&mut ship_a, "/a.txt", b"alpha").await;
    write_file(&mut ship_b, "/a.txt", b"beta").await;

    let ra = steward::compute_content_tree(&ship_a).await.expect("a");
    let rb = steward::compute_content_tree(&ship_b).await.expect("b");
    assert_ne!(ra.root_tree_hash, rb.root_tree_hash);
}
