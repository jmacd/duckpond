// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::compare_content_trees`: the content-tree
//! comparison that realizes design Section 6.2 / Goal 2.  Two ponds are equal
//! iff their root tree hashes match; otherwise the diff descends by child hash
//! to the minimal set of divergent paths.

use steward::{DiffKind, Ship, compare_content_trees};
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

async fn new_pond(label: &str) -> (tempfile::TempDir, Ship) {
    let tmp = tempdir().expect("tempdir");
    let ship = Ship::create_pond(tmp.path().join("pond"), label)
        .await
        .expect("create pond");
    (tmp, ship)
}

/// Identical content across two ponds compares equal with no differences,
/// regardless of pond identity or lineage.
#[tokio::test]
async fn identical_ponds_compare_equal() {
    let (_ta, mut a) = new_pond("pond-a").await;
    let (_tb, mut b) = new_pond("pond-b").await;

    for ship in [&mut a, &mut b] {
        write_file(ship, "/a.txt", b"hello").await;
        mkdir_and_file(ship, "/sub", "/sub/b.txt", b"world").await;
    }

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(cmp.equal, "identical content must compare equal");
    assert!(cmp.differences.is_empty());
}

/// A single differing file is reported as exactly one `Modified` path; the
/// shared, identical subtree is pruned.
#[tokio::test]
async fn single_modified_file_is_isolated() {
    let (_ta, mut a) = new_pond("pond-a").await;
    let (_tb, mut b) = new_pond("pond-b").await;

    for ship in [&mut a, &mut b] {
        write_file(ship, "/shared.txt", b"same").await;
        mkdir_and_file(ship, "/sub", "/sub/keep.txt", b"identical").await;
    }
    // Diverge one nested file only.
    write_file(&mut a, "/sub/diff.txt", b"left").await;
    write_file(&mut b, "/sub/diff.txt", b"right").await;

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(!cmp.equal);
    assert_eq!(cmp.differences.len(), 1, "exactly one divergent path");
    assert_eq!(cmp.differences[0].path, "/sub/diff.txt");
    assert_eq!(cmp.differences[0].kind, DiffKind::Modified);
}

/// Added and removed entries are classified from the left pond's perspective.
#[tokio::test]
async fn added_and_removed_entries_are_classified() {
    let (_ta, mut a) = new_pond("pond-a").await;
    let (_tb, mut b) = new_pond("pond-b").await;

    // Common base.
    for ship in [&mut a, &mut b] {
        write_file(ship, "/common.txt", b"base").await;
    }
    // Only-left and only-right files.
    write_file(&mut a, "/only_left.txt", b"L").await;
    write_file(&mut b, "/only_right.txt", b"R").await;

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(!cmp.equal);

    let removed: Vec<_> = cmp
        .differences
        .iter()
        .filter(|d| d.kind == DiffKind::Removed)
        .map(|d| d.path.as_str())
        .collect();
    let added: Vec<_> = cmp
        .differences
        .iter()
        .filter(|d| d.kind == DiffKind::Added)
        .map(|d| d.path.as_str())
        .collect();

    assert_eq!(removed, vec!["/only_left.txt"], "left-only -> Removed");
    assert_eq!(added, vec!["/only_right.txt"], "right-only -> Added");
}
