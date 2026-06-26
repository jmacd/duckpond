// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::fsck`: the local filesystem-check that
//! computes a deterministic root checksum over the data table and
//! re-validates file content against recorded blake3 hashes.

use std::sync::Once;

use steward::{FsckOptions, Ship};
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

fn pseudo_random(len: usize) -> Vec<u8> {
    // A cheap incompressible byte stream so the large-file blob is real
    // (constant content would ZSTD-compress to near-nothing, leaving a
    // mid-file corruption landing in parquet padding).
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(&root, path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

/// A clean pond fscks OK, the root is deterministic and identical with or
/// without the content pass, and the root changes when data changes.
#[tokio::test]
async fn fsck_root_is_deterministic_and_data_sensitive() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "fsck-test")
        .await
        .expect("create pond");

    write_file(&mut ship, "/small.txt", b"hello duckpond").await;
    // Force an external large-file blob (> 64 KiB threshold).
    let big = pseudo_random(200_000);
    write_file(&mut ship, "/big.bin", &big).await;

    let full = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck full");
    assert!(full.ok(), "clean pond should fsck OK: {:?}", full.errors);
    assert_eq!(full.root.bytes.len(), 32, "root is a 32-byte BLAKE3");
    assert!(full.blobs_checked >= 1, "the large file blob is verified");
    assert!(full.inline_checked >= 1, "the inline file is verified");

    let again = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck again");
    assert_eq!(full.root, again.root, "root is deterministic across runs");

    let quick = steward::fsck(
        &ship,
        FsckOptions {
            verify_content: false,
        },
    )
    .await
    .expect("fsck quick");
    assert_eq!(
        full.root, quick.root,
        "content pass must not change the root checksum"
    );
    assert_eq!(quick.blobs_checked, 0, "--quick skips blob verification");

    let root_before = full.root.clone();
    write_file(&mut ship, "/another.txt", b"more data").await;

    let after = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck after change");
    assert_ne!(
        root_before, after.root,
        "root must change when data is added"
    );
}

/// Corrupting a large-file blob is detected by the content pass but is
/// invisible to the structural-only `--quick` root (which commits to the
/// recorded blake3, not the bytes).
#[tokio::test]
async fn fsck_detects_corrupted_large_file_blob() {
    init_log();
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, "fsck-test")
        .await
        .expect("create pond");

    let big = pseudo_random(200_000);
    write_file(&mut ship, "/big.bin", &big).await;

    // Locate and corrupt the external blob.
    let large_dir = pond_path.join("data").join("_large_files");
    let blob = std::fs::read_dir(&large_dir)
        .expect("read _large_files")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("blake3=") && n.ends_with(".parquet"))
        })
        .expect("a large-file blob exists");

    let mut bytes = std::fs::read(&blob).expect("read blob");
    let mid = bytes.len() / 2;
    for b in bytes.iter_mut().skip(mid).take(16) {
        *b ^= 0xFF;
    }
    std::fs::write(&blob, &bytes).expect("write corrupted blob");

    let full = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck full");
    assert!(
        !full.ok(),
        "corrupted blob must be detected by the content pass"
    );
    assert_eq!(full.errors.len(), 1, "exactly one row should fail");

    let quick = steward::fsck(
        &ship,
        FsckOptions {
            verify_content: false,
        },
    )
    .await
    .expect("fsck quick");
    assert!(
        quick.ok(),
        "structural-only --quick cannot see blob byte corruption"
    );
}
