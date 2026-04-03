// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the effective root (chroot) feature.
//!
//! The effective root allows a WD to treat a sub-directory as its root
//! for path resolution. Absolute paths resolve relative to the effective
//! root, `..` is clamped at the boundary, and symlinks cannot escape.
//! Foreign directories (different pond_id) are read-only.

use crate::async_helpers::convenience;
use crate::error::Error;
use crate::memory::new_fs;
use crate::node::FileID;
use crate::wd::Lookup;

#[tokio::test]
async fn test_absolute_path_from_effective_root() {
    // Paths starting with "/" resolve from the effective root,
    // not the global filesystem root.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Build: /sub/site/index.md
    let sub = root.create_dir_path("sub").await.unwrap();
    _ = sub.create_dir_path("site").await.unwrap();
    _ = convenience::create_file_path(&root, "/sub/site/index.md", b"hello from sub")
        .await
        .unwrap();

    // Also create /site/index.md at the real root
    _ = root.create_dir_path("site").await.unwrap();
    _ = convenience::create_file_path(&root, "/site/index.md", b"hello from root")
        .await
        .unwrap();

    // Without effective root: /site/index.md resolves from global root
    let content = root.read_file_path_to_vec("/site/index.md").await.unwrap();
    assert_eq!(content, b"hello from root");

    // With effective root at /sub: /site/index.md resolves to /sub/site/index.md
    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let content = chrooted
        .read_file_path_to_vec("/site/index.md")
        .await
        .unwrap();
    assert_eq!(content, b"hello from sub");
}

#[tokio::test]
async fn test_parent_dir_clamped_at_effective_root() {
    // `..` at the effective root boundary stays at the effective root
    // (chroot semantics) instead of erroring.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Build: /sub/file.txt
    let sub = root.create_dir_path("sub").await.unwrap();
    _ = convenience::create_file_path(&sub, "file.txt", b"in sub")
        .await
        .unwrap();

    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();

    // `../file.txt` from the effective root should stay at root and find file.txt
    let content = chrooted.read_file_path_to_vec("../file.txt").await.unwrap();
    assert_eq!(content, b"in sub");

    // Multiple `..` should also be clamped
    let content = chrooted
        .read_file_path_to_vec("../../../file.txt")
        .await
        .unwrap();
    assert_eq!(content, b"in sub");
}

#[tokio::test]
async fn test_glob_with_effective_root() {
    // Glob patterns with leading `/` should work from effective root.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Build: /sub/a.txt, /sub/b.txt
    let sub = root.create_dir_path("sub").await.unwrap();
    _ = convenience::create_file_path(&sub, "a.txt", b"a")
        .await
        .unwrap();
    _ = convenience::create_file_path(&sub, "b.txt", b"b")
        .await
        .unwrap();

    // Also create /c.txt at real root (should NOT be matched)
    _ = convenience::create_file_path(&root, "c.txt", b"c")
        .await
        .unwrap();

    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let matches = chrooted.collect_matches("/*.txt").await.unwrap();

    // Should find a.txt and b.txt but NOT c.txt
    let names: Vec<String> = matches
        .iter()
        .map(|(np, _)| np.path.file_name().unwrap().to_string_lossy().to_string())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"a.txt".to_string()));
    assert!(names.contains(&"b.txt".to_string()));
}

#[tokio::test]
async fn test_child_wd_inherits_effective_root() {
    // WDs created via open_dir_path should inherit the effective root.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Build: /sub/dir/file.txt
    let sub = root.create_dir_path("sub").await.unwrap();
    _ = sub.create_dir_path("dir").await.unwrap();
    _ = convenience::create_file_path(&root, "/sub/dir/file.txt", b"deep")
        .await
        .unwrap();

    // Also create /dir/file.txt at real root
    _ = root.create_dir_path("dir").await.unwrap();
    _ = convenience::create_file_path(&root, "/dir/file.txt", b"shallow")
        .await
        .unwrap();

    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let child = chrooted.open_dir_path("dir").await.unwrap();

    // From child, `/dir/file.txt` should still resolve within effective root
    let content = child.read_file_path_to_vec("/dir/file.txt").await.unwrap();
    assert_eq!(content, b"deep");
}

#[tokio::test]
async fn test_symlink_absolute_target_within_effective_root() {
    // Symlinks with absolute targets resolve from effective root.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Build: /sub/target.txt, /sub/link -> /target.txt
    let sub = root.create_dir_path("sub").await.unwrap();
    _ = convenience::create_file_path(&sub, "target.txt", b"linked content")
        .await
        .unwrap();
    _ = sub
        .create_symlink_path("link", "/target.txt")
        .await
        .unwrap();

    // From chrooted WD, follow the symlink
    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let content = chrooted.read_file_path_to_vec("link").await.unwrap();
    assert_eq!(content, b"linked content");
}

#[tokio::test]
async fn test_symlink_relative_clamped_at_effective_root() {
    // Relative symlinks that would escape above the effective root
    // are contained. `normalize` prevents `..` beyond the stack base,
    // and the effective root forms that base.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Build: /sub/inner/target.txt, /sub/inner/link -> ../target.txt
    let sub = root.create_dir_path("sub").await.unwrap();
    _ = convenience::create_file_path(&sub, "target.txt", b"at sub")
        .await
        .unwrap();
    _ = sub.create_dir_path("inner").await.unwrap();
    _ = convenience::create_file_path(&root, "/sub/inner/file.txt", b"in inner")
        .await
        .unwrap();

    // Symlink from /sub/inner/link -> ../target.txt (goes up to /sub)
    let inner = sub.open_dir_path("inner").await.unwrap();
    _ = inner
        .create_symlink_path("link", "../target.txt")
        .await
        .unwrap();

    // From the actual root, this symlink resolves fine
    let content = root.read_file_path_to_vec("/sub/inner/link").await.unwrap();
    assert_eq!(content, b"at sub");

    // From chrooted at /sub, the symlink still resolves (stays within /sub)
    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let content = chrooted.read_file_path_to_vec("inner/link").await.unwrap();
    assert_eq!(content, b"at sub");
}

#[tokio::test]
async fn test_backward_compat_no_effective_root_change() {
    // Default behavior (effective root = actual root) is unchanged.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Verify effective root is the actual root
    assert!(root.effective_root().id().has_root_ids());

    // Create and read files normally
    _ = root.create_dir_path("dir").await.unwrap();
    _ = convenience::create_file_path(&root, "/dir/file.txt", b"content")
        .await
        .unwrap();

    let content = root.read_file_path_to_vec("/dir/file.txt").await.unwrap();
    assert_eq!(content, b"content");

    // Open a sub-directory - effective root should still be actual root
    let wd = root.open_dir_path("/dir").await.unwrap();
    assert!(wd.effective_root().id().has_root_ids());
}

#[tokio::test]
async fn test_resolve_root_from_effective_root() {
    // Resolving "/" from a chrooted WD should return the effective root.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    _ = root.create_dir_path("sub").await.unwrap();
    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();

    let (wd, lookup) = chrooted.resolve_path("/").await.unwrap();
    match lookup {
        Lookup::Found(node) => {
            // The found node should be the /sub directory, not the global root
            assert_eq!(node.id(), chrooted.node_path().id());
        }
        other => panic!("Expected Found, got {:?}", std::mem::discriminant(&other)),
    }
    // The returned WD should also be at the effective root
    assert!(wd.is_at_root_boundary());
}

#[tokio::test]
async fn test_as_root_sets_effective_root_to_current() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    _ = root.create_dir_path("a").await.unwrap();
    let wd = root.open_dir_path("/a").await.unwrap();

    // Before as_root: effective root is the actual root
    assert!(wd.effective_root().id().has_root_ids());

    // After as_root: effective root is /a
    let chrooted = wd.as_root();
    assert_eq!(chrooted.effective_root().id(), chrooted.node_path().id());
    assert!(!chrooted.effective_root().id().has_root_ids());
}

#[tokio::test]
async fn test_auto_detect_pond_boundary() {
    // When resolving a path that crosses into a directory with a different
    // pond_id, the effective root is automatically set to the mount point.
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create /imports/foreign/ as a mount point
    let imports = root.create_dir_path("imports").await.unwrap();

    // Create a foreign directory node with a different pond_id
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = imports.insert_node("foreign", foreign_node).await.unwrap();

    // Create a file inside the foreign directory
    // Use the foreign dir as its own root (simulates the producer's perspective)
    let foreign_wd = fs.wd(&foreign_np, foreign_np.clone()).await.unwrap();
    _ = convenience::create_file_path(&foreign_wd, "data.txt", b"foreign data")
        .await
        .unwrap();

    // Resolve /imports/foreign/data.txt from root
    // This should auto-detect the pond boundary at /imports/
    let (wd, lookup) = root
        .resolve_path("/imports/foreign/data.txt")
        .await
        .unwrap();
    assert!(matches!(lookup, Lookup::Found(_)));

    // The returned WD should have effective_root set to /imports/
    // (the parent directory where the pond transition happened)
    assert_eq!(wd.effective_root().id(), imports.node_path().id());
    assert!(!wd.effective_root().id().has_root_ids());
}

#[tokio::test]
async fn test_auto_detect_absolute_path_scoped_to_mount() {
    // After auto-detection, absolute paths resolve within the mount point.
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create /mnt/ as mount point with a foreign sub-directory
    let mnt = root.create_dir_path("mnt").await.unwrap();
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = mnt.insert_node("site", foreign_node).await.unwrap();

    // Create /mnt/site/index.md
    // Use the foreign dir as its own root (simulates the producer's perspective)
    let site_wd = fs.wd(&foreign_np, foreign_np.clone()).await.unwrap();
    _ = convenience::create_file_path(&site_wd, "index.md", b"foreign site")
        .await
        .unwrap();

    // Also create /site/index.md at the real root (different content)
    _ = root.create_dir_path("site").await.unwrap();
    _ = convenience::create_file_path(&root, "/site/index.md", b"local site")
        .await
        .unwrap();

    // Resolve /mnt/site/index.md — this crosses a pond boundary
    let (wd, _) = root.resolve_path("/mnt/site/index.md").await.unwrap();

    // The WD's effective root is /mnt/ — so /site/index.md resolves
    // to /mnt/site/index.md (foreign content), not the local root
    let content = wd.read_file_path_to_vec("/site/index.md").await.unwrap();
    assert_eq!(content, b"foreign site");

    // From the global root, /site/index.md still resolves to local content
    let local_content = root.read_file_path_to_vec("/site/index.md").await.unwrap();
    assert_eq!(local_content, b"local site");
}

// --- Read-only foreign import guard tests ---

/// Helper: sets up a filesystem with /mnt/ (local) containing a foreign
/// directory "foreign" with a file "data.txt" inside it.
/// Returns (fs, root, mnt_wd, foreign_np).
async fn setup_foreign_mount() -> (
    crate::fs::FS,
    crate::wd::WD,
    crate::wd::WD,
    crate::node::NodePath,
) {
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    let mnt = root.create_dir_path("mnt").await.unwrap();

    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = mnt.insert_node("foreign", foreign_node).await.unwrap();

    // Populate the foreign dir as if we are the producer (its own root)
    let producer_wd = fs.wd(&foreign_np, foreign_np.clone()).await.unwrap();
    _ = convenience::create_file_path(&producer_wd, "data.txt", b"foreign data")
        .await
        .unwrap();

    (fs, root, mnt, foreign_np)
}

#[tokio::test]
async fn test_cannot_create_dir_in_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    // Navigating into the foreign dir and creating a subdirectory must fail
    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    let err = foreign_wd
        .create_dir_path("newdir")
        .await
        .expect_err("should reject directory creation in foreign import");
    assert!(
        matches!(err, Error::ReadOnlyImport(_)),
        "expected ReadOnlyImport, got: {err:?}"
    );
}

#[tokio::test]
async fn test_cannot_create_dir_all_in_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    let err = foreign_wd
        .create_dir_all("deep/nested/dir")
        .await
        .expect_err("should reject create_dir_all in foreign import");
    assert!(
        matches!(err, Error::ReadOnlyImport(_)),
        "expected ReadOnlyImport, got: {err:?}"
    );
}

#[tokio::test]
async fn test_cannot_create_dir_via_absolute_path_into_foreign() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    // Even from the global root, creating inside the foreign mount must fail
    let err = root
        .create_dir_path("/mnt/foreign/newdir")
        .await
        .expect_err("should reject directory creation via absolute path");
    assert!(
        matches!(err, Error::ReadOnlyImport(_)),
        "expected ReadOnlyImport, got: {err:?}"
    );
}

#[tokio::test]
async fn test_cannot_create_file_in_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    match convenience::create_file_path(&foreign_wd, "newfile.txt", b"nope").await {
        Err(Error::ReadOnlyImport(_)) => {} // expected
        Err(e) => panic!("expected ReadOnlyImport, got: {e:?}"),
        Ok(_) => panic!("should reject file creation in foreign import"),
    }
}

#[tokio::test]
async fn test_cannot_write_to_existing_foreign_file() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    match foreign_wd.async_writer_path("data.txt").await {
        Err(Error::ReadOnlyImport(_)) => {} // expected
        Err(e) => panic!("expected ReadOnlyImport, got: {e:?}"),
        Ok(_) => panic!("should reject writing to existing foreign file"),
    }
}

#[tokio::test]
async fn test_cannot_create_symlink_in_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    match foreign_wd.create_symlink_path("link", "/data.txt").await {
        Err(Error::ReadOnlyImport(_)) => {} // expected
        Err(e) => panic!("expected ReadOnlyImport, got: {e:?}"),
        Ok(_) => panic!("should reject symlink creation in foreign import"),
    }
}

#[tokio::test]
async fn test_cannot_rename_in_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    let err = foreign_wd
        .rename_entry("data.txt", "renamed.txt")
        .await
        .expect_err("should reject rename in foreign import");
    assert!(
        matches!(err, Error::ReadOnlyImport(_)),
        "expected ReadOnlyImport, got: {err:?}"
    );
}

#[tokio::test]
async fn test_cannot_remove_in_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    let err = foreign_wd
        .remove_entry("data.txt")
        .await
        .expect_err("should reject remove in foreign import");
    assert!(
        matches!(err, Error::ReadOnlyImport(_)),
        "expected ReadOnlyImport, got: {err:?}"
    );
}

#[tokio::test]
async fn test_can_read_foreign_import() {
    let (_fs, root, _mnt, _foreign_np) = setup_foreign_mount().await;

    // Reading must still work
    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    let content = foreign_wd.read_file_path_to_vec("data.txt").await.unwrap();
    assert_eq!(content, b"foreign data");
}

#[tokio::test]
async fn test_can_create_local_sibling_next_to_foreign_mount() {
    let (_fs, _root, mnt, _foreign_np) = setup_foreign_mount().await;

    // Creating a local directory as a sibling of the foreign mount must work
    let local_dir = mnt.create_dir_path("local-stuff").await.unwrap();
    _ = convenience::create_file_path(&local_dir, "notes.txt", b"local notes")
        .await
        .unwrap();
    let content = local_dir.read_file_path_to_vec("notes.txt").await.unwrap();
    assert_eq!(content, b"local notes");
}

#[tokio::test]
async fn test_insert_node_still_works_for_import() {
    // insert_node is the mechanism used by the import system and must
    // remain functional even on directories that contain foreign content.
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    let mnt = root.create_dir_path("mnt").await.unwrap();

    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();

    // insert_node should succeed (it's the import path)
    let np = mnt.insert_node("imported", foreign_node).await.unwrap();
    assert_eq!(np.id().pond_id(), foreign_pond);
}

// --- Gap 2: Symlinks crossing a pond boundary ---

#[tokio::test]
async fn test_symlink_crossing_pond_boundary() {
    // A symlink in the local tree points into a foreign directory.
    // Resolution should detect the pond boundary and set effective_root.
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create /mnt/foreign/ with foreign pond_id and a file inside
    let mnt = root.create_dir_path("mnt").await.unwrap();
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = mnt.insert_node("foreign", foreign_node).await.unwrap();

    // Populate via producer perspective
    let producer_wd = fs.wd(&foreign_np, foreign_np.clone()).await.unwrap();
    _ = convenience::create_file_path(&producer_wd, "secret.txt", b"foreign secret")
        .await
        .unwrap();

    // Create a local symlink /shortcut -> /mnt/foreign
    _ = root
        .create_symlink_path("shortcut", "/mnt/foreign")
        .await
        .unwrap();

    // Following the symlink should cross the pond boundary
    let (wd, lookup) = root.resolve_path("/shortcut/secret.txt").await.unwrap();
    assert!(matches!(lookup, Lookup::Found(_)));

    // The boundary should have been detected
    assert_eq!(wd.effective_root().id(), mnt.node_path().id());

    // Reading through the symlink works
    let content = root
        .read_file_path_to_vec("/shortcut/secret.txt")
        .await
        .unwrap();
    assert_eq!(content, b"foreign secret");

    // But writing through the symlink into the foreign dir is blocked
    let foreign_via_link = root.open_dir_path("/shortcut").await.unwrap();
    match convenience::create_file_path(&foreign_via_link, "nope.txt", b"x").await {
        Err(Error::ReadOnlyImport(_)) => {} // expected
        Err(e) => panic!("expected ReadOnlyImport, got: {e:?}"),
        Ok(_) => panic!("should block writes through symlink into foreign dir"),
    }
}

// --- Gap 3: pond_id stamping on locally created dirs near foreign mount ---

#[tokio::test]
async fn test_local_dir_near_foreign_mount_gets_local_pond_id() {
    // Directories created as siblings of a foreign mount should carry
    // the local pond_id, not the foreign one.
    let (fs, root, mnt, foreign_np) = setup_foreign_mount().await;

    let local_pond_id = root.node_path().id().pond_id();
    let foreign_pond_id = foreign_np.id().pond_id();
    assert_ne!(local_pond_id, foreign_pond_id);

    // Create a local sibling
    let local_dir = mnt.create_dir_path("local-sibling").await.unwrap();
    assert_eq!(
        local_dir.node_path().id().pond_id(),
        local_pond_id,
        "local sibling should have local pond_id"
    );

    // Create a nested local dir
    let nested = local_dir.create_dir_path("nested").await.unwrap();
    assert_eq!(
        nested.node_path().id().pond_id(),
        local_pond_id,
        "nested local dir should have local pond_id"
    );

    // create_dir_all should also stamp local pond_id
    let deep = mnt.create_dir_all("a/b/c").await.unwrap();
    assert_eq!(
        deep.node_path().id().pond_id(),
        local_pond_id,
        "create_dir_all should stamp local pond_id"
    );

    // Verify via open_dir_path that the intermediate dirs also got local ids
    let a = mnt.open_dir_path("a").await.unwrap();
    assert_eq!(a.node_path().id().pond_id(), local_pond_id);
    let ab = mnt.open_dir_path("a/b").await.unwrap();
    assert_eq!(ab.node_path().id().pond_id(), local_pond_id);

    // The foreign dir still has its own pond_id
    assert_eq!(foreign_np.id().pond_id(), foreign_pond_id);

    _ = fs; // keep fs alive
}

// --- Gap 4: Nested pond boundaries ---

#[tokio::test]
async fn test_nested_foreign_mounts_first_boundary_wins() {
    // Two levels of foreign directories with different pond_ids.
    // The effective_root should be set at the first boundary crossing.
    let foreign_pond_1 = uuid7::uuid7();
    let foreign_pond_2 = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create /mnt/level1/ with foreign_pond_1
    let mnt = root.create_dir_path("mnt").await.unwrap();
    let level1_id = FileID::new_physical_dir_id(foreign_pond_1);
    let level1_node = fs
        .persistence
        .create_directory_node(level1_id)
        .await
        .unwrap();
    fs.persistence.store_node(&level1_node).await.unwrap();
    let level1_np = mnt.insert_node("level1", level1_node).await.unwrap();

    // Inside level1, create level2 with foreign_pond_2
    let level1_wd = fs.wd(&level1_np, level1_np.clone()).await.unwrap();
    let level2_id = FileID::new_physical_dir_id(foreign_pond_2);
    let level2_node = fs
        .persistence
        .create_directory_node(level2_id)
        .await
        .unwrap();
    fs.persistence.store_node(&level2_node).await.unwrap();
    let level2_np = level1_wd.insert_node("level2", level2_node).await.unwrap();

    // Create a file in level2
    let level2_wd = fs.wd(&level2_np, level2_np.clone()).await.unwrap();
    _ = convenience::create_file_path(&level2_wd, "deep.txt", b"very deep")
        .await
        .unwrap();

    // Resolve from root — should detect boundary at mnt (first crossing)
    let (wd, lookup) = root
        .resolve_path("/mnt/level1/level2/deep.txt")
        .await
        .unwrap();
    assert!(matches!(lookup, Lookup::Found(_)));

    // The effective_root should be /mnt/ (parent of the first foreign dir)
    assert_eq!(
        wd.effective_root().id(),
        mnt.node_path().id(),
        "effective_root should be the first mount point"
    );

    // Reading still works through both levels
    let content = root
        .read_file_path_to_vec("/mnt/level1/level2/deep.txt")
        .await
        .unwrap();
    assert_eq!(content, b"very deep");

    // Writing is blocked at both levels
    let level1_wd = root.open_dir_path("/mnt/level1").await.unwrap();
    let err = level1_wd.create_dir_path("nope").await;
    assert!(
        matches!(err, Err(Error::ReadOnlyImport(_))),
        "writes in level1 should be blocked: {err:?}"
    );

    let level2_wd = root.open_dir_path("/mnt/level1/level2").await.unwrap();
    let err = level2_wd.create_dir_path("nope").await;
    assert!(
        matches!(err, Err(Error::ReadOnlyImport(_))),
        "writes in level2 should also be blocked: {err:?}"
    );
}

// --- Gap 5: Error cases for effective roots ---

#[tokio::test]
async fn test_open_dir_path_propagates_effective_root_on_boundary() {
    // open_dir_path into a foreign dir should set the effective_root
    // on the returned WD (not just discard it).
    let (_fs, root, mnt, _foreign_np) = setup_foreign_mount().await;

    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();

    // The WD should have effective_root = /mnt/ (the mount point)
    assert_eq!(
        foreign_wd.effective_root().id(),
        mnt.node_path().id(),
        "open_dir_path should propagate detected effective_root"
    );
    assert!(!foreign_wd.effective_root().id().has_root_ids());
}

#[tokio::test]
async fn test_effective_root_survives_relative_navigation() {
    // After entering a foreign dir, navigating deeper with relative
    // paths should preserve the effective_root.
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    let mnt = root.create_dir_path("mnt").await.unwrap();
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = mnt.insert_node("foreign", foreign_node).await.unwrap();

    // Producer creates a subdirectory
    let producer_wd = fs.wd(&foreign_np, foreign_np.clone()).await.unwrap();
    let sub = producer_wd.create_dir_path("subdir").await.unwrap();
    _ = convenience::create_file_path(&sub, "file.txt", b"sub content")
        .await
        .unwrap();

    // Navigate into the foreign dir, then deeper via relative path
    let foreign_wd = root.open_dir_path("/mnt/foreign").await.unwrap();
    let sub_wd = foreign_wd.open_dir_path("subdir").await.unwrap();

    // effective_root should still be /mnt/ at every level
    assert_eq!(foreign_wd.effective_root().id(), mnt.node_path().id());
    assert_eq!(sub_wd.effective_root().id(), mnt.node_path().id());

    // Reading works
    let content = sub_wd.read_file_path_to_vec("file.txt").await.unwrap();
    assert_eq!(content, b"sub content");

    // Writing blocked at every depth
    let err = sub_wd.create_dir_path("nope").await;
    assert!(matches!(err, Err(Error::ReadOnlyImport(_))));
}

#[tokio::test]
async fn test_as_root_on_foreign_wd_allows_writes() {
    // If someone explicitly calls as_root() on a foreign WD (producer
    // perspective), the effective_root becomes the foreign dir itself,
    // so pond_ids match and writes are allowed. This is the correct
    // way to populate foreign content during import setup.
    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    let mnt = root.create_dir_path("mnt").await.unwrap();
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs
        .persistence
        .create_directory_node(foreign_dir_id)
        .await
        .unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = mnt.insert_node("foreign", foreign_node).await.unwrap();

    // Use as_root to act as the producer
    let producer_wd = fs.wd(&foreign_np, foreign_np.clone()).await.unwrap();

    // This should succeed — we are the "producer"
    let sub = producer_wd.create_dir_path("works").await.unwrap();
    _ = convenience::create_file_path(&sub, "ok.txt", b"producer wrote this")
        .await
        .unwrap();

    let content = producer_wd
        .read_file_path_to_vec("works/ok.txt")
        .await
        .unwrap();
    assert_eq!(content, b"producer wrote this");
}
