// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the effective root (chroot) feature.
//!
//! The effective root allows a WD to treat a sub-directory as its root
//! for path resolution. Absolute paths resolve relative to the effective
//! root, `..` is clamped at the boundary, and symlinks cannot escape.

use crate::async_helpers::convenience;
use crate::memory::new_fs;
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
    let content = chrooted
        .read_file_path_to_vec("../file.txt")
        .await
        .unwrap();
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
    _ = convenience::create_file_path(&sub, "a.txt", b"a").await.unwrap();
    _ = convenience::create_file_path(&sub, "b.txt", b"b").await.unwrap();

    // Also create /c.txt at real root (should NOT be matched)
    _ = convenience::create_file_path(&root, "c.txt", b"c").await.unwrap();

    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let matches = chrooted.collect_matches("/*.txt").await.unwrap();

    // Should find a.txt and b.txt but NOT c.txt
    let names: Vec<String> = matches
        .iter()
        .map(|(np, _)| {
            np.path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string()
        })
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
    let content = child
        .read_file_path_to_vec("/dir/file.txt")
        .await
        .unwrap();
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
    _ = convenience::create_file_path(
        &root,
        "/sub/inner/file.txt",
        b"in inner",
    )
    .await
    .unwrap();

    // Symlink from /sub/inner/link -> ../target.txt (goes up to /sub)
    let inner = sub.open_dir_path("inner").await.unwrap();
    _ = inner
        .create_symlink_path("link", "../target.txt")
        .await
        .unwrap();

    // From the actual root, this symlink resolves fine
    let content = root
        .read_file_path_to_vec("/sub/inner/link")
        .await
        .unwrap();
    assert_eq!(content, b"at sub");

    // From chrooted at /sub, the symlink still resolves (stays within /sub)
    let chrooted = root.open_dir_path("/sub").await.unwrap().as_root();
    let content = chrooted
        .read_file_path_to_vec("inner/link")
        .await
        .unwrap();
    assert_eq!(content, b"at sub");
}

#[tokio::test]
async fn test_backward_compat_no_effective_root_change() {
    // Default behavior (effective root = actual root) is unchanged.
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Verify effective root is the actual root
    assert!(root.effective_root().is_root());

    // Create and read files normally
    _ = root.create_dir_path("dir").await.unwrap();
    _ = convenience::create_file_path(&root, "/dir/file.txt", b"content")
        .await
        .unwrap();

    let content = root.read_file_path_to_vec("/dir/file.txt").await.unwrap();
    assert_eq!(content, b"content");

    // Open a sub-directory - effective root should still be actual root
    let wd = root.open_dir_path("/dir").await.unwrap();
    assert!(wd.effective_root().is_root());
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
    assert!(wd.effective_root().is_root());

    // After as_root: effective root is /a
    let chrooted = wd.as_root();
    assert_eq!(chrooted.effective_root().id(), chrooted.node_path().id());
    assert!(!chrooted.effective_root().is_root());
}

#[tokio::test]
async fn test_auto_detect_pond_boundary() {
    // When resolving a path that crosses into a directory with a different
    // pond_id, the effective root is automatically set to the mount point.
    use crate::node::FileID;

    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create /imports/foreign/ as a mount point
    let imports = root.create_dir_path("imports").await.unwrap();

    // Create a foreign directory node with a different pond_id
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs.persistence.create_directory_node(foreign_dir_id).await.unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = imports.insert_node("foreign", foreign_node).await.unwrap();

    // Create a file inside the foreign directory
    let foreign_wd = fs.wd(&foreign_np, root.effective_root().clone()).await.unwrap();
    _ = convenience::create_file_path(&foreign_wd, "data.txt", b"foreign data")
        .await
        .unwrap();

    // Resolve /imports/foreign/data.txt from root
    // This should auto-detect the pond boundary at /imports/
    let (wd, lookup) = root.resolve_path("/imports/foreign/data.txt").await.unwrap();
    assert!(matches!(lookup, Lookup::Found(_)));

    // The returned WD should have effective_root set to /imports/
    // (the parent directory where the pond transition happened)
    assert_eq!(wd.effective_root().id(), imports.node_path().id());
    assert!(!wd.effective_root().is_root());
}

#[tokio::test]
async fn test_auto_detect_absolute_path_scoped_to_mount() {
    // After auto-detection, absolute paths resolve within the mount point.
    use crate::node::FileID;

    let foreign_pond = uuid7::uuid7();
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create /mnt/ as mount point with a foreign sub-directory
    let mnt = root.create_dir_path("mnt").await.unwrap();
    let foreign_dir_id = FileID::new_physical_dir_id(foreign_pond);
    let foreign_node = fs.persistence.create_directory_node(foreign_dir_id).await.unwrap();
    fs.persistence.store_node(&foreign_node).await.unwrap();
    let foreign_np = mnt.insert_node("site", foreign_node).await.unwrap();

    // Create /mnt/site/index.md
    let site_wd = fs.wd(&foreign_np, root.effective_root().clone()).await.unwrap();
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
