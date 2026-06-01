// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! D4 CLI integration test: `pond remote add` -> `pond push` -> `pond pull`
//! roundtrip using a `file://` remote, exercised entirely through the
//! library entry points (no spawned subprocesses).
//!
//! After D5.4, the destination pond is bootstrapped via the public
//! [`steward::Ship::create_replica`] +
//! [`sync_remote::Remote::bootstrap_consumer`] APIs (the
//! sync_steward-only [`sync_remote::Remote::restart_from_compact`] is
//! generic-equivalent via the shared [`sync_remote::Remote::restart_pond_from_compact`]).

use cmd::commands::{
    add_backup_command, add_remote_command, init_command, list_remotes_command, pull_command,
    push_command, remote::remote_config_path,
};
use cmd::common::ShipContext;
use std::sync::Once;
use steward::{PondUserMetadata, REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX, ShipRemoteSteward};
use tempfile::TempDir;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

static INIT_LOG: Once = Once::new();
fn init_log() {
    INIT_LOG.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

/// Build a `ShipContext` for a brand-new pond at `pond_path`.
fn ctx_for(pond_path: &std::path::Path, args: Vec<&str>) -> ShipContext {
    ShipContext {
        pond_path: Some(pond_path.to_path_buf()),
        host_root: None,
        mount_specs: Vec::new(),
        original_args: args.into_iter().map(String::from).collect(),
    }
}

/// Write a small file at `path` within `ship`.  Uses a one-shot write
/// transaction, so the file lives in its own `txn_seq`.
async fn write_small_file(
    ctx: &ShipContext,
    path: &str,
    bytes: &[u8],
    args: Vec<&str>,
) -> anyhow::Result<()> {
    let mut ship = ctx.open_pond().await?;
    ship.write_transaction(
        &PondUserMetadata::new(args.into_iter().map(String::from).collect()),
        async |fs| {
            let root = fs.root().await?;
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
                .await?;
            w.write_all(bytes)
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("write: {}", e)))?;
            w.shutdown()
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("close: {}", e)))?;
            Ok(())
        },
    )
    .await?;
    Ok(())
}

/// Read a file from `ctx` as Vec<u8>.
async fn read_small_file(ctx: &ShipContext, path: &str) -> anyhow::Result<Vec<u8>> {
    let mut ship = ctx.open_pond().await?;
    let tx = ship
        .begin_read(&PondUserMetadata::new(vec![
            "verify".to_string(),
            path.to_string(),
        ]))
        .await?;
    let bytes = {
        let fs = &*tx;
        let root = fs.root().await?;
        root.read_file_path_to_vec(path).await?
    };
    let _ = tx.commit().await?;
    Ok(bytes)
}

/// End-to-end: `pond init` -> write a file -> `pond remote add` -> `pond push`
/// -> bootstrap a second pond -> `pond pull` -> read the file out.
#[tokio::test]
async fn pond_remote_push_pull_roundtrip() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let dst_pond = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    // 1) Source pond + one user write transaction.
    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx).await.expect("init src");
    write_small_file(
        &src_ctx,
        "/hello.txt",
        b"hello from source pond",
        vec!["copy", "hello.txt"],
    )
    .await
    .expect("write hello.txt");

    // 2) Create the remote bucket as a fresh Delta table.  In production
    // this is what `pond remote add` -> first `pond push` would set up;
    // here we do it directly so the test stays focused on the CLI.
    {
        let store_id = {
            let ship = src_ctx.open_pond().await.expect("open src");
            ship.control_table().pond_id_uuid()
        };
        std::fs::create_dir_all(&remote_path).expect("mkdir remote");
        let _ = sync_remote::Remote::create_at_url(&remote_url, store_id, Default::default())
            .await
            .expect("create remote");
    }

    // 3) `pond backup add origin file://...`
    add_backup_command(
        &src_ctx,
        "origin",
        &remote_url,
        false, // push-only
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("remote add");

    // Verify the YAML actually landed in /sys/remotes/origin.
    {
        let bytes = read_small_file(&src_ctx, &remote_config_path("origin"))
            .await
            .expect("read attachment yaml");
        let s = String::from_utf8(bytes).unwrap();
        assert!(
            s.contains(&format!("url: {}", remote_url)),
            "yaml should contain url; got:\n{}",
            s
        );
    }

    // Mode was persisted via raw_config.
    {
        let ship = src_ctx.open_pond().await.expect("open src");
        let mode = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}origin"))
            .await
            .expect("get mode")
            .expect("mode set");
        assert_eq!(mode, "push");
    }

    // 4) `pond push` -- pushes everything > last_pushed_seq.
    push_command(&src_ctx, Some("origin".to_string()))
        .await
        .expect("push");

    // 5) Bootstrap dst pond as a replica of src + first-pull via the
    //    public APIs (D5.4).  No more manual `create_pond_for_restoration
    //    + raw_config_set("last_pulled_seq:<url>", "1")` workaround.
    let remote_for_pull = sync_remote::Remote::open_at_url(&remote_url, Default::default())
        .await
        .expect("open remote for bootstrap");
    {
        let mut dst = steward::Ship::create_replica(&dst_pond, remote_for_pull.store_id())
            .await
            .expect("create dst replica");
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        remote_for_pull
            .bootstrap_consumer(&mut adapter)
            .await
            .expect("bootstrap dst from remote");
    }

    // Re-attach origin on dst as a pull-mode remote.  The bootstrap
    // inherited it as mode=push from src (because remote attachment YAML
    // is portable and pushed with every commit); override here so the
    // default-pull dispatcher finds it.  PATH=`/` because dst is a
    // mirror of src (same pond_id, populated via `create_replica`).
    let dst_ctx = ctx_for(&dst_pond, vec!["pond", "pull"]);
    add_remote_command(
        &dst_ctx,
        "origin",
        &remote_url,
        "/",
        None,
        None,
        None,
        None,
        false,
        true, // overwrite the inherited attachment
    )
    .await
    .expect("re-attach origin on dst");

    // 6) Pull once more via the production command path -- after
    //    bootstrap_consumer this is a no-op (already caught up), but
    //    exercises the wired CLI plumbing.
    pull_command(&dst_ctx, Some("origin".to_string()))
        .await
        .expect("pull origin on dst");

    // 7) The file pushed from src should be readable in dst.
    let bytes = read_small_file(&dst_ctx, "/hello.txt")
        .await
        .expect("read /hello.txt on dst");
    assert_eq!(bytes, b"hello from source pond");

    // 8) `pond remote list` on the SRC pond runs cleanly (prints to
    // stdout; smoke check that the YAML-backed listing works).
    list_remotes_command(&src_ctx, None)
        .await
        .expect("list src");
}

/// `pond push` with no remotes is a no-op success.
#[tokio::test]
async fn pond_push_no_remotes_is_noop() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");
    push_command(&ctx, None).await.expect("push noop");
}

/// `pond pull` with no remotes is a no-op success.
#[tokio::test]
async fn pond_pull_no_remotes_is_noop() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");
    pull_command(&ctx, None).await.expect("pull noop");
}

/// `pond remote add` rejects duplicate names without --overwrite.
#[tokio::test]
async fn pond_remote_add_rejects_duplicate() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    // Two distinct empty directories that `add` will auto-init as
    // fresh remote Delta tables (with our pond's pond_id as store_id).
    let remote_a = scratch.path().join("remote_a");
    let remote_b = scratch.path().join("remote_b");
    let url_a = format!("file://{}", remote_a.display());
    let url_b = format!("file://{}", remote_b.display());
    std::fs::create_dir_all(&remote_a).expect("mkdir remote_a");
    std::fs::create_dir_all(&remote_b).expect("mkdir remote_b");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");

    add_backup_command(
        &ctx, "origin", &url_a, false, None, None, None, None, false, false,
    )
    .await
    .expect("first add");

    let err = add_backup_command(
        &ctx, "origin", &url_b, false, None, None, None, None, false, false,
    )
    .await
    .expect_err("duplicate add should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("already exists") || msg.contains("--overwrite"),
        "expected duplicate-error message, got: {}",
        msg
    );

    // --overwrite succeeds.
    add_backup_command(
        &ctx, "origin", &url_b, false, None, None, None, None, false, true, // overwrite
    )
    .await
    .expect("overwrite add");
}

/// D4.4 post-commit auto-push: a normal write transaction (no explicit
/// `pond push`) should publish to every `/sys/remotes/*` entry whose
/// mode is `push` or `both`.  We verify this by pulling on a second
/// pond and reading back the file written on src AFTER `remote add`.
#[tokio::test]
async fn post_commit_auto_push_publishes_to_file_remote() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let dst_pond = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    // 1) Source pond.
    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx).await.expect("init src");

    // 2) Create the remote bucket as a fresh Delta table.
    let store_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_remote::Remote::create_at_url(&remote_url, store_id, Default::default())
        .await
        .expect("create remote");

    // 3) `pond backup add origin` (push-only).
    add_backup_command(
        &src_ctx,
        "origin",
        &remote_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("remote add");

    // 4) Write a file -- this is the ONLY write that crosses the auto-push
    // threshold; we never call `push_command` directly.  The
    // StewardTransactionGuard::commit() path should trigger
    // `run_post_commit_remotes` and forward seq -> remote.
    write_small_file(
        &src_ctx,
        "/auto.txt",
        b"published by auto-push",
        vec!["copy", "auto.txt"],
    )
    .await
    .expect("write auto.txt");

    // Watermark on src should reflect that the write was pushed.
    let upper_seq = {
        let mut ship = src_ctx.open_pond().await.expect("reopen src");
        ship.as_pond_mut().expect("pond steward").last_write_seq()
    };
    let last_pushed = {
        let ship = src_ctx.open_pond().await.expect("reopen src");
        ship.control_table()
            .raw_config_get(&format!("last_pushed_seq:{}", remote_url))
            .await
            .expect("get watermark")
            .expect("watermark set by auto-push")
    };
    assert_eq!(
        last_pushed,
        upper_seq.to_string(),
        "auto-push should advance last_pushed_seq to last_write_seq"
    );

    // 5) Bootstrap dst as a replica of the remote via the D5.4 public
    //    APIs (no more manual create_pond_for_restoration + raw_config_set
    //    workaround).
    {
        let remote = sync_remote::Remote::open_at_url(&remote_url, Default::default())
            .await
            .expect("open remote for bootstrap");
        let mut dst = steward::Ship::create_replica(&dst_pond, remote.store_id())
            .await
            .expect("create dst replica");
        let mut adapter = ShipRemoteSteward::new(&mut dst);
        remote
            .bootstrap_consumer(&mut adapter)
            .await
            .expect("bootstrap dst from remote");
    }

    // 6) The auto-pushed file should be visible on dst.
    let dst_ctx = ctx_for(&dst_pond, vec!["pond", "pull"]);
    let bytes = read_small_file(&dst_ctx, "/auto.txt")
        .await
        .expect("read /auto.txt on dst");
    assert_eq!(bytes, b"published by auto-push");
}

/// D4.4: a remote with mode=pull must NOT be auto-pushed to.  We
/// verify the watermark `last_pushed_seq:<url>` stays unset after a
/// write transaction.
#[tokio::test]
async fn post_commit_auto_push_skips_pull_mode_remotes() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx).await.expect("init src");

    // Create the bucket so any erroneous push would actually succeed
    // and update the watermark; the test would catch that.
    let store_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_remote::Remote::create_at_url(&remote_url, store_id, Default::default())
        .await
        .expect("create remote");

    add_remote_command(
        &src_ctx,
        "origin",
        &remote_url,
        "/", // pull-mode mirror at root -- store_id will match local
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("remote add (pull)");

    write_small_file(
        &src_ctx,
        "/nopush.txt",
        b"should not be auto-pushed",
        vec!["copy", "nopush.txt"],
    )
    .await
    .expect("write nopush.txt");

    let watermark = {
        let ship = src_ctx.open_pond().await.expect("reopen src");
        ship.control_table()
            .raw_config_get(&format!("last_pushed_seq:{}", remote_url))
            .await
            .expect("get watermark")
    };
    assert!(
        watermark.is_none() || watermark.as_deref() == Some(""),
        "pull-mode remote must not have last_pushed_seq set; got {:?}",
        watermark
    );
}

/// `pond remote add --mode push` against an empty directory should
/// initialize a fresh Delta table at the URL (no separate
/// `Remote::create_at_url` call required).  The subsequent push then
/// works without any manual bootstrap, which is the production CLI
/// contract: `pond init` -> `pond remote add` -> `pond push` is a
/// complete sequence.
#[tokio::test]
async fn pond_remote_add_auto_initializes_fresh_remote() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("src_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let src_ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&src_ctx).await.expect("init src");

    // No Remote::create_at_url call here -- add_backup_command must do it.
    add_backup_command(
        &src_ctx,
        "origin",
        &remote_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("remote add (auto-init)");

    // The remote should now exist as a Delta table with our pond_id
    // as its store_id.
    let expected_pond_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    let remote = sync_remote::Remote::open_at_url(&remote_url, Default::default())
        .await
        .expect("open auto-initialized remote");
    assert_eq!(
        remote.store_id(),
        expected_pond_id,
        "auto-initialized remote must carry our pond_id as store_id"
    );

    // And a subsequent push (with no pre-init) must succeed.
    write_small_file(&src_ctx, "/hello.txt", b"hello", vec!["copy", "hello.txt"])
        .await
        .expect("write hello.txt");
    // The post-commit auto-push already published; an explicit push
    // should be a no-op rather than an error.
    push_command(&src_ctx, Some("origin".to_string()))
        .await
        .expect("explicit push after auto-init");
}

/// `pond remote add --mode push` against a URL that already holds a
/// FOREIGN pond's Delta table (different store_id) must refuse.
/// Otherwise we'd silently overwrite or corrupt the foreign pond's
/// remote.
#[tokio::test]
async fn pond_remote_add_push_refuses_foreign_store_id() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("src_pond");
    let remote_path = scratch.path().join("foreign_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    // Pre-create the remote with a DIFFERENT store_id (simulating a
    // foreign pond's remote).
    let foreign_store_id = uuid::Uuid::new_v4();
    let _ = sync_remote::Remote::create_at_url(&remote_url, foreign_store_id, Default::default())
        .await
        .expect("create foreign remote");

    let src_ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&src_ctx).await.expect("init src");

    let err = add_backup_command(
        &src_ctx,
        "origin",
        &remote_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("push-mode add against foreign store_id should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("does not match") && msg.contains("foreign pond"),
        "expected store_id-mismatch error, got: {}",
        msg
    );
}

/// `pond remote add --mode pull` against an EMPTY URL must refuse:
/// a consumer cannot initialize an upstream pond; the operator must
/// set up the upstream first.
#[tokio::test]
async fn pond_remote_add_pull_refuses_empty_remote() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("empty_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir empty remote");

    let dst_ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&dst_ctx).await.expect("init dst");

    let err = add_remote_command(
        &dst_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("pull-mode add against empty remote should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("not a Delta table") && msg.contains("pull-mode"),
        "expected empty-remote-refusal error, got: {}",
        msg
    );
}

/// D5.7b.1: `pond remote add` persists the mount path under
/// `remote_mount_path:<name>` in raw_config; `pond backup add` leaves
/// the key empty.
#[tokio::test]
async fn pond_remote_add_persists_mount_path() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");

    // Two remotes: an upstream we'll mount as a pull, and a backup we
    // attach for push.
    let upstream_path = scratch.path().join("upstream_bucket");
    let upstream_url = format!("file://{}", upstream_path.display());
    let backup_path = scratch.path().join("backup_bucket");
    let backup_url = format!("file://{}", backup_path.display());
    std::fs::create_dir_all(&upstream_path).expect("mkdir upstream");
    std::fs::create_dir_all(&backup_path).expect("mkdir backup");

    // Initialize upstream remote with a foreign store_id so the pull
    // attach is allowed (any store_id is OK for pull).
    let foreign_id = uuid::Uuid::new_v4();
    let _ = sync_remote::Remote::create_at_url(&upstream_url, foreign_id, Default::default())
        .await
        .expect("init upstream");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");

    // Pull attach with an explicit non-root mount path.
    add_remote_command(
        &ctx,
        "upstream",
        &upstream_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("pull attach");

    // Backup attach (push-only, no mount path).
    add_backup_command(
        &ctx,
        "origin",
        &backup_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("backup attach");

    // Verify control-table state.
    let ship = ctx.open_pond().await.expect("reopen");
    let upstream_mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}upstream"))
        .await
        .expect("get mount key");
    assert_eq!(
        upstream_mount.as_deref(),
        Some("/imports/upstream"),
        "pull attach must record mount path"
    );
    let origin_mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}origin"))
        .await
        .expect("get mount key for backup");
    assert!(
        origin_mount.as_deref().is_none_or(str::is_empty),
        "backup attach must leave mount path empty, got {:?}",
        origin_mount
    );

    let upstream_mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}upstream"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(upstream_mode, "pull");
    let origin_mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}origin"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(origin_mode, "push");
}

/// D5.7b.1: `pond backup add --bidirectional` (i.e. mode=both) keeps the
/// push initialization semantics AND records mode=both for the pull
/// dispatcher.
#[tokio::test]
async fn pond_backup_add_bidirectional_records_both_mode() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");

    add_backup_command(
        &ctx,
        "origin",
        &remote_url,
        true, // bidirectional
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("bidirectional backup attach");

    let ship = ctx.open_pond().await.expect("reopen");
    let mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}origin"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(mode, "both", "bidirectional backup should record mode=both");
}

/// D5.7b.1: `pond remote add` rejects a non-absolute mount path.
#[tokio::test]
async fn pond_remote_add_rejects_relative_mount_path() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");

    let err = add_remote_command(
        &ctx,
        "upstream",
        &remote_url,
        "imports/upstream", // not absolute
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("relative path should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("must be absolute") && msg.contains("imports/upstream"),
        "expected absolute-path-required error, got: {}",
        msg
    );
}

/// D5.7b.1: `pond remote remove` clears both mode AND mount-path keys
/// from raw_config (no orphaned state in the control table).
#[tokio::test]
async fn pond_remote_remove_clears_mount_path_key() {
    use cmd::commands::remove_remote_command;
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    // Pre-init the remote with a foreign store_id (pull attach).
    let foreign_id = uuid::Uuid::new_v4();
    let _ = sync_remote::Remote::create_at_url(&remote_url, foreign_id, Default::default())
        .await
        .expect("create foreign remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx).await.expect("init");

    add_remote_command(
        &ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("attach");

    remove_remote_command(&ctx, "upstream")
        .await
        .expect("remove");

    let ship = ctx.open_pond().await.expect("reopen");
    let mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}upstream"))
        .await
        .expect("get mount key");
    assert!(
        mount.as_deref().is_none_or(str::is_empty),
        "mount path key must be cleared after remove, got {:?}",
        mount
    );
}
