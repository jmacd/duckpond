// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::fetch_object_graph`: the consumer-side
//! fetch walk over a content-addressed remote (design Section 8.5).

use steward::{Ship, fetch_object_graph, push_content_to_remote};
use sync_store::ContentRemote;
use sync_store::content::ObjectHash;
use tempfile::tempdir;
use tinyfs::arrow::parquet::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

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

/// A single-row parquet batch with a `timestamp` (microseconds) column and a
/// string `label`, used to append series versions in tests.
fn series_batch(ts_micros: i64, label: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("label", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![ts_micros])),
            Arc::new(StringArray::from(vec![label])),
        ],
    )
    .expect("series batch")
}

/// Append `versions` to a `TablePhysicalSeries` at `path`, creating it on the
/// first version and appending a new version for each subsequent one.
async fn write_series(ship: &mut Ship, path: &str, versions: &[(i64, &str)]) {
    let path = path.to_string();
    let versions: Vec<(i64, String)> = versions
        .iter()
        .map(|(ts, label)| (*ts, (*label).to_string()))
        .collect();
    ship.write_transaction(&meta("series"), async move |fs| {
        let root = fs.root().await?;
        for (ts, label) in &versions {
            let batch = series_batch(*ts, label);
            let _ = root
                .write_series_from_batch(&path, &batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("series transaction");
}

/// Create a dynamic node (factory + config) at `path` with the given entry
/// type, exercising the recipe path directly without the provider's factory
/// registry (rebuild only needs the stored factory string and config bytes).
async fn write_dynamic(
    ship: &mut Ship,
    path: &str,
    entry_type: tinyfs::EntryType,
    factory: &str,
    config: &[u8],
) {
    let path = path.to_string();
    let factory = factory.to_string();
    let config = config.to_vec();
    ship.write_transaction(&meta("mknod"), async move |fs| {
        let root = fs.root().await?;
        let _ = root
            .create_dynamic_path(&path, entry_type, &factory, config)
            .await?;
        Ok(())
    })
    .await
    .expect("dynamic transaction");
}

async fn push(ship: &Ship) -> (tempfile::TempDir, ContentRemote) {
    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let _ = push_content_to_remote(ship, &mut remote, "main")
        .await
        .expect("push");
    (remote_dir, remote)
}

/// Fetching a pushed pond returns a verified closure whose tip and root tree
/// are present, and every object's bytes hash to its key.
#[tokio::test]
async fn fetch_returns_verified_closure() {
    let (_t, mut ship) = new_pond("fetch").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let (_rt, remote) = push(&ship).await;

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    assert!(!graph.is_empty());
    assert_eq!(graph.tip, remote.get_tip("main").await.expect("tip"));

    // Content-addressing invariant across the whole fetched closure.
    for (hash, bytes) in &graph.bytes {
        assert_eq!(
            ObjectHash::of_bytes(bytes),
            *hash,
            "fetched object must hash to its key"
        );
    }

    // The tip commit's root tree is in the closure.
    let root = graph.root_tree_hash().expect("root tree hash");
    assert!(
        graph.objects.contains_key(&root),
        "root tree must be fetched"
    );

    // The node manifest is fetched and carries one entry per node: the root,
    // both files, and the subdirectory (4 nodes).
    assert_eq!(graph.manifest.len(), 4, "manifest must cover every node");
    assert!(
        graph
            .manifest
            .iter()
            .any(|e| e.parent_node_id.is_empty() && e.name.is_empty()),
        "manifest must contain the root entry"
    );
}

/// Fetching a non-existent ref yields an empty graph, not an error.
#[tokio::test]
async fn fetch_missing_ref_is_empty() {
    let (_t, mut ship) = new_pond("fetch-empty").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    let (_rt, remote) = push(&ship).await;

    let graph = fetch_object_graph(&remote, "does-not-exist")
        .await
        .expect("fetch");
    assert!(graph.is_empty());
    assert!(graph.tip.is_none());
}

/// The fetched closure equals the producer's materialized inline closure plus
/// the commit chain: the consumer fetches exactly what the producer pushed.
#[tokio::test]
async fn fetched_closure_matches_pushed_objects() {
    let (_t, mut ship) = new_pond("fetch-match").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let mat = steward::materialize_content_objects(&ship)
        .await
        .expect("materialize");
    let (_rt, remote) = push(&ship).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    // Every inline materialized object is in the fetched closure.
    for hash in mat.inline.keys() {
        assert!(
            graph.objects.contains_key(hash),
            "materialized object {} must be fetched",
            hash.to_hex()
        );
    }

    // The only fetched objects not in the inline tree closure are commits.
    let commit_hashes: std::collections::BTreeSet<_> =
        graph.commits.iter().map(|(h, _)| *h).collect();
    for hash in graph.objects.keys() {
        assert!(
            mat.inline.contains_key(hash) || commit_hashes.contains(hash),
            "fetched object {} is neither a materialized tree object nor a commit",
            hash.to_hex()
        );
    }
}

/// The full round trip: push a pond, fetch its graph, rebuild into a fresh
/// empty pond, and confirm the rebuilt pond is content-equal to the source
/// (its read-side fold equals the source's root tree hash).
#[tokio::test]
async fn rebuild_reproduces_source_content() {
    let (_t, mut src) = new_pond("src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_file(&mut src, "/b.txt", b"beta").await;
    mkdir_and_file(&mut src, "/sub", "/sub/c.txt", b"gamma").await;
    mkdir_and_file(&mut src, "/sub/deep", "/sub/deep/d.txt", b"delta").await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &graph)
        .await
        .expect("rebuild");

    assert_eq!(outcome.root_tree_hash, Some(src_root));
    assert_eq!(outcome.files, 4);
    assert_eq!(outcome.dirs, 2);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond must be content-equal to the source"
    );
}

/// A multi-version `table:series` survives the full round trip: the rebuilt
/// pond is content-equal to the source, which requires recreating every series
/// version in order so the read-side fold's `series_hash` matches (Section
/// 8.5.3).
#[tokio::test]
async fn rebuild_reproduces_multi_version_series() {
    let (_t, mut src) = new_pond("series-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_series(
        &mut src,
        "/readings.series",
        &[(1_000, "first"), (2_000, "second"), (3_000, "third")],
    )
    .await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "series-dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &graph)
        .await
        .expect("rebuild");

    assert_eq!(outcome.root_tree_hash, Some(src_root));
    assert_eq!(outcome.files, 1);
    assert_eq!(outcome.series, 1);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond with a multi-version series must be content-equal to the source"
    );
}

/// A pond containing dynamic nodes (factory + config recipes) survives the
/// round trip: rebuild recreates each recipe and the read-side fold's
/// `recipe_hash` matches the source (Section 8.5.4 / D4).  A dynamic directory
/// is a leaf recipe -- its generated children are recomputed on read and are
/// not part of the graph.
#[tokio::test]
async fn rebuild_reproduces_dynamic_nodes() {
    let (_t, mut src) = new_pond("dyn-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_dynamic(
        &mut src,
        "/derived",
        tinyfs::EntryType::TableDynamic,
        "sql-derived-series",
        b"sql: SELECT * FROM source\n",
    )
    .await;
    write_dynamic(
        &mut src,
        "/gen",
        tinyfs::EntryType::DirectoryDynamic,
        "dynamic-dir",
        b"pattern: '*.series'\n",
    )
    .await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dyn-dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &graph)
        .await
        .expect("rebuild");

    assert_eq!(outcome.root_tree_hash, Some(src_root));
    assert_eq!(outcome.files, 1);
    assert_eq!(outcome.dynamic, 2);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond with dynamic nodes must be content-equal to the source"
    );
}

/// Rebuilding from an empty graph is a hard error, not a silent no-op.
#[tokio::test]
async fn rebuild_empty_graph_errors() {
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dst")
        .await
        .expect("create dst pond");
    let empty = steward::FetchedGraph::default();
    assert!(steward::rebuild_pond(&mut dst, &empty).await.is_err());
}
