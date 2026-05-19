// SPDX-License-Identifier: Apache-2.0

//! S3 backend smoke tests using a MinIO-compatible endpoint.
//!
//! These tests demonstrate the sandbox prototype's `Remote::*` flow
//! against `s3://` URLs, validating the DESIGN.md §2.5 claim that
//! "production deployments swap LocalFileSystem for S3 as a config
//! change; the schema is identical."
//!
//! All tests in this file are marked `#[ignore]` because they require
//! a running S3-compatible service.  Run them with:
//!
//! ```bash
//! # 1. Start MinIO (or any S3-compatible service)
//! docker run -d --rm --name sandbox-minio-test \
//!   -p 9000:9000 \
//!   -e MINIO_ROOT_USER=minioadmin \
//!   -e MINIO_ROOT_PASSWORD=minioadmin \
//!   minio/minio:latest server /data
//!
//! # 2. Set credentials and run the ignored tests
//! MINIO_ENDPOINT=http://localhost:9000 \
//! MINIO_ROOT_USER=minioadmin \
//! MINIO_ROOT_PASSWORD=minioadmin \
//! cargo test -p sandbox-tests --test s3_minio -- --ignored
//! ```
//!
//! Per-test buckets are created at runtime with names like
//! `sandbox-test-<uuid>`; the AWS_S3_LOCKING_PROVIDER env var is set
//! to `none` to disable DynamoDB-based concurrency locking (the
//! prototype is single-process anyway).

use std::sync::Arc;
use std::sync::Once;

use sync_remote::Remote;
use sync_steward::{Steward, StewardOptions};
use sync_store::checksum::Merkle;
use tempfile::TempDir;
use uuid::Uuid;

static REGISTER_HANDLERS: Once = Once::new();

fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
    REGISTER_HANDLERS.call_once(|| {
        sync_remote::register_s3_handlers();
    });
}

/// Skip the test (returns None) if MinIO env vars aren't set.  Returns
/// `(endpoint, access_key, secret_key)` otherwise.
fn minio_env() -> Option<(String, String, String)> {
    let endpoint = std::env::var("MINIO_ENDPOINT").ok()?;
    let access_key = std::env::var("MINIO_ROOT_USER").ok()?;
    let secret_key = std::env::var("MINIO_ROOT_PASSWORD").ok()?;
    Some((endpoint, access_key, secret_key))
}

/// Build storage options for delta-rs's S3 backend pointed at MinIO.
fn storage_options(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> std::collections::HashMap<String, String> {
    let mut opts = std::collections::HashMap::new();
    opts.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    opts.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
    opts.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key.to_string());
    opts.insert("AWS_REGION".to_string(), "us-east-1".to_string());
    opts.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    // Single-process prototype; no DynamoDB locking needed.
    opts.insert("AWS_S3_LOCKING_PROVIDER".to_string(), "none".to_string());
    opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
    opts
}

/// Sandbox tests use a single fixed bucket and per-test unique
/// prefixes within it.  Set `MINIO_TEST_BUCKET` (default
/// `sandbox-test`) and pre-create it once via:
///
/// ```bash
/// docker exec sandbox-minio-test \
///   sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb --ignore-existing local/sandbox-test"
/// ```
fn test_bucket() -> String {
    std::env::var("MINIO_TEST_BUCKET").unwrap_or_else(|_| "sandbox-test".to_string())
}

/// Build a fresh `s3://bucket/<uuid>/<suffix>` URL for a per-test
/// unique location within the shared test bucket.
fn unique_remote_url(suffix: &str) -> String {
    format!("s3://{}/{}/{}", test_bucket(), Uuid::new_v4(), suffix)
}

fn opts() -> StewardOptions {
    StewardOptions {
        checksum_strategy: Arc::new(Merkle::new()),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires MinIO; run with MINIO_ENDPOINT=... cargo test --ignored"]
async fn s3_remote_create_push_pull_roundtrip() {
    init();
    let Some((endpoint, ak, sk)) = minio_env() else {
        eprintln!("skipping: MINIO_ENDPOINT/MINIO_ROOT_USER/MINIO_ROOT_PASSWORD not set");
        return;
    };

    let remote_url = unique_remote_url("remote");
    let storage_opts = storage_options(&endpoint, &ak, &sk);

    let local_dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(local_dir.path().join("source"), opts())
        .await
        .unwrap();

    // Create remote on S3.
    let mut remote = Remote::create_at_url(&remote_url, source.store_id(), storage_opts.clone())
        .await
        .expect("Remote::create_at_url over s3:// succeeds");

    // Source writes; pushes.
    {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", "k1", b"v1-from-source".to_vec()).unwrap();
        g.put("p", "k2", b"v2-from-source".to_vec()).unwrap();
        let _ = g.commit().await.unwrap();
    }
    remote
        .push(&mut source, 1)
        .await
        .expect("push to s3 succeeds");

    // Consumer pulls from S3.
    let mut consumer = Steward::create_with_options(
        local_dir.path().join("consumer"),
        StewardOptions {
            store_id: Some(source.store_id()),
            checksum_strategy: Arc::new(Merkle::new()),
        },
    )
    .await
    .unwrap();
    let report = remote
        .pull(&mut consumer)
        .await
        .expect("pull from s3 succeeds");
    assert_eq!(report.bundles_applied.len(), 1);

    // Consumer reads the data.
    let r = consumer.begin_read().await.unwrap();
    assert_eq!(
        r.get("p", "k1").await.unwrap(),
        Some(b"v1-from-source".to_vec())
    );
    assert_eq!(
        r.get("p", "k2").await.unwrap(),
        Some(b"v2-from-source".to_vec())
    );

    // Verify against remote passes.
    let report = sync_remote::verify_against_remote(&remote, &consumer)
        .await
        .unwrap();
    assert!(
        report.ok,
        "verify ok over s3, mismatches: {:?}",
        report.mismatches
    );
}

#[tokio::test]
#[ignore = "requires MinIO; run with MINIO_ENDPOINT=... cargo test --ignored"]
async fn s3_remote_maintain_and_restart_from_compact() {
    init();
    let Some((endpoint, ak, sk)) = minio_env() else {
        eprintln!("skipping: MINIO_ENDPOINT/MINIO_ROOT_USER/MINIO_ROOT_PASSWORD not set");
        return;
    };

    let remote_url = unique_remote_url("remote");
    let storage_opts = storage_options(&endpoint, &ak, &sk);

    let local_dir = TempDir::new().unwrap();
    let mut source = Steward::create_with_options(local_dir.path().join("source"), opts())
        .await
        .unwrap();
    let mut remote = Remote::create_at_url(&remote_url, source.store_id(), storage_opts.clone())
        .await
        .unwrap();

    // Source writes 3 times, pushes each, then compacts.
    for i in 1..=3i64 {
        let mut g = source.begin_write().await.unwrap();
        g.put("p", &format!("k{}", i), format!("v{}", i).into_bytes())
            .unwrap();
        let _ = g.commit().await.unwrap();
        remote.push(&mut source, i).await.unwrap();
    }
    let outcome = source.compact(None).await.unwrap();
    assert!(outcome.had_data, "compact merged files");
    remote.push(&mut source, outcome.txn_seq).await.unwrap();

    // Maintain remote: keep only the compact bundle.
    let report = remote
        .maintain(sync_remote::MaintainOptions {
            keep_compact_bundles: 1,
            vacuum_after: true,
        })
        .await
        .unwrap();
    assert!(report.rows_deleted > 0, "older bundles pruned");

    // Bootstrap a fresh consumer via restart_from_compact.
    let consumer_path = local_dir.path().join("consumer");
    let consumer = remote
        .restart_from_compact(&consumer_path)
        .await
        .expect("restart_from_compact over s3 succeeds");

    // Consumer has the same data as source.
    let r = consumer.begin_read().await.unwrap();
    for i in 1..=3i64 {
        assert_eq!(
            r.get("p", &format!("k{}", i)).await.unwrap(),
            Some(format!("v{}", i).into_bytes())
        );
    }

    let report = sync_remote::verify_against_remote(&remote, &consumer)
        .await
        .unwrap();
    assert!(report.ok, "post-restart verify over s3 ok");
}
