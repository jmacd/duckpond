// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Manual performance probe for the read-only remote-read pond
//! (`steward::remote_pond`). Skipped unless `REMOTE_PREVIEW_URL` is set.
//!
//! Example (network-local MinIO / staging):
//!   REMOTE_PREVIEW_URL=s3://water-staging \
//!   S3_ENDPOINT=http://watershop.casparwater.us:9000 \
//!   S3_KEY=caspar S3_SECRET=watertown \
//!   REMOTE_PREVIEW_PATH=/reduced/system_pressure \
//!   cargo test -p steward --test remote_preview -- --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use steward::remote_pond::open_remote_pond;

#[tokio::test]
async fn remote_preview() {
    let Ok(url) = std::env::var("REMOTE_PREVIEW_URL") else {
        eprintln!("REMOTE_PREVIEW_URL not set; skipping remote_preview probe");
        return;
    };

    let mut opts = HashMap::new();
    if let Ok(v) = std::env::var("S3_ENDPOINT") {
        let _ = opts.insert("endpoint".to_string(), v);
    }
    if let Ok(v) = std::env::var("S3_KEY") {
        let _ = opts.insert("access_key_id".to_string(), v);
    }
    if let Ok(v) = std::env::var("S3_SECRET") {
        let _ = opts.insert("secret_access_key".to_string(), v);
    }
    let _ = opts.insert("allow_http".to_string(), "true".to_string());
    let _ = opts.insert(
        "region".to_string(),
        std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
    );

    let t0 = Instant::now();
    let persistence = open_remote_pond(&url, opts)
        .await
        .expect("open remote pond");
    eprintln!("[timing] open_remote_pond: {:?}", t0.elapsed());

    let depth: usize = std::env::var("REMOTE_PREVIEW_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    eprintln!("--- tree (depth {depth}) ---");
    for line in persistence.tree_lines(depth) {
        eprintln!("{line}");
    }
    eprintln!("--- end tree ---");

    // Discover dynamic-directory children (e.g. `/reduced/*`) via the FS glob,
    // which executes the directory factory over the remote persistence.
    let provider_ctx = persistence.provider_context();
    let fs = provider_ctx.filesystem();
    let root = fs.root().await.expect("fs root");

    if let Ok(glob) = std::env::var("REMOTE_PREVIEW_GLOB") {
        let tg = Instant::now();
        let matches = root.collect_matches(&glob).await.expect("collect_matches");
        eprintln!(
            "[timing] glob {glob:?} ({} matches): {:?}",
            matches.len(),
            tg.elapsed()
        );
        for (np, _captures) in &matches {
            eprintln!("  {}", np.path.display());
        }
    }

    let Ok(path) = std::env::var("REMOTE_PREVIEW_PATH") else {
        eprintln!("REMOTE_PREVIEW_PATH not set; tree printed, skipping query");
        return;
    };

    // Build a table provider through the provider API so dynamic paths resolve
    // via their factories. Scheme defaults to `series` (a queryable pond node).
    let scheme = std::env::var("REMOTE_PREVIEW_SCHEME").unwrap_or_else(|_| "series".to_string());
    let url = format!("{scheme}://{path}");
    let provider = provider::Provider::with_context(Arc::new(fs), Arc::new(provider_ctx.clone()))
        .with_root(root.clone());

    let t1 = Instant::now();
    let tp = provider
        .create_table_provider(&url, &provider_ctx.datafusion_session)
        .await
        .expect("create table provider");
    eprintln!(
        "[timing] table provider (schema infer) for {url:?}: {:?}",
        t1.elapsed()
    );

    let session = &provider_ctx.datafusion_session;
    let _ = session.register_table("t", tp).expect("register table");

    let sql = std::env::var("REMOTE_PREVIEW_SQL")
        .unwrap_or_else(|_| "SELECT COUNT(*) AS n FROM t".to_string());
    let t2 = Instant::now();
    let df = session.sql(&sql).await.expect("plan sql");
    let batches = df.collect().await.expect("collect");
    eprintln!("[timing] query {sql:?}: {:?}", t2.elapsed());
    let pretty = arrow::util::pretty::pretty_format_batches(&batches).expect("format");
    eprintln!("{pretty}");
}
