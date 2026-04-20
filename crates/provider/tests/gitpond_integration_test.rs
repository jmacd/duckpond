// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration test: gitpond's GitRootDirectory serves files that the
//! provider reads via csv://.
//!
//! Creates a local git repo with a CSV file, constructs a GitRootDirectory
//! pointing at it, inserts it into a MemoryPersistence FS, then reads the
//! CSV through the format provider pipeline (csv:// -> ensure_url_cached).

use std::process::Command;
use std::sync::Arc;
use tempfile::TempDir;

/// Create a local git repo with `data/sensors.csv` and a bare clone
/// at `bare_path`. Returns the source tempdir (to keep it alive).
fn create_test_git_repo(bare_path: &std::path::Path) -> TempDir {
    let source_dir = TempDir::new().expect("create source tempdir");
    let source_path = source_dir.path();

    let run = |args: &[&str], dir: &std::path::Path| {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap_or_else(|e| panic!("git {:?} failed: {}", args, e));
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    };

    run(&["init"], source_path);
    run(&["config", "user.email", "test@test.com"], source_path);
    run(&["config", "user.name", "Test"], source_path);

    let data_dir = source_path.join("data");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("sensors.csv"),
        "timestamp,temperature,humidity\n\
         2024-01-01T00:00:00Z,20.5,45.0\n\
         2024-01-01T01:00:00Z,21.0,44.5\n\
         2024-01-01T02:00:00Z,19.8,46.2\n",
    )
    .unwrap();

    run(&["add", "."], source_path);
    run(&["commit", "-m", "add sensor data"], source_path);

    // Create bare clone
    if let Some(parent) = bare_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    run(
        &[
            "clone",
            "--bare",
            source_path.to_str().unwrap(),
            bare_path.to_str().unwrap(),
        ],
        source_path,
    );

    source_dir
}

#[tokio::test]
async fn test_gitpond_csv_via_format_provider() {
    use datafusion::prelude::*;
    use tinyfs::{EntryType, Node, NodeType};

    let cache_dir = TempDir::new().expect("cache dir");
    let bare_dir = TempDir::new().expect("bare dir");
    let bare_repo_path = bare_dir.path().join("test.git");

    // Create git repo with CSV data under data/ prefix
    let _source = create_test_git_repo(&bare_repo_path);

    // Set up MemoryPersistence FS
    let persistence: Arc<dyn tinyfs::PersistenceLayer> =
        Arc::new(tinyfs::MemoryPersistence::default());
    let fs = Arc::new(tinyfs::FS::from_arc(persistence.clone()));

    // Create a GitRootDirectory manually and insert it into the FS root
    let dir_id = tinyfs::FileID::from_content(
        tinyfs::PartID::root(),
        EntryType::DirectoryDynamic,
        b"gitdata",
        tinyfs::local_pond_uuid(),
    );

    let git_root = gitpond::tree::GitRootDirectory::new(
        bare_repo_path,
        "main".to_string(),
        Some("data".to_string()),
        dir_id,
    );
    let dir_handle = git_root.create_handle();
    let dir_node = Node::new(dir_id, NodeType::Directory(dir_handle));

    let root = fs.root().await.expect("root");
    let _ = root
        .insert_node("gitdata", dir_node)
        .await
        .expect("insert gitdata");

    // Verify the file is visible
    let (_, lookup) = root
        .resolve_path("/gitdata/sensors.csv")
        .await
        .expect("resolve csv");
    assert!(
        matches!(lookup, tinyfs::Lookup::Found(_)),
        "sensors.csv should be found in gitpond tree"
    );

    // Read CSV through format provider
    let provider_context = tinyfs::ProviderContext::new_for_testing(persistence)
        .with_cache_dir(cache_dir.path().to_path_buf());

    let provider = provider::Provider::with_context(fs, Arc::new(provider_context));
    let session = SessionContext::new();

    let table = provider
        .create_table_provider("csv:///gitdata/sensors.csv", &session)
        .await
        .expect("Should read gitpond CSV via format provider");

    let _ = session.register_table("sensors", table).unwrap();

    let df = session
        .sql("SELECT temperature, humidity FROM sensors ORDER BY temperature")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let temp_col = batch
        .column_by_name("temperature")
        .expect("temperature column")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("f64 array");
    assert!((temp_col.value(0) - 19.8).abs() < 0.01);
    assert!((temp_col.value(1) - 20.5).abs() < 0.01);
    assert!((temp_col.value(2) - 21.0).abs() < 0.01);
}
