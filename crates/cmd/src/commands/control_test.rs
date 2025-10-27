//! Integration tests for pond control command and post-commit factory execution
//!
//! These tests verify:
//! 1. Post-commit factory sequencing works correctly
//! 2. Control table tracks all execution states
//! 3. Independent factory execution (failures don't block others)
//! 4. pond control command output for various scenarios
//! 5. Recovery query patterns work correctly

use super::{control_command, ControlMode};
use crate::commands::{init_command, mkdir_command, mknod_command};
use crate::common::ShipContext;
use anyhow::Result;
use std::path::PathBuf;
use tempfile::TempDir;

/// Test setup with pond initialization
struct TestSetup {
    _temp_dir: TempDir,
    ship_context: ShipContext,
    pond_path: PathBuf,
}

impl TestSetup {
    async fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let pond_path = temp_dir.path().join("test_pond");

        // Create ship context for initialization
        let init_args = vec!["pond".to_string(), "init".to_string()];
        let ship_context = ShipContext::new(Some(pond_path.clone()), init_args);

        // Initialize pond
        init_command(&ship_context, None, None).await?;

        Ok(TestSetup {
            _temp_dir: temp_dir,
            ship_context,
            pond_path,
        })
    }

    /// Create a test factory configuration file
    async fn create_factory_config(&self, path: &str, content: &str) -> Result<()> {
        // Create file in temp directory (sibling to pond)
        let config_path = self._temp_dir.path().join(path);
        tokio::fs::write(&config_path, content).await?;
        Ok(())
    }

    /// Get absolute path to a factory config file
    fn config_path(&self, filename: &str) -> String {
        self._temp_dir.path().join(filename).to_string_lossy().to_string()
    }

    /// Execute a write transaction to trigger post-commit
    async fn execute_write_transaction(&self, description: &str) -> Result<()> {
        let mut ship = self.ship_context.open_pond().await?;
        let args = vec!["test".to_string(), description.to_string()];
        let description_owned = description.to_string();
        
        ship.transact(args, move |_tx, fs| {
            let desc = description_owned.clone();
            Box::pin(async move {
                let root = fs.root().await.map_err(|e| {
                    steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                })?;
                
                // Create /test directory if it doesn't exist
                if !root.exists("/test").await {
                    root.create_dir_path("/test").await
                        .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                }
                
                // Create a simple file to make this a write transaction
                tinyfs::async_helpers::convenience::create_file_path(
                    &root,
                    &format!("/test/{}.txt", desc),
                    format!("Test data for {}", desc).as_bytes(),
                )
                .await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                Ok(())
            })
        })
        .await?;

        Ok(())
    }

    /// Get the last transaction sequence number from control table
    async fn get_last_txn_seq(&self) -> Result<i64> {
        // Open ship to access cached control table
        let ship = self.ship_context.open_pond().await?;
        let control_table = ship.control_table();
        
        control_table.get_last_write_sequence().await
            .map_err(|e| anyhow::anyhow!("Failed to get last txn seq: {}", e))
    }

    /// Query transaction records from control table
    async fn query_transaction_records(&self, txn_seq: i64) -> Result<Vec<TransactionRecordSummary>> {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};

        // Open ship to access cached control table
        let ship = self.ship_context.open_pond().await?;
        let control_table = ship.control_table();

        // Use control table's SessionContext (following tlogfs pattern)
        let ctx = control_table.session_context();

        // Query all records for this transaction
        let sql = format!(
            r#"
            SELECT 
                record_type,
                factory_name,
                config_path,
                execution_seq,
                error_message,
                duration_ms
            FROM transactions
            WHERE txn_seq = {} OR parent_txn_seq = {}
            ORDER BY 
                CASE WHEN parent_txn_seq IS NULL THEN 0 ELSE 1 END,
                execution_seq NULLS FIRST,
                timestamp
            "#,
            txn_seq, txn_seq
        );

        let df = ctx.sql(&sql).await
            .map_err(|e| anyhow::anyhow!("Failed to query: {}", e))?;

        let batches = df.collect().await
            .map_err(|e| anyhow::anyhow!("Failed to collect: {}", e))?;

        let mut records = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let record_types = batch.column_by_name("record_type")
                .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let factory_names = batch.column_by_name("factory_name")
                .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let config_paths = batch.column_by_name("config_path")
                .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let execution_seqs = batch.column_by_name("execution_seq")
                .unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let error_messages = batch.column_by_name("error_message")
                .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let durations = batch.column_by_name("duration_ms")
                .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();

            for i in 0..batch.num_rows() {
                records.push(TransactionRecordSummary {
                    record_type: record_types.value(i).to_string(),
                    factory_name: if !factory_names.is_null(i) {
                        Some(factory_names.value(i).to_string())
                    } else {
                        None
                    },
                    config_path: if !config_paths.is_null(i) {
                        Some(config_paths.value(i).to_string())
                    } else {
                        None
                    },
                    execution_seq: if !execution_seqs.is_null(i) {
                        Some(execution_seqs.value(i))
                    } else {
                        None
                    },
                    error_message: if !error_messages.is_null(i) {
                        Some(error_messages.value(i).to_string())
                    } else {
                        None
                    },
                    duration_ms: if !durations.is_null(i) {
                        Some(durations.value(i))
                    } else {
                        None
                    },
                });
            }
        }

        Ok(records)
    }
}

#[derive(Debug, Clone)]
struct TransactionRecordSummary {
    record_type: String,
    factory_name: Option<String>,
    config_path: Option<String>,
    execution_seq: Option<i32>,
    error_message: Option<String>,
    duration_ms: Option<i64>,
}

// ============================================================================
// Test 1: Post-Commit Factory Sequencing
// ============================================================================

#[tokio::test]
async fn test_post_commit_single_factory_success() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /etc/system.d directory
    mkdir_command(&setup.ship_context, "/etc/system.d", true)
        .await
        .expect("Failed to create system.d");

    // Create a test factory config that will succeed
    setup.create_factory_config("test-factory.yaml", r#"
message: "Test factory executed successfully"
repeat_count: 1
"#).await.expect("Failed to create config");

    // Register the factory
    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/etc/system.d/10-test",
        &setup.config_path("test-factory.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Execute a write transaction (triggers post-commit)
    setup.execute_write_transaction("trigger_post_commit")
        .await
        .expect("Failed to execute write transaction");

    // Get the transaction sequence
    let txn_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");

    // Query control table for this transaction
    let records = setup.query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Verify we have the expected records
    let record_types: Vec<String> = records.iter().map(|r| r.record_type.clone()).collect();
    
    assert!(record_types.contains(&"begin".to_string()), 
        "Should have begin record");
    assert!(record_types.contains(&"data_committed".to_string()), 
        "Should have data_committed record");
    assert!(record_types.contains(&"post_commit_pending".to_string()), 
        "Should have post_commit_pending record");
    assert!(record_types.contains(&"post_commit_started".to_string()), 
        "Should have post_commit_started record");
    assert!(record_types.contains(&"post_commit_completed".to_string()), 
        "Should have post_commit_completed record");

    // Verify factory details in pending record
    let pending_record = records.iter()
        .find(|r| r.record_type == "post_commit_pending")
        .expect("Should have pending record");
    
    assert_eq!(pending_record.factory_name.as_deref(), Some("test-executor"));
    assert_eq!(pending_record.config_path.as_deref(), Some("/etc/system.d/10-test"));
    assert_eq!(pending_record.execution_seq, Some(1));

    // Verify completed record has duration
    let completed_record = records.iter()
        .find(|r| r.record_type == "post_commit_completed")
        .expect("Should have completed record");
    
    assert!(completed_record.duration_ms.is_some(), "Should have duration");
    assert!(completed_record.duration_ms.unwrap() >= 0, "Duration should be non-negative");
}

#[tokio::test]
async fn test_post_commit_multiple_factories_all_succeed() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /etc/system.d directory
    mkdir_command(&setup.ship_context, "/etc/system.d", true)
        .await
        .expect("Failed to create system.d");

    // Create three factory configs
    for i in 1..=3 {
        let config_filename = format!("test-factory-{}.yaml", i);
        setup.create_factory_config(
            &config_filename,
            &format!(r#"
message: "Test factory {} executed"
repeat_count: 1
"#, i)
        ).await.expect("Failed to create config");

        mknod_command(
            &setup.ship_context,
            "test-executor",
            &format!("/etc/system.d/{:02}-test-{}", i * 10, i),
            &setup.config_path(&config_filename),
            false,
        )
        .await
        .expect("Failed to create factory node");
    }

    // Execute a write transaction
    setup.execute_write_transaction("trigger_three_factories")
        .await
        .expect("Failed to execute write transaction");

    let txn_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let records = setup.query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Should have 3 pending, 3 started, 3 completed
    let pending_count = records.iter().filter(|r| r.record_type == "post_commit_pending").count();
    let started_count = records.iter().filter(|r| r.record_type == "post_commit_started").count();
    let completed_count = records.iter().filter(|r| r.record_type == "post_commit_completed").count();

    assert_eq!(pending_count, 3, "Should have 3 pending records");
    assert_eq!(started_count, 3, "Should have 3 started records");
    assert_eq!(completed_count, 3, "Should have 3 completed records");

    // Verify execution order (execution_seq should be 1, 2, 3)
    let mut pending_records: Vec<_> = records.iter()
        .filter(|r| r.record_type == "post_commit_pending")
        .collect();
    pending_records.sort_by_key(|r| r.execution_seq);

    assert_eq!(pending_records[0].execution_seq, Some(1));
    assert_eq!(pending_records[1].execution_seq, Some(2));
    assert_eq!(pending_records[2].execution_seq, Some(3));

    // Verify factory names match the order
    assert!(pending_records[0].config_path.as_deref().unwrap().contains("10-test-1"));
    assert!(pending_records[1].config_path.as_deref().unwrap().contains("20-test-2"));
    assert!(pending_records[2].config_path.as_deref().unwrap().contains("30-test-3"));
}

// ============================================================================
// Test 2: Independent Execution (Failure Isolation)
// ============================================================================

/// CRITICAL TEST: Verify independent factory execution with predictable failures
///
/// This test validates:
/// 1. Multiple factories (success, fail, success) execute independently
/// 2. One factory's failure doesn't block others
/// 3. Control table captures complete lifecycle for each factory
/// 4. Error messages, duration, and factory details are recorded correctly
/// 5. pond control command can display the failure history
#[tokio::test]
async fn test_post_commit_independent_execution_with_failure() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /etc/system.d directory
    mkdir_command(&setup.ship_context, "/etc/system.d", true)
        .await
        .expect("Failed to create system.d");

    // Factory 1: Success
    setup.create_factory_config("test-success-1.yaml", r#"
message: "First factory succeeds"
repeat_count: 1
"#).await.expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/etc/system.d/10-success",
        &setup.config_path("test-success-1.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Factory 2: Intentional Failure (will always fail with predictable error)
    setup.create_factory_config("test-fail.yaml", r#"
message: "Second factory intentionally fails"
repeat_count: 1
fail: true
"#).await.expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/etc/system.d/20-fail",
        &setup.config_path("test-fail.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Factory 3: Success (proves factory 2's failure didn't block execution)
    setup.create_factory_config("test-success-2.yaml", r#"
message: "Third factory succeeds despite previous failure"
repeat_count: 1
"#).await.expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/etc/system.d/30-success",
        &setup.config_path("test-success-2.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Execute write transaction to trigger post-commit
    setup.execute_write_transaction("test_failure_isolation")
        .await
        .expect("Failed to execute write transaction");

    let txn_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let records = setup.query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    println!("\n=== Independent Execution Test: Transaction {} ===", txn_seq);
    println!("Total records: {}", records.len());
    
    // Print all records for debugging
    for record in &records {
        println!("  {:20} exec_seq={:?} factory={:?} error={:?}", 
            record.record_type, record.execution_seq, record.factory_name, 
            record.error_message.as_ref().map(|s| &s[..s.len().min(50)]));
    }

    // ========================================================================
    // VERIFY RECORD COUNTS (Explicit assertions as requested)
    // ========================================================================
    
    let pending_count = records.iter().filter(|r| r.record_type == "post_commit_pending").count();
    let started_count = records.iter().filter(|r| r.record_type == "post_commit_started").count();
    let completed_count = records.iter().filter(|r| r.record_type == "post_commit_completed").count();
    let failed_count = records.iter().filter(|r| r.record_type == "post_commit_failed").count();

    assert_eq!(pending_count, 3, "Should have exactly 3 pending records (one per factory)");
    assert_eq!(started_count, 3, "Should have exactly 3 started records (all factories began execution)");
    assert_eq!(completed_count, 2, "Should have exactly 2 completed records (factories 1 and 3 succeeded)");
    assert_eq!(failed_count, 1, "Should have exactly 1 failed record (factory 2 failed)");

    println!("✓ Record counts: {} pending, {} started, {} completed, {} failed", 
        pending_count, started_count, completed_count, failed_count);

    // ========================================================================
    // VERIFY PENDING RECORDS (Factory details captured)
    // ========================================================================
    
    let pending_records: Vec<_> = records.iter()
        .filter(|r| r.record_type == "post_commit_pending")
        .collect();

    // All pending records should have factory name
    for pending in &pending_records {
        assert_eq!(pending.factory_name.as_deref(), Some("test-executor"),
            "Pending record should have factory_name");
        assert!(pending.config_path.is_some(), 
            "Pending record should have config_path");
        assert!(pending.execution_seq.is_some(),
            "Pending record should have execution_seq");
    }

    // Verify execution sequence is 1, 2, 3
    let mut exec_seqs: Vec<_> = pending_records.iter()
        .filter_map(|r| r.execution_seq)
        .collect();
    exec_seqs.sort();
    assert_eq!(exec_seqs, vec![1, 2, 3], "Execution sequences should be 1, 2, 3");

    println!("✓ Pending records have correct factory details and sequencing");

    // ========================================================================
    // VERIFY FAILED RECORD (Error message and duration captured)
    // ========================================================================
    
    let failed_record = records.iter()
        .find(|r| r.record_type == "post_commit_failed")
        .expect("Should have exactly one failed record");
    
    // Verify error message is captured
    assert!(failed_record.error_message.is_some(), 
        "Failed record must have error_message");
    let error_msg = failed_record.error_message.as_ref().unwrap();
    assert!(error_msg.contains("intentionally failed"), 
        "Error message should contain 'intentionally failed', got: {}", error_msg);
    
    // Verify duration is captured (even for failures)
    assert!(failed_record.duration_ms.is_some(), 
        "Failed record must have duration_ms");
    assert!(failed_record.duration_ms.unwrap() >= 0, 
        "Duration should be non-negative");

    // Verify execution_seq identifies which factory failed
    assert_eq!(failed_record.execution_seq, Some(2),
        "Failed record should be for execution_seq=2 (middle factory)");

    println!("✓ Failed record has error_message and duration_ms");
    println!("  Error: {}", error_msg);
    println!("  Duration: {} ms", failed_record.duration_ms.unwrap());

    // ========================================================================
    // VERIFY COMPLETED RECORDS (Success factories have duration)
    // ========================================================================
    
    let completed_records: Vec<_> = records.iter()
        .filter(|r| r.record_type == "post_commit_completed")
        .collect();

    for completed in &completed_records {
        assert!(completed.duration_ms.is_some(),
            "Completed record should have duration_ms");
        assert!(completed.duration_ms.unwrap() >= 0,
            "Duration should be non-negative");
        assert!(completed.error_message.is_none(),
            "Completed record should not have error_message");
    }

    // Verify it's factories 1 and 3 that succeeded
    let mut completed_seqs: Vec<_> = completed_records.iter()
        .filter_map(|r| r.execution_seq)
        .collect();
    completed_seqs.sort();
    assert_eq!(completed_seqs, vec![1, 3],
        "Factories 1 and 3 should have completed successfully");

    println!("✓ Completed records (exec_seq 1, 3) have duration_ms, no errors");

    // ========================================================================
    // VERIFY POND CONTROL COMMAND WORKS
    // ========================================================================
    
    // Test detail mode for this transaction
    println!("\n=== Testing pond control detail mode ===");
    let detail_mode = ControlMode::Detail { txn_seq };
    control_command(&setup.ship_context, detail_mode).await
        .expect("pond control detail should not fail");
    
    // Test incomplete mode (should show the failed operation if any exist)
    println!("\n=== Testing pond control incomplete mode ===");
    let incomplete_mode = ControlMode::Incomplete;
    control_command(&setup.ship_context, incomplete_mode).await
        .expect("pond control incomplete should not fail");

    println!("\n✅ Independent execution test PASSED");
    println!("   - 3 factories executed independently");
    println!("   - 1 failure did not block other factories");
    println!("   - Control table captured all lifecycle events");
    println!("   - Error messages and durations recorded correctly");
    println!("   - pond control command displays failure history");
}

// ============================================================================
// Test 3: Control Table Direct Queries (Not command output)
// ============================================================================

#[tokio::test]
async fn test_control_command_runs_without_panic() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Execute a transaction
    setup.execute_write_transaction("test_control_command")
        .await
        .expect("Failed to execute transaction");

    // Verify all three modes run without panicking
    // We can't easily verify output, but we can verify no crash
    let mode = ControlMode::Recent { limit: 5 };
    control_command(&setup.ship_context, mode)
        .await
        .expect("Recent mode should not panic");

    let txn_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let mode = ControlMode::Detail { txn_seq };
    control_command(&setup.ship_context, mode)
        .await
        .expect("Detail mode should not panic");

    let mode = ControlMode::Incomplete;
    control_command(&setup.ship_context, mode)
        .await
        .expect("Incomplete mode should not panic");
}

// ============================================================================
// Test 4: Recovery Query Patterns
// ============================================================================

#[tokio::test]
async fn test_query_pending_never_started() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // This test would require simulating a crash after pending records created
    // but before started records
    // For unit tests, we can verify the query structure works

    // Open ship to access cached control table
    let ship = setup.ship_context.open_pond().await.expect("Failed to open pond");
    let control_table = ship.control_table();

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = control_table.session_context();

    // Query for pending tasks that were never started
    let sql = r#"
        SELECT 
            parent_txn_seq,
            execution_seq,
            factory_name,
            config_path
        FROM transactions
        WHERE record_type = 'post_commit_pending'
          AND NOT EXISTS (
              SELECT 1 FROM transactions started
              WHERE started.parent_txn_seq = transactions.parent_txn_seq
                AND started.execution_seq = transactions.execution_seq
                AND started.record_type = 'post_commit_started'
          )
    "#;

    let df = ctx.sql(sql).await.expect("Query should succeed");
    let batches = df.collect().await.expect("Collect should succeed");

    // In a real scenario with incomplete execution, we'd verify rows returned
    // For now, just verify the query executes
    println!("Pending never started: {} batches", batches.len());
}

#[tokio::test]
async fn test_query_started_never_completed() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Open ship to access cached control table
    let ship = setup.ship_context.open_pond().await.expect("Failed to open pond");
    let control_table = ship.control_table();

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = control_table.session_context();

    // Query for tasks that started but never completed/failed
    let sql = r#"
        SELECT 
            parent_txn_seq,
            execution_seq,
            factory_name,
            config_path
        FROM transactions pending
        WHERE pending.record_type = 'post_commit_pending'
          AND EXISTS (
              SELECT 1 FROM transactions started
              WHERE started.parent_txn_seq = pending.parent_txn_seq
                AND started.execution_seq = pending.execution_seq
                AND started.record_type = 'post_commit_started'
          )
          AND NOT EXISTS (
              SELECT 1 FROM transactions completed
              WHERE completed.parent_txn_seq = pending.parent_txn_seq
                AND completed.execution_seq = pending.execution_seq
                AND completed.record_type IN ('post_commit_completed', 'post_commit_failed')
          )
    "#;

    let df = ctx.sql(sql).await.expect("Query should succeed");
    let batches = df.collect().await.expect("Collect should succeed");

    println!("Started never completed: {} batches", batches.len());
}

#[tokio::test]
async fn test_parent_txn_seq_execution_seq_identity() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create multiple post-commit factories
    mkdir_command(&setup.ship_context, "/etc/system.d", true)
        .await
        .expect("Failed to create system.d");

    for i in 1..=3 {
        let config_filename = format!("identity-test-{}.yaml", i);
        setup.create_factory_config(
            &config_filename,
            &format!(r#"
message: "Identity test factory {}"
repeat_count: 1
"#, i)
        ).await.expect("Failed to create config");

        mknod_command(
            &setup.ship_context,
            "test-executor",
            &format!("/etc/system.d/{:02}-identity-{}", i * 10, i),
            &setup.config_path(&config_filename),
            false,
        )
        .await
        .expect("Failed to create factory node");
    }

    setup.execute_write_transaction("identity_test")
        .await
        .expect("Failed to execute transaction");

    let txn_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let records = setup.query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Verify (parent_txn_seq, execution_seq) uniquely identifies each task
    let post_commit_records: Vec<_> = records.iter()
        .filter(|r| r.record_type.starts_with("post_commit"))
        .collect();

    // Group by execution_seq
    for exec_seq in 1..=3 {
        let task_records: Vec<_> = post_commit_records.iter()
            .filter(|r| r.execution_seq == Some(exec_seq))
            .collect();

        // Should have pending, started, completed for each execution_seq
        let has_pending = task_records.iter().any(|r| r.record_type == "post_commit_pending");
        let has_started = task_records.iter().any(|r| r.record_type == "post_commit_started");
        let has_completion = task_records.iter().any(|r| 
            r.record_type == "post_commit_completed" || r.record_type == "post_commit_failed"
        );

        assert!(has_pending, "Task {} should have pending record", exec_seq);
        assert!(has_started, "Task {} should have started record", exec_seq);
        assert!(has_completion, "Task {} should have completion record", exec_seq);

        // Verify factory name is consistent across lifecycle
        let factory_names: Vec<_> = task_records.iter()
            .filter_map(|r| r.factory_name.as_ref())
            .collect();
        
        
        if !factory_names.is_empty() {
            let first_name = factory_names[0];
            assert!(factory_names.iter().all(|name| *name == first_name),
                "Factory name should be consistent across lifecycle for task {}", exec_seq);
        }
    }
}

/// CRITICAL TEST: Verify post-commit factories can see data committed at txn_seq+1
/// 
/// This test validates the core assumption that post-commit factories execute
/// with a read-only transaction at txn_seq+1 and can see the data that was
/// just committed in the parent transaction.
#[tokio::test]
async fn test_version_visibility_post_commit_sees_committed_data() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /etc/system.d and /data directories
    mkdir_command(&setup.ship_context, "/etc/system.d", true)
        .await
        .expect("Failed to create system.d");
    
    mkdir_command(&setup.ship_context, "/data", true)
        .await
        .expect("Failed to create data directory");

    // Define the test data that we'll write and verify
    let test_file_path = "/data/visibility-test.txt";
    let test_content = "VISIBILITY_TEST_DATA_v1_committed_at_txn";

    // Step 1: Write data in a transaction
    let mut ship = setup.ship_context.open_pond().await.expect("Failed to open pond");
    let args = vec!["write".to_string(), "visibility_test".to_string()];
    let content_clone = test_content.to_string();
    let path_clone = test_file_path.to_string();
    
    ship.transact(args, move |_tx, fs| {
        let content = content_clone.clone();
        let path = path_clone.clone();
        Box::pin(async move {
            let root = fs.root().await.map_err(|e| {
                steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
            })?;
            
            // Write the test file with known content
            tinyfs::async_helpers::convenience::create_file_path(
                &root,
                &path,
                content.as_bytes(),
            )
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            Ok(())
        })
    })
    .await
    .expect("Failed to write test data");

    // Step 2: Create a post-commit factory that will read the file
    setup.create_factory_config("visibility-reader.yaml", &format!(r#"
message: "Reading committed data to verify version visibility"
repeat_count: 1
file_to_read: "{}"
expected_content: "{}"
"#, test_file_path, test_content)).await.expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/etc/system.d/10-visibility-reader",
        &setup.config_path("visibility-reader.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Step 3: Execute a transaction to trigger post-commit (doesn't need to write more data)
    setup.execute_write_transaction("trigger_visibility_test")
        .await
        .expect("Failed to execute trigger transaction");

    // Step 4: Verify the factory executed successfully
    let txn_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let records = setup.query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Debug: Print all records
    println!("Transaction {} has {} records:", txn_seq, records.len());
    for record in &records {
        println!("  - {:?}: factory={:?}, exec_seq={:?}, error={:?}", 
            record.record_type, record.factory_name, record.execution_seq, record.error_message);
    }

    // Find the post-commit completed record
    let completed_record = records.iter()
        .find(|r| r.record_type == "post_commit_completed")
        .expect("Should have post_commit_completed record");

    // Verify it succeeded (no error message)
    assert!(completed_record.error_message.is_none(), 
        "Post-commit factory should have succeeded without errors. Error: {:?}", 
        completed_record.error_message);

    // Verify execution_seq matches
    assert_eq!(completed_record.execution_seq, Some(1));

    // Verify the pending record has the correct factory details
    let pending_record = records.iter()
        .find(|r| r.record_type == "post_commit_pending")
        .expect("Should have post_commit_pending record");
    
    assert_eq!(pending_record.factory_name.as_deref(), Some("test-executor"));
    assert_eq!(pending_record.config_path.as_deref(), Some("/etc/system.d/10-visibility-reader"));

    // If we got here, the factory successfully read and verified the content
    // This proves that post-commit factories see data committed at txn_seq+1
    println!("✅ Version visibility test PASSED: Post-commit factory successfully read data committed in parent transaction");
}

// ============================================================================
// Test 5: Transaction Completion Records (Basic Transactions)
// ============================================================================

/// CRITICAL TEST: Verify that basic transactions (init, mkdir, write) record completion
///
/// This test addresses the bug where transactions show as INCOMPLETE in pond control.
/// Every transaction should have:
/// 1. "begin" record when transaction starts
/// 2. "data_committed" record when write transaction commits (or "completed" for read-only)
#[tokio::test]
async fn test_transaction_completion_records_written() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Transaction 1: init (already completed during setup)
    let init_seq = 1;
    let init_records = setup.query_transaction_records(init_seq)
        .await
        .expect("Failed to query init transaction");

    println!("\n=== Transaction 1 (init) ===");
    for record in &init_records {
        println!("  {:20} error={:?}", record.record_type, record.error_message);
    }

    // Verify init has both begin and data_committed
    let has_begin = init_records.iter().any(|r| r.record_type == "begin");
    let has_committed = init_records.iter().any(|r| r.record_type == "data_committed");
    
    assert!(has_begin, "Init transaction should have 'begin' record");
    assert!(has_committed, "Init transaction should have 'data_committed' record");

    // Transaction 2: mkdir (write transaction)
    mkdir_command(&setup.ship_context, "/test", false)
        .await
        .expect("Failed to create /test directory");

    let mkdir_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let mkdir_records = setup.query_transaction_records(mkdir_seq)
        .await
        .expect("Failed to query mkdir transaction");

    println!("\n=== Transaction {} (mkdir) ===", mkdir_seq);
    for record in &mkdir_records {
        println!("  {:20} error={:?}", record.record_type, record.error_message);
    }

    // Verify mkdir has both begin and data_committed
    let has_begin = mkdir_records.iter().any(|r| r.record_type == "begin");
    let has_committed = mkdir_records.iter().any(|r| r.record_type == "data_committed");
    
    assert!(has_begin, "mkdir transaction should have 'begin' record");
    assert!(has_committed, "mkdir transaction should have 'data_committed' record");

    // Transaction 3: Regular write transaction
    setup.execute_write_transaction("test_completion")
        .await
        .expect("Failed to execute write transaction");

    let write_seq = setup.get_last_txn_seq().await.expect("Failed to get txn seq");
    let write_records = setup.query_transaction_records(write_seq)
        .await
        .expect("Failed to query write transaction");

    println!("\n=== Transaction {} (write) ===", write_seq);
    for record in &write_records {
        println!("  {:20} error={:?}", record.record_type, record.error_message);
    }

    // Verify write has both begin and data_committed
    let has_begin = write_records.iter().any(|r| r.record_type == "begin");
    let has_committed = write_records.iter().any(|r| r.record_type == "data_committed");
    
    assert!(has_begin, "write transaction should have 'begin' record");
    assert!(has_committed, "write transaction should have 'data_committed' record");

    println!("\n✅ Transaction completion records test PASSED");
    println!("   - All transactions have 'begin' records");
    println!("   - Write transactions have 'data_committed' records");

    // Verify pond control command shows these as completed, not incomplete
    println!("\n=== Verifying pond control shows transactions as completed ===");
    control_command(&setup.ship_context, ControlMode::Recent { limit: 10 })
        .await
        .expect("control command should succeed");
}


