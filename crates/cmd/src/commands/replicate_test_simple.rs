//! Simplified replication tests
//!
//! Tests verify the basic replication and sync workflow without
//! full command-line integration

use anyhow::Result;
use datafusion::prelude::SessionContext;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use crate::common::ShipContext;
use crate::commands::init_command;

/// Simple test setup
struct SimpleReplicationTest {
    source_pond: PathBuf,
    #[allow(dead_code)]
    replica_pond: PathBuf,
    #[allow(dead_code)]
    backups_path: PathBuf,
    _source_temp: TempDir,
    _replica_temp: TempDir,
    _backups_temp: TempDir,
}

impl SimpleReplicationTest {
    fn new() -> Result<Self> {
        let source_temp = TempDir::new()?;
        let replica_temp = TempDir::new()?;
        let backups_temp = TempDir::new()?;

        Ok(Self {
            source_pond: source_temp.path().to_path_buf(),
            replica_pond: replica_temp.path().to_path_buf(),
            backups_path: backups_temp.path().to_path_buf(),
            _source_temp: source_temp,
            _replica_temp: replica_temp,
            _backups_temp: backups_temp,
        })
    }

    async fn init_source(&self) -> Result<()> {
        let ship_context = ShipContext::new(
            Some(self.source_pond.clone()),
            vec!["pond".to_string()],
        );

        init_command(&ship_context, None, None).await?;
        Ok(())
    }

    async fn verify_source_pond_identity(&self) -> Result<String> {
        let ship_context = ShipContext::new(
            Some(self.source_pond.clone()),
            vec!["pond".to_string()],
        );

        let ship = ship_context.open_pond().await?;
        let control_table = ship.control_table();

        let pond_metadata = control_table
            .get_pond_metadata()
            .await?
            .ok_or_else(|| anyhow::anyhow!("Source pond has no metadata"))?;

        Ok(pond_metadata.pond_id)
    }

    /// Compare two ponds to verify they have the same identity
    async fn compare_pond_identity(
        pond1_path: &PathBuf,
        pond2_path: &PathBuf,
    ) -> Result<(String, String, bool)> {
        let ship1 = ShipContext::new(Some(pond1_path.clone()), vec!["pond".to_string()])
            .open_pond()
            .await?;
        let ship2 = ShipContext::new(Some(pond2_path.clone()), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let metadata1 = ship1
            .control_table()
            .get_pond_metadata()
            .await?
            .ok_or_else(|| anyhow::anyhow!("Pond 1 has no metadata"))?;

        let metadata2 = ship2
            .control_table()
            .get_pond_metadata()
            .await?
            .ok_or_else(|| anyhow::anyhow!("Pond 2 has no metadata"))?;

        // Check if identity matches
        let matches = metadata1.pond_id == metadata2.pond_id
            && metadata1.birth_timestamp == metadata2.birth_timestamp
            && metadata1.birth_hostname == metadata2.birth_hostname
            && metadata1.birth_username == metadata2.birth_username;

        Ok((metadata1.pond_id.clone(), metadata2.pond_id.clone(), matches))
    }

    /// Compare transaction sequences between two ponds
    async fn compare_transaction_sequences(
        pond1_path: &PathBuf,
        pond2_path: &PathBuf,
    ) -> Result<(i64, i64)> {
        let ship1 = ShipContext::new(Some(pond1_path.clone()), vec!["pond".to_string()])
            .open_pond()
            .await?;
        let ship2 = ShipContext::new(Some(pond2_path.clone()), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let last_seq_1 = ship1.control_table().get_last_write_sequence().await?;
        let last_seq_2 = ship2.control_table().get_last_write_sequence().await?;

        Ok((last_seq_1, last_seq_2))
    }

    /// Query transaction records from a pond
    async fn get_transaction_records(pond_path: &PathBuf) -> Result<Vec<(i64, String)>> {
        let ship = ShipContext::new(Some(pond_path.clone()), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let control_table = ship.control_table();
        let ctx = SessionContext::new();
        
        ctx.register_table("transactions", Arc::new(control_table.table().clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))?;

        let sql = r#"
            SELECT txn_seq, record_type
            FROM transactions
            WHERE transaction_type = 'write'
            ORDER BY txn_seq
        "#;

        let df = ctx.sql(sql).await
            .map_err(|e| anyhow::anyhow!("Failed to query: {}", e))?;

        let batches = df.collect().await
            .map_err(|e| anyhow::anyhow!("Failed to collect: {}", e))?;

        let mut records = Vec::new();
        
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let txn_seqs = batch.column_by_name("txn_seq")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            
            let record_types = batch.column_by_name("record_type")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                records.push((txn_seqs.value(i), record_types.value(i).to_string()));
            }
        }

        Ok(records)
    }

    /// Get comprehensive statistics about a pond's structure
    async fn get_pond_stats(pond_path: &PathBuf) -> Result<PondStats, anyhow::Error> {
        let ship = ShipContext::new(Some(pond_path.clone()), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let control_table = ship.control_table();
        let ctx = SessionContext::new();
        
        // Register control table
        ctx.register_table("transactions", Arc::new(control_table.table().clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register transactions table: {}", e))?;

        // Get transaction count
        let txn_count = ctx
            .sql("SELECT COUNT(*) as cnt FROM transactions WHERE transaction_type = 'write'")
            .await?
            .collect()
            .await?;
        let transaction_count = txn_count[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        // Get last write sequence
        let last_write_seq = control_table.get_last_write_sequence().await?;

        // Get commit history from data persistence
        let persistence = tlogfs::OpLogPersistence::open(&format!(
            "{}/data",
            pond_path.to_string_lossy()
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open data persistence: {}", e))?;

        let commit_history = persistence
            .get_commit_history(None) // Get all commits
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get commit history: {}", e))?;

        let commit_count = commit_history.len() as i64;

        // Query oplog entries (nodes created/modified)
        let data_table = persistence.table();
        ctx.register_table("oplog", Arc::new(data_table.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register oplog table: {}", e))?;

        // Count nodes by file_type
        let node_stats = ctx
            .sql(
                r#"
                SELECT 
                    file_type,
                    COUNT(DISTINCT node_id) as count
                FROM oplog
                GROUP BY file_type
                ORDER BY file_type
                "#,
            )
            .await?
            .collect()
            .await?;

        let mut nodes_by_type = std::collections::HashMap::new();
        for batch in node_stats {
            if batch.num_rows() == 0 {
                continue;
            }

            let file_types = batch
                .column_by_name("file_type")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let counts = batch
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let file_type = file_types.value(i).to_string();
                let count = counts.value(i);
                nodes_by_type.insert(file_type, count);
            }
        }

        // Count total unique nodes
        let total_nodes = ctx
            .sql("SELECT COUNT(DISTINCT node_id) as cnt FROM oplog")
            .await?
            .collect()
            .await?;
        let total_node_count = total_nodes[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        // Count partitions (unique part_id values)
        let partition_count = ctx
            .sql("SELECT COUNT(DISTINCT part_id) as cnt FROM oplog")
            .await?
            .collect()
            .await?;
        let partition_count = partition_count[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        Ok(PondStats {
            transaction_count,
            last_write_seq,
            commit_count,
            total_node_count,
            partition_count,
            nodes_by_type,
        })
    }
}

/// Statistics about a pond's structure for comparison
#[derive(Debug, Clone, PartialEq)]
struct PondStats {
    /// Number of write transactions in control table
    transaction_count: i64,
    /// Last write sequence number
    last_write_seq: i64,
    /// Number of commits in data store history
    commit_count: i64,
    /// Total number of unique nodes
    total_node_count: i64,
    /// Number of unique partitions (part_id values)
    partition_count: i64,
    /// Breakdown of nodes by file_type (DirectoryPhysical, FileDataPhysical, etc.)
    nodes_by_type: std::collections::HashMap<String, i64>,
}

#[tokio::test]
async fn test_simple_pond_creation() -> Result<()> {
    let test = SimpleReplicationTest::new()?;
    test.init_source().await?;
    
    let pond_id = test.verify_source_pond_identity().await?;
    assert!(!pond_id.is_empty(), "Pond ID should not be empty");
    assert_eq!(pond_id.len(), 36, "Pond ID should be a UUID (36 chars)");

    Ok(())
}

#[tokio::test]
async fn test_two_independent_ponds_have_different_ids() -> Result<()> {
    let test1 = SimpleReplicationTest::new()?;
    test1.init_source().await?;
    let pond_id_1 = test1.verify_source_pond_identity().await?;

    let test2 = SimpleReplicationTest::new()?;
    test2.init_source().await?;
    let pond_id_2 = test2.verify_source_pond_identity().await?;

    assert_ne!(
        pond_id_1, pond_id_2,
        "Two independent ponds should have different IDs"
    );

    Ok(())
}

#[tokio::test]
async fn test_pond_identity_comparison() -> Result<()> {
    // Create two ponds
    let test1 = SimpleReplicationTest::new()?;
    test1.init_source().await?;

    let test2 = SimpleReplicationTest::new()?;
    test2.init_source().await?;

    // Compare identities - should be different
    let (id1, id2, matches) = 
        SimpleReplicationTest::compare_pond_identity(&test1.source_pond, &test2.source_pond).await?;

    assert_ne!(id1, id2, "Two independent ponds should have different IDs");
    assert!(!matches, "Two independent ponds should not have matching identity");

    // Compare transaction sequences - both should have same initial sequence
    let (seq1, seq2) = 
        SimpleReplicationTest::compare_transaction_sequences(&test1.source_pond, &test2.source_pond).await?;
    
    // Both ponds just created, should have the same initial sequence (likely 0 or 1)
    assert_eq!(seq1, seq2, "Newly created ponds should have same initial sequence");

    Ok(())
}

#[tokio::test]
async fn test_query_transaction_records() -> Result<()> {
    let test = SimpleReplicationTest::new()?;
    test.init_source().await?;

    // Query transaction records
    let records = SimpleReplicationTest::get_transaction_records(&test.source_pond).await?;

    // A newly initialized pond should have at least one transaction record
    assert!(!records.is_empty(), "Initialized pond should have transaction records");

    // Check that we have "begin" record (init creates a transaction)
    let record_types: Vec<String> = records.iter().map(|(_, rt)| rt.clone()).collect();
    assert!(
        record_types.contains(&"begin".to_string()),
        "Init should create transaction with 'begin' record type"
    );

    // Verify transaction sequences are sequential
    let txn_seqs: Vec<i64> = records.iter().map(|(seq, _)| *seq).collect();
    for window in txn_seqs.windows(2) {
        assert!(
            window[1] > window[0],
            "Transaction sequences should be monotonically increasing"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_pond_structural_statistics() -> Result<()> {
    let test = SimpleReplicationTest::new()?;
    test.init_source().await?;

    // Get statistics about the pond structure
    let stats = SimpleReplicationTest::get_pond_stats(&test.source_pond).await?;

    // Debug: print what we got
    println!("Pond stats: {:?}", stats);
    println!("Nodes by type: {:?}", stats.nodes_by_type);

    // Verify basic structure
    assert!(
        stats.transaction_count > 0,
        "Should have at least one transaction"
    );
    assert!(stats.commit_count > 0, "Should have at least one commit");
    assert!(
        stats.total_node_count > 0,
        "Should have at least one node (root directory)"
    );

    // Verify we have directory nodes (at least root) - "dir:physical" in the serialized schema
    let has_directories = stats.nodes_by_type.contains_key("dir:physical")
        || stats.nodes_by_type.contains_key("dir:dynamic");
    
    assert!(
        has_directories,
        "Should have directory nodes. Got: {:?}",
        stats.nodes_by_type.keys().collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn test_compare_pond_structures() -> Result<()> {
    // Create two independent ponds
    let test1 = SimpleReplicationTest::new()?;
    test1.init_source().await?;

    let test2 = SimpleReplicationTest::new()?;
    test2.init_source().await?;

    // Get statistics from both
    let stats1 = SimpleReplicationTest::get_pond_stats(&test1.source_pond).await?;
    let stats2 = SimpleReplicationTest::get_pond_stats(&test2.source_pond).await?;

    // Two freshly initialized ponds should have identical structure
    assert_eq!(
        stats1.transaction_count, stats2.transaction_count,
        "Transaction counts should match"
    );
    assert_eq!(
        stats1.commit_count, stats2.commit_count,
        "Commit counts should match"
    );
    assert_eq!(
        stats1.total_node_count, stats2.total_node_count,
        "Total node counts should match"
    );
    assert_eq!(
        stats1.partition_count, stats2.partition_count,
        "Partition counts should match"
    );
    assert_eq!(
        stats1.nodes_by_type, stats2.nodes_by_type,
        "Node type distributions should match"
    );

    // But they should NOT have the same identity (different pond IDs)
    let (id1, id2, matches) =
        SimpleReplicationTest::compare_pond_identity(&test1.source_pond, &test2.source_pond)
            .await?;
    assert_ne!(id1, id2, "Different ponds should have different IDs");
    assert!(!matches, "Different ponds should not match identity");

    Ok(())
}

