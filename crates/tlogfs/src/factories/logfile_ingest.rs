// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Logfile Ingestion Factory Tests
//!
//! The canonical logfile-ingest factory implementation is in the provider crate:
//! `provider::factory::logfile_ingest`
//!
//! This module contains integration tests that exercise the factory with
//! OpLogPersistence (the production persistence layer).

// Re-export the config type from provider for convenience in tests
pub use provider::factory::logfile_ingest::LogfileIngestConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let config = LogfileIngestConfig {
            name: "test".to_string(),
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_config_empty_name() {
        let config = LogfileIngestConfig {
            name: "".to_string(),
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
        };

        assert!(config.validate().is_err());
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for logfile snapshotting and incremental verification
    //!
    //! These tests simulate a growing logfile and verify that:
    //! 1. Initial snapshots capture the full file correctly
    //! 2. Appends are detected and ingested incrementally
    //! 3. Incremental checksum verification (bao-tree) works correctly
    //! 4. Content is correctly copied into the pond

    use super::LogfileIngestConfig;
    use provider::{ExecutionContext, FactoryContext};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;

    /// Test fixture for simulating a growing logfile
    struct LogfileSimulator {
        _temp_dir: TempDir,
        log_path: PathBuf,
        content: Vec<u8>,
    }

    impl LogfileSimulator {
        /// Create a new simulator with an empty logfile
        async fn new() -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let log_path = temp_dir.path().join("app.log");

            // Create empty file
            fs::write(&log_path, b"").await?;

            Ok(Self {
                _temp_dir: temp_dir,
                log_path,
                content: Vec::new(),
            })
        }

        /// Append a line to the logfile
        async fn append_line(&mut self, line: &str) -> std::io::Result<()> {
            let mut line_bytes = line.as_bytes().to_vec();
            line_bytes.push(b'\n');

            self.content.extend_from_slice(&line_bytes);

            // Append to file
            use tokio::io::AsyncWriteExt;
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.log_path)
                .await?;
            file.write_all(&line_bytes).await?;
            file.flush().await?;

            Ok(())
        }

        /// Get the current size of the logfile
        fn size(&self) -> u64 {
            self.content.len() as u64
        }

        /// Get the full content for verification
        fn content(&self) -> &[u8] {
            &self.content
        }

        /// Get the path to the logfile
        fn path(&self) -> &PathBuf {
            &self.log_path
        }

        /// Compute blake3 hash of current content
        fn blake3(&self) -> String {
            blake3::hash(&self.content).to_hex().to_string()
        }
    }

    /// Test fixture for pond state
    struct PondSimulator {
        _temp_dir: TempDir,
        store_path: String,
        /// FileID of the config node (created in new())
        config_file_id: tinyfs::FileID,
    }

    impl PondSimulator {
        /// Create a new pond with a config file for logfile-ingest factory
        async fn new(config: &LogfileIngestConfig) -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let store_path = temp_dir
                .path()
                .join("pond.db")
                .to_string_lossy()
                .to_string();

            // Initialize pond persistence using tlogfs OpLogPersistence
            let mut persistence = crate::persistence::OpLogPersistence::create_test(&store_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Create config file in the pond and associate with factory
            let config_file_id = {
                let tx = persistence
                    .begin_test()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let state = tx
                    .state()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let fs = tinyfs::FS::new(state)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let root = fs
                    .root()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Create /etc/system.d/ directory structure
                _ = root
                    .create_dir_path("/etc")
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                _ = root
                    .create_dir_path("/etc/system.d")
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Serialize config to YAML
                let config_yaml = serde_yaml::to_string(&config)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Create config file with factory association
                let config_node = root
                    .create_dynamic_path(
                        "/etc/system.d/logfile-ingest.yaml",
                        tinyfs::EntryType::FileDynamic,
                        "logfile-ingest",
                        config_yaml.as_bytes().to_vec(),
                    )
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let file_id = config_node.id();

                tx.commit_test()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                file_id
            };

            Ok(Self {
                _temp_dir: temp_dir,
                store_path,
                config_file_id,
            })
        }

        /// Get store path
        fn store_path(&self) -> &str {
            &self.store_path
        }

        /// Execute the logfile-ingest factory within a transaction
        /// This properly opens a transaction, creates the context, executes, and commits
        async fn execute_factory(&self, config: &LogfileIngestConfig) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(self.store_path())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Get State from transaction - it implements PersistenceLayer
            let state = tx
                .state()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Create ProviderContext from State
            let provider_ctx = state.as_provider_context();

            // Create FactoryContext with real config FileID
            let factory_ctx = FactoryContext::new(provider_ctx, self.config_file_id);

            // Execute the factory using the provider crate's implementation
            let exec_ctx = ExecutionContext::pond_readwriter(vec![]);
            provider::factory::logfile_ingest::execute(
                serde_json::to_value(config).map_err(|e| std::io::Error::other(e.to_string()))?,
                factory_ctx,
                exec_ctx,
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Commit the transaction to persist changes
            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(())
        }

        /// Verify a file exists in the pond with expected properties
        async fn verify_file(
            &self,
            pond_path: &str,
            filename: &str,
            expected_size: u64,
            expected_blake3: &str,
            expected_content: &[u8],
        ) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(self.store_path())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;
            let file = file_node
                .as_file()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Read content
            use tokio::io::AsyncReadExt;
            let mut content = Vec::new();
            let mut reader = file
                .async_reader()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = reader.read_to_end(&mut content).await?;

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Verify size
            assert_eq!(
                content.len() as u64,
                expected_size,
                "File size mismatch: expected {}, got {}",
                expected_size,
                content.len()
            );

            // Verify blake3
            let actual_blake3 = blake3::hash(&content).to_hex().to_string();
            assert_eq!(actual_blake3, expected_blake3, "Blake3 hash mismatch");

            // Verify byte-by-byte content
            assert_eq!(
                content.as_slice(),
                expected_content,
                "Content mismatch: bytes differ"
            );

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_single_logfile_growth_and_snapshots() {
        // Setup: Create a simulated logfile first
        let mut logfile = LogfileSimulator::new().await.unwrap();

        // Configuration for logfile ingestion (needs logfile path)
        let config = LogfileIngestConfig {
            name: "test_app".to_string(),
            active_pattern: logfile.path().to_string_lossy().to_string(),
            archived_pattern: "".to_string(), // No archived files for this test
            pond_path: "logs/test_app".to_string(),
        };

        // Create pond with the config (creates real config file with FileID)
        let pond = PondSimulator::new(&config).await.unwrap();

        // Step 1: Append initial content to logfile
        logfile
            .append_line("2025-01-03 00:00:00 INFO Starting application")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:01 INFO Initialized database")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:02 INFO Listening on port 8080")
            .await
            .unwrap();

        let size_v1 = logfile.size();
        let blake3_v1 = logfile.blake3();

        println!("Logfile v1: {} bytes, blake3={}", size_v1, &blake3_v1[..16]);

        // Step 2: First ingestion - should capture entire file as v1
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should exist in pond with correct content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v1,
            &blake3_v1,
            logfile.content(),
        )
        .await
        .unwrap();

        println!("✓ Initial snapshot successful");

        // Step 3: Append more content to logfile
        logfile
            .append_line("2025-01-03 00:01:00 INFO Processed 100 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:02:00 INFO Processed 200 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:03:00 WARN Connection timeout for client 127.0.0.1")
            .await
            .unwrap();

        let size_v2 = logfile.size();
        let blake3_v2 = logfile.blake3();
        let appended_bytes = size_v2 - size_v1;

        println!(
            "Logfile v2: {} bytes (+{} appended), blake3={}",
            size_v2,
            appended_bytes,
            &blake3_v2[..16]
        );

        // Step 4: Second ingestion - should detect append and create v2
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should have updated content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v2,
            &blake3_v2,
            logfile.content(),
        )
        .await
        .unwrap();

        println!("✓ Incremental append successful");

        // Step 5: Append even more content
        logfile
            .append_line("2025-01-03 00:04:00 INFO Processed 300 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:05:00 ERROR Database connection failed")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:05:01 INFO Reconnected to database")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:06:00 INFO Processed 400 requests")
            .await
            .unwrap();

        let size_v3 = logfile.size();
        let blake3_v3 = logfile.blake3();
        let appended_bytes_v3 = size_v3 - size_v2;

        println!(
            "Logfile v3: {} bytes (+{} appended), blake3={}",
            size_v3,
            appended_bytes_v3,
            &blake3_v3[..16]
        );

        // Step 6: Third ingestion - should detect second append and create v3
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should have latest content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v3,
            &blake3_v3,
            logfile.content(),
        )
        .await
        .unwrap();

        println!("✓ Second incremental append successful");
        println!("✓ All snapshots and verifications passed!");
    }

    #[tokio::test]
    async fn test_logfile_no_change_detection() {
        // Test that running ingestion multiple times without file changes is idempotent
        let mut logfile = LogfileSimulator::new().await.unwrap();

        let config = LogfileIngestConfig {
            name: "test_app".to_string(),
            active_pattern: logfile.path().to_string_lossy().to_string(),
            archived_pattern: "".to_string(),
            pond_path: "logs/test_app".to_string(),
        };

        // Create pond with config (gets real FileID)
        let pond = PondSimulator::new(&config).await.unwrap();

        // Append some content
        logfile
            .append_line("2025-01-03 00:00:00 INFO Test line 1")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:01 INFO Test line 2")
            .await
            .unwrap();

        let size = logfile.size();
        let blake3 = logfile.blake3();

        // First ingestion
        pond.execute_factory(&config).await.unwrap();

        // Second ingestion - no changes
        pond.execute_factory(&config).await.unwrap();

        // Third ingestion - still no changes
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should still have same content
        pond.verify_file("logs/test_app", "app.log", size, &blake3, logfile.content())
            .await
            .unwrap();

        println!("✓ Idempotency test passed - multiple ingestions without changes work correctly");
    }

    #[tokio::test]
    async fn test_logfile_rotation() {
        //! Test log rotation scenario:
        //! 1. Active file grows, we snapshot it
        //! 2. Active file rotates (becomes archived with timestamp, new active file starts)
        //! 3. Factory should:
        //!    - Rename pond's active file to match the archived filename (preserving version history)
        //!    - Create a new file for the new active content
        //!
        //! Assumption: We snapshot at least once before a second rotation happens,
        //! so there is only one reasonable candidate for matching.

        // Setup: Create temp directory for log files
        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("app.log");
        let archived_pattern = temp_dir.path().join("app-*.log").to_string_lossy().to_string();

        // Create initial active file with some content
        let initial_content = b"2025-01-03 00:00:00 INFO Starting application\n\
                                2025-01-03 00:00:01 INFO Initialized database\n\
                                2025-01-03 00:00:02 INFO Listening on port 8080\n";
        fs::write(&active_path, initial_content).await.unwrap();

        let initial_blake3 = blake3::hash(initial_content).to_hex().to_string();
        let initial_size = initial_content.len() as u64;

        // Configuration
        let config = LogfileIngestConfig {
            name: "test_app".to_string(),
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: archived_pattern.clone(),
            pond_path: "logs/test_app".to_string(),
        };

        // Create pond
        let pond = PondSimulator::new(&config).await.unwrap();

        // Step 1: First ingestion - capture initial active file
        pond.execute_factory(&config).await.unwrap();

        pond.verify_file(
            "logs/test_app",
            "app.log",
            initial_size,
            &initial_blake3,
            initial_content,
        )
        .await
        .unwrap();

        println!("✓ Step 1: Initial snapshot captured ({} bytes)", initial_size);

        // Step 2: Simulate file growth before rotation
        let growth_content = b"2025-01-03 00:01:00 INFO Processed 100 requests\n\
                               2025-01-03 00:02:00 INFO Processed 200 requests\n";
        let mut grown_content = initial_content.to_vec();
        grown_content.extend_from_slice(growth_content);

        fs::write(&active_path, &grown_content).await.unwrap();

        let grown_blake3 = blake3::hash(&grown_content).to_hex().to_string();
        let grown_size = grown_content.len() as u64;

        // Capture the growth
        pond.execute_factory(&config).await.unwrap();

        pond.verify_file(
            "logs/test_app",
            "app.log",
            grown_size,
            &grown_blake3,
            &grown_content,
        )
        .await
        .unwrap();

        println!("✓ Step 2: File growth captured ({} bytes)", grown_size);

        // Step 3: Simulate rotation
        // - Active file becomes archived with timestamp
        // - New active file starts fresh
        let archived_path = temp_dir.path().join("app-2025-01-03T00-03-00.log");
        fs::rename(&active_path, &archived_path).await.unwrap();

        // New active file with fresh content
        let new_active_content = b"2025-01-03 00:03:00 INFO Log rotated - starting fresh\n\
                                   2025-01-03 00:03:01 INFO New log session begins\n";
        fs::write(&active_path, new_active_content).await.unwrap();

        let new_active_blake3 = blake3::hash(new_active_content).to_hex().to_string();
        let new_active_size = new_active_content.len() as u64;

        println!(
            "Rotation: archived {} bytes to {}, new active {} bytes",
            grown_size,
            archived_path.file_name().unwrap().to_string_lossy(),
            new_active_size
        );

        // Step 4: Ingestion after rotation should:
        // - Detect that active file shrunk (rotation occurred)
        // - Find the archived file that matches the pond's tracked content
        // - Rename pond file from "app.log" to "app-2025-01-03T00-03-00.log"
        // - Create new "app.log" with the fresh content
        pond.execute_factory(&config).await.unwrap();

        // Verify: The archived file in pond should have the pre-rotation content
        // (with all version history preserved from when it was the active file)
        pond.verify_file(
            "logs/test_app",
            "app-2025-01-03T00-03-00.log",
            grown_size,
            &grown_blake3,
            &grown_content,
        )
        .await
        .unwrap();

        println!("✓ Step 4a: Archived file preserved with correct content");

        // Verify: The new active file should have the new content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            new_active_size,
            &new_active_blake3,
            new_active_content,
        )
        .await
        .unwrap();

        println!("✓ Step 4b: New active file created with fresh content");

        // Step 5: Continue growing the new active file to ensure incremental append still works
        let more_content = b"2025-01-03 00:04:00 INFO Post-rotation processing continues\n";
        let mut final_active_content = new_active_content.to_vec();
        final_active_content.extend_from_slice(more_content);

        fs::write(&active_path, &final_active_content).await.unwrap();

        let final_active_blake3 = blake3::hash(&final_active_content).to_hex().to_string();
        let final_active_size = final_active_content.len() as u64;

        pond.execute_factory(&config).await.unwrap();

        pond.verify_file(
            "logs/test_app",
            "app.log",
            final_active_size,
            &final_active_blake3,
            &final_active_content,
        )
        .await
        .unwrap();

        println!("✓ Step 5: Post-rotation append captured");

        // Verify archived file is still intact
        pond.verify_file(
            "logs/test_app",
            "app-2025-01-03T00-03-00.log",
            grown_size,
            &grown_blake3,
            &grown_content,
        )
        .await
        .unwrap();

        println!("✓ Archived file still intact after subsequent operations");
        println!("✓ All rotation tests passed!");
    }

    /// Extended verification helper that checks both content AND metadata blake3
    struct Blake3Verifier {
        store_path: String,
    }

    impl Blake3Verifier {
        fn new(store_path: &str) -> Self {
            Self {
                store_path: store_path.to_string(),
            }
        }

        /// Verify that a file's cumulative blake3 matches expected value.
        /// This checks BOTH:
        /// 1. The content hashes to the expected blake3
        /// 2. The stored OplogEntry.blake3 field matches
        /// 3. The SeriesOutboard.cumulative_blake3 matches (for FilePhysicalSeries)
        async fn verify_file_blake3(
            &self,
            pond_path: &str,
            filename: &str,
            expected_content: &[u8],
        ) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(&self.store_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;
            
            let file_id = file_node.id();

            // 1. Read content and verify it matches expected
            use tokio::io::AsyncReadExt;
            let file = file_node
                .as_file()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let mut content = Vec::new();
            let mut reader = file
                .async_reader()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = reader.read_to_end(&mut content).await?;

            assert_eq!(
                content.as_slice(),
                expected_content,
                "Content bytes don't match expected"
            );

            // 2. Compute expected cumulative blake3 using IncrementalHashState
            // IMPORTANT: We use IncrementalHashState::root_hash(), NOT blake3::hash()!
            // For FilePhysicalSeries, the cumulative_blake3 is the bao-tree incremental
            // root hash, which differs from blake3::hash() for exactly 1 block (16KB).
            // This is intentional - it ensures consistency with append operations.
            let mut hash_state = utilities::bao_outboard::IncrementalHashState::new();
            hash_state.ingest(expected_content);
            let expected_blake3 = hash_state.root_hash().to_hex().to_string();

            // 3. Query OplogEntry records to verify stored blake3
            let state = tx.state().map_err(|e| std::io::Error::other(e.to_string()))?;
            let entries = state
                .query_records(file_id)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            assert!(!entries.is_empty(), "No OplogEntry records found for file");

            // Get the latest version's entry
            let latest_entry = entries
                .iter()
                .max_by_key(|e| e.version)
                .expect("No entries found");

            // 4. Verify OplogEntry.blake3 field
            let stored_blake3 = latest_entry
                .blake3
                .as_ref()
                .expect("OplogEntry.blake3 should be set for FilePhysicalSeries");

            assert_eq!(
                stored_blake3, &expected_blake3,
                "OplogEntry.blake3 mismatch!\n  Expected: {}\n  Stored:   {}",
                expected_blake3, stored_blake3
            );

            // 5. Verify SeriesOutboard.cumulative_blake3 (if bao_outboard present)
            if let Some(bao_bytes) = latest_entry.get_bao_outboard() {
                let series_outboard = utilities::bao_outboard::SeriesOutboard::from_bytes(bao_bytes)
                    .map_err(|e| std::io::Error::other(format!("Failed to parse SeriesOutboard: {}", e)))?;
                
                let outboard_blake3 = blake3::Hash::from_bytes(series_outboard.cumulative_blake3)
                    .to_hex()
                    .to_string();
                
                assert_eq!(
                    outboard_blake3, expected_blake3,
                    "SeriesOutboard.cumulative_blake3 mismatch!\n  Expected: {}\n  Outboard: {}",
                    expected_blake3, outboard_blake3
                );

                // Also verify cumulative_size matches content length
                assert_eq!(
                    series_outboard.cumulative_size,
                    expected_content.len() as u64,
                    "SeriesOutboard.cumulative_size mismatch"
                );
            }

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(())
        }

        /// Get version count for a file
        async fn get_version_count(&self, pond_path: &str, filename: &str) -> std::io::Result<usize> {
            let mut persistence = crate::persistence::OpLogPersistence::open(&self.store_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;

            let state = tx.state().map_err(|e| std::io::Error::other(e.to_string()))?;
            let entries = state
                .query_records(file_node.id())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(entries.len())
        }
    }

    #[tokio::test]
    async fn test_blake3_validation_small_versions_then_rotation() {
        //! Comprehensive blake3 validation test:
        //! 1. Create small file (v1) - verify cumulative blake3
        //! 2. Append small content (v2) - verify cumulative blake3
        //! 3. Append more content (v3) - verify cumulative blake3
        //! 4. Rotate: rename active to archived, create new active
        //! 5. Verify archived file still has correct cumulative blake3
        //! 6. Verify new active file has its own blake3
        //! 7. Append to new active (v2 of new file) - verify cumulative blake3

        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("app.log");
        let archived_pattern = temp_dir.path().join("app-*.log").to_string_lossy().to_string();

        // === Phase 1: Small Version 1 ===
        let v1_content = b"2025-01-03 00:00:00 INFO Starting\n";
        fs::write(&active_path, v1_content).await.unwrap();

        let config = LogfileIngestConfig {
            name: "blake3_test".to_string(),
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: archived_pattern.clone(),
            pond_path: "logs/blake3_test".to_string(),
        };

        let pond = PondSimulator::new(&config).await.unwrap();
        let verifier = Blake3Verifier::new(pond.store_path());

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/blake3_test", "app.log", v1_content).await.unwrap();
        println!("✓ v1 ({} bytes): cumulative blake3 correct", v1_content.len());

        // === Phase 2: Append small content (v2) ===
        let v2_append = b"2025-01-03 00:01:00 INFO Processing\n";
        let mut v2_cumulative = v1_content.to_vec();
        v2_cumulative.extend_from_slice(v2_append);
        fs::write(&active_path, &v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/blake3_test", "app.log", &v2_cumulative).await.unwrap();
        let v2_versions = verifier.get_version_count("logs/blake3_test", "app.log").await.unwrap();
        assert_eq!(v2_versions, 2, "Should have 2 versions after append");
        println!("✓ v2 ({} bytes, {} versions): cumulative blake3 correct", v2_cumulative.len(), v2_versions);

        // === Phase 3: Append more content (v3) ===
        let v3_append = b"2025-01-03 00:02:00 INFO More work\n2025-01-03 00:03:00 INFO Done\n";
        let mut v3_cumulative = v2_cumulative.clone();
        v3_cumulative.extend_from_slice(v3_append);
        fs::write(&active_path, &v3_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/blake3_test", "app.log", &v3_cumulative).await.unwrap();
        let v3_versions = verifier.get_version_count("logs/blake3_test", "app.log").await.unwrap();
        assert_eq!(v3_versions, 3, "Should have 3 versions");
        println!("✓ v3 ({} bytes, {} versions): cumulative blake3 correct", v3_cumulative.len(), v3_versions);

        // === Phase 4: Rotation ===
        let archived_filename = "app-2025-01-03T00-04-00.log";
        let archived_path = temp_dir.path().join(archived_filename);
        fs::rename(&active_path, &archived_path).await.unwrap();

        // New active file
        let new_v1_content = b"2025-01-03 00:04:00 INFO Log rotated\n";
        fs::write(&active_path, new_v1_content).await.unwrap();

        pond.execute_factory(&config).await.unwrap();

        // === Phase 5: Verify archived file ===
        verifier.verify_file_blake3("logs/blake3_test", archived_filename, &v3_cumulative).await.unwrap();
        let archived_versions = verifier.get_version_count("logs/blake3_test", archived_filename).await.unwrap();
        assert_eq!(archived_versions, 3, "Archived file should preserve 3 versions");
        println!("✓ Archived file ({} bytes, {} versions): cumulative blake3 preserved", v3_cumulative.len(), archived_versions);

        // === Phase 6: Verify new active file ===
        verifier.verify_file_blake3("logs/blake3_test", "app.log", new_v1_content).await.unwrap();
        let new_v1_versions = verifier.get_version_count("logs/blake3_test", "app.log").await.unwrap();
        assert_eq!(new_v1_versions, 1, "New active should have 1 version");
        println!("✓ New active v1 ({} bytes, {} version): blake3 correct", new_v1_content.len(), new_v1_versions);

        // === Phase 7: Append to new active ===
        let new_v2_append = b"2025-01-03 00:05:00 INFO Post-rotation work\n";
        let mut new_v2_cumulative = new_v1_content.to_vec();
        new_v2_cumulative.extend_from_slice(new_v2_append);
        fs::write(&active_path, &new_v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/blake3_test", "app.log", &new_v2_cumulative).await.unwrap();
        let new_v2_versions = verifier.get_version_count("logs/blake3_test", "app.log").await.unwrap();
        assert_eq!(new_v2_versions, 2, "New active should have 2 versions");
        println!("✓ New active v2 ({} bytes, {} versions): cumulative blake3 correct", new_v2_cumulative.len(), new_v2_versions);

        // Final verification: archived file unchanged
        verifier.verify_file_blake3("logs/blake3_test", archived_filename, &v3_cumulative).await.unwrap();
        println!("✓ Archived file still correct after new active changes");

        println!("\n✓ All blake3 validation tests passed!");
    }

    #[tokio::test]
    async fn test_blake3_validation_large_versions() {
        //! Test blake3 validation with larger versions that span multiple bao-tree blocks.
        //! Block size is 16KB, so we test with content > 16KB to exercise incremental hashing.

        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("large.log");
        let archived_pattern = temp_dir.path().join("large-*.log").to_string_lossy().to_string();

        // === Phase 1: Create 20KB file (> 1 block) ===
        let v1_content: Vec<u8> = (0..20_000)
            .map(|i| format!("Line {}: {:0>100}\n", i, i))
            .flat_map(|s| s.into_bytes())
            .take(20_000)
            .collect();
        fs::write(&active_path, &v1_content).await.unwrap();

        let config = LogfileIngestConfig {
            name: "large_blake3_test".to_string(),
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: archived_pattern.clone(),
            pond_path: "logs/large_test".to_string(),
        };

        let pond = PondSimulator::new(&config).await.unwrap();
        let verifier = Blake3Verifier::new(pond.store_path());

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/large_test", "large.log", &v1_content).await.unwrap();
        println!("✓ Large v1 ({} bytes, {} blocks): blake3 correct", v1_content.len(), v1_content.len() / 16384 + 1);

        // === Phase 2: Append 18KB (crosses block boundary) ===
        let v2_append: Vec<u8> = (0..18_000)
            .map(|i| format!("Append {}: {:0>90}\n", i, i))
            .flat_map(|s| s.into_bytes())
            .take(18_000)
            .collect();
        let mut v2_cumulative = v1_content.clone();
        v2_cumulative.extend_from_slice(&v2_append);
        fs::write(&active_path, &v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/large_test", "large.log", &v2_cumulative).await.unwrap();
        println!("✓ Large v2 ({} bytes, {} blocks): cumulative blake3 correct", 
                 v2_cumulative.len(), v2_cumulative.len() / 16384 + 1);

        // === Phase 3: Rotation with large file ===
        let archived_filename = "large-2025-01-03.log";
        let archived_path = temp_dir.path().join(archived_filename);
        fs::rename(&active_path, &archived_path).await.unwrap();

        // New active with 25KB content
        let new_v1_content: Vec<u8> = (0..25_000)
            .map(|i| format!("New {}: {:0>95}\n", i, i))
            .flat_map(|s| s.into_bytes())
            .take(25_000)
            .collect();
        fs::write(&active_path, &new_v1_content).await.unwrap();

        pond.execute_factory(&config).await.unwrap();

        verifier.verify_file_blake3("logs/large_test", archived_filename, &v2_cumulative).await.unwrap();
        println!("✓ Archived large ({} bytes): blake3 preserved", v2_cumulative.len());

        verifier.verify_file_blake3("logs/large_test", "large.log", &new_v1_content).await.unwrap();
        println!("✓ New large v1 ({} bytes, {} blocks): blake3 correct", 
                 new_v1_content.len(), new_v1_content.len() / 16384 + 1);

        println!("\n✓ All large file blake3 validation tests passed!");
    }

    #[tokio::test]
    async fn test_blake3_validation_block_boundary_append() {
        //! Test appending content that crosses exactly at block boundaries (16KB).
        //! This exercises edge cases in incremental hashing.

        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("boundary.log");

        // Create exactly 16KB (1 block) for v1
        let v1_content: Vec<u8> = vec![b'A'; 16 * 1024];
        fs::write(&active_path, &v1_content).await.unwrap();

        let config = LogfileIngestConfig {
            name: "boundary_test".to_string(),
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: "".to_string(),
            pond_path: "logs/boundary_test".to_string(),
        };

        let pond = PondSimulator::new(&config).await.unwrap();
        let verifier = Blake3Verifier::new(pond.store_path());

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/boundary_test", "boundary.log", &v1_content).await.unwrap();
        println!("✓ Exactly 1 block (16384 bytes): blake3 correct");

        // Append exactly 16KB more (now 2 full blocks)
        let v2_append: Vec<u8> = vec![b'B'; 16 * 1024];
        let mut v2_cumulative = v1_content.clone();
        v2_cumulative.extend_from_slice(&v2_append);
        fs::write(&active_path, &v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/boundary_test", "boundary.log", &v2_cumulative).await.unwrap();
        println!("✓ Exactly 2 blocks (32768 bytes): cumulative blake3 correct");

        // Append 1 byte (partial 3rd block)
        let v3_append: Vec<u8> = vec![b'C'; 1];
        let mut v3_cumulative = v2_cumulative.clone();
        v3_cumulative.extend_from_slice(&v3_append);
        fs::write(&active_path, &v3_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/boundary_test", "boundary.log", &v3_cumulative).await.unwrap();
        println!("✓ 2 blocks + 1 byte (32769 bytes): cumulative blake3 correct");

        // Fill to exactly 3 blocks
        let v4_append: Vec<u8> = vec![b'D'; 16 * 1024 - 1];
        let mut v4_cumulative = v3_cumulative.clone();
        v4_cumulative.extend_from_slice(&v4_append);
        fs::write(&active_path, &v4_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier.verify_file_blake3("logs/boundary_test", "boundary.log", &v4_cumulative).await.unwrap();
        println!("✓ Exactly 3 blocks (49152 bytes): cumulative blake3 correct");

        println!("\n✓ All block boundary blake3 validation tests passed!");
    }
}

