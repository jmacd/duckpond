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
}
