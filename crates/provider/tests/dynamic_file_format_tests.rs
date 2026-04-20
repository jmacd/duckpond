// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Test that dynamic files (FileDynamic) served by dynamic directories
//! can be read through external format providers (e.g., csv://).
//!
//! This exercises the fix in ensure_url_cached() that detects dynamic files
//! and reads them via async_reader() instead of querying the persistence
//! layer for version records (which don't exist for ephemeral nodes).

#[cfg(test)]
mod dynamic_file_format_tests {
    use datafusion::prelude::*;
    use provider::Provider;
    use std::sync::Arc;

    /// Build a MemoryPersistence-backed FS with a dynamic directory at /data
    /// containing a FileDynamic child "test.csv" with the given CSV content.
    ///
    /// This simulates what gitpond does: a DirectoryDynamic node whose children
    /// are FileDynamic nodes created on-the-fly (not written through the oplog).
    async fn setup_fs_with_dynamic_csv(
        csv_content: &str,
    ) -> (
        Arc<tinyfs::FS>,
        Arc<dyn tinyfs::PersistenceLayer>,
        tempfile::TempDir,
    ) {
        use async_trait::async_trait;
        use futures::stream;
        use tinyfs::{
            DirHandle, Directory, DirectoryEntry, EntryType, Metadata, Node, NodeMetadata, NodeType,
        };

        // -- Dynamic directory that serves one CSV file child --

        struct TestDynamicDir {
            parent_file_id: tinyfs::FileID,
            csv_content: String,
        }

        impl TestDynamicDir {
            fn child_file_id(&self) -> tinyfs::FileID {
                let parent_part_id = tinyfs::PartID::from_node_id(self.parent_file_id.node_id());
                tinyfs::FileID::from_content(
                    parent_part_id,
                    EntryType::FileDynamic,
                    b"test.csv",
                    self.parent_file_id.pond_id(),
                )
            }

            fn child_node(&self) -> Node {
                let child_id = self.child_file_id();
                let file = provider::ConfigFile::new(self.csv_content.clone().into_bytes());
                Node::new(child_id, NodeType::File(file.create_handle()))
            }
        }

        #[async_trait]
        impl Directory for TestDynamicDir {
            async fn get(&self, name: &str) -> tinyfs::Result<Option<Node>> {
                if name == "test.csv" {
                    Ok(Some(self.child_node()))
                } else {
                    Ok(None)
                }
            }

            async fn insert(&mut self, _name: String, _node: Node) -> tinyfs::Result<()> {
                Err(tinyfs::Error::Other("read-only".to_string()))
            }

            async fn remove(&mut self, _name: &str) -> tinyfs::Result<Option<Node>> {
                Err(tinyfs::Error::Other("read-only".to_string()))
            }

            async fn entries(
                &self,
            ) -> tinyfs::Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = tinyfs::Result<DirectoryEntry>> + Send>,
                >,
            > {
                let child_id = self.child_file_id();
                let entry = DirectoryEntry::new(
                    "test.csv".to_string(),
                    child_id.node_id(),
                    EntryType::FileDynamic,
                    0,
                );
                Ok(Box::pin(stream::iter(vec![Ok(entry)])))
            }
        }

        #[async_trait]
        impl Metadata for TestDynamicDir {
            async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
                Ok(NodeMetadata {
                    version: 1,
                    size: None,
                    blake3: None,
                    bao_outboard: None,
                    entry_type: EntryType::DirectoryDynamic,
                    timestamp: 0,
                })
            }
        }

        // -- Set up FS --

        let persistence: Arc<dyn tinyfs::PersistenceLayer> =
            Arc::new(tinyfs::MemoryPersistence::default());
        let fs = Arc::new(tinyfs::FS::from_arc(persistence.clone()));
        let root = fs.root().await.unwrap();

        // Create /data as a physical directory first, then replace with dynamic
        // Actually, we can insert a dynamic directory node directly.
        let dir_id = tinyfs::FileID::from_content(
            tinyfs::PartID::root(),
            EntryType::DirectoryDynamic,
            b"data-dynamic-dir",
            tinyfs::local_pond_uuid(),
        );

        let dynamic_dir = TestDynamicDir {
            parent_file_id: dir_id,
            csv_content: csv_content.to_string(),
        };
        let dir_handle = DirHandle::new(Arc::new(tokio::sync::Mutex::new(
            Box::new(dynamic_dir) as Box<dyn Directory>
        )));
        let dir_node = Node::new(dir_id, NodeType::Directory(dir_handle));

        let _ = root.insert_node("data", dir_node).await.unwrap();

        let cache_dir = tempfile::tempdir().unwrap();

        (fs, persistence, cache_dir)
    }

    #[tokio::test]
    async fn test_dynamic_file_readable_via_csv_format_provider() {
        let csv_content = "name,value\nalpha,10\nbeta,20\ngamma,30\n";
        let (fs, persistence, cache_dir) = setup_fs_with_dynamic_csv(csv_content).await;

        // Create provider with context (needed for cache path)
        let session = Arc::new(SessionContext::new());
        let provider_context = tinyfs::ProviderContext::new_for_testing(persistence)
            .with_cache_dir(cache_dir.path().to_path_buf());

        let provider = Provider::with_context(fs, Arc::new(provider_context));

        // Read the dynamic file via csv:// scheme
        let table = provider
            .create_table_provider("csv:///data/test.csv", &session)
            .await
            .expect("Should read dynamic file via csv:// format provider");

        // Register and query
        let _ = session.register_table("test_data", table).unwrap();

        let df = session
            .sql("SELECT name, value FROM test_data ORDER BY value")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);

        // Verify values
        let name_col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "alpha");
        assert_eq!(name_col.value(1), "beta");
        assert_eq!(name_col.value(2), "gamma");
    }

    #[tokio::test]
    async fn test_dynamic_file_cached_on_second_read() {
        let csv_content = "x,y\n1,2\n3,4\n";
        let (fs, persistence, cache_dir) = setup_fs_with_dynamic_csv(csv_content).await;

        let session = Arc::new(SessionContext::new());
        let provider_context = tinyfs::ProviderContext::new_for_testing(persistence)
            .with_cache_dir(cache_dir.path().to_path_buf());

        let provider = Provider::with_context(fs, Arc::new(provider_context));

        // First read — should cache
        let table1 = provider
            .create_table_provider("csv:///data/test.csv", &session)
            .await
            .expect("First read should succeed");

        // Verify data
        let _ = session.register_table("t1", table1).unwrap();
        let df = session.sql("SELECT COUNT(*) as cnt FROM t1").await.unwrap();
        let results = df.collect().await.unwrap();
        let cnt = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2);

        // Second read — should hit cache (no re-read of dynamic file)
        let table2 = provider
            .create_table_provider("csv:///data/test.csv", &session)
            .await
            .expect("Second read should succeed (from cache)");

        let session2 = SessionContext::new();
        let _ = session2.register_table("t2", table2).unwrap();
        let df2 = session2
            .sql("SELECT COUNT(*) as cnt FROM t2")
            .await
            .unwrap();
        let results2 = df2.collect().await.unwrap();
        let cnt2 = results2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt2, 2);
    }
}
