use futures::stream::{self, Stream};
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use super::super::memory::new_fs;
use crate::async_helpers::convenience;
use crate::dir::Directory;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::fs::FS;
use crate::node::Node;
use crate::node::NodeType;
use std::collections::BTreeSet;

pub struct ReverseDirectory {
    fs: FS,
    target_path: PathBuf,
}

impl ReverseDirectory {
    pub fn new<P: AsRef<Path>>(fs: FS, target_path: P) -> Self {
        Self {
            fs,
            target_path: target_path.as_ref().into(),
        }
    }

    pub fn new_handle<P: AsRef<Path>>(fs: FS, target_path: P) -> DirectoryHandle {
        DirectoryHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self::new(
            fs,
            target_path,
        )))))
    }
}

#[async_trait::async_trait]
impl Directory for ReverseDirectory {
    async fn get(&self, name: &str) -> error::Result<Option<Node>> {
        let original_name = reverse_string(name);
        let path = self.target_path.join(&original_name);

        match self.fs.root().await?.get_node_path(&path).await {
            Ok(node) => Ok(Some(node.node)),
            Err(error::Error::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn insert(&mut self, name: String, _id: Node) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    async fn entries(
        &self,
    ) -> error::Result<Pin<Box<dyn Stream<Item = error::Result<crate::DirectoryEntry>> + Send>>> {
        let root = self.fs.root().await?;
        let dir = root.open_dir_path(&self.target_path).await?;
        let mut entry_stream = dir.entries().await?;

        let mut sub = Vec::new();
        use futures::StreamExt;
        while let Some(result) = entry_stream.next().await {
            let dir_entry = result?;
            let np = dir.get(&dir_entry.name).await?.ok_or_else(|| error::Error::NotFound(dir_entry.name.into()))?;
            sub.push(np);
        }

        let mut reversed_items = Vec::new();
        for np in sub {
            let node_type = np.node.node_type.lock().await;
            let entry_type = match &*node_type {
                NodeType::Directory(_) => crate::EntryType::DirectoryPhysical,
                NodeType::File(_) => crate::EntryType::FileDataPhysical,
                NodeType::Symlink(_) => crate::EntryType::Symlink,
            };
            let dir_entry = crate::DirectoryEntry::new(
                reverse_string(&np.basename()),
                np.node.id.node_id(),
                entry_type,
                0, // @@@
            );
            reversed_items.push(Ok(dir_entry));
        }

        Ok(Box::pin(stream::iter(reversed_items)))
    }
}

#[async_trait::async_trait]
impl crate::Metadata for ReverseDirectory {
    async fn metadata(&self) -> error::Result<crate::NodeMetadata> {
        Ok(crate::NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: crate::EntryType::DirectoryDynamic,
            timestamp: 0, // TODO
        })
    }
}

fn reverse_string(s: &str) -> String {
    s.chars().rev().collect()
}

#[tokio::test]
async fn test_reverse_directory() {
    // Create a filesystem with some test files
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();
    _ = root.create_dir_path("/1").await.unwrap();
    _ = convenience::create_file_path(&root, "/1/hello.txt", b"Hello World")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/1/test.bin", b"Binary Data")
        .await
        .unwrap();

    _ = root
        .create_node_path("/2", || {
            Ok(NodeType::Directory(ReverseDirectory::new_handle(
                fs.clone(),
                "/1",
            )))
        })
        .await
        .unwrap();

    // Try to access the reversed filenames through the reverse directory
    let result1 = root.read_file_path_to_vec("/2/txt.olleh").await.unwrap();
    assert_eq!(result1, b"Hello World");

    let result2 = root.read_file_path_to_vec("/2/nib.tset").await.unwrap();
    assert_eq!(result2, b"Binary Data");

    // Test iterator functionality of ReverseDirectory
    let reverse_dir = root.open_dir_path("/2").await.unwrap();

    let mut entry_stream = reverse_dir.entries().await.unwrap();
    let mut actual = Vec::new();
    use futures::StreamExt;
    while let Some(result) = entry_stream.next().await {
        let dir_entry = result.unwrap();
        let np = reverse_dir.get(&dir_entry.name).await.unwrap().unwrap();
        let file_node = np.as_file().await.unwrap();
        let reader = file_node.async_reader().await.unwrap();
        let content = crate::async_helpers::buffer_helpers::read_all_to_vec(reader)
            .await
            .unwrap();
        actual.push((np.basename(), content));
    }

    let actual: BTreeSet<_> = actual.into_iter().collect();

    let expected = BTreeSet::from([
        ("txt.olleh".to_string(), b"Hello World".to_vec()),
        ("nib.tset".to_string(), b"Binary Data".to_vec()),
    ]);
    assert_eq!(actual, expected);
}
