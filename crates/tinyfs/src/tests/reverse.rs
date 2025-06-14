use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use futures::stream::{self, Stream};

use crate::dir::Directory;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::fs::FS;
use crate::node::NodeRef;
use crate::node::NodeType;
use std::collections::BTreeSet;
use super::super::memory::new_fs;

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
        DirectoryHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self::new(fs, target_path)))))
    }
}

#[async_trait::async_trait]
impl Directory for ReverseDirectory {
    async fn get(&self, name: &str) -> error::Result<Option<NodeRef>> {
        let original_name = reverse_string(name);
        let path = self.target_path.join(&original_name);

        match self.fs.root().await?.get_node_path(&path).await {
            Ok(node) => Ok(Some(node.node)),
            Err(error::Error::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn insert(&mut self, name: String, _id: NodeRef) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    async fn entries(&self) -> error::Result<Pin<Box<dyn Stream<Item = error::Result<(String, NodeRef)>> + Send>>> {
        let root = self.fs.root().await?;
        let dir = root.open_dir_path(&self.target_path).await?;
        let mut dir_stream = dir.read_dir().await?;

        let mut sub = Vec::new();
        use futures::StreamExt;
        while let Some(np) = dir_stream.next().await {
            sub.push(np);
        }

        let mut reversed_items = Vec::new();
        for np in sub {
            reversed_items.push(Ok((reverse_string(&np.basename()), np.node.clone())));
        }

        Ok(Box::pin(stream::iter(reversed_items)))
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
    root.create_dir_path("/1").await.unwrap();
    root.create_file_path("/1/hello.txt", b"Hello World")
        .await.unwrap();
    root.create_file_path("/1/test.bin", b"Binary Data")
        .await.unwrap();

    root.create_node_path("/2", || {
        Ok(NodeType::Directory(ReverseDirectory::new_handle(fs.clone(), "/1")))
    })
    .await.unwrap();

    // Try to access the reversed filenames through the reverse directory
    let result1 = root.read_file_path("/2/txt.olleh").await.unwrap();
    assert_eq!(result1, b"Hello World");

    let result2 = root.read_file_path("/2/nib.tset").await.unwrap();
    assert_eq!(result2, b"Binary Data");

    // Test iterator functionality of ReverseDirectory
    let reverse_dir = root.open_dir_path("/2").await.unwrap();

    let mut dir_stream = reverse_dir.read_dir().await.unwrap();
    let mut actual = Vec::new();
    use futures::StreamExt;
    while let Some(np) = dir_stream.next().await {
        actual.push((np.basename(), np.read_file().await.unwrap()));
    }
    
    let actual: BTreeSet<_> = actual.into_iter().collect();

    let expected = BTreeSet::from([
        ("txt.olleh".to_string(), b"Hello World".to_vec()),
        ("nib.tset".to_string(), b"Binary Data".to_vec()),
    ]);
    assert_eq!(actual, expected);
}
