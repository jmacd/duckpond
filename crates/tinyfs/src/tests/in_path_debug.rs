#[cfg(test)]
mod in_path_tests {
    use log::debug;
    use crate::Lookup;
    use crate::memory::new_fs;

    #[tokio::test]
    async fn test_in_path_root_directory() {
        debug!("Starting test_in_path_root_directory");

        // Create a simple in-memory filesystem
        let fs = new_fs().await;
        let root = fs.root().await.unwrap();

        debug!("Created filesystem and got root");

        // Test in_path with root directory "/"
        debug!("About to call in_path with '/'");

        let result = root
            .in_path("/", |_wd, lookup| async move {
                debug!("Inside in_path callback!");
                debug!("  lookup variant: {:?}", std::mem::discriminant(&lookup));

                match lookup {
                    Lookup::Found(node_path) => {
                        debug!("  Found: path = {:?}", node_path.path);
                    }
                    Lookup::NotFound(parent_path, name) => {
                        debug!("  NotFound: parent = {:?}, name = '{}'", parent_path, name);
                    }
                    Lookup::Empty(node_path) => {
                        debug!("  Empty: path = {:?}", node_path.path);
                    }
                }

                Ok(())
            })
            .await;

        debug!("in_path call completed");

        match result {
            Ok(()) => debug!("✅ Test passed"),
            Err(e) => debug!("❌ Test failed: {}", e),
        }
    }

    #[tokio::test]
    async fn test_in_path_simple_paths() {
        debug!("Starting test_in_path_simple_paths");

        // Create a simple in-memory filesystem
        let fs = new_fs().await;
        let root = fs.root().await.unwrap();

        // Test some simple paths to make sure basic functionality works
        let test_paths = vec![".", "nonexistent", "nonexistent/file"];

        for path in test_paths {
            debug!("Testing path: '{}'", path);

            let result = root
                .in_path(path, |_wd, lookup| async move {
                    debug!(
                        "  Result for '{}': {:?}",
                        path,
                        std::mem::discriminant(&lookup)
                    );
                    Ok(())
                })
                .await;

            match result {
                Ok(()) => debug!("  ✅ Path '{}' completed", path),
                Err(e) => debug!("  ❌ Path '{}' failed: {}", path, e),
            }
        }
    }
}
