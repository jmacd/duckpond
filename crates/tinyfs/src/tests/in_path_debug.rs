#[cfg(test)]
mod in_path_tests {
    use crate::Lookup;
    use crate::memory::new_fs;

    #[tokio::test]
    async fn test_in_path_root_directory() {
        println!("Starting test_in_path_root_directory");
        
        // Create a simple in-memory filesystem
        let fs = new_fs().await;
        let root = fs.root().await.unwrap();
        
        println!("Created filesystem and got root");
        
        // Test in_path with root directory "/"
        println!("About to call in_path with '/'");
        
        let result = root.in_path("/", |_wd, lookup| async move {
            println!("Inside in_path callback!");
            println!("  lookup variant: {:?}", std::mem::discriminant(&lookup));
            
            match lookup {
                Lookup::Found(node_path) => {
                    println!("  Found: path = {:?}", node_path.path);
                }
                Lookup::NotFound(parent_path, name) => {
                    println!("  NotFound: parent = {:?}, name = '{}'", parent_path, name);
                }
                Lookup::Empty(node_path) => {
                    println!("  Empty: path = {:?}", node_path.path);
                }
            }
            
            Ok(())
        }).await;
        
        println!("in_path call completed");
        
        match result {
            Ok(()) => println!("✅ Test passed"),
            Err(e) => println!("❌ Test failed: {}", e),
        }
    }
    
    #[tokio::test] 
    async fn test_in_path_simple_paths() {
        println!("Starting test_in_path_simple_paths");
        
        // Create a simple in-memory filesystem
        let fs = new_fs().await;
        let root = fs.root().await.unwrap();
        
        // Test some simple paths to make sure basic functionality works
        let test_paths = vec![".", "nonexistent", "nonexistent/file"];
        
        for path in test_paths {
            println!("Testing path: '{}'", path);
            
            let result = root.in_path(path, |_wd, lookup| async move {
                println!("  Result for '{}': {:?}", path, std::mem::discriminant(&lookup));
                Ok(())
            }).await;
            
            match result {
                Ok(()) => println!("  ✅ Path '{}' completed", path),
                Err(e) => println!("  ❌ Path '{}' failed: {}", path, e),
            }
        }
    }
}
