//! Tests for ProviderContext transaction management

#[cfg(test)]
mod tests {
    use crate::ProviderContext;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tinyfs::memory::persistence::MemoryPersistence;

    #[tokio::test]
    async fn test_provider_context_transaction_guard() {
        // Create a provider context with memory persistence
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(
            session,
            HashMap::new(),
            Arc::new(persistence),
        );

        // Begin a transaction using the guard pattern
        let guard = context.begin_transaction().expect("Should create transaction");

        // Access filesystem through the guard
        let root = guard.root().await.expect("Should get root");
        
        // Verify root exists
        assert_eq!(root.node_path().path, PathBuf::from("/"));

        // Guard automatically cleans up on drop
        drop(guard);

        // Can create another transaction after the first one is dropped
        let _guard2 = context.begin_transaction().expect("Should create second transaction");
    }

    #[tokio::test]
    async fn test_provider_context_filesystem() {
        // Create a provider context
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(
            session,
            HashMap::new(),
            Arc::new(persistence),
        );

        // Get filesystem directly (no guard - for cases where guard isn't needed)
        let fs = context.filesystem();
        let root = fs.root().await.expect("Should get root");
        
        assert_eq!(root.node_path().path, PathBuf::from("/"));
    }
}
