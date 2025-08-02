// CLI command for listing available dynamic factories
use tlogfs::factory::FactoryRegistry;
use anyhow::Result;
use diagnostics::*;

/// List all available dynamic node factories
pub async fn list_factories_command() -> Result<()> {
    log_debug!("Listing available dynamic factories");
    
    let factories = FactoryRegistry::list_factories();
    
    if factories.is_empty() {
        println!("No dynamic factories available.");
    } else {
        println!("Available dynamic factories:");
        println!();
        
        for factory in factories {
            println!("  {} - {}", factory.name, factory.description);
            
            // Show what node types this factory supports
            let mut capabilities = vec![];
            if factory.create_directory_with_context.is_some() {
                capabilities.push("directories");
            }
            if factory.create_file_with_context.is_some() {
                capabilities.push("files");
            }
            
            if !capabilities.is_empty() {
                println!("    Supports: {}", capabilities.join(", "));
            }
            
            println!();
        }
    }
    
    Ok(())
}
