// CLI command for listing available dynamic factories
use tlogfs::factory::FactoryRegistry;
use anyhow::Result;
use log::debug;

/// List all available dynamic node factories
pub async fn list_factories_command() -> Result<()> {
    debug!("Listing available dynamic factories");
    
    let factories = FactoryRegistry::list_factories();
    
    if factories.is_empty() {
        println!("No dynamic factories available.");
    } else {
        println!("Available dynamic factories:");
        println!();
        
        for factory in factories {
            println!("  {} - {}", factory.name, factory.description);
            
            // Show what node types this factory supports
	    if factory.execute.is_some() {
		println!("    runs:    {}", factory.entry_type);
	    } else {
		println!("    creates: {}", factory.entry_type);
	    }
            println!();
        }
    }
    
    Ok(())
}
