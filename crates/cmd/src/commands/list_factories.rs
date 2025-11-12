// CLI command for listing available dynamic factories
use anyhow::Result;
use log::debug;
use tlogfs::factory::FactoryRegistry;

/// List all available dynamic node factories
#[allow(clippy::print_stdout)]
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
            let mut capabilities = Vec::new();
            if factory.create_directory.is_some() {
                capabilities.push("directory");
            }
            if factory.create_file.is_some() {
                capabilities.push("file");
            }
            if factory.execute.is_some() {
                capabilities.push("executable");
            }

            if !capabilities.is_empty() {
                println!("    supports: {}", capabilities.join(", "));
            }
            println!();
        }
    }

    Ok(())
}
