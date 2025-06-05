use std::env;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use arrow_array::StringArray;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "pond")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new pond with an empty oplog
    Init,
    /// Show the operation log contents
    Show,
}

fn find_pond_dir() -> Result<PathBuf> {
    match env::var("POND") {
        Ok(val) => Ok(PathBuf::from(val)),
        Err(_) => Err(anyhow!("POND environment variable not set")),
    }
}

fn get_store_path() -> Result<PathBuf> {
    let pond_dir = find_pond_dir()?;
    Ok(pond_dir.join("store"))
}

async fn init_command() -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Initializing pond at: {}", store_path.display());

    // Check if store already exists
    if store_path.exists() {
        return Err(anyhow!("Pond already exists at {}", store_path.display()));
    }

    // Create the store directory if it doesn't exist
    std::fs::create_dir_all(&store_path)?;

    // Initialize the oplog with an empty root directory entry using OplogEntry schema
    oplog::tinylogfs::create_oplog_table(&store_path_str).await?;

    println!("Successfully initialized pond with empty root directory");
    Ok(())
}

async fn show_command() -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Opening pond at: {}", store_path.display());

    // Check if store exists
    if !store_path.exists() {
        return Err(anyhow!(
            "Pond does not exist at {}. Run 'pond init' first.",
            store_path.display()
        ));
    }

    // Use the new OplogEntry table provider for filesystem operations
    use datafusion::prelude::*;
    let ctx = SessionContext::new();

    // Register the OplogEntry table that can read filesystem operations
    let oplog_table = oplog::tinylogfs::OplogEntryTable::new(store_path_str.to_string());
    ctx.register_table("filesystem_ops", std::sync::Arc::new(oplog_table))?;

    // Query all filesystem operations ordered by part_id and node_id
    let df = ctx
        .sql(
            "SELECT part_id, node_id, file_type, metadata 
         FROM filesystem_ops 
         ORDER BY part_id, node_id",
        )
        .await?;
    let results = df.collect().await?;

    println!("Filesystem operations:");
    if results.is_empty() {
        println!("(empty)");
    } else {
        println!(
            "Part ID                          | Node ID                          | Type      | Metadata"
        );
        println!(
            "-------------------------------- | -------------------------------- | --------- | --------"
        );
        for batch in &results {
            for row_idx in 0..batch.num_rows() {
                let part_id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_idx);
                let node_id = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_idx);
                let file_type = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_idx);
                let metadata = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_idx);

                println!(
                    "{} | {} | {:9} | {}",
                    &part_id[..32], // Truncate for display
                    &node_id[..32], // Truncate for display
                    file_type,
                    metadata
                );
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Init => init_command().await,
        Commands::Show => show_command().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_init_and_show() -> Result<()> {
        let tmp = tempdir()?;
        let pond_path = tmp.path().join("test_pond");

        // Set the POND environment variable
        unsafe {
            env::set_var("POND", pond_path.to_string_lossy().to_string());
        }

        // Test init command
        init_command().await?;

        // Verify the store was created
        let store_path = get_store_path()?;
        assert!(store_path.exists());

        // Test show command
        show_command().await?;

        // Test that init fails if run again
        let result = init_command().await;
        assert!(result.is_err());

        Ok(())
    }
}
