use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod common;
mod commands;

use common::{FilesystemChoice, ShipContext};

#[cfg(test)]
mod tests;

#[derive(Parser)]
#[command(author, version, about = "DuckPond - A very small data lake")]
#[command(name = "pond")]
struct Cli {
    /// Pond path override (defaults to POND env var)
    #[arg(long, global = true)]
    pond: Option<PathBuf>,
    
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new pond
    Init,
    /// Recover from crash by checking and restoring transaction metadata
    Recover,
    /// Show pond contents
    Show {
        /// Which filesystem to access
        #[arg(long, short = 'f', default_value = "data")]
        filesystem: FilesystemChoice,
    },
    /// List files and directories (ls -l style)
    List {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
        /// Show all files including hidden ones
        #[arg(short, long)]
        all: bool,
        /// Which filesystem to access
        #[arg(long, short = 'f', default_value = "data")]
        filesystem: FilesystemChoice,
    },
    /// Read a file from the pond
    Cat {
        /// File path to read
        path: String,
        /// Which filesystem to access
        #[arg(long, short = 'f', default_value = "data")]
        filesystem: FilesystemChoice,
    },
    /// Copy files into the pond (supports multiple files like UNIX cp)
    Copy {
        /// Source file paths (one or more files to copy)
        #[arg(required = true)]
        sources: Vec<String>,
        /// Destination path in pond (file name or directory)
        dest: String,
    },
    /// Create a directory in the pond
    Mkdir {
        /// Directory path to create
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize diagnostics first
    diagnostics::init_diagnostics();
    
    // Capture original command line arguments before clap parsing for transaction metadata
    let original_args: Vec<String> = std::env::args().collect();
    
    diagnostics::log_debug!("Main function started");
    let cli = Cli::parse();
    diagnostics::log_debug!("CLI parsed successfully");

    // Create the ship context that contains everything needed for ship operations
    let ship_context = ShipContext::new(cli.pond, original_args);

    match cli.command {
        Commands::Init => {
            // Init command creates new pond
            commands::init_command(&ship_context).await
        }
        Commands::Recover => {
            // Recover command works with potentially damaged pond, handle specially
            commands::recover_command(&ship_context).await
        }
        
        // Read-only commands that don't need transactions
        Commands::Show { filesystem } => {
            commands::show_command(filesystem).await
        }
        Commands::List { pattern, all, filesystem } => {
            commands::list_command(&pattern, all, filesystem).await
        }
        Commands::Cat { path, filesystem } => {
            commands::cat_command(&path, filesystem).await
        }
        
        // Write commands that need transactions
        Commands::Copy { sources, dest } => {
            let ship = ship_context.create_ship_with_transaction().await?;
            commands::copy_command(ship, &sources, &dest).await
        }
        Commands::Mkdir { path } => {
            let ship = ship_context.create_ship_with_transaction().await?;
            commands::mkdir_command(ship, &path).await
        }
    }
}
