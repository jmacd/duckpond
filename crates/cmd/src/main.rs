use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod common;
mod commands;

use common::{FilesystemChoice, ShipContext};

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
    // /// Recover from crash by checking and restoring transaction metadata
    // Recover,
    // /// Show pond contents
    // Show {
    //     /// Which filesystem to access
    //     #[arg(long, short = 'f', default_value = "data")]
    //     filesystem: FilesystemChoice,
    // },
    // /// List files and directories (ls -l style)
    // List {
    //     /// Pattern to match (supports wildcards, defaults to "**/*")
    //     #[arg(default_value = "**/*")]
    //     pattern: String,
    //     /// Show all files including hidden ones
    //     #[arg(short, long)]
    //     all: bool,
    //     /// Which filesystem to access
    //     #[arg(long, short = 'f', default_value = "data")]
    //     filesystem: FilesystemChoice,
    // },
    // /// Describe file schemas and types
    // Describe {
    //     /// Pattern to match (supports wildcards, defaults to "**/*")
    //     #[arg(default_value = "**/*")]
    //     pattern: String,
    //     /// Which filesystem to access
    //     #[arg(long, short = 'f', default_value = "data")]
    //     filesystem: FilesystemChoice,
    // },
    /// Read a file from the pond
    Cat {
        /// File path to read
        path: String,
        /// Which filesystem to access
        #[arg(long, short = 'f', default_value = "data")]
        filesystem: FilesystemChoice,
        /// Display mode [default: raw] [possible values: raw, table]
        #[arg(long, default_value = "raw")]
        display: String,
        /// Time range start (Unix timestamp in milliseconds, optional)
        #[arg(long)]
        time_start: Option<i64>,
        /// Time range end (Unix timestamp in milliseconds, optional)
        #[arg(long)]
        time_end: Option<i64>,
        /// SQL query to execute on the file:series data
        #[arg(long)]
        query: Option<String>,
    },
    // /// Copy files into the pond (supports multiple files like UNIX cp)
    // Copy {
    //     /// Source file paths (one or more files to copy)
    //     #[arg(required = true)]
    //     sources: Vec<String>,
    //     /// Destination path in pond (file name or directory)
    //     dest: String,
    //     /// EXPERIMENTAL: Format handling [default: auto] [possible values: auto, data, parquet, series]
    //     #[arg(long, default_value = "auto")]
    //     format: String,
    // },
    // /// Create a directory in the pond
    // Mkdir {
    //     /// Directory path to create
    //     path: String,
    // },
    // /// Create a dynamic node in the pond
    // Mknod {
    //     /// Factory type (hostmount, etc.)
    //     factory_type: String,
    //     /// Path where the dynamic node will be created
    //     path: String,
    //     /// Configuration file path
    //     config_path: String,
    // },
    // /// List available dynamic node factories
    // ListFactories,
}

#[tokio::main]
async fn main() -> Result<()> {
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
        // Commands::Recover => {
        //     // Recover command works with potentially damaged pond, handle specially
        //     commands::recover_command(&ship_context).await
        // }
        
        // // Read-only commands that use ShipContext for consistency
        // Commands::Show { filesystem } => {
        //     commands::show_command(&ship_context, filesystem, |output| {
        //         print!("{}", output);
        //     }).await
        // }
        // Commands::List { pattern, all, filesystem } => {
        //     commands::list_command(&ship_context, &pattern, all, filesystem, |output| {
        //         print!("{}", output);
        //     }).await
        // }
        // Commands::Describe { pattern, filesystem } => {
        //     commands::describe_command(&ship_context, &pattern, filesystem).await
        // }
        Commands::Cat { path, filesystem, display, time_start, time_end, query } => {
            commands::cat_command_with_sql(&ship_context, &path, filesystem, &display, None, time_start, time_end, query.as_deref()).await
        }
        
        // // Write commands that use scoped transactions
        // Commands::Copy { sources, dest, format } => {
        //     let ship = ship_context.create_ship().await?;
        //     commands::copy_command(ship, &sources, &dest, &format).await
        // }
        // Commands::Mkdir { path } => {
        //     let ship = ship_context.create_ship().await?;
        //     commands::mkdir_command(ship, &path).await
        // }
        // Commands::Mknod { factory_type, path, config_path } => {
        //     let ship = ship_context.create_ship().await?;
        //     commands::mknod_command(ship, &factory_type, &path, &config_path).await
        // }
        // Commands::ListFactories => {
        //     commands::list_factories_command().await
        // }
    }
}
