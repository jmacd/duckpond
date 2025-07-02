use anyhow::Result;
use clap::{Parser, Subcommand};

mod common;
mod commands;

#[cfg(test)]
mod tests;

#[derive(Parser)]
#[command(author, version, about = "DuckPond - A very small data lake")]
#[command(name = "pond")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new pond
    Init,
    /// Show pond contents
    Show,
    /// List files and directories (ls -l style)
    List {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
        /// Show all files including hidden ones
        #[arg(short, long)]
        all: bool,
    },
    /// Read a file from the pond
    Cat {
        /// File path to read
        path: String,
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
    
    diagnostics::log_debug!("Main function started");
    let cli = Cli::parse();
    diagnostics::log_debug!("CLI parsed successfully");

    match cli.command {
        Commands::Init => commands::init_command().await,
        Commands::Show => commands::show_command().await,
        Commands::List { pattern, all } => commands::list_command(&pattern, all).await,
        Commands::Cat { path } => commands::cat_command(&path).await,
        Commands::Copy { sources, dest } => {
            diagnostics::log_debug!("CLI copy command triggered");
            commands::copy_command(&sources, &dest).await
        },
        Commands::Mkdir { path } => commands::mkdir_command(&path).await,
    }
}
