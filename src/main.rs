mod hydrovu;
pub mod pond;

use anyhow::Result;

use clap::{Parser, Subcommand};

use std::path::PathBuf;

/// Duckpond is a small data lake.
#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run!
    Run,

    /// Initialize a new pond
    Init,

    /// Apply a resource definition
    Apply {
	/// file_name is the input
	#[clap(short)]
	file_name: PathBuf,
    },

    /// Get and display resource(s)
    Get {
	name: Option<String>,
    },

    Export {
	name: String,
    },
}

fn main() {
    match main_result() {
	Ok(_) => {},
	Err(err) =>  eprintln!("{:?}", err),
    }
}

fn main_result() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Run => pond::run(),

	Commands::Init => pond::init(),

	Commands::Apply{file_name} => pond::apply(file_name),

	Commands::Get{name} => pond::get(name.clone()),

	Commands::Export{name} => pond::export_data(name.clone()),
    }
}
