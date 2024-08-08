mod hydrovu;
pub mod pond;

use anyhow::{Result, anyhow};

use clap::{Parser, Subcommand};

use pond::backup;

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

	#[arg(short, value_parser = parse_key_val, number_of_values = 1)]
	vars: Vec<(String, String)>,
    },

    /// Get and display resource(s)
    Get {
	name: Option<String>,
    },

    Export {
	name: String,
    },

    Check,

    Backup {
	#[command(subcommand)]
	command: backup::Commands,
    },
}

/// Parse a single key-value pair
fn parse_key_val(s: &str) -> Result<(String, String)>
{
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
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

	Commands::Apply{file_name, vars} => pond::apply(file_name, vars),

	Commands::Get{name} => pond::get(name.clone()),

	Commands::Export{name} => pond::export_data(name.clone()),

	Commands::Check => pond::check(),

	Commands::Backup{command} => Ok(backup::sub_main(command)?),
    }
}
