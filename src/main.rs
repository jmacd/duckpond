#![feature(path_add_extension)]

mod hydrovu;
pub mod pond;

use anyhow::{anyhow, Result};

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

    Check,

    Backup {
        #[command(subcommand)]
        command: backup::Commands,
    },

    Cat {
        path: String,
    },

    List {
        pattern: String,
    },

    Export {
	#[arg(short, long)]
        pattern: String,

	#[arg(short, long)]
	dir: PathBuf
    },
}

/// Parse a single key-value pair
fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

fn main() {
    match main_result() {
        Ok(_) => {}
        Err(err) => eprintln!("{:?}", err),
    }
}

fn main_result() -> Result<()> {
    env_logger::init();

    match Cli::parse().command {
        Commands::Run => pond::run(),

        Commands::Init => pond::init(),

        Commands::Apply { file_name, vars } => pond::apply(file_name, &vars),

        Commands::Get { name } => pond::get(name.clone()),

        Commands::Check => pond::check(),

        Commands::Backup { command } => backup::sub_main(&command),

        Commands::Cat { path } => pond::cat(path.clone()),


        Commands::List { pattern } => pond::list(&pattern),

	Commands::Export { pattern, dir } => pond::export(pattern, &dir),
    }
}
