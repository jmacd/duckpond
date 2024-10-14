#![warn(unused_extern_crates)]
#![feature(os_str_display)]
#![feature(path_add_extension)]

mod hydrovu;
pub mod pond;

use anyhow::{anyhow, Result};

use clap::{Parser, Subcommand};

use pond::backup;

use std::path::PathBuf;

/// Duckpond is a small data lake.  Register resources and run them to
/// collect, archive, and process your files, tables, and timeseries.
#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run will execute the registered resource handlers to fetch new data.
    Run,

    /// Init will initialize a new Pond.
    Init,

    /// Apply applies a resource definition file, entering a new unique resource.
    Apply {
        /// file_name is the name of the input file (YAML).
        #[clap(short)]
        file_name: PathBuf,

	/// args is the set of key=value variables to replace in the
	/// input file.  For example, `{{ key }}` will be replaced
	/// with the argument `value`.
        #[arg(short, value_parser = parse_key_val, number_of_values = 1)]
        vars: Vec<(String, String)>,
    },

    /// Get and display resource(s)
    /// TODO this command needs work!
    Get {
        name: Option<String>,
    },

    /// Check applies consistency checks, computes and prints a tree
    /// hash over each resource in the Pond.
    Check,

    /// Backup has a sub-menu of commands for interacting with
    /// backups.
    Backup {
        #[command(subcommand)]
        command: backup::Commands,
    },

    /// Cat prints the contents of the file from the Pond.
    #[clap(visible_alias = "print")]
    Cat {
        path: String,
    },

    /// List prints matching file names in the Pond.
    #[clap(visible_alias = "ls")]
    List {
        pattern: String,
    },

    /// Export copies matching files from the Pond to the host file
    /// system.
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
