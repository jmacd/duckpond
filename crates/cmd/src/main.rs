// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::{Parser, Subcommand};
use common::ShipContext;
use panic_alloc::PanicOnLargeAlloc;
use std::path::PathBuf;
use std::str::FromStr;

mod commands;
mod common;
mod panic_alloc;
mod template_utils;

// External modules
use hydrovu as _;

#[global_allocator]
static PEAK_ALLOC: PanicOnLargeAlloc = PanicOnLargeAlloc::new(3000);

/// Control table subcommands
#[derive(Debug, Subcommand)]
enum ControlCommand {
    /// Show recent transactions with summary status
    Recent {
        /// Number of recent transactions to show
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    /// Show detailed lifecycle for a specific transaction
    Detail {
        /// Transaction sequence number
        #[arg(long)]
        txn_seq: i64,
    },
    /// Show incomplete operations (for recovery)
    Incomplete,
    /// Sync with remote: retry failed pushes OR pull new bundles
    Sync {
        /// Optional: Base64-encoded remote config for recovery (use same as pond init --config)
        #[arg(long)]
        config: Option<String>,
    },
    /// Show pond configuration (ID, factory modes, metadata, settings)
    ShowConfig,
    /// Set a configuration value
    SetConfig {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
}

/// Parse a single key-value pair
fn parse_key_value<T, U>(
    s: &str,
) -> Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[derive(Parser)]
#[command(author, version, about = "DuckPond - A very small data lake")]
#[command(name = "pond")]
struct Cli {
    /// Pond path override (defaults to POND env var)
    #[arg(long, global = true)]
    pond: Option<PathBuf>,

    /// Template variables in key=value format (can be repeated)
    #[arg(long = "var", short = 'v', global = true, value_parser = parse_key_value::<String, String>)]
    variables: Vec<(String, String)>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new pond
    Init {
        /// Initialize from remote backup (path to restore config YAML)
        #[arg(long)]
        from_backup: Option<PathBuf>,
        /// Initialize from base64-encoded replication config (from 'pond run /etc/system.d/10-remote replicate')
        #[arg(long, conflicts_with = "from_backup")]
        config: Option<String>,
    },
    /// Recover from crash by checking and restoring transaction metadata
    Recover,
    /// Show pond contents
    Show {
        /// Display mode: brief (summary stats), concise (1 line per tx), or detailed (full dump)
        #[arg(long, short = 'm', default_value = "brief")]
        mode: String,
    },
    /// Query control table for transaction status, post-commit execution, or manual replica sync
    /// Control table operations and pond configuration
    Control {
        #[command(subcommand)]
        command: ControlCommand,
    },
    /// List files and directories (ls -l style)
    List {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
        /// Show all files including hidden ones
        #[arg(short, long)]
        all: bool,
    },
    /// Describe file schemas and types
    Describe {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
    },
    /// Read a file from the pond
    Cat {
        /// File path to read
        path: String,
        /// Output format [default: raw] [possible values: raw, table]
        #[arg(long, default_value = "raw")]
        format: String,
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
    /// Copy files into or out of the pond
    ///
    /// Copy IN (host → pond): pond copy file1.csv file2.csv /dest/path --format=table
    /// Copy OUT (pond → host): pond copy '/pattern/**/*.series' host:///output/dir
    Copy {
        /// Source paths: host files (copy IN) or pond paths/patterns (copy OUT)
        #[arg(required = true)]
        sources: Vec<String>,
        /// Destination: pond path (copy IN) or host://path (copy OUT)
        dest: String,
        /// Format for copying IN [possible values: data, table, series] (ignored for copy OUT)
        #[arg(long, default_value = "data")]
        format: String,
    },
    /// Create a directory in the pond
    Mkdir {
        /// Directory path to create
        path: String,
        /// Create parent directories as needed (like mkdir -p)
        #[arg(short = 'p', long = "parents")]
        parents: bool,
    },
    /// Create node (factory objects like CSV, SQL views, etc.)
    Mknod {
        /// Factory type to create (use 'list-factories' to see all available types)
        factory_type: String,
        /// Path where the node will be created
        path: String,
        /// Path to configuration file for the factory
        #[arg(long)]
        config_path: String,
        /// Overwrite existing dynamic node with new configuration
        #[arg(long)]
        overwrite: bool,
    },
    /// List available dynamic node factories
    ListFactories,
    /// Execute a run configuration (e.g., hydrovu collector)
    Run {
        /// Path to the configuration file to execute
        path: String,
        /// Additional arguments to pass to the factory (e.g., subcommands like 'replicate', 'list-bundles')
        #[arg(num_args = 0..)]
        args: Vec<String>,
    },
    /// Detect temporal overlaps using complete time series data analysis
    DetectOverlaps {
        /// Series file patterns to analyze (e.g., "/sensors/*.series")
        patterns: Vec<String>,
        /// Show detailed overlap analysis with row-level data
        #[arg(long)]
        verbose: bool,
        /// Output format [default: summary] [possible values: summary, full]
        #[arg(long, default_value = "summary")]
        format: String,
    },
    /// Set temporal bounds override for files
    SetTemporalBounds {
        /// File pattern to apply bounds to
        pattern: String,
        /// Minimum timestamp (human-readable, e.g., "2024-01-01 00:00:00", "2024-01-01T00:00:00Z")
        #[arg(long)]
        min_time: Option<String>,
        /// Maximum timestamp (human-readable, e.g., "2024-12-31 23:59:59", "2024-12-31T23:59:59Z")
        #[arg(long)]
        max_time: Option<String>,
    },
    /// Export pond data to external Parquet files with time partitioning
    Export {
        /// File patterns to export (e.g., "/sensors/*.series")
        #[arg(short, long)]
        pattern: Vec<String>,
        /// Output directory for exported files
        #[arg(short, long)]
        dir: PathBuf,
        /// Temporal partitioning levels (comma-separated: year,month,day,hour,minute)
        #[arg(long, default_value = "")]
        temporal: String,
        /// Time range start (human-readable, e.g., "2024-01-01 00:00:00", "2024-01-01T00:00:00Z")
        #[arg(long)]
        start_time: Option<String>,
        /// Time range end (human-readable, e.g., "2024-12-31 23:59:59", "2024-12-31T23:59:59Z")
        #[arg(long)]
        end_time: Option<String>,
    },
}

#[allow(clippy::print_stdout)]
fn print_handler(output: &str) {
    print!("{}", output);
}

#[tokio::main]
async fn main() -> Result<()> {
    // Capture original command line arguments before clap parsing for transaction metadata
    let original_args: Vec<String> = std::env::args().collect();

    // Initialize env_logger from RUST_LOG; default to `info` when RUST_LOG is not set
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::debug!("Main function started");
    let cli = Cli::parse();
    log::debug!("CLI parsed successfully");

    // Create the ship context with global variables
    let ship_context = if cli.variables.is_empty() {
        ShipContext::new(cli.pond.as_ref(), original_args.clone())
    } else {
        let variables_map: std::collections::HashMap<String, String> =
            cli.variables.into_iter().collect();
        ShipContext::with_variables(cli.pond.clone(), original_args.clone(), variables_map)
    };

    let result = match cli.command {
        Commands::Init {
            from_backup,
            config,
        } => {
            // Init command creates new pond (optionally from backup or base64 config)
            commands::init_command(&ship_context, from_backup.as_deref(), config.as_deref()).await
        }
        Commands::Recover => {
            // Recover command works with potentially damaged pond, handle specially
            commands::recover_command(&ship_context).await
        }

        // Read-only commands that use ShipContext for consistency
        Commands::Show { mode } => {
            commands::show_command(&ship_context, &mode, print_handler).await
        }
        Commands::Control { command } => {
            let control_mode = match command {
                ControlCommand::Recent { limit } => {
                    commands::control::ControlMode::Recent { limit }
                }
                ControlCommand::Detail { txn_seq } => {
                    commands::control::ControlMode::Detail { txn_seq }
                }
                ControlCommand::Incomplete => commands::control::ControlMode::Incomplete,
                ControlCommand::Sync { config } => commands::control::ControlMode::Sync {
                    config: config.clone(),
                },
                ControlCommand::ShowConfig => commands::control::ControlMode::ShowConfig,
                ControlCommand::SetConfig { key, value } => {
                    commands::control::ControlMode::SetConfig { key, value }
                }
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::List { pattern, all } => {
            commands::list_command(&ship_context, &pattern, all, print_handler).await
        }
        Commands::Describe { pattern } => {
            commands::describe_command(&ship_context, &pattern, print_handler).await
        }
        Commands::Cat {
            path,
            format,
            time_start,
            time_end,
            query,
        } => {
            commands::cat_command(
                &ship_context,
                &path,
                &format,
                None,
                time_start,
                time_end,
                query.as_deref(),
            )
            .await
        }

        // Write commands that use scoped transactions
        Commands::Copy {
            sources,
            dest,
            format,
        } => commands::copy_command(&ship_context, &sources, &dest, &format).await,
        Commands::Mkdir { path, parents } => {
            commands::mkdir_command(&ship_context, &path, parents).await
        }
        Commands::Mknod {
            factory_type,
            path,
            config_path,
            overwrite,
        } => {
            commands::mknod_command(&ship_context, &factory_type, &path, &config_path, overwrite)
                .await
        }
        Commands::ListFactories => commands::list_factories_command().await,
        Commands::Run { path, args } => commands::run_command(&ship_context, &path, args).await,
        Commands::DetectOverlaps {
            patterns,
            verbose,
            format,
        } => commands::detect_overlaps_command(&ship_context, &patterns, verbose, &format).await,
        Commands::SetTemporalBounds {
            pattern,
            min_time,
            max_time,
        } => {
            commands::set_temporal_bounds_command(&ship_context, pattern, min_time, max_time).await
        }
        Commands::Export {
            pattern,
            dir,
            temporal,
            start_time,
            end_time,
        } => {
            commands::export_command(
                &ship_context,
                &pattern,
                &dir.to_string_lossy(),
                &temporal,
                start_time,
                end_time,
            )
            .await
        }
    };

    // Log peak memory usage
    let peak_mem = PEAK_ALLOC.peak_usage_as_mb();
    log::info!("Peak memory usage: {:.2} MB", peak_mem);

    // Print large allocations report
    PEAK_ALLOC.print_large_allocs();

    result
}
