pub mod hydrovu;
pub mod pond;

use futures::executor;

use anyhow::{Context, Result};
use chrono::offset::Utc;
use chrono::offset::FixedOffset;
use chrono::DateTime;

use clap::{Parser, Subcommand};

use std::path::PathBuf;
use std::path::Path;

use datafusion::{
    prelude::{ParquetReadOptions, SessionContext},
};

/// Duckpond is a small data lake.
#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Synchronize metadata
    Sync,

    /// Load metadata
    Load,

    /// Read data
    Read {
        /// An example option
        #[clap(long)]
        until_time: String,
    },

    Init,

    Apply {
	/// file_name is the input
	#[clap(short)]
	file_name: PathBuf,
    },

    Get {
	name: Option<String>,
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
        Commands::Sync => hydrovu::sync()?,

        Commands::Load => {
	    // let ctx = SessionContext::new();
	    // let pond = pond::open()?;

	    // show(&ctx, pond.path_of("units.parquet"))?;
	    // show(&ctx, pond.path_of("params.parquet"))?;
	    // show(&ctx, pond.path_of("locations.parquet"))?;
	    // show(&ctx, pond.path_of("pond.parquet"))?;
	    // show(&ctx, pond.path_of("HydroVu.parquet"))?;
        }
        Commands::Read{until_time} => {
	    let time = date2utc(until_time)?;
	    let _x = hydrovu::read(&time)?;
	    // @@@
	},

	Commands::Init => pond::init()?,

	Commands::Apply{file_name} => pond::apply(file_name)?,

	Commands::Get{name} => pond::get(name.clone())?,
    }

    Ok(())
}

fn show<P: AsRef<Path>>(ctx: &SessionContext, name: P) -> Result<()> {
    let df = executor::block_on(ctx.read_parquet(name.as_ref().to_str().unwrap(), ParquetReadOptions::default()))
	.with_context(|| format!("read parquet failed {}", name.as_ref().display()))?;
    executor::block_on(df.show())
	.with_context(|| "show failed")?;
    Ok(())
}

fn date2utc(str: &String) -> Result<DateTime<FixedOffset>> {
    if str == "now" {
	let now = Utc::now();
	let now_fixed: DateTime<FixedOffset> = now.into();
	return Ok(now_fixed);
    }
    let date = DateTime::parse_from_rfc3339(&str)
	.with_context(|| format!("could not parse rfc3339 timestamp, use yyyy-mm-ddThh:mm:ssZ: {}", str))?;
    Ok(date)
}
