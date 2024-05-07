use futures::executor;
use hydrovu::error::Error;

use anyhow::Context;
use chrono::offset::Utc;
use chrono::offset::FixedOffset;
use chrono::DateTime;

use clap::{Parser, Subcommand};

use datafusion::{
    prelude::{ParquetReadOptions, SessionContext},
};

pub mod hydrovu;

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
}

fn main() {
    match main_result() {
	Ok(_) => {},
	Err(err) => {
	    match err {
		Error::Anyhow(err) => eprintln!("{:?}", err)
	    }
	},
    }
}

fn main_result() -> Result<(), Error> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Sync => hydrovu::sync()?,

        Commands::Load => {
	    let ctx = SessionContext::new();

	    show(&ctx, "units.parquet")?;
	    show(&ctx, "params.parquet")?;
	    show(&ctx, "locations.parquet")?;
        }
        Commands::Read{until_time} => {
	    let time = date2utc(until_time)?;
	    let _x = hydrovu::read(&time)?;
	    // @@@
	},
    }

    Ok(())
}

fn show(ctx: &SessionContext, name: &str) -> Result<(), Error> {
    let df = executor::block_on(ctx.read_parquet(name, ParquetReadOptions::default()))
	.with_context(|| format!("read parquet failed {}", name))?;
    executor::block_on(df.show())
	.with_context(|| "show failed")?;
    Ok(())
}

fn date2utc(str: &String) -> Result<DateTime<FixedOffset>, Error> {
    if str == "now" {
	let now = Utc::now();
	let now_fixed: DateTime<FixedOffset> = now.into();
	return Ok(now_fixed);
    }
    let date = DateTime::parse_from_rfc3339(&str)
	.with_context(|| format!("could not parse rfc3339 timestamp, use yyyy-mm-ddThh:mm:ssZ: {}", str))?;
    Ok(date)
}
