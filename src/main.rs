use futures::executor;

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
    Read,
}

fn main() {
    match main_result() {
	Ok(_) => {},
	Err(err) => {
	    match err {
		hydrovu::Error::Anyhow(err) => eprintln!("{:?}", err)
	    }
	},
    }
}

fn main_result() -> Result<(), hydrovu::Error> {
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
        Commands::Read => {
	    let _x = hydrovu::read()?;
	    // @@@
	},
    }

    Ok(())
}

use anyhow::Context;

fn show(ctx: &SessionContext, name: &str) -> Result<(), hydrovu::Error> {
    let df = executor::block_on(ctx.read_parquet(name, ParquetReadOptions::default()))
	.with_context(|| format!("read parquet failed {}", name))?;
    executor::block_on(df.show())
	.with_context(|| "show failed")?;
    Ok(())
}
