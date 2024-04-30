use futures::executor;

use std::error::Error;

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

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Sync => hydrovu::sync()?,

        Commands::Load => {
	    let ctx = SessionContext::new();

	    show(&ctx, "units.parquet")?;
	    show(&ctx, "params.parquet")?;
	    show(&ctx, "locations.parquet")?;
            ()
        }
        Commands::Read => {
	    let _x = hydrovu::read()?;
	    // @@@
	    ()
	},
    }

    Ok(())
}

fn show(ctx: &SessionContext, name: &str) -> Result<(), Box<dyn Error>> {
    let df = executor::block_on(ctx.read_parquet(name, ParquetReadOptions::default()))?;
    executor::block_on(df.show())?;
    Ok(())
}
