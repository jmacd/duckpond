use crate::hydrovu::Client;
use crate::hydrovu::utc2date;

use crate::hydrovu::client::fetch_locations;
use crate::pond::Pond;
use crate::pond::UniqueSpec;
use crate::pond::crd::HydroVuSpec;
use crate::pond::sub_main_cmd;

use clap::Subcommand;

use anyhow::Result;
use std::rc::Rc;

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Scan prints raw timeseries data.
    Scan {
        #[arg(short, long)]
        uuid: String,

        #[arg(short, long)]
        loc_id: i64,

        #[arg(short, long)]
        start_ts: i64,

        #[arg(short, long)]
        end_ts: i64,
    },

    /// Location prints raw location data.
    Locations {
        #[arg(short, long)]
        uuid: String,
    },
}

pub fn hydrovu_sub_main(command: &Commands) -> Result<()> {
    let mut pond = crate::pond::open()?;
    match command {
        Commands::Scan {
            uuid,
            loc_id,
            start_ts,
            end_ts,
        } => scan(&mut pond, uuid.as_str(), *loc_id, *start_ts, *end_ts),
        Commands::Locations { uuid } => listlocs(&mut pond, uuid.as_str()),
    }
}

fn listlocs(pond: &mut Pond, uuid: &str) -> Result<()> {
    sub_main_cmd(
        pond,
        uuid,
        |_pond: &mut Pond, spec: &mut UniqueSpec<HydroVuSpec>| -> Result<()> {
            let client = Rc::new(Client::new(crate::hydrovu::creds(spec.inner()))?);
            for locs in fetch_locations(client.clone()) {
                for loc in locs? {
                    eprintln!("Location {}: {}: {}", loc.description, loc.name, loc.id);
                }
            }
            Ok(())
        },
    )
}

fn scan(pond: &mut Pond, uuid: &str, loc_id: i64, start_ts: i64, end_ts: i64) -> Result<()> {
    sub_main_cmd(
        pond,
        uuid,
        |_pond: &mut Pond, spec: &mut UniqueSpec<HydroVuSpec>| -> Result<()> {
            let client = Rc::new(Client::new(crate::hydrovu::creds(spec.inner()))?);

            for one_data in
                crate::hydrovu::fetch_data(client.clone(), loc_id, start_ts, Some(end_ts))
            {
                let one = one_data?;

                let num_points = one
                    .parameters
                    .iter()
                    .fold(0, |acc, e| acc + e.readings.len());

                let min_one = one
                    .parameters
                    .iter()
                    .map(|x| &x.readings)
                    .flatten()
                    .fold(std::i64::MAX, |a, b| a.min(b.timestamp));

                let max_one = one
                    .parameters
                    .iter()
                    .map(|x| &x.readings)
                    .flatten()
                    .fold(std::i64::MIN, |a, b| a.max(b.timestamp));

                eprintln!(
                    "min/max {} {}: {} {}: {}",
                    &min_one,
                    &max_one,
                    utc2date(min_one)?,
                    utc2date(max_one)?,
                    num_points
                );
            }
            Ok(())
        },
    )
}
