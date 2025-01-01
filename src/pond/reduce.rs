// 1 week @ 1hr = 168 points
// 2 weeks = 336
// 4 weeks = 672
// 30 days = 720 = 360*2hr
// 60 days =     = 360*4hr

// select time_bucket('2 hours', epoch_ms(CAST("Timestamp"*1000 as BIGINT))) as twohours, avg("AT500_Bottom.DO.mg/L") from read_parquet('./tmp/combined-FieldStation.parquet') group by twohours order by twohours;

// select "Timestamp", epoch_ms(CAST("Timestamp"*1000 as BIGINT)), "AT500_Bottom.DO.mg/L" from read_parquet('./tmp/combined-FieldStation.parquet') ;

//resolution: 1h

use crate::pond::UniqueSpec;
use crate::pond::wd::WD;
use crate::pond::Pond;
use crate::pond::crd::ReduceSpec;
use crate::pond::InitContinuation;

use anyhow::{Result};

static PARTITIONS: [&'static str; 5_] = ["year",
			 "quarter",
			 "month",
			 "week",
			 "day"
			 ];

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<ReduceSpec>, _former: Option<UniqueSpec<ReduceSpec>>) -> Result<Option<InitContinuation>> {
    // for coll in &uspec.spec.datasets {
    //     // _ = parse_glob(&coll.pattern)?;
    //     // let cv = vec![coll.clone()];
    //     // wd.write_whole_file(&coll.name, FileType::SynTree, &cv)?;
    // }

    Ok(None)
}

pub fn start(
    _pond: &mut Pond,
    _spec: &UniqueSpec<ReduceSpec>,
) -> Result<()> {
    return Ok(())
}
