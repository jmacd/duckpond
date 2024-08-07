use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::start_noop;
use crate::pond::backup::Common;
use crate::pond::backup::State;
use crate::pond::backup::new_bucket;
use crate::pond::wd::WD;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::crd::CopySpec;
use crate::pond::writer::MultiWriter;
use crate::pond::dir::read_entries_from_builder;

use core::str::FromStr;

use s3::bucket::Bucket;

use uuid::Uuid;

use std::path::PathBuf;
use std::path::Path;

use std::io::Write;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use anyhow::{Context, Result, anyhow};

struct Copy {
    common: Common,
    mine: PathBuf,
}

fn new_copy(uspec: &UniqueSpec<CopySpec>, bucket: Bucket) -> Result<Copy> {
    Ok(Copy{
	common: Common::new(bucket, uspec.spec.backup_uuid.clone().unwrap()),
	mine: uspec.dirpath(),
    })
}

pub fn split_path<P: AsRef<Path>>(path: P) -> Result<(PathBuf, String)> {
    let mut pb = path.as_ref().to_path_buf();
    pb.pop();
    Ok((pb, path.as_ref().file_name().unwrap().to_string_lossy().to_string()))
}

pub fn init_func(_wd: &mut WD, uspec: &mut UniqueSpec<CopySpec>) -> Result<Option<InitContinuation>> {
    let bucket = new_bucket(&uspec.spec.s3)?;

    if let None = uspec.spec.backup_uuid {
	let find_uuid = bucket.list("".to_string(), Some("/".to_string()))?;

	// Yuuck
	let mut found: Vec<Uuid> = Vec::new();
	for res in find_uuid {
	    if let Some(pfxs) = res.common_prefixes {
		for x in pfxs {
		    let p = Path::new(&x.prefix).iter().next().unwrap().to_string_lossy();
		    if let Ok(uuid) = Uuid::from_str(&p) {
			found.push(uuid);
		    }
		}
	    }
	}
	if found.len() == 1 {
	    uspec.spec.backup_uuid = Some(found.get(0).unwrap().to_string());
	} else {
	    return Err(anyhow!("uuid is not set or unique: {:?}", found))
	}
    }
    eprintln!("copy from backup {}", uspec.spec.backup_uuid.clone().unwrap());
    
    let mut copy = new_copy(&uspec, bucket)?;
    
    let state = copy.common.read_object::<State>(&copy.common.bpondpath())?;

    Ok(Some(Box::new(|pond: &mut Pond| {
	let state = state;
	let mut copy = copy;

	for num in 1..=state.last {
	    copy.copy_batch(pond, num)?;
	}

	let lstatevec = vec![state];
	pond.in_path(&copy.mine, |wd| wd.write_whole_file("state", FileType::Table, &lstatevec))
    })))
}

pub fn run(pond: &mut Pond, uspec: &UniqueSpec<CopySpec>) -> Result<()> {
    let bucket = new_bucket(&uspec.spec.s3)?;
    let mut copy = new_copy(&uspec, bucket)?;
    let dp = uspec.dirpath();

    let state = copy.common.read_object::<State>(&copy.common.bpondpath())?;
    
    let lstatevec = pond.in_path(
	&dp,
	|wd| wd.read_file::<State>("state"),
    )?;
	
    if lstatevec.len() != 1 {
	return Err(anyhow!("too many entries in local backup state"));
    }

    let lstate = lstatevec.get(0).unwrap();

    for num in lstate.last+1..=state.last {
	copy.copy_batch(pond, num)?;
    }
	
    Ok(())
}

pub fn start(pond: &mut Pond, spec: &UniqueSpec<CopySpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {
    start_noop(pond, spec)
}

impl Copy {
    fn copy_batch(&mut self, pond: &mut Pond, num: u64) -> Result<()> {
	eprintln!("copy backup batch {}", num);
	let entries = self.read_entries(format!("{}{}", self.common.brootpath(), num).as_str())?;

	for ent in &entries {
	    if ent.ftype == FileType::Tree {
		pond.in_path(PathBuf::new().join(&ent.prefix), |_wd| Ok(()))?;
	    }
	}
	
	for ent in &entries {
	    let pb = PathBuf::from(&ent.prefix);
	    let (mut dp, bn) = split_path(pb)?;

	    let levels = dp.components().fold(0, |acc, _x| acc+1);
	    if levels < 2 {
		let mut np = self.mine.clone();
		np.push(dp);
		dp = np;
	    }
	    pond.in_path(dp, |wd| {
		wd.create_any_file(bn.as_str(), ent.ftype, |mut f| {
		    f.write_all(ent.content.as_ref().unwrap().as_slice()).with_context(|| "write whole file")
		})
	    })?;
	}
	Ok(())
    }
    
    pub fn read_entries(&mut self, name: &str) -> Result<Vec<DirEntry>> {
	let resp_data = self.common.bucket.get_object(name)?;

	if resp_data.status_code() != 200 {
	    return Err(anyhow!("read {}: status code == {}", name, resp_data.status_code()));
	}

	let cursor = resp_data.bytes().clone();
	
	let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)
 	    .with_context(|| format!("open {} failed", name))?;

	read_entries_from_builder(builder)
    }
}
