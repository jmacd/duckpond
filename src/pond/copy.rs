use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::backup::Common;
use crate::pond::backup::State;
use crate::pond::backup::new_s3;
use crate::pond::wd::WD;
use crate::pond::dir::DirEntry;
use crate::pond::dir::by2ft;
use crate::pond::dir::FileType;
use crate::pond::crd::S3CopySpec;
use crate::pond::writer::MultiWriter;

use arrow::array::as_string_array;
use arrow::array::as_primitive_array;
use arrow::array::AsArray;
use arrow::array::ArrayRef;
use arrow::datatypes::{Int32Type,UInt64Type,UInt8Type};

use std::path::PathBuf;
use std::path::Path;

use std::io::Write;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use anyhow::{Context, Result, anyhow};

struct Copy {
    common: Common,
    mine: PathBuf,
}

fn new_copy(uspec: &UniqueSpec<S3CopySpec>) -> Result<Copy> {
    Ok(Copy{
	common: new_s3(&uspec.spec.s3)?,
	mine: uspec.dirpath(),
    })
}

fn split_path<P: AsRef<Path>>(path: P) -> Result<(PathBuf, String)> {
    let mut pb = path.as_ref().to_path_buf();
    pb.pop();
    Ok((pb, path.as_ref().file_name().unwrap().to_string_lossy().to_string()))
}

pub fn init_func(_wd: &mut WD, uspec: &UniqueSpec<S3CopySpec>) -> Result<Option<InitContinuation>> {
    let mut copy = new_copy(&uspec)?;
    let dp = uspec.dirpath();
    let state = copy.common.read_object::<State>("/POND")?;

    Ok(Some(Box::new(|pond: &mut Pond| {
	let state = state;
	let dp = dp;
	let mut copy = copy;

	for num in 1..=state.last {
	    copy.copy_batch(pond, num)?;
	}

	let lstatevec = vec![state];
	pond.in_path(&dp, |wd| wd.write_whole_file("state", &lstatevec))
    })))
}

pub fn run(_d: &mut WD, _spec: &UniqueSpec<S3CopySpec>) -> Result<()> {
    Ok(())
}

pub fn start(_pond: &mut Pond, uspec: &UniqueSpec<S3CopySpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {
    let mut copy = new_copy(&uspec)?;
    let dp = uspec.dirpath();

    let state = copy.common.read_object::<State>("/POND")?;

    Ok(Box::new(|pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	let mut copy = copy;
	let state = state;
	let dp = dp;

	let lstatevec = pond.in_path(
	    &dp,
	    |wd| wd.read_file::<State>("state"),
	)?;
	
	if lstatevec.len() != 1 {
	    return Err(anyhow!("too many entries in local backup state"));
	}

	let lstate = lstatevec.get(0).unwrap();

	eprintln!("backup {} local state {}", state.last, lstate.last);

	for num in state.last+1..=lstate.last {
	    copy.copy_batch(pond, num)?;
	}
	
	Ok(Box::new(|_writer| -> Result<()> {
	    Ok(())
	}))
    }))
}

impl Copy {
    fn copy_batch(&mut self, pond: &mut Pond, num: u64) -> Result<()> {
	eprintln!("copy backup batch {}", num);
	let entries = self.read_entries(format!("{}", num).as_str())?;

	for ent in &entries {
	    if ent.ftype == FileType::Tree {
		pond.in_path(PathBuf::new().join(&ent.prefix), |_wd| Ok(()))?;
	    }
	}
	
	for ent in &entries {
	    if ent.ftype == FileType::Tree {
		continue;
	    }
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
    
    // TODO share code w/ dir.rs which does the same.
    // Note there is a different treatment for content field, and they're
    // nearly identical.
    pub fn read_entries(&mut self, name: &str) -> Result<Vec<DirEntry>> {

	let resp_data = self.common.bucket.get_object(name)?;

	if resp_data.status_code() != 200 {
	    return Err(anyhow!("read {}: status code == {}", name, resp_data.status_code()));
	}

	let cursor = resp_data.bytes().clone();
	
	let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)
 	    .with_context(|| format!("open {} failed", name))?;

	let reader = builder.build()
	    .with_context(|| "initialize reader failed")?;

	let mut ents = Vec::new();

	for rec in reader {
	    let batch = rec?;
	    let sha256: &ArrayRef = batch.column(4);
	    let content: &ArrayRef = batch.column(5);
	    let pfxs = as_string_array(batch.column(0));
	    let nums = as_primitive_array::<Int32Type>(batch.column(1));
	    let sizes = as_primitive_array::<UInt64Type>(batch.column(2));
	    let ftypes = as_primitive_array::<UInt8Type>(batch.column(3));

	    let comb = pfxs.iter()
		.zip(nums.iter())
		.zip(sizes.iter())
		.zip(ftypes.iter())
		.zip(sha256.as_fixed_size_binary().iter())
		.zip(content.as_binary::<i32>().iter());

	    ents.extend(comb.map(|(((((pfx, num), sz), ftype), sha), content): (((((Option<&str>, Option<i32>), Option<u64>), Option<u8>), Option<&[u8]>), Option<&[u8]>)| -> DirEntry {
	    
		DirEntry{
		    prefix: pfx.unwrap().to_string(),
		    number: num.unwrap(),
		    size: sz.unwrap(),
		    ftype: by2ft(ftype.unwrap()).unwrap(),
		    sha256: sha.unwrap().try_into().expect("sha256 has wrong length"),
		    content: Some(Vec::from(content.unwrap())),
		}
	    }));
	}

	Ok(ents)
    }
}
