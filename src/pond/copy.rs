use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::backup::Common;
use crate::pond::backup::State;
use crate::pond::backup::new_s3;
use crate::pond::wd::WD;
use crate::pond::dir::DirEntry;
use crate::pond::dir::by2ft;
use crate::pond::crd::S3CopySpec;
use crate::pond::writer::MultiWriter;

use arrow::array::as_string_array;
use arrow::array::as_primitive_array;
use arrow::array::AsArray;
use arrow::array::ArrayRef;
use arrow::datatypes::{Int32Type,UInt64Type,UInt8Type};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use anyhow::{Context, Result, anyhow};

struct Copy {
    common: Common,
}

fn new_copy(uspec: &UniqueSpec<S3CopySpec>) -> Result<Copy> {
    Ok(Copy{
	common: new_s3(&uspec.spec.s3)?,
    })
}

pub fn init_func(_wd: &mut WD, uspec: &UniqueSpec<S3CopySpec>) -> Result<Option<InitContinuation>> {
    let mut copy = new_copy(&uspec)?;

    let state = copy.common.read_object::<State>("/POND")?;

    Ok(Some(Box::new(|_pond| {
	let state = state;
	let mut copy = copy;
	eprintln!("calling copy finish init func");

	for num in 1..=state.last {
	    // @@@ HERE YOU ARE
	    // have to use custom arrow logic for this struct, see writer.rs
	    // but for read path.
	    //let _data = copy.common.read_objects::<DirEntry>(format!("{}", num).as_str())?;
	    
	    //pond.in_path("", |wd| {
	    eprintln!("read a batch {}", num);
	    //})
	}
	Ok(())
    })))
}

pub fn run(_d: &mut WD, _spec: &UniqueSpec<S3CopySpec>) -> Result<()> {
    Ok(())
}

pub fn start(_pond: &mut Pond, _uspec: &UniqueSpec<S3CopySpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {

    Ok(Box::new(|pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	eprintln!("calling backup finish run func");
	pond.in_path("", |_wd| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	    	    
	    Ok(Box::new(|_writer| -> Result<()> {
		Ok(())
	    }))
	})
    }))
}

impl Copy {
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

	let mut reader = builder.build()
	    .with_context(|| "initialize reader failed")?;

	let mut ents = Vec::new();

	for rec in reader {
	    let batch = rec?;
	    let uuid: &ArrayRef = batch.column(2);
	    let sha256: &ArrayRef = batch.column(5);
	    let content: &ArrayRef = batch.column(6);

	    let comb = as_string_array(batch.column(0)).iter()
		.zip(as_primitive_array::<Int32Type>(batch.column(1)).iter())
		.zip(uuid.as_fixed_size_binary().iter())
		.zip(as_primitive_array::<UInt64Type>(batch.column(3)).iter())
		.zip(as_primitive_array::<UInt8Type>(batch.column(4)).iter())
		.zip(sha256.as_fixed_size_binary().iter())
		.zip(content.as_fixed_size_binary().iter());

	    ents.extend(comb.map(|((((((pfx, num), uuid), sz), ftype), sha), content): ((((((Option<&str>, Option<i32>), Option<&[u8]>), Option<u64>), Option<u8>), Option<&[u8]>), Option<&[u8]>)| -> DirEntry {
		let ub: uuid::Bytes = uuid.unwrap().try_into().expect("uuid has wrong length");
	    
		DirEntry{
		    prefix: pfx.unwrap().to_string(),
		    number: num.unwrap(),
		    uuid: uuid::Uuid::from_bytes(ub),
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
