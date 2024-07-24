use crate::pond;
use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::ForArrow;
use crate::pond::ForPond;
use crate::pond::wd::WD;
use crate::pond::crd::S3BackupSpec;
use crate::pond::crd::S3Fields;
use crate::pond::dir::FileType;
use crate::pond::writer::MultiWriter;

use s3::bucket::Bucket;
use s3::region::Region;
use s3::creds::Credentials;

use serde::{Serialize, Deserialize};

use rand::prelude::thread_rng;
use rand::Rng;

use std::sync::Arc;
use std::path::Path;
use std::path::PathBuf;
use std::env::temp_dir;

use arrow::datatypes::{DataType, Field, FieldRef};

use anyhow::{Context, Result, anyhow};

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Commands {
    List {
	#[arg(short,long)]
	uuid: String,
    },
}

pub struct Common {
    pub bucket: Bucket,
}

struct Backup {
    common: Common,
    writer_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct State {
    pub last: u64,
}

impl ForArrow for State {
    fn for_arrow() -> Vec<FieldRef> {
	vec![
            Arc::new(Field::new("last", DataType::UInt64, false)),
	]
    }
}

impl Backup {
    fn open_and_put<P: AsRef<Path>>(&mut self, path: P, newpath: &str) -> Result<()> {
	let mut file = std::fs::File::open(path)?;
	let resp = self.common.bucket.put_object_stream(&mut file, newpath).with_context(|| "could not put object")?;
	let _ = resp;
	//eprintln!(" put {} status is {:?}", newpath, resp);
	Ok(())
    }
}

impl Common {
    fn read_objects<T: for<'a> Deserialize<'a>>(&mut self, name: &str) -> Result<Vec<T>> {
	let resp_data = self.bucket.get_object(name)?;

	if resp_data.status_code() != 200 {
	    return Err(anyhow!("read {}: status code == {}", name, resp_data.status_code()));
	}

	let cursor = resp_data.bytes().clone();
	
	let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)
 	    .with_context(|| format!("open {} failed", name))?;

	let mut reader = builder.build()
	    .with_context(|| "initialize reader failed")?;
	let input = reader.next();

	match input {
	    None => Err(anyhow!("no records")),
	    Some(value) => {
		let records = value?;
		Ok(serde_arrow::from_record_batch(&records)?)
	    }
	}
    }

    pub fn read_object<T: for<'a> Deserialize<'a>>(&mut self, name: &str) -> Result<T> {
	let mut recs = self.read_objects::<T>(name)?;
	
	if recs.len() == 1 {
	    Ok(recs.remove(0))
	} else {
	    Err(anyhow!("expected one record have {}", recs.len()))
	}
    }

    fn write_object<T: Serialize + ForArrow>(
	&mut self,
	name: &str,
	record: &T,
    ) -> Result<()> {
	let records = vec![record];
	let batch = serde_arrow::to_record_batch(T::for_arrow().as_slice(), &records)
            .with_context(|| "serialize arrow data failed")?;

	let mut data: Vec<u8> = Vec::new();

	let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
		ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
            ))
            .build();

	let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(props))
            .with_context(|| "new arrow writer failed")?;

	writer
            .write(&batch)
            .with_context(|| "write parquet data failed")?;
	writer
            .close()
            .with_context(|| "close parquet file failed")?;

	let resp_data = self.bucket.put_object(name, data.as_slice())?;

	if resp_data.status_code() != 200 {
	    return Err(anyhow!("status code == {}", resp_data.status_code()));
	}
	
	Ok(())
    }	
}

pub fn new_s3(s3: &S3Fields) -> Result<Common> {
    let region = Region::Custom{
	region: s3.region.clone(),
	endpoint: s3.endpoint.clone(),
    };
    let creds = Credentials::new(Some(s3.key.as_str()), Some(s3.secret.as_str()), None, None, None)?;

    Ok(Common{
	bucket: Bucket::new(s3.bucket.as_str(), region, creds)?,
    })
}

fn new_backup(uspec: &UniqueSpec<S3BackupSpec>, writer_id: usize) -> Result<Backup> {
    Ok(Backup{
	common: new_s3(&uspec.spec.s3)?,
	writer_id: writer_id,
    })
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<S3BackupSpec>) -> Result<Option<InitContinuation>> {
    let mut backup = new_backup(&uspec, wd.w.add_writer())?;

    let state = State{
	last: 1,
    };

    match backup.common.read_object::<State>("/POND") {
	Err(_) => {},
	Ok(_) => return Err(anyhow!("pond backup already exists")),
    }

    let dp = uspec.dirpath();

    Ok(Some(Box::new(|pond| {
	let dp = dp;
	//eprintln!("calling backup finish init func");
	pond.in_path("", |wd| {
	    let mut backup = backup;
	    let state = state;

	    // this will copy an empty state directory belonging to this resource.
	    copy_pond(wd, backup.writer_id)?;

	    let mut path = temp_dir();
	    let mut rng = thread_rng();
	
	    path.push(format!("{}.parquet", rng.gen::<u64>()));

	    wd.w.writer_mut(backup.writer_id)
		.ok_or(anyhow!("invalid writer"))?
		.commit_to_local_file(&path)?;

	    backup.open_and_put(&path, "/1")?;

	    backup.common.write_object("/POND", &state)?;

	    let statevec = vec![state];
	    wd.in_path(&dp, |wd| wd.write_whole_file("state", &statevec))
	})
    })))
}

fn copy_pond(wd: &mut WD, writer_id: usize) -> Result<()> {
    let ents = wd.d.ents.clone();
    for ent in &ents {
	let mut went = ent.clone();

	let pfx: &str;

	match ent.ftype {
	    FileType::Tree => pfx = "dir",
	    _ => pfx = ent.prefix.as_str(),
	};

	went.content = Some(std::fs::read(wd.prefix_num_path(pfx, went.number))?);
	went.prefix = wd.d.relp.join(&ent.prefix).to_string_lossy().to_string();
		
	let writer = wd.w.writer_mut(writer_id).ok_or(anyhow!("missing writer"))?;
	writer.record(&went)?;

	if let FileType::Tree = ent.ftype {
	    wd.in_path(&ent.prefix, |d| copy_pond(d, writer_id))?;
	}
    }
    Ok(())
}

pub fn run(_d: &mut WD, _spec: &UniqueSpec<S3BackupSpec>) -> Result<()> {
    Ok(())
}

pub fn start(pond: &mut Pond, uspec: &UniqueSpec<S3BackupSpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {
    let uspec = uspec.clone();
    let mut backup = new_backup(&uspec, pond.writer.add_writer())?;
    let s3_state = backup.common.read_object::<State>("/POND")?;

    let dp = uspec.dirpath();
    let local_state = pond.in_path(
	&dp,
	|wd| wd.read_file::<State>("state"),
    )?;

    if local_state.len() != 1 {
	return Err(anyhow!("too many entries in local backup state"));
    }

    if *local_state.get(0).unwrap() != s3_state {
	return Err(anyhow!("local and remote states are not equal, repair needed"));
    }

    Ok(Box::new(|pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	let dp = dp;
	//eprintln!("calling backup finish run func");
	pond.in_path(&dp, |wd| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	    let mut state = s3_state;

	    state.last += 1;

	    let statevec = vec![state.clone()];
	    wd.write_whole_file("state", &statevec)?;

	    //eprintln!("have written new state file");
	    	    
	    Ok(Box::new(|writer| -> Result<()> {
		let mut backup = backup;
		let mut path = temp_dir();
		let mut rng = thread_rng();
		let state = state;
	
		path.push(format!("{}.parquet", rng.gen::<u64>()));

		//eprintln!("here calling commit");
	    
		writer.writer_mut(backup.writer_id)
		    .ok_or(anyhow!("invalid writer"))?
		    .commit_to_local_file(&path)?;

		backup.open_and_put(&path, format!("/{}", state.last).as_str())?;

		backup.common.write_object("/POND", &state)
	    }))
	})
    }))
}

pub fn sub_main(command: &Commands) -> Result<()> {
    let mut pond = pond::open()?;
    match command {
        Commands::List{uuid} => {
	    let kind = S3BackupSpec::spec_kind();
	    let specs: Vec<UniqueSpec<S3BackupSpec>> = pond.in_path(PathBuf::new().join(kind).join(uuid), |wd| wd.read_file(kind))?;
	    let mut onespec: Vec<_> = specs.iter().filter(|x| x.uuid.to_string() == *uuid).collect();

	    if onespec.len() == 0 {
		return Err(anyhow!("uuid not found {}", uuid.to_string()));
	    }
	    let spec = onespec.remove(0);

	    eprintln!("got it {:?}", spec);
	},
    }
    Ok(())
}
