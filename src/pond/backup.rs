use crate::pond::InitContinuation;
use crate::pond::wd::WD;
use crate::pond::crd::S3BackupSpec;
use crate::pond::dir::FileType;

use s3::bucket::Bucket;
use s3::region::Region;
use s3::creds::Credentials;

use serde::{Serialize, Deserialize};

use rand::prelude::thread_rng;
use rand::Rng;

use std::sync::Arc;
use std::path::Path;
use std::env::temp_dir;

use arrow::datatypes::{DataType, Field, FieldRef};

use anyhow::{Context, Result, anyhow};

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

struct Backup {
    bucket: Bucket,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct State {
    last: u64,
}

fn state_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("last", DataType::UInt64, false)),
    ]
}

impl Backup {
    fn open_and_put<P: AsRef<Path>>(&mut self, path: P, newpath: &str) -> Result<()> {
	let mut file = std::fs::File::open(path)?;
	let resp = self.bucket.put_object_stream(&mut file, newpath).with_context(|| "could not put object")?;
	eprintln!("status is {:?}", resp);
	Ok(())
    }

    fn read_object<T: for<'a> Deserialize<'a>>(&mut self, name: &str) -> Result<T> {
	let resp_data = self.bucket.get_object(name)?;

	if resp_data.status_code() != 200 {
	    return Err(anyhow!("status code == {}", resp_data.status_code()));
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
		let mut recs: Vec<T> = serde_arrow::from_record_batch(
		    &value.with_context(|| "deserialize record batch failed")?)?;

		if recs.len() == 1 {
		    Ok(recs.remove(0))
		} else {
		    Err(anyhow!("expected one record have {}", recs.len()))
		}
	    },
	}
    }

    fn write_object<T: Serialize>(
	&mut self,
	name: &str,
	record: &T,
	fields: &[Arc<Field>],
    ) -> Result<()> {
	let records = vec![record];
	let batch = serde_arrow::to_record_batch(fields, &records)
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

pub fn init_func(_d: &mut WD, spec: &S3BackupSpec) -> Result<Option<InitContinuation>> {
    let region = Region::Custom{
	region: spec.region.clone(),
	endpoint: spec.endpoint.clone(),
    };
    let creds = Credentials::new(Some(spec.key.as_str()), Some(spec.secret.as_str()), None, None, None)?;

    let backup = Backup{
	bucket: Bucket::new(spec.bucket.as_str(), region, creds)?,
    };

    Ok(Some(Box::new(|pond| pond.in_path("", |wd| {
	let mut backup = backup;
	copy_pond(wd)?;

	let mut path = temp_dir();
	let mut rng = thread_rng();
	
	path.push(format!("{}.parquet", rng.gen::<u64>()));

	wd.w.commit_to_local_file(&path)?;

	let pond = State{
	    last: 1,
	};

	backup.open_and_put(&path, "/00001")?;

	backup.write_object("/POND", &pond, state_fields().as_slice())?;

	let check: State = backup.read_object("/POND")?;

	eprintln!("read back {:?}", check);
	
	Ok(())
    }))))
 }

fn copy_pond(wd: &mut WD) -> Result<()> {
    let ents = wd.d.ents.clone();
    for ent in &ents {
	let mut went = ent.clone();

	let pfx: &str;

	match ent.ftype {
	    FileType::Tree => pfx = "dir",
	    _ => pfx = ent.prefix.as_str(),
	};

	went.content = Some(std::fs::read(wd.prefix_num_path(pfx, went.number))?);
		
	wd.w.record(went)?;

	if let FileType::Tree = ent.ftype {
	    wd.in_path(&ent.prefix, |d| copy_pond(d))?;
	}
    }
    Ok(())
}
