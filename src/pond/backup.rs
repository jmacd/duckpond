use crate::pond::InitContinuation;
use crate::pond::wd::WD;
use crate::pond::crd::S3BackupSpec;
use crate::pond::dir::FileType;

use s3::bucket::Bucket;
use s3::region::Region;
use s3::creds::Credentials;

use rand::prelude::thread_rng;
use rand::Rng;

use std::path::Path;
use std::env::temp_dir;

// use futures::executor::block_on;
// use async_std::fs::OpenOptions;

use anyhow::{Context, Result};

// async fn open_and_put<P: AsRef<Path>>(path: P, bucket: &mut Bucket) -> Result<()> {
//     let mut file = OpenOptions::new()
// 	.read(true)
// 	.write(false)
// 	.open(path.as_ref())
// 	.await?;
//     let resp = bucket.put_object_stream(&mut file, "/path").await.with_context(|| "could not put object")?;
//     eprintln!("status is {}", resp.status_code());
//     Ok(())
// }

fn open_and_put<P: AsRef<Path>>(path: P, bucket: &mut Bucket) -> Result<()> {
    let mut file = std::fs::File::open(path)?;
    let resp = bucket.put_object_stream(&mut file, "/path").with_context(|| "could not put object")?;
    eprintln!("status is {}", resp);
    Ok(())
}

pub fn init_func(_d: &mut WD, spec: &S3BackupSpec) -> Result<Option<InitContinuation>> {
    let region = Region::Custom{
	region: spec.region.clone(),
	endpoint: spec.endpoint.clone(),
    };
    let creds = Credentials::new(Some(spec.key.as_str()), Some(spec.secret.as_str()), None, None, None)?;
    let bucket = Bucket::new(spec.bucket.as_str(), region, creds)?;

    match bucket.exists() {
	Err(x) => eprintln!("bucket exists error {:?}", x),
	Ok(_) => (),
    }

    Ok(Some(Box::new(|pond| pond.in_path("", |wd| {
	let mut bucket = bucket;
	copy_pond(wd)?;

	let mut path = temp_dir();
	let mut rng = thread_rng();
	
	path.push(format!("{}.parquet", rng.gen::<u64>()));

	wd.w.commit_to_local_file(&path)?;
	
	//block_on(open_and_put(&path, &mut bucket))?;
	open_and_put(&path, &mut bucket)?;

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
