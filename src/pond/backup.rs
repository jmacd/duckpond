use crate::pond::InitContinuation;
use crate::pond::wd::WD;
use crate::pond::crd::S3BackupSpec;
use crate::pond::dir::FileType;

use s3::bucket::Bucket;
use s3::region::Region;
use s3::creds::Credentials;

use anyhow::Result;

pub fn init_func(_d: &mut WD, spec: &S3BackupSpec) -> Result<Option<InitContinuation>> {
    let region = Region::Custom{
	region: "duckpond_configured".to_string(),
	endpoint: spec.endpoint.clone(),
    };
    let creds = Credentials::new(Some(spec.key.as_str()), Some(spec.secret.as_str()), None, None, None)?;
    let bucket = Bucket::new(spec.bucket.as_str(), region, creds)?;

    Ok(Some(Box::new(|pond| pond.in_path("", |wd| {
	let bucket = bucket;	// Note: move bucket here, then borrow for recursive call
	copy_pond(wd, &bucket)
    }))))
 }

fn copy_pond(wd: &mut WD, bucket: &Bucket) -> Result<()> {
    let ents = wd.d.ents.clone();
    for ent in &ents {
	eprintln!("copy dirent {:?}", ent);

	if let FileType::Tree = ent.ftype {
	    eprintln!("copy subdir {}", ent.prefix);
	    wd.in_path(&ent.prefix, |d| copy_pond(d, bucket))?;
	}
    }
    Ok(())
}
