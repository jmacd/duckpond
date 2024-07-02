use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::wd::WD;
use crate::pond::crd::S3BackupSpec;
use crate::pond::dir::Directory;

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

    Ok(Some(Box::new(|pond| copy_pond(pond, bucket))))
 }

fn copy_pond(pond: &mut Pond, bucket: Bucket) -> Result<()> {
    copy_recursive(&mut pond.root, &bucket)
}

fn copy_recursive(dir: &mut Directory, bucket: &Bucket) -> Result<()> {
    for ent in &dir.ents {
	//
    }
    for (ref _name, ref mut sub) in &mut dir.subdirs {
	copy_recursive(sub, bucket)?;
    }
    Ok(())
}
