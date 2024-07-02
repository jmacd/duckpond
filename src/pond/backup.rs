use crate::pond::wd::WD;
use crate::pond::crd::S3BackupSpec;

use s3::bucket::Bucket;
use s3::region::Region;
use s3::creds::Credentials;

use anyhow::Result;

pub fn init_func(_d: &mut WD, spec: &S3BackupSpec) -> Result<()> {

    let region = Region::Custom{
	region: "duckpond_configured".to_string(),
	endpoint: spec.endpoint.clone(),
    };
    let creds = Credentials::new(Some(spec.key.as_str()), Some(spec.secret.as_str()), None, None, None)?;
    let bucket = Bucket::new(spec.bucket.as_str(), region, creds)?;

    // Next?
    // Walk the tree, write each direntry with content set (in some order)
    // Break out links (?) for large files
    
    Ok(())
}
