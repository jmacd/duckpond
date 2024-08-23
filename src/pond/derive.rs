use crate::pond::crd::DeriveSpec;
use crate::pond::wd::WD;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;

use anyhow::Result;

pub fn init_func(
    _wd: &mut WD,
    _uspec: &UniqueSpec<DeriveSpec>,
) -> Result<Option<InitContinuation>> {
    Ok(None)
}
