use crate::pond::crd::DeriveSpec;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use anyhow::Result;

pub fn init_func(
    _wd: &mut WD,
    _uspec: &UniqueSpec<DeriveSpec>,
) -> Result<Option<InitContinuation>> {
    Ok(None)
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<DeriveSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    start_noop(pond, spec)
}
