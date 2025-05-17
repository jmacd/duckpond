use datafusion::error::DataFusionError;
use deltalake::DeltaTableError;
use arrow_schema::ArrowError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("table error")]
    Delta(#[from] DeltaTableError),

    #[error("table error")]
    Fusion(#[from] DataFusionError),

    #[error("arrow error")]
    Arrow(#[from] ArrowError),

    #[error("serde error")]
    Serde(#[from] serde_arrow::Error),

    #[error("missing data")]
    Missing,
}
