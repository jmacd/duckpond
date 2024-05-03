#[derive(Debug)]
pub enum Error {
    Anyhow(anyhow::Error),
}

impl From<anyhow::Error> for Error {
    fn from(value: anyhow::Error) -> Self {
        Self::Anyhow(value)
    }
}
