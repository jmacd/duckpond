use super::dir::DirEntry;

#[derive(Debug)]
pub struct Writer {
    updates: Vec<DirEntry>,
}

impl Writer {
    pub fn new() -> Self {
	Writer{
	    updates: vec![],
	}
    }

    pub fn record(&mut self, update: DirEntry) {
	self.updates.push(update)
    }
}
