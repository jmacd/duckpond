use super::dir::DirEntry;

// @@@ Not like this.  This will load something from the list of
// backup resources and then, be transactional? Don't buffer
// all updates in memory, apply them and then commit them.

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
