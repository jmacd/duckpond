// SPDX-License-Identifier: Apache-2.0

//! The transparency-log checkpoint: a signed-tree-head in the C2SP
//! `tlog-checkpoint` note-body format.
//!
//! A checkpoint is the published commitment to the log's state at a given size.
//! Its body is three newline-terminated lines (design Section 7, Decision D5):
//!
//! ```text
//! <origin>
//! <size>
//! <base64(root_hash)>
//! ```
//!
//! * `origin` -- a schema-less identifier for the log (DuckPond uses
//!   `duckpond/<pond_id>`), distinguishing this log from any other.
//! * `size` -- the number of leaves (commit spine records) folded into the tree.
//! * `root_hash` -- the RFC 6962 tree head over those leaves, SHA-256, standard
//!   base64 with padding.
//!
//! Signing is deferred with the rest of the trust root (design Section 10), so
//! the checkpoint is written as a bare note body with no signature lines.  The
//! body is exactly the byte string a future signer will sign, so adding
//! signatures later does not change the checkpoint's meaning.

use std::io;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use super::merkle::LogHash;

/// The file name of the checkpoint within a log directory.
pub const CHECKPOINT_FILE: &str = "checkpoint";

/// A parsed transparency-log checkpoint (unsigned tlog-checkpoint note body).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// The log's schema-less origin identifier.
    pub origin: String,
    /// The number of leaves folded into `root`.
    pub size: u64,
    /// The RFC 6962 tree head over the first `size` leaves.
    pub root: LogHash,
}

impl Checkpoint {
    /// Construct a checkpoint for `size` leaves with tree head `root`.
    #[must_use]
    pub fn new(origin: impl Into<String>, size: u64, root: LogHash) -> Self {
        Self {
            origin: origin.into(),
            size,
            root,
        }
    }

    /// Encode the checkpoint as a C2SP tlog-checkpoint note body.
    #[must_use]
    pub fn encode(&self) -> String {
        format!(
            "{}\n{}\n{}\n",
            self.origin,
            self.size,
            BASE64.encode(self.root.as_bytes())
        )
    }

    /// Parse a checkpoint note body.
    ///
    /// The three required lines are read; any trailing extension lines are
    /// ignored so a future signer's added lines do not break parsing.
    ///
    /// # Errors
    ///
    /// Returns an error if the body has fewer than three lines, the size is not
    /// a non-negative integer, or the root is not a 32-byte base64 value.
    pub fn parse(body: &str) -> Result<Self, CheckpointError> {
        let mut lines = body.lines();
        let origin = lines
            .next()
            .ok_or(CheckpointError::MissingLine("origin"))?
            .to_string();
        let size = lines
            .next()
            .ok_or(CheckpointError::MissingLine("size"))?
            .parse::<u64>()
            .map_err(|_| CheckpointError::BadSize)?;
        let root_b64 = lines.next().ok_or(CheckpointError::MissingLine("root"))?;
        let root_bytes = BASE64
            .decode(root_b64.as_bytes())
            .map_err(|_| CheckpointError::BadRoot)?;
        let root_arr: [u8; 32] = root_bytes
            .try_into()
            .map_err(|_| CheckpointError::BadRoot)?;
        Ok(Self {
            origin,
            size,
            root: LogHash::from_bytes(root_arr),
        })
    }

    /// Atomically write this checkpoint into the log directory `dir`.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from creating the directory or writing the file.
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        super::tiles::write_atomic(&checkpoint_path(dir), self.encode().as_bytes())
    }

    /// Read the checkpoint from the log directory `dir`, if present.
    ///
    /// # Errors
    ///
    /// Returns an I/O error other than "not found", or a parse error if the
    /// file exists but is malformed.
    pub fn read(dir: &Path) -> Result<Option<Self>, CheckpointError> {
        let path = checkpoint_path(dir);
        match std::fs::read_to_string(&path) {
            Ok(body) => Ok(Some(Self::parse(&body)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CheckpointError::Io(e)),
        }
    }
}

/// The checkpoint file path within a log directory.
#[must_use]
pub fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join(CHECKPOINT_FILE)
}

/// Errors from checkpoint parsing or I/O.
#[derive(Debug)]
pub enum CheckpointError {
    /// A required line was absent from the note body.
    MissingLine(&'static str),
    /// The size line was not a non-negative integer.
    BadSize,
    /// The root line was not a 32-byte base64 value.
    BadRoot,
    /// An underlying I/O error while reading the checkpoint.
    Io(io::Error),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::MissingLine(name) => write!(f, "checkpoint missing {name} line"),
            CheckpointError::BadSize => write!(f, "checkpoint size is not an integer"),
            CheckpointError::BadRoot => write!(f, "checkpoint root is not a 32-byte base64 value"),
            CheckpointError::Io(e) => write!(f, "checkpoint I/O error: {e}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl From<io::Error> for CheckpointError {
    fn from(e: io::Error) -> Self {
        CheckpointError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tlog::hash_leaf;

    #[test]
    fn checkpoint_round_trips() {
        let cp = Checkpoint::new("duckpond/pond-a", 42, hash_leaf(b"leaf"));
        let encoded = cp.encode();
        assert_eq!(encoded.lines().count(), 3);
        assert!(encoded.ends_with('\n'));
        let parsed = Checkpoint::parse(&encoded).expect("parse");
        assert_eq!(parsed, cp);
    }

    #[test]
    fn checkpoint_ignores_trailing_extension_lines() {
        let cp = Checkpoint::new("duckpond/pond-a", 7, hash_leaf(b"x"));
        let mut body = cp.encode();
        body.push_str("timestamp 1700000000\n");
        let parsed = Checkpoint::parse(&body).expect("parse");
        assert_eq!(parsed, cp);
    }

    #[test]
    fn checkpoint_rejects_short_body() {
        assert!(matches!(
            Checkpoint::parse("origin\n1\n"),
            Err(CheckpointError::MissingLine("root"))
        ));
    }

    #[test]
    fn checkpoint_rejects_bad_root() {
        assert!(matches!(
            Checkpoint::parse("origin\n1\nnot-base64!!\n"),
            Err(CheckpointError::BadRoot)
        ));
    }

    #[test]
    fn checkpoint_file_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(Checkpoint::read(dir.path()).expect("read empty").is_none());
        let cp = Checkpoint::new("duckpond/pond-b", 3, hash_leaf(b"root"));
        cp.write(dir.path()).expect("write");
        let read = Checkpoint::read(dir.path()).expect("read").expect("some");
        assert_eq!(read, cp);
    }
}
