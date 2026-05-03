// SPDX-License-Identifier: Apache-2.0

//! Bundle-format remote-sync layer for the sandbox.
//!
//! See `../../DESIGN.md` §2.5 for the full design.  In one paragraph:
//! the remote is a single Delta Lake table per pond family.  Every push
//! produces one Delta commit containing 1 manifest row + N checksum
//! rows + C data rows (chunked file content), routed across three
//! `partition_kind` partitions (`manifest`, `checksum`, `data`).  The
//! Delta commit is the atomic boundary for the entire bundle, and
//! Delta's own vacuum/checkpoint machinery is the only retention
//! mechanism.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod error;
pub mod schema;

pub use error::{RemoteError, Result};
pub use schema::{
    BLAKE3_LEN, CHECKSUM_KIND_HOMOMORPHIC, CHECKSUM_KIND_MERKLE, CHUNK_SIZE_BYTES,
    COMMIT_KIND_COMPACT, COMMIT_KIND_WRITE, FILE_ACTION_ADD, FILE_ACTION_REMOVE,
    PARTITION_KIND_CHECKSUM, PARTITION_KIND_DATA, PARTITION_KIND_MANIFEST, RemoteRow, RowBody,
    arrow_schema, delta_columns, partition_columns, record_batch_to_rows, rows_to_record_batch,
};

#[cfg(test)]
mod smoke {
    #[test]
    fn workspace_compiles() {}
}
