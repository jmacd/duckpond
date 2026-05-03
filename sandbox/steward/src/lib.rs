// SPDX-License-Identifier: Apache-2.0

//! Transaction lifecycle, control table, maintenance, verification.
//!
//! See `../README.md` for context.
//!
//! # Quick tour
//!
//! - [`Steward`]: the top-level handle.  Wraps a `sandbox-store::Store`
//!   and a control table; allocates `txn_seq` monotonically; emits
//!   lifecycle records and per-partition checksums on every commit.
//! - [`WriteGuard`]: in-flight write transaction; accepts puts/deletes;
//!   consumed by `commit` or `abort`.
//! - [`ReadGuard`]: read-only view (no control-table writes).
//! - [`control_table::ControlTable`]: low-level access to the lifecycle
//!   record store.
//! - [`verify_local`]: re-derive partition checksums from current data
//!   and compare against the latest `DataCommitted` record.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod control_table;
mod error;
mod guard;
mod steward;

pub use control_table::{
    ChecksumValue, CommitKind, ControlRecord, DataCommittedMetadata, PartitionChecksums, RecordKind,
};
pub use error::{Result, StewardError};
pub use guard::{ReadGuard, WriteGuard};
pub use steward::{
    CommitOutcome, Steward, StewardOptions, VerifyMismatch, VerifyReport, verify_local,
};
