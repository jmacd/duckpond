// SPDX-License-Identifier: Apache-2.0

//! Delta-Lake-backed key/value store with per-partition content checksums.
//!
//! See `../README.md` for context.
//!
//! # Quick tour
//!
//! - [`Store`]: the Delta-Lake-backed table holding versioned key/value rows.
//! - [`Op`]: a single Put or Delete in a batch.
//! - [`StoreError`] / [`Result`]: the error type used throughout.
//!
//! # Schema
//!
//! See [`schema`] for the column layout.  Briefly: each row is one
//! version of one item, partitioned by `partition_key`, with a
//! BLAKE3-of-value field on every row to support per-partition content
//! checksums (added in a later todo).

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod error;
pub mod schema;
mod store;

pub use error::{Result, StoreError};
pub use store::{Op, Store};
