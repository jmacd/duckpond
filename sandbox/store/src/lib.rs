// SPDX-License-Identifier: Apache-2.0

//! Delta-Lake-backed key/value store with per-partition content checksums.
//!
//! See `../README.md` for context.

#![deny(unsafe_code)]
#![warn(missing_docs)]

//! Placeholder - implementation lands in `store-schema` and `checksum-trait` todos.

#[cfg(test)]
mod smoke {
    #[test]
    fn workspace_compiles() {
        // This test exists so `cargo test` in the empty skeleton runs at
        // least one passing test and we know the workspace plumbing is
        // wired correctly.  Replaced by real tests as the crate fills in.
    }
}
