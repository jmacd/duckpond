// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! DuckPond Utilities
//!
//! Shared utilities used across DuckPond crates

pub mod banner;
pub mod perf_trace;

// Available for testing in this crate and any crate that depends on utilities in dev-dependencies
#[cfg(any(test, feature = "test-helpers"))]
pub mod test_helpers;
