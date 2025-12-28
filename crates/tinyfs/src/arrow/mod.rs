// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Arrow integration for TinyFS
//!
//! This module provides Arrow ecosystem integration, including:
//! - Schema generation from Rust types via ForArrow trait
//! - Parquet file I/O with async support
//! - Integration with the serde_arrow ecosystem

pub mod parquet;
pub mod schema;
pub use parquet::ParquetExt;
pub use schema::ForArrow;
