// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared configuration parsing helpers for SQL-derived factories.
//!
//! The dynamic factories (`temporal-reduce`, `timeseries-join`,
//! `timeseries-pivot`, ...) all follow the same two-step config flow:
//!
//! - `validate`: receives raw YAML bytes, parses them into a [`serde_json::Value`]
//!   and deserializes into the typed config to surface schema errors early.
//! - `file`/`directory`: receives the already-parsed [`serde_json::Value`] and
//!   deserializes it into the typed config.
//!
//! These helpers centralize that boilerplate so each factory only writes its
//! factory-specific extra validation.

use serde::de::DeserializeOwned;
use serde_json::Value;
use tinyfs::{Result as TinyFSResult, ResultExt};

/// Parse raw YAML config bytes into a [`serde_json::Value`] and a typed config.
///
/// Used by factory `validate` functions: the returned [`Value`] is the canonical
/// representation stored for the node, while the typed `T` is available for any
/// factory-specific validation. `ctx` is used as the error context when the typed
/// deserialization fails (e.g. `"Invalid timeseries-join config"`).
pub(crate) fn parse_yaml_config<T: DeserializeOwned>(
    config: &[u8],
    ctx: &str,
) -> TinyFSResult<(Value, T)> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8 in config")?;
    let value: Value = serde_yaml::from_str(config_str).map_other_context("Invalid YAML config")?;
    let typed: T = serde_json::from_value(value.clone()).map_other_context(ctx)?;
    Ok((value, typed))
}

/// Deserialize an already-parsed [`serde_json::Value`] into a typed config.
///
/// Used by factory `file`/`directory` functions. `ctx` is the error context used
/// when deserialization fails.
pub(crate) fn config_from_value<T: DeserializeOwned>(value: Value, ctx: &str) -> TinyFSResult<T> {
    serde_json::from_value(value).map_other_context(ctx)
}
