// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Force-link external factory crates so their `linkme` distributed slice
// entries are included. Without this, `FactoryRegistry` and `SchemeRegistry`
// won't see factories registered in hydrovu, sitegen, or remote crates
// when running lib tests.
use hydrovu as _;
use remote as _;
use sitegen as _;

pub mod commands;
pub mod common;
pub mod error_utils;
pub mod template_utils;
