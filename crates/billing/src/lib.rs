// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Caspar Water bookkeeping (billing v2).
//!
//! Tiny double-entry bookkeeping system on top of TinyFS / TLogFS / Steward.
//! Operator-facing CLI is the primary interface, exposed as the executable
//! factory `accounts`.
//!
//! See `crates/billing/README.md` and `docs/billing-v2.md` for design and
//! usage.

pub mod chart;
pub mod cli;
pub mod dates;
pub mod domain;
pub mod factory;
pub mod lookup;
pub mod money;
pub mod policy;
pub mod schema;
pub mod store;
