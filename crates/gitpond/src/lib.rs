// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Git-Ingest Factory
//!
//! Pulls files from a git repository branch into the pond filesystem.
//! Maintains a bare repo cache at `{POND}/git/{node-id}.git/` for
//! efficient incremental fetches. Tracks synced state via a manifest
//! file alongside the bare repo.

mod factory;
mod git;
mod sync;
