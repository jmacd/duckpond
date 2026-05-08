// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! CLI subcommand handlers.
//!
//! Each module implements one top-level subcommand group from
//! `factory::AccountsSub`. Handlers receive a `&WD` (root of the pond) and
//! the parsed clap args; they perform their reads/writes via `store::*`
//! helpers, then return `Result<()>`.
//!
//! Output is via `log::info!` (which env_logger renders to stderr by
//! default; the workspace lints forbid `println!` / `eprintln!`).

pub mod adjust;
pub mod bills;
pub mod business;
pub mod connection;
pub mod customer;
pub mod cycle;
pub mod expense;
pub mod journal;
pub mod opening;
pub mod payment;
pub mod policy;
pub mod reports;
pub mod tenancy;
pub mod verify;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    TinyFS(#[from] tinyfs::Error),
    #[error("{0}")]
    Lookup(#[from] crate::lookup::LookupError),
    #[error("{0}")]
    Date(#[from] crate::dates::DateParseError),
    #[error("invalid input: {0}")]
    Invalid(String),
}

impl From<CliError> for tlogfs::TLogFSError {
    fn from(e: CliError) -> Self {
        match e {
            CliError::TinyFS(e) => tlogfs::TLogFSError::TinyFS(e),
            other => tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(other.to_string())),
        }
    }
}

pub type Result<T> = std::result::Result<T, CliError>;
