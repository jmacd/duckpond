// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! # Sitegen — Static site generator for DuckPond
//!
//! Replaces the Tera-based TemplateFactory with a Markdown + Maud pipeline.
//! Uses pulldown-cmark for markdown rendering and Maud for HTML layouts.
//!
//! ## Usage
//!
//! ```bash
//! pond run /etc/site.yaml build ./dist
//! ```

mod config;
mod factory;
mod layouts;
pub mod markdown;
mod routes;
mod shortcodes;

pub use config::SiteConfig;
