// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Sitegen factory — registered as an executable factory, invoked via `pond run`.
//!
//! ```bash
//! pond run /etc/site.yaml build ./dist
//! ```
//!
//! The execute function:
//! 1. Parses the site.yaml config from the pond
//! 2. Runs export stages to produce ExportContext per stage
//! 3. Expands the route tree into page jobs
//! 4. Renders each page: markdown → shortcodes → HTML → layout
//! 5. Writes the complete site to the output directory

use crate::config::SiteConfig;
use crate::layouts::{self, LayoutContext};
use crate::routes;
use crate::shortcodes::{self, ExportContext, ShortcodeContext};
use log::{debug, info, warn};
use maudit::content::markdown::shortcodes::preprocess_shortcodes;
use maudit::content::render_markdown;
use provider::{register_executable_factory, ExecutionContext, FactoryContext};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Clap command parsing
// ---------------------------------------------------------------------------

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct SitegenCommand {
    #[command(subcommand)]
    command: SitegenSubcommand,
}

#[derive(Debug, Subcommand)]
enum SitegenSubcommand {
    /// Build the complete static site
    Build {
        /// Output directory for generated files
        output_dir: String,
    },
}

// ---------------------------------------------------------------------------
// Factory registration
// ---------------------------------------------------------------------------

fn validate_config(config: &[u8]) -> tinyfs::Result<Value> {
    let config: SiteConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid site.yaml: {}", e)))?;

    serde_json::to_value(&config)
        .map_err(|e| tinyfs::Error::Other(format!("Config serialization error: {}", e)))
}

async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    Ok(())
}

async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: SiteConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Parse command
    let args: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();
    let cmd = SitegenCommand::try_parse_from(args)
        .map_err(|e| tinyfs::Error::Other(format!("{}", e)))?;

    match cmd.command {
        SitegenSubcommand::Build { output_dir } => {
            let output_path = std::path::PathBuf::from(&output_dir);

            // TODO: Run export stages using pond export infrastructure.
            // For now, exports is empty — the factory will wire this up
            // once we integrate with the existing export system.
            let exports: BTreeMap<String, ExportContext> = BTreeMap::new();

            // Build a closure that reads files from the pond
            let read_pond_file = |path: &str| -> Result<String, String> {
                let fs = context.context.filesystem();
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    let root = fs.root().await.map_err(|e| e.to_string())?;
                    let data = root.read_file_path_to_vec(path).await.map_err(|e| e.to_string())?;
                    String::from_utf8(data).map_err(|e| e.to_string())
                })
            };

            generate_site(&config, &exports, &read_pond_file, &output_path)
                .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

            Ok(())
        }
    }
}

register_executable_factory!(
    name: "sitegen",
    description: "Static site generator — Markdown + Maud templates powered by Maudit",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

// ---------------------------------------------------------------------------
// Site generation
// ---------------------------------------------------------------------------

/// Frontmatter parsed from the top of each markdown page.
#[derive(Debug, Deserialize)]
struct Frontmatter {
    #[serde(default)]
    title: String,
    #[serde(default = "default_layout")]
    layout: String,
}

fn default_layout() -> String {
    "default".to_string()
}

/// Generate the complete static site.
fn generate_site(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
    output_dir: &Path,
) -> Result<(), GenerateError> {
    // Expand the route tree into page jobs
    let jobs = routes::expand_routes(config, exports);
    info!("Route expansion: {} pages to generate", jobs.len());

    // Build collections for navigation shortcodes
    let collections = routes::build_collections(exports);

    // Render sidebar partial if configured
    let sidebar_html = render_partial(config, "sidebar", read_pond_file);

    // Render each page
    for job in &jobs {
        let raw_md = read_pond_file(&job.page_source).map_err(|e| {
            GenerateError(format!("Cannot read '{}': {}", job.page_source, e))
        })?;

        let (fm_yaml, body) = split_frontmatter(&raw_md);
        let fm: Frontmatter = if fm_yaml.is_empty() {
            Frontmatter {
                title: job.page_source.clone(),
                layout: default_layout(),
            }
        } else {
            serde_yaml::from_str(&fm_yaml).map_err(|e| {
                GenerateError(format!("Bad frontmatter in '{}': {}", job.page_source, e))
            })?
        };

        // Interpolate capture groups into the title
        let title = interpolate_captures(&fm.title, &job.captures);

        // Build shortcode context for this page
        let sc_ctx = Arc::new(ShortcodeContext {
            captures: job.captures.clone(),
            datafiles: job.datafiles.clone(),
            collections: collections.clone(),
            site_title: config.site.title.clone(),
            current_path: format!("/{}", job.output_path),
            breadcrumbs: job.breadcrumbs.clone(),
        });

        // Rewrite {{ $0 }} → {{ cap0 }}, nav-list → nav_list, etc.
        let preprocessed = shortcodes::preprocess_variables(&body);

        // Expand shortcodes
        let sc = shortcodes::register_shortcodes(sc_ctx);
        let expanded = preprocess_shortcodes(&preprocessed, &sc, None, Some(&job.page_source))
            .map_err(|e| {
                GenerateError(format!("Shortcode error in '{}': {}", job.page_source, e))
            })?;

        // Render markdown → HTML
        let content_html = render_markdown(&expanded, None, None, None);

        // Wrap in layout
        let full_html = layouts::apply_layout(
            &fm.layout,
            &LayoutContext {
                title: &title,
                site_title: &config.site.title,
                base_url: &config.site.base_url,
                content: &content_html,
                sidebar: sidebar_html.as_deref(),
            },
        );

        // Write output file
        let out_path = output_dir.join(&job.output_path);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                GenerateError(format!("mkdir {:?}: {}", parent, e))
            })?;
        }
        std::fs::write(&out_path, full_html.as_bytes()).map_err(|e| {
            GenerateError(format!("write {:?}: {}", out_path, e))
        })?;

        debug!("wrote {}", job.output_path);
    }

    info!("Site generation complete: {} pages to {:?}", jobs.len(), output_dir);
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn render_partial(
    config: &SiteConfig,
    name: &str,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
) -> Option<String> {
    let path = config.partials.get(name)?;
    match read_pond_file(path) {
        Ok(md) => Some(render_markdown(&md, None, None, None)),
        Err(e) => {
            warn!("Cannot read partial '{}' from '{}': {}", name, path, e);
            None
        }
    }
}

fn interpolate_captures(text: &str, captures: &[String]) -> String {
    let mut result = text.to_string();
    for (i, val) in captures.iter().enumerate() {
        result = result.replace(&format!("{{{{ ${} }}}}", i), val);
    }
    result
}

fn split_frontmatter(content: &str) -> (String, String) {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return (String::new(), content.to_string());
    }
    let after = &trimmed[3..];
    if let Some(end) = after.find("\n---") {
        (after[..end].trim().to_string(), after[end + 4..].to_string())
    } else {
        (String::new(), content.to_string())
    }
}

#[derive(Debug)]
struct GenerateError(String);

impl std::fmt::Display for GenerateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GenerateError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_frontmatter() {
        let (fm, body) = split_frontmatter("---\ntitle: Hi\n---\n\n# Body");
        assert_eq!(fm, "title: Hi");
        assert!(body.contains("# Body"));
    }

    #[test]
    fn test_split_frontmatter_none() {
        let (fm, body) = split_frontmatter("# Just markdown");
        assert!(fm.is_empty());
        assert_eq!(body, "# Just markdown");
    }

    #[test]
    fn test_interpolate_captures() {
        assert_eq!(
            interpolate_captures("{{ $0 }} data", &["Temperature".to_string()]),
            "Temperature data"
        );
    }

    #[test]
    fn test_interpolate_empty() {
        assert_eq!(interpolate_captures("Static", &[]), "Static");
    }
}
