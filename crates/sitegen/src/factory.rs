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
use crate::markdown::{preprocess_shortcodes, render_markdown};
use crate::routes::{self, ContentContext, ContentPage};
use crate::shortcodes::{self, ExportContext, ShortcodeContext};
use log::{debug, info, warn};
use provider::{ExecutionContext, FactoryContext, register_executable_factory};
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
    let cmd =
        SitegenCommand::try_parse_from(args).map_err(|e| tinyfs::Error::Other(format!("{}", e)))?;

    match cmd.command {
        SitegenSubcommand::Build { output_dir } => {
            let output_path = std::path::PathBuf::from(&output_dir);

            // Run export stages: glob-match pond files, export as Hive-partitioned
            // parquet, scan output for real files with start_time/end_time.
            let exports = run_export_stages(&config, &context, &output_path).await?;

            // Run content stages: glob-match markdown data files, parse frontmatter.
            let content = run_content_stages(&config, &context).await?;

            // Pre-read all files from the pond into a map.
            // We cannot use block_on inside this async context, so we collect
            // everything we need up front.
            let fs = context.context.filesystem();
            let root = fs.root().await?;

            let mut file_cache: BTreeMap<String, String> = BTreeMap::new();

            // Collect all page paths from route expansion
            let jobs = routes::expand_routes(&config, &exports, &content);
            for job in &jobs {
                if !file_cache.contains_key(&job.page_source) {
                    let data = root
                        .read_file_path_to_vec(&job.page_source)
                        .await
                        .map_err(|e| {
                            tinyfs::Error::Other(format!(
                                "Cannot read page '{}': {}",
                                job.page_source, e
                            ))
                        })?;
                    let text = String::from_utf8(data).map_err(|e| {
                        tinyfs::Error::Other(format!("Non-UTF8 page '{}': {}", job.page_source, e))
                    })?;
                    file_cache.insert(job.page_source.clone(), text);
                }
            }

            // Collect partials
            for path in config.partials.values() {
                if !file_cache.contains_key(path) {
                    match root.read_file_path_to_vec(path).await {
                        Ok(data) => {
                            if let Ok(text) = String::from_utf8(data) {
                                file_cache.insert(path.clone(), text);
                            }
                        }
                        Err(e) => {
                            warn!("Cannot read partial '{}': {}", path, e);
                        }
                    }
                }
            }

            // Collect static assets
            for asset in &config.static_assets {
                if !file_cache.contains_key(&asset.pattern) {
                    match root.read_file_path_to_vec(&asset.pattern).await {
                        Ok(data) => {
                            if let Ok(text) = String::from_utf8(data) {
                                file_cache.insert(asset.pattern.clone(), text);
                            }
                        }
                        Err(e) => {
                            warn!("Cannot read static asset '{}': {}", asset.pattern, e);
                        }
                    }
                }
            }

            // Simple lookup closure — no async needed
            let read_pond_file = |path: &str| -> Result<String, String> {
                file_cache
                    .get(path)
                    .cloned()
                    .ok_or_else(|| format!("File not in cache: {}", path))
            };

            generate_site(&config, &exports, &content, &read_pond_file, &output_path)
                .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

            // Copy static assets from pond
            copy_static_assets(&config, &read_pond_file, &output_path)?;

            // Write built-in assets (style.css, chart.js)
            write_builtin_assets(&output_path)?;

            Ok(())
        }
    }
}

/// Run export stages: glob-match pond files, export as Hive-partitioned parquet,
/// scan output for real files with start_time/end_time.
///
/// Uses `provider::export::export_series_to_parquet` — the same DataFusion
/// `COPY TO ... PARTITIONED BY` pipeline used by `pond export`.
///
/// # Automatic Partitioning
///
/// For each stage, the export:
///
/// 1. Discovers all resolutions from glob matches (looking for `res=Xh` in
///    capture groups).
/// 2. Computes per-resolution partition plans using
///    `partitions::compute_partitions()` — each resolution gets the finest
///    temporal partition that satisfies the ≤ 2 file invariant for its
///    maximum viewport width.
/// 3. Exports each match with its resolution-specific temporal partition.
async fn run_export_stages(
    config: &SiteConfig,
    context: &FactoryContext,
    output_dir: &std::path::Path,
) -> Result<BTreeMap<String, ExportContext>, tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;
    let provider_ctx = &context.context;
    let data_dir = output_dir.join("data");
    let mut exports = BTreeMap::new();

    for stage in &config.exports {
        let matches = root.collect_matches(&stage.pattern).await?;
        info!(
            "Export stage '{}': {} matches for '{}'",
            stage.name,
            matches.len(),
            stage.pattern
        );

        let target_points = stage.target_points;
        let display = crate::partitions::DisplayConfig { target_points };

        // Discover resolutions from matches
        let mut resolutions = Vec::new();
        for (_node_path, captures) in &matches {
            if let Some(res) = crate::partitions::extract_resolution_from_captures(captures)
                && !resolutions.contains(&res)
            {
                resolutions.push(res);
            }
        }

        // Compute per-resolution partition plans
        let partition_plans = if resolutions.is_empty() {
            warn!(
                "Export stage '{}': no resolutions found in captures. \
                 Using default temporal partition ['year'].",
                stage.name
            );
            None
        } else {
            // max_width = coarsest_resolution * target_points
            // This is the widest viewport the coarsest resolution will serve.
            let mut max_res_secs = 0u64;
            for res in &resolutions {
                if let Some(secs) = crate::partitions::parse_duration_secs(res) {
                    max_res_secs = max_res_secs.max(secs);
                }
            }
            let max_width_secs = max_res_secs * target_points;

            let (partitions, warnings) =
                crate::partitions::compute_partitions(&resolutions, &display, max_width_secs);

            for w in &warnings {
                warn!("Export stage '{}': {}", stage.name, w);
            }

            info!(
                "Export stage '{}': auto-partitioning {} resolutions ({:?})",
                stage.name,
                partitions.len(),
                resolutions,
            );
            for (res, plan) in &partitions {
                info!(
                    "  {} → {:?} (partition ≈ {}s, max {} pts/file)",
                    res, plan.temporal, plan.partition_width_secs, plan.max_points_per_file
                );
            }

            Some(partitions)
        };

        let mut by_key: BTreeMap<String, Vec<shortcodes::ExportedFile>> = BTreeMap::new();

        for (node_path, captures) in &matches {
            let path_str = node_path.path.to_string_lossy().to_string();
            let key = captures.first().cloned().unwrap_or_default();

            let rel = series_path_to_data_rel(&path_str);
            let export_dir = data_dir.join(&rel);

            // Look up resolution-specific temporal partition
            let temporal_parts: Vec<String> = if let Some(ref partitions) = partition_plans {
                if let Some(res) = crate::partitions::extract_resolution_from_captures(captures) {
                    if let Some(plan) = partitions.get(&res) {
                        plan.temporal.clone()
                    } else {
                        warn!(
                            "  {} — resolution '{}' not in partition plan, using ['year']",
                            path_str, res
                        );
                        vec!["year".to_string()]
                    }
                } else {
                    vec!["year".to_string()]
                }
            } else {
                // No resolutions discovered — single partition
                vec!["year".to_string()]
            };

            let (export_outputs, _schema) = provider::export::export_series_to_parquet(
                &root,
                &path_str,
                &export_dir,
                &temporal_parts,
                captures,
                &data_dir,
                provider_ctx,
            )
            .await
            .map_err(|e| {
                tinyfs::Error::Other(format!("export_series_to_parquet '{}': {}", path_str, e))
            })?;

            for (_caps, export_output) in &export_outputs {
                let mut temporal = BTreeMap::new();
                for component in export_output.file.components() {
                    let s = component.as_os_str().to_string_lossy();
                    if let Some((k, v)) = s.split_once('=')
                        && temporal_parts.contains(&k.to_string())
                    {
                        temporal.insert(k.to_string(), v.to_string());
                    }
                }

                let base = config.site.base_url.trim_end_matches('/');
                by_key
                    .entry(key.clone())
                    .or_default()
                    .push(shortcodes::ExportedFile {
                        path: path_str.clone(),
                        file: format!("{}/data/{}", base, export_output.file.to_string_lossy()),
                        captures: captures.clone(),
                        temporal,
                        start_time: export_output.start_time.unwrap_or(0),
                        end_time: export_output.end_time.unwrap_or(0),
                    });
            }

            info!(
                "  {} → {} partitioned files ({:?})",
                path_str,
                export_outputs.len(),
                temporal_parts,
            );
        }

        exports.insert(stage.name.clone(), ExportContext { by_key });
    }

    Ok(exports)
}

/// Run content stages: glob-match markdown data files, parse frontmatter for metadata.
///
/// Each content stage globs for data files in the pond, reads their frontmatter
/// to extract title/weight/slug/hidden, and returns a `ContentContext` with pages
/// sorted by (weight, title).
async fn run_content_stages(
    config: &SiteConfig,
    context: &FactoryContext,
) -> Result<BTreeMap<String, ContentContext>, tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;
    let mut content = BTreeMap::new();

    for stage in &config.content {
        let matches = root.collect_matches(&stage.pattern).await?;
        info!(
            "Content stage '{}': {} matches for '{}'",
            stage.name,
            matches.len(),
            stage.pattern
        );

        let mut pages = Vec::new();
        for (node_path, _captures) in &matches {
            let path_str = node_path.path.to_string_lossy().to_string();

            // Read the file to extract frontmatter
            let data = root.read_file_path_to_vec(&path_str).await.map_err(|e| {
                tinyfs::Error::Other(format!(
                    "Cannot read content file '{}': {}",
                    path_str, e
                ))
            })?;
            let text = String::from_utf8(data).map_err(|e| {
                tinyfs::Error::Other(format!("Non-UTF8 content file '{}': {}", path_str, e))
            })?;

            let (fm_yaml, _body) = split_frontmatter(&text);
            let fm: Frontmatter = if fm_yaml.is_empty() {
                Frontmatter {
                    title: slug_from_path(&path_str),
                    layout: default_layout(),
                    weight: default_weight(),
                    slug: None,
                    hidden: false,
                    section: None,
                }
            } else {
                serde_yaml::from_str(&fm_yaml).map_err(|e| {
                    tinyfs::Error::Other(format!(
                        "Bad frontmatter in content file '{}': {}",
                        path_str, e
                    ))
                })?
            };

            let slug = fm
                .slug
                .unwrap_or_else(|| slug_from_path(&path_str));

            pages.push(ContentPage {
                title: if fm.title.is_empty() {
                    slug.clone()
                } else {
                    fm.title
                },
                slug,
                weight: fm.weight,
                hidden: fm.hidden,
                section: fm.section,
                source_path: path_str,
            });
        }

        // Sort by (weight, title) for stable ordering
        pages.sort_by(|a, b| a.weight.cmp(&b.weight).then_with(|| a.title.cmp(&b.title)));

        info!(
            "Content stage '{}': {} pages resolved",
            stage.name,
            pages.len()
        );

        content.insert(stage.name.clone(), ContentContext { pages });
    }

    Ok(content)
}

/// Derive a URL slug from a pond path (filename without extension).
///
/// e.g. "/pages/water-system.md" → "water-system"
fn slug_from_path(path: &str) -> String {
    let filename = path.rsplit('/').next().unwrap_or(path);
    filename
        .strip_suffix(".md")
        .or_else(|| filename.strip_suffix(".markdown"))
        .unwrap_or(filename)
        .to_string()
}

/// Copy static assets from pond to output directory.
fn copy_static_assets(
    config: &SiteConfig,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
    output_dir: &Path,
) -> Result<(), tinyfs::Error> {
    for asset in &config.static_assets {
        // For static assets, read from pond and write to output
        // The pattern is a literal path like "/etc/static/style.css"
        match read_pond_file(&asset.pattern) {
            Ok(content) => {
                // Strip leading /etc/static/ to get relative output path
                let rel = asset
                    .pattern
                    .strip_prefix("/etc/static/")
                    .unwrap_or(&asset.pattern);
                let out_path = output_dir.join(rel);
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| tinyfs::Error::Other(format!("mkdir {:?}: {}", parent, e)))?;
                }
                std::fs::write(&out_path, content.as_bytes())
                    .map_err(|e| tinyfs::Error::Other(format!("write {:?}: {}", out_path, e)))?;
                debug!("copied static asset: {}", rel);
            }
            Err(e) => {
                warn!("Cannot read static asset '{}': {}", asset.pattern, e);
            }
        }
    }
    Ok(())
}

/// Write built-in assets (style.css, chart.js) to the output directory.
///
/// These are compiled into the binary via `include_str!` so sitegen always
/// produces a self-contained site without requiring files in the pond.
fn write_builtin_assets(output_dir: &Path) -> Result<(), tinyfs::Error> {
    static STYLE_CSS: &str = include_str!("../assets/style.css");
    static CHART_JS: &str = include_str!("../assets/chart.js");

    for (name, content) in [("style.css", STYLE_CSS), ("chart.js", CHART_JS)] {
        let path = output_dir.join(name);
        std::fs::write(&path, content.as_bytes())
            .map_err(|e| tinyfs::Error::Other(format!("write {:?}: {}", path, e)))?;
        debug!("wrote built-in asset: {}", name);
    }
    Ok(())
}

/// Convert a pond series path to a relative directory path for export.
///
/// e.g. "/reduced/single_param/Temperature/res=1h.series"
///    → "single_param/Temperature/res=1h"
///
/// The first path component (e.g. "reduced") is stripped, and the
/// `.series` extension is removed. The result is used as a subdirectory
/// under the `data/` output directory.
fn series_path_to_data_rel(pond_path: &str) -> String {
    let stripped = pond_path.trim_start_matches('/');
    let parts: Vec<&str> = stripped.splitn(2, '/').collect();
    let rel = if parts.len() > 1 { parts[1] } else { stripped };
    rel.strip_suffix(".series").unwrap_or(rel).to_string()
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
    /// Sort weight for content pages (lower = higher in nav). Default: 100
    #[serde(default = "default_weight")]
    weight: i32,
    /// URL slug override. If absent, derived from filename.
    #[serde(default)]
    slug: Option<String>,
    /// If true, page renders but does not appear in navigation.
    #[serde(default)]
    hidden: bool,
    /// Navigation section for grouping (e.g., "About", "Blog").
    #[serde(default)]
    section: Option<String>,
}

fn default_layout() -> String {
    "default".to_string()
}

fn default_weight() -> i32 {
    100
}

/// Generate the complete static site.
fn generate_site(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
    output_dir: &Path,
) -> Result<(), GenerateError> {
    // Expand the route tree into page jobs
    let jobs = routes::expand_routes(config, exports, content);
    info!("Route expansion: {} pages to generate", jobs.len());

    // Build collections for navigation shortcodes
    let collections = routes::build_collections(exports, content);

    // Build content_pages map for content_nav shortcode
    let content_pages: BTreeMap<String, Vec<ContentPage>> = content
        .iter()
        .map(|(name, ctx)| (name.clone(), ctx.pages.clone()))
        .collect();

    // Render each page
    for job in &jobs {
        // Render sidebar per-page so nav_list can highlight the active link
        let base = config.site.base_url.trim_end_matches('/');
        let current_path = if base.is_empty() || base == "/" {
            format!("/{}", job.output_path)
        } else {
            format!("{}/{}", base, job.output_path)
        };
        let sidebar_html = render_partial(
            config,
            "sidebar",
            read_pond_file,
            &collections,
            &content_pages,
            &current_path,
        );
        let raw_md = read_pond_file(&job.page_source)
            .map_err(|e| GenerateError(format!("Cannot read '{}': {}", job.page_source, e)))?;

        let (fm_yaml, body) = split_frontmatter(&raw_md);
        let fm: Frontmatter = if fm_yaml.is_empty() {
            Frontmatter {
                title: job.page_source.clone(),
                layout: default_layout(),
                weight: default_weight(),
                slug: None,
                hidden: false,
                section: None,
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
            content_pages: content_pages.clone(),
            site_title: config.site.title.clone(),
            current_path: current_path.clone(),
            breadcrumbs: job.breadcrumbs.clone(),
            base_url: config.site.base_url.clone(),
        });

        // Rewrite {{ $0 }} → {{ cap0 }}, nav-list → nav_list, etc.
        let preprocessed = shortcodes::preprocess_variables(&body);

        // Expand shortcodes
        let sc = shortcodes::register_shortcodes(sc_ctx);
        let expanded =
            preprocess_shortcodes(&preprocessed, &sc, Some(&job.page_source)).map_err(|e| {
                GenerateError(format!("Shortcode error in '{}': {}", job.page_source, e))
            })?;

        // Render markdown → HTML
        let content_html = render_markdown(&expanded);

        // Wrap in layout
        let full_html = layouts::apply_layout(
            &fm.layout,
            &LayoutContext {
                title: &title,
                site_title: &config.site.title,
                content: &content_html,
                sidebar: sidebar_html.as_deref(),
            },
        );

        // Write output file
        let out_path = output_dir.join(&job.output_path);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| GenerateError(format!("mkdir {:?}: {}", parent, e)))?;
        }
        std::fs::write(&out_path, full_html.as_bytes())
            .map_err(|e| GenerateError(format!("write {:?}: {}", out_path, e)))?;

        debug!("wrote {}", job.output_path);
    }

    info!(
        "Site generation complete: {} pages to {:?}",
        jobs.len(),
        output_dir
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn render_partial(
    config: &SiteConfig,
    name: &str,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
    collections: &BTreeMap<String, Vec<String>>,
    content_pages: &BTreeMap<String, Vec<ContentPage>>,
    current_path: &str,
) -> Option<String> {
    let path = config.partials.get(name)?;
    match read_pond_file(path) {
        Ok(md) => {
            // Build a shortcode context for the partial — includes current_path
            // so nav_list can highlight the active page link.
            let sc_ctx = Arc::new(ShortcodeContext {
                captures: vec![],
                datafiles: vec![],
                collections: collections.clone(),
                content_pages: content_pages.clone(),
                site_title: config.site.title.clone(),
                current_path: current_path.to_string(),
                breadcrumbs: vec![],
                base_url: config.site.base_url.clone(),
            });
            let preprocessed = shortcodes::preprocess_variables(&md);
            let sc = shortcodes::register_shortcodes(sc_ctx);
            let expanded =
                preprocess_shortcodes(&preprocessed, &sc, Some(path)).unwrap_or(preprocessed);
            Some(render_markdown(&expanded))
        }
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
        (
            after[..end].trim().to_string(),
            after[end + 4..].to_string(),
        )
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

    #[test]
    fn test_slug_from_path() {
        assert_eq!(slug_from_path("/pages/water-system.md"), "water-system");
        assert_eq!(slug_from_path("/content/history.md"), "history");
        assert_eq!(slug_from_path("readme.markdown"), "readme");
        assert_eq!(slug_from_path("/etc/site/index.md"), "index");
    }

    #[test]
    fn test_frontmatter_with_weight() {
        let yaml = "title: Water\nweight: 10\nhidden: true";
        let fm: Frontmatter = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(fm.title, "Water");
        assert_eq!(fm.weight, 10);
        assert!(fm.hidden);
        assert!(fm.slug.is_none());
        assert_eq!(fm.layout, "default");
    }
}
