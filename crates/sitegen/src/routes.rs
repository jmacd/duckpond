// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Route expansion — turns a `SiteConfig` route tree + export results into
//! a flat list of `PageJob`s, one per output HTML file.
//!
//! Static routes produce one page. Template routes produce one page per unique
//! `$0` value from the linked export stage.

use crate::config::{RouteConfig, RouteType, SiteConfig};
use crate::shortcodes::{ExportContext, ExportedFile};
use std::collections::BTreeMap;

/// Metadata extracted from a content page's frontmatter.
#[derive(Debug, Clone)]
pub struct ContentPage {
    /// Page title (from frontmatter)
    pub title: String,
    /// URL slug (from frontmatter or derived from filename)
    pub slug: String,
    /// Sort weight (lower = higher in nav). Default: 100
    pub weight: i32,
    /// If true, page renders but does not appear in nav
    pub hidden: bool,
    /// Navigation section (for grouping in sidebar). Default: none
    pub section: Option<String>,
    /// Pond path to the source markdown file
    pub source_path: String,
}

/// All discovered content pages for one content stage, sorted by (weight, title).
#[derive(Debug, Clone)]
pub struct ContentContext {
    pub pages: Vec<ContentPage>,
}

/// One page to generate — the flattened output of route expansion.
#[derive(Debug, Clone)]
pub struct PageJob {
    /// Output path relative to dist/ (e.g., "params/Temperature.html")
    pub output_path: String,
    /// Pond path to the markdown template (e.g., "/etc/site/param.md")
    pub page_source: String,
    /// Capture group values ($0, $1, ...) — empty for static routes
    pub captures: Vec<String>,
    /// Exported data files for this page (empty for static routes)
    pub datafiles: Vec<ExportedFile>,
    /// Breadcrumb trail: (label, url)
    pub breadcrumbs: Vec<(String, String)>,
}

/// Expand the config route tree into a flat list of page jobs.
///
/// Template routes are expanded using export results: one page per unique
/// `$0` value in the matching export stage.
/// Content routes are expanded using content results: one page per matched file.
pub fn expand_routes(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
) -> Vec<PageJob> {
    let mut jobs = Vec::new();
    let base = config.site.base_url.trim_end_matches('/').to_string();
    let home_url = if base.is_empty() || base == "/" {
        "/".to_string()
    } else {
        format!("{}/", base)
    };
    let breadcrumbs = vec![("Home".to_string(), home_url)];

    for route in &config.routes {
        expand_route(route, "", &breadcrumbs, &base, exports, content, &mut jobs);
    }

    jobs
}

/// Recursively expand a single route node and its children.
fn expand_route(
    route: &RouteConfig,
    parent_path: &str,
    parent_breadcrumbs: &[(String, String)],
    base_url: &str,
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
    jobs: &mut Vec<PageJob>,
) {
    match route.route_type {
        RouteType::Static => {
            let url_path = if route.slug.is_empty() {
                parent_path.to_string()
            } else {
                format!("{}/{}", parent_path, route.slug)
            };

            let output_path = if url_path.is_empty() {
                "index.html".to_string()
            } else {
                format!("{}/index.html", url_path.trim_start_matches('/'))
            };

            let mut breadcrumbs = parent_breadcrumbs.to_vec();
            if !route.slug.is_empty() {
                let raw_url = if url_path.starts_with('/') {
                    url_path.clone()
                } else {
                    format!("/{}", url_path)
                };
                let url = prefix_base(base_url, &raw_url);
                breadcrumbs.push((route.name.clone(), url));
            }

            if let Some(ref page) = route.page {
                jobs.push(PageJob {
                    output_path,
                    page_source: page.clone(),
                    captures: vec![],
                    datafiles: vec![],
                    breadcrumbs: breadcrumbs.clone(),
                });
            }

            // Recurse into children
            for child in &route.routes {
                expand_route(child, &url_path, &breadcrumbs, base_url, exports, content, jobs);
            }
        }

        RouteType::Template => {
            let export_name = route.export.as_deref().unwrap_or("");
            let export_ctx = match exports.get(export_name) {
                Some(ctx) => ctx,
                None => {
                    log::warn!(
                        "Route '{}' references export '{}' which has no results",
                        route.name,
                        export_name
                    );
                    return;
                }
            };

            // One page per unique $0 value
            for (key, files) in &export_ctx.by_key {
                // Replace $0 in slug with actual value
                let slug = route.slug.replace("$0", key);
                let url_path = format!("{}/{}", parent_path, slug);
                let output_path = format!("{}.html", url_path.trim_start_matches('/'));

                let mut breadcrumbs = parent_breadcrumbs.to_vec();
                let raw_url = if url_path.starts_with('/') {
                    url_path.clone()
                } else {
                    format!("/{}", url_path)
                };
                let url = prefix_base(base_url, &raw_url);
                breadcrumbs.push((key.clone(), url));

                let captures = vec![key.clone()];

                if let Some(ref page) = route.page {
                    jobs.push(PageJob {
                        output_path,
                        page_source: page.clone(),
                        captures,
                        datafiles: files.clone(),
                        breadcrumbs: breadcrumbs.clone(),
                    });
                }

                // Recurse into children (rare for template routes, but supported)
                for child in &route.routes {
                    expand_route(child, &url_path, &breadcrumbs, base_url, exports, content, jobs);
                }
            }
        }

        RouteType::Content => {
            let content_name = route.content.as_deref().unwrap_or("");
            let content_ctx = match content.get(content_name) {
                Some(ctx) => ctx,
                None => {
                    log::warn!(
                        "Route '{}' references content '{}' which has no results",
                        route.name,
                        content_name
                    );
                    return;
                }
            };

            // One page per content file, sorted by (weight, title)
            for page in &content_ctx.pages {
                let url_path = if parent_path.is_empty() {
                    format!("/{}", page.slug)
                } else {
                    format!("{}/{}", parent_path, page.slug)
                };
                let output_path = format!("{}.html", url_path.trim_start_matches('/'));

                let mut breadcrumbs = parent_breadcrumbs.to_vec();
                let url = prefix_base(base_url, &url_path);
                breadcrumbs.push((page.title.clone(), url));

                jobs.push(PageJob {
                    output_path,
                    page_source: page.source_path.clone(),
                    captures: vec![],
                    datafiles: vec![],
                    breadcrumbs,
                });
            }

            // Content routes do not recurse — each page is a leaf
        }
    }
}

/// Prepend the base URL prefix to a path.
///
/// `base_url` is pre-trimmed of trailing `/` (empty string means root).
fn prefix_base(base_url: &str, path: &str) -> String {
    if base_url.is_empty() {
        path.to_string()
    } else {
        format!("{}{}", base_url, path)
    }
}

/// Collect all unique $0 values from an export context — used for navigation.
pub fn collection_keys(export: &ExportContext) -> Vec<String> {
    export.by_key.keys().cloned().collect()
}

/// Build the collections map for shortcode context (collection name → list of keys).
///
/// Merges export collections (keyed by $0 values) with content collections
/// (keyed by page titles, ordered by weight).
pub fn build_collections(
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
) -> BTreeMap<String, Vec<String>> {
    let mut collections: BTreeMap<String, Vec<String>> = exports
        .iter()
        .map(|(name, ctx)| (name.clone(), collection_keys(ctx)))
        .collect();

    // Content collections use titles (ordered by weight) as the values
    for (name, ctx) in content {
        let titles: Vec<String> = ctx
            .pages
            .iter()
            .filter(|p| !p.hidden)
            .map(|p| p.title.clone())
            .collect();
        collections.insert(name.clone(), titles);
    }

    collections
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn test_config() -> SiteConfig {
        SiteConfig {
            site: SiteMeta {
                title: "Test".to_string(),
                base_url: "/test/".to_string(),
            },
            content: vec![],
            exports: vec![ExportStage {
                name: "params".to_string(),
                pattern: "/reduced/single_param/*/*.series".to_string(),
                target_points: 1500,
            }],
            routes: vec![RouteConfig {
                name: "home".to_string(),
                route_type: RouteType::Static,
                slug: "".to_string(),
                page: Some("/etc/site/index.md".to_string()),
                export: None,
                content: None,
                routes: vec![RouteConfig {
                    name: "params".to_string(),
                    route_type: RouteType::Static,
                    slug: "params".to_string(),
                    page: Some("/etc/site/params.md".to_string()),
                    export: None,
                    content: None,
                    routes: vec![RouteConfig {
                        name: "param".to_string(),
                        route_type: RouteType::Template,
                        slug: "$0".to_string(),
                        page: Some("/etc/site/param.md".to_string()),
                        export: Some("params".to_string()),
                        content: None,
                        routes: vec![],
                    }],
                }],
            }],
            partials: BTreeMap::new(),
            static_assets: vec![],
        }
    }

    fn test_exports() -> BTreeMap<String, ExportContext> {
        let mut by_key = BTreeMap::new();
        by_key.insert(
            "Temperature".to_string(),
            vec![ExportedFile {
                path: "params/Temperature/data.parquet".to_string(),
                file: "data/Temperature/data.parquet".to_string(),
                captures: vec!["Temperature".to_string()],
                temporal: BTreeMap::new(),
                start_time: 0,
                end_time: 0,
            }],
        );
        by_key.insert(
            "DO".to_string(),
            vec![ExportedFile {
                path: "params/DO/data.parquet".to_string(),
                file: "data/DO/data.parquet".to_string(),
                captures: vec!["DO".to_string()],
                temporal: BTreeMap::new(),
                start_time: 0,
                end_time: 0,
            }],
        );

        let mut exports = BTreeMap::new();
        exports.insert("params".to_string(), ExportContext { by_key });
        exports
    }

    #[test]
    fn test_expand_static_route() {
        let config = test_config();
        let exports = test_exports();
        let content = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content);

        // Should have: index.html, params/index.html, params/Temperature.html, params/DO.html
        assert_eq!(jobs.len(), 4);
        assert_eq!(jobs[0].output_path, "index.html");
        assert_eq!(jobs[1].output_path, "params/index.html");
    }

    #[test]
    fn test_expand_template_route() {
        let config = test_config();
        let exports = test_exports();
        let content = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content);

        let template_jobs: Vec<_> = jobs.iter().filter(|j| !j.captures.is_empty()).collect();
        assert_eq!(template_jobs.len(), 2);

        let paths: Vec<_> = template_jobs
            .iter()
            .map(|j| j.output_path.as_str())
            .collect();
        assert!(paths.contains(&"params/DO.html"));
        assert!(paths.contains(&"params/Temperature.html"));
    }

    #[test]
    fn test_breadcrumbs() {
        let config = test_config();
        let exports = test_exports();
        let content = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content);

        // Template route should have: Home > params > Temperature
        let temp_job = jobs
            .iter()
            .find(|j| j.output_path == "params/Temperature.html")
            .unwrap();
        assert_eq!(temp_job.breadcrumbs.len(), 3);
        assert_eq!(temp_job.breadcrumbs[0].0, "Home");
        assert_eq!(temp_job.breadcrumbs[1].0, "params");
        assert_eq!(temp_job.breadcrumbs[2].0, "Temperature");
    }

    #[test]
    fn test_build_collections() {
        let exports = test_exports();
        let content = BTreeMap::new();
        let collections = build_collections(&exports, &content);
        assert_eq!(collections.len(), 1);
        let params = collections.get("params").unwrap();
        assert!(params.contains(&"Temperature".to_string()));
        assert!(params.contains(&"DO".to_string()));
    }

    #[test]
    fn test_expand_content_route() {
        let mut content = BTreeMap::new();
        content.insert(
            "pages".to_string(),
            ContentContext {
                pages: vec![
                    ContentPage {
                        title: "Water".to_string(),
                        slug: "water".to_string(),
                        weight: 10,
                        hidden: false,
                        section: None,
                        source_path: "/pages/water.md".to_string(),
                    },
                    ContentPage {
                        title: "History".to_string(),
                        slug: "history".to_string(),
                        weight: 30,
                        hidden: false,
                        section: None,
                        source_path: "/pages/history.md".to_string(),
                    },
                ],
            },
        );

        let config = SiteConfig {
            site: SiteMeta {
                title: "Test".to_string(),
                base_url: "/".to_string(),
            },
            content: vec![],
            exports: vec![],
            routes: vec![RouteConfig {
                name: "home".to_string(),
                route_type: RouteType::Static,
                slug: "".to_string(),
                page: Some("/etc/site/index.md".to_string()),
                export: None,
                content: None,
                routes: vec![RouteConfig {
                    name: "pages".to_string(),
                    route_type: RouteType::Content,
                    slug: "".to_string(),
                    page: None,
                    export: None,
                    content: Some("pages".to_string()),
                    routes: vec![],
                }],
            }],
            partials: BTreeMap::new(),
            static_assets: vec![],
        };

        let exports = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content);

        // index.html + water.html + history.html
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].output_path, "index.html");
        assert_eq!(jobs[1].output_path, "water.html");
        assert_eq!(jobs[1].page_source, "/pages/water.md");
        assert_eq!(jobs[2].output_path, "history.html");

        // Breadcrumbs: Home > Water
        assert_eq!(jobs[1].breadcrumbs.len(), 2);
        assert_eq!(jobs[1].breadcrumbs[1].0, "Water");
    }

    #[test]
    fn test_content_collections() {
        let mut content = BTreeMap::new();
        content.insert(
            "pages".to_string(),
            ContentContext {
                pages: vec![
                    ContentPage {
                        title: "Water".to_string(),
                        slug: "water".to_string(),
                        weight: 10,
                        hidden: false,
                        section: None,
                        source_path: "/pages/water.md".to_string(),
                    },
                    ContentPage {
                        title: "Secret".to_string(),
                        slug: "secret".to_string(),
                        weight: 50,
                        hidden: true,
                        section: None,
                        source_path: "/pages/secret.md".to_string(),
                    },
                ],
            },
        );

        let exports = BTreeMap::new();
        let collections = build_collections(&exports, &content);
        let pages = collections.get("pages").unwrap();
        assert_eq!(pages.len(), 1); // hidden page excluded
        assert_eq!(pages[0], "Water");
    }
}
