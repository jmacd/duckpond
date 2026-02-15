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
pub fn expand_routes(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
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
        expand_route(route, "", &breadcrumbs, &base, exports, &mut jobs);
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
                expand_route(child, &url_path, &breadcrumbs, base_url, exports, jobs);
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
                    expand_route(child, &url_path, &breadcrumbs, base_url, exports, jobs);
                }
            }
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
pub fn build_collections(
    exports: &BTreeMap<String, ExportContext>,
) -> BTreeMap<String, Vec<String>> {
    exports
        .iter()
        .map(|(name, ctx)| (name.clone(), collection_keys(ctx)))
        .collect()
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
                routes: vec![RouteConfig {
                    name: "params".to_string(),
                    route_type: RouteType::Static,
                    slug: "params".to_string(),
                    page: Some("/etc/site/params.md".to_string()),
                    export: None,
                    routes: vec![RouteConfig {
                        name: "param".to_string(),
                        route_type: RouteType::Template,
                        slug: "$0".to_string(),
                        page: Some("/etc/site/param.md".to_string()),
                        export: Some("params".to_string()),
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
        let jobs = expand_routes(&config, &exports);

        // Should have: index.html, params/index.html, params/Temperature.html, params/DO.html
        assert_eq!(jobs.len(), 4);
        assert_eq!(jobs[0].output_path, "index.html");
        assert_eq!(jobs[1].output_path, "params/index.html");
    }

    #[test]
    fn test_expand_template_route() {
        let config = test_config();
        let exports = test_exports();
        let jobs = expand_routes(&config, &exports);

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
        let jobs = expand_routes(&config, &exports);

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
        let collections = build_collections(&exports);
        assert_eq!(collections.len(), 1);
        let params = collections.get("params").unwrap();
        assert!(params.contains(&"Temperature".to_string()));
        assert!(params.contains(&"DO".to_string()));
    }
}
