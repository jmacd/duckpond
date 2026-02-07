// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Site configuration — parsed from `/etc/site.yaml` inside the pond.

use serde::{Deserialize, Serialize};

/// Top-level site configuration.
///
/// ```yaml
/// site:
///   title: "Noyo Harbor Blue Economy"
///   base_url: "/"
///
/// exports:
///   - name: "params"
///     pattern: "/reduced/single_param/*/*.series"
///     temporal: ["year", "month"]
///
/// routes:
///   - name: "home"
///     type: static
///     slug: ""
///     page: "/etc/site/index.md"
///     routes: [...]
///
/// partials:
///   sidebar: "/etc/site/sidebar.md"
///
/// static:
///   - pattern: "/etc/static/*"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteConfig {
    pub site: SiteMeta,
    #[serde(default)]
    pub exports: Vec<ExportStage>,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub partials: std::collections::BTreeMap<String, String>,
    #[serde(rename = "static", default)]
    pub static_assets: Vec<StaticAsset>,
}

/// Site-wide metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteMeta {
    pub title: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
}

fn default_base_url() -> String {
    "/".to_string()
}

/// An export stage: runs `pond export` internally to produce data files
/// that become template context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportStage {
    /// Name referenced by routes (e.g., "params", "sites")
    pub name: String,
    /// Pond glob pattern (e.g., "/reduced/single_param/*/*.series")
    pub pattern: String,
    /// Temporal partition keys (e.g., ["year", "month"])
    #[serde(default)]
    pub temporal: Vec<String>,
}

/// A route in the hierarchical route tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    /// Route name (for logging/debugging)
    pub name: String,
    /// "static" or "template"
    #[serde(rename = "type")]
    pub route_type: RouteType,
    /// URL slug — may contain `$0`, `$1` for template routes
    pub slug: String,
    /// Pond path to markdown page (e.g., "/etc/site/index.md")
    #[serde(default)]
    pub page: Option<String>,
    /// For template routes: which export stage provides data
    #[serde(default)]
    pub export: Option<String>,
    /// Nested child routes
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
}

/// Route type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RouteType {
    Static,
    Template,
}

/// Static asset to copy verbatim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticAsset {
    pub pattern: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let yaml = r#"
site:
  title: "Test Site"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    temporal: ["year", "month"]

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
    routes:
      - name: "param"
        type: template
        slug: "$0"
        export: "params"
        page: "/etc/site/param.md"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.site.title, "Test Site");
        assert_eq!(config.site.base_url, "/");
        assert_eq!(config.exports.len(), 1);
        assert_eq!(config.exports[0].name, "params");
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.routes[0].routes.len(), 1);
        assert_eq!(config.routes[0].routes[0].route_type, RouteType::Template);
    }
}
