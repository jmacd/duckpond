// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Site configuration -- parsed from `/etc/site.yaml` inside the pond.

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
    pub content: Vec<ContentStage>,
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

/// A content stage: globs markdown data files in the pond and reads
/// their frontmatter for metadata (title, weight, slug).
///
/// Content stages are resolved before route expansion. A `content` route
/// references a stage by name, producing one page per matched file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentStage {
    /// Name referenced by content routes (e.g., "pages")
    pub name: String,
    /// Pond glob pattern matching data files (e.g., "/pages/*.md")
    pub pattern: String,
}

/// An export stage: runs `pond export` internally to produce data files
/// that become template context.
///
/// # Automatic Temporal Partitioning
///
/// Parquet files are split into Hive-style temporal partitions automatically.
/// The system discovers resolutions from the matched files (e.g., `res=1h`,
/// `res=24h`) and computes the right partition level for each resolution
/// so that any viewport loads at most 2 files.
///
/// `target_points` controls the trade-off: higher values mean finer partitions
/// (more data per file, fewer files). Default is 1500, matching a typical
/// screen width in pixels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportStage {
    /// Name referenced by routes (e.g., "params", "sites")
    pub name: String,
    /// Pond glob pattern (e.g., "/reduced/single_param/*/*.series")
    pub pattern: String,
    /// Target data points per screen width for partition sizing.
    /// Typical value: 1000-2000. Default: 1500.
    /// Controls how parquet files are temporally partitioned per resolution.
    #[serde(default = "default_target_points")]
    pub target_points: u64,
}

fn default_target_points() -> u64 {
    1500
}

/// A route in the hierarchical route tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    /// Route name (for logging/debugging)
    pub name: String,
    /// "static" or "template"
    #[serde(rename = "type")]
    pub route_type: RouteType,
    /// URL slug -- may contain `$0`, `$1` for template routes
    pub slug: String,
    /// Pond path to markdown page (e.g., "/etc/site/index.md")
    #[serde(default)]
    pub page: Option<String>,
    /// For template routes: which export stage provides data
    #[serde(default)]
    pub export: Option<String>,
    /// For content routes: which content stage provides pages
    #[serde(default)]
    pub content: Option<String>,
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
    Content,
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
        assert_eq!(config.exports[0].target_points, 1500); // default
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.routes[0].routes.len(), 1);
        assert_eq!(config.routes[0].routes[0].route_type, RouteType::Template);
    }

    #[test]
    fn parse_content_config() {
        let yaml = r#"
site:
  title: "Caspar Water"

content:
  - name: "pages"
    pattern: "/pages/*.md"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
    routes:
      - name: "pages"
        type: content
        slug: ""
        content: "pages"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.content.len(), 1);
        assert_eq!(config.content[0].name, "pages");
        assert_eq!(config.content[0].pattern, "/pages/*.md");
        assert_eq!(config.routes[0].routes[0].route_type, RouteType::Content);
        assert_eq!(config.routes[0].routes[0].content.as_deref(), Some("pages"));
    }

    #[test]
    fn parse_config_with_target_points() {
        let yaml = r#"
site:
  title: "Custom Points"

exports:
  - name: "metrics"
    pattern: "/reduced/*/*/*.series"
    target_points: 2000
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.exports[0].target_points, 2000);
    }
}
