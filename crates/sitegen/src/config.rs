// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Site configuration -- parsed from `site.yaml` (pond or host filesystem).

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
///     page: "/site/index.md"
///     routes: [...]
///
/// partials:
///   sidebar: "/site/sidebar.md"
///
/// static:
///   - pattern: "/static/*"
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
    /// Ordered list of sidebar entries.
    /// Each entry is either a plain string (matched by page title substring)
    /// or a map with `label` and `children` for hierarchical navigation.
    /// When present, `content_nav` renders pages in this exact order.
    /// Pages not listed are excluded from the sidebar.
    #[serde(default)]
    pub sidebar: Vec<SidebarEntry>,
    /// RSS feed configuration. If present, an RSS feed is generated.
    /// Requires `site.site_url` to be set for absolute link generation.
    #[serde(default)]
    pub feed: Option<FeedConfig>,
    /// Theme overrides for CSS custom properties.
    /// Any key here overrides the corresponding CSS variable in `:root`.
    /// Example: `accent: "#1a365d"` sets `--accent: #1a365d`.
    #[serde(default)]
    pub theme: std::collections::BTreeMap<String, String>,
    /// Sub-sites to generate recursively from imported ponds.
    /// Each sub-site references a sitegen config inside an imported mount
    /// and generates into a subdirectory of the output.
    #[serde(default)]
    pub subsites: Vec<SubsiteConfig>,
}

impl SiteConfig {
    /// Rewrite sidebar hrefs when a subsite is mounted at a different base_url.
    ///
    /// Any href starting with `old_base` is rewritten to start with `new_base`.
    pub fn rewrite_sidebar_urls(&mut self, old_base: &str, new_base: &str) {
        fn rewrite(href: &mut String, old: &str, new: &str) {
            if let Some(rest) = href.strip_prefix(old) {
                *href = format!("{}{}", new, rest);
            }
        }

        for entry in &mut self.sidebar {
            match entry {
                SidebarEntry::WithChildren {
                    href, children, ..
                } => {
                    if let Some(h) = href {
                        rewrite(h, old_base, new_base);
                    }
                    for child in children {
                        rewrite(&mut child.href, old_base, new_base);
                    }
                }
                SidebarEntry::DirectLink { href, .. } => {
                    rewrite(href, old_base, new_base);
                }
                SidebarEntry::Simple(_) => {}
            }
        }
    }
}

/// Site-wide metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteMeta {
    pub title: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    /// Canonical site URL for RSS feed links (e.g., "https://casparwater.org").
    /// Required for RSS feed generation; if absent, feed is skipped.
    #[serde(default)]
    pub site_url: Option<String>,
    /// GitHub repository URL shown as an icon in the top bar.
    /// If absent, the GitHub icon is omitted.
    #[serde(default)]
    pub github_url: Option<String>,
}

/// RSS feed configuration (optional section in site.yaml).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    /// Content section to include in the feed (default: "Blog").
    #[serde(default = "default_feed_section")]
    pub section: String,
    /// Which content stage provides pages (default: first content stage).
    #[serde(default)]
    pub content: Option<String>,
    /// Channel description (default: site title).
    #[serde(default)]
    pub description: Option<String>,
}

fn default_feed_section() -> String {
    "Blog".to_string()
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
    /// Pond glob pattern or format-provider URL pattern.
    ///
    /// Bare paths (e.g., `/reduced/single_param/*/*.series`) are resolved
    /// directly as queryable pond files.
    ///
    /// URL-scheme patterns (e.g., `jsonlogs:///logs/watershop/*.jsonl`)
    /// are resolved through the format provider registry, allowing raw
    /// data files to be queried via DataFusion.
    pub pattern: String,
    /// Target data points per screen width for partition sizing.
    /// Typical value: 1000-2000. Default: 1500.
    /// Controls how parquet files are temporally partitioned per resolution.
    #[serde(default = "default_target_points")]
    pub target_points: u64,
    /// Name of the timestamp column for temporal partitioning.
    /// Default: "timestamp". Override for format-provider patterns where
    /// the timestamp column has a different name (e.g., "__REALTIME_TIMESTAMP"
    /// for journald logs).
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,
}

fn default_target_points() -> u64 {
    1500
}

fn default_timestamp_column() -> String {
    "timestamp".to_string()
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
    /// Path to markdown page (e.g., "/site/index.md")
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

/// A sidebar entry: either a simple label or a label with sub-navigation children.
///
/// Simple entries are plain strings matched by page title substring.
/// Entries with children render a parent link plus nested sub-items
/// that expand when the parent or any child is the current page.
///
/// ```yaml
/// sidebar:
///   - "Political"                    # simple
///   - label: "Monitoring"            # with children
///     children:
///       - label: "Well Depth"
///         href: "/data/well-depth.html"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SidebarEntry {
    /// Label with nested sub-navigation items and optional parent href.
    WithChildren {
        label: String,
        #[serde(default)]
        href: Option<String>,
        children: Vec<SidebarChild>,
    },
    /// Direct link with label and href (no children, no page matching).
    DirectLink { label: String, href: String },
    /// Simple label matched by page title substring.
    Simple(String),
}

impl SidebarEntry {
    /// The display label for this entry.
    pub fn label(&self) -> &str {
        match self {
            SidebarEntry::Simple(s) => s,
            SidebarEntry::WithChildren { label, .. } => label,
            SidebarEntry::DirectLink { label, .. } => label,
        }
    }

    /// Children, if any.
    pub fn children(&self) -> &[SidebarChild] {
        match self {
            SidebarEntry::Simple(_) => &[],
            SidebarEntry::WithChildren { children, .. } => children,
            SidebarEntry::DirectLink { .. } => &[],
        }
    }

    /// Direct href, if specified.
    pub fn href(&self) -> Option<&str> {
        match self {
            SidebarEntry::Simple(_) => None,
            SidebarEntry::WithChildren { href, .. } => href.as_deref(),
            SidebarEntry::DirectLink { href, .. } => Some(href),
        }
    }
}

/// A child item in a sidebar entry with sub-navigation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidebarChild {
    /// Display label
    pub label: String,
    /// Direct URL path (e.g., "/data/well-depth.html")
    pub href: String,
}

/// A sub-site to generate recursively from an imported pond.
///
/// ```yaml
/// subsites:
///   - name: "noyo"
///     path: "/sources/noyo"
///     config: "/system/etc/90-sitegen"
///     base_url: "/noyo/"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsiteConfig {
    /// Display name (used for logging and output directory fallback).
    pub name: String,
    /// Pond path to the import mount point (e.g., "/sources/noyo").
    pub path: String,
    /// Path to the sitegen config within the foreign pond (absolute,
    /// resolved relative to the mount point via effective_root).
    pub config: String,
    /// Override the sub-site's base_url for the combined site.
    /// If omitted, the sub-site's own base_url is used.
    #[serde(default)]
    pub base_url: Option<String>,
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
    page: "/site/index.md"
    routes:
      - name: "param"
        type: template
        slug: "$0"
        export: "params"
        page: "/site/param.md"
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
    page: "/site/index.md"
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

    #[test]
    fn parse_minimal_config_no_site_url() {
        let yaml = r#"
site:
  title: "Test"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert!(config.site.site_url.is_none());
        assert!(config.feed.is_none());
    }

    #[test]
    fn parse_config_with_site_url_and_feed() {
        let yaml = r#"
site:
  title: "Test Blog"
  site_url: "https://example.com"

feed:
  section: "Updates"
  description: "Latest updates"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.site.site_url.as_deref(), Some("https://example.com"));
        let feed = config.feed.expect("feed config");
        assert_eq!(feed.section, "Updates");
        assert_eq!(feed.description.as_deref(), Some("Latest updates"));
        assert!(feed.content.is_none());
    }

    #[test]
    fn parse_feed_defaults() {
        let yaml = r#"
site:
  title: "Test"
  site_url: "https://example.com"

feed: {}
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        let feed = config.feed.expect("feed config");
        assert_eq!(feed.section, "Blog");
        assert!(feed.content.is_none());
        assert!(feed.description.is_none());
    }

    #[test]
    fn parse_sidebar_mixed_entries() {
        let yaml = r#"
site:
  title: "Test"

sidebar:
  - "Political"
  - "Blog"
  - label: "Monitoring"
    children:
      - label: "Well Depth"
        href: "/data/well-depth.html"
      - label: "Tank Level"
        href: "/data/tank-level.html"
  - "Thanks"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.sidebar.len(), 4);
        assert_eq!(config.sidebar[0].label(), "Political");
        assert!(config.sidebar[0].children().is_empty());
        assert_eq!(config.sidebar[2].label(), "Monitoring");
        assert_eq!(config.sidebar[2].children().len(), 2);
        assert_eq!(config.sidebar[2].children()[0].label, "Well Depth");
        assert_eq!(
            config.sidebar[2].children()[0].href,
            "/data/well-depth.html"
        );
        assert_eq!(config.sidebar[3].label(), "Thanks");
    }

    #[test]
    fn parse_config_with_subsites() {
        let yaml = r#"
site:
  title: "Caspar Infrastructure"
  base_url: "/"

subsites:
  - name: "noyo"
    path: "/sources/noyo"
    config: "/system/etc/90-sitegen"
    base_url: "/noyo/"
  - name: "water"
    path: "/sources/water"
    config: "/etc/site.yaml"
  - name: "septic"
    path: "/sources/septic"
    config: "/etc/site.yaml"
    base_url: "/septic/"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/system/site/index.md"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.subsites.len(), 3);
        assert_eq!(config.subsites[0].name, "noyo");
        assert_eq!(config.subsites[0].path, "/sources/noyo");
        assert_eq!(config.subsites[0].config, "/system/etc/90-sitegen");
        assert_eq!(config.subsites[0].base_url.as_deref(), Some("/noyo/"));
        assert_eq!(config.subsites[1].name, "water");
        assert!(config.subsites[1].base_url.is_none());
        assert_eq!(config.subsites[2].base_url.as_deref(), Some("/septic/"));
        // Existing fields still work
        assert_eq!(config.routes.len(), 1);
        assert!(config.exports.is_empty());
    }

    #[test]
    fn parse_config_no_subsites_default() {
        let yaml = r#"
site:
  title: "Standalone Site"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert!(config.subsites.is_empty());
    }
}
