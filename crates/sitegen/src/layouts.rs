// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Maud HTML layouts for sitegen pages.
//!
//! Layouts wrap rendered markdown content in a complete HTML document.
//! Selected by frontmatter `layout: data|default` in each markdown page.

use maud::{DOCTYPE, Markup, PreEscaped, html};

/// DuckPond version baked into generated HTML as `<meta name="generator">`.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Context passed to layout functions.
pub struct LayoutContext<'a> {
    /// Page title (from frontmatter)
    pub title: &'a str,
    /// Site title (from site.yaml)
    pub site_title: &'a str,
    /// Rendered HTML content (from markdown + shortcodes)
    pub content: &'a str,
    /// Rendered sidebar HTML (from sidebar partial, if any)
    pub sidebar: Option<&'a str>,
}

/// Apply a named layout to rendered content.
///
/// Layout names come from markdown frontmatter:
/// ```yaml
/// ---
/// title: "Temperature"
/// layout: data
/// ---
/// ```
pub fn apply_layout(name: &str, ctx: &LayoutContext) -> String {
    let markup = match name {
        "data" => data_layout(ctx),
        "page" => page_layout(ctx),
        _ => default_layout(ctx),
    };
    markup.into_string()
}

/// Layout for data pages (parameter/site detail pages).
///
/// Includes CDN scripts for DuckDB-WASM and Observable Plot,
/// a sidebar for navigation, and a main content area.
fn data_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                meta name="generator" content=(format!("DuckPond v{}", VERSION));
                title { (ctx.title) " — " (ctx.site_title) }
                link rel="stylesheet" href="/style.css";
            }
            body {
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="data-page" {
                    (PreEscaped(ctx.content))
                }
                // Our glue code — loads DuckDB-WASM + Observable Plot dynamically
                script src="/chart.js" type="module" {}
            }
        }
    }
}

/// Layout for content pages (articles, documentation, blog posts).
///
/// Sidebar navigation + article wrapper, no CDN scripts.
/// Content is wrapped in `<article>` for semantic HTML.
fn page_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                meta name="generator" content=(format!("DuckPond v{}", VERSION));
                title { (ctx.title) " — " (ctx.site_title) }
                link rel="stylesheet" href="/style.css";
            }
            body {
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    article {
                        (PreEscaped(ctx.content))
                    }
                }
            }
        }
    }
}

/// Default layout for static pages (index, listing pages).
///
/// No CDN scripts — these are informational pages without interactive charts.
fn default_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                meta name="generator" content=(format!("DuckPond v{}", VERSION));
                title { (ctx.title) " — " (ctx.site_title) }
                link rel="stylesheet" href="/style.css";
            }
            body {
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="hero" {
                    (PreEscaped(ctx.content))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_layout() {
        let ctx = LayoutContext {
            title: "Home",
            site_title: "Test Site",
            content: "<h1>Hello</h1>",
            sidebar: None,
        };
        let html = apply_layout("default", &ctx);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Home — Test Site"));
        assert!(html.contains("<h1>Hello</h1>"));
        assert!(!html.contains("duckdb")); // No CDN scripts in default layout
    }

    #[test]
    fn test_data_layout_with_sidebar() {
        let ctx = LayoutContext {
            title: "Temperature",
            site_title: "Noyo Harbor",
            content: "<p>Chart here</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
        };
        let html = apply_layout("data", &ctx);
        assert!(html.contains("chart.js")); // Glue code present
        assert!(html.contains("type=\"module\"")); // Module script
        assert!(html.contains("class=\"sidebar\""));
        assert!(html.contains("<ul><li>Nav</li></ul>"));
        assert!(html.contains("data-page"));
    }

    #[test]
    fn test_unknown_layout_falls_back_to_default() {
        let ctx = LayoutContext {
            title: "Page",
            site_title: "Site",
            content: "<p>Content</p>",
            sidebar: None,
        };
        let html = apply_layout("nonexistent", &ctx);
        assert!(html.contains("class=\"hero\"")); // Default layout uses hero class
    }

    #[test]
    fn test_page_layout() {
        let ctx = LayoutContext {
            title: "Water System",
            site_title: "Caspar Water",
            content: "<h1>Water</h1><p>Info</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
        };
        let html = apply_layout("page", &ctx);
        assert!(html.contains("content-page")); // Page layout class
        assert!(html.contains("<article>")); // Wrapped in article
        assert!(html.contains("class=\"sidebar\"")); // Sidebar present
        assert!(!html.contains("chart.js")); // No CDN scripts
        assert!(html.contains("Water System — Caspar Water")); // Title
    }
}
