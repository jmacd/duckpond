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
    /// Publication date for blog posts (ISO 8601, e.g. "2024-06-15")
    pub date: Option<&'a str>,
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
        "blog" => blog_layout(ctx),
        _ => default_layout(ctx),
    };
    markup.into_string()
}

/// Common `<head>` elements shared by all layouts.
fn common_head(ctx: &LayoutContext) -> Markup {
    html! {
        meta charset="utf-8";
        meta name="viewport" content="width=device-width, initial-scale=1";
        meta name="generator" content=(format!("DuckPond v{}", VERSION));
        title { (ctx.title) " -- " (ctx.site_title) }
        link rel="preconnect" href="https://fonts.googleapis.com";
        link rel="preconnect" href="https://fonts.gstatic.com" crossorigin;
        link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap";
        link rel="stylesheet" href="/style.css";
    }
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
                (common_head(ctx))
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
                // Our glue code -- loads DuckDB-WASM + Observable Plot dynamically
                script src="/chart.js" type="module" {}
            }
        }
    }
}

/// Layout for content pages (articles, documentation, blog index).
///
/// Sidebar navigation + card-wrapped article with back-to-home link.
/// Content is wrapped in a card container matching the blog post style.
fn page_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    nav class="blog-back" {
                        a href="/" {
                            span class="blog-back-arrow" { "\u{2190}" }
                            " Home"
                        }
                    }
                    article class="blog-post" {
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
            }
        }
    }
}

/// Layout for individual blog posts.
///
/// Renders the post inside a card container matching the blog grid card
/// aesthetic: lighter background, rounded edges, date/title header, with
/// a back-arrow link to the blog index.
fn blog_layout(ctx: &LayoutContext) -> Markup {
    let date_display = ctx.date.map(format_date);
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    nav class="blog-back" {
                        a href="blog.html" {
                            span class="blog-back-arrow" { "\u{2190}" }
                            " Blog"
                        }
                    }
                    article class="blog-post" {
                        header class="blog-post-header" {
                            @if let Some(ref date) = date_display {
                                time class="blog-post-date" { (date) }
                            }
                            h1 class="blog-post-title" { (ctx.title) }
                        }
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
            }
        }
    }
}

/// Format an ISO 8601 date string for display (e.g. "2024-06-15" -> "June 15, 2024").
fn format_date(iso: &str) -> String {
    let parts: Vec<&str> = iso.split('-').collect();
    if parts.len() != 3 {
        return iso.to_string();
    }
    let month = match parts[1] {
        "01" => "January",
        "02" => "February",
        "03" => "March",
        "04" => "April",
        "05" => "May",
        "06" => "June",
        "07" => "July",
        "08" => "August",
        "09" => "September",
        "10" => "October",
        "11" => "November",
        "12" => "December",
        _ => return iso.to_string(),
    };
    let day = parts[2].trim_start_matches('0');
    format!("{} {}, {}", month, day, parts[0])
}

/// Default layout for static pages (index, listing pages).
///
/// No CDN scripts -- these are informational pages without interactive charts.
fn default_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
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
            date: None,
        };
        let html = apply_layout("default", &ctx);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Home -- Test Site"));
        assert!(html.contains("<h1>Hello</h1>"));
        assert!(!html.contains("duckdb")); // No CDN scripts in default layout
        assert!(html.contains("fonts.googleapis.com")); // Google Fonts
    }

    #[test]
    fn test_data_layout_with_sidebar() {
        let ctx = LayoutContext {
            title: "Temperature",
            site_title: "Noyo Harbor",
            content: "<p>Chart here</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: None,
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
            date: None,
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
            date: None,
        };
        let html = apply_layout("page", &ctx);
        assert!(html.contains("content-page"), "Page layout class");
        assert!(html.contains("blog-post"), "Card container");
        assert!(html.contains("blog-back"), "Back nav present");
        assert!(html.contains("href=\"/\""), "Back to home: {}", html);
        assert!(html.contains("class=\"sidebar\""), "Sidebar present");
        assert!(!html.contains("chart.js"), "No CDN scripts");
        assert!(html.contains("Water System -- Caspar Water"), "Title");
    }

    #[test]
    fn test_blog_layout() {
        let ctx = LayoutContext {
            title: "My Blog Post",
            site_title: "Test Blog",
            content: "<p>Post content here</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: Some("2025-03-10"),
        };
        let html = apply_layout("blog", &ctx);
        assert!(html.contains("blog-post"), "Expected blog-post class: {}", html);
        assert!(html.contains("blog-back"), "Expected back nav: {}", html);
        assert!(html.contains("blog.html"), "Expected back link to blog: {}", html);
        assert!(html.contains("March 10, 2025"), "Expected formatted date: {}", html);
        assert!(html.contains("My Blog Post"), "Expected title: {}", html);
        assert!(html.contains("blog-post-title"), "Expected title class: {}", html);
        assert!(html.contains("Post content here"), "Expected content: {}", html);
        assert!(html.contains("class=\"sidebar\""), "Expected sidebar: {}", html);
        assert!(!html.contains("chart.js"), "No CDN scripts in blog layout");
    }

    #[test]
    fn test_blog_layout_no_date() {
        let ctx = LayoutContext {
            title: "Undated Post",
            site_title: "Blog",
            content: "<p>No date</p>",
            sidebar: None,
            date: None,
        };
        let html = apply_layout("blog", &ctx);
        assert!(html.contains("blog-post"), "Expected blog-post: {}", html);
        assert!(!html.contains("blog-post-date"), "No date element expected: {}", html);
    }
}
