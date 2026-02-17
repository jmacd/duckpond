// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Built-in shortcodes for sitegen markdown templates.
//!
//! Shortcodes are `{{ name key="value" }}` directives in markdown that expand
//! to HTML at build time. They are the serialization boundary between typed
//! Rust data (ExportedFile structs) and client-side HTML/JS.
//!
//! Uses our own shortcode parser (in `crate::markdown`) with closures that
//! capture an `Arc<ShortcodeContext>` with DuckPond data.

use crate::markdown::{ShortcodeArgs, Shortcodes};
use crate::routes::ContentPage;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One exported data file with capture groups and time range.
///
/// This is the typed struct that shortcodes receive directly — no JSON
/// serialization boundary between export and rendering.
#[derive(Debug, Clone)]
pub struct ExportedFile {
    /// Pond path to the data file
    pub path: String,
    /// Relative URL to exported .parquet file (e.g., "data/Temperature/res=1h.parquet")
    pub file: String,
    /// Capture groups from pattern matching ($0, $1, ...)
    pub captures: Vec<String>,
    /// Temporal partition values (e.g., {"year": "2025", "month": "01"})
    pub temporal: BTreeMap<String, String>,
    /// UTC timestamp for start of data (seconds since epoch)
    pub start_time: i64,
    /// UTC timestamp for end of data (seconds since epoch)
    pub end_time: i64,
}

/// All exported files for one export stage, grouped by $0 value.
#[derive(Debug, Clone)]
pub struct ExportContext {
    /// Files grouped by the $0 capture value
    pub by_key: BTreeMap<String, Vec<ExportedFile>>,
}

/// Context passed to shortcodes during rendering.
///
/// Each template page gets its own `ShortcodeContext`, wrapped in `Arc` and
/// captured by the shortcode closures. This is the bridge between DuckPond's
/// typed data and the shortcode expansion engine.
#[derive(Debug, Clone)]
pub struct ShortcodeContext {
    /// Capture group values for this page ($0, $1, ...)
    pub captures: Vec<String>,
    /// All exported data files for this page (filtered to this $0 value)
    pub datafiles: Vec<ExportedFile>,
    /// Named collections for nav-list (e.g., "params" → ["Temperature", "DO", ...])
    pub collections: BTreeMap<String, Vec<String>>,
    /// Content page lists for content_nav (e.g., "pages" → [ContentPage, ...])
    pub content_pages: BTreeMap<String, Vec<ContentPage>>,
    /// Site title from config
    pub site_title: String,
    /// Current URL path
    pub current_path: String,
    /// Breadcrumb segments: (label, href)
    pub breadcrumbs: Vec<(String, String)>,
    /// Base URL prefix for all generated links (e.g., "/noyo-harbor/")
    pub base_url: String,
}

/// Build a `Shortcodes` instance with all built-in shortcodes registered.
///
/// Each closure captures `Arc<ShortcodeContext>` — no thread-locals, no RefCell.
/// This function is called once per page, creating a fresh shortcodes set
/// with the page's specific context.
pub fn register_shortcodes(ctx: Arc<ShortcodeContext>) -> Shortcodes {
    let mut shortcodes = Shortcodes::new();

    // {{ cap0 }}, {{ cap1 }}, ... — capture group values
    //
    // Design doc uses `{{ $0 }}` syntax, but shortcode name validation
    // requires ^[A-Za-z_][0-9A-Za-z_]+$. So templates use `{{ cap0 }}` and
    // `preprocess_variables()` rewrites `{{ $0 }}` → `{{ cap0 }}` before rendering.
    for i in 0..10usize {
        let c = ctx.clone();
        let name = format!("cap{}", i);
        shortcodes.register(&name, move |_args: &ShortcodeArgs| {
            c.captures.get(i).cloned().unwrap_or_default()
        });
    }

    // {{ chart }} — emit chart container with inline datafile manifest as JSON.
    // Client-side chart.js reads the JSON to load parquet via DuckDB-WASM.
    {
        let c = ctx.clone();
        shortcodes.register("chart", move |_args: &ShortcodeArgs| {
            render_chart(&c.datafiles)
        });
    }

    // {{ nav_list collection="params" base="/params" /}} — link list for a collection
    {
        let c = ctx.clone();
        shortcodes.register("nav_list", move |args: &ShortcodeArgs| {
            let collection = args.get_str("collection").unwrap_or("");
            let base = args
                .get_str("base")
                .or_else(|| args.get_str("path"))
                .unwrap_or("");
            let prefixed_base = prefix_with_base_url(&c.base_url, base);
            render_nav_list(&c, collection, &prefixed_base)
        });
    }

    // {{ breadcrumb }} — breadcrumb trail
    {
        let c = ctx.clone();
        shortcodes.register("breadcrumb", move |_args: &ShortcodeArgs| {
            render_breadcrumb(&c.breadcrumbs)
        });
    }

    // {{ site_title }} — site title from config
    {
        let c = ctx.clone();
        shortcodes.register("site_title", move |_args: &ShortcodeArgs| {
            c.site_title.clone()
        });
    }

    // {{ base_url }} — base URL for use in markdown links: [Home]({{ base_url /}})
    {
        let c = ctx.clone();
        shortcodes.register("base_url", move |_args: &ShortcodeArgs| c.base_url.clone());
    }

    // {{ content_nav content="pages" /}} — navigation list for content pages
    // Renders titles sorted by weight with active-page highlighting.
    {
        let c = ctx.clone();
        shortcodes.register("content_nav", move |args: &ShortcodeArgs| {
            let content_name = args.get_str("content").unwrap_or("");
            render_content_nav(&c, content_name)
        });
    }

    // {{ figure src="/img/photo.jpg" caption="..." float="right" /}}
    // Renders a <figure> with optional float positioning.
    {
        let c = ctx.clone();
        shortcodes.register("figure", move |args: &ShortcodeArgs| {
            render_figure(&c, args)
        });
    }

    shortcodes
}

/// Pre-process markdown, rewriting design-doc syntax into valid shortcode names.
///
/// Maudit requires shortcode names matching `^[A-Za-z_][0-9A-Za-z_]+$`, so:
/// - `{{ $0 }}` → `{{ cap0 }}`
/// - `{{ $1 }}` → `{{ cap1 }}`
/// - `{{ nav-list ... }}` → `{{ nav_list ... }}`
/// - `{{ site-title }}` → `{{ site_title }}`
///
/// Also rewrites the YAML frontmatter variable references.
pub fn preprocess_variables(content: &str) -> String {
    content
        .replace("{{ $0 }}", "{{ cap0 /}}")
        .replace("{{ $1 }}", "{{ cap1 /}}")
        .replace("{{ $2 }}", "{{ cap2 /}}")
        .replace("{{ $3 }}", "{{ cap3 /}}")
        .replace("nav-list", "nav_list")
        .replace("site-title", "site_title")
        .replace("content-nav", "content_nav")
        .replace("base-url", "base_url")
}

// ---------------------------------------------------------------------------
// Shortcode renderers
// ---------------------------------------------------------------------------

/// Prepend base_url to an absolute path.
///
/// If `base_url` is `/` (default), returns `path` unchanged.
/// If `base_url` is `/noyo-harbor/`, returns `/noyo-harbor/params` for `/params`.
fn prefix_with_base_url(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.is_empty() || base == "/" {
        return path.to_string();
    }
    if path.starts_with('/') {
        format!("{}{}", base, path)
    } else {
        format!("{}/{}", base, path)
    }
}

/// Render a chart container with inline datafile manifest.
///
/// Emits a `<div class="chart-container">` with a `<script type="application/json">`
/// block containing the file manifest. Client-side chart.js reads this.
fn render_chart(datafiles: &[ExportedFile]) -> String {
    if datafiles.is_empty() {
        return "<div class=\"chart-container\"><p>No data files available.</p></div>".to_string();
    }

    let files_json: Vec<serde_json::Value> = datafiles
        .iter()
        .map(|f| {
            serde_json::json!({
                "path": f.path,
                "file": f.file,
                "captures": f.captures,
                "temporal": f.temporal,
                "start_time": f.start_time,
                "end_time": f.end_time,
            })
        })
        .collect();

    let json = serde_json::to_string(&files_json).unwrap_or_else(|_| "[]".to_string());

    format!(
        "<div class=\"chart-container\" id=\"chart\">\
         <script type=\"application/json\" class=\"chart-data\">{}</script>\
         </div>",
        json
    )
}

/// Render a navigation list for a named collection.
fn render_nav_list(ctx: &ShortcodeContext, collection: &str, path: &str) -> String {
    let items = match ctx.collections.get(collection) {
        Some(items) => items,
        None => return format!("<!-- nav_list: unknown collection '{}' -->", collection),
    };

    if items.is_empty() {
        return format!("<!-- nav_list: collection '{}' is empty -->", collection);
    }

    let mut html = String::from("<ul class=\"nav-list\">\n");
    for item in items {
        let href = format!("{}/{}.html", path, item);
        let is_active = ctx.current_path == href;
        let li_class = if is_active { " class=\"active\"" } else { "" };
        let aria = if is_active {
            " aria-current=\"page\""
        } else {
            ""
        };
        html.push_str(&format!(
            "  <li{}><a href=\"{}\"{}>{}</a></li>\n",
            li_class, href, aria, item
        ));
    }
    html.push_str("</ul>");
    html
}

/// Render a breadcrumb trail as an HTML nav element.
fn render_breadcrumb(breadcrumbs: &[(String, String)]) -> String {
    if breadcrumbs.is_empty() {
        return String::new();
    }

    let mut html = String::from("<nav class=\"breadcrumb\" aria-label=\"breadcrumb\"><ol>\n");
    let last = breadcrumbs.len() - 1;
    for (i, (label, href)) in breadcrumbs.iter().enumerate() {
        if i == last {
            html.push_str(&format!(
                "  <li class=\"active\" aria-current=\"page\">{}</li>\n",
                label
            ));
        } else {
            html.push_str(&format!("  <li><a href=\"{}\">{}</a></li>\n", href, label));
        }
    }
    html.push_str("</ol></nav>");
    html
}

/// Render a navigation list for content pages (ordered by weight).
///
/// Pages are grouped by their `section` frontmatter field. Each section
/// becomes a collapsible `<details>` element. Pages without a section
/// appear as top-level items. A section is auto-opened if it contains
/// the current page.
fn render_content_nav(ctx: &ShortcodeContext, content_name: &str) -> String {
    let pages = match ctx.content_pages.get(content_name) {
        Some(pages) => pages,
        None => {
            return format!(
                "<!-- content_nav: unknown content '{}' -->",
                content_name
            )
        }
    };

    if pages.is_empty() {
        return format!("<!-- content_nav: content '{}' is empty -->", content_name);
    }

    let base = ctx.base_url.trim_end_matches('/');

    // Build href for a page
    let page_href = |slug: &str| -> String {
        if base.is_empty() || base == "/" {
            format!("/{}.html", slug)
        } else {
            format!("{}/{}.html", base, slug)
        }
    };

    // Render a single <li> for a page
    let render_li = |page: &ContentPage, indent: &str| -> String {
        let href = page_href(&page.slug);
        let is_active = ctx.current_path == href;
        let li_class = if is_active { " class=\"active\"" } else { "" };
        let aria = if is_active {
            " aria-current=\"page\""
        } else {
            ""
        };
        format!(
            "{}<li{}><a href=\"{}\"{}>{}</a></li>\n",
            indent, li_class, href, aria, page.title
        )
    };

    // Group pages by section, preserving weight order.
    // We use a Vec of (section_name, pages) to preserve first-seen order.
    let mut sections: Vec<(Option<String>, Vec<&ContentPage>)> = Vec::new();
    for page in pages {
        if page.hidden {
            continue;
        }
        let key = page.section.clone();
        if let Some(group) = sections.iter_mut().find(|(k, _)| *k == key) {
            group.1.push(page);
        } else {
            sections.push((key, vec![page]));
        }
    }

    // Determine which section to expand. Exactly one section is expanded at
    // all times: the one containing the active page, or the first section if
    // no page matches (e.g., home/index page).
    let active_section: Option<Option<String>> = sections.iter().find_map(|(key, section_pages)| {
        if section_pages
            .iter()
            .any(|p| ctx.current_path == page_href(&p.slug))
        {
            Some(key.clone())
        } else {
            None
        }
    });
    // Fall back to the first section when no page is active.
    let expanded_section = active_section
        .or_else(|| sections.first().map(|(k, _)| k.clone()));

    let mut html = String::from("<nav class=\"nav-list\">\n");
    for (section, section_pages) in &sections {
        match section {
            None => {
                // Top-level items (no section grouping)
                html.push_str("<ul>\n");
                for page in section_pages {
                    html.push_str(&render_li(page, "  "));
                }
                html.push_str("</ul>\n");
            }
            Some(section_name) => {
                // Expand only the one section that should be open.
                let expanded = expanded_section.as_ref() == Some(section);
                let class = if expanded {
                    "nav-section expanded"
                } else {
                    "nav-section"
                };
                html.push_str(&format!(
                    "<div class=\"{}\">\n  <h3 class=\"nav-section-title\">{}</h3>\n  <ul>\n",
                    class, section_name
                ));
                for page in section_pages {
                    html.push_str(&render_li(page, "    "));
                }
                html.push_str("  </ul>\n</div>\n");
            }
        }
    }
    html.push_str("</nav>");
    html
}

/// Render a `<figure>` element with optional float positioning.
///
/// Shortcode: `{{ figure src="/img/photo.jpg" caption="..." float="right" /}}`
///
/// `float` can be "right", "left", or "full" (default: no float).
/// Caption text is also used as the `alt` attribute.
fn render_figure(_ctx: &ShortcodeContext, args: &ShortcodeArgs) -> String {
    let src = args.get_str("src").unwrap_or("");
    let caption = args.get_str("caption").unwrap_or("");
    let float = args.get_str("float").unwrap_or("");
    let alt = args.get_str("alt").unwrap_or(caption);

    let class = match float {
        "right" => " class=\"figure-right\"",
        "left" => " class=\"figure-left\"",
        "full" => " class=\"figure-full\"",
        _ => "",
    };

    let mut html = format!("<figure{}>\n", class);
    if !src.is_empty() {
        html.push_str(&format!(
            "  <a href=\"{}\" target=\"_blank\"><img src=\"{}\" alt=\"{}\"></a>\n",
            src,
            src,
            html_escape(alt),
        ));
    }
    if !caption.is_empty() {
        html.push_str(&format!("  <figcaption>{}</figcaption>\n", caption));
    }
    html.push_str("</figure>");
    html
}

/// Minimal HTML escaping for attribute values.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_variables() {
        let input = "# {{ $0 }}\n\n{{ breadcrumb /}}\n\n{{ chart /}}";
        let output = preprocess_variables(input);
        assert!(output.contains("{{ cap0 /}}"));
        assert!(output.contains("{{ chart /}}"));
        assert!(output.contains("{{ breadcrumb /}}"));
    }

    #[test]
    fn test_register_shortcodes_creates_all() {
        let ctx = Arc::new(ShortcodeContext {
            captures: vec!["Temperature".to_string()],
            datafiles: vec![ExportedFile {
                path: "params/Temperature/data.parquet".to_string(),
                file: "data/Temperature/data.parquet".to_string(),
                captures: vec!["Temperature".to_string()],
                temporal: BTreeMap::from([
                    ("year".to_string(), "2025".to_string()),
                    ("month".to_string(), "01".to_string()),
                ]),
                start_time: 1704067200,
                end_time: 1706745599,
            }],
            collections: BTreeMap::from([(
                "params".to_string(),
                vec!["Temperature".to_string(), "DO".to_string()],
            )]),
            content_pages: BTreeMap::new(),
            site_title: "Test Site".to_string(),
            current_path: "/params/Temperature".to_string(),
            breadcrumbs: vec![
                ("Home".to_string(), "/".to_string()),
                ("Params".to_string(), "/params".to_string()),
                ("Temperature".to_string(), "/params/Temperature".to_string()),
            ],
            base_url: "/".to_string(),
        });

        let shortcodes = register_shortcodes(ctx);
        // Verify shortcodes work by running a known shortcode through preprocess
        use crate::markdown::preprocess_shortcodes;
        let result = preprocess_shortcodes("{{ cap0 /}}", &shortcodes, None);
        assert_eq!(result.unwrap(), "Temperature");
    }

    #[test]
    fn test_render_chart_empty() {
        let html = render_chart(&[]);
        assert!(html.contains("No data files"));
    }

    #[test]
    fn test_render_chart_with_files() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::from([("year".to_string(), "2025".to_string())]),
            start_time: 100,
            end_time: 200,
        }];
        let html = render_chart(&files);
        assert!(html.contains("chart-container"));
        assert!(html.contains("data.parquet"));
    }

    #[test]
    fn test_render_breadcrumb() {
        let crumbs = vec![
            ("Home".to_string(), "/".to_string()),
            ("Here".to_string(), "/here".to_string()),
        ];
        let html = render_breadcrumb(&crumbs);
        assert!(html.contains("<nav"));
        assert!(html.contains("Home"));
        assert!(html.contains("aria-current"));
    }

    #[test]
    fn test_render_nav_list() {
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::from([(
                "params".to_string(),
                vec!["Temperature".to_string(), "DO".to_string()],
            )]),
            content_pages: BTreeMap::new(),
            site_title: String::new(),
            current_path: "/params/Temperature.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
        };
        let html = render_nav_list(&ctx, "params", "/params");
        assert!(html.contains("Temperature"));
        assert!(html.contains("/params/DO.html"));
        // Active item gets class and aria attribute
        assert!(
            html.contains(r#"<li class="active"><a href="/params/Temperature.html" aria-current="page">Temperature</a></li>"#),
            "Expected active class on Temperature, got: {}", html
        );
        // Non-active item has no extra attributes
        assert!(
            html.contains("<li><a href=\"/params/DO.html\">DO</a></li>"),
            "Expected plain li for DO, got: {}",
            html
        );
    }

    #[test]
    fn test_render_content_nav() {
        let pages = vec![
            ContentPage {
                title: "Water System".to_string(),
                slug: "water".to_string(),
                weight: 10,
                hidden: false,
                section: None,
                source_path: "/content/water.md".to_string(),
            },
            ContentPage {
                title: "History".to_string(),
                slug: "history".to_string(),
                weight: 20,
                hidden: false,
                section: None,
                source_path: "/content/history.md".to_string(),
            },
            ContentPage {
                title: "Hidden Page".to_string(),
                slug: "hidden".to_string(),
                weight: 5,
                hidden: true,
                section: None,
                source_path: "/content/hidden.md".to_string(),
            },
        ];
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/water.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
        };
        let html = render_content_nav(&ctx, "pages");
        // Hidden page excluded
        assert!(!html.contains("Hidden Page"), "Hidden page should be excluded: {}", html);
        // Wrapped in nav
        assert!(html.contains("<nav class=\"nav-list\">"), "Expected nav wrapper: {}", html);
        // No sections → flat <ul>
        assert!(html.contains("<ul>"), "Expected ul for unsectioned pages: {}", html);
        // Active page gets class
        assert!(
            html.contains(r#"<li class="active"><a href="/water.html" aria-current="page">Water System</a></li>"#),
            "Expected active class on Water System, got: {}", html
        );
        // Non-active page
        assert!(
            html.contains("<li><a href=\"/history.html\">History</a></li>"),
            "Expected plain li for History, got: {}", html
        );
    }

    #[test]
    fn test_render_content_nav_sections() {
        let pages = vec![
            ContentPage {
                title: "Water".to_string(),
                slug: "water".to_string(),
                weight: 10,
                hidden: false,
                section: Some("About".to_string()),
                source_path: "/content/water.md".to_string(),
            },
            ContentPage {
                title: "History".to_string(),
                slug: "history".to_string(),
                weight: 20,
                hidden: false,
                section: Some("About".to_string()),
                source_path: "/content/history.md".to_string(),
            },
            ContentPage {
                title: "Blog".to_string(),
                slug: "blog".to_string(),
                weight: 80,
                hidden: false,
                section: Some("Blog".to_string()),
                source_path: "/content/blog.md".to_string(),
            },
        ];
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/water.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
        };
        let html = render_content_nav(&ctx, "pages");
        // Active section is expanded
        assert!(
            html.contains("nav-section expanded"),
            "Expected expanded class on active section, got: {}", html
        );
        // Section title rendered
        assert!(
            html.contains("<h3 class=\"nav-section-title\">About</h3>"),
            "Expected About section title, got: {}", html
        );
        // Inactive section is not expanded
        assert!(
            html.contains("\"nav-section\""),
            "Expected collapsed Blog section, got: {}", html
        );
        // No <details> or <summary>
        assert!(!html.contains("<details"), "Should not use <details>: {}", html);
        assert!(!html.contains("<summary"), "Should not use <summary>: {}", html);

        // Exactly one section is expanded
        assert_eq!(
            html.matches("nav-section expanded").count(),
            1,
            "Exactly one section should be expanded, got: {}", html
        );

        // When current_path is the Blog page, Blog section is expanded, About is not.
        let ctx_blog = ShortcodeContext {
            current_path: "/blog.html".to_string(),
            ..ctx.clone()
        };
        let html_blog = render_content_nav(&ctx_blog, "pages");
        assert!(
            html_blog.contains("<div class=\"nav-section expanded\">\n  <h3 class=\"nav-section-title\">Blog</h3>"),
            "Blog section should be expanded on blog page, got: {}", html_blog
        );
        assert_eq!(
            html_blog.matches("nav-section expanded").count(),
            1,
            "Only Blog section should be expanded, got: {}", html_blog
        );

        // When no page is active (home page), the first section is expanded.
        let ctx_home = ShortcodeContext {
            current_path: "/index.html".to_string(),
            ..ctx.clone()
        };
        let html_home = render_content_nav(&ctx_home, "pages");
        assert!(
            html_home.contains("<div class=\"nav-section expanded\">\n  <h3 class=\"nav-section-title\">About</h3>"),
            "First section (About) should be expanded on home page, got: {}", html_home
        );
        assert_eq!(
            html_home.matches("nav-section expanded").count(),
            1,
            "Only one section expanded on home page, got: {}", html_home
        );
    }

    #[test]
    fn test_render_figure() {
        use crate::markdown::ShortcodeArgs;
        use std::collections::HashMap;

        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::new(),
            site_title: String::new(),
            current_path: "/".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
        };

        let args = ShortcodeArgs::from_map(HashMap::from([
            ("src".to_string(), "/img/photo.jpg".to_string()),
            ("caption".to_string(), "A nice photo".to_string()),
            ("float".to_string(), "right".to_string()),
        ]));

        let html = render_figure(&ctx, &args);
        assert!(html.contains("figure-right"), "Expected float right class: {}", html);
        assert!(html.contains("/img/photo.jpg"), "Expected src in img: {}", html);
        assert!(html.contains("<figcaption>A nice photo</figcaption>"), "Expected caption: {}", html);
    }
}
