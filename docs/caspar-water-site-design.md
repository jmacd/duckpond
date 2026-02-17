# Caspar Water Website — Design Document

## Goal

Build the Caspar Water Company website using DuckPond's sitegen factory.
Content lives in the pond as data files (markdown). Each page appears in
the left-nav sidebar. The site combines prose content pages with
data-driven monitoring pages — all from one `site.yaml`, one `pond run`
invocation, zero external tooling.

## Content Inventory

Source: `/Volumes/sourcecode/src/caspar.water/site/content/`

| File | Title | Description |
|------|-------|-------------|
| `water.md` | Water | Water source, quality, aquifer capacity |
| `system.md` | System Design | Treatment process, infrastructure |
| `history.md` | Our History | Lumber town origins, evolution |
| `monitoring.md` | Monitoring | Instruments, OpenTelemetry, well depth |
| `operator.md` | Operator | Treatment and distribution operators |
| `political.md` | Political | Water district proposal, smart growth, traffic |
| `software.md` | Software | Open-source tools, DuckPond, Supruglue |
| `blog.md` | Blog | Placeholder — future blog content |
| `thanks.md` | Thanks | Acknowledgments |

These files are currently HTML `<section>` fragments. They will be
converted to markdown with YAML frontmatter and stored in the pond
at `/pages/`.

## Pond Layout

```
/
├── pages/                          # Content pages (data files, raw markdown bytes)
│   ├── water.md
│   ├── system.md
│   ├── history.md
│   ├── monitoring.md
│   ├── operator.md
│   ├── political.md
│   ├── software.md
│   ├── blog.md
│   └── thanks.md
├── pages/img/                      # Images referenced by content
│   ├── caspar-fire-1891.jpg
│   ├── district-proposed.jpg
│   ├── system-proposed.jpg
│   └── septic-proposed.jpg
├── sensors/                        # Synthetic/real sensor data (existing)
├── combined/                       # Joined timeseries (existing)
├── reduced/                        # Reduced timeseries (existing)
└── etc/
    ├── site.yaml                   # Site configuration
    ├── site/
    │   ├── index.md                # Home page template
    │   ├── sidebar.md              # Sidebar partial
    │   ├── param.md                # Per-parameter data page template
    │   └── site.md                 # Per-site data page template
    └── static/                     # Static assets (if any beyond builtins)
```

### Why `/pages/` Not `/etc/site/`?

Content pages are **content**, not configuration. They live in the
pond's main namespace alongside data, not buried under `/etc/`. The
`/etc/site/` directory holds templates and config — structural files
that control *how* content is rendered. Content pages are *what* gets
rendered.

This also means content pages are first-class pond citizens: you can
`pond copy` them in and out, `pond cat` them, and manage them with
the same tools used for everything else.

## Content Page Format

Each page is a markdown file with YAML frontmatter:

```markdown
---
title: "Our History"
weight: 30
---

The Caspar Water System has roots dating back to the town's origins
as a lumber community in the mid-19th century. Caspar grew around
the Caspar Lumber Company, which operated until 1955.

{{ figure src="/img/caspar-fire-1891.jpg" caption="This 1891 fire
insurance map shows a 1½-inch fire hydrant on modern-day Caspar
Frontage Road West." float="right" /}}

In its heyday, Caspar was a thriving company town on the Mendocino
coast, with a sawmill, railway system, and infrastructure to support
its workers...
```

### Frontmatter Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `title` | Yes | String | Page title, used in nav and `<title>` |
| `weight` | No | Integer | Sort order in nav (lower = higher). Default: 100 |
| `layout` | No | String | Layout name. Default: `"page"` |
| `slug` | No | String | URL slug. Default: derived from filename |
| `hidden` | No | Bool | If true, page renders but does not appear in nav. Default: false |

### Ordering via Weight

Weight determines sidebar ordering. Suggested initial assignments:

| Weight | Page | Rationale |
|--------|------|-----------|
| 10 | Water | Primary topic — what we do |
| 20 | System Design | How the system works |
| 30 | History | Context |
| 40 | Monitoring | Technical operations |
| 50 | Operator | Who runs it |
| 60 | Political | Vision and proposals |
| 70 | Software | Technical details |
| 80 | Blog | Future content |
| 90 | Thanks | End matter |

Weight provides stable ordering without depending on filenames. Pages
with equal weight fall back to alphabetical by title.

## Site Configuration

```yaml
# /etc/site.yaml

site:
  title: "Caspar Water"
  base_url: "/"

content:
  - name: "pages"
    pattern: "/pages/*.md"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    target_points: 1500

  - name: "sites"
    pattern: "/reduced/single_site/*/*.series"
    target_points: 1500

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
    routes:
      - name: "pages"
        type: content
        content: "pages"

      - name: "data"
        type: static
        slug: "data"
        page: "/etc/site/data-index.md"
        routes:
          - name: "params"
            type: static
            slug: "params"
            page: "/etc/site/params.md"
            routes:
              - name: "param"
                type: template
                slug: "$0"
                export: "params"
                page: "/etc/site/param.md"

          - name: "sites"
            type: static
            slug: "sites"
            page: "/etc/site/sites.md"
            routes:
              - name: "site"
                type: template
                slug: "$0"
                export: "sites"
                page: "/etc/site/site.md"

partials:
  sidebar: "/etc/site/sidebar.md"

static:
  - pattern: "/etc/static/*"
```

### New Top-Level: `content`

The `content` section declares content stages, analogous to `exports`
for data. Each content stage globs data files in the pond and reads
their frontmatter metadata:

```yaml
content:
  - name: "pages"
    pattern: "/pages/*.md"
```

A `content` route references a content stage by name:

```yaml
- name: "pages"
  type: content
  content: "pages"       # references the content stage
```

### How `content` Routes Expand

1. Glob `/pages/*.md` — finds 9 data files
2. Read each file's bytes, parse frontmatter (title, weight, slug)
3. Sort by weight (ascending), then title (alphabetical)
4. Generate one `PageJob` per file:
   - `slug` from frontmatter or filename (e.g., `history.md` → `history`)
   - `output_path`: `{slug}.html` (e.g., `history.html`)
   - `page_source`: the content file itself — body IS the page
5. Populate collection `"pages"` with `(title, slug, weight)` tuples for nav

### Output Structure

```
dist/
├── index.html                      # Home page
├── water.html                      # Content page
├── system.html                     # Content page
├── history.html                    # Content page
├── monitoring.html                 # Content page
├── operator.html                   # Content page
├── political.html                  # Content page
├── software.html                   # Content page
├── blog.html                       # Content page
├── thanks.html                     # Content page
├── img/                            # Copied images
│   ├── caspar-fire-1891.jpg
│   └── ...
├── data/                           # Exported timeseries (existing)
│   └── single_param/...
├── data/
│   ├── params/
│   │   ├── index.html
│   │   ├── Temperature.html
│   │   └── DO.html
│   └── sites/
│       ├── index.html
│       ├── NorthDock.html
│       └── SouthDock.html
├── style.css                       # Built-in
└── chart.js                        # Built-in
```

## Sidebar

The sidebar shows two sections: content pages and data pages.

### `/etc/site/sidebar.md`

```markdown
## 💧 Caspar Water

{{ content_nav content="pages" /}}

### Monitoring Data

{{ nav_list collection="params" base="/data/params" /}}

{{ nav_list collection="sites" base="/data/sites" /}}
```

### New Shortcode: `content_nav`

Like `nav_list` but renders from a content stage instead of an export
stage. It has access to titles and weights, so it renders a properly
ordered, titled navigation:

```html
<ul class="nav-list">
  <li><a href="/water.html">Water</a></li>
  <li><a href="/system.html">System Design</a></li>
  <li><a href="/history.html">Our History</a></li>
  <li class="active"><a href="/monitoring.html" aria-current="page">Monitoring</a></li>
  ...
</ul>
```

Active-page highlighting works the same way as existing `nav_list` —
the shortcode compares each link's URL against `ctx.current_path`.

## New Layout: `page`

Content pages get a `page` layout — similar to `default` but with a
sidebar and article formatting:

```rust
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
                main {
                    article {
                        (PreEscaped(ctx.content))
                    }
                }
            }
        }
    }
}
```

No CDN scripts — content pages are pure prose. The `data` layout
(with chart.js, DuckDB-WASM) remains for data pages.

## New Shortcode: `figure`

For images with captions and float positioning. Replaces the inline
`<div style="float: right; ...">` pattern in the existing HTML.

```markdown
{{ figure src="/img/caspar-fire-1891.jpg" caption="This 1891 fire
insurance map..." float="right" /}}
```

Renders:

```html
<figure class="figure-right">
  <a href="/img/caspar-fire-1891.jpg" target="_blank">
    <img src="/img/caspar-fire-1891.jpg" alt="This 1891 fire insurance map...">
  </a>
  <figcaption>This 1891 fire insurance map...</figcaption>
</figure>
```

CSS classes: `figure-right`, `figure-left`, `figure-full`. The `alt`
text defaults to caption text.

## Content Migration

The 9 HTML fragments need conversion to markdown with frontmatter.
Most content is straightforward: paragraphs, lists, links, headings.

### Patterns That Need Handling

| HTML Pattern | Markdown Equivalent |
|---|---|
| `<p>text</p>` | Plain paragraph |
| `<ul><li>` | `- item` |
| `<strong>text</strong>` | `**text**` |
| `<em>text</em>` | `*text*` |
| `<a href="...">text</a>` | `[text](...)` |
| `<h2>`, `<h4>` | `##`, `####` |
| `<div style="float:right">` + `<img>` + caption | `{{ figure ... /}}` shortcode |
| `<div class="indented"><p><b>...</b><br><em>...</em></p></div>` | Blockquote: `> **title**\n> *text*` |
| `<div class="science">` | Inline HTML (passed through by pulldown-cmark) |
| `🚧 emoji` | Passes through as-is |

### Example Conversion: `history.md`

**Before** (HTML fragment):
```html
<section id="history" class="content-section">
  <h2 class="section-title">Our History</h2>
  <p>The Caspar Water System has roots...</p>
  <div style="float: right; margin-left: 20px; max-width: 500px;">
    <a href="./img/caspar-fire-1891.jpg" target="_blank">
      <img src="./img/caspar-fire-1891.jpg" alt="Caspar Fire 1891" ...>
    </a>
    <p style="font-size: 0.75rem;">This 1891 fire insurance map...</p>
  </div>
  <p>In its heyday...</p>
</section>
```

**After** (markdown with frontmatter):
```markdown
---
title: "Our History"
weight: 30
---

The Caspar Water System has roots dating back to the town's origins
as a lumber community in the mid-19th century. Caspar grew around
the Caspar Lumber Company, which operated until 1955.

{{ figure src="/img/caspar-fire-1891.jpg" caption="This 1891 fire
insurance map shows a 1½-inch fire hydrant on modern-day Caspar
Frontage Road West." float="right" /}}

In its heyday, Caspar was a thriving company town on the Mendocino
coast, with a sawmill, railway system, and infrastructure to support
its workers.
```

## Implementation Plan

### Phase 1: `content` Route Type (Rust)

| File | Change |
|------|--------|
| `config.rs` | Add `Content` variant to `RouteType`. Add `content` field to `RouteConfig`. Add `ContentStage` struct and `content` vec to `SiteConfig` |
| `routes.rs` | Handle `RouteType::Content` in `expand_route()` — glob files, parse frontmatter, sort by weight, emit `PageJob` per file. Add content collections to `build_collections()` |
| `shortcodes.rs` | Add `content_nav` shortcode. Add `figure` shortcode |
| `layouts.rs` | Add `page` layout |
| `factory.rs` | Extend `Frontmatter` struct (weight, slug, hidden). Wire content stages into file cache and route expansion. Pass content collections alongside export collections |

New structs:

```rust
/// A content stage: globs data files, reads frontmatter metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentStage {
    pub name: String,
    pub pattern: String,
}

/// Metadata extracted from a content page's frontmatter.
#[derive(Debug, Clone)]
pub struct ContentPage {
    pub title: String,
    pub slug: String,
    pub weight: i32,
    pub hidden: bool,
    pub source_path: String,   // pond path to the markdown file
}

/// All discovered content pages for one content stage.
#[derive(Debug, Clone)]
pub struct ContentContext {
    /// Pages sorted by (weight, title)
    pub pages: Vec<ContentPage>,
}
```

Route expansion for `Content`:

```rust
RouteType::Content => {
    let content_name = route.content.as_deref().unwrap_or("");
    let content_ctx = match content_stages.get(content_name) {
        Some(ctx) => ctx,
        None => return,
    };

    for page in &content_ctx.pages {
        let url_path = format!("{}/{}", parent_path, page.slug);
        let output_path = format!("{}.html", url_path.trim_start_matches('/'));

        let mut breadcrumbs = parent_breadcrumbs.to_vec();
        breadcrumbs.push((page.title.clone(), prefix_base(base_url, &url_path)));

        jobs.push(PageJob {
            output_path,
            page_source: page.source_path.clone(),
            captures: vec![],
            datafiles: vec![],
            breadcrumbs,
        });
    }
}
```

### Phase 2: Convert Content

Convert the 9 HTML fragments to markdown files with frontmatter.
Copy into the pond with `pond copy`.

### Phase 3: Wire Up `site.yaml`

Write the site config for Caspar Water, create sidebar partial,
create home page, run `pond run /etc/site.yaml build ./dist`.

### Phase 4: Images

Copy images into pond at `/pages/img/`. Reference from content pages.
The `figure` shortcode handles rendering. Static asset config copies
images to `dist/img/`.

### Phase 5: Testsuite

Write a test (`testsuite/tests/207-sitegen-content-pages.sh`) that:
1. Creates a pond with a few content pages
2. Configures a site.yaml with a content route
3. Runs `pond run /etc/site.yaml build`
4. Verifies output HTML files exist
5. Verifies sidebar contains nav links in weight order
6. Verifies page content is rendered

## Design Decisions

### Content vs. Template: Why Not Reuse Template Routes?

Template routes are fundamentally **data-driven**: they require an
export stage that produces `ExportedFile` structs with temporal
partitions, capture groups, and time ranges. Content pages have none
of that. They're **prose files** with titles and ordering.

Forcing content pages through the export pipeline would mean:
- Fake export stages that produce empty file lists
- Frontmatter parsing bolted onto a system designed for parquet metadata
- Ordering by weight shoehorned into alphabetical BTreeMap iteration

A `content` route type keeps each concern clean.

### Content Stage vs. Inline Pattern

The `content` section in site.yaml is separate from `routes` for the
same reason `exports` is separate: the glob and metadata extraction
happen **before** route expansion. The results feed into both route
generation AND sidebar navigation. If the pattern were inline on the
route, the sidebar shortcode wouldn't have access to the page list
at render time.

### Slugs from Filenames

Deriving the URL slug from the filename (stripping `.md`) is the
simplest default and matches the user's mental model: `water.md` →
`/water.html`. Frontmatter `slug` override is there for special
cases but shouldn't be needed often.

### No Subdirectory Nesting (Yet)

The initial design handles a flat list of content pages. Nested
subdirectories (e.g., `/pages/political/water-district.md`) can be
added later if the site grows. The `content` route type and
`ContentContext` struct are designed to support this — `pattern` could
use `**/*.md` — but sorting and nav rendering would need hierarchy
awareness.

### No Dates (Yet)

Blog posts will eventually need `date` frontmatter for
reverse-chronological ordering. The `weight` field handles the
current use case (manually ordered pages). When blog functionality
is added, a `blog` content stage with date-aware sorting is a
natural extension.

### HTML Passthrough

pulldown-cmark passes raw HTML through unchanged. Content pages that
have complex HTML (the chemistry equations in `system.md`, for
example) can use inline HTML alongside markdown. No special handling
needed.
