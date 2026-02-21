# Caspar Water Website — Design Document

## Status

**All phases complete.** The content page system is fully implemented
in the sitegen crate, all 9 content pages are converted to markdown,
the site renders locally via `water/render.sh`, and test 207 passes
in Docker. 48 unit tests pass.

### What's Working

- Content route type in sitegen (config, routes, factory, layouts, shortcodes, markdown)
- 9 content pages with YAML frontmatter (title, weight, layout, section)
- Collapsible sidebar nav sections (About / Blog) with click-to-toggle JS
- Exactly one section expanded at a time (active page's section, or first section on home)
- Heading anchors on h2+ (h1 gets `id` but no `#` link)
- `figure` shortcode for images with float positioning
- `content_nav` shortcode with section grouping
- `water/render.sh` builds the site via `cargo run` (no Docker needed for dev)
- `testsuite/tests/207-sitegen-content-pages.sh` passes in Docker

### What Remains

- Images: copy images into pond, reference from content, configure static assets
- Data pages: wire up monitoring data exports alongside content pages
- CSS polish: typography, spacing, color refinement
- Mobile: responsive sidebar behavior
- Blog: date-based ordering when real blog posts are written

## Goal

Build the Caspar Water Company website using DuckPond's sitegen factory.
Content lives in the pond as data files (markdown). Each page appears in
the left-nav sidebar. The site combines prose content pages with
data-driven monitoring pages — all from one `site.yaml`, one `pond run`
invocation, zero external tooling.

## Content Inventory

Source: `water/content/` (local development) → `/content/` (in pond)

| File | Title | Section | Weight | Description |
|------|-------|---------|--------|-------------|
| `water.md` | Water | About | 10 | Water source, quality, aquifer capacity |
| `system.md` | System Design | About | 20 | Treatment process, infrastructure |
| `history.md` | Our History | About | 30 | Lumber town origins, evolution |
| `monitoring.md` | Monitoring | About | 40 | Instruments, OpenTelemetry, well depth |
| `operator.md` | Operator | About | 50 | Treatment and distribution operators |
| `political.md` | Political | About | 60 | Water district proposal, smart growth, traffic |
| `software.md` | Software | About | 70 | Open-source tools, DuckPond, Supruglue |
| `blog.md` | Blog | Blog | 80 | Placeholder — future blog content |
| `thanks.md` | Thanks | About | 90 | Acknowledgments |

All files are markdown with YAML frontmatter.

## Pond Layout (Development vs. In-Pond)

Development files live in `water/` and are copied into the pond by
`render.sh`. The pond layout mirrors the directory structure:

```
water/                              # Local development directory
├── render.sh                       # Build script (cargo run based)
├── site.yaml                       # Site configuration
├── content/                        # Content pages (markdown + frontmatter)
│   ├── water.md
│   ├── system.md
│   ├── history.md
│   ├── monitoring.md
│   ├── operator.md
│   ├── political.md
│   ├── software.md
│   ├── blog.md
│   └── thanks.md
├── site/                           # Templates and partials
│   ├── index.md                    # Home page
│   └── sidebar.md                  # Sidebar partial
└── dist/                           # Generated output (gitignored)
```

In the pond:

```
/
├── content/                        # Content pages (pond copy'd in)
│   ├── water.md
│   ├── system.md
│   └── ...
└── etc/
    ├── site.yaml                   # Site configuration (mknod)
    └── site/
        ├── index.md                # Home page template
        └── sidebar.md              # Sidebar partial
```

### Why `/content/` Not `/site/`?

Content pages are **content**, not configuration. They live in the
namespace alongside data. The `/site/` directory holds templates and
config — structural files that control *how* content is rendered.
Content pages are *what* gets rendered.

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
| `section` | No | String | Nav section grouping (e.g., "About", "Blog"). Pages with the same section are grouped under a collapsible heading |

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

The actual configuration (`water/site.yaml`):

```yaml
factory: sitegen

site:
  title: "Caspar Water"
  base_url: "/"

content:
  - name: "pages"
    pattern: "/content/*.md"

exports: []

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

partials:
  sidebar: "/site/sidebar.md"

static_assets: []
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

The sidebar renders from `sidebar.md`. It uses raw HTML for the site
title (to avoid heading anchor injection) and the `content_nav`
shortcode for page links.

### `/site/sidebar.md`

```markdown
<h2><a href="{{ base_url /}}">Caspar Water</a></h2>

{{ content_nav content="pages" /}}
```

### `content_nav` Shortcode

Renders a sectioned navigation from a content stage. Pages are grouped
by their `section` frontmatter field. Each section is a collapsible
div with a clickable title.

**Section behavior:** Exactly one section is expanded at all times.
When a content page is active, its section is expanded. When no content
page is active (e.g., home page), the first section is expanded. A
small inline script handles click-to-toggle — clicking a collapsed
section expands it and collapses the previously open one. Clicking the
already-expanded section does nothing.

Rendered HTML structure:

```html
<nav class="nav-list">
  <div class="nav-section expanded">
    <h3 class="nav-section-title">About</h3>
    <ul>
      <li><a href="/water.html">Water</a></li>
      <li><a href="/system.html">System Design</a></li>
      <li class="active"><a href="/history.html" aria-current="page">Our History</a></li>
      ...
    </ul>
  </div>
  <div class="nav-section">
    <h3 class="nav-section-title">Blog</h3>
    <ul>
      <li><a href="/blog.html">Blog</a></li>
    </ul>
  </div>
</nav>
```

CSS:
- `.nav-section > ul { display: none }` — collapsed by default
- `.nav-section.expanded > ul { display: block }` — expanded section shows items
- `.nav-section-title { cursor: pointer; user-select: none }` — clickable titles
- `.sidebar li.active a` — bold + accent color for active page

Pages without a `section` field render as top-level `<ul>` items
outside any section grouping.

## `page` Layout

Content pages use the `page` layout — sidebar + article, no CDN scripts.
A small inline script at the end of `<body>` handles section toggle.

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
                main class="content-page" {
                    article {
                        (PreEscaped(ctx.content))
                    }
                }
                script {
                    // Toggle nav sections — one expanded at a time
                    (PreEscaped(NAV_SECTION_TOGGLE_JS))
                }
            }
        }
    }
}
```

## Heading Anchors

Markdown headings get automatic `id` attributes and anchor links via
`inject_heading_anchors()`. The `slugify()` function converts heading
text to URL-safe IDs (lowercase, hyphens, strip non-alphanumeric).

- **h1**: gets `id` attribute only (no visible `#` link) — page titles
  should not have self-referencing anchors
- **h2+**: gets `id` attribute AND a clickable `#` link for deep linking

Example output for `## Water Quality`:
```html
<h2 id="water-quality"><a href="#water-quality">#</a> Water Quality</h2>
```

## `figure` Shortcode

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

### Phase 1: `content` Route Type (Rust) ✅

All Rust implementation is complete across 6 source files:

| File | Changes |
|------|---------|
| `config.rs` | `ContentStage` struct, `Content` variant in `RouteType`, `content` field on `RouteConfig`, `content` vec on `SiteConfig` |
| `routes.rs` | `ContentPage` struct (title, slug, weight, hidden, section, source_path), `ContentContext`, Content arm in `expand_route()` |
| `shortcodes.rs` | `content_nav` with section grouping + single-expanded logic, `figure` shortcode with float positioning |
| `layouts.rs` | `page` layout with sidebar + article + nav-toggle script |
| `factory.rs` | `Frontmatter` struct (title, weight, slug, hidden, section), `run_content_stages()`, wiring through `render_partial()` |
| `markdown.rs` | `inject_heading_anchors()` (h2+ only), `slugify()`, `ShortcodeArgs::from_map()` test helper |

48 unit tests pass.

Key structs as implemented:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentStage {
    pub name: String,
    pub pattern: String,
}

#[derive(Debug, Clone)]
pub struct ContentPage {
    pub title: String,
    pub slug: String,
    pub weight: i32,
    pub hidden: bool,
    pub section: Option<String>,
    pub source_path: String,
}

#[derive(Debug, Clone)]
pub struct ContentContext {
    pub pages: Vec<ContentPage>,
}
```

### Phase 2: Convert Content ✅

All 9 HTML fragments converted to markdown with YAML frontmatter.
Files live in `water/content/` and are `pond copy`'d into the pond
by `render.sh`. Each file has `section: About` or `section: Blog`.

### Phase 3: Wire Up `site.yaml` ✅

`water/site.yaml` is the working config. `water/render.sh` handles
the full build cycle: `pond init` → `pond copy` content + templates →
`pond mknod` site factory → `pond run /site.yaml build ./dist`.

The script uses a `pond()` shell function wrapping `cargo run` so
no Docker container is needed for local development.

### Phase 4: Images

Not yet started. Images need to be copied into the pond and
referenced from content pages. The `figure` shortcode is ready.

### Phase 5: Testsuite ✅

`testsuite/tests/207-sitegen-content-pages.sh` tests the content
page system end-to-end with synthetic data (alpha/beta/gamma pages).
Verifies HTML output, sidebar nav links in weight order, active page
highlighting, and page content rendering. Passes in Docker.

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

### Sections Instead of Flat Nav

The `section` frontmatter field groups pages under collapsible headings
in the sidebar. This was chosen over a flat list because the content
inventory has natural groupings (informational pages vs. blog posts).

Sections are rendered as `<div class="nav-section">` with CSS show/hide
and a small inline script for toggling. We explored and rejected
`<details>/<summary>` — the browser-native disclosure triangles look
dated and cannot be styled consistently.

**Invariant:** Exactly one section is expanded at all times. This is
enforced both at render time (server) and interactively (client JS).
When no page is active, the first section opens. Clicking the
already-open section does nothing.

### Sidebar Title as Raw HTML

The sidebar heading uses raw `<h2><a href="...">Caspar Water</a></h2>`
instead of markdown `## [Caspar Water](...)`. This avoids the heading
anchor injection that would add a `#` link to the site title.

### No Subdirectory Nesting (Yet)

The initial design handles a flat list of content pages. Nested
subdirectories (e.g., `/content/political/water-district.md`) can be
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

## Local Development

```bash
cd water
./render.sh            # Build and open in browser (Vite on port 4175)
./render.sh --no-open  # Build without opening browser
```

The script uses hostmount (`pond run -d ./water`) — no pond directory
needed. It runs a single command:

```bash
pond run -d "${SCRIPT_DIR}" host+sitegen:///site.yaml build "${OUTDIR}"
```

This reads `water/site.yaml` and templates directly from the host filesystem.
