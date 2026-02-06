# Sitegen Factory Design Document

## Overview

This document describes the design for `crates/sitegen`, a new DuckPond crate that uses [Maudit](https://maudit.org/) as a library to provide static site generation. The goal is to replace the current Tera-based `TemplateFactory` with a Rust-native solution that:

1. Uses **Markdown** for content authoring (not HTML templates)
2. Derives **routes from DuckPond patterns** (not manually defined)
3. Provides **type-safe templating** via Maud (optional)
4. Keeps all content and configuration **inside the pond**

### Maudit vs Maud

These are **unrelated projects** with similar names:

| Library | What it is | Used for |
|---------|------------|----------|
| **Maudit** | Rust static site generator | Core SSG engine, routing, markdown rendering |
| **Maud** | Rust HTML macro (`html! { ... }`) | Type-safe HTML in layouts (optional) |

Maudit has built-in integration with Maud as one of its templating options, but we can also use pure Markdown or other engines. This design focuses on Maudit as the SSG foundation.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              DuckPond                                    │
│                                                                          │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────┐ │
│  │ Data Files       │   │ Templates        │   │ Config               │ │
│  │ /reduced/**      │   │ /etc/templates/  │   │ /etc/site.yaml       │ │
│  │ *.series         │   │ *.md             │   │                      │ │
│  └────────┬─────────┘   └────────┬─────────┘   └───────────┬──────────┘ │
│           │                      │                         │            │
│           └──────────────────────┴─────────────────────────┘            │
│                                  │                                       │
│                                  ▼                                       │
│                    ┌─────────────────────────────┐                      │
│                    │      SitegenFactory         │                      │
│                    │      (crates/sitegen)        │                      │
│                    │                             │                      │
│                    │  1. Pattern match data      │                      │
│                    │  2. Interpolate templates   │                      │
│                    │  3. Generate routes         │                      │
│                    │  4. Call Maudit coronate()  │                      │
│                    └──────────────┬──────────────┘                      │
│                                   │                                      │
└───────────────────────────────────┼──────────────────────────────────────┘
                                    │
                                    ▼
                          ┌─────────────────┐
                          │   dist/         │
                          │   ├── index.html│
                          │   ├── params/   │
                          │   ├── sites/    │
                          │   └── assets/   │
                          └─────────────────┘
```

## Configuration Schema

```yaml
# /etc/site.yaml

site:
  title: "Noyo Harbor Blue Economy"
  base_url: "/"

# Export stages - specify what data to export and how
# These run first, producing file manifests with temporal partitioning
exports:
  - name: "params"
    pattern: "/reduced/single_param/**/*.series"
    temporal: ["year", "month"]               # Partition by year/month
    
  - name: "sites"
    pattern: "/reduced/single_site/**/*.series"
    temporal: ["year", "month"]

# Hierarchical route tree - URLs are built from nesting
# Routes reference exports to get their data context
routes:
  - name: "home"
    type: static
    slug: ""                                    # Root: /index.html
    page: "/etc/site/index.md"
    routes:
      - name: "params"
        type: static
        slug: "params"                          # /params/index.html
        page: "/etc/site/params.md"
        routes:
          - name: "param"
            type: template
            slug: "$0"                          # /params/Temperature.html
            export: "params"                    # Links to export stage
            page: "/etc/site/param.md"
            
      - name: "sites"
        type: static
        slug: "sites"                           # /sites/index.html
        page: "/etc/site/sites.md"
        routes:
          - name: "site"
            type: template
            slug: "$0"                          # /sites/BDock.html
            export: "sites"                     # Links to export stage
            page: "/etc/site/site.md"

# Partials included by pages (not routes)
partials:
  sidebar: "/etc/site/sidebar.md"

# Static assets to copy
static:
  - pattern: "/etc/static/*"
```

### Build Command

```bash
pond run /etc/site.yaml build ./dist
```

This single command:
1. Reads the site definition
2. Runs all export stages (exports data files with temporal partitioning)
3. Builds file manifests with timestamps
4. Renders all routes with manifest context
5. Outputs HTML + data files to `./dist`

### Route Types

| Type | Has `pattern` | Has `slug` | Produces |
|------|---------------|------------|----------|
| `static` | No | Yes | One page at that path |
| `template` | Yes | Yes (with `$n`) | One page per unique `$n` value |

### Route Expansion Example

Given this config fragment:
```yaml
exports:
  - name: "params"
    pattern: "/reduced/single_param/**/*.series"
    temporal: ["year", "month"]

routes:
  - name: "params"
    type: static
    slug: "params"
    page: "/etc/site/params.md"
    routes:
      - name: "param"
        type: template
        slug: "$0"
        export: "params"                    # Links to export stage
        page: "/etc/site/param.md"
```

The export stage matches:
- `/reduced/single_param/Temperature/res=1h/data.series` → `$0 = "Temperature"`, `$1 = "res=1h"`
- `/reduced/single_param/Temperature/res=4h/data.series` → `$0 = "Temperature"`, `$1 = "res=4h"`
- `/reduced/single_param/DO/res=1h/data.series` → `$0 = "DO"`, `$1 = "res=1h"`

The build produces:
```
dist/
├── params/
│   ├── index.html                      (from static "params" route)
│   ├── Temperature.html                (from template, $0="Temperature")
│   ├── Temperature/
│   │   └── res=1h/
│   │       └── year=2025/
│   │           └── month=01/
│   │               └── data.parquet    (exported with temporal partitioning)
│   └── DO.html                         (from template, $0="DO")
│   └── DO/
│       └── ...
```

Each template page's shortcode context receives its `&[ExportedFile]` directly — typed Rust structs, no serialization step.

### Current vs New Mapping

| Current `template.yaml` | New `site.yaml` | Output |
|-------------------------|-------------------|--------|
| `name: params`, `out: $0.html` | `slug: "$0"` under `slug: "params"` | `/params/Temperature.html` |
| `name: sites`, `out: $0.html` | `slug: "$0"` under `slug: "sites"` | `/sites/BDock.html` |
| `name: index`, `out: index.html` | `type: static`, `slug: ""` | `/index.html` |
| `name: page` (sidebar) | `partials.sidebar` | Included, not a route |

## Template Format

### Example: `/etc/site/param.md`

Template for individual parameter pages (e.g., `/params/Temperature.html`).

```markdown
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ breadcrumb }}

{{ time-picker }}

{{ chart }}
```

### Example: `/etc/site/site.md`

Template for individual site pages (e.g., `/sites/BDock.html`).

```markdown
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }} Monitoring Station

{{ breadcrumb }}

{{ time-picker }}

{{ chart }}
```

### Example: `/etc/site/sidebar.md`

The sidebar is a **partial** included by layouts. It receives the full navigation context.

```markdown
## 🌊 Noyo Harbor

### Overview
- [Home](/)

### By Parameter
{{ nav-list collection="params" path="/params" }}

### By Site
{{ nav-list collection="sites" path="/sites" }}
```

### Example: `/etc/site/index.md`

Static home page at root.

```markdown
---
title: "Noyo Harbor Blue Economy"
layout: default
---

# Noyo Harbor Blue Economy

Water Quality Monitoring Dashboard

{{ map }}

## About This Project

The Noyo Harbor Blue Economy project monitors water quality at key locations...

- **Dissolved Oxygen (DO)** - Critical for marine life health
- **Salinity** - Indicates freshwater/seawater mixing  
- **Temperature** - Affects oxygen levels and species habitat
```

### Context Available to Templates

All templates receive implicit context from the route configuration and pattern matching:

| Variable | Type | Description |
|----------|------|-------------|
| `$0`, `$1`, ... | `String` | Capture groups from pattern (for template routes) |
| `datafiles` | `&[ExportedFile]` | All matched files for this route, with captures and time ranges |
| `params` | `&[String]` | All unique param names (from all template routes, for navigation) |
| `sites` | `&[String]` | All unique site names (from all template routes, for navigation) |
| `site` | `&SiteConfig` | Site-wide config from site.yaml |

For a template route with `export: "params"`, each generated page receives:
- `$0` = the unique value for this page (e.g., "Temperature")
- `datafiles` = `&[ExportedFile]` — all exported files where `$0 == "Temperature"`:
  ```rust
  [
    ExportedFile { path: "params/Temperature/res=1h/...", captures: ["Temperature", "res=1h"], .. },
    ExportedFile { path: "params/Temperature/res=4h/...", captures: ["Temperature", "res=4h"], .. },
  ]
  ```

---

# Open Questions

## Q1: Template Interpolation Syntax

~~What syntax should we use for variable interpolation in Markdown templates?~~

**DECIDED: Shortcodes only**

All dynamic content uses shortcode syntax `{{ name attr="value" }}`. No separate variable interpolation syntax.

Examples:
- `{{ $0 }}` - outputs the first capture group value
- `{{ $1 }}` - outputs the second capture group
- `{{ nav-list collection="params" }}` - outputs a navigation list
- `{{ chart }}` - outputs a chart container with datafiles

This keeps markdown clean and provides a single, consistent mechanism for all dynamic content.

---

## Q2: Loop and Conditional Syntax

~~How should we handle iteration and conditionals?~~

**DECIDED: Shortcodes handle loops**

No loop syntax in markdown. Use shortcodes that encapsulate iteration:

```markdown
### By Parameter
{{ nav-list collection="params" path="/params" }}
```

The `nav-list` shortcode knows how to iterate over the collection and emit markdown links.

**Charts are not a loop problem.** A `{{ chart }}` shortcode receives all `&[ExportedFile]` for the page. Whether to render one multi-series chart or multiple panels is a client-side JS decision, not a template decision. The markdown stays `{{ chart }}` — one shortcode, one container, all data.

For conditionals, either:
- Define separate templates for different cases
- Use shortcodes that conditionally render: `{{ if-param value="Temperature" }}...{{ /if-param }}`
- Handle in layouts (Maud/HTML)

---

## Q3: Shortcode Implementation

~~Where does shortcode logic live?~~

**DECIDED: Built-in shortcodes in Rust**

Shortcodes like `{{ $0 }}`, `{{ chart }}`, `{{ map }}`, `{{ nav-list }}` are implemented in the `crates/sitegen` Rust code.

```rust
shortcodes.register("$0", |_, ctx| ctx.captures[0].clone());
shortcodes.register("$1", |_, ctx| ctx.captures[1].clone());

shortcodes.register("chart", |attrs, ctx| {
    // ctx.datafiles is Vec<ExportedFile> — typed structs, not JSON
    // shortcode decides how to get file info to client-side JS
    ChartWidget::render(&ctx.datafiles)
});

shortcodes.register("nav-list", |attrs, ctx| {
    let collection = attrs.get("collection").unwrap(); // "params" or "sites"
    let path = attrs.get("path").unwrap();             // "/params" or "/sites"
    let items = ctx.get_collection(collection);
    items.iter()
        .map(|item| format!("- [{}]({}/{})", item, path, item))
        .collect::<Vec<_>>()
        .join("\n")
});
```

**Pros**: Type-safe, fast, full access to DuckPond context  
**Future**: Can add pond-based custom shortcodes later if needed

---

## Q4: Layout System

~~How should layouts wrap Markdown content?~~

**DECIDED: Maud layouts in Rust, selected by frontmatter**

Layouts are Maud functions in the DuckPond codebase. The markdown frontmatter `layout: name` selects which one to use.

```rust
// Built-in layouts
fn data_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                title { (ctx.title) }
                link rel="stylesheet" href="/style.css";
            }
            body {
                (PreEscaped(&ctx.sidebar))
                main {
                    (PreEscaped(&ctx.content))
                }
                script src="/lib.js" {}
            }
        }
    }
}

fn default_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                title { (ctx.title) }
                link rel="stylesheet" href="/style.css";
            }
            body {
                (PreEscaped(&ctx.sidebar))
                main.hero {
                    (PreEscaped(&ctx.content))
                }
                script src="/lib.js" {}
            }
        }
    }
}

// Dispatch based on frontmatter
fn apply_layout(name: &str, ctx: &LayoutContext) -> Markup {
    match name {
        "data" => data_layout(ctx),
        "default" | _ => default_layout(ctx),
    }
}
```

Markdown selects layout:
```markdown
---
title: "{{ $0 }}"
layout: data
---
```

**Pros**: Type-safe, consistent with shortcode decision, refactorable  
**Future**: Could add pond-based layouts later if needed

---

## Q5: Build Trigger

~~How is the site build triggered?~~

**DECIDED: `pond run` with site definition**

Use DuckPond's factory command entry point:

```bash
pond run /etc/site.yaml build ./dist
```

The site definition includes **export stages** that specify what data to export and how. This replaces the manual `export.sh` script:

```yaml
# /etc/site.yaml

site:
  title: "Noyo Harbor Blue Economy"
  base_url: "/"

# Export stages - run before rendering, data becomes template context
exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    temporal: ["year", "month"]
    
  - name: "sites"
    pattern: "/reduced/single_site/*/*.series"
    temporal: ["year", "month"]

# Routes reference exports by name
routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
    routes:
      - name: "param"
        type: template
        slug: "$0"
        export: "params"                        # Links to export stage above
        page: "/etc/site/param.md"
        
      - name: "site"
        type: template
        slug: "$0"
        export: "sites"
        page: "/etc/site/site.md"

static:
  - pattern: "/etc/static/*"
```

### Export Stage Output

Each export stage produces **typed Rust structs** that accumulate into route context. No JSON serialization boundary — shortcodes receive structs directly.

```rust
/// One exported file with its temporal partition and time range
struct ExportedFile {
    path: PathBuf,              // relative to dist/, e.g. "params/Temperature/res=1h/year=2025/month=01/data.parquet"
    captures: Vec<String>,      // $0 = "Temperature", $1 = "res=1h"
    temporal: BTreeMap<String, String>,  // {"year": "2025", "month": "01"}
    start_time: i64,
    end_time: i64,
}

/// All exported files for one route, grouped by $0 value
struct ExportContext {
    name: String,                               // "params"
    by_key: BTreeMap<String, Vec<ExportedFile>>, // keyed by $0
}
```

When rendering the `Temperature` page, the shortcode context contains:

```rust
// ctx.datafiles: &[ExportedFile] — all files where $0 == "Temperature"
[
    ExportedFile { path: "params/Temperature/res=1h/year=2025/month=01/data.parquet",
                   captures: ["Temperature", "res=1h"],
                   temporal: {"year": "2025", "month": "01"},
                   start_time: 1704067200, end_time: 1706745599 },
    ExportedFile { path: "params/Temperature/res=1h/year=2025/month=02/data.parquet",
                   captures: ["Temperature", "res=1h"],
                   temporal: {"year": "2025", "month": "02"},
                   start_time: 1706745600, end_time: 1709251199 },
]
```

### Shortcodes Receive Structs Directly

The `{{ chart }}` shortcode has typed access to the data — it decides how to convey file info to client-side JS:

```rust
shortcodes.register("chart", |attrs, ctx| {
    // ctx.datafiles is &[ExportedFile] — no deserialization needed
    // The shortcode decides the HTML representation:
    // data attributes, inline script, separate manifest file, etc.
    ChartWidget::render(&ctx.datafiles)
});
```

The key insight: the **shortcode** is the serialization boundary, not the template system. Each shortcode decides its own client-side data strategy.

---

## Q6: Datafile Manifest Format

~~How are datafiles passed to templates/client?~~

**DECIDED: No manifest — structs flow directly into shortcodes**

Export stages produce `Vec<ExportedFile>` structs. These accumulate into route context as the build progresses. Shortcodes receive typed `&[ExportedFile]` — no serialization step between export and rendering.

The question of how data reaches **client-side JS** is an implementation detail of each shortcode, not the template system. The `{{ chart }}` shortcode might use data attributes, an inline `<script>`, or a separate `.json` file — that's its business.

```rust
// The shortcode IS the serialization boundary
impl ChartWidget {
    fn render(files: &[ExportedFile]) -> String {
        // Could emit data-attributes:
        //   <div class="chart" data-files='[...]'></div>
        // Could emit inline script:
        //   <script>window.chartData = [...]</script>
        // Could write a sidecar file and reference it:
        //   <div class="chart" data-src="Temperature.json"></div>
        // Decision is local to this shortcode, not global.
    }
}
```

This avoids a global "manifest format" decision and lets each widget optimize its own data delivery.

---

## Q7: Asset Pipeline

~~How are CSS/JS assets handled?~~

**DECIDED: Static copy, no JS toolchain**

Assets in `/etc/static/` are copied verbatim to `dist/`. No bundler, no Node.js, no npm.

```yaml
# In site.yaml
static:
  - pattern: "/etc/static/*"
```

Produces:
```
dist/
├── style.css          (copied from /etc/static/style.css)
├── chart.js           (copied from /etc/static/chart.js — glue code)
├── params/
│   └── ...
```

Vendor libraries (DuckDB-WASM, charting lib) are loaded via CDN `<script>` tags hardcoded in the Maud layout:

```rust
fn data_layout(ctx: &LayoutContext) -> Markup {
    html! {
        head {
            // ...
            link rel="stylesheet" href="/style.css";
            script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/..." {}
            script src="https://cdn.jsdelivr.net/npm/@observablehq/plot/..." {}
        }
        body {
            // ...
            script src="/chart.js" {}  // our glue code, static copy
        }
    }
}
```

**Why**: The site needs two vendor libs (loaded pre-built from CDN) and ~100 lines of glue JS. No import trees, no transpilation, no tree-shaking needed. Zero JS toolchain keeps the build pure Rust.

---

## Q8: Development Workflow

~~How do we iterate on templates during development?~~

**DECIDED: Build to dist, serve with Vite**

Two-step workflow: DuckPond builds the site, Vite serves it for dev iteration.

```bash
# 1. Build the site
pond run /etc/site.yaml build ./dist

# 2. Serve locally with hot reload on file changes
npx vite dist
```

Vite adds zero config file serving with live reload — when you re-run the build, the browser refreshes. No Vite config needed, it just serves a static directory.

**Why**: Keeps the build pure Rust (`pond run`). Vite is only a dev-time convenience for serving static files with auto-reload — it never touches the build pipeline. Could also use `python -m http.server` if you don't want Node at all, just without live reload.

---

## Q9: Error Handling

~~What happens when templates have errors?~~

**DECIDED: Build fails with error message**

Template errors are build errors. The build stops and reports the file, line, and what went wrong.

```
Error in /etc/site/param.md:12
  Unknown shortcode: $site (did you mean $0?)

Error in /etc/site/index.md:8
  Shortcode "map" failed: no map data configured
```

No error pages, no fallbacks. If the site doesn't build cleanly, you fix it before deploying. Same philosophy as a compiler error.

---

## Q10: Integration Depth

~~How tightly should SitegenFactory integrate with Maudit?~~

**DECIDED: Use Maudit as a library**

Call `maudit::coronate()` (or build a custom entrypoint per the [library guide](https://maudit.org/docs/library/)) directly from `crates/sitegen`. SitegenFactory constructs the routes, content sources, and build options, then hands them to Maudit's build pipeline.

```rust
impl SitegenFactory {
    fn build(&self, output_dir: &Path) -> Result<()> {
        // 1. Run export stages → Vec<ExportContext>
        // 2. Expand routes → Vec<dyn FullRoute>
        // 3. Build with Maudit
        let options = BuildOptions {
            output_dir: output_dir.to_path_buf(),
            base_url: self.config.site.base_url.clone(),
            ..Default::default()
        };
        
        maudit::coronate(&self.routes, self.content_sources(), &options)?;
        Ok(())
    }
}
```

**Why**: Full control over routing and content loading while leveraging Maudit's markdown rendering, asset handling, and page generation. Single Rust binary, no external tools. The [library guide](https://maudit.org/docs/library/) shows exactly how to replace `coronate()` with a custom entrypoint if we need finer control later.

---

## Next Steps

1. ~~Resolve open questions above~~ ✅ All decided
2. Create `crates/sitegen` with `maudit` and `maud` dependencies
3. Implement `SitegenFactory` struct and `Directory` trait
4. Create example templates in `noyo/` test data
5. Write integration tests
6. Migrate noyo-harbor-site to new system

---

## Appendix: Current TemplateFactory Behavior

For reference, the current system:

1. **Pattern matching**: `in_pattern` globs files in pond
2. **Capture groups**: `**` and `*` become `$0`, `$1`, etc.
3. **Template reading**: Tera template loaded from pond
4. **Context building**: `export` object with hierarchical match data
5. **Rendering**: Tera renders template with context
6. **Output**: Files appear in dynamic directory, exported via `pond export`

The new system should preserve the pattern-matching and capture semantics while improving the authoring experience.
