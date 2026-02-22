# Template Variables & Template Factory — Archived Design

**Removed:** February 2026
**Reason:** Superseded by the sitegen factory, which handles context propagation
explicitly rather than through implicit template variable injection.

---

## Overview

DuckPond had two related subsystems removed during the Phase 2A cleanup:

1. **Template Variables** — a key-value mechanism for passing CLI context through
   the transaction lifecycle into factory rendering at read time.
2. **Template Factory** — a Tera-based dynamic file factory that rendered `.html.tmpl`
   templates stored in the pond, producing HTML pages with embedded data references.

These were designed together to support a multi-stage export pipeline: export
Parquet data to host → inject export metadata into template variables → render
HTML templates that reference the exported data files.

The sitegen factory replaced this pipeline with an explicit, self-contained
Markdown + Maud approach that doesn't require implicit context propagation.

---

## Template Variables

### Data Flow

```
CLI (--var k=v)
  → ShipContext.template_variables: HashMap<String, String>
    → PondUserMetadata.vars: HashMap<String, String>
      → State.set_template_variables(): Arc<Mutex<HashMap<String, Value>>>
        → State.as_provider_context() [snapshot clone]
          → ProviderContext.template_variables: Arc<Mutex<HashMap<String, Value>>>
            → TemplateFactory reads at render time
```

### CLI Interface

```bash
pond cat --var site_name="My Site" --var year=2024 /templates/index.html
pond export --var site_name="My Site" --pattern "/data/**" --dir ./output
```

The `-v` / `--var` global argument accepted `key=value` pairs. These were stored
on `ShipContext` and threaded into every transaction's `PondUserMetadata`.

### Steward Layer

In `Ship::begin_txn()`, after starting the underlying tlogfs transaction, the
vars from `PondUserMetadata` were converted to JSON and set on `State`:

```rust
// Convert HashMap<String, String> → HashMap<String, Value>
let structured_variables = HashMap::from([(
    "vars".to_string(),
    serde_json::Value::Object(
        meta.vars.iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect()
    ),
)]);
state.set_template_variables(structured_variables)?;
```

The vars were nested under a `"vars"` key, so templates accessed them as
`{{ vars.site_name }}`.

### ProviderContext

`State::as_provider_context()` snapshot-cloned the template variables into a
new `ProviderContext`. This was a fresh clone each time — the `ProviderContext`
had its own `Arc<Mutex<HashMap>>` independent of State's.

### Control Table

The control table serialized `PondUserMetadata.vars` into the `environment`
field of each transaction record, preserving a record of what variables were
passed for audit purposes.

---

## Template Factory

### Registration

```rust
register_dynamic_factory!(name: "template", factory: TemplateFactory);
```

Registered as a **dynamic factory** — created with `pond mknod dynamic-dir`,
content computed on every read (no stored data).

### Config Format (YAML)

```yaml
entries:
  - in_pattern: "/data/{0}/**"
    out_pattern: "{0}.html"
    template_file: "/templates/data.html.tmpl"
  - in_pattern: "/summary/**"
    out_pattern: "index.html"
    template_file: "/templates/index.html.tmpl"
```

Each entry defined:
- `in_pattern` — glob pattern matching source data files in the pond
- `out_pattern` — output filename (with capture group substitution)
- `template_file` — path to a Tera template file stored in the pond

### How It Worked

1. `TemplateFactory` implemented both `DynamicDirectoryFactory` (for listing) and
   `DynamicFile` (for reading).
2. On directory listing: matched `in_pattern` against pond files, generated
   virtual filenames via `out_pattern` substitution.
3. On file read: loaded the Tera template from `template_file`, rendered it with:
   - `vars.*` — CLI variables from `ProviderContext.template_variables`
   - `export.*` — export pipeline results (if running inside an export stage)
   - `args[0]`, `args[1]`, ... — capture groups from `in_pattern`
4. Custom Tera functions:
   - `insert_from_path(path="/some/pond/path")` — inline another pond file's content
   - `group(items=..., by="field")` — group arrays by a field value
5. Custom Tera filters:
   - `to_json` — serialize value to JSON string
   - `keys` — extract keys from an object

### Template File Example

```html
<!-- /templates/data.html.tmpl -->
<html>
<head><title>{{ vars.site_name }} - {{ args[0] }}</title></head>
<body>
  <h1>Data for {{ args[0] }}</h1>
  <div id="data">
    {{ insert_from_path(path="/data/" ~ args[0] ~ "/summary.json") }}
  </div>
</body>
</html>
```

### Implementation Location

- `crates/provider/src/factory/template.rs` (791 lines)
- Key structs: `TemplateFactory`, `TemplateDirectory`, `TemplateFile`,
  `TemplateCollection`, `TemplateSpec`

---

## Export Pipeline (Multi-Stage)

### How Stages Chained

The `pond export` command accepted multiple `--pattern` arguments, each becoming
a sequential "stage":

```bash
pond export \
  --pattern "/data/**/*.series" \
  --pattern "/templates/**" \
  --dir ./output \
  --temporal month
```

- **Stage 1**: Exported Parquet series files to host, producing an `ExportSet`
  (metadata about what was exported: file paths, temporal bounds, schemas).
- **Stage 2+**: Before processing, called `State::add_export_data(export_json)`
  which injected the previous stage's `ExportSet` as JSON under the `"export"`
  key in `template_variables`. Template factory files in this stage could then
  access `{{ export.files }}`, `{{ export.schema }}`, etc.

### add_export_data

```rust
// In State (persistence.rs):
pub fn add_export_data(&self, export_data: serde_json::Value) -> Result<(), TLogFSError> {
    let mut vars = self.template_variables.lock()...;
    vars.insert("export".to_string(), export_data);
    Ok(())
}
```

This was the bridge between the export pipeline and the template system —
previous stage results became template context for the next stage.

### ExportSet / TemplateSchema Types

```rust
pub struct TemplateSchema {
    pub fields: Vec<TemplateField>,
    pub instrument: String,
    // ...
}

pub struct TemplateField {
    pub name: String,
    pub instrument: String,
    pub unit: String,
    pub agg: String,
}

pub enum ExportSet {
    Empty,
    Leaf(ExportLeaf),
    Branch(HashMap<String, ExportSet>),
}
```

These types existed in `crates/provider/src/export.rs` to structure export
metadata for template consumption.

---

## Why Removed

1. **Implicit context propagation** — template variables threaded invisible state
   through layers (CLI → metadata → State → ProviderContext → factory), making
   the system hard to reason about.
2. **Dead code** — the export pipeline with template stages had zero testsuite
   coverage and was not actively used.
3. **Replaced by sitegen** — the sitegen factory handles HTML generation through
   an explicit Markdown + Maud pipeline with self-contained context, eliminating
   the need for implicit variable passing.
4. **Coupling** — template variables on `State` and `ProviderContext` created
   coupling between tlogfs internals and the presentation layer, blocking the
   Steward trait extraction (Phase 2A/2B).

---

## What Was Kept

- **`template_utils.rs`** (`crates/cmd/src/template_utils.rs`) — Tera-based YAML
  config expansion for `mknod`. Still supports `{{ env(name="FOO") }}` in factory
  YAML configs. The `{{ vars.* }}` expansion no longer has a source but the
  function remains for env() support.
- **`pond export`** — the command itself remains for exporting pond data to the
  host filesystem. The multi-stage pipeline and template variable injection were
  stripped; export now processes patterns independently without stage chaining.
