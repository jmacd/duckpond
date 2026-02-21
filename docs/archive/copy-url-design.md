# Design: Unified URL-Driven `pond copy`

## Problem Statement

`pond copy` has a parallel, inconsistent interface compared to the rest of DuckPond:

- `host://` is a bespoke prefix (not parsed by `provider::Url`, just `strip_prefix`)
- `--format=data|table|series` controls the **entry type** via a separate flag
- There's no way to **transform** data during copy (e.g., ingest CSV as a table, decompress zstd)
- `pond cat` already has a richer URL model (`csv+gzip:///path`) but copy doesn't use it

## Design Goal

Replace `--format` with scheme modifiers on the source URL, using the `+suffix` syntax
already implemented for compression. The source URL fully describes: where the data is,
how to decode it, and what entry type to create.

## URL Grammar

```
[host+][format[+compression][+entrytype]]:///path
```

**Segments** (all optional, order-independent per category):

| Position | Values | Meaning |
|----------|--------|--------|
| host | `host+` prefix | Read from host filesystem (vs pond-internal path) |
| format | `csv`, `oteljson`, `excelhtml`, ... | How to parse bytes into Arrow batches |
| compression | `gzip`, `zstd`, `bzip2` | How to decompress the byte stream |
| entrytype | `table`, `series` | What pond entry type to create (default: `data`) |

The `host` token is always a **prefix** on the scheme (`host+csv`, `host+table`).
This is consistent with the existing `pond cat host+csv:///path` syntax.

## Examples

| Command | Host? | Decompress? | Parse? | Store as |
|---------|-------|-------------|--------|----------|
| `copy host:///tmp/f.csv /data/` | yes | — | — | `data` (raw bytes) |
| `copy host+table:///tmp/f.parquet /data/` | yes | — | — | `table` (validated PAR1) |
| `copy host+series:///tmp/f.parquet /data/` | yes | — | — | `series` (PAR1 + temporal) |
| `copy host+oteljson:///tmp/f.json /data/` | yes | — | oteljson→Arrow | `table` (implied) |
| `copy host+oteljson+zstd:///tmp/f.json.zst /data/` | yes | zstd | oteljson→Arrow | `table` (implied) |
| `copy host+csv+gzip:///tmp/f.csv.gz /data/` | yes | gzip | csv→Arrow | `table` (implied) |
| `copy host+csv+gzip+series:///tmp/f.csv.gz /data/` | yes | gzip | csv→Arrow | `series` |

## Key Design Decisions

### 1. Where `host` goes: prefix position

**Decision**: `host` is always a **prefix** on the scheme: `host+csv:///path`,
`host+table:///path`, `host+oteljson+gzip:///path`.

This is consistent with the existing `pond cat host+csv:///path` syntax introduced
before this design. An earlier version of this document proposed putting `host` in
the URL authority position (`csv://host/path`), but that was rejected in favor of
prefix consistency — one syntax for "host file" everywhere.

`provider::Url` already has an `is_host` flag that strips the `host+` prefix before
parsing. The remaining scheme segments (`csv+gzip`, `table`, `series`) are parsed
normally.

| URL | is_host | format | compression | entry_type | path |
|-----|---------|--------|-------------|------------|------|
| `host:///tmp/f.csv` | yes | `host` | — | — | `/tmp/f.csv` |
| `host+table:///tmp/f.parquet` | yes | `file` | — | `table` | `/tmp/f.parquet` |
| `host+series:///tmp/f.parquet` | yes | `file` | — | `series` | `/tmp/f.parquet` |
| `host+csv:///tmp/data.csv` | yes | `csv` | — | `table` (implied) | `/tmp/data.csv` |
| `host+oteljson+zstd:///tmp/f.json` | yes | `oteljson` | `zstd` | `table` (implied) | `/tmp/f.json` |
| `host+csv+gzip+series:///tmp/f.csv.gz` | yes | `csv` | `gzip` | `series` | `/tmp/f.csv.gz` |

### 2. Implicit entry type when a format provider is involved

When a format provider (csv, oteljson, etc.) is used, it produces Arrow RecordBatches.
These must be stored as `table` or `series` — storing parsed Arrow data as `data` makes
no sense (you'd lose the parsed structure). So:

- **Format provider present → default entry type is `table`**
- **`+series` suffix → override to `series`** (requires a timestamp column)
- **No format provider → default entry type is `data`** (raw bytes, current behavior)

### 3. Copy OUT (pond → host) — no changes

Export keeps current behavior: export format is determined by source entry type in the pond
(table/series → Parquet via DataFusion, data → raw bytes). The destination `host:///path`
remains a simple prefix. No changes needed here for now.

### 4. `provider::Url` integration

`pond copy` currently does its own `strip_prefix("host://")` parsing. The new design would:

- Parse source URLs through `provider::Url::parse()`
- Extend `provider::Url` with:
  - `is_host()` — returns true when `host+` prefix is present (already exists)
  - `entry_type()` — returns parsed `+table`/`+series` suffix
  - `host_path()` — returns the URL path for host filesystem resolution

### 5. Suffix classification in `provider::Url`

Currently `provider::Url` splits the scheme on `+` and recognizes compression suffixes.
With this design, we parse multiple `+` segments into categories:

```
host+oteljson+zstd+series → is_host=true, format=oteljson, compression=zstd, entry_type=series
host+table                → is_host=true, format=file,     compression=none, entry_type=table
host+csv+gzip             → is_host=true, format=csv,      compression=gzip, entry_type=table (implied)
csv+gzip                  → is_host=false, format=csv,     compression=gzip, entry_type=table (implied)
```

The `host+` prefix is stripped first (already implemented). Then the remaining scheme
segments are split on `+` and classified. The `Url` struct gains an `entry_type` field:

```rust
pub struct Url {
    inner: url::Url,
    format_scheme: String,              // "csv", "oteljson", "file", etc.
    compression: Option<String>,        // "gzip", "zstd", "bzip2"
    entry_type: Option<String>,         // "table", "series"
    is_host: bool,                      // true when host+ prefix was present
}
```

Each `+` segment is classified into its category. Known compressions → `compression`,
known entry types → `entry_type`, unknown → error. Order in the scheme string doesn't
matter, but the convention is `format+compression+entrytype`.

## Implementation Phases

### Phase 1: Extend `provider::Url` with entry type modifiers

- Add `entry_type: Option<String>` field to `Url`
- Parse `+table` / `+series` from scheme suffixes (alongside existing `+compression`)
- Add `entry_type()`, `is_host()`, `host_path()` accessors
- Unit tests for all combinations

### Phase 2: ~~Recognize `host` authority~~ (Cancelled)

~~Authority-based `host` was rejected in favor of prefix-only `host+`.~~
`is_host()` already works via the `host+` prefix. No authority parsing needed.

### Phase 3: Refactor `pond copy` to use `provider::Url`

- Replace `parse_host_path()` with `provider::Url::parse()`
- Replace `--format` flag logic with URL-derived entry type
- Keep `--format` flag working as a deprecated fallback during transition
- Remove `get_entry_type_for_file()` — entry type comes from the URL

### Phase 4: Wire format providers into copy-in

When source URL has a format provider (csv, oteljson, etc.):

1. Open host file as `AsyncRead`
2. Decompress if compression suffix present
3. Feed through format provider's `open_stream()` → RecordBatch stream
4. Write batches through `ArrowWriter` → Parquet bytes → pond (as `table` or `series`)
5. Extract temporal bounds from parquet metadata for series

This reuses the exact same `FormatProvider` pipeline that `pond cat` uses.

### Phase 5: Deprecate `--format` flag

- Emit deprecation warning when `--format` is used alongside a URL that already encodes the type
- Eventually remove the flag entirely

## Current State of `provider::Url` Parsing

Today's implementation is **flat, not positional**. The `Url::parse()` method does a
single `rfind('+')` on the scheme and checks if the trailing suffix is a known compression.
There is no multi-segment parsing, no layering, and no ordering:

- `csv+gzip:///path` → scheme=`csv`, compression=`gzip` (one split, one check)
- Unknown suffixes are silently kept as part of the scheme name

The runtime pipeline is hardcoded as exactly three stages:

1. **tinyfs** → read bytes from pond
2. **decompress** → optional, based on `compression` field
3. **format provider** → selected by `scheme` name, produces Arrow batches

There is no concept of source location (`host`) in the URL, no entry type suffix, and
no chaining of multiple providers. The `host` position (URL authority) is currently
rejected with an error.

This means the multi-segment parsing (`[host+]format+compression+entrytype`)
is **new work** — it extends the current single-split parsing into a multi-segment parser
with classified positions. The `host+` prefix is already handled (stripped before parsing).
The remaining work is classifying `+table`/`+series` as entry type suffixes alongside
the existing `+gzip`/`+zstd` compression suffixes. The existing `pond cat` behavior
will continue to work unchanged.

## Risks & Open Questions

### Ambiguity in `+` suffixes

Is `table` a compression or entry type? Solved by classification: known compressions
(`gzip`, `zstd`, `bzip2`) and known entry types (`table`, `series`) are disjoint sets.

### Series timestamp column

When a format provider produces a table, how does the user specify which column is the
timestamp for `+series`? Options:

- Query parameter: `host+csv+series:///f.csv?timestamp_column=ts`
- Auto-detect: first timestamp-typed column
- Design decision needed before Phase 4

### Copy OUT with format providers

Could we eventually support exporting in a specific format?
e.g., `pond copy /data/readings.parquet csv://host/output.csv`

Out of scope for this design but the URL model supports it later.

### Recursive directory copy with format

`copy host+oteljson:///logs/ /data/` — should the format apply to every file in the
directory? Probably yes, but needs error handling for files that don't parse.
