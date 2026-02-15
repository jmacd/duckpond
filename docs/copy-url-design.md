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
[format[+compression][+entrytype]:]//[host]/path
```

**Segments** (all optional, order-independent per category):

| Position | Values | Meaning |
|----------|--------|---------|
| format | `csv`, `oteljson`, `excelhtml`, ... | How to parse bytes into Arrow batches |
| compression | `gzip`, `zstd`, `bzip2` | How to decompress the byte stream |
| entrytype | `table`, `series` | What pond entry type to create (default: `data`) |
| host literal | `host` | Read from host filesystem (vs pond-internal path) |

The `host` token occupies the **authority** position in the URL (the part between `://` and the path).

## Examples

| Command | Host? | Decompress? | Parse? | Store as |
|---------|-------|-------------|--------|----------|
| `copy host:///tmp/f.csv /data/` | yes | — | — | `data` (raw bytes) |
| `copy host+table:///tmp/f.parquet /data/` | yes | — | — | `table` (validated PAR1) |
| `copy host+series:///tmp/f.parquet /data/` | yes | — | — | `series` (PAR1 + temporal) |
| `copy oteljson://host/f.json /data/` | yes | — | oteljson→Arrow | `table` (implied) |
| `copy oteljson+zstd://host/f.json.zst /data/` | yes | zstd | oteljson→Arrow | `table` (implied) |
| `copy csv+gzip://host/f.csv.gz /data/` | yes | gzip | csv→Arrow | `table` (implied) |
| `copy csv+gzip+series://host/f.csv.gz /data/` | yes | gzip | csv→Arrow | `series` |

## Key Design Decisions

### 1. Where `host` goes: authority position

Two options were considered:

- **Authority** (`csv://host/path`): Natural URL reading — "csv file on host at path".
  The authority says *where*, which is semantically correct. We already reject non-empty
  host in `provider::Url`, so there's no conflict.
- **Scheme suffix** (`csv+host:///path`): Consistent with `+compression`, but `host` isn't
  a compression or format — it's a location. Reads oddly.

**Decision**: Use the authority position for `host` when a format scheme is present.

This reconciles with the existing `host:///path` syntax:

| URL | scheme | authority | path | Meaning |
|-----|--------|-----------|------|---------|
| `host:///tmp/f.csv` | `host` | (empty) | `/tmp/f.csv` | Raw copy from host (current) |
| `host+table:///tmp/f.parquet` | `host+table` | (empty) | `/tmp/f.parquet` | Copy from host as validated table |
| `host+series:///tmp/f.parquet` | `host+series` | (empty) | `/tmp/f.parquet` | Copy from host as time-series |
| `csv://host/data.csv` | `csv` | `host` | `/data.csv` | Parse CSV from host, store as table |
| `oteljson+zstd://host/metrics.json` | `oteljson+zstd` | `host` | `/metrics.json` | Decompress+parse from host |

**Rule**: `host` as a **scheme** means "raw copy from host filesystem". Format providers
(`csv`, `oteljson`) use `host` in the **authority** to indicate host filesystem source.

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
  - `is_host()` — returns true when authority == `"host"` or scheme == `"host"`
  - `host_path()` — returns the resolved host filesystem path
  - `entry_type_override()` — parses `+table`/`+series` suffixes

### 5. Suffix classification in `provider::Url`

Currently `provider::Url` splits the scheme on `+` and recognizes compression suffixes.
With this design, we parse multiple `+` segments into categories:

```
oteljson+zstd+series → format=oteljson, compression=zstd, entry_type=series
host+table           → format=host,     compression=none, entry_type=table
csv+gzip             → format=csv,      compression=gzip, entry_type=table (implied)
```

The `Url` struct would gain an `entry_type` field:

```rust
pub struct Url {
    format_scheme: String,              // "csv", "oteljson", "host", "file", etc.
    compression: Option<String>,        // "gzip", "zstd", "bzip2"
    entry_type: Option<String>,         // "table", "series"
    host: String,                       // "host" or empty
    path: String,
    query: HashMap<String, String>,
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

### Phase 2: Recognize `host` authority in `provider::Url`

- Allow `host` as a non-empty authority (currently rejected)
- `is_host()` returns true for both scheme=`host` and authority=`host`
- `host_path()` returns the resolved filesystem path

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

This means the positional grammar described in this design (`source+format+compression+entrytype`)
is **new work** — it extends the current single-split parsing into a multi-segment parser
with ordered positions. The existing `pond cat` behavior will continue to work unchanged
since it only uses format+compression, which are the two middle positions.

## Risks & Open Questions

### Ambiguity in `+` suffixes

Is `table` a compression or entry type? Solved by classification: known compressions
(`gzip`, `zstd`, `bzip2`) and known entry types (`table`, `series`) are disjoint sets.

### Series timestamp column

When a format provider produces a table, how does the user specify which column is the
timestamp for `+series`? Options:

- Query parameter: `csv+series://host/f.csv?timestamp_column=ts`
- Auto-detect: first timestamp-typed column
- Design decision needed before Phase 4

### Copy OUT with format providers

Could we eventually support exporting in a specific format?
e.g., `pond copy /data/readings.parquet csv://host/output.csv`

Out of scope for this design but the URL model supports it later.

### Recursive directory copy with format

`copy oteljson://host/logs/ /data/` — should the format apply to every file in the
directory? Probably yes, but needs error handling for files that don't parse.
