# Log Viewer

DuckPond's sitegen system can generate static log viewer pages that display
journald logs (or any JSONL data) in-browser using DuckDB-WASM.  The logs
stay in their original JSONL format inside the pond; format providers convert
them to queryable Parquet at site-build time.

## Architecture

```
journalctl --output=json
    |
    v
journal-ingest factory
    -> /logs/watershop/*.jsonl  (FilePhysicalSeries, original JSONL)
    -> temporal metadata: min/max __REALTIME_TIMESTAMP per file
    |
    v
sitegen export stage (pattern: jsonlogs:///logs/watershop/*.jsonl)
    -> UrlPatternMatcher expands glob
    -> jsonlogs format provider reads JSONL -> Arrow RecordBatches
    -> COPY TO Parquet in dist/data/
    |
    v
log-viewer.js (client-side)
    -> DuckDB-WASM loads Parquet files
    -> SQL queries with OFFSET/LIMIT pagination
    -> Unit filter pills, priority coloring, timestamp formatting
```

## Changes Made

### journal-ingest (`crates/provider/src/factory/journal_ingest.rs`)

- Added `timestamp_field` config option (default: `__REALTIME_TIMESTAMP`)
- `compute_file_bounds()` scans grouped entries for min/max timestamps
- `write_entries()` calls `set_temporal_metadata(min, max, field_name)` on
  each file writer before shutdown
- Temporal bounds and extended attributes (`duckpond.timestamp_column`) are
  now persisted for FilePhysicalSeries

### tlogfs persistence (`crates/tlogfs/src/`)

- `persistence.rs`: `store_file_content_ref` handles `FilePhysicalSeries`
  with Series metadata in both small-file and large-file paths
- `file.rs`: Added `FilePhysicalSeries` match arm in metadata extraction
  to use precomputed temporal metadata when available
- `schema.rs`: Relaxed `new_large_file_series` assertion to accept
  `FilePhysicalSeries` (was hardcoded to `TablePhysicalSeries`)

### Export pipeline (`crates/provider/src/export.rs`)

- Refactored into two functions:
  - `export_series_to_parquet` — backwards-compatible wrapper that resolves
    a pond path to a table provider
  - `export_table_provider_to_parquet` — core function that accepts any
    `TableProvider` and a configurable `timestamp_column` name

### Sitegen (`crates/sitegen/src/`)

- **config.rs**: `ExportStage` gains `timestamp_column` field (default:
  `"timestamp"`, serde default for backward compatibility)
- **factory.rs**: `run_export_stages` detects URL-scheme patterns
  (`contains("://")`) and routes them through `run_format_provider_export`,
  which uses `UrlPatternMatcher` + `Provider::create_table_provider()` +
  `export_table_provider_to_parquet()`
- **shortcodes.rs**: New `{{ log_viewer /}}` shortcode emitting a
  `<div id="log-viewer">` with inline JSON manifest (same pattern as chart)
- **layouts.rs**: New `logs` layout that loads `log-viewer.js`

### Client-side (`crates/sitegen/assets/`)

- **log-viewer.js**: DuckDB-WASM log viewer
  - Loads parquet files from the page's JSON manifest
  - Creates a unified SQL view across all files
  - Auto-detects unit, message, timestamp, and priority columns
  - Unit filter pills for selecting systemd units
  - Newer/Older/Latest pagination (200 rows per page)
  - Priority-colored rows (emergency through debug)
  - Timestamps formatted from microseconds to ISO datetime
- **style.css**: Log viewer styles using the existing theme CSS variables

### Linux site (`linux/`)

- **site.yaml**: Sitegen config with `jsonlogs:///logs/watershop/*.jsonl`
  export pattern and `timestamp_column: "__REALTIME_TIMESTAMP"`
- **site/**: Page templates (index, logs-index, logs detail, sidebar)
- **setup.sh**: Updated to install sitegen factory and copy site templates

## Usage

```bash
# Initial setup (destroys existing pond)
./linux/setup.sh

# Subsequent journal collection
./linux/run.sh

# Generate site
pond run /system/etc/site build /path/to/output

# Serve locally
cd /path/to/output && python3 -m http.server 8080
```

## Site Configuration

```yaml
factory: sitegen

site:
  title: "Linux Logs"
  base_url: "/"

exports:
  - name: "logs"
    pattern: "jsonlogs:///logs/watershop/*.jsonl"
    timestamp_column: "__REALTIME_TIMESTAMP"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "logs"
        type: static
        slug: "logs"
        page: "/site/logs-index.md"
        routes:
          - name: "log-detail"
            type: template
            slug: "$0"
            page: "/site/logs.md"
            export: "logs"
```

The `timestamp_column` field tells the export pipeline which column
contains timestamps for temporal partitioning.  For journald data this
is `__REALTIME_TIMESTAMP` (microseconds since epoch).

## Design Decisions

- **Original data stays original**: JSONL logs are stored as
  `FilePhysicalSeries` and never converted at ingest time.  The
  `jsonlogs` format provider bridges to DataFusion queryability
  on-the-fly during site build.

- **No temporal partitioning for logs**: Format provider exports
  skip Hive-style partitioning because the jsonlogs provider
  produces all-Utf8 columns; timestamp casting would be needed
  for `date_part()`.  Each source file exports as a single
  parquet file.

- **Timestamp at ingest**: journal-ingest computes per-file
  min/max timestamps during entry grouping (cheap — already
  parsing JSON) and stores them as file metadata.
