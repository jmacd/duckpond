# DuckPond CLI Reference

> **Living documentation** - Built from experimentation, updated when confusion is discovered.

## Quick Reference

| Command | Purpose | Example |
|---------|---------|---------|
| `pond init` | Initialize a new pond | `pond init` |
| `pond mkdir` | Create directories | `pond mkdir /data` |
| `pond copy` | Copy files into pond | `pond copy data.csv /data/` |
| `pond list` | List files (glob patterns) | `pond list '**/*'` |
| `pond cat` | Read file contents | `pond cat /data/file.csv` |
| `pond describe` | Show file schema | `pond describe /data/*.csv` |
| `pond mknod` | Create factory nodes | `pond mknod --config f.yaml /path` |
| `pond run` | Execute factory nodes | `pond run /etc/system.d/20-foo collect` |
| `pond control` | Query transaction history | `pond control recent` |

## Environment

| Variable | Purpose | Default |
|----------|---------|---------|
| `POND` | Path to pond storage | Required |
| `RUST_LOG` | Logging level | `info` |

## Commands in Detail

### pond init

Initialize a new pond at the path specified by `$POND`.

```bash
export POND=/data/mypond
pond init
```

Creates the pond directory structure with Delta Lake metadata.

---

### pond list

List files and directories matching a glob pattern.

```bash
# List everything (default behavior)
pond list              # defaults to '**/*' - all files recursively

# List root directory entries
pond list /            # top-level files and directories only

# List specific directory contents  
pond list /data/       # trailing slash lists contents of /data
pond list '/data/*'    # equivalent to above
pond list '/data/**/*' # recursive under /data

# Match specific entry (file or directory)
pond list /data        # shows the /data entry itself (not contents)

# Pattern matching
pond list '**/*.csv'   # all CSV files
pond list '/sensors/*/readings.series'
```

**Pattern Behavior Summary**:

| Pattern | Meaning |
|---------|---------|
| (none) | All files recursively (`**/*`) |
| `/` | Root directory entries only |
| `/dir/` | Contents of /dir (trailing slash = `/dir/*`) |
| `/dir` | Entry named 'dir' exactly |
| `**/*.ext` | All files with extension recursively |

💡 **Tip**: Use trailing slash to list directory contents: `pond list /data/`

---

### pond mkdir

Create directories in the pond.

```bash
pond mkdir /data
pond mkdir /sensors/temperature
```

Parent directories are created automatically (like `mkdir -p`).

---

### pond copy

Copy external files into the pond.

```bash
# Copy single file (use host:// prefix for external files)
pond copy host:///tmp/data.csv /data/readings.csv

# Copy to directory (keeps filename)
pond copy host:///tmp/data.csv /data/
```

⚠️ **Deprecation**: Plain paths like `/tmp/file.csv` still work but are deprecated.
Use `host:///path/to/file` for external filesystem paths.

#### Format Options

| Format | Purpose | Use Case |
|--------|---------|----------|
| `--format=data` | Store as raw bytes (default) | CSV, JSON, any file |
| `--format=table` | Store as queryable Parquet | Parquet files ONLY |
| `--format=series` | Store as time-series Parquet | Parquet with timestamp |

⚠️ **Important**: `--format=table` validates that input is Parquet (PAR1 magic bytes).
To copy CSV files, use the default `--format=data`, then query with `csv://` prefix.

```bash
# ✅ Correct: Copy CSV as raw data
pond copy host:///tmp/data.csv /data/readings.csv

# ✅ Correct: Copy Parquet as table
pond copy host:///tmp/data.parquet /data/readings.parquet --format=table

# ❌ Wrong: This will error - CSV is not Parquet
pond copy host:///tmp/data.csv /data/readings.csv --format=table
```

#### Copy OUT (pond → host)

Export files from the pond to the host filesystem. The destination must have the `host://` prefix.
Glob patterns are supported for matching multiple files.

```bash
# Export all series files preserving directory structure
pond copy '/hydrovu/devices/**/*.series' host:///tmp/export

# Export a single file
pond copy /data/readings.parquet host:///tmp/output
```

The `--format` flag is **ignored** when copying out — the export format is determined by the
source entry type (table/series → Parquet via DataFusion, data → raw bytes).

#### --strip-prefix

When copying out, pond paths are preserved relative to the destination. This can produce
unwanted nesting (e.g. exporting `/hydrovu/...` into a directory called `hydrovu/` creates
`hydrovu/hydrovu/...`). Use `--strip-prefix` to remove a leading path prefix:

```bash
# Without --strip-prefix: creates output/hydrovu/devices/123/foo.series
pond copy '/hydrovu/**/*.series' host:///tmp/output

# With --strip-prefix: creates output/devices/123/foo.series
pond copy '/hydrovu/**/*.series' host:///tmp/output --strip-prefix=/hydrovu
```

---

### pond cat

Read and optionally transform file contents.

```bash
# Read raw file contents
pond cat /data/readings.csv

# Query CSV files with SQL (use csv:// prefix)
pond cat csv:///data/readings.csv --sql "SELECT * FROM source WHERE temp > 20"

# Query OtelJSON Lines files (use oteljson:// prefix)
pond cat oteljson:///logs/metrics.json --sql "SELECT * FROM source ORDER BY timestamp"

# Query Parquet files (stored with --format=table)
pond cat /data/readings.parquet --sql "SELECT AVG(temperature) as avg_temp FROM source"
```

The table is always named `source` in SQL queries.

#### Output Format (`--format`)

| Flag | Output | When to use |
|------|--------|-------------|
| `--format=raw` (default) | Parquet bytes (binary) | Piping to files, downstream tools |
| `--format=table` | Human-readable text table | Terminal display, debugging |

The `--format` flag controls **output** format only. It is independent of `--sql`.
`--sql` controls **what** data is queried; `--format` controls **how** it's displayed.

```bash
# Parquet bytes to file (default)
pond cat oteljson:///ingest/data.json --sql "SELECT * FROM source" > output.parquet

# Human-readable table to terminal
pond cat oteljson:///ingest/data.json --format=table --sql "SELECT * FROM source LIMIT 5"

# Human-readable table, no SQL (full SELECT * ORDER BY timestamp)
pond cat oteljson:///ingest/data.json --format=table
```

#### URL Schemes for Querying

| Scheme | Purpose | Example |
|--------|---------|--------|
| `csv://` | Parse file as CSV | `pond cat csv:///data/file.csv --sql "..."` |
| `oteljson://` | Parse file as OtelJSON Lines | `pond cat oteljson:///logs/metrics.json --sql "..."` |
| `excelhtml://` | Parse file as Excel HTML | `pond cat excelhtml:///data/export.html --sql "..."` |
| `file://` | Raw bytes or Parquet | `pond cat file:///data/file.parquet` |
| (none) | Auto-detect | `pond cat /data/file.csv` |

⚠️ **Important**: The `--sql` flag (or `--query` alias) only works when the file can be parsed as a table:
- Parquet files (stored with `--format=table` or `--format=series`)
- CSV files when using `csv://` prefix
- OtelJSON Lines files when using `oteljson://` prefix (two-pass: discovers all metric names as columns)
- Raw data files without a scheme will output raw bytes, ignoring `--sql`

#### Schema Discovery with `information_schema`

DataFusion's `information_schema` is enabled, so you can discover column names dynamically:

```bash
# List all columns in the source table
pond cat oteljson:///ingest/data.json --format=table --sql "
  SELECT column_name, data_type
  FROM information_schema.columns
  WHERE table_name = 'source'
  ORDER BY column_name
"
```

This is useful when exploring unfamiliar data — you don't need to know the column
names in advance. `pond describe` also shows the schema, but `information_schema`
lets you query metadata with SQL alongside your data queries.

---

### pond describe

Show schema information for files.

```bash
pond describe /data/readings.csv
pond describe '/sensors/**/*.parquet'
```

---

### pond mknod

Create a factory node from a YAML configuration.

```bash
# From config file
pond mknod --config /path/to/config.yaml /destination/path

# Specific factory types
pond mknod sql-derived-table /derived/view --config-path filter.yaml
pond mknod remote /etc/system.d/10-remote --config-path remote.yaml
```

See [Factory Types](#factory-types) for configuration options.

---

### pond run

Execute a factory node's commands.

```bash
# Run data collection
pond run /etc/system.d/20-hydrovu collect

# Push to remote backup
pond run /etc/system.d/10-remote push

# Pull from remote
pond run /etc/system.d/10-remote pull
```

---

### pond control

Query the pond's transaction history and configuration.

```bash
# Recent transactions
pond control recent
pond control recent --limit 20

# Transaction details
pond control detail --txn-seq 42

# SQL query on control table
pond control --sql "SELECT txn_seq, cli_args FROM control_table ORDER BY txn_seq DESC LIMIT 5"

# Show configuration
pond control show-config
```

---

## Factory Types

### sql-derived-table

Apply SQL transformation to a single file.

```yaml
factory: "sql-derived-table"
config:
  patterns:
    source: "table:///raw/data.csv"
  query: "SELECT timestamp, value * 2 as doubled FROM source"
```

### sql-derived-series

Apply SQL to multiple files/versions (time series).

```yaml
factory: "sql-derived-series"
config:
  patterns:
    source: "series:///sensors/*"
  query: "SELECT * FROM source WHERE temperature > 20"
```

### synthetic-timeseries

Generate synthetic timeseries with configurable waveforms. Produces Arrow
RecordBatches in memory (no files on disk) — useful for testing, demos, and
development with deterministic, visually distinct data.

Each named **point** becomes a Float64 column. Its value at every timestamp is the
**sum** of one or more waveform **components**, so you can layer signals to create
complex but predictable shapes.

```yaml
start: "2024-01-01T00:00:00Z"
end: "2024-01-02T00:00:00Z"
interval: "15m"
time_column: "timestamp"          # optional, default: "timestamp"
points:
  - name: "temperature"
    components:
      - type: sine
        amplitude: 10.0
        period: "24h"
        offset: 20.0              # baseline value
      - type: line
        slope: 0.0002             # slow upward drift

  - name: "pressure"
    components:
      - type: sine
        amplitude: 5.0
        period: "12h"
        offset: 1013.0
      - type: square
        amplitude: 2.0
        period: "6h"

  - name: "humidity"
    components:
      - type: triangle
        amplitude: 15.0
        period: "8h"
        offset: 60.0
      - type: sine
        amplitude: 3.0
        period: "3h"
        phase: 1.57               # phase offset in radians
```

**Component types:**

| Type | Formula | Parameters |
|------|---------|------------|
| `sine` | `offset + amplitude × sin(2π·t/period + phase)` | `amplitude`, `period`, `offset`, `phase` |
| `triangle` | `offset + amplitude × tri(t/period + phase)` | `amplitude`, `period`, `offset`, `phase` |
| `square` | `offset + amplitude × sign(sin(2π·t/period + phase))` | `amplitude`, `period`, `offset`, `phase` |
| `line` | `offset + slope × t` | `slope`, `offset` |

- `t` is seconds elapsed since `start`.
- `period` and `interval` accept human-readable durations: `30s`, `15m`, `1h`, `2d`, `1h30m`, etc.
- All parameters default to `0.0` if omitted.

**Usage:**

```bash
# Create the factory node
pond mkdir /sensors
pond mknod synthetic-timeseries /sensors/synth --config-path synth.yaml

# Query with SQL (table is always named "source")
pond cat /sensors/synth --sql "SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM source"
pond cat /sensors/synth --sql "SELECT * FROM source ORDER BY timestamp LIMIT 10"

# Check value ranges
pond cat /sensors/synth --sql "
  SELECT
    MIN(temperature) AS temp_min, MAX(temperature) AS temp_max,
    MIN(pressure)    AS pres_min, MAX(pressure)    AS pres_max
  FROM source
"
```

**Notes:**
- Data is generated on every query from the config — there is no stored state.
- The node appears as `TableDynamic` in `pond list` output.
- The timestamp column is `Timestamp(Millisecond, UTC)`.
- Point names must be unique; at least one point with at least one component is required.
- Works with `pond describe`, `pond cat --sql`, and any downstream factory
  that reads `series:///` or `file:///` patterns.

---

### dynamic-dir

A virtual directory whose child entries are produced by other factories.
Each entry specifies a `name`, a `factory` type, and a `config` block.
`dynamic-dir` is the glue that lets you compose multiple factory outputs
under a single path — for example, several `timeseries-join` or
`synthetic-timeseries` nodes side-by-side.

```yaml
factory: "dynamic-dir"
config:
  entries:
    - name: "station_a"
      factory: "synthetic-timeseries"
      config:
        start: "2024-01-01T00:00:00Z"
        end:   "2024-01-15T00:00:00Z"
        interval: "1h"
        points:
          - name: "temperature"
            components:
              - type: sine
                amplitude: 5.0
                period: "24h"
                offset: 20.0

    - name: "combined"
      factory: "timeseries-join"
      config:
        inputs:
          - pattern: "series:///sensors/station_a"
            scope: "A"
          - pattern: "series:///sensors/station_b"
            scope: "B"
```

- Nested factory configs are validated recursively at `mknod` time.
- The directory is **read-only** — no `pond copy` into it.
- Child entries appear with the `EntryType` reported by each factory's
  metadata (e.g. `TableDynamic` for timeseries-join).
- Each child gets a deterministic `FileID` derived from the parent's
  `NodeID`, the entry name, factory, and config.

**Usage:**

```bash
pond mkdir /sensors
pond mknod dynamic-dir /sensors/all --config-path all.yaml

# Browse the virtual directory
pond list /sensors/all

# Query a child entry directly
pond cat /sensors/all/station_a --sql "SELECT COUNT(*) FROM source"
```

---

### timeseries-join

Combines two or more time-series inputs into a single wide table by
FULL OUTER JOIN on the time column. Each input can have an optional
**scope** prefix (column names become `Scope.OriginalColumn`), an
optional **time range** filter, and optional **transforms**.

Inputs that share the same `scope` are merged with `UNION BY NAME`
first, then the distinct scope groups are joined. This lets you stitch
together device replacements (same scope, non-overlapping ranges) while
also combining data from different sensor types (different scopes).

```yaml
factory: "timeseries-join"
config:
  time_column: "timestamp"          # optional, default: "timestamp"
  inputs:
    - pattern: "series:///data/station_a"
      scope: "A"
    - pattern: "series:///data/station_b"
      scope: "B"
```

**Input fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `pattern` | yes | URL pattern to match input files. Supported schemes: `series`, `csv`, `excelhtml`, `file`. Glob wildcards (`*`, `**`) are supported. |
| `scope` | no | Prefix added to every non-timestamp column: `Scope.Column`. If omitted, columns keep their original names. |
| `range.begin` | no | ISO 8601 start time — rows before this are excluded. |
| `range.end` | no | ISO 8601 end time — rows after this are excluded. |
| `transforms` | no | List of paths to table-transform factories applied to this input before joining (e.g. `["/etc/hydro_rename"]`). |

**Behaviour:**

- At least **two inputs** are required (use `sql-derived-series` for one).
- The result is ordered by the time column.
- Where one input has data and another does not, the missing columns are
  `NULL` (FULL OUTER JOIN semantics).
- The output time column is `COALESCE`-d across all scope groups so there
  are no NULLs in the time column itself.
- The node reports `EntryType::TableDynamic`.

**Production example** (combine.yaml inside a `dynamic-dir`):

```yaml
entries:
  - name: "Silver"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "/hydrovu/devices/**/SilverVulink1.series"
          scope: Vulink
          range:
            end: 2024-05-30T00:00:00Z
        - pattern: "/hydrovu/devices/**/SilverVulink2.series"
          scope: Vulink
          range:
            begin: 2024-05-30T00:00:00Z
        - pattern: "/hydrovu/devices/**/SilverAT500.series"
          scope: AT500_Surface
```

**Usage:**

```bash
pond mkdir /combined
pond mknod dynamic-dir /combined --config-path combine.yaml

# See all joined series
pond list /combined

# Query the combined data
pond cat /combined/Silver --sql "SELECT * FROM source LIMIT 10"

# Check the time span
pond cat /combined/Silver --sql "
  SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM source
"
```

---

### timeseries-pivot

Selects specific columns from multiple inputs matched by a glob pattern,
producing a wide table with one row per unique timestamp. Use it to pull
a single measurement (e.g. dissolved oxygen) across all sites into one
queryable view.

Each matched input's columns are **scoped** with the captured wildcard
segment as prefix (e.g. `Silver.DO.mg/L`, `BDock.DO.mg/L`). Missing
columns are `NULL`-padded automatically.

```yaml
factory: "timeseries-pivot"
config:
  pattern: "series:///combined/*"     # wildcard captures the site name
  columns:
    - "AT500_Surface.DO.mg/L"
    - "AT500_Bottom.DO.mg/L"
  time_column: "timestamp"            # optional, default: "timestamp"
  transforms:                         # optional
    - "/etc/hydro_rename"
```

**Config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `pattern` | yes | URL pattern with a `*` wildcard. The captured segment becomes the scope prefix for that input's columns. Supported schemes: `series`, `csv`, `excelhtml`, `file`, `data`, `table`, `oteljson`. |
| `columns` | yes | List of column names to select from each matched input. At least one column required. |
| `time_column` | no | Name of the time column. Default: `"timestamp"`. |
| `transforms` | no | List of paths to table-transform factories applied to each input. |

**Behaviour:**

- The pattern `series:///combined/*` matching `/combined/Silver` and
  `/combined/BDock` produces columns like:
  `timestamp`, `Silver.AT500_Surface.DO.mg/L`, `BDock.AT500_Surface.DO.mg/L`, …
- Uses LEFT JOIN on the time column (not FULL OUTER JOIN), from a CTE
  of all unique timestamps across all inputs.
- Columns that don't exist in a particular input are `NULL`-padded
  (Float64) via the `null_padding` transform.
- The node reports `EntryType::TableDynamic`.

**Production example** (single.yaml inside a `dynamic-dir`):

```yaml
entries:
  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "/combined/*"
      columns:
        - "AT500_Surface.DO.mg/L"
        - "AT500_Bottom.DO.mg/L"

  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "/combined/*"
      columns:
        - "AT500_Surface.Temperature.C"
        - "AT500_Bottom.Temperature.C"
```

**Usage:**

```bash
pond mknod dynamic-dir /pivot --config-path single.yaml

# See the pivoted views
pond list /pivot

# Query dissolved oxygen across all sites
pond cat /pivot/DO --sql "SELECT * FROM source ORDER BY timestamp LIMIT 20"
```

---

### temporal-reduce

Creates time-bucketed aggregations of source time-series at one or more
resolutions. The factory produces a **directory** (not a file) with this
structure:

```
/reduce/<site>/res=1h.series
/reduce/<site>/res=6h.series
/reduce/<site>/res=1d.series
```

The source pattern can match multiple files; each match becomes a
separate site subdirectory named by `out_pattern` substitution.

```yaml
factory: "temporal-reduce"
config:
  in_pattern: "series:///sources/*"   # glob — captured group is $0
  out_pattern: "$0"                   # output site name from captured group
  time_column: "timestamp"
  resolutions: ["1h", "6h", "1d"]
  aggregations:
    - type: "avg"
      columns: ["temperature", "pressure"]
    - type: "min"
      columns: ["temperature"]
    - type: "max"
      columns: ["temperature"]
    - type: "count"
      columns: ["*"]                  # count of rows per bucket
```

**Config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `in_pattern` | yes | URL pattern to match source series. Glob wildcards (`*`, `**`) supported. Captured groups become `$0`, `$1`, … for `out_pattern`. |
| `out_pattern` | yes | Output site name using captured groups (e.g. `"$0"`). |
| `time_column` | yes | Name of the timestamp column in the source. |
| `resolutions` | yes | List of time bucket sizes. Parsed with humantime: `"1h"`, `"6h"`, `"1d"`, `"30m"`, etc. |
| `aggregations` | yes | List of aggregation operations (see below). |
| `transforms` | no | List of paths to table-transform factories applied to each input before aggregation. |

**Aggregation operations:**

| Field | Required | Description |
|-------|----------|-------------|
| `type` | yes | One of `avg`, `min`, `max`, `sum`, `count`. |
| `columns` | no | List of column names to aggregate. Supports single-`*` glob patterns (e.g. `"Vulink*.Temperature.C"`). If omitted, applies to all numeric columns discovered from the source schema. Use `["*"]` with `count` for row count. |

Output column names are `original_column.agg_type` — e.g.
`temperature.avg`, `temperature.min`, `pressure.avg`.

**Behaviour:**

- Uses `DATE_TRUNC` for time bucketing, so buckets align to calendar
  boundaries (hour 0, midnight, etc.).
- Source schema is discovered dynamically on first query — column names
  in `columns` are matched against actual schema at runtime.
- Each resolution file (`res=1h.series`, etc.) is an independent
  `TableDynamic` node backed by `SqlDerivedFile`.
- The directory structure is **read-only**.

**Usage:**

```bash
# Create the source data
pond mkdir /sources
pond mknod synthetic-timeseries /sources/weather --config-path weather.yaml

# Create the temporal-reduce factory
pond mknod temporal-reduce /reduce --config-path reduce.yaml

# Browse the directory structure
pond list /reduce                        # → /reduce/weather
pond list /reduce/weather                # → res=1h.series, res=6h.series, ...

# Query the hourly aggregation
pond cat /reduce/weather/res=1h.series --sql "
  SELECT timestamp,
         ROUND(\"temperature.avg\", 2) AS temp_avg,
         ROUND(\"temperature.min\", 2) AS temp_min,
         ROUND(\"temperature.max\", 2) AS temp_max
  FROM source
  ORDER BY timestamp
"

# Check schema
pond describe /reduce/weather/res=1h.series
```

### sitegen

Static site generator using Maudit + Maud. Reads data from the pond,
applies Markdown templates with shortcodes, and produces a complete
static HTML site with interactive charts (DuckDB-WASM + Observable Plot).

```yaml
factory: sitegen

site:
  title: "My Dashboard"
  base_url: "/"                       # Use "/subdir/" for non-root deploy

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    temporal: ["year", "month"]       # Partition exported Parquet by year/month
  - name: "sites"
    pattern: "/reduced/single_site/*/*.series"
    temporal: ["year", "month"]

routes:
  - name: "home"
    type: static
    slug: ""                          # Root: /index.html
    page: "/etc/site/index.md"
  - name: "params"
    type: static
    slug: "params"                    # /params/index.html (if page given)
    routes:
      - name: "param-detail"
        type: template
        slug: "$0"                    # /params/Temperature.html, etc.
        page: "/etc/site/data.md"
        export: "params"              # Links to export stage by name
  - name: "sites"
    type: static
    slug: "sites"
    routes:
      - name: "site-detail"
        type: template
        slug: "$0"                    # /sites/NorthDock.html, etc.
        page: "/etc/site/data.md"
        export: "sites"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []                     # Extra files to copy (optional)
```

**Config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `site.title` | yes | Site title, used in HTML `<title>` and layout |
| `site.base_url` | yes | Base URL path: `"/"` for root, `"/myapp/"` for subdirectory |
| `exports` | yes | List of data export stages (see below) |
| `routes` | yes | Hierarchical route tree |
| `partials` | no | Named Markdown partials (e.g., sidebar) |
| `static_assets` | no | List of patterns for static files to copy |

**Export fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Name referenced by `export:` in routes |
| `pattern` | yes | Glob pattern matching pond files. `*` captures become `$0`, `$1`, ... |
| `temporal` | no | List of temporal partition keys (e.g., `["year", "month"]`) |

**Route types:**

| Type | Produces | Has `export`? | `slug` semantics |
|------|----------|---------------|------------------|
| `static` | One page at that path | No | Literal slug (empty = root) |
| `template` | One page per unique `$0` value | Yes | `$0` expands from matched captures |

**Behaviour:**

- Routes are hierarchical — nested routes inherit the parent slug as prefix.
- Template routes generate one HTML page per unique `$0` value from the linked export.
- Export stages run first: match data files, extract temporal bounds, export Parquet with partitioning.
- Each template page receives its matched `ExportedFile` structs as context for shortcodes.

**Layouts** (set in Markdown frontmatter):

| Layout | Purpose |
|--------|---------|
| `default` | Full-width content (home pages, text content) |
| `data` | Sidebar + chart area with JS chart infrastructure |

**Shortcodes** (used in Markdown templates):

| Shortcode | Purpose | Example |
|-----------|---------|---------|
| `{{ $0 }}` | First capture group value | Page title from pattern match |
| `{{ $1 }}` | Second capture group | Resolution name, etc. |
| `{{ chart /}}` | Chart container + DuckDB-WASM chart renderer | Data pages |
| `{{ breadcrumb /}}` | Breadcrumb navigation from route hierarchy | Data pages |
| `{{ nav_list collection="name" base="/path" /}}` | Navigation list from export collection | Sidebar |

⚠️ **Shortcode syntax**: Use self-closing `{{ name /}}` form. Attributes use
`key="value"` syntax: `{{ nav_list collection="params" base="/params" /}}`.
In the title frontmatter, `{{ $0 }}` does NOT need the closing `/}}`.

**Markdown templates — example data page (`data.md`):**

```markdown
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ breadcrumb /}}

{{ chart /}}
```

**Markdown templates — example sidebar (`sidebar.md`):**

```markdown
## My Dashboard

- [Home](/)

### By Parameter

{{ nav_list collection="params" base="/params" /}}

### By Site

{{ nav_list collection="sites" base="/sites" /}}
```

**Build command:**

```bash
pond run /etc/site.yaml build ./dist
```

This single command: reads the site config → runs export stages (exports Parquet
with temporal partitioning) → renders all routes (Markdown → HTML via Maud layouts)
→ writes everything to `./dist`.

**Serving locally:**

```bash
# Vite (live reload on re-build)
npx vite ./dist --port 4174 --open

# or plain HTTP
python3 -m http.server 8000 -d ./dist
```

**Usage:**

```bash
# Store templates in the pond
pond copy host:///path/to/site /etc/site

# Create the sitegen factory node
pond mknod sitegen /etc/site.yaml --config-path /path/to/site.yaml

# Build the site
pond run /etc/site.yaml build ./dist

# Update templates without recreating the factory
pond copy host:///path/to/site /etc/site --overwrite

# Update factory config
pond mknod sitegen /etc/site.yaml --overwrite --config-path /path/to/site.yaml
```

**Notes:**
- The generated site uses DuckDB-WASM to load Parquet files client-side — no server needed.
- Charts use Observable Plot for rendering.
- Vendor JS is loaded from CDN — no Node.js build toolchain required.
- All CSS and JS is bundled into the HTML layouts (Maud code in `crates/sitegen`).
- The site is fully self-contained after build — deploy to any static host.

---

### column-rename

Transform factory for renaming, casting, or dropping columns. Applied via
the `transforms` field of other factories (timeseries-join, timeseries-pivot,
temporal-reduce).

```yaml
rules:
  - type: direct
    from: "Date Time"
    to: "timestamp"
    cast: timestamp              # Optional: cast to type
  - type: pattern
    pattern: "^(.+) \\((.+)\\)$"
    replacement: "$1.$2"         # Regex capture groups
```

**Rule types:**

| Type | Fields | Description |
|------|--------|-------------|
| `direct` | `from`, `to`, optional `cast` | Rename one column exactly; optionally cast its type |
| `pattern` | `pattern`, `replacement` | Regex match on column names; `$1`, `$2` for capture groups |

**Usage:**

```bash
# Create the transform node
pond mknod column-rename /etc/hydro_rename --config-path hrename.yaml

# Reference it from other factories via transforms field
```

```yaml
# In a timeseries-join config:
inputs:
  - pattern: "series:///data/readings"
    scope: "Station"
    transforms: ["/etc/hydro_rename"]
```

**Notes:**
- The transform is applied at query time, not stored — it wraps the source TableProvider.
- Multiple transforms are applied in order.
- The `cast` field supports: `timestamp`, `float64`, `int64`, `utf8`.

---

### remote

Backup and replication to S3-compatible storage.

```yaml
# Local file backup
url: "file:///backup/location"
compression_level: 3

# S3 backup
url: "s3://bucket-name"
endpoint: "http://localhost:9000"  # For MinIO
region: "us-east-1"
access_key_id: "..."
secret_access_key: "..."
allow_http: true  # Required for non-HTTPS endpoints
```

**Commands:**
```bash
# Push local pond to remote backup
pond run /etc/system.d/10-remote push

# Pull from remote (restore)
pond run /etc/system.d/10-remote pull

# Verify backup integrity
pond run /etc/system.d/10-remote verify

# List files in remote storage
pond run /etc/system.d/10-remote list-files

# Show files with verification script (for external tool validation)
pond run /etc/system.d/10-remote show           # All files
pond run /etc/system.d/10-remote show "/data/*" # Pattern match
pond run /etc/system.d/10-remote show --script  # Generate copy-pastable scripts
```

#### Emergency Recovery (duckpond-emergency)

A standalone shell script for disaster recovery when the pond binary is unavailable.
Uses only DuckDB to read backup data directly from parquet files.

**Location:** `crates/cmd/scripts/duckpond-emergency`

**Requirements:**
- DuckDB CLI
- b3sum (optional, for BLAKE3 verification)

**Usage:**
```bash
# List all files in backup
duckpond-emergency /path/to/backup list

# Show backup metadata
duckpond-emergency /path/to/backup info

# Extract files matching pattern (SQL LIKE syntax: % = wildcard)
duckpond-emergency /path/to/backup extract "_delta_log%" ./delta_logs/

# Verify BLAKE3 checksums
duckpond-emergency /path/to/backup verify

# Export all files
duckpond-emergency /path/to/backup export-all ./full_restore/
```

**S3/MinIO:**
```bash
export AWS_ENDPOINT_URL="http://localhost:9000"
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
duckpond-emergency s3://bucket/backup list
```

### hydrovu

HydroVu API data collection.

```yaml
client_id: "xxx"
client_secret: "yyy"
devices:
  - name: "Station A"
    id: 12345
    scope: "StationA"
```

### logfile-ingest

Mirror rotating log files from the host filesystem into the pond. Tracks files with bao-tree blake3 digests for efficient append detection.

```yaml
# Pattern for archived (immutable) log files
archived_pattern: /var/log/app/app.log.*
# Pattern for the active (append-only) log file
active_pattern: /var/log/app/app.log
# Destination path within the pond
pond_path: /logs/app
```

**Usage:**
```bash
# Create the factory node
pond mknod logfile-ingest /etc/system.d/10-logs --config-path ingest.yaml

# Run ingestion (push mode)
pond run /etc/system.d/10-logs
pond run /etc/system.d/10-logs push   # explicit

# Verify checksums (b3sum format)
pond run /etc/system.d/10-logs b3sum
```

**Behavior:**
- **Archived files** (matching `archived_pattern`): Immutable - ingested once, verified unchanged
- **Active file** (matching `active_pattern`): Append-only - detects new bytes via cumulative bao-tree hash
- **Rotation detection**: When active file shrinks or content prefix changes, searches for matching archived file

**Important:** Files >64KB are stored externally in parquet (not inline in oplog). See `docs/large-file-storage-implementation.md`.

**For Linux system logs (/var/log/syslog):**

Configure logrotate with `nocompress` to keep archived logs readable:

```
# /etc/logrotate.d/syslog-nocompress
/var/log/syslog
/var/log/messages
{
    rotate 4
    weekly
    missingok
    notifempty
    nocompress          # Required for logfile-ingest
    create 644 root adm
    postrotate
        /usr/lib/rsyslog/rsyslog-rotate 2>/dev/null || true
    endscript
}
```

Example configuration for syslog:
```yaml
archived_pattern: /var/log/syslog.*
active_pattern: /var/log/syslog
pond_path: /logs/system
```

**Container testing notes:**

When testing in Docker containers, rsyslog needs kernel logging disabled:
```bash
# Disable imklog module (not available in containers)
sed -i 's/module(load="imklog")/#module(load="imklog")/' /etc/rsyslog.conf
rsyslogd

# Generate log entries with logger command
logger -t myapp "Test message"
```

---

## Glob Patterns

DuckPond uses glob patterns for file matching:

| Pattern | Matches |
|---------|---------|
| `*` | Any single path component |
| `**` | Any number of path components |
| `?` | Any single character |
| `[abc]` | Character class |

Examples:
- `**/*` - All files everywhere
- `/*` - Files in root only
- `/data/**/*.csv` - All CSVs under /data
- `/sensors/*/latest.series` - latest.series in any sensor subdirectory

---

## End-to-End Pipeline Examples

DuckPond factories compose into data pipelines. Here are two real-world patterns.

### Multi-Site / Multi-Parameter (Noyo)

The Noyo Harbor example monitors water quality at multiple sites, each with multiple
sensor parameters. The pipeline fans out then fans in:

```
hydrovu (collector)
    → /hydrovu/devices/**/*.series       (raw per-device series)

timeseries-join (/combined)
    → /combined/NorthDock                (all params at one site, wide table)
    → /combined/SouthDock

timeseries-pivot (/singled)
    → /singled/Temperature               (one param across all sites, wide table)
    → /singled/DO

temporal-reduce (/reduced)
    → /reduced/single_site/NorthDock/res={1h,6h,1d}.series
    → /reduced/single_param/Temperature/res={1h,6h,1d}.series

sitegen (/etc/site.yaml)
    → dist/ (HTML + Parquet, served as static site)
```

**Key configs:** `noyo/combine.yaml` (join), `noyo/single.yaml` (pivot),
`noyo/reduce.yaml` (reduce), `noyo/site.yaml` (sitegen).

**Setup pattern:**
```bash
pond init
pond copy host:///path/to/site  /etc/site          # Markdown templates
pond mknod dynamic-dir /combined --config-path combine.yaml
pond mknod dynamic-dir /singled  --config-path single.yaml
pond mknod dynamic-dir /reduced  --config-path reduce.yaml
pond mknod sitegen /etc/site.yaml --config-path site.yaml
pond mknod column-rename /etc/hydro_rename --config-path hrename.yaml
pond run /etc/site.yaml build ./dist
```

### Single-Source (Septic)

The septic station example has a single OTelJSON data source with ~40 metrics from two
scopes (BME280 environment sensor + Orenco Modbus registers) at different sample rates.
There's no need for join/pivot — the `oteljson://` URL scheme provides a single wide table.

```
logfile-ingest (/etc/ingest)
    → /ingest/septicstation.json         (raw OTelJSON Lines file)

temporal-reduce (/reduced)                using oteljson:// URL scheme
    → /reduced/septic/res={1h,6h,1d}.series

sitegen (/etc/site.yaml)
    → dist/ (HTML + Parquet)
```

**Key difference from Noyo:** The `in_pattern` in `temporal-reduce` uses the
`oteljson://` URL scheme directly, skipping the join/pivot stages:

```yaml
factory: "temporal-reduce"
config:
  in_pattern: "oteljson:///ingest/septicstation.json"
  out_pattern: "septic"
  time_column: "timestamp"
  resolutions: [1h, 6h, 1d]
  aggregations:
    - type: "avg"
      columns:
        - "septicstation_temperature"
        - "orenco_RT_Pump1_Amps"
        # ... all gauge metrics
```

**Setup pattern:**
```bash
pond init
pond mkdir -p /etc/system.d
pond mkdir -p /ingest
pond copy host:///path/to/site /etc/site
pond mknod logfile-ingest /etc/ingest --config-path ingest.yaml
pond mknod remote /etc/system.d/1-backup --config-path backup.yaml
pond mknod temporal-reduce /reduced --config-path reduce.yaml
pond mknod sitegen /etc/site.yaml --config-path site.yaml

# Operational cycle:
pond run /etc/ingest                        # ingest new log data
pond run /etc/site.yaml build ./dist        # build site (reduce is dynamic)
pond run /etc/system.d/1-backup push        # backup to S3
```

**Notes on single-source pipelines:**
- `oteljson://` discovers all metric names as columns automatically (two-pass parser).
- Different sample rates (e.g., 5min Modbus, 10min BME280) produce NULL-padded rows
  in the merged table. `temporal-reduce` aggregations handle NULLs correctly — `avg`,
  `min`, `max` skip NULLs per SQL semantics.
- No glob in `in_pattern` means `out_pattern` is a literal name (not `$0`).
- `temporal-reduce` is a dynamic factory — its output is computed on read. No
  explicit `pond run /reduced update` step is needed; sitegen triggers computation
  when it reads the export data during `build`.

### Choosing a Pipeline Shape

| Scenario | Pipeline | Why |
|----------|----------|-----|
| Multiple sites, multiple params | join → pivot → reduce → sitegen | Need cross-site and cross-param views |
| Single source, many metrics | reduce → sitegen | oteljson:// or csv:// provides the wide table |
| Single source, few metrics | reduce → sitegen | Same, but simpler reduce config |
| Raw file archive only | logfile-ingest + remote | No analysis, just backup |

---

## Factory Type Quick Reference

| Factory | Kind | Created By | Output Type |
|---------|------|------------|-------------|
| `synthetic-timeseries` | dynamic | `pond mknod` | `TableDynamic` |
| `sql-derived-table` | dynamic | `pond mknod` | `TableDynamic` |
| `sql-derived-series` | dynamic | `pond mknod` | `TableDynamic` |
| `dynamic-dir` | dynamic | `pond mknod` | `DynamicDirectory` |
| `timeseries-join` | dynamic | `pond mknod` | `TableDynamic` |
| `timeseries-pivot` | dynamic | `pond mknod` | `TableDynamic` |
| `temporal-reduce` | dynamic | `pond mknod` | `DynamicDirectory` of `TableDynamic` |
| `column-rename` | transform | `pond mknod` | Wraps `TableProvider` |
| `sitegen` | executable | `pond mknod` + `pond run ... build` | Static files on host |
| `logfile-ingest` | executable | `pond mknod` + `pond run` | `data` entries in pond |
| `hydrovu` | executable | `pond mknod` + `pond run ... collect` | `table:series` in pond |
| `remote` | executable | `pond mknod` + `pond run ... push/pull` | Backup bundles on S3 |

**Dynamic** factories compute on every read — no stored state.
**Executable** factories have side effects — run explicitly with `pond run`.
**Transform** factories are referenced via the `transforms` field of other factories.

---

## Troubleshooting

### "No matching files" when files exist

**Cause**: Pattern doesn't match actual paths.
**Fix**: Use `pond list '**/*'` first to see what exists, then refine pattern.

### `pond cat` outputs binary garbage

**Cause**: You're catting a table/series entry without `--format=table` or `--sql`.
The default output format is Parquet bytes (binary).
**Fix**: Add `--format=table` for human-readable output, or `--sql "SELECT * FROM source LIMIT 10"`.

### `pond copy --format=table` errors on CSV

**Cause**: `--format=table` validates PAR1 magic bytes — only Parquet input accepted.
**Fix**: Use default `--format=data` for CSV. Query CSV with `pond cat csv:///path --sql "..."`.

### Export pattern matches nothing in sitegen

**Cause**: The export `pattern` in site.yaml doesn't match what temporal-reduce creates.
**Fix**: Run `pond list '/reduced/**'` to see the actual directory structure. Common
mismatch: the reduce output might be `/reduced/site/res=1h.series` but the export pattern
expects `/reduced/site/*.series` or `/reduced/site/*/*.series`.

### oteljson:// queries are slow

**Cause**: The two-pass parser reads the entire file twice — once to discover columns,
once to parse data. Large files (>100MB) can be slow.
**Fix**: For repeated queries, consider ingesting into a table:series via SQL pipeline.
For one-off exploration, use `--sql` with `LIMIT` to reduce output.

### Dynamic factory shows stale data

**Cause**: Dynamic factories are re-computed on every read, but they read from their
source which may itself be cached or stale.
**Fix**: For logfile-ingest sources, run `pond run /etc/ingest` first to ingest new data.
The dynamic factory chain (temporal-reduce → sitegen) will then see the updated source.

### temporal-reduce columns not found

**Cause**: Column names in `aggregations.columns` don't match the source schema.
Column names are matched against the actual schema at runtime.
**Fix**: Discover column names first:
```bash
pond cat oteljson:///ingest/data.json --format=table --sql "
  SELECT column_name FROM information_schema.columns
  WHERE table_name = 'source' ORDER BY column_name
"
```

### sitegen build fails with "unknown shortcode"

**Cause**: Typo in Markdown template or using wrong shortcode syntax.
**Fix**: Check shortcode names: `{{ $0 }}`, `{{ chart /}}`, `{{ breadcrumb /}}`,
`{{ nav_list collection="..." base="..." /}}`. Note the self-closing `/}}` syntax.

---

## See Also

- [testsuite/](../testsuite/) - Runnable test scripts
- [duckpond-overview.md](duckpond-overview.md) - Architecture overview
