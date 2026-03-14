# Web Log Ingestion Design

## Goal

Ingest web server access logs (nginx, Apache, or similar) from the local
machine into a DuckPond instance, enabling historical traffic queries and
pageview monitoring exposed through sitegen dashboards.

## Decisions

| Question | Decision |
|---|---|
| Log formats supported | **Combined Log Format** (text) via new `weblog://` provider, **JSON** via existing `jsonlogs://` |
| Ingestion mechanism | **`logfile-ingest`** factory (already exists) for rotating log file mirroring |
| New code: format provider | **`weblog://`** -- parses CLF/Combined text format into typed Arrow columns |
| Aggregation | **`sql-derived-series`** nodes compute traffic metrics from raw logs |
| sql-derived format allowlist | **Expand** to include `weblog` and `jsonlogs` |
| Presentation | **Sitegen** exports aggregated series as time-series charts |
| Storage granularity | **One `FilePhysicalSeries` per log file** (mirrored by logfile-ingest) |
| Entry type for raw logs | **`FilePhysicalSeries`** (via logfile-ingest, append-only log files) |

## Architecture

```
 +-----------------------------------------------------+
 |  Web Server (nginx/Apache)                          |
 |    /var/log/nginx/access.log      (active, rotating)|
 |    /var/log/nginx/access.log.1    (archived)        |
 |    /var/log/nginx/access.log.2.gz (archived, gzip)  |
 +-----------------------------------------------------+
          |
          v
 +-----------------------------------------------------+
 |  logfile-ingest factory (already exists)             |
 |    archived_pattern: /var/log/nginx/access.log.*     |
 |    active_pattern:   /var/log/nginx/access.log       |
 |    pond_path:        /logs/web/mysite                |
 |                                                      |
 |  Mirrors rotating log files into pond as             |
 |  FilePhysicalSeries with bao-tree tracking           |
 +-----------------------------------------------------+
          |
          v
 +-----------------------------------------------------+
 |  /logs/web/mysite/                                   |
 |    access.log         (FilePhysicalSeries, active)   |
 |    access.log.1       (FilePhysicalSeries, archived) |
 |    access.log.2.gz    (FilePhysicalSeries, archived) |
 +-----------------------------------------------------+
          |
          v (queried via weblog:// or jsonlogs:// format provider)
 +-----------------------------------------------------+
 |  sql-derived-series nodes (dynamic, computed on read)|
 |                                                      |
 |  /derived/web/mysite/                                |
 |    pageviews.series    -- hourly pageview counts     |
 |    status_codes.series -- status code breakdown      |
 |    top_pages.series    -- top pages by request count |
 |    referrers.series    -- top referrers              |
 +-----------------------------------------------------+
          |
          v (exported by sitegen)
 +-----------------------------------------------------+
 |  sitegen                                             |
 |    exports:                                          |
 |      - name: "traffic"                               |
 |        pattern: "/derived/web/*/*.series"             |
 |    renders: time-series charts of traffic metrics    |
 +-----------------------------------------------------+
```

## Layer 1: Format Provider -- `weblog://`

### What It Parses

The **Combined Log Format** (nginx default, Apache `LogFormat combined`):

```
93.184.216.34 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326 "http://www.example.com/start.html" "Mozilla/5.0 ..."
```

Fields (positional):
1. `remote_addr` -- Client IP address
2. `remote_ident` -- RFC 1413 identity (always `-`)
3. `remote_user` -- Authenticated user (or `-`)
4. `timestamp` -- `[DD/Mon/YYYY:HH:MM:SS +ZZZZ]`
5. `request` -- `"METHOD /path HTTP/version"`
6. `status` -- HTTP status code
7. `body_bytes_sent` -- Response size in bytes
8. `http_referer` -- Referrer URL (Combined format only)
9. `http_user_agent` -- User agent string (Combined format only)

### Output Schema

The `weblog://` provider parses each line into typed Arrow columns:

| Column | Arrow Type | Source | Notes |
|---|---|---|---|
| `timestamp` | `TimestampMicrosecond(UTC)` | Parsed from `[DD/Mon/YYYY:HH:MM:SS +ZZZZ]` | Native timestamp for DataFusion |
| `remote_addr` | `Utf8` | First field | Client IP |
| `remote_user` | `Utf8` | Third field | `-` becomes null |
| `method` | `Utf8` | Extracted from request | `GET`, `POST`, etc. |
| `path` | `Utf8` | Extracted from request | URL path |
| `protocol` | `Utf8` | Extracted from request | `HTTP/1.1`, etc. |
| `status` | `UInt16` | Status code | Native integer for filtering |
| `body_bytes_sent` | `UInt64` | Response size | Native integer for aggregation |
| `http_referer` | `Utf8` | Referrer header | `-` becomes null |
| `http_user_agent` | `Utf8` | User agent | Full UA string |

### Design Choices

**Typed columns, not all-Utf8.** Unlike `jsonlogs://` (which uses all-Utf8 for
flexibility with unknown schemas), `weblog://` parses into native Arrow types
because:
- The schema is fixed and well-known
- `timestamp` as native TimestampMicrosecond enables DataFusion temporal functions
  (`date_trunc`, `date_part`) without `CAST` in every query
- `status` as UInt16 enables direct comparison (`WHERE status >= 400`)
- `body_bytes_sent` as UInt64 enables `SUM`/`AVG` without casting

**Robust parsing.** Lines that don't match the expected format are skipped with a
log warning (same pattern as `jsonlogs://`). Common edge cases:
- Malformed request strings (e.g., raw bytes, protocol scanners)
- Missing Combined fields (falls back to Common Log Format -- 7 fields instead of 9)
- IPv6 addresses in `remote_addr`

**Compression support.** Works with existing compression layer:
`weblog+gzip:///path`, `weblog+zstd:///path`. This handles archived logs
compressed by logrotate.

### URL Examples

```bash
# Read raw text access log from host
pond cat host+weblog:///var/log/nginx/access.log --format=table \
  --sql "SELECT timestamp, method, path, status FROM source LIMIT 20"

# Read gzip-compressed archived log
pond cat host+weblog+gzip:///var/log/nginx/access.log.2.gz --format=table \
  --sql "SELECT path, COUNT(*) as hits FROM source GROUP BY path ORDER BY hits DESC LIMIT 10"

# Read from pond (after logfile-ingest mirrors the file)
pond cat weblog:///logs/web/mysite/access.log --format=table \
  --sql "SELECT date_trunc('hour', timestamp) as hour, COUNT(*) as requests \
         FROM source GROUP BY hour ORDER BY hour"
```

### Implementation

New file: `crates/provider/src/format/weblog.rs`

Registered as `"weblog"` via `register_format_provider!`.

Parsing approach:
- Line-by-line regex or manual split (regex is clearer for CLF)
- CLF regex pattern:
  ```
  ^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d{3}) (\d+|-)(?:\s+"([^"]*)" "([^"]*)")?$
  ```
- Timestamp parsing: `chrono::NaiveDateTime::parse_from_str` with
  `%d/%b/%Y:%H:%M:%S %z` format, convert to microseconds since epoch
- Request splitting: `"GET /path HTTP/1.1"` -> `("GET", "/path", "HTTP/1.1")`
- Batch size: 8192 rows per RecordBatch (same as CSV provider)

### JSON Access Logs (Alternative)

For nginx configured with JSON output (`log_format json ...`), the existing
`jsonlogs://` provider already works. Example nginx config:

```nginx
log_format json_combined escape=json
  '{'
    '"timestamp":"$time_iso8601",'
    '"remote_addr":"$remote_addr",'
    '"method":"$request_method",'
    '"path":"$uri",'
    '"status":$status,'
    '"body_bytes_sent":$body_bytes_sent,'
    '"http_referer":"$http_referer",'
    '"http_user_agent":"$http_user_agent"'
  '}';

access_log /var/log/nginx/access.json json_combined;
```

Then: `pond cat jsonlogs:///logs/web/mysite/access.json --sql "SELECT ..."`

The `jsonlogs://` provider stores everything as Utf8, so queries need
`CAST(status AS INT)` etc.  For JSON logs, the aggregation SQL in
`sql-derived-series` handles the casting.

## Layer 2: Ingestion -- `logfile-ingest` Factory

The existing `logfile-ingest` factory (documented in
`docs/archive/logfile-ingestion-factory-design.md`) handles all the hard
filesystem work:

- Mirrors rotating log files from host directories into the pond
- Tracks archived (immutable) and active (append-only) files
- Uses bao-tree blake3 digests for efficient change detection
- Detects log rotation (active file shrinks, new archived file appears)
- Incremental append for the active file (only reads new bytes)

### Configuration for Web Logs

```yaml
factory: logfile-ingest

# Web server access logs (CLF/Combined text format)
name: mysite-access
archived_pattern: "/var/log/nginx/access.log.*"
active_pattern: "/var/log/nginx/access.log"
pond_path: "logs/web/mysite"
```

Place this factory node in `/system/etc/web-ingest`:
```bash
pond mknod /system/etc/web-ingest --config web-ingest.yaml
pond run web-ingest push
```

Schedule via cron or systemd timer (hourly or more frequently):
```bash
# Every 15 minutes
*/15 * * * * pond run web-ingest push
```

### Multiple Sites

For multiple vhosts/sites, use separate factory nodes:

```bash
# Site 1: main website
pond mknod /system/etc/web-ingest-main --config main-ingest.yaml

# Site 2: API server
pond mknod /system/etc/web-ingest-api --config api-ingest.yaml
```

Each with its own `pond_path` (`logs/web/main`, `logs/web/api`).

## Layer 3: Aggregation -- `sql-derived-series` Nodes

Raw access logs are useful for ad-hoc queries, but sitegen needs pre-aggregated
time-series data for charting. `sql-derived-series` nodes compute these
aggregations dynamically.

### Prerequisite: Expand sql-derived Format Allowlist

Currently `sql-derived-series` only accepts `csv`, `excelhtml`, and `oteljson`
as format providers. This design requires adding `weblog` and `jsonlogs` to:

1. `KNOWN_SCHEMES` array (line ~691 in sql_derived.rs)
2. `is_format_provider` match (lines ~439 and ~953 in sql_derived.rs)

This is a small, mechanical change -- add the scheme strings to the existing
match expressions.

### Aggregation Nodes

All nodes live under `/derived/web/mysite/` and reference raw logs via
`weblog:///logs/web/mysite/*.log*` patterns.

#### Pageviews per Hour

```yaml
factory: sql-derived-series

patterns:
  logs: "weblog:///logs/web/mysite/access.*"

query: >
  SELECT
    date_trunc('hour', timestamp) as timestamp,
    path,
    COUNT(*) as requests,
    COUNT(DISTINCT remote_addr) as unique_visitors
  FROM logs
  WHERE status >= 200 AND status < 400
    AND method = 'GET'
    AND path NOT LIKE '%.css'
    AND path NOT LIKE '%.js'
    AND path NOT LIKE '%.png'
    AND path NOT LIKE '%.jpg'
    AND path NOT LIKE '%.svg'
    AND path NOT LIKE '%.woff%'
    AND path NOT LIKE '/data/%'
  GROUP BY date_trunc('hour', timestamp), path
  ORDER BY timestamp
```

```bash
pond mknod /derived/web/mysite/pageviews.series --config pageviews.yaml
```

#### Status Code Distribution

```yaml
factory: sql-derived-series

patterns:
  logs: "weblog:///logs/web/mysite/access.*"

query: >
  SELECT
    date_trunc('hour', timestamp) as timestamp,
    CASE
      WHEN status >= 200 AND status < 300 THEN '2xx'
      WHEN status >= 300 AND status < 400 THEN '3xx'
      WHEN status >= 400 AND status < 500 THEN '4xx'
      WHEN status >= 500 THEN '5xx'
    END as status_class,
    COUNT(*) as count
  FROM logs
  GROUP BY date_trunc('hour', timestamp), status_class
  ORDER BY timestamp
```

#### Top Pages (Daily)

```yaml
factory: sql-derived-series

patterns:
  logs: "weblog:///logs/web/mysite/access.*"

query: >
  SELECT
    date_trunc('day', timestamp) as timestamp,
    path,
    COUNT(*) as hits,
    COUNT(DISTINCT remote_addr) as visitors
  FROM logs
  WHERE status >= 200 AND status < 400
    AND method = 'GET'
    AND path NOT LIKE '%.css'
    AND path NOT LIKE '%.js'
    AND path NOT LIKE '%.png'
    AND path NOT LIKE '%.jpg'
    AND path NOT LIKE '%.svg'
    AND path NOT LIKE '%.woff%'
    AND path NOT LIKE '/data/%'
  GROUP BY date_trunc('day', timestamp), path
  ORDER BY timestamp, hits DESC
```

#### Referrer Tracking

```yaml
factory: sql-derived-series

patterns:
  logs: "weblog:///logs/web/mysite/access.*"

query: >
  SELECT
    date_trunc('day', timestamp) as timestamp,
    CASE
      WHEN http_referer IS NULL THEN 'direct'
      WHEN http_referer = '-' THEN 'direct'
      ELSE regexp_replace(http_referer, '^https?://([^/]+).*', '\1')
    END as referrer_domain,
    COUNT(*) as visits
  FROM logs
  WHERE status >= 200 AND status < 400
    AND method = 'GET'
  GROUP BY date_trunc('day', timestamp), referrer_domain
  ORDER BY timestamp, visits DESC
```

### JSON Log Variant

For JSON-formatted nginx logs, swap `weblog://` for `jsonlogs://` in patterns
and add `CAST()` for numeric fields:

```yaml
patterns:
  logs: "jsonlogs:///logs/web/mysite/access.*"

query: >
  SELECT
    date_trunc('hour', CAST(timestamp AS TIMESTAMP)) as timestamp,
    path,
    COUNT(*) as requests
  FROM logs
  WHERE CAST(status AS INT) >= 200 AND CAST(status AS INT) < 400
  GROUP BY date_trunc('hour', CAST(timestamp AS TIMESTAMP)), path
  ORDER BY timestamp
```

## Layer 4: Presentation -- Sitegen

Sitegen already supports exporting queryable pond files as parquet and
rendering time-series charts via `chart.js`. The aggregated
`sql-derived-series` nodes produce exactly the right shape: timestamped
tables with numeric columns.

### Site Config Addition

Add to `site.yaml` (or create a new site config for a traffic dashboard):

```yaml
exports:
  - name: "traffic"
    pattern: "/derived/web/*/*.series"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "traffic"
        type: static
        slug: "traffic"
        routes:
          - name: "traffic-detail"
            type: template
            slug: "$0"
            page: "/site/traffic.md"
            export: "traffic"
```

### Traffic Dashboard Page

`/site/traffic.md`:
```markdown
---
title: "Traffic: {{ title }}"
layout: data
---

{{ chart / }}
```

The `{{ chart / }}` shortcode renders the exported parquet data as a
time-series chart. Each `sql-derived-series` node becomes a separate
chart page showing pageviews, status codes, referrers, etc.

### What Sitegen Already Handles

- Export partitioning by `date_part('month', timestamp)` -- traffic data
  is automatically split into monthly parquet files
- Client-side DuckDB-WASM queries the parquet files with time-range filtering
- Time-series line/area charts with interactive zoom
- Static HTML output, no server needed for viewing

### What Might Need Enhancement (Future)

- **Table view shortcode**: For "top pages" data, a ranked table is more
  useful than a time-series chart. Sitegen could add a `{{ table / }}`
  shortcode that renders a sortable HTML table from exported parquet.
  This is optional -- the chart view still works, just not ideal for
  ranked data.
- **Dashboard layout**: A combined page showing all traffic metrics at
  once (pageviews chart + status chart + top pages table + referrer table).
  Currently sitegen renders one export per page.

## Storage Layout

```
/logs/
  web/
    mysite/                          # per-site directory
      access.log                     # FilePhysicalSeries (active, mirrored)
      access.log.1                   # FilePhysicalSeries (archived)
      access.log.2.gz                # FilePhysicalSeries (archived, gzip)

/derived/
  web/
    mysite/
      pageviews.series               # sql-derived-series (dynamic)
      status_codes.series            # sql-derived-series (dynamic)
      top_pages.series               # sql-derived-series (dynamic)
      referrers.series               # sql-derived-series (dynamic)

/system/
  etc/
    web-ingest/                      # logfile-ingest factory node
    90-sitegen/                      # sitegen factory node
  site/
    traffic.md                       # traffic dashboard template
```

## Implementation Phases

### Phase 1: `weblog://` Format Provider

New file: `crates/provider/src/format/weblog.rs`

- Implement `FormatProvider` trait
- Parse Combined Log Format (and degrade gracefully to Common Log Format)
- Typed output schema (timestamp, status, body_bytes_sent as native types)
- Register via `register_format_provider!(scheme: "weblog", ...)`
- Add `pub mod weblog;` to `crates/provider/src/format/mod.rs`
- Unit tests following `jsonlogs.rs` test patterns

### Phase 2: Expand `sql-derived` Format Allowlist

In `crates/provider/src/factory/sql_derived.rs`:

- Add `"weblog"` and `"jsonlogs"` to `KNOWN_SCHEMES` (line ~691)
- Add `"weblog" | "jsonlogs"` to both `is_format_provider` matches (~439, ~953)
- This is 3 edits to string match expressions

### Phase 3: Integration Testing

Testsuite scripts exercising the full pipeline:

1. Create sample CLF access log on host
2. `pond init && pond mkdir /logs/web/test`
3. `logfile-ingest` mirrors the log
4. `pond cat weblog:///logs/web/test/access.log --format=table` queries it
5. `sql-derived-series` computes pageview aggregation
6. Verify aggregated data via `pond cat --format=table`

### Phase 4: Sitegen Traffic Dashboard

- Add export + route config for traffic data
- Create traffic dashboard template page
- Build and verify HTML output

## Related Documentation

- [Logfile Ingestion Factory Design](archive/logfile-ingestion-factory-design.md) -- raw log file mirroring
- [Journal Log Collection Design](journal-log-collection-design.md) -- similar pattern for systemd logs
- [Sitegen Design](sitegen-design.md) -- static site generation
- [DuckPond System Patterns](duckpond-system-patterns.md) -- transaction handling, factory patterns
- [CLI Reference](cli-reference.md) -- URL schemes, format providers
