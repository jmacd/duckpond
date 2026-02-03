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
# List everything
pond list              # defaults to '**/*'
pond list '**/*'       # explicit

# List specific directory contents  
pond list '/data/*'    # files in /data
pond list '/data/**/*' # recursive

# Pattern matching
pond list '**/*.csv'   # all CSV files
pond list '/sensors/*/readings.series'
```

⚠️ **Common Mistake**: `pond list /` returns "EmptyPath" error.  
Use `pond list '/*'` or `pond list` instead.

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

---

### pond cat

Read and optionally transform file contents.

```bash
# Read raw file contents
pond cat /data/readings.csv

# Query CSV files with SQL (use csv:// prefix)
pond cat csv:///data/readings.csv --query "SELECT * FROM source WHERE temp > 20"

# Query Parquet files (stored with --format=table)
pond cat /data/readings.parquet --query "SELECT AVG(temperature) as avg_temp FROM source"
```

The table is always named `source` in SQL queries.

#### URL Schemes for Querying

| Scheme | Purpose | Example |
|--------|---------|--------|
| `csv://` | Parse file as CSV | `pond cat csv:///data/file.csv --query "..."` |
| `file://` | Raw bytes or Parquet | `pond cat file:///data/file.parquet` |
| (none) | Auto-detect | `pond cat /data/file.csv` |

⚠️ **Important**: The `--query` flag only works when the file can be parsed as a table:
- Parquet files (stored with `--format=table` or `--format=series`)
- CSV files when using `csv://` prefix
- Raw data files without a scheme will output raw bytes, ignoring `--query`

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

### temporal-reduce

Time-bucketed aggregations at multiple resolutions.

```yaml
factory: "temporal-reduce"
config:
  in_pattern: "series:///sensors/*"
  time_column: "timestamp"
  resolutions: [1h, 6h, 1d]
  aggregations:
    - type: "avg"
      columns: ["temperature", "pressure"]
```

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

## Troubleshooting

### "EmptyPath" error

```
Error: Failed to list files matching '/' from data filesystem: EmptyPath
```

**Cause**: `/` alone is not a valid glob pattern.  
**Fix**: Use `pond list '/*'` or `pond list` (uses default `**/*`).

### "No matching files" when files exist

**Cause**: Pattern doesn't match actual paths.  
**Fix**: Use `pond list '**/*'` first to see what exists, then refine pattern.

---

## See Also

- [experiments/](../experiments/) - Runnable test scripts
- [duckpond-overview.md](duckpond-overview.md) - Architecture overview
