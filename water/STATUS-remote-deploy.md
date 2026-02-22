# Water Remote Deployment -- Status

**Date:** 2026-02-22  
**Remote host:** `jmacd@linux.local` (Linux amd64, podman)  
**Podman volume:** `pond-water`  
**Container image:** `duckpond:latest-amd64` (locally built, sha256:3388fbd4ec0f)

---

## What We're Doing

Deploying the "Caspar Water" site generator to a remote Linux host.  
The pipeline reads ~9.2 GB of OTelJSON log files (96 files spanning Oct 2022 -- Feb 2026),
reduces them into time-bucketed aggregations (5 metrics x 5 resolutions), and
generates a static HTML site with interactive charts.

## Pipeline Steps

| Step | Script | Status |
|------|--------|--------|
| 1. Build amd64 image | `build-remote.sh` | DONE -- image on remote |
| 2. Initialize pond + ingest data | `setup-remote.sh` + `run-remote.sh` | DONE -- 96 files ingested |
| 3. Push configs + create factories | `update-remote.sh` | DONE -- configs pushed, factories created |
| 4. Generate site | `generate-remote.sh` | DONE but **output only contains year=2025** |

## The Bug: Only Recent Data Appears

### Root Cause (identified and fixed in code, not yet verified end-to-end)

The `temporal-reduce` factory had a **HashMap overwrite bug**: when multiple
ingested files (e.g., `casparwater-2022-*.json`, `casparwater-2023-*.json`, ...)
all map to the same output name (`data`), `HashMap::insert` kept only the LAST
file. Only the most recent log file's data appeared in the output.

### Fix Applied (uncommitted)

In `crates/provider/src/factory/temporal_reduce.rs`:

- Changed grouping from `HashMap::insert` (overwrites) to
  `HashMap::entry().or_default().push()` (collects all sources per output)
- When multiple sources map to the same output, the factory now passes the
  original **glob URL** (`oteljson:///ingest/casparwater*.json`) to the
  underlying `SqlDerivedFile`, which already supports multi-file UNION ALL
- When only one source maps to an output, the concrete source URL is used
  (preserving existing single-file behavior)

### Current Status

The fix is:
- **In the local source tree** -- uncommitted (83 insertions, 24 deletions)
- **In the locally-built container image** (`duckpond:latest-amd64`, sha256:3388fbd4ec0f)
- **Pushed to the remote host** via `build-remote.sh` (podman load)
- **Tests pass** -- all 141 tests including 6 temporal-reduce-specific tests

BUT: the **last `generate-remote.sh` run still produced only year=2025 data**.
This needs investigation -- the fix may not have been active during that run
(the factories were recreated with `mknod --overwrite` using the *old* image
before the new image was built).

## What Needs to Happen Next

1. **Re-run `update-remote.sh`** to recreate the factory nodes using the NEW
   image (the one with the temporal_reduce fix). The `mknod --overwrite`
   commands rebuild the factory metadata.

2. **Re-run `generate-remote.sh`** to produce the site with the fixed
   temporal-reduce factory reading ALL 96 log files.

3. **Verify** the exported parquet files contain data from 2022 through 2026.

## Remote Pond Inventory

### Ingested Data (`/ingest/**`)
- **96 files** total
- First: `casparwater-2022-10-05T08-53-53.507.json` (~100 MB)
- Last archived: `casparwater-2026-02-19T08-28-37.726.json` (~100 MB)
- Active: `casparwater.json` (~26.7 MB)
- Date range: **Oct 2022 -- Feb 2026** (~3.4 years)

### Factory Nodes
- `/etc/ingest` -- logfile-ingest factory (reads raw logs from `/data/`)
- `/reduced` -- dynamic-dir with temporal-reduce config (reads from `/ingest/casparwater*.json`)
- `/etc/site.yaml` -- sitegen factory (reads from `/reduced` and `/site`, `/content`)

### Config on Remote (`/home/jmacd/water-config/`)
- `ingest.yaml` -- logfile-ingest config
- `reduce-remote.yaml` -- temporal-reduce config (**has glob pattern**: `casparwater*.json`)
- `site-remote.yaml` -- sitegen config
- `site/` -- page templates (index.md, data.md, sidebar.md)
- `content/` -- content pages (*.md)

### Images on Remote
| Image | Tag | ID | Age |
|-------|-----|----|-----|
| `localhost/duckpond` | `latest-amd64` | `3388fbd4ec0f` | 2 hours (has fix) |
| `ghcr.io/jmacd/duckpond/duckpond` | `pr-52-amd64` | `78cdd94d291d` | 11 hours (no fix) |

### Export (local, from last generate)
- HTML pages: 11 files (index, blog, history, monitoring, etc.)
- Data: 5 metrics x 5 resolutions, **all year=2025 only** (bug)
- Location: `water/export/`

## Uncommitted Changes

```
 M crates/provider/src/factory/temporal_reduce.rs   -- multi-source glob fix
 M docs/cli-reference.md                            -- schema caveat docs
 M water/pond-remote.sh                             -- IMAGE env var
 M water/reduce-remote.yaml                         -- glob patterns
 M water/setup-remote.sh                            -- minor fix
 M water/update-remote.sh                           -- mknod --overwrite
?? water/BUGREPORT-copy-overwrite.md                -- copy lacks --overwrite
```

## Scripts Reference

| Script | Purpose | When to Run |
|--------|---------|-------------|
| `build-remote.sh` | Cross-compile, build Docker image, push to remote | After code changes |
| `setup-remote.sh` | `pond init` + ingest all log files | Once (or to reingest) |
| `run-remote.sh` | Run any factory command | As needed |
| `update-remote.sh` | Push configs + `mknod --overwrite` factories | After config changes |
| `generate-remote.sh` | Run sitegen, rsync output locally | To rebuild the site |
| `pond-remote.sh` | Low-level: run any pond command on remote | Building block for other scripts |
