# Cross-Pond Staging Pipeline — April 8, 2026

## Problem

The Noyo Harbor site (served from staging at `watershop.local:4180`) was
not showing HydroVu sensor data beyond mid-October 2025.  The static
laketech HTML data ends at the `2025-10-16T21:00:00Z` boundary where
instruments moved from BDock/Princess to the Field Station.  The HydroVu
API collect was expected to pick up from the archive watermark and fetch
recent data, but nothing new appeared.

## Root Causes

### 1. Missing hydrovu archive copy in staging setup

Production (`noyo-blue-econ/scripts/setup.sh`) copies three data sets
into the pond:

```
copy host:///root/laketech  /laketech/data
copy host:///root/hydrovu   /hydrovu        ← missing in staging
copy host:///root/site      /system/site
```

Staging's `noyo/setup.sh` was missing the hydrovu copy.  Without the
archive `.series` files, `find_youngest_timestamp()` returned 0, causing
the collector to start from epoch.  With `max_points_per_run: 10000`,
each run crawled through years-old data that fell below the Field
Station's `begin: 2025-10-16` cutoff — so nothing appeared on the site.

### 2. Stale pond UUID in site import config

The staging architecture uses three separate ponds:

```
water/pond  →  backup to MinIO  →  site/pond imports via S3
noyo/pond   →  backup to MinIO  →  site/pond imports via S3
```

`site/setup.sh` discovers the water and noyo pond UUIDs and bakes them
into the import config URLs (e.g., `s3://noyo-staging/pond-<UUID>`).
When `noyo/setup.sh` is re-run (which does `rm -rf pond` + `init`), a
new pond UUID is created.  The site pond's import config still points at
the old UUID, so `import.sh` pulls stale data — silently succeeding with
zero new files.

### 3. Deleted `dist/` directory

The `generate.sh` script does `rm -rf dist && mkdir dist`, which
replaces the directory inode.  If `python3 -m http.server` was started
in the old `dist/`, it continues serving the deleted directory tree.
The server must be restarted after each site generation.

## Fixes Applied

### `terraform/station/staging/noyo/setup.sh`

- Added `copy host://.../hydrovu /hydrovu` to load archive data
- Reordered to copy data **before** `mknod hydrovu` (matching production
  order), since `mknod` creates the `/hydrovu` directory structure and
  `copy` fails on `AlreadyExists`

### `terraform/station/staging/run-all.sh`

- Added pre-flight UUID verification: before running the pipeline,
  `run-all.sh` now reads the current pond UUIDs from `water/pond` and
  `noyo/pond`, compares them against the UUIDs baked into the site
  import configs, and exits with a clear error message if they don't
  match

## Verification

After applying fixes, `setup-all.sh` followed by `run-all.sh` completed
successfully:

- Noyo collect fetched ~4,200 records from the HydroVu API, resuming
  from the Feb 9, 2026 archive watermark through April 9, 2026
- Vulink devices caught up to current day; AT500 devices reached
  March 9 (limited by `max_points_per_run`, will catch up on subsequent
  runs)
- Site generation produced data through April 2026, visible at
  `watershop.local:4180/noyo-harbor/`

## Lesson

When the staging architecture has multiple ponds linked by UUID-bearing
import configs, any `setup.sh` that recreates a source pond invalidates
downstream import references.  The UUID check in `run-all.sh` makes this
failure mode visible immediately rather than silently serving stale data.
