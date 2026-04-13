# UUID Discovery for Cross-Pond Import

## How Pond UUIDs Work

Every duckpond pond has a UUID identity, generated at creation time via
UUIDv7 (`uuid7::uuid7()` in `tinyfs/src/context.rs:260-279`). This UUID
is **random and time-ordered** — there is no way to specify it at
creation time. The only path to a stable UUID is restore/replica, which
preserves the source pond's ID.

## S3 Layout

When a pond backs up to S3, it writes under a prefix derived from its
UUID:

```
s3://bucket/pond-{uuid}/
  POND-META-{uuid}
  FILE-META-{uuid}-{date}-{txn_seq}
  POND-FILE-{content-hash}
```

The `list-ponds` command scans for `pond-*` prefixes in a bucket and
reports each pond's UUID and URL.

## How Cross-Pond Import Works

Import configs specify a `source_path` and `local_path` but **not** a
pond UUID. The UUID is discovered at import initialization time:

1. `mknod remote` connects to the remote S3 URL
2. `extract_pond_id()` scans for `FILE-META-{pond_id}-...` objects
3. The discovered UUID is cached in import metadata
4. Subsequent pulls use the cached partition/node IDs

So the import config itself is UUID-free, but the **S3 URL must contain
the pond UUID** as a path component (e.g., `s3://bucket/pond-{uuid}`).

## The Discovery Problem

Since pond UUIDs are random, any script that sets up cross-pond imports
must first discover the UUID of the source pond. Today this is done
dynamically in setup scripts:

### `local/setup.sh`

```bash
# Discovers water pond UUID from staging MinIO
WATER_UUID=$(pond run "host+remote:///..." list-ponds | grep pond-id | awk '{print $2}')
# Bakes it into import URL
echo "s3://water-staging/pond-$WATER_UUID"
```

### `terraform/station/watershop/site/setup.sh`

```bash
# Discovers sibling pond UUIDs via `pond config`
WATER_UUID=$(pond config /home/jmacd/.../water | grep pond-id | awk '{print $2}')
# Generates import config with baked UUID
```

### `duckpond/cross/setup.sh`

Same pattern: query source ponds, extract UUIDs, generate import configs
with baked-in `pond-{uuid}` URLs.

## Why This Blocks Consolidation

We want a single set of parameterized configs that work across dev,
staging, and production. But import URLs contain pond UUIDs that:

- Are different for every pond instance (staging vs production)
- Change if a pond is recreated (e.g., after a failed setup)
- Cannot be predicted or specified at creation time

This means import configs **cannot be fully static**. Some form of
UUID resolution must happen at setup/deploy time.

## Chosen Solution: Option C — One Bucket Per Pond

Each pond backs up to its own dedicated S3 bucket. The bucket name is the
stable, human-readable identity. No `pond-{uuid}` prefix is needed in URLs.

```
s3://noyo-production/       ← Noyo pond backup
s3://water-staging/         ← Water pond backup (staging)
s3://septic-production/     ← Septic pond backup
```

Import configs use bare bucket URLs:
```yaml
url: "s3://noyo-production"
import:
  source_path: "/**"
  local_path: "/sources/noyo"
```

### What Changed

- `build_table_url()` no longer auto-appends `pond-{uuid}` to bare bucket URLs
- `list-ponds` command removed (one pond per bucket makes it unnecessary)
- Setup scripts no longer need UUID discovery — bucket names are known infrastructure
- Internal backup format unchanged: `POND-META-{uuid}`, `FILE-META-{uuid}-...`,
  `POND-FILE-{hash}` still use UUIDs for provenance tracking
- `extract_pond_id()` still discovers the pond UUID from backup contents for
  import provenance

### Why This Works

- Bucket names are stable infrastructure, managed by Terraform/admin
- Import configs become fully static and checkable
- No UUID discovery step in setup scripts
- Recreating a pond (new UUID) doesn't break import configs — same bucket

## Superseded Options

### Option A: Keep Dynamic Discovery (Status Quo)

Setup scripts continue to discover UUIDs and generate import configs.
The parameterized configs cover everything except import URLs, which
are generated per-environment.

- **Pro**: Works today, no duckpond code changes
- **Con**: Setup scripts remain complex; import configs are generated
  artifacts rather than checked-in source of truth

### Option B: Env Var for Import URLs

Import URLs use `${env:WATER_POND_URL}` etc. Setup scripts discover
UUIDs once, write them to the systemd `EnvironmentFile`, and configs
expand them at runtime.

```yaml
import:
  - url: "${env:WATER_POND_URL}"
    source_path: /reduced
    local_path: /sources/water
```

- **Pro**: Configs are fully static and checked in; UUID discovery is
  a one-time provisioning step
- **Con**: Requires re-provisioning env file if a source pond is
  recreated; but this is an infrequent operation

### Option C: Stable Pond Names (Duckpond Code Change)

Add support for named ponds — either user-specified UUIDs at creation
time or name-based lookup (`s3://bucket/pond-name/noyo` instead of
`s3://bucket/pond-{uuid}`).

- **Pro**: Eliminates UUID discovery entirely
- **Con**: Requires duckpond core changes (table URL construction,
  `extract_pond_id`, `list-ponds`, import metadata); out of scope
  per current plan

### Option D: Convention-Based Prefix

Instead of `s3://bucket/pond-{uuid}`, use a known prefix like
`s3://bucket/noyo-production/`. Duckpond already constructs the
`pond-{uuid}` suffix automatically when given a bare bucket URL, but
if the URL already contains a path, it uses that path as-is.

This would require verifying that the backup/restore logic works with
non-UUID prefixes. If it does, ponds could back up to predictable
paths without code changes.

- **Pro**: No UUID discovery needed; no duckpond code changes if it
  already works
- **Con**: Needs verification; may conflict with `list-ponds` which
  expects `pond-*` prefix pattern

## Prior Recommendation (Superseded)

Option B was previously recommended but Option C (one bucket per pond) was
chosen instead as it eliminates UUID discovery entirely with minimal code changes.
