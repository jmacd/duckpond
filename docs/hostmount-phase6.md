# Hostmount Phase 6 -- Site Config Restructuring

**Status:** COMPLETE

## What Phase 6 Delivers

Phase 6 applies the hostmount design to the Caspar Water site. The `water/` directory
is restructured so that `render.sh` uses a single `pond run -d` command instead of
creating a throwaway pond, copying files in, and tearing it down.

The motivating transformation:

**Before (15+ commands):**
```bash
export POND="./water/.pond"
pond init
pond mkdir /content
for f in ./water/content/*.md; do
    pond copy "host://${f}" "/content/$(basename "$f")"
done
pond mkdir /etc && pond mkdir /etc/site
for f in ./water/site/*.md; do
    pond copy "host://${f}" "/etc/site/$(basename "$f")"
done
pond mknod sitegen /etc/site.yaml --config-path ./water/site.yaml
pond run /etc/site.yaml build ./dist
```

**After (1 command):**
```bash
pond run -d ./water host+sitegen:///site.yaml build ./dist
```

No pond. No copying. No mknod. The host `water/` directory IS the filesystem.

## What Changed

### `water/site.yaml` -- Path Restructuring

Template and partial paths changed from `/etc/site/` to `/site/`, matching the
host directory layout directly:

| Config field | Before | After |
|---|---|---|
| `routes[0].page` | `/etc/site/index.md` | `/site/index.md` |
| `partials.sidebar` | `/etc/site/sidebar.md` | `/site/sidebar.md` |

The `/etc/` prefix was a pond convention (config files live under `/etc/`). With
hostmount, the paths map directly to the host directory tree -- `water/site/index.md`
becomes `/site/index.md` when the hostmount root is `water/`.

Content paths (`/content/*.md`) were already correct -- no change needed.

### `water/render.sh` -- Complete Rewrite

Reduced from 48 lines to 41 lines. The entire pond lifecycle (init, mkdir, copy,
mknod) is gone. The core is now:

```bash
pond run -d "${SCRIPT_DIR}" host+sitegen:///site.yaml build "${OUTDIR}"
```

This uses the Phase 4+5 hostmount infrastructure:
- `-d "${SCRIPT_DIR}"` sets the hostmount root to `water/`
- `host+sitegen:///site.yaml` tells the CLI to use the host filesystem with the
  sitegen factory, reading config from `/site.yaml` (relative to hostmount root)
- `build "${OUTDIR}"` is the factory subcommand with output directory

The `POND` environment variable is no longer set or needed. No `.pond/` directory
is created.

### `water/.gitignore` -- Removed `.pond/` Entry

The `.pond/` directory is no longer created, so the gitignore entry was removed.
Only `dist/` remains ignored.

### `crates/sitegen/src/factory.rs` -- Static Asset Path Handling

The `copy_static_assets()` function had a hardcoded `strip_prefix("/etc/static/")`
to extract relative output paths from static asset patterns. This was replaced with
`Path::file_name()` which extracts just the filename -- works regardless of where
static assets live in the filesystem tree.

Before:
```rust
let rel = asset.pattern.strip_prefix("/etc/static/").unwrap_or(&asset.pattern);
let out_path = output_dir.join(rel);
```

After:
```rust
let filename = Path::new(&asset.pattern)
    .file_name()
    .and_then(|n| n.to_str())
    .unwrap_or(&asset.pattern);
let out_path = output_dir.join(filename);
```

### `crates/sitegen/src/config.rs` -- Doc Comment Update

Updated the example paths in the doc comment from `/etc/site/sidebar.md` and
`/etc/static/*` to `/site/sidebar.md` and `/static/*`.

## Why No Code Changes Were Needed

The sitegen factory has no pond-specific assumptions. All filesystem access goes
through the `ProviderContext` -> `PersistenceLayer` -> `FS` -> `WD` abstraction:

- `root.collect_matches(pattern)` for content globbing
- `root.read_file_path_to_vec(path)` for template/partial/page reading

These methods work identically whether the persistence layer is `OpLogPersistence`
(tlogfs/pond) or `HostmountPersistence` (host filesystem). The config paths are
just strings resolved against the WD root -- changing them from `/etc/site/index.md`
to `/site/index.md` is a pure config change.

## Host Directory Layout

The `water/` directory maps 1:1 to the hostmount filesystem:

```
water/                    Hostmount root (via -d ./water)
+-- site.yaml             /site.yaml       (factory config)
+-- content/              /content/        (content pages)
|   +-- water.md          /content/water.md
|   +-- system.md         /content/system.md
|   +-- history.md        /content/history.md
|   +-- monitoring.md     /content/monitoring.md
|   +-- operator.md       /content/operator.md
|   +-- political.md      /content/political.md
|   +-- software.md       /content/software.md
|   +-- blog.md           /content/blog.md
|   +-- thanks.md         /content/thanks.md
+-- site/                 /site/           (templates + partials)
|   +-- index.md          /site/index.md
|   +-- sidebar.md        /site/sidebar.md
+-- dist/                 (output, not part of hostmount)
+-- render.sh             (build script, not part of hostmount)
```

## Test Results

All tests pass with the changes:

| Test | Checks | Description |
|------|--------|-------------|
| 48 sitegen unit tests | 48/48 | Config parsing, markdown, routes, shortcodes |
| 201 sitegen-markdown-maudit | 43/43 | Full sitegen pipeline with data exports |
| 204 sitegen-html-passthrough | 13/13 | HTML passthrough in markdown |
| 205 sitegen-base-url | 25/25 | Base URL handling |
| 207 sitegen-content-pages | 13/13 | Pond-based content pages (unchanged) |
| 300 hostmount-list | 5/5 | Hostmount list proof of life |
| 301 hostmount-cat | 12/12 | Hostmount cat variations |
| 302 hostmount-copy-out | 14/14 | Hostmount copy out |
| 303 hostmount-sitegen | 13/13 | Hostmount sitegen (standalone test) |

Local `render.sh` generates 10 HTML pages (9 content + 1 home) with working sidebar
navigation, active page highlighting, and heading anchors.

## What's Next

- **Phase 7**: Deprecate remaining ad-hoc host access -- remove `strip_prefix("host://")`
  from `pond copy`, route all host access through hostmount tinyfs
