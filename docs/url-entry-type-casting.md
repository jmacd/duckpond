# URL Entry-Type Casting (read/access-time archetype reinterpretation)

Status: Design / implementation plan (not yet implemented)
Author: design notes for jmacd
Related: `hostmount-design.md`, `format-cache-design.md`, `remote-redesign.md`

## Motivation

The caspar.water "noyo-harbor" sub-site lost its historical HydroVu data after the
post-D6 staging reset. The history exists as committed Parquet `.series` archive files in
the `noyo-blue-econ` git repo and is exposed into the pond via the `git-ingest` factory.
However, those archive files are **invisible to the `timeseries-join` (combine) factory**,
so the dashboards only show data that live API collection has slowly back-filled from epoch.

Root cause: `git-ingest` exposes every blob as `EntryType::FileDynamic` (a *data/dynamic*
file). The combine join resolves its input patterns to *series* nodes and filters out
everything else by the node's **stored** entry type. The archive Parquet files are real
series data, but because they are stored as the *data* archetype they are excluded.

The general fix — and the feature this document specifies — is to make the **URL
entry-type suffix a read-time cast** that reinterprets a node's archetype, rather than a
filter on the node's stored archetype. This already works for host files
(`host+series:///x.parquet`); we are extending the same, already-intended semantics to
pond paths uniformly.

## Background: the entry-type system

A non-directory, non-symlink entry has one of **six** types, formed by an
**archetype × embodiment** product:

| archetype | physical embodiment      | dynamic embodiment |
| --------- | ------------------------ | ------------------ |
| data      | `FilePhysicalVersion`    | `FileDynamic`      |
| table     | `TablePhysicalVersion`   | `TableDynamic` (table-shaped dynamic) |
| series    | `FilePhysicalSeries` / `TablePhysicalSeries` | `TableDynamic` (series-shaped dynamic) |

Helpers (`crates/tinyfs/src/entry_type.rs`):
- `base_format()` (`:66-78`) collapses embodiment, returning `file:data` / `file:table` /
  `file:series` — this is the **archetype**.
- `is_dynamic()` (`:16`), `is_physical()` (`:25`) — the **embodiment**.
- `is_data_file()` (`:31`), `is_table_file()` (`:40`), `is_series_file()` (`:46`),
  `is_parquet_file()` (`:55`).

Archetype semantics:
- **data** — opaque byte content. One logical value per access; versions are independent
  byte blobs.
- **table** — a single Parquet table; the meaningful value is the **latest version**.
- **series** — a multi-version Parquet time series; the meaningful value is **all
  versions concatenated** (oldest first), with temporal metadata for time-travel.

## The casting model (read/access-time only)

There are **no write-path URLs**. A URL always *reads/accesses* an existing path or
pattern. The entry-type suffix in the URL declares the archetype the caller wants to
**interpret the bytes as**. Embodiment (physical/dynamic) is irrelevant to the cast — it
only affects how bytes are produced, not how they are interpreted.

All Parquet-backed archetypes share the same byte format, so casts are defined by how
bytes are read or produced:

| from \\ to | data | table | series |
| ---------- | ---- | ----- | ------ |
| **data**   | identity (bytes) | read the bytes as one Parquet table | read the bytes as a one-version Parquet series |
| **table**  | produce Parquet bytes of the latest version | identity | reinterpret latest version as a one-version series |
| **series** | produce Parquet bytes of **all** versions concatenated | reinterpret as latest version only | identity |

Notes:
- A *data* file cast to *table*/*series* must contain valid Parquet bytes; otherwise the
  read errors. (Symmetric to `host+series:///x.parquet` asserting a host file is Parquet.)
- *table → data* yields the **last** version's Parquet bytes; *series → data* yields **all**
  versions' Parquet bytes (this is already how physical series concatenate on byte-read —
  `FilePhysicalSeries` uses a `ChainedReader`, see `entry_type.rs:26-27`,
  `crates/tlogfs/src/file.rs:220`).
- The cast is independent of physical/dynamic: a git-ingested `FileDynamic` Parquet blob
  cast to *series* reads exactly like a physical *data* file cast to *series*.

### URL syntax (already supported by the `Url` type)

Canonical form (`crates/provider/src/lib.rs:122`):
`[host+]scheme[+compression][+entrytype]:///path[?query]`

- `series:///path` parses as `scheme="file"`, `entry_type()=Some("series")`
  (`lib.rs:217`, `url_pattern_matcher.rs:82-83`).
- `file+series:///path`, `csv+series:///path`, `host+series:///path` likewise carry
  `entry_type()=Some("series")`.
- The **target archetype** is `url.entry_type()`, falling back to the scheme name for the
  bare builtins (`series`/`table`/`data`/`file`).

## Current behavior and the gaps

1. **Resolution filters by stored archetype (the central gap).**
   - `url_pattern_matcher.rs::match_builtin_type` (`:96-158`): builds an `entry_types`
     set per requested key (`series => [TablePhysicalSeries, TableDynamic]`, `data =>
     [FilePhysicalVersion, FileDynamic]`, etc.) and keeps a match only if the node's
     **stored** `file_id.entry_type()` is in that set (`:150`).
   - `sql_derived.rs::resolve_pattern_to_queryable_files` (`:417-592`) + the caller at
     `:840-852`: Series mode searches `[TablePhysicalSeries, TableDynamic]` and matches on
     `actual_entry_type` (`:557`).
   - Effect: a `series://` pattern over git-ingested `FileDynamic` Parquet matches **zero**
     files.

2. **`as_queryable()` gate rejects data nodes before any cast.**
   - `provider_api.rs::create_builtin_table_provider` (`:233-276`) resolves the node then
     calls `file_guard.as_queryable()` (`:261`), which is `None` for a *data* file
     (default impl `crates/tinyfs/src/file.rs:55`; only `OpLogFile` series/table implement
     it, `crates/tlogfs/src/file.rs:212`). Result: `"File ... is not queryable"`.

3. **`create_table_provider` keys the read strategy off the stored type, not a requested
   archetype.**
   - `crates/provider/src/table_creation.rs:53-189` already builds a Parquet `ListingTable`
     and selects versions via `TableProviderOptions.version_selection`
     (`AllVersions` ⇒ series, `LatestVersion` ⇒ table; `:200-217`). The conversion
     machinery exists — but there is **no parameter carrying the requested target
     archetype**; callers pick options based on the stored type.

4. **The host path already implements the cast (precedent / proof of intent).**
   - `provider_api.rs:131-136` short-circuits `host+table`/`host+series` (scheme `file`)
     to `create_host_parquet_table_provider` (`:183`), reading raw host bytes directly as
     Parquet because "the URL asserts they are queryable Parquet." Pond paths should do the
     same for *data* nodes.

## Design specification

Make the requested archetype (`url.entry_type()` or builtin scheme) drive interpretation
end-to-end, applying the casting matrix when it differs from the node's stored archetype.

### 1. Resolution: match by path, interpret by archetype

`match_builtin_type` and `resolve_pattern_to_queryable_files` must treat the requested
archetype as the **interpretation**, not an exclusion predicate:
- Expand the path/glob to candidate nodes (skip directories and symlinks).
- Do **not** drop a node because its stored archetype differs from the requested one. Any
  of the three file archetypes is castable to any other (per the matrix), so all file
  entry types are candidates.
- Carry the requested target archetype forward on each match (e.g. on `MatchedFile` /
  the reconstructed file URL) so the provider layer can apply the cast.
- The bare `file:///` form (no `entry_type()`) keeps "wildcard / interpret as stored type"
  behaviour.

Open refinement: decide whether a glob like `series:///dir/*` should match *non-Parquet*
data files and then error at read, or pre-skip by extension. Recommended: match by path,
error clearly at read if bytes are not Parquet (consistent with the host-cast behaviour
and with the "assert via URL" model). Globs in practice target specific suffixes
(`*.series`).

### 2. Thread the target archetype into `create_table_provider`

Add a target-archetype field to `TableProviderOptions`
(`crates/provider/src/table_provider_options.rs`), e.g.
`interpret_as: Archetype` (Data/Table/Series), defaulting to "derive from stored type"
for backward compatibility.

`create_table_provider` (`table_creation.rs`) maps the **requested** archetype to the
read strategy:
- requested **series** ⇒ `VersionSelection::AllVersions` + Parquet `ListingTable`.
- requested **table** ⇒ `VersionSelection::LatestVersion` + Parquet `ListingTable`.
- requested **data** (when reading as bytes, not a table) ⇒ byte path, not a TableProvider
  (see §4).

Because the underlying `ListingTable` reads Parquet via the tinyfs ObjectStore regardless
of stored type, a *data/dynamic* (git) node requested as *series* will read correctly once
it reaches this function. Verify version enumeration (`VersionSelection::to_url_pattern`)
works for dynamic git nodes (single logical version); if not, route single-version data
nodes through a direct single-file `ListingTable` (mirroring
`create_host_parquet_table_provider`).

### 3. Bypass the `as_queryable()` gate for cast reads

In `create_builtin_table_provider`, when `url.entry_type()` requests table/series and the
resolved node is a *data* archetype (or otherwise returns `None` from `as_queryable()`),
do **not** error. Instead build the provider from the node's bytes as Parquet — a pond
analogue of `create_host_parquet_table_provider`, reading via the node's
`async_reader()`. For nodes that *are* natively queryable, keep delegating to
`as_table_provider` but pass the requested archetype through.

### 4. The reverse cast (series/table → data) on byte reads

Reading a series/table node through a `data://` (or byte) access must yield Parquet bytes:
- series ⇒ all versions concatenated (already the `FilePhysicalSeries` `ChainedReader`
  behaviour).
- table ⇒ last version's bytes.
This is the byte-read path (`async_reader`/`read_file_version`), not the TableProvider
path. Confirm the access layer chooses versions per requested archetype. (Lower priority
for the noyo use-case, which only needs data→series, but specified here for completeness.)

### 5. Propagate the cast through `timeseries-join` / `sql-derived`

The join builds input table providers by reconstructing a per-file URL and calling
`provider_api.create_table_provider` (`sql_derived.rs:~964, ~1078`) or
`queryable_file.as_table_provider` (`:~1151`). Ensure the reconstructed URL preserves the
`+series`/`+table` suffix (the requested archetype) so the provider layer applies the cast.
This is the path the noyo combine config exercises.

## Affected read surfaces

- `timeseries-join` (combine) and `sql-derived(-series/-table)` factories — primary.
- Direct builtin reads via `provider_api::create_table_provider` (e.g. `pond cat`, sitegen
  queries) — benefit automatically once §2/§3 land.
- Caching: `TableProviderKey` (`table_creation.rs:68`) **must include the requested
  archetype**, so `series://X` and `table://X` over the same node cache distinctly.

## Edge cases

- **Compression suffix**: `series+gzip://` etc. must compose with the cast (decompress,
  then interpret as Parquet).
- **Schema inference / 0-byte versions**: existing `infer_schema` already skips 0-byte
  override versions (`table_creation.rs:117-124`); unchanged.
- **Non-Parquet data cast to table/series**: clear error, naming the path and requested
  archetype.
- **Temporal bounds**: `get_temporal_bounds` (`table_creation.rs:144`) returns unbounded
  for nodes lacking metadata (git nodes) — acceptable; data-quality filtering simply does
  not apply.
- **Format-provider schemes** (`csv+series://`, `excelhtml://`): unchanged — those route
  through their format provider, which already maps to the *data* lookup; casting concerns
  the **builtin/Parquet** path only.

## Testing plan

- Unit: `Url` parsing of `series://`, `file+series://`, `table://`, `data://` → correct
  `entry_type()`/scheme (extend existing `lib.rs` tests).
- Resolution: a glob over a directory containing `FileDynamic` Parquet files matches under
  `series://` (regression for the current exclusion).
- Provider: `create_table_provider` over a *data/dynamic* node requested as *series*
  returns a queryable provider with the expected rows; over a *table* request returns the
  latest version only.
- Join: a `timeseries-join` with one `series:///…/*.series` input pointing at git-ingested
  Parquet yields the archive rows; combined with a live series input, the union spans the
  full range.
- Cache: `series://X` and `table://X` over the same node produce distinct providers.

## First consumer: caspar.water noyo archives

Once the cast lands in duckpond:
1. `config/noyo.yaml`: add a `git-ingest` `mknod` exposing the `noyo-blue-econ` HydroVu
   archives at a read-only path (e.g. `/system/archive/hydrovu`, `prefix: hydrovu`).
2. Add `timeseries-join` inputs casting those Parquet files as series, e.g.
   `series:///system/archive/hydrovu/devices/**/NoyoCenterVulink_2*.series` with the
   matching `scope`, alongside the existing live `/hydrovu/devices/**/…` inputs.
3. Rebuild the duckpond image, deploy staging, `pond apply`, rebuild the site; verify the
   noyo DO/Temperature/Salinity pages show history back to the archive range (Feb 2026).

## Out of scope (tracked separately)

- **HydroVu resume seeding.** `find_youngest_timestamp`
  (`crates/hydrovu/src/lib.rs:430-526`) reads temporal metadata from
  `list_file_versions` (the oplog), which git dynamic nodes lack. Making live API
  collection resume from the archive max (instead of epoch) requires either footer-derived
  bounds in `find_youngest` or seeding `/hydrovu/devices` directly. Display (this doc) is
  independent: the combine union surfaces archive history immediately while live
  collection back-fills the recent tail.
