# Remote-pond preview: study, measurements, and the partial-clone design

**Status:** analysis / design record. The exploratory "remote-read pond"
implementation described in §2 was reverted after this study; this document
preserves what we learned and the design we recommend instead.

## 1. Motivation

We wanted to **preview a pond that lives on an S3 content-addressed remote**
(e.g. `s3://water-staging`) — run queries and sitegen over its history — *without*
paying for a full `pond pull` of the whole dataset (~11.4 GB on staging). The
concrete driver was iterating on **leak analysis** against real historical
`system_pressure` data over a multi-year window, where a full clone felt too
heavy for a preview loop.

Guiding principle (from the operator): *use the pond tool for this; if our
ability to locally preview a full remote dataset is weak, slow down and improve
the CLI and docs rather than bolting on a bespoke path.*

## 2. What we built (the spike) — and what it measured

We implemented a read-only **remote-read pond**: a second `PersistenceLayer`
that serves a pond *directly* over the remote, with no local rebuild. Key
properties:

- **Lazy open.** Fetch only the tip commit + node manifest (2 objects). Build the
  full static node tree from the manifest's `parent_node_id` links alone. Resolve
  each node's version blob hashes and bytes lazily on first access.
- **Dynamic directories.** `reduced` / `analysis` are `DirectoryDynamic`
  (factory-computed). We instantiated them through the `FactoryRegistry` so
  derived series (temporal-reduce output) resolve over the remote.
- **Rollup cache wiring.** Attached a local cache dir
  (`~/.watertown/cache/remote/<pond_uuid>/`) to the `ProviderContext`, which
  flips `temporal_reduce::try_rollup_table_provider` from `Ok(None)`
  (recompute-everything) to the incremental rollup path.
- **Cheap version sizing.** Added `ContentRemote::object_size()` (a `HEAD` on
  external blobs / inline-row length — no body download) plus an on-disk
  `version_sizes.json` sidecar, because `list_file_versions` otherwise downloaded
  *every* version's full blob just to read its byte length.

### 2.1 Measurements (staging, MinIO `watershop:9000`, throttled port)

Target: `/reduced/system-pressure/data/res=1d.series`,
`SELECT COUNT(*), MIN(timestamp), MAX(timestamp)` → 1397 daily rows,
2022-08-13 … 2026-07-18.

| Run | open | schema-infer (cache work) | query |
|---|---|---|---|
| **Cold** (empty cache) | 24 s | **1564 s** (~26 min: downloads all source, builds partials) | 4 ms |
| **Warm** (partials present, HEAD sizing) | 13 s | **116 s** | 3 ms |
| **Warm2** (+ size sidecar) | 13 s | **119 s** (unchanged) | 4 ms |
| No-cache control | — | > 11 min (recompute) | — |

### 2.2 What the numbers told us

- **Enabling the rollup cache works**: derived queries drop from "recompute all
  history every time" to **3 ms** once warm; the cache stores decomposable
  per-version partials that need no invalidation.
- **The cold warm-up is expensive** (~26 min) because temporal-reduce derived
  series are **O(all source history)** — the first materialization must read the
  whole source once.
- **The residual warm 116 s is not sizing** (the sidecar didn't help). It is
  **~108 sequential remote round-trips** in `resolve_source_files`: one per
  ingest *rotation* node, each fetching that node's series object to enumerate
  versions. (Rotations are name-timestamped archived source files,
  `casparwater-<ISO>.json`, produced by `logfile-ingest`; the glob
  `oteljson:///ingest/casparwater*.json` matches ~108 of them.)

## 3. Why the remote-read pond under-delivers

1. **The premise partly self-defeats.** The motivating use case is previewing
   *derived* series over the full dataset. But temporal-reduce scans all source,
   so a full-range derived preview still transfers a large, query-scoped slice of
   the source and pays a ~26-min first warm-up. "No bulk download" mostly
   evaporates exactly where we wanted it.
2. **Read-only.** No ingest / maintain / export / write. It is a preview lens,
   not a pond.
3. **A second, parallel storage engine to maintain.** ~700 lines re-deriving
   pond internals (lazy manifest walk, dynamic-node reconstruction, object-store
   size contract, uuid7↔uuid bridging). It can drift from the real tlogfs engine
   and duplicates invariants.
4. **Still needs more machinery to be pleasant.** The 116 s warm enumeration
   wants parallelization + persisted version lists; true "recent-window" previews
   want a time-bound mechanism (see §5) that does not exist yet.

The throttled port 9000 is the real cost driver for *any* bulk transfer — the
spike worked around it but did not remove it. Notably, a **full clone over the
same port moves *more* bytes** (everything, incl. reduced outputs and oplog).

## 4. Options compared

| Dimension | Remote-read pond (spike) | Full local clone (`pond pull`) | Partial "promisor" clone (§5) |
|---|---|---|---|
| First full-derived preview | ~26 min warm-up, then 3 ms | one-time full pull, then fast | metadata pull + windowed bytes, then fast |
| Bytes moved (targeted) | queried columns/versions | everything | skeleton + in-window bytes |
| Warm re-open | 116 s (108 round-trips) | zero (local pond) | zero (local pond) |
| Features | read-only query/sitegen | full (write/maintain/export) | full (write/maintain/export) |
| Correctness surface | 2nd parallel backend | production engine | production engine + residency axis |
| Freshness | live (reads tip) | point-in-time snapshot | point-in-time snapshot |
| Disk | query-scoped cache | full (~11.4 GB) | skeleton + window |

**Decision:** set the remote-read pond aside. It is not worth carrying as a
permanent feature for previewing derived data. Prefer a **real local pond**
(full clone now; partial/promisor clone as the targeted improvement), which
reuses the robust engine and existing CLI/docs.

## 5. The partial ("promisor") clone design

The insight that makes a partial clone *consistent* is separating two axes that
are easy to conflate:

- **Structural identity** — the tree/manifest: nodes, parents, names, and each
  version's `blake3`. Drives the content-addressed Merkle fold.
- **Content residency** — whether a version's actual bytes are stored locally.

These are independent in the existing code:

- `rebuild_pond` gates on the local tree folding to the **same root hash as the
  remote tip**. But `content_tree.rs` computes each node's `child_hash` from the
  recorded **`blake3`**, *not* by re-reading bytes
  (`build_content_tree_for_table`: "Hash/index paths never need blob bytes";
  `row_blob_hash(&row.blake3, row.content…)` prefers `blake3`).
- tlogfs `OplogEntry` already has `content: Option<Vec<u8>>` (nullable — it is how
  large files store bytes externally), alongside separate `blake3`, `size`,
  `min_event_time`, `max_event_time` columns (`tlogfs/src/schema.rs`).

**Therefore a version row with `content: None` but a valid `blake3` folds to the
identical root — the integrity gate passes without the bytes.** This is exactly
git's partial-clone / promisor model, and the tlogfs schema already accommodates
it (a `content: None` row is the same shape as the existing large-file
external-content case).

### 5.1 Structure

A partial clone is a **real, writable local tlogfs pond** containing:

- a **complete metadata skeleton**: every commit, the full node manifest, every
  directory and node, and for every series **every version row** (`blake3`,
  `size`, `min/max_event_time`);
- **blob bytes only for versions inside the requested window** (e.g. rotations
  whose `max_event_time ≥ T`).

Out-of-window versions are rows with `content: None` — "promises." The pond folds
to the **same root hash as the remote**, so it is provably the same pond, with a
subset of bytes resident.

### 5.2 How tinyfs stays consistent

1. **No dangling references** — the tree and every version row are present, so
   path resolution, directory listing, series enumeration, and the Merkle fold
   are complete and correct.
2. **Residency is an orthogonal axis** — identical to tlogfs's existing
   inline-vs-external content split. "Not resident" ≠ "not in the tree."
3. **Read of an evicted version** → policy: (a) **lazy-fetch** the blob from the
   promisor remote (reuse `ContentRemote::get_blob_reader`), or (b) **fail
   cleanly** ("content evicted; widen the window"). Because queries prune on
   `min/max_event_time`, a windowed preview never touches evicted versions, so
   (b) is acceptable for previews and (a) makes it seamless.
4. **Extending the window** = fetch more blobs into existing rows. No tree
   mutation, no re-fold — identity never changed.

### 5.3 Why the tempting shortcuts do NOT work

- **Shallow commit history** (git `--depth`): useless here. All rotations are
  *live children of the current tip tree*, not buried in old commits. Truncating
  history removes zero data.
- **Structurally deleting old nodes**: produces a *different* tree with a
  *different* root — loses the "folds to remote root" guarantee and turns
  window-extension into a tree mutation. Weaker, more fragile invariant.

Only the promisor model keeps `local root hash == remote root hash`.

### 5.4 Genuine costs / open reconciliation

To write a correct skeleton row for an *evicted* version without its bytes:

1. **`blake3`** — the remote series object yields per-version content hashes for
   free (`decode_series`). ⚠️ **Open question:** tlogfs `FilePhysicalSeries.blake3`
   is documented as the *cumulative bao-tree* hash (`schema.rs`), while the remote
   stores per-version blob hashes. Confirm these line up (or store the cumulative
   form / `bao_outboard`). This is the one real unknown before implementing.
2. **`size`** — cheap `HEAD` via `ContentRemote::object_size()` (salvage from the
   spike).
3. **`min/max_event_time`** — *not* present in remote metadata; requires a parquet
   **footer / column-stats range read** per version (small, not the full file).
   This is the "metadata pull": N cheap footer reads vs. N full-blob downloads.
4. **`bao_outboard`** — needed for verified streaming; fetch lazily with the blob
   or store it in the skeleton.

Net shape: *N cheap metadata reads (skeleton) + windowed full-blob fetches*,
versus a full clone's *N full-blob fetches*.

## 6. Salvage from the spike

- **`ContentRemote::object_size()`** — cheap `HEAD`/inline-length sizing; broadly
  useful and a direct building block for §5.2/§5.4. (Reverted with the spike; to
  be re-landed on its own when the partial-clone work begins.)
- The **lazy manifest walk** could become a low-cost `pond ls --remote` for
  structure inspection without any clone.
- The **"skip rotations outside a window via name timestamp"** idea maps cleanly
  onto §5's `--since` selection of which blobs to fetch.

## 7. Recommended local clone-to-preview workflow

Until a partial clone lands, the supported preview path is a **full local clone**
of the pond, then normal tooling:

1. Create/open a local pond and pull from the remote (`pond pull` /
   `fetch_object_graph` + `rebuild_pond`). Expect a one-time full transfer; kick
   it off and let it run. Over the throttled staging port this is slow — prefer a
   faster transport if available.
2. Query / sitegen / leak-analysis against the local pond exactly as in
   production; the rollup cache lives at `{POND_ROOT}/cache/` and warms once.

**Future target — bounded preview clone:** `pond pull --since <T>` implementing
§5 (skeleton + windowed residency), giving cheap "last N days / years" previews
without a bespoke read path. Time-bound belongs on the **pull**, not on a
parallel read engine.

> Action item: verify and document the exact working full-clone CLI invocation
> (command, storage options, cache location) in the operator guide, and record
> the measured full-pull cost against staging.

## 8. Status / next steps

- [x] Studied and measured the remote-read pond (this document).
- [x] Decided to set it aside; revert the spike commit.
- [ ] Re-land `ContentRemote::object_size()` standalone when partial-clone work
      begins.
- [ ] Resolve the blake3 cumulative-vs-per-version reconciliation (§5.4.1).
- [ ] Prototype `pond pull --since <T>` (skeleton write + windowed fetch +
      read-miss policy).
- [ ] Document the verified full-clone preview workflow (§7 action item).
