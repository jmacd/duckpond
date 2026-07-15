# Bounded-Memory Temporal Reduce: Sealed Runs + Hot Window

> **Status:** Proposed. Motivated by the watershop **selfmon** deployment, which
> was run deliberately as a long-lived stressor and exposed unbounded memory
> growth in the `temporal-reduce` read path. This note proposes reorganizing the
> reduced-series materialization so that **no operator ever holds state
> proportional to total history**, while **all history is retained and remains
> fully queryable** (this bounds *memory*, not *how far back you can query*). The
> raw object model, the series delta layout, and the factory configuration
> surface are carried forward unchanged; only *how the reduced output is merged,
> cached, and stored* changes.

---

## 0. Problem statement

Selfmon renders a dashboard every ~1 min by reducing an append-only perf/journal
series into `res=60/600/3600/…` rollups (`temporal-reduce`) and building a site
from them. Over a single uninterrupted run ("epoch") its peak memory grows
**without bound** and is reset only by wiping the instance.

Observed on watershop (`pond-selfmon@watershop-selfmon`, daily-max of pond's own
"Peak memory usage", one epoch):

| Date | Daily max (tracked) |
|---|---|
| 2026-07-09 | 2220 MB |
| 2026-07-10 | 2287 MB |
| 2026-07-11 | 2329 MB |
| 2026-07-12 | 2344 MB |
| 2026-07-13 | 2362 MB |
| 2026-07-14 | 2372 MB |
| 2026-07-15 | 2383 MB |

Monotonic, ~+25 MB/day, no plateau; live process **RSS ≈ 3.9 GB** (RSS includes
mmap'd parquet and hash-table state the internal counter does not track).
Previous epochs show the same sawtooth: a steady climb (May 7 ~120 MB → Jun 10
~1.1 GB) reset only by reinit/`reset_instances`.

Storage is *not* the problem — `maintain --prune/--keep-txns` already bounds the
control oplog (control dir = 25 MB, healthy). The problem is **memory**, and the
sharper framing that drives this design:

> Processing more data should cost more **time**, not more **memory**. A
> streaming scan is O(1) memory regardless of length. If memory scales with
> total history, some operator is holding O(N) state.

## 1. Root cause

Two blocking (pipeline-breaking) operators run over **all of history on every
build**, in `TemporalReduce::…` merged-cache construction:

### 1a. `GROUP BY time_bucket` over the entire partials set — the primary O(N)

`partials_table` is a listing table over the **whole** partials directory (all
cached versions = full history):

```
crates/provider/src/factory/temporal_reduce.rs:531-543   // register listing_table_from_dir(glob_dir) as __rollup_partials_*
crates/provider/src/factory/temporal_reduce.rs:1499-1517  // merge_sql: SELECT date_bin(...) AS time_bucket, <merge_exprs> FROM partials GROUP BY time_bucket ORDER BY time_bucket
```

Hash aggregation is *stateful and blocking*: it builds an in-memory hash table
with **one accumulator per distinct group** and emits nothing until all input is
consumed. The group key is `time_bucket`, so:

```
groups = number of time buckets over all history = O(N)
```

That group table is resident memory proportional to total data. This is the
operation that fundamentally needs more RAM for more data — not the scan, the
**group state**. Unlike a sort it does not spill gracefully, and its size is
dictated by group cardinality, which grows linearly with elapsed wall-clock.

### 1b. Redundant global `ORDER BY ts` over the whole reconstructed series

The incremental splice re-sorts the entire series (old cache ⧺ freshly merged
tail) even though both inputs are already sorted and disjoint at `lo`:

```
crates/provider/src/factory/temporal_reduce.rs:628-634
    SELECT * FROM (
        SELECT * FROM {old}  WHERE ts <  lo    -- entire history, already sorted
        UNION ALL
        SELECT * FROM ({merge}) WHERE ts >= lo -- new tail, merge_sql already ORDER BY time_bucket
    ) ORDER BY ts                              -- <-- forces a full Sort over all N rows
```

`{old}` is a parquet cache written sorted; `{merge}` ends in `ORDER BY
time_bucket` (§1a); and everything in branch 1 is `< lo ≤` everything in branch
2. The outer `ORDER BY` is therefore **redundant** but forces DataFusion to plan
a `Sort` buffering all N rows (spills, but still large + O(N) bookkeeping).

### 1c. Why "increments" don't help today

The code *is* reaching for incrementality — per-version partials, a persisted
`frontier` (`write_frontier`, `temporal_reduce.rs:515-518`),
`find_uncached_members` (`:470`), and a `dirty_lo` splice point (`:547,582`) —
but the **read-side reconstruction still re-groups and re-sorts all history**
every build and treats the merged cache as mutable. Nothing is ever *final*.

## 2. Key insight: time buckets seal

Time-bucketing has a property the current design discards:

> A reduced bucket is a pure function of a bounded, contiguous slice of input.
> Once its window is far enough in the past that **no future ingest can land in
> it**, its aggregate is final forever.

So `GROUP BY time_bucket` is not the flaw — downsampling *is* time-bucketing. The
flaw is grouping over **all** time, **every** build, and treating the result as
**mutable**. A sealed bucket should be computed **once** and never re-scanned,
re-grouped, or re-sorted.

## 3. Proposed architecture: sealed append-only runs + bounded hot window

Reorganize each reduced series into two zones separated by a **watermark**
(derived from the existing `frontier` minus an allowed-lateness bound):

- **Sealed history** — buckets below the watermark. Immutable. Written **once**
  as sorted, append-only run files (LSM-style segments). Never re-scanned,
  re-grouped, or re-sorted. Reading them is concatenation.
- **Hot window** — the bounded set of open buckets above the watermark that can
  still receive late/straddling data. The **only** place `GROUP BY` runs; holds
  O(hot-window) state — constant, independent of history.

Every stage becomes bounded **in memory** — while retaining *all* history and
keeping it fully queryable:

| Stage | Today | Proposed |
|---|---|---|
| Aggregate memory | O(all buckets) hash table | O(hot window) |
| Rebuild compute / tick | O(all partials) re-group + O(N) re-sort | O(new data) |
| Read memory | full re-merge + global sort, O(N) | k-way **SortPreservingMerge** of sorted runs + hot set, **O(1)** |
| Read time (full history) | O(N) | O(N) — but streamed, constant memory |
| Disk | grows (append-only) | grows (append-only) — **unchanged; all history kept** |

Read path = `SortPreservingMerge(sealed_run_0 … sealed_run_k, hot_buckets)` over
already-sorted, disjoint inputs → streaming, O(1) memory, no global sort, no
global GROUP BY.

**Memory and retention are independent.** This design bounds *memory*, not *how
far back you can query*. Every bucket ever computed stays on disk and stays
readable forever; a full-history scan simply takes longer (O(N) time) while using
constant memory (O(1)). Nothing here drops, expires, or windows the data.

This is the standard streaming-aggregation model (watermarks + allowed lateness
+ immutable output runs). Watertown already has the substrate: series types
store **append-only deltas concatenated on read**
(`temporal_reduce.rs:452-456`), and the raw layer is log-structured.

## 4. Concrete change proposal (increments toward the target architecture)

The end state (§3) is the goal from the first commit. The phases below are
**increments that each land real structural progress toward it** — not throwaway
stopgaps. Every phase moves the system closer to sealed-runs + hot-window and is
independently reviewable and shippable.

### Phase 1 — Bound the aggregate and the read to the hot window

The two O(N) operators (§1a, §1b) are eliminated by only ever
grouping/sorting the buckets that can still change:

1. **Scope the aggregate to the dirty window.** Push `time_bucket >= lo` *into*
   the partials scan feeding `merge_sql` so the hash table only holds recent
   (open) buckets, never all history. `merge_sql` (`:1499`) gains a lower-bound
   predicate on the `date_bin` expression; the splice (around `:608-644`) passes
   `dirty_lo` down instead of filtering the merged output afterward.
2. **Replace the global `ORDER BY ts` with an order-preserving concat** (`:631`).
   Branch 1 (`ts < lo`, sealed prefix) and branch 2 (`ts >= lo`, freshly merged
   tail) are each already sorted and disjoint at `lo`, so emit branch 1 then
   branch 2 under a `SortPreservingMerge` (declare both inputs sorted on `ts`) —
   no buffering Sort.

This already makes per-tick **aggregate + sort memory** O(hot window) instead of
O(N), using the *same* watermark concept (`lo` = the dirty boundary) that Phase 2
generalizes. It is the first real slice of the target model, not a detour.

> **Remaining after Phase 1:** branch 1 is still copied *through* the merged
> cache each rebuild — O(N) *I/O and time*, but O(1) *memory* (streamed). Phase 2
> removes that copy by making the sealed prefix immutable so it is never
> rewritten.

### Phase 2 — Sealed immutable runs + explicit watermark

Turn the "sealed prefix" from Phase 1 into first-class immutable storage,
replacing the single mutable merged-output cache
(`rollup_cache::merged_cache_path` / `write_merged_cache` /
`verify_merged_cache` / `*_coverage`):

1. **Watermark.** Define `watermark = frontier − allowed_lateness` (§5). A bucket
   whose window end `≤ watermark` is *sealed* and can never change again.
2. **Sealed runs.** When buckets cross the watermark, append them **once** to an
   immutable sorted run file (content-addressed; never rewritten). Maintain a
   small manifest of run files + their `[lo,hi]` bucket ranges. Optionally
   compact adjacent small runs in the background (LSM leveling) — a bounded,
   O(run) operation, never O(N).
3. **Hot state only.** `GROUP BY` runs exclusively over partials whose bucket
   `> watermark`. Bounded group table, permanently.
4. **Read = SortPreservingMerge** over (sealed runs ⧺ hot buckets). No global
   sort, no global GROUP BY, no rewrite of history. The `ProviderContext` export
   hint (`digest` / `changed_since`) is derived from the newest sealed run + hot
   set.

This removes the O(N) per-tick I/O left after Phase 1 and makes memory O(hot
window) end-to-end. The existing `frontier`, per-version partials, and `dirty_lo`
machinery are **repurposed** (frontier → watermark source; partials → hot-window
inputs), not thrown away.

### Phase 3 — Hierarchical rollup (cheaper coarse history; still keeps everything)

Build `res=600` by folding sealed `res=60` runs, `res=3600` from `res=600`, etc.
The resolutions already nest (`parse_nesting_resolutions`, `:1076`), so each
level is a bounded fold over the level below and is exponentially smaller.

This is a **compute/disk efficiency** improvement, **not** a retention policy:
all resolutions, including fine `res=60`, are kept for all history and stay
queryable. Coarse levels just make long-range queries and rebuilds cheaper. No
data is ever dropped, expired, or windowed.

## 5. The one semantic decision this forces

**Maximum allowed lateness — when is a bucket final?** Ingests can deliver
out-of-order/straddling data across ticks, so a bucket is only sealable once no
future version can contribute to it. Options:

- **A. Fixed lateness bound** (e.g. seal buckets whose end `< frontier − 5 min`).
  Simple; late data beyond the bound is dropped or lands in a correction run.
- **B. Source-driven** (seal at `frontier` when the source is strictly
  append-only in time — true for the perf/journal series here). Zero lateness;
  maximal sealing.
- **C. Per-factory config** (`allowed_lateness: <duration>`), defaulting to B.

Recommendation: **C defaulting to B**, since selfmon's inputs are monotonic in
time. Everything else in §3–4 follows mechanically once this is fixed. Note this
is purely about *when a bucket stops changing* (so it can be sealed) — it does
**not** bound retention; sealed buckets are kept forever.

## 6. Compatibility & migration

- Config surface (`resolutions`, `aggregations`, `in_pattern`, …) unchanged.
- **All history is retained and queryable throughout** — no phase drops, expires,
  or windows data. Only the *memory* used to compute and serve it changes.
- Phase 1 is cache-compatible (no on-disk format change).
- Phase 2 changes the on-disk reduced-cache layout; migrate by discarding the
  old merged cache and rebuilding once from partials (a single cold O(N) build,
  then bounded memory forever). Corrupt/absent caches already trigger a full
  rebuild today, so the fallback path exists.
- Raw series, object model, transparency log, and `steward`/`sync-store` remote
  formats are untouched.
- **Applies to every `temporal-reduce` consumer** (site-prod/-staging dashboards,
  the noyo subsite, selfmon, and any third-party pond) with no config change.
  Selfmon is merely the highest-cadence, longest-epoch stressor that surfaced it
  first.

## 7. Risks & validation

- **Output equivalence.** Golden test: Phase 1/2 output byte-identical (or
  aggregate-identical modulo float assoc) to the current full rebuild on a fixed
  fixture. Reuse existing `temporal_reduce` tests (`test_factory.rs`,
  `test_support.rs`).
- **Full-history queryability.** Assert a query spanning genesis→now returns the
  same rows before and after each phase (retention is never reduced), and that it
  runs in constant memory (O(1)) regardless of series length.
- **Late data correctness.** Test straddling/out-of-order versions against the
  chosen lateness bound; assert sealed buckets are never mutated and late-beyond-
  bound data follows the documented policy (drop vs correction run) — hard
  failure, no silent fallback.
- **Memory regression guard.** Extend the selfmon stressor: assert daily-max peak
  memory plateaus across an epoch (flat, not monotonic) on a synthetic
  ever-growing series, while disk keeps growing (proving history is retained).
- **Interaction with `POND_MEMORY_LIMIT_MB`** (FairSpillPool,
  `tlogfs/src/persistence.rs`): Phase 2 should make the reduced path fit well
  under the pool regardless of history; verify the pool is no longer the binding
  constraint for selfmon sitegen builds.

## 8. Implementation progress

| Phase | Scope | Status |
|---|---|---|
| 1 | Hot-window GROUP BY + order-preserving concat read (cache-compatible) | Proposed |
| 2 | Watermark + sealed immutable runs + bounded hot window; SortPreservingMerge read | Proposed |
| 3 | Hierarchical rollup (cheaper coarse history; retains all data) | Proposed |
