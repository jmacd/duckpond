# Incremental timeseries rollup: partial-aggregate caching + cascading

Status: design / not yet implemented. Companion to `format-cache-design.md`
and `efficiency-priorities.md`. Captures the plan for making temporal-reduce
exports cost O(new data) instead of O(history), for later reference.

Code citations use `crate/file.rs:line` and were verified against the working
tree when written.

## 1. Problem

A `temporal-reduce` node downsamples a source series into per-resolution
aggregations. Today every build rescans **all** input history for **every**
resolution:

- `generate_temporal_sql` emits
  `... FROM {table} GROUP BY date_bin(interval, ts, epoch)` with **no time
  predicate** (`crates/provider/src/factory/temporal_reduce.rs:556-571`).
- The node is `table:dynamic`: content is recomputed on every read, and
  sitegen materializes each resolution to a static file at build time, so
  scan cost == export cost == O(history) per resolution per build.

This is the root of `remote-followup.md` C2's scaling concern: per-build cost
grows with pond age. The format cache (`format-cache-design.md`) already makes
the *leaf parse* incremental; this design extends the same idea to the
*aggregation*.

### Enabling invariant: sequential input

HydroVu and oteljson ingest are **structurally sequential**: data arrives in
non-decreasing event-time order, append-only. Late-arriving rows (event time
older than already-ingested data) are an invariant violation, not a routine
case. Under this invariant:

- Once the input frontier advances past a bucket boundary, that bucket is
  **sealed** -- it can never change.
- Only the single open (frontier) bucket is mutable between builds.

A full `--rebuild` is therefore a **recovery tool** for invariant violations,
not a steady-state dependency.

## 2. Prerequisite: decomposable aggregates

Supported aggregations are `Avg, Min, Max, Count, Sum`
(`crates/provider/src/factory/temporal_reduce.rs:80-97`). All are decomposable
over a partition union except `Avg`, and `Avg = Sum / Count`. The shared
prerequisite for everything below is:

> Store `Sum` and `Count` partials internally; compute `Avg = Sum/Count` at
> read time. Never store `Avg` as a non-mergeable value.

With that, every internal rollup value is associative: the aggregate of a union
of partitions is a cheap merge of per-partition partials
(`Sum=sum`, `Count=sum`, `Min=min`, `Max=max`).

## 3. Architecture #2: per-version partial-aggregate caching

The most duckpond-native option: reuse the immutable, content-addressed cache
mechanism proven in `testsuite/tests/534` and `535`, applied to aggregation
output rather than leaf parse.

### Mechanism

Ingest already lands **one immutable parquet per input version** (per ingest
tick / rotated file). For a sequential source these versions are time-disjoint
except at adjacent boundaries.

```
for each input version V (identified by its blake3, already stored):
    if cache has partials(V): reuse
    else: compute partial aggregates over V only, keyed by bucket:
          (bucket, sum, count, min, max) per (resolution, column)
          write to cache, keyed by V.blake3        <- same trick as format cache
read result(resolution) = merge_partials(all cached partials for that resolution)
```

- **Cache key = input version blake3** -- identical to the physical-file path in
  `format-cache-design.md` (`cache/{scheme}_{node_id}/v{version}_{blake3}.parquet`).
  Immutable versions are never invalidated; new ingest computes exactly one new
  partial.
- **No watermark, no mutable state file.** Immutability + content addressing do
  the bookkeeping. `rm -rf {POND}/cache/` stays safe (recomputes on next read).
- **Per-build cost = O(new versions)**, typically one.

### Boundary buckets

A bucket may straddle two adjacent versions (raw points for one wall-clock hour
split across two ingest files). Because partials are decomposable, the read-time
merge simply groups partials by `bucket` and combines them -- the straddling
bucket is reconstructed exactly from its two partials. No special-casing beyond
`GROUP BY bucket` over the unioned partials.

### Read-time merge SQL (sketch)

```sql
SELECT bucket AS timestamp,
       SUM(sum_x)              AS "x.sum",
       SUM(count_x)            AS "x.count",
       SUM(sum_x)/SUM(count_x) AS "x.avg",   -- Avg reconstructed
       MIN(min_x)             AS "x.min",
       MAX(max_x)             AS "x.max"
FROM   <union of cached per-version partials for this resolution>
GROUP BY bucket
ORDER BY bucket
```

The union of partials is small (one row per bucket per version), so the merge is
cheap and independent of raw history size.

### Relationship to the existing format cache

This is a second cache tier with the same key discipline:

| Tier | Caches | Key | Granularity |
|------|--------|-----|-------------|
| format cache (exists) | parsed raw bytes -> parquet | blake3 (physical) / node_id (dynamic) | per source version |
| rollup cache (this) | per-version partial aggregates | input version blake3 | per (version, resolution) |

Both are throwaway, per-version-immutable, and live under `{POND}/cache/`.

## 4. Architecture #3: cascading rollups

Coarse resolutions should never touch raw history. Build a chain where each
level aggregates the next-finer level's **partials**:

```
raw --> 1h partials --> 6h partials --> 1d partials --> 1w partials
```

- `6h` re-buckets `1h` partials (date_bin of a coarser interval over the finer
  buckets), merging decomposable partials. `1d` from `6h`, etc.
- Each coarser level scans a tiny finer level, not raw. Total work drops from
  `O(history x #resolutions)` to roughly `O(history)` once (finest level, via #2)
  plus `O(small)` per coarser level.
- Requires the same decomposable partials (section 2). `date_bin` boundaries must
  nest (each coarser interval is a multiple of the finer) so finer buckets never
  split across a coarser boundary.

Combined with #2, only the finest level pays per-version incremental cost; coarser
levels ride on already-cached finer partials.

### Non-decomposable aggregates (future)

If `median`, `quantile`, or `count(distinct)` are ever added, they are **not**
decomposable and cannot cascade or merge from partials. They would require
sketch state (t-digest / HDR for quantiles, HyperLogLog for distinct) stored as
the partial. The current aggregate set (`sum,count,min,max,avg`) needs none of
this; document the constraint so new aggregates are vetted against it.

## 5. Combined design

1. Internally rewrite `Avg` -> `Sum`+`Count` partials (section 2).
2. Finest resolution: per-input-version partial computation, cached by blake3
   (#2). New ingest -> one new partial.
3. Coarser resolutions: cascade from the finest level's partials (#3).
4. Read/export: merge cached partials with `GROUP BY bucket`, reconstruct `Avg`.
5. Full history retained at every resolution; per-build cost = O(new data).
6. `--rebuild` recomputes all partials from scratch; used only on invariant
   violation (non-sequential arrival) or aggregate/config change.

## 6. Correctness notes

- **Open bucket**: the frontier bucket is mutable; it is simply the partial of
  the newest (still-growing) input version and is recomputed whenever that
  version's blake3 changes. No special handling.
- **Sequentiality**: the design assumes non-decreasing event time. If violated,
  an old bucket would need a partial it no longer recomputes -> stale. Detection:
  compare new input min-ts against the prior frontier; regression triggers a
  warning and should gate `--rebuild`. Hard-fail rather than silently merge,
  consistent with the project's no-fallback philosophy.
- **Cache safety**: identical to format cache -- per-version-immutable, content
  addressed, throwaway.
- **Aggregate/config change**: changing aggregations or resolutions invalidates
  semantics, not content. Fold a config hash into the rollup-cache subdir so a
  config change yields a fresh cache namespace rather than mixing schemes.

## 7. API / config changes

- `AggregationType` internal lowering: `Avg` emits `Sum`+`Count` partials;
  reconstruct at read (`crates/provider/src/factory/temporal_reduce.rs:80-97`,
  `498-575`).
- New rollup-cache module mirroring `crates/provider/src/format_cache.rs`
  (key by input version blake3; `find_uncached_versions` /
  `cache_write_version` analogues).
- `generate_temporal_sql` splits into: (a) per-version partial SQL, (b)
  cross-version merge SQL.
- Cascading: resolutions parsed into a nesting chain (finer divides coarser);
  reject non-nesting resolution sets at validation
  (`crates/provider/src/factory/temporal_reduce.rs:1029-1033`).

## 8. Tests (mirror 534/535 style)

- **Incrementality**: N input versions; assert build K computes exactly one new
  partial (one `[SAVE]`-style line) and reuses K-1 cached partials; per-build
  work independent of history length.
- **Correctness**: partial-merged output == single full `GROUP BY date_bin`
  recompute, byte-for-byte, including `Avg`.
- **Boundary bucket**: a bucket split across two versions reconstructs exactly.
- **Cascade**: `1d` from cascaded partials == `1d` direct from raw.
- **Invariant violation**: out-of-order input is detected and surfaced (not
  silently miscomputed); `--rebuild` restores correctness.

## 9. Phasing

1. Decomposable-aggregate lowering (`Avg`->`Sum`/`Count`) -- safe no-op refactor,
   landable alone.
2. Architecture #2 (per-version partial cache) -- delivers O(delta) builds at the
   finest resolution; biggest win.
3. Architecture #3 (cascading) -- makes coarse/all-time views nearly free.
4. Sequentiality-violation detection + `--rebuild` recovery path.

## 10. Alternatives considered (not chosen now)

- **Windowed push-down** (`WHERE ts >= anchor - window` per resolution): bounds
  per-build scan but **drops** history outside each resolution's window and needs
  rebuild for any backfill. Simpler, but lossy; superseded by #2 which retains
  full history at O(delta).
- **Incremental materialized view + watermark**: textbook continuous aggregate;
  correct but introduces mutable watermark/append state that #2 avoids by leaning
  on immutability.
- **LSM-tiered series compaction + hot tail**: the right eventual home (also
  fixes oplog/version growth, see `efficiency-priorities.md`), but a much larger
  change than #2/#3.
