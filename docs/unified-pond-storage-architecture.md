# Unified Pond Storage Architecture (Decision D9)

Status: **target architecture.** Steps 4a (spine relocation), 4b (incremental
commit fold), and 5 (`commit_object` node-keyed Merkle root) have landed; steps
5b and 6 are in progress. This document describes where the system ends
up once that sequence completes, and why the design is coherent. It is the
architectural companion to the phased implementation plan in
`docs/incremental-content-tree-design.md` (see its Section 10 and progress
table); read this for the "what and why," read that for the "how and when."

## 1. The one invariant

> **The pond is one `data/` Delta Lake instance. Everything durable and shared
> lives in the pond. `control/` is not part of the pond: it is a local
> concurrency gate plus a cache, disposable and reconstructable from the pond.
> Authority flows one way: `pond -> control`, never the reverse.**

Concretely: nothing may exist *only* in `control/` that cannot be rebuilt from
`data/`, with the single, deliberate exception of **this replica's local
operator state** (which remote it pushes to, how far it has pushed) -- and that
state is, by definition, not pond content and must not travel with the pond.

Every design choice below is a consequence of holding this invariant.

## 2. On-disk layout

```
{POND}/
|- data/                     THE POND -- one Delta Lake instance, the only source of truth
|  |- _delta_log/            Delta transaction log (commit history, pond_txn metadata)
|  |- _large_files/blake3=*  external blobs > 64 KiB (no Delta schema fits raw bytes)
|  |- part_id=<uuid>/*.parquet
|  |    filesystem rows: one partition per directory; user files + dirs
|  |
|  |- INDEX node   (tinyfs::INDEX_NODE_UUID,  ".pond-node-index")
|  |    DERIVED cache. Node manifest + incremental Merkle/child-hash caches.
|  |    Fold-excluded, hidden from enumeration. Rebuilt on pull.
|  |
|  +- LOG node     (tinyfs::LOG_NODE_UUID,    ".pond-commit-log")
|       AUTHORITATIVE history. Append-only series; one version per
|       content-changing commit = one encoded commit_object (embeds
|       root_tree_hash + parent_commit_hash + provenance). Fold-excluded,
|       hidden from enumeration, never collapsed. IS the transparency-log
|       leaf sequence, pond-resident.
|
|- control/                  NOT THE POND -- disposable, local-only
|  |- write lock             single-writer concurrency gate (advisory OS lock)
|  |- audit log              Begin / DataCommitted / Failed / Completed per txn
|  |- spine cache            root_tree_hash / parent / commit_hash / commit_object
|  |                         (a copy of the LOG tail; NOT authoritative)
|  +- operator settings      remote configs' modes, last_pushed/last_pulled seq
|                            (local to this replica; the only non-rebuildable state)
|
|- tlog/                     DERIVED export -- C2SP tlog-tiles + checkpoint over the LOG node
+- git/                      DERIVED cache -- bare repo mirror
```

Only `data/` is authoritative and shared. `control/`, `tlog/`, and `git/` are
all reconstructable; a replica that loses any of them can rebuild it from
`data/` (operator settings excepted, see Section 6).

## 3. The two reserved nodes

All commit machinery lives in two reserved fixed-`FileID` nodes under the root.
Both are `FilePhysicalSeries` raw-byte nodes, both are excluded from the
content-tree fold, and both are hidden from directory enumeration. They differ
only in authority and transfer semantics.

| | content | authoritative? | transferred on pull? | recovered by |
|---|---|---|---|---|
| **INDEX** node | node manifest + incremental caches | derived | no (rebuilt) | re-fold the live rows |
| **LOG** node | append-only `commit_object` per commit | **authoritative** | yes | it *is* the history |

They are fold-excluded for the same reason: their contents are *derived from*
(INDEX) or *reference* (LOG, via `root_tree_hash`) the very root they would
otherwise be hashed into. Folding them in would be self-referential -- the same
argument that excluded the index node in Phase 2.

They are two nodes rather than one because their economics differ. INDEX is
`O(n)` in pond size and cheaply rebuilt, so it is **not shipped** (bandwidth).
LOG is small, append-only, and carries provenance that exists nowhere else, so
it **is shipped** -- it is the history.

### Why the LOG node is what makes control disposable

The commit object embeds **provenance**: sequence number, commit time, author,
and the request (CLI args) that produced the commit. Before D9 that provenance
was written only into `control/`'s `DataCommittedMetadata`, so a control loss
meant the spine could not be reconstructed faithfully -- author and request were
gone, and `pond rebuild-control` had to record them as `"unknown"`. That was the
architectural "slip": a pond-derived, durable fact lived only in the disposable
layer.

Relocating the commit object into the pond-resident LOG node closes the slip.
Provenance now lands in `data/`, atomically, in the same Delta transaction as
the data it describes. The pond is **self-describing at every committed
version**, and the control spine is demoted to a redundant cache.

## 4. What a commit does (end state)

A single `pond` invocation is one transaction with one `TransactionGuard`. On a
content-changing write, before the Delta transaction finalizes, the steward:

1. **Folds the changeset** (step 4b: incremental, `O(change)` along the touched
   path; today's 4a code still folds `O(n)` and keeps the full fold as an
   oracle). This yields the new `root_tree_hash` and the node-manifest Merkle
   root without a second scan.
2. **Writes the INDEX node** version: the node manifest plus the incremental
   caches needed to make the *next* commit `O(change)`.
3. **Reads the LOG tip** (parent commit hash) from the committed table and
   **builds the commit object** (`root_tree_hash` + parent + `node_manifest_hash`
   + `node_manifest_root` + provenance).
4. **Appends the LOG node** version = that commit object.
5. Commits INDEX + LOG + data rows **in one Delta transaction**.

After the Delta commit lands, the steward writes the control audit row and the
spine cache (both disposable), then reconciles the derived `tlog/` tile export
against the LOG node. None of the post-commit steps are authoritative; a crash
between the Delta commit and the control write leaves the pond correct and the
caches self-healing on the next open or commit.

This eliminates the pre-D9 non-atomic window ("commit the data, then separately
record the spine in control").

### Compaction adds no commit

Compaction (`Ship::compact`) rewrites parquet via Delta `optimize`. It is
**content-preserving** -- the invariant check guarantees `root_tree_hash` is
byte-identical to the parent's -- so it is transparent to the content graph and
appends **no LOG leaf**, exactly as `git gc`/repack adds no commits. Push and
pull resolve the tip from the last content-changing commit, whose root already
matches. This is why compaction never needed to solve the "inject a row into an
`optimize` commit" atomicity problem: there is nothing to inject.

The LOG node is therefore excluded from collapse/compaction candidacy: every one
of its versions is a permanent transparency-log leaf and must never be merged
away.

## 5. Where 4b, 5, and 5b take us

Step 4a made the architecture correct. The remaining steps make it efficient and
remove the last pond-derived data from the authoritative-in-control position.

- **4b -- incremental commit fold.** Both roots (`root_tree_hash` and the
  manifest Merkle root) are computed from the changeset along the touched path
  instead of a full scan, and the INDEX node persists the incremental caches
  (child-hash map + `NodeMerkle` nodes) that make this possible. Commit cost
  goes from `O(n)` to `O(change)`. The post-commit full fold, kept in 4a as a
  validation oracle, is retired here. **This is a performance change, not an
  authority change** -- but it is what makes the INDEX node a genuine durable
  incremental cache rather than a per-commit full rewrite.
- **5 -- `commit_object` node-keyed Merkle root.** *(Done.)* The commit object
  gains a `node_manifest_root` (the `NodeMerkle` root) alongside the flat
  `node_manifest_hash`. The flat hash is retained as the manifest object's
  push/pull fetch-and-verify key; the Merkle root adds an incremental identity
  commitment (verified by the pull path against the tip commit) and paves the
  way for a later incremental (delta-INDEX) manifest transfer.
- **5b -- checksum subsumption.** A partition *is* a directory; that directory's
  `tree_hash` in the INDEX node *is* its content checksum. Replication and fsck
  compare content-tree hashes, and the Tier-0 `row_leaf_digest` partition
  checksums are retired. After 5b, the per-transaction partition checksums --
  today the last pond-derived datum that `control/` holds and
  `rebuild-control` cannot recover -- are **no longer needed**, because the same
  guarantee comes from the pond-resident tree hashes.

The endpoint: **`control/` holds nothing pond-derived that isn't rebuildable
from `data/`.** The audit log comes from Delta `pond_txn` history; the spine
comes from the LOG node; the checksums come from INDEX tree hashes. The invariant
in Section 1 becomes fully realized rather than aspirational.

- **6 -- validation.** Incremental-vs-rebuild equivalence for both roots, plus a
  "discard `control/`, rebuild from the pond" test that asserts the
  reconstructed spine matches byte-for-byte (now possible because provenance is
  in the LOG node). Keep `tlog_materialize_test`, `content_pull_test`, and
  `testsuite/tests/719-tlog-verify.sh` green.

## 6. What legitimately stays local (and why that is coherent)

`control/` is disposable, not empty. After the full sequence it retains exactly
three things, none of which contradict the invariant:

1. **The write lock.** An ephemeral, single-host concurrency gate. It describes a
   momentary intent, not durable state; there is nothing to reconstruct.
2. **A cache of pond-derived facts.** The audit log and the spine cache are
   copies of what `data/` already proves. They exist for fast local queries
   (`pond log`, tip lookup) and are rebuildable at any time -- `pond
   rebuild-control` already reconstructs the audit skeleton from Delta history,
   and post-5b it can reconstruct the spine from the LOG node and the checksums
   from INDEX.
3. **Local operator state.** Which remotes this replica is attached to, their
   modes, and the `last_pushed_seq` / `last_pulled_seq` watermarks. This is the
   one class of state that is *not* rebuildable from the pond -- and that is
   correct: it is not pond content. It describes *this replica's relationship to
   other replicas*, is deliberately never shipped with a backup (so pushing a
   pond never leaks local watermarks), and is re-established with `pond remote
   add` after a rebuild. A pond is a shared artifact; its replication topology is
   local.

This is the coherence test for any future addition: if a new datum is *content*
or *history*, it belongs in `data/` (in the tree, the INDEX node, or the LOG
node). If it is *this replica talking to other replicas*, it belongs in
`control/`. Nothing else may live only in `control/`.

## 7. Replication, restated in these terms

- **Push** ships content by hash (blobs, trees, the commit chain from the LOG
  node) plus external `_large_files` blobs by hash. It never ships `control/` or
  the INDEX node.
- **Pull** fetches the commit chain and content closure, reconstructs `data/`,
  and **rebuilds the INDEX node locally** (never trusting a shipped index). The
  LOG node is populated from the fetched commits -- it *is* the transferred
  history.
- **`tlog/`** is a derived SHA-256 tile export of the LOG node, reconciled after
  each commit; a lost or lagging export self-heals against the LOG node.

Two replicas are identical iff their `root_tree_hash` matches -- a pure,
lineage-independent content check that needs neither `control/` nor matching
`node_id`s, only the pond.

## 8. Glossary of authority

| Datum | Authoritative home | Disposable copies |
|---|---|---|
| User files & directories | `data/` filesystem rows | -- |
| Large blobs (> 64 KiB) | `data/_large_files/` (by hash) | remote chunk table |
| `root_tree_hash`, node manifest | recomputable from `data/` rows; cached in INDEX node | -- |
| Commit spine + provenance | **LOG node** (`data/`) | `control/` spine cache |
| Transaction audit log | Delta `pond_txn` history (`data/`) | `control/` audit rows |
| Partition content checksum | INDEX node `tree_hash` (post-5b) | `control/` checksums (retired) |
| Transparency-log leaves | **LOG node** (`data/`) | `tlog/` tile export |
| Remote topology & watermarks | `control/` (local only) | -- |
| Write lock | `control/` (ephemeral) | -- |
