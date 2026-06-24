# Content Checksum Foundation: Making Today's Checksums Carry Forward

> **Status:** Plan-ahead design note. Captures decisions that let the
> *existing* content checksums serve replica comparison now and a
> standards-compliant transparency log later -- **without** committing to
> signing, key custody, or witnesses yet. Those are deferred on purpose
> (see [Deferred](#deferred-explicitly-out-of-scope)).
>
> Companion to `docs/design-attestation-and-publishing.md` (the full
> transparency-log / publishing design). This note is the narrower
> question: *what shape should the checksum substrate take so nothing has
> to be re-derived when we get there?*

---

## 1. Goal

Make the content checksum we already compute a sound, reusable foundation
for three consumers, in priority order:

1. **Replica / state comparison** (today): is replica B byte-identical to
   pond A? -- already shipped as `pond fsck`.
2. **Recursive partition equality** (near term): look up a content digest
   for any directory/partition in O(1) and compare two clones in
   O(divergence) instead of O(size).
3. **Transparency log** (later): publish an append-only, independently
   verifiable log of the data, using industry-standard tooling.

The throughline is a single **leaf atom** (Section 3) that every consumer
reuses. The two tree *shapes* built over that atom are different and must
not be conflated (Section 4).

---

## 2. What exists today (the substrate)

| Piece | Where | What it gives us |
|-------|-------|------------------|
| Per-version content hash | `OplogEntry.blake3` (`crates/tlogfs/src/schema.rs`) | `blake3(version bytes)` for `*PhysicalVersion` / `TablePhysicalSeries` |
| Cumulative series hash | `FilePhysicalSeries.blake3` = `bao_root(v1\|\|...\|\|vN)` | content+history hash of a whole series |
| Verified streaming | `bao-tree 0.16`, `crates/tinyfs/src/bao_validating_reader.rs` | byte-range inclusion proofs *within one object* |
| Content-addressed blobs | `_large_files/blake3=<hash>.parquet` | dedup + content addressing for files > 64 KiB |
| State Merkle | `sync_store::checksum::Merkle` | per-partition + (via fsck) root membership checksum |
| `pond fsck` | `crates/steward/src/fsck.rs` | two-level state Merkle + content re-hash pass |

All of this is **BLAKE3-based**. There is currently **no `sha2`
dependency** in the workspace.

---

## 3. The leaf atom (the durable contract)

Everything downstream consumes one atom. We fix its contract now:

> **A version's content identity is `blake3(version bytes)`, recorded once
> at ingest, and never recomputed.** It commits to exact stored bytes
> (**byte equality**, including the physical parquet encoding), and it is
> **invariant under compaction** because compaction reorganizes the live
> queryable set but never rewrites already-recorded version rows.

Consequences of this contract:

- **Byte equality for parquet is deliberate and simplifying.** Two parquet
  files with identical logical rows but different encodings hash
  differently. We accept that: the atom proves "these exact bytes," not
  "this equivalent data." Logical-data equality is explicitly a non-goal
  (it would require a canonical Arrow encoding -- out of scope).
- **Append-once dissolves the compaction tension.** Because the atom is
  written at ingest and never recomputed, a future transparency log can
  treat each version hash as an immutable leaf even though
  `pond maintain --compact` rewrites parquet layout. The *state* Merkle
  (Section 4a) is recomputed and stays compaction-invariant by hashing row
  content, not file layout; the *history* log (Section 4b) never recomputes
  at all.
- **Large files are already content-addressed**, so their atom is their
  storage key for free.

`FilePhysicalSeries` is the one special case: its `blake3` is a *cumulative
bao root*, not a single version's hash. Consumers that need a per-version
leaf for a series must use the per-version digest, not the cumulative
field. (`pond fsck` already special-cases this -- see `fsck.rs`
`verify_row_content`.)

---

## 4. Two Merkle shapes (do not conflate)

The same leaf atom feeds two structurally different trees. This is the
central point of this document.

### 4a. State Merkle -- "are these two states equal?" (have it)

`sync_store::checksum::Merkle` (`crates/sync-store/src/checksum/merkle.rs`):

- `leaf  = BLAKE3("L" || key.len() u32le || key || value_blake3)`
- `node  = BLAKE3("N" || left || right)`; odd node **paired with itself**
- leaves **sorted by `(key, value)`**; empty tree tag `"E"`

This is a **sorted-set (membership) tree**. It answers state equality
perfectly and is the right tool for replica comparison and dedup. It does
**not** model record position or log prefix, so it **cannot** produce
inclusion or consistency proofs -- and that is fine, because that is not
its job.

`pond fsck` folds it twice today: per-partition (`node_id/version` leaves)
then a root over `pond_id/part_id` partition digests.

### 4b. History log -- "is record N present; did the log only grow?" (build later)

A transparency log is an **append-only** tree:

- leaves in **commit order** (position = index), never sorted
- odd node **promoted** (carried up unchanged), never paired with self
- supports **inclusion** (O(lg N)) and **consistency** (O(lg N)) proofs
- **append-once**: never recomputed

This is the **RFC 6962 / C2SP tlog-tiles** construction. It is a *second
tree*, not a reinterpretation of 4a. Its leaves take the Section 3 atom as
input (the record payload); the tree hash is computed fresh (Section 5).

### Why the distinction is load-bearing

| | State Merkle (4a) | History log (4b) |
|---|---|---|
| Leaf order | sorted by key | append/commit order |
| Odd node | paired with self | promoted |
| Recompute on compaction | yes (layout-independent) | never |
| Proofs | none -- root compare only | inclusion + consistency |
| Hash function | BLAKE3 (Section 5) | **SHA-256** (Section 5) |

Note: BLAKE3's *own internal tree* (and bao) is append-order /
left-complete / odd-node-promoted -- structurally **closer to 4b than 4a
is**. The obstacle to reusing 4a as a log was never the hash function; it
is the sorted/pair-with-self construction. The hash choice in Section 5 is
about ecosystem interop, not tree shape.

---

## 5. Hash layering: BLAKE3/bao for content, SHA-256 for the published log

**Decision:** run two hash functions at two layers. This is normal and
correct (content identity != attestation tree); Certificate Transparency
does the same conceptually.

| Layer | Hash | Rationale |
|-------|------|-----------|
| **Content identity** (per-version, bao streaming, large-file CAS, state Merkle) | **BLAKE3** | already shipped; bao verified streaming + CAS; fast on large sensor files; switching away would discard bao and gain nothing |
| **Published transparency-log tree** (leaves, interior nodes, tree head, checkpoint) | **SHA-256** | standards compliance (RFC 6962 / C2SP), off-the-shelf verifiers, and access to the **public witness** ecosystem for anti-equivocation |

Why SHA-256 for the log specifically:

- **Interop / witnesses.** Public witnesses co-sign only standard SHA-256
  checkpoints; standard verifiers (Go sumdb, CT tooling, Cloudflare Azul
  `tlog_tiles` / `signed_note`, browser WASM) assume SHA-256. The whole
  point of "verify with nothing but a browser" depends on speaking the
  standard.
- **Compliance optics.** SHA-256 is FIPS 180-4 and decades-scrutinized --
  meaningful for a regulator even though BLAKE3 is cryptographically strong.
- **Cost is irrelevant here.** A log hashes only 32-byte record digests and
  O(lg N) interior nodes per append; BLAKE3's speed advantage does not
  matter at the log layer. Speed is a reason to *keep* BLAKE3 for content,
  not to adopt SHA-256 for it.

**Therefore:** do **not** migrate content hashing off BLAKE3. Add SHA-256
only at the publish boundary, where the log leaf carries the BLAKE3 content
digest as payload and the signed/witnessed tree head is SHA-256.

### bao chains under the log (a bonus, not the log)

bao gives byte-range inclusion proofs *within* a file; the tlog gives
record inclusion proofs *across* the log. Chained, they yield end-to-end
provenance:

```
this row-range  --(bao)-->  is in file with blake3 H
file blake3 H   --(tlog)--> is record N in the attested log
```

bao is an asset for this chaining, but it is **not** the log tree and does
not affect the SHA-256 decision.

---

## 6. Recursive state Merkle (the near-term, no-regret step)

Generalize `fsck`'s fixed two-level fold (4a) into a **recursive** fold
over the directory/partition tree, and **persist each child's subtree
digest in its parent**:

- file leaf  = Section 3 atom (or series cumulative bao root)
- directory  = fold of `(child entry, child subtree digest)` over its
  entries
- the root is then an O(1) lookup, and comparing two clones descends only
  where subtree digests differ -- **O(divergence)**, the same top-down
  walk git fetch and Merkle anti-entropy use.

**Keying:** keep the existing identifiers (`part_id` / `node_id` /
position). This is correct for the actual goal -- comparing **clones**,
which preserve those identifiers (cross-pond import preserves them
verbatim; see `testsuite/tests/718-fsck-replica-equality.sh`). Path/name
keying would only be needed to equate two *independently built* ponds with
the same content, which is **not** a goal.

**Cost to design around:** naive Merkle propagation rehashes every ancestor
on each deep write and serializes through the root. Mitigation: compute
touched subtrees at **commit time** only (DuckPond is already
per-transaction, so this is bounded by the txn working set -- the git
model), and/or back per-partition digests with the `Homomorphic`
`PartitionChecksum` strategy already present for O(1) incremental update.

**Payoff now:** fast partition-level content lookup + clone comparison.
**Payoff later:** each persisted per-partition digest is exactly the input
a per-partition checkpoint signs -- the attestation design's "checkpoint
per partition + pond-level manifest" recursion, ready to sign when key
custody is decided.

---

## 7. Reusable vs net-new (summary)

| Capability | Status | Reuses |
|-----------|--------|--------|
| Per-version content atom | have | BLAKE3, schema row |
| Verified byte-range streaming | have | bao-tree |
| Large-file CAS | have | `_large_files/blake3=` |
| State equality / replica compare | have | state Merkle, `pond fsck` |
| Recursive persisted partition digests | **near-term** | state Merkle + commit-time fold / homomorphic |
| Append-only log tree (SHA-256, tiles) | later | atom as leaf payload; new tree + `sha2` dep |
| Inclusion / consistency proofs | later | append-only tree |
| Signed checkpoints, witnesses, publishing | **deferred** | -- |

---

## Deferred (explicitly out of scope)

These need decisions we are intentionally **not** making yet:

- **Signing & key custody.** No signing exists today; no key model is
  chosen. The transparency log's trust root is a signing key -- but none of
  Sections 3, 4a, 5 (content layer), or 6 require it.
- **Witnesses / anti-equivocation.** Depends on the published SHA-256
  format; design later.
- **Leaf granularity** (per-transaction vs per-row). Per-transaction is the
  cheap strong default; per-row layers on later.
- **Tile layout, multi-channel publishing, sitegen integration.** Covered
  by `docs/design-attestation-and-publishing.md`; not part of the
  foundation.

---

## References

- `docs/design-attestation-and-publishing.md` -- full transparency-log /
  publishing design (tlog-tiles, C2SP, witnesses, claimant model).
- `crates/sync-store/src/checksum/merkle.rs` -- the state Merkle.
- `crates/steward/src/fsck.rs` -- two-level state Merkle + content pass.
- `testsuite/tests/718-fsck-replica-equality.sh` -- demonstrates clone
  identifier preservation.
- RFC 6962 (Certificate Transparency); C2SP `tlog-tiles`,
  `tlog-checkpoint`, `signed-note`; Russ Cox, "Transparent Logs for
  Skeptical Clients."
