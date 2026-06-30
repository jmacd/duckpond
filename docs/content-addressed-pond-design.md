# Content-Addressed Pond: A Clean-Slate Design

> **Status:** First-principles design note. Describes the pond as a
> content-addressed object store with a built-in transparency log, designed
> from scratch. It does not assume the current bundle/frontier replication
> machinery and is deliberately written so it can be read on its own.

---

## 1. Thesis

A pond is a **content-addressed store of immutable objects**, advanced by an
**append-only sequence of commits**, one commit per transaction. Everything
the system needs -- integrity checking, replica comparison, peer-to-peer
sync, and an externally verifiable transparency log -- is a thin layer over
that single shape.

The whole design follows from one rule:

> **Content is addressed by the hash of its bytes; lineage lives only in the
> commit.** No content hash anywhere depends on `pond_id`, node identity,
> sequence number, or wall-clock time.

That separation is what makes content comparable across ponds, makes sync a
hash-set difference, and makes the transparency log a derived index rather
than a second source of truth.

---

## 2. Goals (in priority order)

1. **Integrity.** Detect any corruption of stored bytes, cheaply and
   locally, with no separate bookkeeping to fall out of sync.
2. **Comparison.** Decide "are these two ponds (or two subtrees) identical?"
   by comparing a single hash, and localize any difference proportional to
   the size of the difference, independent of lineage.
3. **Sync.** Transfer to a peer only the objects it lacks, by hash. Both
   one-way clone/fetch and, because each pond has a single writer,
   fast-forward push-pull and cross-pond grafting.
4. **Transparency.** Publish an append-only, independently verifiable log of
   commits, using standard tooling, so a skeptical third party can check
   inclusion and append-only growth without trusting the operator.

Goals 1-3 are served by the content graph (Sections 4-6). Goal 4 is the log
(Section 7), which sits on the same commit spine.

---

## 3. Three Merkle layers, one spine

There are exactly three hashing structures, and they never compete because
they are oriented differently:

```
TIME    transparency log    Merkle over the commit sequence     append order
          | leaf = a commit
SPACE   content tree        commit -> tree -> tree -> blob       filesystem shape
          | a directory's child hash IS the child's tree hash
BYTES   verified streaming   byte ranges within one blob          byte offset
```

The **commit** is the shared spine: it is the *top* of the SPACE tree (it
names a `root_tree_hash`) and the *leaf* of the TIME log (the log orders
commit hashes). SPACE answers "what does the pond contain, and what
diverged?"; TIME answers "is this commit recorded, and did the log only
grow?"; BYTES answers "is this byte range inside this object?".

---

## 4. The objects

Four immutable object kinds, each addressed by the BLAKE3 hash of its bytes
(the store invariant is **name == hash(bytes)**):

| Object  | Hashes over | Carries lineage? |
|---------|-------------|------------------|
| blob    | the exact stored bytes of one file version | no |
| tree    | the sorted entries of one directory | no |
| commit  | `(root_tree_hash, parent_commit_hash, node_manifest_hash, provenance)` | **yes -- the only place** |
| ref     | a commit hash (a pond's tip, or a grafted foreign tip) | pointer |

### 4.1 Blob -- the leaf atom

A file version's identity is `blake3(version bytes)`, computed once at ingest
and never recomputed. It commits to the exact stored bytes -- byte equality,
not logical equality. Two files with identical logical content but different
physical encodings hash differently; that is accepted and simplifying.
Large blobs additionally carry a byte-range verification structure (the BYTES
layer) so a reader can verify any range without the whole object.

### 4.2 Tree -- a directory, content only

A tree hashes a directory as its sorted entries:

```
tree_hash(dir) = blake3( sorted_by_name[ (name, entry_type, child_hash) ] )
```

where a subdirectory's `child_hash` is that subdirectory's `tree_hash`, and a
file's `child_hash` is its blob hash. The entry value is **content plus
type, nothing else** -- no node identity, no partition, no timestamp.

Two consequences, both load-bearing:

- **Recursive:** equal `tree_hash` means identical subtree in one compare;
  unequal means descend by `child_hash` to the divergent children. Subtree
  identity is a single-hash question.
- **Why content-only is required, not just nice:** a hash that is both
  content-only and *child-independent* cannot uniquely identify a subtree.
  Two directories with the same local entry names but different descendants
  would share a hash, and a flat set of `(parent, name, child)` edges keyed
  by such hashes cannot tell a swap apart -- two ponds with subtrees
  exchanged would hash equal while differing. The recursive fold is exactly
  what removes that collision: folding the children in makes the parent hash
  change whenever any descendant changes. (A positional, path-keyed manifest
  also avoids the collision and avoids the per-ancestor write, but it cannot
  answer subtree identity by a single hash and does not dedup moved
  subtrees; it is the right choice only if subtree-level operations are never
  needed. This design keeps the recursive tree.)

### 4.3 Commit -- where lineage lives

A commit wraps one transaction:

```
commit = ( root_tree_hash, parent_commit_hash, node_manifest_hash, provenance )
provenance = pond_id, seq, time, author, original request
```

This is the inversion the whole design rests on: blobs and trees are pure
content; **all provenance is isolated in the commit.** That keeps subtree
hashes comparable across ponds while preserving full lineage for audit and
for the log. A pond has a single writer, so its commits form a **linear
chain**; `parent_commit_hash` makes that chain a hash chain already.

The `node_manifest_hash` names the commit's **node manifest** (Section 4.5),
the one place node identity is recorded. It is part of the commit and therefore
of lineage; the pure trees and blobs stay identity-free.

### 4.4 Ref

A ref is a named pointer to a commit hash -- a pond's own tip, or the tip of
a foreign pond that has been grafted in. Sync moves refs forward.

### 4.5 Node manifest -- identity for the mirror

A tree entry is `(name, entry_type, child_hash)` and carries no `node_id`, so
the content objects dedup across ponds (Section 1). But a consumer that pulls a
pond and wants to update it incrementally needs to know *which* node each
position is, so that a rename stays a rename and a node's version history is
preserved. The node manifest supplies exactly that, out of band from the pure
content:

```
manifest = list of ( node_id, parent_node_id, name, entry_type, child_hash )
```

one row per node in the commit's tree -- the source's real `NodeID`s. It is a
content-addressed object like any other (its hash is `node_manifest_hash`), so
it dedups across commits when the structure is unchanged. A read-only mirror
adopts these ids verbatim (Decision D8), making it row-identical to the source
and turning incremental pull into a `node_id`-keyed diff (Section 8.5.2).
Because identity lives only here and in the commit -- never in trees, blobs, or
series -- content comparison, dedup, and the transparency log are unaffected.

---

## 5. Storage: the commit spine and the tree-hash table

The object model above is logical. Two physical decisions make it cheap on a
columnar, log-structured substrate where each directory is its own partition
and each write appends a file.

### 5.1 Recursive hashing writes the ancestor chain -- accept it

When a deep file changes, every ancestor directory's `tree_hash` genuinely
changes, because each ancestor's hash is a fold over its descendants. So a
commit must record a new tree hash for every directory on the path from the
change to the root. The **count** of touched ancestors is intrinsic to
recursive hashing and cannot be avoided. What can be avoided is the **cost
per touched ancestor**.

### 5.2 One tree-hash table, one file per commit

Do **not** store a directory's `tree_hash` inside the directory's own content
record. Doing so would rewrite the full directory snapshot to change 32
bytes, and -- because each directory is a separate partition -- would add a
new file to every ancestor partition on every deep write. That is the worst
case for a log-structured store: many small files, fast metadata growth.

Instead, route all of a commit's touched `(node, tree_hash)` updates into a
**single tree-hash table**, partitioned by `pond_id`, written as **one file
per transaction**. The ancestor chain becomes *rows in one file* rather than
*files across many partitions*. Directory content records are never rewritten
for a descendant-only change.

This also keeps the two digests cleanly separated:

- the directory's own content record stays the **own-content** digest --
  child-independent, unchanged when only descendants change;
- the tree-hash table holds the **recursive** digest -- the content-only
  fold, beside the data, not inside it.

The row is keyed by **node identity plus version** (`(pond_id, node_id,
version)`), a per-node index whose lifecycle is identical to the existing
per-node version history -- ordinary retention and vacuum manage it, with no
separate garbage collection. The row *values* are content hashes, so sync
still transfers and dedups **by hash on the wire** regardless of the storage
key; content-addressing for transfer does not require content-addressing at
rest. A content-addressed-at-rest store (rows keyed by `tree_hash`, so
identical subtrees collapse across commits and ponds) is a possible later
optimization, but it saves almost nothing -- the rows are tens of bytes -- and
it would require a reachability/GC subsystem over the commit DAG, so this
design does not adopt it. Either way it is one file per commit; object-per-file
storage is the shape to avoid. See Decision D1 in Section 11.

### 5.3 The top-level commit record is the spine

Per commit, one small, permanent record:

```
( seq, pond_id, root_tree_hash, parent_commit_hash, node_manifest_hash, provenance )
```

This is simultaneously the top of the SPACE tree and -- as Section 7 shows --
the leaf of the TIME log. It is tiny, so it is kept forever. The bulky
per-node tree-hash deltas of Section 5.2 are a compactable detail and may be
checkpointed and vacuumed; the commit spine is append-once.

Because the commit record carries `root_tree_hash`, and that hash **is** the
transparency-log leaf (Section 7), the root fold must be computed **at commit
time** -- there is no leaf to log otherwise. Computing it incrementally
(folding only the touched ancestor chain, reading unchanged children from the
tree-hash table) is exactly why the table persists tree hashes rather than
recomputing them at compare time. Persisting is therefore not a tuning choice
but a requirement of the log unification. See Decision D3 in Section 11.

---

## 6. Integrity and comparison

### 6.1 Integrity is the store invariant

Because an object's name is its hash, "is this object intact?" is "re-hash
the bytes; do they equal the name?" There is no separate recorded-hash to
drift from the bytes, because the name is the recorded hash. A scrub walks
all objects reachable from a ref, re-hashes each, and verifies the byte-range
chains of large blobs. No separate integrity Merkle is computed.

### 6.2 Comparison falls out of the tree

- **Equal `root_tree_hash`** -> the two ponds have identical content. Replica
  equality is the degenerate case: a clone has identical content, hence an
  identical root.
- **Unequal** -> descend by `child_hash` to the divergent subtrees, work
  proportional to the difference.

Both are independent of `pond_id` and lineage, because the tree is
content-only.

---

## 7. The transparency log is the spine, indexed

The log is not a second structure. The append-only sequence of commit records
from Section 5.3, ordered by `seq`, **is** the log's leaf sequence. The log
adds three things over that existing chain:

1. **A standard leaf hash.** The content graph is BLAKE3; the published log
   leaf is hashed with **SHA-256**, so off-the-shelf witnesses and verifiers
   can co-sign it.
2. **A proof index.** A binary, append-order Merkle (the RFC 6962 /
   tlog-tiles construction) over the leaves, giving O(log n) inclusion and
   consistency proofs instead of an O(n) chain walk.
3. **Signed checkpoints** (deferred -- the trust root is a signing key).

The seam between the two hash functions is exactly the leaf: the leaf payload
is the BLAKE3 `root_tree_hash` plus provenance; the log hashes that leaf's
serialization with SHA-256. Internally the system never leaves BLAKE3;
SHA-256 appears only at the publishing boundary.

So one spine carries two Merkle indices: the content tree folds **below** each
leaf (SPACE, BLAKE3, n-ary, sorted by name); the log Merkle folds **over** the
sequence of leaves (TIME, SHA-256, binary, append order). They share leaves,
never shapes.

End-to-end, a claim chains across all three layers:

```
this byte range  --(BYTES)-->  is in blob H
blob H           --(SPACE)-->  is at path P in commit C's tree
commit C         --(TIME)-->   is record N in the attested log
```

---

## 8. Sync

Because each pond has a single writer, its commit chain is linear and there
are never two concurrent commits on one ref. Sync is therefore git-shaped and
needs no merge engine:

- **Compare:** exchange tip commit hashes; equal `root_tree_hash` means
  identical, else descend by child hash to the divergent objects.
- **Transfer:** "I have commit X, you have Y -- send me the objects reachable
  from Y that I lack," by hash. Works between any two ponds with the same
  content, regardless of their lineage.
- **Push-pull** is fast-forward; **cross-pond import** is grafting another
  pond's linear chain in as a distinct ref, carrying its commits and, later,
  its checkpoints.

The sync frontier is a **single commit hash**, not a per-partition checksum
list and not a `(pond_id, seq)` pair; reachability replaces both.

### 8.1 One system, not two: the remote is a delta-managed object table

This replaces the prior bundle/frontier remote outright. There is no
coexistence period and no backwards compatibility: `(pond_id, seq)` frontiers,
per-bundle manifests, and per-partition checksum lists are **deleted**, not
wrapped. A backup is reconstructible -- the source pond is the single writer
and holds all content -- so the migration is a clean reset: re-push the full
object graph to a freshly-formatted remote.

The remote stays **delta-managed**; content-addressing is expressed *inside*
Delta, not as a hand-rolled object store beside it. The remote is one Delta
table whose rows are content objects:

```
( object_hash, object_kind, value, value_blake3, ... )   -- one row per object
( ref_name, tip_commit_hash )                            -- the pond's tip ref
```

Object rows are keyed by their content hash, so identical blobs, trees, and
commits dedup naturally; `value_blake3` already exists in the store schema and
*is* the object id. The tip ref is a distinguished row.

### 8.2 Atomicity comes from Delta, not from object ordering

A push is **one Delta commit** that writes the new object rows *and* advances
the tip ref together. Delta's transaction-log commit -- backed by
`object_store`'s S3 support (the conditional-put / lock the Delta protocol
already requires on S3) -- makes that commit atomic. Therefore:

- The tip ref can never advance without its reachable object closure, because
  both land in the same Delta commit. No "objects-before-ref" two-phase write
  is needed; Delta provides the invariant.
- No separate compare-and-swap, lock service, or ref-pointer file is
  introduced. The single storage abstraction (delta-rs over object_store) is
  the only thing the remote relies on.

### 8.3 Push and pull computation

- **Push (producer):** the single writer knows the tip it last pushed.
  Everything reachable from that tip is already on the remote, so the new-object
  set is computed **locally** -- the current commit's reachable inventory minus
  the last-pushed tip's reachable inventory (the `missing_from` set). It writes
  those object rows and the new tip in one Delta commit. No remote listing.
- **Pull (consumer):** read the remote tip ref; if it equals the local tip,
  done. Otherwise walk the object graph from the remote tip by child hash,
  fetching only object rows whose hash is absent locally, and pruning subtrees
  whose `child_hash` already matches. Apply by **rebuilding** the working pond
  from the fetched objects (Section 8.5), then set the local tip.

The passive remote needs only Delta's ordinary read/append; no server-side
compute, consistent with the single-writer, git-shaped model above.

### 8.4 Large-file blobs stay external (D7)

A file larger than the large-file threshold is stored out-of-row in the
content-addressed large-file store (`_large_files/blake3=<hash>.parquet`)
precisely so it is never loaded into a row. Object materialization preserves
this: a **small** blob's bytes become an inline `objects` row, but a **large**
blob is *not* inlined. It transfers by hash through the existing external-blob
path, which already replicates `_large_files/blake3=*` content-addressed.

So materialization yields two sets: inline objects (trees, commits, series
manifests, symlinks, recipes, and small blobs) written as `objects` rows in
the atomic push commit, and a set of large-blob hashes transferred externally.
Both are addressed by the same BLAKE3 hash, so reachability, dedup, and the
consumer's "fetch only what I lack" walk are identical regardless of where a
blob's bytes physically live. This keeps the streaming, do-not-collect
discipline: multi-gigabyte files never materialize into memory or into the
remote's row table.

**Implementation status (D7, fe614cfe + streaming refinement).** Large blobs
transfer out-of-row through a sibling content-addressed blob store on the remote
(`_blobs/blob=<hash>`), never as inline `objects` rows: push streams each blob
local->remote via a multipart upload (skipping blobs the remote already holds),
hashing as it goes so a value can never be stored under a key it does not equal;
pull's tree descent finds them by hash in the blob store when they are not an
inline row and re-externalizes locally. A multi-gigabyte blob therefore never
bloats the remote Delta table and the producer side never collects it into one
`Vec<u8>`. The full closure is complete and content-addressed. (The consumer's
local rebuild still buffers each version blob; fully streaming ingest is a later
refinement.)

### 8.5 Pull and consumer rebuild (Fork 2)

Push (Section 8.3) is the easy half: the producer already holds every object
and re-expresses it. The hard half is the **consumer**, which holds a remote
object graph -- commits, trees, blobs, series manifests, recipes -- and must
turn it into a *working tlogfs pond*. The object graph deliberately throws away
everything tlogfs needs at rest: a tree object lists `(name, entry_type,
child_hash)` but carries **no node_ids, no partitions, no versions, no
timestamps, no txn_seq**. Rebuild is the inverse of the read-side fold: walk the
graph top-down and synthesize the `OplogEntry` rows the fold originally read.

#### 8.5.1 The fetch walk

Reading the remote tip ref gives a commit hash. If it equals the local tip, the
pull is a no-op. Otherwise:

1. Fetch the tip commit object; decode it (`Commit::decode`) to get
   `root_tree_hash` and `parent_commit_hash`.
2. Walk the commit chain back along `parent_commit_hash` until reaching a commit
   the consumer already has (or the genesis commit on first pull).
3. For the resulting set of commits, walk each `root_tree_hash` by `child_hash`,
   fetching only objects whose hash is absent locally and **pruning any subtree
   whose `child_hash` already matches** a tree the consumer holds. This is the
   `missing_from` walk in reverse: work is proportional to the difference, not
   the pond size.

The fetch walk is pure content addressing and needs no identity decisions: it
moves bytes keyed by hash. Identity enters only at materialization.

#### 8.5.2 The consumer adopts the source's node_ids via a commit manifest

To write `OplogEntry` rows the consumer must assign each node a `node_id` and a
`part_id` (its parent directory's node_id). The pure content objects -- trees,
blobs, series -- deliberately carry **neither**: a tree entry is
`(name, entry_type, child_hash)` with no identity, which is exactly what lets
two ponds with overlapping content share object hashes (Section 1). So the
content graph alone tells the consumer *what* the tree contains and *where* (by
name), but not which `node_id` the source used.

That missing identity is what an incremental pull needs. Resolving a position by
**path** is not enough: when a name disappears and another appears, the consumer
cannot tell a *rename* (same node, preserve identity and version history) from a
*delete plus a fresh create* -- the two are indistinguishable by name and
content. Without node identity, every rename degrades to delete-and-recreate
(re-materializing the node's content under a new id) and every deletion must be
found by diffing each directory's names.

The fix is to ship identity **out of band, in the commit**. The commit is
already the one pond-specific object -- it carries `pond_id`, `seq`, and
provenance -- so two ponds never share a commit anyway. It is therefore the
correct home for node identity, and putting identity there leaves the trees,
blobs, and series **node_id-free and still dedup-shareable**. Each commit
references a **node manifest**: a content-addressed object listing, for every
node in that commit's tree,

```text
node_id  ->  (parent_node_id, name, entry_type, child_hash)
```

These are the source's real `NodeID`s, not derived and not re-minted. The
consumer is a read-only mirror (Decision D9): it **adopts** them verbatim,
creating each node with the explicit id the manifest gives (the persistence
layer already supports explicit-id creation, and `part_id` is the manifest's
`parent_node_id`). The manifest is a full snapshot of the logical tree, so it
dedups across commits whenever the structure is unchanged and the consumer never
needs the previous manifest -- it diffs the incoming manifest against its own
current rows.

With node identity in hand, incremental pull is a **node_id-keyed diff** and the
hard cases collapse into set arithmetic over `node_id`:

- present in the manifest, absent locally -- **create** with the given id;
- absent in the manifest, present locally -- **delete**;
- same id, changed `child_hash` -- **write a new version** in place (history
  preserved; for a series, append only the missing version suffix, Section
  8.5.3; for a directory, recurse; for a dynamic node, update its recipe);
- same id, changed `name` or `parent_node_id` -- **rename** in place, with no
  re-materialization.

No path search, no per-directory name diff for deletion, and no rename
ambiguity. Because the consumer adopts source ids it becomes **row-identical**
to the source, so the node_id-keyed fsck Merkle now genuinely matches across
replicas. This is **Decision D8 (decided, implemented)**: adopt source node_ids
via the commit manifest. The two alternatives are rejected -- *deriving*
`node_id = H(parent_node_id, name)` buys cross-consumer id agreement that the
content-tree hash already provides and breaks on rename; *locally minting* ids
forces the fragile path-resolution scheme above.

Cross-pond mounts are scoped out of the content tree entirely. A pond that
imports another mounts a foreign root, whose well-known `ROOT_UUID` would
otherwise collide with the local root. A foreign mount is a graft by reference:
its subtree lives in the foreign pond's own content tree, and the push filters
rows to the local `pond_id`. So the fold *omits* a foreign mount completely --
it contributes neither a tree entry nor a child object -- and the node manifest,
built from the same child lists, excludes it too. The local pond's content tree
is therefore exactly its own data, independent of whether a foreign subtree
happens to be replicated locally. This is what makes multi-hop imports
non-transitive: when C imports B which imports A, B's published tree carries no
record of A *in the fold*, so C's reconstruction folds equal to B's tip and never
re-replicates A's content.

Omitting the mount node from the fold would, by itself, leave nothing in the
content graph recording *which* foreign tip B grafted. To keep the graft a
first-class, content-addressed reference, each cross-pond import also writes a
small pin file at `/sys/grafts/<name>` recording the foreign `pond_id`, the
mount path, and the foreign tip commit hash. That pin is ordinary pond-owned
content: it is covered by the importing pond's commit hash and replicates to
consumers through the normal tree/push/pull path. A consumer therefore learns
the pinned foreign tip without fetching the foreign closure -- an inert
reference, exactly like a git submodule pointer whose submodule has not been
checked out. The pin pins the tip; it does not bring the foreign mount node back
into the fold, so non-transitivity is preserved.

**Implementation status.** Cross-pond import is content-native: `pond pull` on a
non-root mount opens the foreign `ContentRemote`, fetches its object graph, and
`steward::import_pond` rebuilds the foreign tree under its own `pond_id`
partition (initializing a foreign root v1, adopting source node_ids, advancing
only the foreign seq frontier), then mounts the foreign root and writes the
`/sys/grafts/<name>` pin. Rows now carry their FileID's pond_id at rest, so
foreign-rooted writes persist under the foreign partition. Multi-hop imports
(C imports B which imports A) fold equal to B's tip because foreign mounts are
omitted from the fold; C still sees B's graft pin to A under the mount
(`/imports/B/sys/grafts/<name>`) as a dangling content reference, without
re-replicating A's content, so the case is closed (test
`cross_pond_3deep_does_not_re_replicate_foreign_mount`).


#### 8.5.3 Versions, series, and provenance

- **Single-version files.** One blob object becomes one `FilePhysicalVersion`
  row: `content` = blob bytes (or external reference for large blobs), `blake3`
  = the blob hash, `version` = the node's current version on the consumer.
- **Series.** A series object references an ordered list of version blob hashes.
  A full rebuild recreates one row per version, in order. An incremental pull
  reads the node's current versions, asserts they are a **prefix** of the
  incoming list (the append-only invariant; a violation is a hard error), and
  appends only the missing **suffix** -- never re-appending versions it already
  holds. Writing each version's exact bytes reproduces its `blake3`, so the
  consumer's recomputed series object equals the source's.
- **Provenance and txn_seq.** Each commit object carries `Provenance{pond_id,
  seq, time_micros, author, request}`. The consumer is a mirror of the source,
  so it adopts the **source pond_id** and stamps each rebuilt row's `txn_seq`
  from the commit's `seq`, walking commit-by-commit oldest-first. This mirrors
  today's cross-pond import semantics under the foreign pond_id, and keeps the
  consumer's view of the source's history faithful rather than re-sequencing it
  locally. Whether a consumer may also be a *writable fork* (its own pond_id,
  its own new commits on top of a pulled base) is **Decision D9 (open)**; the
  first cut is a read-only mirror.

#### 8.5.4 Dynamic nodes and large blobs

A dynamic node's `child_hash` is its recipe hash (Section 9): a recipe object
encoding the factory type and the stored config bytes. Its generated children
are not in the graph. Rebuild decodes the recipe back into `(factory, config)`
and writes a dynamic-node row (`factory` + config bytes), then stops -- the
consumer recomputes downstream on read, exactly as the producer would. Large
blobs are referenced by hash and fetched through the external `_large_files`
path (Section 8.4); the rebuilt row is a large-file row whose `blake3` names the
external object.

#### 8.5.5 What rebuild establishes

After a pull the consumer holds: the fetched object closure (verifiable by
re-hashing each object against its key), a set of `OplogEntry` rows whose live
tree folds back to the remote `root_tree_hash` (the rebuild is correct iff the
read-side fold of the rebuilt pond equals the tip's `root_tree_hash`), and a
local tip ref equal to the remote tip. That fold-equals-tip check is the
rebuild's acceptance test and the natural first integration test. Because the
consumer adopted the manifest's node_ids, it is additionally **row-identical**
to the source, so the node_id-keyed fsck Merkle matches across replicas -- a
stronger check than the content fold alone.

---

## 9. Special cases to pin

The `child_hash` an entry contributes to its parent's `tree_hash` depends on
the node kind:

| Node kind | `child_hash` |
|-----------|--------------|
| file (physical version) | `blake3(version bytes)` |
| series / multi-version file | cumulative content-and-history hash (a stable bao root over all versions), not any single version's hash |
| directory | `tree_hash` -- the recursive fold |
| symlink | `blake3(target path)` |
| dynamic dir / read-time `table:dynamic` | `recipe_hash` -- `blake3` of a recipe object encoding the factory type **and** the stored config bytes |

- **Series.** Uses the cumulative hash above so the entry commits to the whole
  history, stable across appends.
- **Dynamic and computed nodes.** Nodes whose content is computed on read have
  no stored output bytes. Their `child_hash` is the hash of their
  **definition** -- a *recipe object* that encodes the factory type **and** the
  stored config bytes. The semantics are explicit: for a dynamic node,
  tree-hash equality means *the recipe is identical*, not that the output bytes
  are. Two nodes that share config bytes but invoke different factories
  therefore hash differently, and a consumer can reconstruct which factory to
  instantiate -- the factory type is *in* the hashed object, not carried
  out-of-band. Its dynamically generated children are derived, not stored, so
  they are **not** folded into its hash. This is correct for sync -- the recipe
  and its real upstream source data (ordinary blobs/series that sync normally)
  transfer, and the consumer recomputes downstream. An *executed* factory that
  writes real data (for example, a collector that materializes a
  `table:series`) produces physical blobs and is hashed as a series, not by this
  rule; only genuinely read-time-computed nodes use the recipe hash. Within the
  recipe object the config bytes are taken **as-is**, byte-for-byte, with no
  canonicalization. This is the same byte-equality stance the whole design takes
  (Section 4.1): reformatting a config (whitespace, key order) reads as a
  change, which is accepted as simpler than defining a canonical form. See
  Decision D2 in Section 11.
- **Compaction / rewrite.** Reorganizing storage legitimately produces new
  blobs and trees and therefore a new content hash. That is recorded honestly
  as a new commit whose trees were rewritten, with a rewrite relationship to
  its parent. There is no "content unchanged across a rewrite" invariant to
  maintain, because byte-equality blobs make a rewrite genuinely new content.

---

## 10. What this is not (deferred)

- **Signing, key custody, witnesses.** The content layers need no key; the
  log's trust root is a signing key, chosen later.
- **Logical (vs byte) equality.** Would require a canonical encoding; out of
  scope. Comparison is byte-exact.
- **A merge model.** One-way fetch and fast-forward push-pull are covered by
  the single-writer model. A three-way merge is needed only if concurrent
  writers ever share one ref, which the model avoids.
- **Log leaf granularity finer than per-commit.** Per-commit leaves are the
  default; per-row proofs can layer on later.

---

## 11. Decisions and remaining open items

### Decided

- **D1 -- Tree-hash row key: identity index, not content-addressed at rest.**
  Rows are keyed `(pond_id, node_id, version)`. Sync still dedups by hash on
  the wire because the values are content hashes. At-rest content-addressing
  (keying by `tree_hash`) is rejected for now: it saves negligible storage and
  would require reachability/GC over the commit DAG. It remains a possible
  later optimization (Section 5.2).
- **D3 -- Persist tree hashes at commit, do not recompute at compare.** Forced
  by the log unification: the commit's `root_tree_hash` is the log leaf, so the
  fold must run at commit time. Recomputing only the touched ancestor chain
  (children read from the persisted table) is the cheap, incremental way to
  produce it (Sections 5.3, 7).
- **D4 -- Dynamic-node `child_hash` is the recipe hash.** Read-time-computed
  nodes hash a *recipe object* that encodes the **factory type and the stored
  config bytes** (`recipe_hash`), not the config bytes alone. Folding the
  factory into the hash is required for correctness and for rebuild: config
  bytes alone do not identify the factory, so two nodes with identical config
  under different factories would collide and a consumer could not know which
  factory to instantiate. Tree equality means recipe equality, not output
  equality; generated children are not folded (Section 9).
- **D2 -- Encodings start simple and are not frozen.** Three byte encodings are
  needed: (a) the **tree wire format** -- the byte layout of a tree's entry
  list (delimiters, `entry_type` representation, name encoding, sort
  collation); (b) the **recipe object** for D4 -- magic header, length-prefixed
  factory type, then the config bytes taken **as-is**, byte-for-byte, with no
  canonicalization; and (c) the **series object** -- magic header plus the
  ordered version blob hashes. All start with the simplest reasonable choice.
  Because the project allows a **clean reset at will** (no
  legacy ponds to migrate), these formats are *not* permanently frozen -- they
  can be improved later by resetting. The one constraint: any two ponds that
  sync, and any one pond across a format change, must share the same encoding
  version, since hashes do not match across versions; a format change is a
  coordinated reset of all participating ponds.
- **D6 -- One delta-managed remote; no bundle/frontier coexistence.** The
  content-addressed remote is a single system that replaces bundle/frontier and
  `(pond_id, seq)` outright -- no backwards compatibility, no dual-path
  migration window. The remote stays delta-managed: content objects are rows in
  one Delta table keyed by content hash, and the tip ref advances in the **same
  atomic Delta commit** that writes the object rows. Atomicity comes from
  delta-rs over `object_store` S3 support, not a hand-rolled CAS or an
  objects-before-ref ordering. A backup is reconstructible from the
  single-writer source, so the switch is a clean re-push to a freshly-formatted
  remote (Section 8.1-8.3).
- **D7 -- Large-file blobs stay external; only small blobs become object
  rows.** Object materialization does not inline a file above the large-file
  threshold; it transfers by hash through the existing external-blob path
  (`_large_files/blake3=*`), while small blobs, trees, commits, series
  manifests, symlinks, and recipes are inline `objects` rows. Same BLAKE3 hash
  either way, so reachability/dedup/walk are uniform and large files never load
  into memory or the remote row table (Section 8.4).
- **D8 -- Consumer adopts the source's node_ids via a commit manifest.** A
  pulled object graph is reconstructed under the **source's real `node_id`s**,
  not locally-minted or derived ones. Each commit references a content-addressed
  **node manifest** listing `node_id -> (parent_node_id, name, entry_type,
  child_hash)` for every node in the tree; the consumer (a read-only mirror,
  D9) adopts those ids verbatim via explicit-id creation. Incremental pull is
  then a `node_id`-keyed diff -- create / delete / new-version / rename fall out
  as set arithmetic, with renames and deletions handled without path search or
  re-materialization, and the consumer ends up row-identical to the source. The
  pure objects (blob/tree/series) stay node_id-free so dedup and the
  transparency log are unaffected; identity lives only in the commit-referenced
  manifest. Cross-pond mounts are scoped out of the manifest and unresolved
  foreign mounts fold by mount identity. Rejected: *deriving*
  `node_id = H(parent_node_id, name)` (buys
  cross-consumer id agreement the content-tree hash already gives, breaks on
  rename) and *locally minting* ids (forces fragile path resolution; rename
  degrades to delete-and-recreate). See Section 8.5.2.

### Open

- **D5 -- Checkpoint cadence and the SHA-256 publish format.** When the log
  materializes tiles and emits checkpoints; deferred together with signing
  (Section 10).
- **D9 -- Mirror vs writable fork.** Whether a consumer is only a read-only
  mirror of the source (adopts the source pond_id, replays its commits) or may
  also be a writable fork that lands its own commits on a pulled base
  (Section 8.5.3). First cut: read-only mirror.

---

## 12. The shape in one picture

```
              ref ---> commit_N ---> commit_{N-1} ---> ... ---> commit_0
                          |                                       (TIME: SHA-256
                          | root_tree_hash                         Merkle over this
                          v                                        linear sequence;
                        tree(root)                                 each commit is a leaf)
                        /   |    \
                   tree   blob   tree        (SPACE: BLAKE3 recursive fold;
                   /  \           |           content only, no lineage)
                blob  blob       blob
                  |                            (BYTES: per-blob byte-range
              byte ranges                       verification)
```

One linear spine of commits per pond. Below each commit, a content-only
BLAKE3 tree. Over the spine, a SHA-256 transparency log. Lineage lives in the
commits and nowhere else.
