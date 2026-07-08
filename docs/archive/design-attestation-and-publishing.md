# Cryptographic Attestation and Publishing: Design

## Overview

This document describes a unified design for cryptographic attestation
of pond data, multi-channel publishing, and verifiable data
distribution across a network of ponds. It connects three existing
design threads:

1. **BLAKE3/bao-tree integrity** (implemented) -- per-file content hashes
2. **Cross-pond import** (designed) -- provenance via `pond_id`
3. **Static site generation** (implemented) -- sitegen factory

And introduces three new capabilities:

4. **Transparent log attestation** -- per-partition append-only
   verifiable logs using the C2SP tlog-tiles specification, with
   signed checkpoints
5. **Multi-channel publishing** -- push attested content to static
   sites, RSS, s@-compatible feeds, and Bluesky/AT Proto
6. **Multi-pond verification** -- attestation chains across a network
   of producer and consumer ponds

The two core goals:

**(a) Measurement fidelity and non-repudiation.** A pond operator
can prove they did not tamper with their own data. Time-series
sensor readings (water quality, flow rates, pump cycles) are
recorded in an append-only transparent log. Once attested, data
cannot be silently modified or removed. The chain of attestation
extends from the sensor device, through cross-pond import, to the
published dashboard.

**(b) Authentic, verifiable content on social networks.** Blog posts
are cryptographically signed. When cross-posted to Bluesky, RSS, or
s@, each post links to the attested static page where readers can
independently verify the data. The social network provides reach;
the static site provides proof.

### Design Heritage

This design is based on the **tiled transparent log** model developed
for Certificate Transparency (CT) and Go's checksum database. The key
references are:

- [Transparent Logs for Skeptical Clients](https://research.swtch.com/tlog)
  (Russ Cox, 2019) -- the foundational data structure
- [C2SP tlog-tiles](https://c2sp.org/tlog-tiles) -- the static HTTP
  API for serving log tiles
- [C2SP signed-note](https://c2sp.org/signed-note) -- Ed25519-signed
  text envelope
- [C2SP tlog-checkpoint](https://c2sp.org/tlog-checkpoint) -- signed
  tree head format
- [Cloudflare Azul](https://github.com/cloudflare/azul) -- Rust
  implementation providing `tlog_tiles` and `signed_note` crates
- [Static CT API](https://c2sp.org/static-ct-api) -- the modern CT
  log design that replaces dynamic APIs with cacheable tiles

The Rust crate ecosystem offers production-grade implementations:

| Crate | From | What |
|-------|------|------|
| `tlog_tiles` | Cloudflare Azul | Tiled merkle tree, checkpoints |
| `signed_note` | Cloudflare Azul | Ed25519-signed notes |
| `siglog` | prefix-dev | Full transparency log server |
| `transparentlog_core` | community | TransparentLog trait, proofs |
| `merkle-log` | community | Minimal tlog data structure |

### Motivating Example

The Caspar Water Company operates a water system and a septic system.
Sensor data is collected by small devices (BeaglePlay, Raspberry Pi)
running DuckPond. The data shows pump cycles, flow rates, water
quality, and leak detection. The company wants to:

- Publish a dashboard (HTML, charts) on GitHub Pages
- Prove the published data is authentic (not doctored)
- Allow regulators or neighbors to verify independently
- Blog about water issues with cryptographically signed posts

The verification should require nothing more than a web browser and
the published static files.

---

## Architecture

```
Pond (data collection)
  |
  |  BLAKE3 per-file hashes (existing)
  |  bao-tree outboard (existing, streaming verification)
  |
  v
Per-Partition Transparent Log (new)
  |
  |  Each partition is an append-only log of OpLog records
  |  Leaves = BLAKE3 hashes of files/versions
  |  Stored as tlog tiles (static, cacheable, immutable)
  |  Signed checkpoints attest to log state
  |
  v
Signed Checkpoint (new, C2SP tlog-checkpoint)
  |
  |  Ed25519 signature over (origin, tree_size, root_hash)
  |  Using C2SP signed-note format
  |  One checkpoint per partition, plus a pond-level manifest
  |
  v
Static Site (existing sitegen + new tlog tile output)
  |
  |  HTML pages, data files, RSS feed
  |  Tlog tiles served as static files
  |  Checkpoints, inclusion proofs
  |
  v
GitHub Pages (new publishing backend)
  |
  |  git commit = independent temporal anchor
  |  Tiles served directly from static hosting
  |  HTTPS = domain-based identity
  |  Public repository = transparent history
```

### Layers

| Layer | What it proves | Existing? |
|-------|---------------|-----------|
| BLAKE3 hash | This file has not changed since hashing | Yes |
| bao-tree outboard | Streaming read matches expected hash | Yes |
| Tlog inclusion proof | This record is in the log (O(lg N) proof) | No |
| Tlog consistency proof | The log only grew -- nothing was removed | No |
| Signed checkpoint | The pond owner attested this tree state | No |
| Git commit | This content existed at this time | No (manual deploys today) |
| HTTPS/TLS | This domain published this content | Yes (hosting) |

---

## Glossary: Transparency Terms and DuckPond Mapping

The transparency log ecosystem introduces many terms. This section
defines each one and shows exactly how it maps to DuckPond.

### Data Structures

| Term | Definition | DuckPond equivalent |
|------|-----------|-------------------|
| **Transparent log** (tlog) | An append-only sequence of records backed by a merkle tree, providing cryptographic proof that nothing was removed or modified | Per-partition OpLog, formalized as a tlog |
| **Merkle tree** | A binary tree of hashes where each parent is the hash of its children. One root hash summarizes all the data | Built over OpLog leaf hashes per partition |
| **Tree head** / **Root hash** | The single hash at the top of the merkle tree. Changes if any record changes | SHA-256 root hash of a partition's tlog |
| **Leaf** | A bottom-level entry in the merkle tree, representing one record | One OpLog record (file create/version/metadata) |
| **Interior node** | A hash computed from two child hashes. Not stored directly -- recomputed from tiles | Computed as `SHA-256(0x01 \|\| left \|\| right)` |
| **Tile** | A fixed-size block of consecutive hashes (default: 256) at one level of the tree. Immutable once full. The key innovation that makes tlogs servable as static files | A file in `{POND}/tlog/part/{part_id}/tile/L/N` |
| **Partial tile** | The rightmost tile at each level, not yet full. The only mutable tile. Written as `tile/L/N.p/W` where W is the width | Updates on each transaction that modifies the partition |
| **Entry bundle** | The actual record data (not just hashes) bundled into tile-sized groups, so clients can fetch records efficiently | OpLog record data grouped in bundles of 256 |

### Proofs

| Term | Definition | When you'd use it in DuckPond |
|------|-----------|------------------------------|
| **Inclusion proof** | O(lg N) hashes proving a specific record is in the log. You walk sibling hashes from leaf to root | "Prove this sensor reading is in the attested log" |
| **Consistency proof** | O(lg N) hashes proving an older log is a prefix of a newer log -- nothing was deleted or reordered | "Prove no data was removed since the last checkpoint" |
| **Full audit** | Recompute every hash from scratch and compare to a known root. Detects any tampering | `pond verify` in audit mode |

### Signatures and Trust

| Term | Definition | DuckPond equivalent |
|------|-----------|-------------------|
| **Signed note** | A text body + blank line + Ed25519 signature lines. The universal envelope for signed data in the C2SP ecosystem | Format for checkpoints, manifests, and signed posts |
| **Checkpoint** | A signed note containing: origin line, tree size, root hash. The log operator's attestation of current state | Signed partition state: "partition X has N records with this root" |
| **Signer key** | Ed25519 private key in `PRIVATE+KEY+name+id+keydata` format. Used to sign checkpoints | Pond operator's private key (stored securely, never in the pond) |
| **Verifier key** (vkey) | Ed25519 public key in `name+id+keydata` format. Used to verify signatures | Published in discovery document (`satproto.json`) |
| **Key ID** | First 4 bytes of `SHA-256(name \|\| 0x0A \|\| 0x01 \|\| pubkey)`. Short identifier, not cryptographically strong | Auto-computed from the pond's key name and public key |

### Roles (Claimant Model from transparency.dev)

The [Claimant Model](https://github.com/google/trillian/blob/master/docs/claimantmodel/CoreModel.md)
is a framework from transparency.dev for reasoning about who trusts
whom. Applied to DuckPond:

| Role | Definition | Who plays it in DuckPond |
|------|-----------|------------------------|
| **Claimant** | Makes a claim and signs it | The pond operator (Caspar Water Company) |
| **Claim** | The statement being trusted | "This sensor data is exactly what my system recorded, unmodified" |
| **Statement** | The signed artifact representing the claim | The signed checkpoint + tiles |
| **Believer** | Takes action based on the claim | A regulator accepting the water quality report; a neighbor reading the dashboard |
| **Verifier** | Independently checks the claim is true | Anyone who downloads tiles and recomputes proofs; a WASM verifier in the browser |
| **Arbiter** | Acts when a false claim is discovered | A regulatory body; the community; the legal system |

**DuckPond's claim in Claimant Model notation:**

```
Claim:     "I, ${pond_operator}, attest that partition ${part_id}
            contains exactly ${tree_size} records with merkle root
            ${root_hash}, and this log is append-only."
Statement: Signed checkpoint (C2SP tlog-checkpoint format)
Claimant:  Pond operator (identified by Ed25519 verifier key)
Believer:  Dashboard readers, regulators, downstream ponds
Verifier:  Anyone with the verifier key and access to tiles
Arbiter:   Regulatory body, community, legal system
```

**Device-level claim (for regulated systems):**

```
Claim:     "I, ${device_id}, am the registered sensor at
            ${installation_location}, and this data batch
            contains unmodified readings from my sensors."
Statement: Device-signed data batch
Claimant:  Monitoring device (identified by TPM-backed key)
Believer:  Pond operator (imports device data)
Verifier:  Anyone with the device certificate (issued by authority)
Arbiter:   State water board (registration authority)
```

**Authority-level claim (for the registration authority):**

```
Claim:     "I, ${authority}, certify that device ${device_id}
            was inspected and registered at ${location} on
            ${date}, operated by ${operator}."
Statement: Device certificate (signed note)
Claimant:  State water board (identified by authority key)
Believer:  Public, other regulators, operators
Verifier:  Anyone with the authority's public key
Arbiter:   Federal EPA, courts, public accountability
```

### Ecosystem Projects

| Project | What it is | Relationship to DuckPond |
|---------|-----------|------------------------|
| **Certificate Transparency** (CT) | The original transparency log system (2013). Every TLS certificate must be logged so domain owners can detect mis-issuance | Pioneered the data structures we use. DuckPond is NOT a CT log, but uses the same math |
| **Trillian** | Google's open-source transparency log library (Go). Powers CT. Offers log mode + map mode | The original server implementation. DuckPond uses the same tree construction but in Rust |
| **Trillian Tessera** | Next-gen Trillian: static tile-based API, no dynamic server needed | The Go reference for the static tile approach we adopt |
| **C2SP** | "Crypto Constructions, Standards, and Protocols" -- community spec org that standardized tlog-tiles, signed-note, tlog-checkpoint | The specifications DuckPond follows |
| **Azul** | Cloudflare's Rust implementation of static CT API. Provides `tlog_tiles` and `signed_note` crates | The Rust crates we'd depend on |
| **Siglog** | prefix-dev's Rust transparency log server (Tessera-compatible). Includes witness server | Reference implementation; possibly borrow witness code |
| **Witness** | An independent server that co-signs checkpoints after verifying consistency. Prevents the log operator from showing different logs to different people (split-view attack) | Phase 8: a neighbor or regulator could run a witness for our pond |
| **Go checksum database** (sumdb) | Go's module mirror uses a tlog to ensure `go get` always returns the same code for a given module version | Direct ancestor of the tile design. Russ Cox's blog post is the canonical reference |

### Putting It All Together

Here is how a verification flow works, using these terms:

```
1. Believer (regulator) visits https://casparwater.org/verify

2. Browser fetches:
   - Discovery document (satproto.json) --> gets verifier key
   - Checkpoint for the partition of interest --> signed note

3. Browser verifies Ed25519 signature on checkpoint using verifier key
   Now the regulator trusts: "partition X has N records, root = R"

4. Regulator picks a specific sensor reading to verify

5. Browser fetches 2-3 tiles (static files, ~24 KB total)
   Computes inclusion proof: leaf hash --> sibling hashes --> root

6. If computed root matches checkpoint root R:
   "This sensor reading is provably in the attested log"

7. If regulator cached a previous checkpoint (size M, root R'),
   browser fetches consistency proof tiles and verifies:
   "The log grew from M to N records, nothing was removed"
```

---

## Transparent Log Design

### Core Concept: Per-Partition Append-Only Logs

DuckPond already has an append-only log at its core: the OpLog. Each
OpLog record represents a file creation, version append, or metadata
update. These records are stored in Delta Lake, partitioned by
`part_id`.

The key insight is that each partition's OpLog IS a transparent log.
We formalize this using the **tiled transparent log** model from
Certificate Transparency and Go's checksum database.

A transparent log provides three guarantees:

1. **Inclusion proof** (O(lg N)): prove a specific record is in the
   log without downloading the whole log
2. **Consistency proof** (O(lg N)): prove the current log is an
   append-only extension of a previously observed log (nothing was
   removed or modified)
3. **Auditor iteration**: efficiently scan all records to detect
   anomalies

### Per-Partition, Not Per-Pond

The fundamental unit of verification is the **partition**, not the
pond. A pond is a collection of partitions (local and imported), each
identified by a `part_id` (UUID v7). Each partition corresponds to
one directory and all its direct children (files, versions).

Each partition gets its own transparent log. The pond maintains a
signed manifest referencing the current checkpoint of each partition.

**Why per-partition, not per-pond?** A single pond-level log (as in
Go's sumdb or Certificate Transparency) would be simpler to audit,
but DuckPond's partitions are the unit of data access AND data
movement. Every query touches only the partitions it needs --
scanning the entire pond to verify one directory would defeat the
performance model. And when a partition moves between ponds
(cross-pond import), its tlog travels with it, self-contained.

This design follows from how data moves in DuckPond:

- **Cross-pond import** transfers whole partitions (see
  `design-cross-pond-import.md`)
- **Remote backup** packages partitions as chunked bundles
- **The Delta Lake table** is partitioned by `part_id`
- **Each parquet file** lives inside exactly one `part_id=xxx/`
  partition directory
- **Every query** is scoped to specific partitions -- touching the
  whole pond for verification would break the performance model

A per-partition tlog checkpoint travels with the partition. When a
consumer pond imports a partition, it imports the tlog tiles and
checkpoint too. Verification is local to the partition -- you don't
need the producer's entire pond state.

### Leaf Construction

Each leaf in a partition's transparent log is one OpLog record. The
leaf data is serialized from the fields that matter for integrity:

```
leaf_data = (
  part_id          ||    # partition identity
  node_id          ||    # file/dir identity
  file_type        ||    # entry type (deterministic behavior)
  version          ||    # version number
  blake3_content   ||    # BLAKE3 content hash (already computed)
  name             ||    # filename
  txn_seq               # transaction sequence
)
```

The tlog leaf hash is then: `SHA-256(0x00 || leaf_data)` per RFC 6962.

Note that `blake3_content` is the existing per-file BLAKE3 hash
from `OplogEntry.blake3` in `schema.rs`. We are not re-hashing file
content; the leaf data embeds the existing BLAKE3 hash, and the tlog
tree hashes over that with SHA-256. Two hash functions, two layers:
BLAKE3 for "is this file intact?" and SHA-256 for "is this record in
the append-only log?"

### Tiled Storage

Following the C2SP tlog-tiles specification, the merkle tree over
partition records is stored as **tiles** -- fixed-size blocks of
consecutive hashes that are immutable once complete.

```
Partition part_id=019503a1-8e55-...
Transparent log with 270 records (leaves)

Tile layout (height=8, width=256):
  Level 0: tile(0,0) [256 leaf hashes]  tile(0,1).p/14 [14 leaf hashes]
  Level 1: tile(1,0).p/1 [1 hash = hash of tile(0,0)]
```

Key properties of tiles:

- **Immutable**: once a tile is full (256 entries), it never changes.
  This makes them perfectly cacheable.
- **Static files**: tiles are served at predictable paths:
  `<prefix>/tile/<L>/<N>` for full tiles,
  `<prefix>/tile/<L>/<N>.p/<W>` for partial tiles.
- **Self-authenticating**: any tile can be verified against its parent
  tile or the signed checkpoint.

For a partition with N records, the tile storage requires:
- ~N/256 full level-0 tiles + 1 partial
- ~N/65536 full level-1 tiles + 1 partial
- And so on up the tree

Storage overhead is roughly 2N hashes (one interior node per leaf),
each 32 bytes (SHA-256). For a partition with 10,000 records:
~640 KB of tile data, split across ~40 tile files.

### Hash Function: SHA-256 for Tiles, BLAKE3 for Content

The C2SP specs and RFC 6962 use SHA-256 for merkle tree hashing.
DuckPond uses BLAKE3 for content hashing (file integrity, bao-tree).

**Decision: use SHA-256 for tlog tiles, keep BLAKE3 for content.**

These are two different layers with different goals:

- **Content layer (BLAKE3)**: "has this file been modified?" -- the
  OpLog record's `blake3` field answers this. BLAKE3 is fast,
  already integrated, and used at write time.
- **Transparency layer (SHA-256)**: "is this record in the log, and
  has the log only grown?" -- the tlog tiles answer this. SHA-256
  is the standard, and using it means any existing tlog client,
  monitor, or witness can verify our trees without modification.

The leaf data for each tlog entry *contains* the BLAKE3 content
hash, so a verifier gets both: standard tlog verification (SHA-256
tree) AND content integrity (BLAKE3 in the leaf). Two hash functions
at two layers, each where it belongs.

Rationale for not using BLAKE3 in the tlog layer:

- Using a non-standard hash creates a private ecosystem of one --
  no existing tool can verify the tree
- The `tlog_tiles` and `signed_note` crates work out of the box
  with SHA-256; BLAKE3 would require forking or wrapping
- Witnesses, monitors, and the Go sumdb ecosystem all speak SHA-256
- Performance of the tlog hash is irrelevant: we hash ~2N 32-byte
  values per checkpoint update, which is microseconds either way
- BLAKE3's speed advantage matters for content hashing (megabytes
  of file data), not for tree hashing (kilobytes of hash values)

The RFC 6962 hash construction uses domain separation:

```
leaf_hash     = SHA-256(0x00 || leaf_data)
interior_hash = SHA-256(0x01 || left_hash || right_hash)
```

The `0x00`/`0x01` prefix bytes prevent second-preimage attacks
(a leaf cannot be confused with an interior node).

### Signed Checkpoints

Each partition's transparent log state is captured by a **checkpoint**
following the C2SP tlog-checkpoint specification:

```
casparwater.org/pond/019503a1/part/019503a1-8e55
270
q7X3aBnKL2m+RhVk1xjJ+kOzMfQP9d5iH8rjKV2dXaI=

-- casparwater.org/pond/019503a1 Az3grlg...base64-ed25519-signature...
```

The checkpoint body has three lines:
1. **Origin**: identifies the log (pond + partition)
2. **Tree size**: number of leaves (OpLog records)
3. **Root hash**: base64-encoded SHA-256 merkle root

The signature uses the C2SP signed-note format: Ed25519 over the
checkpoint text. The key name matches the origin.

### Pond-Level Manifest

The pond does NOT have a single transparent log. Instead, it
maintains a **partition manifest** -- a signed note listing the
current checkpoint of each partition:

```
casparwater.org/pond/019603b2
txn_seq=42
timestamp=2026-03-12T07:30:00Z

part_id=019603b2-9e66  path=/             size=4    root=a1b2c3...  provenance=local
part_id=019503a1-8e55  path=/sources/sep  size=142  root=e5f6a7...  provenance=imported  source=019503a1-7c44
previous_manifest=blake3-of-previous-manifest

-- casparwater.org/pond/019603b2 Az3grlg...base64-ed25519-signature...
```

This is a signed note (not JSON) -- following the C2SP convention
that signed notes are the envelope for all attested data. The
manifest says: "at transaction 42, these partitions had these tree
sizes and roots."

Extension lines carry per-partition metadata. The `previous_manifest`
line forms a hash chain across manifests.

Imported partitions carry their `source` pond_id so a verifier can
cross-check against the source pond's own checkpoint.

### Why Per-Partition

| Concern | Per-pond log | Per-partition log |
|---------|-------------|-------------------|
| Cross-pond import | Must rebuild entire log | Import partition tlog directly |
| Selective verification | Must download all data | Verify one partition independently |
| Incremental updates | Append to one giant log | Only update affected partition log |
| Trust boundaries | Single trust domain | Each partition has independent provenance |
| Backup/restore | Log doesn't survive partial restore | Each partition tlog is self-contained |
| Static hosting | One giant tile set | Tiles organized by partition |

### Incremental Updates

The transparent log is append-only by design. On each write
transaction:

1. Identify which partitions were modified (Delta Lake `add` actions
   tell us which `part_id` values received new parquet files)
2. For each modified partition, compute leaf hashes for new records
3. Append new leaves to the partition's tlog (update tiles)
4. Produce a new signed checkpoint for each modified partition
5. Update the pond-level manifest with new checkpoints
6. Sign the manifest

Only new tiles (or updates to the rightmost partial tile) need to be
written. All existing tiles are immutable. This is fundamentally
more efficient than recomputing a full merkle tree.

For a partition that grows from 270 to 275 records:
- The partial tile at level 0 gains 5 new leaf hashes
- No full tiles change (they are immutable)
- The partial tiles at higher levels may update
- A new signed checkpoint is produced
- Total work: O(lg N) hash operations, not O(N)

---

## Delta Lake Verification Gap

### The Problem

Delta Lake's `_delta_log/` contains JSON commit files. Each commit
lists `add` actions pointing to parquet data files. DuckPond trusts
this log to reconstruct the filesystem state.

**The Delta log itself is unsigned.** There is no built-in mechanism
in Delta Lake to verify that:

- A commit JSON file has not been modified
- The list of `add` actions has not been tampered with
- Parquet file references have not been swapped
- Commits have not been deleted or reordered

Delta Lake provides ACID transactions and optimistic concurrency for
*concurrent writers*, but it does NOT provide tamper detection for
*storage-level attacks*. If someone has write access to the object
store (S3, local disk), they can modify `_delta_log/` entries.

### Current Protections

DuckPond has defenses, but they don't cover the full stack:

| Layer | Protection | Gap |
|-------|-----------|-----|
| File content | BLAKE3 hash per OpLog record | Verified at read time |
| Large files | bao-tree outboard for streaming verification | Verified at read time |
| Remote transfer | ChunkedReader with BLAKE3 verification | Transfer integrity only |
| Delta commit metadata | `PondTxnMetadata` with `txn_seq` | Metadata is advisory, not signed |
| Delta log files | None | **Vulnerable to tampering** |
| Parquet file references | None | **Could be swapped** |

### How Transparent Logs Close the Gap

The per-partition transparent log provides the critical property that
the Delta log lacks: **append-only verifiability**. The tlog model
closes the gap in two ways:

**1. Content verification via inclusion proofs.**

If someone tampers with the Delta log to swap parquet files:
1. The swapped parquet file contains different OpLog records
2. Different records produce different leaf hashes
3. The leaf hashes no longer match the tiles
4. Inclusion proofs fail against the signed checkpoint
5. Verification fails

**2. History verification via consistency proofs.**

If someone deletes or reorders Delta commits:
1. The transparent log's tree size only grows
2. A consistency proof proves old tree is a prefix of new tree
3. If entries were removed, consistency proof is impossible
4. Any client that cached a previous checkpoint can detect tampering

This is the key advantage of the tlog model over a simple snapshot
merkle root: the **consistency proof** mechanism makes it impossible
to silently drop entries from the log without detection by any client
that has seen a previous checkpoint.

### Commit-Level Hashing

To fully close the Delta Lake gap, each Delta commit should include
the tlog checkpoints of all modified partitions in its commit
metadata:

```rust
// In commit_impl(), after writing records:
let mut metadata = metadata.to_delta_metadata();

// For each modified partition, store its checkpoint
for part_id in modified_partitions {
    let checkpoint = partition_tlog.latest_checkpoint(part_id)?;
    metadata.insert(
        format!("tlog_checkpoint.{}", part_id),
        checkpoint.to_string(),
    );
}
```

This means every Delta commit carries the tlog state at the time
of commit. A verifier can walk the Delta log and check that:

1. Each commit's tlog checkpoints are consistent with subsequent ones
   (tree size only grows, consistency proofs hold)
2. The chain of commits is unbroken (no gaps in version numbers)
3. The latest commit's checkpoints match the published attestation

### Parquet File Checksums

Delta Lake `add` actions support an optional `stats` field with file
statistics. DuckPond should also record a BLAKE3 hash of the entire
parquet file (not just individual records) in the `add` action's
tags or stats. This creates a three-level verification chain:

```
Signed checkpoint (tlog-checkpoint)
  |-- verifies --> Partition tlog root hash
      |-- verifies --> Tile hashes (immutable, cached)
          |-- verifies --> Individual leaf hashes (OpLog records)
              |-- verifies --> File content (BLAKE3 per record)

Delta commit metadata
  |-- contains --> Partition tlog checkpoints
      |-- verifies --> Consistency with previous commits
          |-- verifies --> The same OpLog records
```

Both chains arrive at the same data. Agreement between them is
strong evidence of integrity.

---

## Attestation Format

An attestation is the combination of:
1. A **signed checkpoint** per partition (C2SP tlog-checkpoint)
2. A **signed pond manifest** listing all partition checkpoints
3. The **tlog tiles** that back the checkpoints (static files)

### Per-Partition Checkpoint

Each partition checkpoint follows the C2SP tlog-checkpoint format
exactly -- a signed note with origin, tree size, and root hash:

```
casparwater.org/pond/019503a1/part/019503a1-8e55
142
e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2=

-- casparwater.org/pond/019503a1 Az3grlg...base64-ed25519-signature...
```

The checkpoint is the fundamental attestation unit. It says: "this
partition has exactly 142 records, and the SHA-256 merkle root over
all of them is this hash." The Ed25519 signature makes this
non-repudiable.

### Pond-Level Manifest

See the manifest format in the Transparent Log Design section above.
The manifest is a higher-level attestation that binds together all
partition checkpoints at a specific transaction.

### Attestation Chain

Each manifest references the previous one via `previous_manifest`
(BLAKE3 hash of the previous manifest text). This forms an
append-only chain. A gap in the chain (missing `txn_seq` values) is
visible to any verifier.

Additionally, every partition checkpoint is itself part of a
consistency chain: a verifier who cached a previous checkpoint can
request a consistency proof proving the log only grew. This is
stronger than a hash chain -- it is cryptographically verifiable
that no entries were removed.

### Per-Partition Attestation (Producer Side)

A producer pond (e.g., the septic system on a BeaglePlay) publishes
its partition checkpoints independently. The checkpoint format is
self-contained -- a consumer pond that imports this partition can
verify the imported data by:

1. Downloading the producer's signed checkpoint
2. Computing leaf hashes from the imported OpLog records
3. Building the merkle tree
4. Checking the root matches the checkpoint
5. Verifying the Ed25519 signature

---

## Verification

### What a Verifier Needs

1. The operator's **public key** (verifier key in C2SP signed-note
   format, from the discovery document)
2. The **signed checkpoint** (tlog-checkpoint format)
3. Access to the **tile endpoint** (static files on the published
   site or in the partition's tlog storage)

### Verification Modes

The tlog model supports three verification modes, each useful in
different scenarios:

**Mode 1: Full audit** (auditor scans all records)

1. Fetch the latest signed checkpoint
2. Verify the Ed25519 signature
3. Download all level-0 tiles (the leaf hashes)
4. Recompute the merkle tree from tiles
5. Check that the computed root matches the checkpoint root
6. Optionally, verify each leaf hash against the actual OpLog record

This is what `pond verify` does locally, or what a regulator does
for a comprehensive audit.

**Mode 2: Record inclusion** (verify one specific record)

1. Fetch the latest signed checkpoint (origin + size + root)
2. Verify the Ed25519 signature
3. Compute the leaf hash of the record in question
4. Fetch the O(lg N) tiles needed for the inclusion proof
5. Compute the proof: walk sibling hashes from leaf to root
6. Check that the computed root matches the checkpoint root

This requires downloading only ~3 tiles (for large logs), not the
entire dataset. Perfect for verifying "is this specific sensor
reading in the attested log?"

**Mode 3: Consistency check** (verify log only grew)

1. Retrieve a previously-cached checkpoint (old_size, old_root)
2. Fetch the latest signed checkpoint (new_size, new_root)
3. Verify the Ed25519 signature on the new checkpoint
4. Fetch the O(lg N) tiles needed for the consistency proof
5. Verify that old_root is a prefix of new_root
6. Cache the new checkpoint for future checks

This is the "skeptical client" model from Russ Cox's design. Any
client that has ever seen a checkpoint can verify that the log has
not been tampered with, in O(lg N) time and bandwidth.

### Tile-Based Proof Construction

Unlike RFC 6962's dynamic proof APIs, the tlog-tiles model serves
proofs as static files. The client computes which tiles it needs
and fetches them directly:

```
Published site structure:
  /tlog/
    /part/{part_id}/
      checkpoint                      # Signed checkpoint
      tile/0/000                      # Level 0, index 0 (256 hashes)
      tile/0/001                      # Level 0, index 1
      tile/0/002.p/14                 # Level 0, index 2, partial (14 hashes)
      tile/1/000.p/1                  # Level 1, index 0, partial
      entries/000                     # Entry bundles (actual records)
      entries/001.p/14               # Partial entry bundle
```

For a record inclusion proof with tree size 526:
- Client computes the leaf hash
- Client fetches tile(0, N/256) to find the leaf's sibling hashes
- Client fetches tile(1, N/65536) for the next level
- Client reconstructs the proof path to the root
- Client checks against the checkpoint root

Total: 2-3 tile fetches (each ~8 KB), regardless of log size.

### Browser-Based Verification

The published site includes a `/verify` page with client-side
JavaScript/WASM that:

1. Fetches the signed checkpoint for a partition
2. Fetches the public key from the discovery document
3. Lets the user pick a file to verify
4. Downloads the relevant tiles (2-3 fetches)
5. Computes the inclusion proof client-side
6. Verifies the Ed25519 signature
7. Shows a green checkmark or red X

This requires a WASM build of BLAKE3 and ed25519 for the browser.
The tile-based approach makes this practical -- the browser downloads
kilobytes, not megabytes.

---

## Discovery Document

The pond publishes a discovery document at a well-known path,
combining s@-compatible fields with DuckPond-specific extensions:

```json
// https://{domain}/satellite/satproto.json
{
  "satproto_version": "0.1.0",
  "pond_id": "019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00",
  "public_key": "casparwater.org/pond/019503a1+c74f20a3+AekyeRrm56hA...",
  "capabilities": ["attestation", "tlog", "blog", "atproto"],
  "tlog_url": "/tlog/",
  "manifest_url": "/tlog/manifest",
  "feed_url": "/feed.xml",
  "atproto_did": "did:web:casparwater.org"
}
```

The `public_key` is in C2SP verifier key format:
`<name>+<hex-key-id>+<base64-signature-type-and-pubkey>`. This is
both the signing identity for checkpoints and the verification key
for clients.

Fields:

- `public_key`: Ed25519 verifier key for tlog checkpoints and
  signed posts. Compatible with s@ clients that look for this field.
- `tlog_url`: base path for tile/checkpoint static files
- `manifest_url`: signed pond-level manifest (all partition states)
- `feed_url`: RSS/Atom feed for blog posts
- `atproto_did`: (optional) AT Protocol DID for cross-posting to
  Bluesky and the AT Proto federation. See Multi-Channel Publishing.

---

## Blog and RSS

### Content Authoring

Blog posts are Markdown files in the pond with date-aware frontmatter:

```markdown
---
title: "We Fixed the Leak"
date: 2026-03-10
layout: post
section: Blog
tags: [water, infrastructure, leaks]
attestation_scope: "/reduced/flow-totals/**"
---

Last month we detected a leak in the main distribution line...

{{ chart export="leak-data" }}

The data above is cryptographically attested. You can verify it
has not been tampered with using our [attestation page](/verify).
```

The `attestation_scope` frontmatter (optional) links the blog post
to a specific data scope. The post is a narrative wrapper around
verifiable data.

### RSS/Atom Feed

Sitegen generates a standard RSS 2.0 or Atom feed from blog posts:

```
dist/
  feed.xml              # RSS/Atom feed
  posts/
    2026-03-10-leak.html
    2026-03-01-spring.html
```

The feed is generated at build time from content-stage blog posts,
ordered by date descending. Each entry includes title, date, summary,
and link.

### Signed Posts (s@-compatible)

In addition to HTML and RSS, each blog post is published as a signed
note (not JSON -- following the C2SP signed-note format):

```
dist/
  satellite/
    satproto.json                          # Discovery + public key
    posts/
      index                                # Post list (newest first, signed)
      20260310T000000Z-a1b2                # Signed post (plaintext note)
      20260301T000000Z-c3d4
  tlog/
    manifest                               # Signed pond-level manifest
    part/
      019503a1-8e55/
        checkpoint                         # Signed partition checkpoint
        tile/0/000                         # Merkle tree tiles
        entries/000                        # Entry bundles
```

A signed post (plaintext note, not encrypted):

```
id: 20260310T000000Z-a1b2
author: casparwater.org
created_at: 2026-03-10T00:00:00Z
title: We Fixed the Leak
tlog_ref: /tlog/part/019503a1-8e55/checkpoint

Last month we detected a leak in the main distribution line...

-- casparwater.org/pond/019503a1 Az3grlg...base64-ed25519-signature...
```

The signature proves the post was authored by the domain owner. The
`tlog_ref` line links the narrative to the underlying data proof --
a reader can follow the link to the partition checkpoint and verify
the data independently.

---

## GitHub Publishing Backend

### Why GitHub

- GitHub Pages provides HTTPS hosting with custom domains
- Git history is an independent temporal anchor for attestations
- GitHub's infrastructure is a credible third-party witness
- The repository can be public for full transparency
- GitHub Actions can automate build-and-deploy

### Integration with Sitegen

A new output target for `pond run site.yaml build`:

```yaml
# In site.yaml
publish:
  target: github
  repo: "casparwater/casparwater.github.io"
  branch: "main"
```

The build pipeline:

```
pond run site.yaml build ./dist
  1. Run export stages (existing)
  2. Render HTML pages (existing)
  3. Generate RSS feed (new)
  4. Build/update tlog tiles for attested partitions (new)
  5. Sign partition checkpoints (new)
  6. Sign pond-level manifest (new)
  7. Generate signed blog posts (new)
  8. Write everything to ./dist

pond run site.yaml publish
  1. cd dist
  2. git add -A
  3. git commit -m "Site update: txn_seq {N}"
  4. git push origin {branch}
```

Tiles are the ideal publishing artifact: they are static, immutable,
and cache-friendly. Once a full tile is published, it never changes.
Only the rightmost partial tiles and the checkpoint need updating on
each publish cycle.

### Git as Temporal Anchor

Each git commit creates an independent merkle root (git's tree hash)
over the published content. This provides:

- **Tamper evidence**: GitHub's commit log is append-only from the
  publisher's perspective. Rewriting history (force push) is visible
  in the reflog and to any watcher.
- **Timestamps**: Git commits have author and committer timestamps.
  GitHub's server-side hooks add their own timestamp.
- **Independent verification**: The git tree hash and the pond's
  merkle root are computed independently. If both agree on the
  content, it is very strong evidence of authenticity.

### Alternative Backends

The publishing interface should be generic enough to support:

- **GitHub Pages** (initial target)
- **Any git remote** (GitLab Pages, Codeberg, self-hosted)
- **S3/R2 static hosting** (existing remote factory infra)
- **IPFS** (content-addressed hosting -- natural fit)

The attestation and signing layers are backend-independent. Only the
`publish` step changes per backend.

---

## Multi-Channel Publishing

A pond produces two kinds of content: **measurement data** (sensor
readings, time series, tables) and **narrative content** (blog posts,
reports, dashboards). Both benefit from cryptographic attestation,
but they reach audiences through different channels.

### Publishing Channels

The same Ed25519 keypair signs content across all channels. The
content is the same; only the format and distribution method differ.

```
                          pond attest
                              |
                    +----signed checkpoint----+
                    |         |               |
                    v         v               v
              Static Site   RSS Feed    Social Networks
              (tiles,HTML)  (Atom/RSS)  (AT Proto, s@)
                    |         |               |
                    v         v               v
              GitHub Pages  Feed readers  Bluesky, s@
              S3, IPFS                    clients
```

| Channel | Format | Audience | Verification |
|---------|--------|----------|-------------|
| **Static site** | HTML + tlog tiles + checkpoints | Anyone with the URL | Full tlog verification (inclusion/consistency proofs) |
| **RSS/Atom feed** | XML feed of blog posts | Feed reader users | Feed links to attested content on static site |
| **Signed posts** | C2SP signed-note (plaintext) | s@ clients, archivists | Ed25519 signature on post text; `tlog_ref` links to data proof |
| **AT Proto cross-post** | Bluesky post via API | Bluesky users (large audience) | Post contains link to attested static page |
| **AT Proto PDS** (future) | Full AT Proto repository | Entire fediverse | MST-based self-authenticating repo |

### Channel 1: Static Site (Primary)

The static site is the authoritative source. All other channels
reference it. It contains:

- HTML pages (dashboard, blog posts, charts)
- Tlog tiles and checkpoints (cryptographic attestation)
- Signed pond manifest (all partition states)
- Discovery document (`satproto.json`)
- Verification page (`/verify`) with WASM verifier

This is what regulators, auditors, and technical users consume.

### Channel 2: RSS/Atom Feed

Standard feed for blog posts. Each `<item>` includes:

```xml
<item>
  <title>We Fixed the Leak</title>
  <link>https://casparwater.org/posts/2026-03-10-leak.html</link>
  <pubDate>Tue, 10 Mar 2026 00:00:00 GMT</pubDate>
  <description>Last month we detected a leak...</description>
  <!-- DuckPond extension: attestation reference -->
  <duckpond:attestation>
    <duckpond:checkpoint>/tlog/part/019503a1-8e55/checkpoint</duckpond:checkpoint>
    <duckpond:verifier-key>casparwater.org/pond/019503a1+c74f20a3+...</duckpond:verifier-key>
  </duckpond:attestation>
</item>
```

The `duckpond:attestation` extension is optional metadata that
attestation-aware feed readers could use. Standard readers ignore it.

### Channel 3: Signed Posts (s@-compatible)

Plaintext signed notes in the `satellite/posts/` directory. These
are static files -- no server needed. An s@ client that supports
plaintext posts can aggregate them alongside encrypted posts from
other s@ users.

DuckPond diverges from s@ in two ways:
- **No encryption**: posts are public (signed, not sealed)
- **C2SP signed-note format** instead of s@'s JSON + separate
  signature, giving us compatibility with the tlog ecosystem

### Channel 4: AT Protocol Cross-Posting

When a blog post is published, DuckPond can optionally create a
Bluesky post via the AT Protocol API:

```
pond run site.yaml publish --bluesky
```

This:
1. Authenticates to the user's PDS (using stored app password)
2. Creates a `app.bsky.feed.post` record containing:
   - Post text (summary of the blog post)
   - A link facet pointing to the attested static page
   - An embed card with the post title and description
3. The Bluesky post appears in followers' feeds

The Bluesky post is ephemeral social reach. The linked static page
is the permanent, cryptographically verifiable record. A reader who
clicks through can verify the attestation independently.

**Why this works without running a PDS**: Cross-posting uses the
standard AT Proto API (`com.atproto.repo.createRecord`) to write
into the user's existing Bluesky PDS. The pond operator has a normal
Bluesky account. The post content lives on both Bluesky (for reach)
and the static site (for permanence and verification).

### Channel 5: AT Protocol PDS (Future)

The ambitious path: DuckPond acts as a minimal AT Protocol Personal
Data Server. The pond's data repository would be a signed Merkle
Search Tree containing pond records as AT Proto lexicon-typed objects.

Structural overlap between AT Proto repos and DuckPond:

| AT Proto concept | DuckPond equivalent |
|-----------------|-------------------|
| Repository (MST) | Partition (tlog) |
| Commit (signed root) | Checkpoint (signed root) |
| Record (CBOR object) | OpLog entry (parquet row) |
| DID (identity) | Pond ID + verifier key |
| CAR export | Partition backup bundle |
| Signing key | Pond signing key (Ed25519) |

Both use SHA-256 merkle trees with Ed25519 signatures. The
difference: AT Proto's MST supports key-value lookup and deletion
(current state), while tlog is append-only (complete history). For
measurement non-repudiation, append-only is essential. For social
posting, the MST's editability is fine.

A DuckPond PDS would:
1. Register a `did:web:{domain}` DID (or `did:plc` for portability)
2. Serve the pond's content as AT Proto records using the `app.bsky.*`
   lexicons for blog-like posts
3. Implement the `com.atproto.sync.*` endpoints so relays can
   index the content
4. The MST would be generated from pond data at publish time

This makes the pond a first-class participant in the AT Proto
federation. Blog posts, data reports, and attestation announcements
would appear directly in Bluesky feeds without cross-posting.

### The Signing Key Unification

All channels use the same Ed25519 keypair:

```
Ed25519 keypair (pond signing key)
  |
  |-- Signs tlog checkpoints (C2SP tlog-checkpoint)
  |-- Signs pond manifests (C2SP signed-note)
  |-- Signs blog posts (C2SP signed-note)
  |-- Signs AT Proto commits (AT Proto repo spec)
  |
  |-- Verifier key published in:
  |     satproto.json (discovery document)
  |     DID document (AT Proto identity)
  |     /tlog/manifest (pond manifest)
  |
  One identity across all channels.
  One key to verify everything.
```

Note: AT Proto uses `did:web` or `did:plc` for identity resolution.
A `did:web:casparwater.org` DID document would publish the same
Ed25519 public key, making it discoverable through both the s@
discovery document and AT Proto's DID resolution.

---

## Multi-Pond Verification Topology

The Caspar Water Company scenario involves multiple ponds producing
and consuming data. This section describes how attestation chains
work across the network.

### Pond Network

```
+-------------------+     +-------------------+     +-------------------+
| BeaglePlay        |     | Raspberry Pi      |     | Central Server    |
| (septic system)   |     | (water sensors)   |     | (dashboard)       |
|                   |     |                   |     |                   |
| pond: septic      |     | pond: water       |     | pond: dashboard   |
| partitions:       |     | partitions:       |     | partitions:       |
|   /ingest/        |     |   /ingest/        |     |   /               |
|   /system/...     |     |   /system/...     |     |   /sources/sep/   |
|                   |     |                   |     |   /sources/water/ |
| signs with:       |     | signs with:       |     |   /reduced/       |
|   key_septic      |     |   key_water       |     |   /blog/          |
|                   |     |                   |     |   /system/...     |
| publishes to:     |     | publishes to:     |     |                   |
|   S3 backup       |     |   S3 backup       |     | signs with:       |
|   (tiles + data)  |     |   (tiles + data)  |     |   key_dashboard   |
+--------+----------+     +--------+----------+     |                   |
         |                          |                | publishes to:     |
         |     cross-pond import    |                |   GitHub Pages    |
         +----------+   +----------+                |   (tiles + site)  |
                    |   |                            +-------------------+
                    v   v
              +-------------------+
              | Central Server    |
              | imports partitions|
              | from septic and   |
              | water ponds       |
              +-------------------+
```

### Attestation Chain Across Ponds

Each pond has its own signing key and produces its own checkpoints.
When data flows from a producer to a consumer, the attestation
chain preserves provenance:

**Step 1: Producer attests**

The septic BeaglePlay runs `pond attest`. This produces:
- Signed checkpoint for `/ingest/` partition (key_septic)
- Tiles stored locally and backed up to S3

**Step 2: Consumer imports**

The central server runs cross-pond import. It receives:
- The partition's OpLog records (parquet files)
- The partition's tlog tiles
- The producer's signed checkpoint

**Step 3: Consumer verifies import**

Before accepting the import, the consumer:
1. Recomputes leaf hashes from the imported OpLog records
2. Verifies they match the imported tiles
3. Verifies the tiles against the producer's signed checkpoint
4. Checks consistency with the previous imported checkpoint
   (if this is an incremental update)

If any step fails, the import is rejected.

**Step 4: Consumer attests its own state**

The central server runs `pond attest`. Its manifest includes:

```
casparwater.org/pond/dashboard
txn_seq=100
timestamp=2026-03-12T07:30:00Z

part_id=...  path=/               size=12   root=...  provenance=local
part_id=...  path=/sources/sep    size=142  root=...  provenance=imported  source=pond_septic  source_checkpoint=...
part_id=...  path=/sources/water  size=890  root=...  provenance=imported  source=pond_water   source_checkpoint=...
part_id=...  path=/reduced        size=50   root=...  provenance=local
part_id=...  path=/blog           size=8    root=...  provenance=local

-- casparwater.org/pond/dashboard ...signature...
```

The manifest records provenance for each partition. For imported
partitions, it includes:
- `source`: the source pond's identity
- `source_checkpoint`: the producer's checkpoint that was verified
  at import time

A verifier can follow this chain: dashboard's manifest references
the septic pond's checkpoint, which can be independently verified
against the septic pond's published tiles.

### Non-Repudiation Across the Chain

The measurement fidelity guarantee is layered:

| Layer | What it proves | Who is accountable |
|-------|---------------|--------------------|
| Sensor -> OpLog | BLAKE3 hash computed at write time | The device (BeaglePlay) |
| OpLog -> Tlog | SHA-256 tlog over all records | The producer pond operator |
| Tlog -> Checkpoint | Ed25519 signature over tree state | The producer's signing key |
| Import -> Manifest | Consumer verified producer's checkpoint | The consumer pond operator |
| Manifest -> GitHub | Git commit with signed content | GitHub (temporal witness) |

No single party can tamper with data without breaking the chain.
The producer can't silently modify old readings (tlog consistency
proofs). The consumer can't claim it received different data (it
verified the producer's checkpoint). The publisher can't present
data that differs from what was attested (tlog inclusion proofs).

### What Can't Be Proven

The attestation chain proves data integrity and provenance, but
does NOT prove (see also "What Device Attestation Adds" in the
Regulatory Compliance section for how device attestation narrows
some of these gaps):

- **Sensor accuracy**: a miscalibrated sensor produces authentic
  garbage. The attestation proves the garbage is exactly the garbage
  the sensor produced.
- **Sensor existence**: the attestation does not prove a physical
  sensor exists. A pond operator could generate synthetic data and
  attest to it. Device attestation with authority-issued certificates
  narrows this gap (see Regulatory Compliance section).
- **Completeness**: the attestation proves what IS in the log, not
  what SHOULD be in the log. If a sensor was offline for a day, the
  attestation faithfully reflects the gap -- but can't prove whether
  the gap was accidental or intentional.
- **Timeliness**: timestamps come from the device's clock. A device
  with a wrong clock produces correctly-signed data with wrong
  timestamps. Git commits provide an independent timestamp from
  GitHub's servers.

### Blog Post Authenticity

Blog posts carry two levels of authenticity:

**1. Post authenticity (Ed25519 signature)**

Each post is a signed note. The signature proves:
- The text was authored by the holder of the signing key
- The text has not been modified since signing
- The `tlog_ref` links to a specific data attestation

**2. Data authenticity (tlog attestation)**

The `tlog_ref` in a post links to a partition checkpoint. A reader
can verify:
- The referenced data is in the log (inclusion proof)
- The data has not been modified since the checkpoint was signed
- The log has only grown since any previous checkpoint they saw

**Combined**: a blog post that says "our leak detection data shows
X" is linked to the actual data, and both the post and the data
are cryptographically signed. A reader can verify the post is
authentic, the data is authentic, and the post's claims about the
data are verifiable.

### Social Network Reach with Verifiable Content

When a signed post is cross-posted to Bluesky or shared via RSS,
the social network post contains a link to the attested content.
The reach comes from the social network; the verification comes
from the static site.

```
Bluesky post (ephemeral reach):
  "We fixed the main distribution line leak. Flow data shows a
   47% reduction in overnight losses. Verified data:
   https://casparwater.org/posts/2026-03-10-leak.html
   #water #infrastructure"

Static page (permanent verification):
  - Full blog post text
  - Embedded charts from attested data
  - "Verify this data" button
  - Links to tlog checkpoint and tiles
  - Signed note version of the post

A skeptical reader clicks "Verify this data":
  1. Browser fetches checkpoint (signed note, ~200 bytes)
  2. Browser verifies Ed25519 signature
  3. Browser fetches 2-3 tiles (~24 KB)
  4. Browser computes inclusion proof for the data
  5. Green checkmark: "This data is cryptographically attested
     by casparwater.org since 2026-03-10"
```

The social network is the megaphone. The static site is the
courthouse. You shout from the megaphone and prove it in court.

---

## Relationship to Cross-Pond Import

The cross-pond import design (see `design-cross-pond-import.md`)
describes how one pond imports data from another. Attestation adds
a verification layer on top:

### Producer Pond (e.g., septic on BeaglePlay)

1. Collects sensor data
2. Computes BLAKE3 hashes (existing)
3. Builds merkle tree over attested scope (new)
4. Signs attestation (new)
5. Pushes backup to S3 (existing)
6. Publishes attestation to static site (new)

### Consumer Pond (e.g., central dashboard)

1. Imports data via cross-pond import (designed)
2. Verifies BLAKE3 hashes during transfer (existing)
3. **Can additionally verify** imported data against the producer's
   published attestation
4. Runs sitegen over imported + local data
5. Publishes its own attestation (covering both local and imported data)
6. Its attestation implicitly covers the imported data, creating a
   chain of attestations across ponds

### Trust Model

| What | Who trusts whom | How |
|------|----------------|-----|
| Transfer integrity | Consumer trusts BLAKE3 | ChunkedReader verification (existing) |
| Data authenticity | Anyone trusts producer's key | Ed25519 signature over merkle root (new) |
| Temporal claims | Anyone trusts git/GitHub | Git commit history (new) |
| Domain identity | Anyone trusts TLS/CA | HTTPS certificate (existing) |

The attestation does NOT prove the sensors are calibrated or the
readings are physically correct. It proves: "this specific data is
exactly what the pond operator's system recorded, and it has not been
modified since."

---

## Key Management

Key management is the hardest part of any signing system. This
section covers the full lifecycle from personal use through
regulated utility deployments. The design uses trait-based
abstraction so the same `pond attest` code works across all tiers.

### Design Principle: Abstract Signing from Day One

All signing operations go through the RustCrypto `signature` crate's
`Signer<ed25519::Signature>` trait. This means the caller never
touches key material directly -- it calls `signer.sign(message)` and
gets a signature back. The signer implementation determines where
and how keys are stored.

```
pond attest --> dyn Signer<ed25519::Signature>
                     |
           +---------+-----------+
           |                     |
      FileKeypair           HsmSigner
     (ed25519-dalek)     (PKCS#11 backend)
     personal use       utility deployment
```

This costs nothing in Tier 1 (you implement `FileKeypair` and move
on) but enables Tier 3 without changing any attestation code.

### Rust Crate Stack

All crates are Apache-2.0, MIT, or BSD-3 licensed:

| Crate | License | Purpose |
|-------|---------|---------|
| `signature` | Apache-2.0/MIT | Generic `Signer`/`Verifier` traits |
| `ed25519` | Apache-2.0/MIT | Ed25519 trait definitions |
| `ed25519-dalek` | BSD-3 | Software Ed25519 implementation |
| `signed_note` | Apache-2.0 | C2SP signed-note format |
| `tlog_tiles` | Apache-2.0 | Tlog tree + checkpoints |
| `cryptoki` | Apache-2.0/MIT | PKCS#11 HSM binding (Tier 3) |
| `secrecy` | Apache-2.0/MIT | Zeroize secrets in memory |

**Evaluated and rejected:**

| Crate | License | Reason for rejection |
|-------|---------|---------------------|
| `strong-box` | GPL-3.0 | Symmetric encryption (AEAD), not signing. GPL incompatible. Interesting `RotatingStrongBox` pattern for time-based key rotation, but wrong problem domain. |
| `ct-logs` | MIT | Only bundles CT root certificates, not a signing tool |
| `atlas-transparency-log` | MIT | Full server framework, heavier than needed |

### Tier 1: Personal / Development

**Who**: Individual pond operators, development, home monitoring.

**Key storage**: Ed25519 keypair in an encrypted file on disk,
unlocked via environment variable or OS keychain integration.

```bash
# Generate a keypair (one-time setup)
pond key generate --output ~/.pond/signing-key

# Public key is stored in pond config
pond config set signing_key_public "ed25519:<base64-public-key>"

# Private key is provided at runtime via environment
export POND_SIGNING_KEY_FILE=~/.pond/signing-key
# or inline (less secure, useful for CI):
export POND_SIGNING_KEY="base64-private-key"
```

**Key rotation trigger**: Ad hoc (compromise, hardware change).

**Rotation procedure**:
1. Generate new keypair
2. Sign a rotation note with old key endorsing new key
3. Publish new public key in discovery document
4. Old key moves to `previousKeys[]` in discovery document
5. Old attestations remain verifiable (old public key is published)

**Security properties**: Adequate for personal data integrity.
Not suitable for regulatory compliance.

### Tier 2: Small Organization

**Who**: Small water districts, community monitoring programs,
organizations with a few operators.

**Key storage**: Cloud KMS (AWS KMS, Azure Key Vault, GCP Cloud HSM).
The private key never leaves the cloud HSM -- signing happens
remotely via API call. The `Signer` trait wraps the KMS client.

**Key rotation trigger**: Policy-driven (e.g., annual) or
personnel change.

**Rotation procedure**:
1. KMS generates new key version
2. Double-sign checkpoints with old and new key during transition
   (C2SP signed-note natively supports multiple signatures)
3. Update discovery document and DID document
4. After transition period, retire old key version in KMS
5. KMS audit log records all key lifecycle events

**Additional requirements**:
- Access control: IAM roles determine who can sign
- Audit trail: KMS provides built-in logging
- Backup: KMS handles key durability

### Tier 3: Regulated Utility

**Who**: Municipal water systems (5000+ connections), utilities
subject to EPA/state regulatory reporting, environments requiring
FIPS 140-2 compliance.

**Key storage**: FIPS 140-2 Level 3 hardware security module (HSM)
via PKCS#11 interface. The `cryptoki` crate provides Rust bindings.

**Key rotation trigger**: Regulatory policy (e.g., annual),
personnel change, security incident, HSM lifecycle.

**Rotation procedure** (formal ceremony):
1. HSM generates new Ed25519 keypair internally
   (key material never exported)
2. Old key signs a transition checkpoint covering current log state
3. New key signs the same checkpoint (same tree size/hash)
4. Both signatures appear on the transition checkpoint note
5. Independent witnesses co-sign the transition checkpoint
6. Old key is retired in HSM audit log
7. Regulatory body receives rotation notification with both
   public keys and the transition checkpoint
8. Discovery document and DID document updated

**Additional requirements**:
- Multi-operator signing authority (quorum or role-based approval
  for attestation, not just one person)
- Independent witnesses (state environmental agency, third-party
  auditor, neighboring utility)
- Formal audit trail (HSM + pond control table + witness records)
- Key escrow or recovery plan (what happens if HSM fails)
- Separation of duties (key custodian != data operator)

### Key Rotation: How C2SP Signed-Note Handles It

The signed-note format was designed for multi-key scenarios. A
checkpoint note can carry multiple signatures:

```
mywater.example.com/partition-abc
42
sha256:h8fRk...

-- mywater.example.com/key2024 <base64(keyID || sig)>
-- mywater.example.com/key2026 <base64(keyID || sig)>
-- witness.stateagency.gov <base64(keyID || sig)>
```

**Verifiers ignore signatures from unknown keys** (per C2SP spec).
This means rotation requires no protocol changes:

1. **Start**: Add new key's signature to every checkpoint
2. **Transition**: Clients learn about new key (update trust config)
3. **End**: Stop signing with old key

During transition, any verifier works -- those knowing only the old
key verify against the old signature, those knowing the new key
verify against the new one. There is no moment of breakage.

### Key Lifecycle Events

Every key lifecycle event should be recorded in the pond's control
table (the steward audit log) and optionally published:

| Event | Recorded in | Published to |
|-------|------------|-------------|
| Key generation | Control table | Discovery document |
| Key activation | Control table | First signed checkpoint |
| Key rotation (start) | Control table | Transition checkpoint (double-signed) |
| Key rotation (end) | Control table | Discovery `previousKeys[]` |
| Key compromise | Control table | Advisory notice (out-of-band) |
| Key retirement | Control table | Discovery `previousKeys[]` with `retired_at` |

### Discovery Document Key Fields

The discovery document (see Discovery Document section) tracks the
full key history:

```json
{
  "pond_id": "uuid",
  "current_key": {
    "algorithm": "ed25519",
    "public_key": "base64...",
    "key_id": "base64(first-4-bytes-of-sha256)",
    "activated_at": "2026-01-15T00:00:00Z"
  },
  "previous_keys": [
    {
      "algorithm": "ed25519",
      "public_key": "base64...",
      "key_id": "base64...",
      "activated_at": "2024-06-01T00:00:00Z",
      "retired_at": "2026-01-15T00:00:00Z",
      "retirement_reason": "scheduled_rotation"
    }
  ],
  "key_rotation_policy": "annual",
  "witnesses": [
    {
      "name": "witness.stateagency.gov",
      "public_key": "base64...",
      "role": "regulatory_witness"
    }
  ]
}
```

### What NOT to Build

- **Custom key management system**: Use the `signature` trait +
  existing backends (ed25519-dalek, cryptoki, cloud KMS SDKs).
- **Custom rotation protocol**: C2SP signed-note already handles it
  via multi-signature support.
- **Custom witness infrastructure**: Use existing witness networks
  (e.g., `witness.sigsum.org`) or simple cosigning when ready.
- **HSM integration now**: Just ensure the `dyn Signer` trait
  boundary exists. Plug in HSM backends when a Tier 3 deployment
  materializes.
- **Automatic key rotation**: For signing keys (unlike encryption
  keys), scheduled rotation adds complexity with limited benefit.
  Rotate on policy or event, not on a timer.

### Comparison: Signing Key Rotation vs Encryption Key Rotation

The `strong-box` crate (GPL-3.0, evaluated but not adopted)
demonstrates an interesting pattern for symmetric encryption:
`RotatingStrongBox` auto-rotates encryption keys on a time schedule
and retains old keys for decryption of old ciphertexts. This works
because encryption is bidirectional -- you need the key to decrypt.

Signing is fundamentally different:
- **Encryption rotation** is about limiting exposure (fewer
  ciphertexts per key = harder to attack)
- **Signing rotation** is about operational security (compromise
  response, personnel changes, policy compliance)
- Old signatures remain **permanently verifiable** with the old
  *public* key (which is freely published). There is no "old key
  needed for decryption" problem.
- Frequent rotation of signing keys adds complexity (more keys to
  track, more transition checkpoints) without proportional security
  benefit, since the public key is not secret.

**Recommendation**: Rotate signing keys on events (compromise,
personnel, policy) not on timers. For Tier 3 regulated deployments,
an annual rotation policy satisfies compliance without excessive
operational burden.

---

## Regulatory Compliance and Device Attestation

This section addresses the end-to-end trust problem for regulated
water systems: from the sensor device on a well pump or chlorine
injector, through the data pipeline, to the monthly compliance
report submitted to the state water board. The goal is a system
where every link in the chain is cryptographically attested, and
the state agency has tools to verify compliance without trusting
the operator's assertions alone.

### The Regulatory Context

Under the Safe Drinking Water Act (SDWA) and state primacy agency
rules, public water systems must:

- **Monitor** regulated parameters at specified frequencies
  (coliform bacteria monthly, chlorine residual daily/weekly,
  turbidity continuously for surface water, plus ~15 other chemical
  and radiological parameters at various frequencies)
- **Report** results to the state primacy agency (e.g., California
  State Water Resources Control Board, Oregon Health Authority)
- **Publish** annual Consumer Confidence Reports (CCRs) to customers
- **Retain records** for regulatory audit (typically 5-12 years)

Current reporting is largely paper-based or via state-specific
electronic data portals (e.g., Ohio eDWR, EPA SDWIS). There is
no cryptographic verification -- the state trusts that the operator
is reporting truthfully. An operator who wanted to misreport could
simply submit different numbers.

**DuckPond's opportunity**: provide a tool that makes it *harder
to lie than to tell the truth* -- where the default workflow
produces a verifiable chain from sensor to report, and tampering
requires breaking cryptographic proofs.

### Acceptance Test

The acceptance criterion for this design is concrete:

> A small water company (e.g., 125 connections) operates DuckPond
> on a BeaglePlay at each well site. The system continuously
> collects chlorine residual, pH, turbidity, pressure, flow rate,
> temperature, and ~10 additional parameters at one-minute
> intervals. At month-end, `pond report --month 2026-03` generates
> a signed compliance report containing:
>
> 1. Summary statistics per parameter (min, max, mean, P95)
> 2. Any exceedance events with timestamps
> 3. Total water production (gallons)
> 4. A tlog checkpoint proving the underlying data is append-only
>    and untampered
> 5. An Ed25519 signature from the operator's attested signing key
> 6. Device attestation certificates for each sensor node
>
> The state water board can verify this report using `pond verify`
> or a web-based verifier, without accessing the raw data, and be
> assured that the operator did not fabricate or alter readings.

### Trust Chain: Sensor to Report

The verification problem has four layers, each requiring different
cryptographic machinery:

```
Layer 4: REPORT ATTESTATION
  "This report accurately summarizes the data"
  --> Operator signs the report with Ed25519 key
  --> Report embeds tlog checkpoint reference
  --> Verifier can check: report matches attested data

Layer 3: DATA ATTESTATION (existing tlog design)
  "This data is append-only and unmodified"
  --> Per-partition tlog with signed checkpoints
  --> Inclusion proofs for any record
  --> Consistency proofs between reports

Layer 2: PIPELINE ATTESTATION
  "This data traveled from the device to the pond unmodified"
  --> Cross-pond import with signed partition checkpoints
  --> BLAKE3 content hashes end-to-end
  --> Git timestamps as independent timeline

Layer 1: DEVICE ATTESTATION
  "This data came from THIS specific registered sensor"
  --> Device identity bound to hardware root of trust
  --> Device registered with authority (state water board)
  --> Each data batch signed by device key
```

Layers 2-4 are addressed by the existing design. Layer 1 -- device
attestation -- is the new capability described here.

### Device Identity and Hardware Roots of Trust

A monitoring device (e.g., BeaglePlay running DuckPond at a well
site) needs a cryptographic identity that is:

- **Bound to hardware**: the private key cannot be extracted and
  used on a different device
- **Verifiable**: a remote party can confirm the key belongs to
  a specific physical device
- **Registered**: the state water board has a record mapping
  the device's public key to a specific meter/sensor installation

**Hardware options for embedded Linux devices:**

| Platform | Root of Trust | Key Storage | Rust Support |
|----------|--------------|-------------|-------------|
| TI AM625 (BeaglePlay) | TI Foundational Security (TIFS) HSM | On-chip secure enclave | Via OP-TEE + PKCS#11 |
| External TPM 2.0 chip | TPM Endorsement Key | TPM non-volatile storage | `tss-esapi` crate (Apache-2.0) |
| Secure Element (e.g., ATECC608) | Factory-provisioned key | Tamper-resistant IC | `cryptoauthlib` FFI bindings |
| Software-only (dev/Tier 1) | None (file on disk) | Filesystem | `ed25519-dalek` |

**Recommended approach**: For production Tier 3 deployments, use
an external TPM 2.0 chip connected via I2C or SPI. The BeaglePlay
has mikroBUS and Grove expansion headers suitable for this. The
`tss-esapi` crate (from the Parsec project, Apache-2.0 licensed)
provides safe Rust bindings to TPM 2.0 via `tpm2-tss`.

For Tier 1/2, software keys are acceptable (the device is under
the operator's physical control and the primary threat is
data tampering, not device impersonation).

### Device Attestation Flow

```
PROVISIONING (one-time, at installation):

  1. Device generates Ed25519 keypair in TPM/secure enclave
     (key material never leaves hardware)

  2. Device produces a Certificate Signing Request (CSR)
     containing:
     - Device public key
     - Device serial number / hardware ID
     - Installation location (GPS or address)
     - Sensor type and calibration date

  3. Water system operator signs the CSR with their
     operator key (the pond's Ed25519 key)

  4. Operator submits the device registration to the
     state water board registration authority

  5. Registration authority issues a device certificate:
     - Binds device public key to a registered meter ID
     - Signed by the authority's key
     - Includes validity period and sensor class

  6. Device certificate stored on device and in operator's
     pond (as metadata in the device's partition)

OPERATION (continuous):

  1. Device collects sensor readings at configured interval

  2. Device signs each data batch with its device key
     (the key bound to hardware)

  3. Data batch includes:
     - Sensor readings (Parquet table:series)
     - Timestamp range
     - Device certificate reference
     - BLAKE3 hash of the batch content
     - Device signature over the above

  4. Data batch is written to the device's local pond
     partition and replicated to the central pond via
     the remote factory (cross-pond import)

  5. Central pond verifies device signature on import
     (Layer 2 pipeline attestation)

REPORTING (monthly or as required):

  1. `pond report` aggregates data from all device partitions

  2. Report includes device attestation chain:
     - For each sensor: device certificate + sample of
       device-signed batches
     - Operator attestation: tlog checkpoint covering
       the reporting period
     - Operator signature on the report

  3. State water board verifies:
     - Device certificates are valid (signed by authority)
     - Device signatures on data batches are valid
     - Operator tlog checkpoint is consistent with
       previous submissions
     - Report statistics match the attested data
```

### The State Water Board as Registration Authority

The state water board (or its delegate) operates as a lightweight
certificate authority for water monitoring devices. This is NOT a
full X.509 PKI -- it is a purpose-built registration system using
the same Ed25519 + signed-note primitives as the rest of DuckPond.

**Authority infrastructure (what the water board runs):**

```
Registration Authority Pond
|
+-- /devices/
|   +-- {device-id}/
|       +-- registration.json     (device metadata, location, class)
|       +-- certificate.signed    (signed note: authority vouches for device)
|       +-- calibration-history/  (calibration records)
|
+-- /operators/
|   +-- {operator-id}/
|       +-- registration.json     (operator metadata, system ID, permit #)
|       +-- public-key.pem        (operator's verifier key)
|       +-- devices/              (symlinks to registered devices)
|
+-- /reports/
|   +-- {operator-id}/
|       +-- {period}/
|           +-- submission.signed (the operator's signed report)
|           +-- verification.log  (authority's verification results)
|
+-- /authority/
    +-- signing-key              (authority's Ed25519 key, HSM-backed)
    +-- device-registry.tlog     (transparency log of all registrations)
    +-- revocations/             (revoked device certificates)
```

**Key insight**: The registration authority itself uses a
transparency log for device registrations. This means:

- All device registrations are append-only and auditable
- An operator can verify their devices are correctly registered
- A third party can audit the full registry
- Revocations are logged (a revoked device can't be silently
  un-revoked)

**The authority's tlog is structurally identical to a partition
tlog.** The same `tlog_tiles` + `signed_note` code runs on both
sides. DuckPond is the tool for both the operator AND the regulator.

### Device Certificate Format

A device certificate is a signed note (C2SP signed-note format):

```
caspar-well-1.device.casparwater.org
device_type: chlorine_analyzer
device_serial: HV-2024-0847
installation: well_1_chlorine
location: 38.9847,-123.7891
calibrated: 2026-01-15
operator: casparwater.org
operator_key: ed25519:abc123...
registered: 2026-01-20
valid_until: 2027-01-20
sensor_class: continuous_online

-- ca.waterboard.ca.gov <base64(keyID || authority_signature)>
-- casparwater.org <base64(keyID || operator_cosignature)>
```

The certificate carries two signatures:
1. The registration authority (state water board) vouching for
   the device's registration
2. The operator co-signing to confirm the device is theirs

### What Device Attestation Adds to "What Can't Be Proven"

The existing "What Can't Be Proven" section (above) lists four
limitations. Device attestation addresses two of them partially:

| Limitation | Without device attestation | With device attestation |
|-----------|--------------------------|----------------------|
| Sensor existence | Cannot prove a physical sensor exists | The device certificate, signed by the authority after a site inspection, proves a sensor WAS installed. It does not prove the sensor is STILL installed or functioning. |
| Sensor accuracy | Cannot prove sensor is calibrated | The device certificate records the calibration date. Combined with calibration history in the authority's registry, there is a verifiable calibration chain. Still cannot prove accuracy between calibrations. |
| Completeness | Cannot prove data isn't missing | Unchanged. A device that goes offline produces a gap. The tlog faithfully records the gap. |
| Timeliness | Timestamps come from device clock | With TPM-backed attestation, the device clock can be cross-referenced against NTP and the tlog append order. Still not proof of correct time, but more anchors. |

**Device attestation does NOT make these problems fully solvable.**
It narrows the trust gap from "the operator says they have sensors"
to "the authority inspected and registered sensors, and the sensors
are signing data with hardware-bound keys." The remaining gap
(calibration drift, sensor failure, clock skew) requires physical
auditing, which is outside the scope of cryptographic systems.

### The Operator-Authority Relationship

Both sides run DuckPond. Both sides benefit from the same tooling:

```
OPERATOR (small water company)          AUTHORITY (state water board)
|                                       |
| pond (on BeaglePlay at well)          | pond (registration authority)
| +-- /sensors/chlorine/                | +-- /devices/{dev-id}/
| |   (table:series, signed by device)  | |   (device certificate, calibration)
| +-- /sensors/flow/                    | +-- /operators/{op-id}/
| |   (table:series, signed by device)  | |   (operator registration, public key)
| +-- /system/etc/remote/              | +-- /reports/{op-id}/{period}/
| |   (replicate to central)            | |   (submitted reports, verification)
| +-- /reports/                         | +-- /authority/
|     (generated compliance reports)    |     (signing key, device registry tlog)
|                                       |
| pond attest                           | pond verify --submission {report}
| pond report --month 2026-03           | pond device register {csr}
| pond run remote push                  | pond device revoke {device-id}
|                                       | pond device list --operator {op-id}
```

**New CLI commands for the authority role:**

| Command | Role | Purpose |
|---------|------|---------|
| `pond device register` | Authority | Register a device, issue certificate |
| `pond device revoke` | Authority | Revoke a device certificate |
| `pond device list` | Both | List registered devices |
| `pond device inspect` | Both | Show device certificate and status |
| `pond report generate` | Operator | Generate compliance report for a period |
| `pond report submit` | Operator | Submit report to authority |
| `pond report verify` | Authority | Verify a submitted report |

### Report Generation

`pond report generate --month 2026-03` produces a signed compliance
report by querying the pond's time-series data:

```sql
-- Example: monthly chlorine summary (one of ~16 parameters)
SELECT
  date_trunc('day', timestamp) as date,
  min(value) as min_cl2,
  max(value) as max_cl2,
  avg(value) as mean_cl2,
  approx_percentile(value, 0.95) as p95_cl2,
  count(*) as sample_count,
  count(CASE WHEN value < 0.2 THEN 1 END) as below_minimum
FROM source
WHERE timestamp >= '2026-03-01' AND timestamp < '2026-04-01'
GROUP BY date_trunc('day', timestamp)
ORDER BY date
```

The report includes:
1. Summary statistics per parameter per day
2. Exceedance events (readings outside regulatory limits)
3. Total production/consumption volumes
4. Data completeness metrics (expected vs actual readings)
5. Device attestation references (certificate + sample signatures)
6. Tlog checkpoint for the reporting period
7. Operator signature over the entire report

The report format is a signed note containing structured data
(likely CBOR or JSON), with the tlog checkpoint embedded as a
cross-reference.

### Rust Crate Stack for Device Attestation

| Crate | License | Purpose |
|-------|---------|---------|
| `tss-esapi` | Apache-2.0 | TPM 2.0 ESAPI bindings (Parsec project) |
| `tss-esapi-sys` | Apache-2.0 | Low-level FFI to `tpm2-tss` C library |
| `ed25519-dalek` | BSD-3 | Software Ed25519 for non-TPM devices |
| `signature` | Apache-2.0/MIT | Trait abstraction (`dyn Signer`) |
| `x509-cert` | Apache-2.0/MIT | X.509 certificate handling (if needed) |
| `der` | Apache-2.0/MIT | ASN.1 DER encoding/decoding |

**Note**: The `tss-esapi` crate requires the `tpm2-tss` C library
to be installed on the target system. For cross-compilation to ARM
(BeaglePlay), the `tss-esapi-sys` crate supports bundled builds.
This is a compile-time dependency, not a runtime service.

### Tiered Device Attestation

Like key management, device attestation has tiers:

**Tier 1 -- Software identity (personal/dev)**:
- Device generates Ed25519 keypair in a file
- No hardware binding (key can be copied to another device)
- Suitable for development and personal monitoring
- Provides data integrity but not device identity

**Tier 2 -- Platform identity (small organization)**:
- Device uses TI AM625's TIFS secure enclave or OP-TEE
- Key generated in Trusted Execution Environment
- Key is bound to the specific SoC (harder to extract)
- No external attestation of device identity

**Tier 3 -- Certified identity (regulated utility)**:
- External TPM 2.0 chip with Endorsement Key (EK)
- Device key generated in TPM (non-exportable)
- TPM vendor certificate proves key is in genuine TPM
- State water board registers TPM-backed device key
- Full attestation chain: TPM vendor --> device --> operator --> authority

### Open Questions (Device Attestation)

**Q9: Device Certificate Format**

Should device certificates use the C2SP signed-note format (as
shown above) or standard X.509 certificates? Signed notes are
simpler and consistent with the rest of the design. X.509 is
the industry standard for device PKI and has broader tooling
support.

**Proposed**: Start with signed-note format for consistency.
Add X.509 export capability for interoperability with existing
water board systems that expect standard certificates.

**Q10: Authority Infrastructure Scope**

How much of the registration authority infrastructure should
DuckPond implement? Options:

A. **Minimal**: `pond device register` and `pond device verify`
   commands. The authority manages certificates as signed notes
   in their own pond. No central server.

B. **Moderate**: Add a device registry tlog. The authority runs
   a DuckPond instance with a transparency log of all registered
   devices. Operators can verify their registrations.

C. **Full**: Authority runs a web service with REST API for
   device registration, revocation checking (OCSP-like), and
   certificate distribution. Operators interact programmatically.

**Proposed**: Start with **A**, design for **B**. The beauty of
using DuckPond for both sides is that the authority's device
registry IS a pond, and it gets all the same attestation properties.
A full web service (C) may be needed eventually but should not
be a prerequisite.

**Q11: Offline Device Operation**

Water monitoring devices may lose connectivity for hours or days
(rural well sites, cellular dead zones). How does device attestation
work offline?

**Proposed**: Device signs data batches locally using its TPM-backed
key. Batches accumulate in the device's local pond. When connectivity
resumes, batches are replicated to the central pond with their
device signatures intact. The central pond verifies signatures
on import. The tlog faithfully records the temporal gap in
replication (not in data collection).

**Q12: Calibration Verification**

Can the calibration chain be integrated into the device attestation?

**Proposed**: The authority's device record includes a calibration
history (dates, standards used, results). Each calibration event
is appended to the device's tlog in the authority's registry.
An expired calibration does not revoke the device certificate but
flags a warning in report verification. This mirrors current
regulatory practice where overdue calibration is a compliance
finding, not a data rejection.

---

## Implementation Phases

The pragmatic approach: three core phases to get the basic loop
working, then extend. Ship `pond attest` + `pond verify` + sitegen
tiles before tackling witnesses, Delta integration, or cross-pond
verification.

### Phase 1: pond attest

Get the core tlog working end-to-end for one partition.

- Add `tlog_tiles` and `signed_note` crates as dependencies
  (from Cloudflare's Azul project). These use SHA-256 and Ed25519
  per the C2SP specs -- use them as-is, no custom hasher.
- Define leaf data format: serialize OpLog record fields (part_id,
  node_id, file_type, version, blake3_content, name, txn_seq) as
  the leaf input to `SHA-256(0x00 || leaf_data)`
- Define on-disk tlog tile storage layout within the pond:
  `{POND}/tlog/part/{part_id}/tile/...` and
  `{POND}/tlog/part/{part_id}/checkpoint`
- Key generation: `pond config generate-signing-key`
  - Generates Ed25519 keypair using `signed_note::generate_key`
  - Stores verifier key (public) in pond config
  - Prints signer key (private) for the operator to store securely
  - Key format: C2SP `PRIVATE+KEY+<name>+<id>+<keydata>`
- `pond attest` command:
  1. Scans partitions in the Delta table
  2. For each partition, collects all OpLog records in order
  3. Computes leaf hashes from record fields
  4. Builds the tlog tile tree, writes tiles to storage
  5. Signs a checkpoint per partition (C2SP tlog-checkpoint)
  6. Signs a pond-level manifest listing all partition checkpoints
- Unit tests: build a tlog, add records, verify tiles grow
  append-only, verify the checkpoint signature

### Phase 2: pond verify

Verification against signed checkpoints.

- `pond verify` command with two modes:
  - **Full audit**: recompute all leaf hashes from OpLog records,
    rebuild tiles, compare against signed checkpoint root
  - **Consistency check**: given a cached previous checkpoint,
    verify the current checkpoint is a consistent extension
    (tree size only grew, old root is a prefix of new root)
- Cache the latest checkpoint per partition in pond metadata.
  Refuse to accept a checkpoint with a smaller tree size
  ("complain loudly and fail" per Russ Cox's sumdb design)
- Inclusion proof: given a specific OpLog record, compute the
  O(lg N) proof that it is in the log
- Test: attest, add records, attest again, verify consistency
  between old and new checkpoints. Tamper with a tile, verify
  that `pond verify` catches it.

### Phase 3: Sitegen + Publishing

Publish tiles as static files alongside the existing site.

- Copy tlog tiles + checkpoints to sitegen output (`dist/tlog/...`)
- RSS/Atom feed generation from blog posts
- Signed blog posts (C2SP signed-note format)
- Discovery document generation (`satproto.json` with verifier key
  and tlog endpoint)
- `pond run site.yaml publish` -- git commit + push to GitHub Pages
- Tiles are immutable static files -- perfect for CDN caching. Only
  the rightmost partial tiles and checkpoint change per publish.
- Verification page (HTML + client-side WASM/JS verifier)

### Future Phases (after the basic loop works)

These are ordered by value, not by dependency:

**AT Proto Cross-Posting** -- `pond run site.yaml publish --bluesky`
creates Bluesky posts via the AT Proto API, linking to the attested
static page. Requires a Bluesky account and app password stored
securely (not in the pond). Summary text + link facet + embed card.
Simple API call -- no PDS needed.

**Device Attestation** -- `pond device register` for the authority
role, device key generation via `tss-esapi` for TPM-backed devices.
Device-signed data batches with hardware-bound keys. Device
certificates in C2SP signed-note format. See the Regulatory
Compliance and Device Attestation section for the full design.

**Compliance Reporting** -- `pond report generate --month 2026-03`
aggregates time-series data into regulatory compliance reports.
Reports embed tlog checkpoint references and device attestation
chains. `pond report verify` for the authority role to validate
submissions. SQL-based parameter aggregation using DataFusion.

**Delta Lake Integration** -- Store partition tlog checkpoints in
Delta commit metadata. Record BLAKE3 hash of each parquet file in
Delta `add` action tags. Extend `pond verify` to walk the Delta log
and check checkpoint consistency across commits. This closes the
Delta Lake verification gap.

**Cross-Pond Attestation** -- Producer pond publishes per-partition
tlog (tiles + checkpoint). Consumer pond imports partition tlog
alongside data and verifies against producer's checkpoint. Consumer
manifest records provenance with `source` and `source_checkpoint`
fields. Transitive attestation chains across ponds.

**Witness Integration** -- Implement C2SP tlog-witness protocol.
An independent server co-signs checkpoints after verifying
consistency. A neighbor or regulator could run a witness. Note: for
a single-writer system like DuckPond, Git already serves as a
partial witness (GitHub maintains an independent append-only history
of the published tiles).

**Authority Device Registry** -- The state water board runs a
DuckPond instance as a device registration authority. Device
registrations are logged in a transparency log (same tlog-tiles
infrastructure). Operators can verify their devices are correctly
registered. Revocations are logged and auditable.

**AT Proto PDS** -- DuckPond acts as a minimal Personal Data Server.
Register a `did:web` DID, serve pond content as AT Proto records,
implement `com.atproto.sync.*` endpoints for relay indexing. The
pond's Ed25519 key signs AT Proto commits. Most ambitious future
phase -- depends on the AT Proto ecosystem stabilizing around the
static/self-hosted PDS use case.

---

## Open Questions

### Q1: Attestation Granularity

~~Should attestations cover the entire pond, or specific scopes?~~

**DECIDED: Per-partition.** The partition is the natural unit:
partitions move between ponds (cross-pond import), are backed up
independently, and map 1:1 to directories. The pond-level
attestation is a signed manifest of partition tlog checkpoints,
not a single monolithic tree.

### Q2: Delta Lake Log Integrity

How far should we go to verify the Delta Lake log itself?

**Proposed**: Three levels, implemented incrementally:

1. **Parquet file checksums in add actions** (Phase 4) -- Each
   `add` action records the BLAKE3 hash of the parquet file it
   references. A verifier can re-hash the file and compare.

2. **Partition tlog checkpoints in commit metadata** (Phase 4) --
   Each commit records the tlog checkpoint (tree size + root) for
   all modified partitions. A verifier can check consistency
   between successive commits using tlog consistency proofs.

3. **Commit chain hashing** (Phase 4) -- Each commit metadata
   includes the BLAKE3 hash of the previous commit's JSON. This
   makes the Delta log itself a hash chain, detectable if commits
   are deleted or reordered.

Level 1-2 detect content tampering. Level 3 detects log tampering.
Together they close the gap between "Delta Lake provides ACID" and
"Delta Lake provides tamper evidence."

### Q3: Attestation Storage

Where do tlog tiles and checkpoints live?

**Option A**: In the pond (e.g., `{POND}/tlog/part/{part_id}/...`).
Backed up with everything else. Self-contained.

**Option B**: Only in the published output (git repo). The pond
doesn't store tiles -- they're a publication artifact.

**Option C**: Both. The pond stores the tlog tiles (needed for
incremental updates and consistency proofs), and sitegen publishes
them as static files.

Leaning toward **C**: the pond is the source of truth for the
append-only log, and publication makes it publicly verifiable.

### Q4: s@ and AT Proto Compatibility

How closely should we follow the s@ and AT Protocol specs?

**Proposed multi-channel approach:**

- **s@**: Use the discovery document structure and post file layout.
  Publish plaintext signed notes instead of encrypted blobs. Skip
  encryption, follow management, and key exchange. An s@ client
  that supports plaintext posts can aggregate our feed.

- **AT Proto**: Cross-post summaries to Bluesky via API (channel 4)
  for reach, linking to the attested static page. Longer term,
  consider acting as a minimal PDS (channel 5) using `did:web` so
  the pond is a first-class AT Proto participant.

- **C2SP signed-note**: The universal envelope for all signed
  content. Checkpoints, manifests, blog posts, and signed data
  exports all use this format. Compatible with the tlog ecosystem,
  witnesses, and monitors.

The three specs are complementary, not competing: C2SP for
cryptographic rigor, s@ for static social presence, AT Proto for
mainstream social reach.

### Q5: Verification UX

How does a non-technical person verify data?

**Proposed**: The published site includes a `/verify` page with
client-side JavaScript/WASM that:

1. Fetches the signed checkpoint for a partition
2. Fetches the verifier key from the discovery document
3. Lets the user pick a record to verify
4. Downloads the relevant tiles (2-3 tile fetches, ~24 KB total)
5. Computes the inclusion proof client-side
6. Verifies the Ed25519 signature
7. Shows a green checkmark or red X

The tile-based approach makes this practical for browsers: the
verification requires kilobytes of downloads, not the entire dataset.

### Q6: Relationship to Certificate Transparency / Witnesses

~~Should we integrate with existing timestamping infrastructure?~~

**Partially answered by tlog-tiles adoption.** By adopting the C2SP
specifications, DuckPond's transparency logs are structurally
compatible with the CT ecosystem. The path to stronger assurance:

1. **Git timestamps** (Phase 6) -- reasonable first approximation
2. **Witness co-signing** (Phase 8) -- an independent witness
   verifies checkpoint consistency and co-signs, providing
   split-view detection. The `siglog` crate includes a witness
   server implementation.
3. **CT-style gossip** -- multiple witnesses sharing checkpoints
   to detect if the log operator shows different views to different
   parties. This is the gold standard for transparency.

The witness model is more practical for DuckPond than RFC 3161
timestamps. A neighbor, a regulator, or a community group could
run a witness that co-signs the water company's checkpoints,
providing independent assurance without trusting a centralized
timestamping authority.

### Q7: BLAKE3 vs SHA-256 Interoperability

~~The C2SP specifications use SHA-256. We propose BLAKE3.~~

**DECIDED: SHA-256 for the tlog layer, BLAKE3 for content.** These
are separate layers. The tlog (tiles, tree hashing, checkpoints) uses
SHA-256 per C2SP/RFC 6962 so that standard clients, monitors, and
witnesses work without modification. Content hashing (OpLog records,
file integrity, bao-tree) continues to use BLAKE3. The leaf data
contains the BLAKE3 content hash, so verifiers get both.

Performance is not a concern: tlog tree hashing operates on kilobytes
of hash values, where SHA-256 and BLAKE3 are both effectively instant.
BLAKE3's speed advantage matters for content hashing (megabytes of
file data), which remains unchanged.

### Q8: Tile Height

The C2SP spec uses tiles of height 8 (256 hashes per tile, 8 KB
with SHA-256, 8 KB with BLAKE3 since both are 32-byte hashes).
This is a reasonable default. But for small ponds with few records,
a smaller tile height (e.g., 4, giving 16 hashes per tile) might
be more appropriate to avoid mostly-empty tiles.

**Proposed**: Use height 8 (the standard). Small ponds will have
mostly-partial tiles, which is fine -- the partial tile format
handles this gracefully. Consistency with the wider tlog ecosystem
is worth more than minor space savings.
