# Large File Parquet Storage: Implementation Analysis

**Date:** January 1, 2026  
**Status:** Analysis & Recommendation

## User Request

> "We should be able to literally copy the large-file parquet encodings into the remote as they have the correct schema. We should be able to literally restore the same large-file encodings to the _large_files area for pond replicas."

## Current Architecture

```
tlogfs writes: _large_files/sha256=X → raw binary file
remote backup: reads raw → chunks → parquet → Delta Lake
remote restore: reads parquet → reconstructs → raw binary file
```

## Desired Architecture

```
tlogfs writes: _large_files/blake3=X/chunks.parquet → chunked parquet
remote backup: copies parquet file → Delta Lake (no re-chunking)
remote restore: copies parquet file → _large_files/ (no reconstruction)
```

## Implementation Challenges

### Challenge 1: Circular Dependencies

**Current:**
- `remote` depends on `tlogfs`
- `tlogfs` does NOT depend on `remote`

**Problem:**
- `ChunkedWriter` lives in `remote`
- Can't use it in `tlogfs` without circular dependency

**Solutions:**

**Option A: Move Chunking to `utilities` Crate**
```
utilities/
  chunked_files.rs  ← ChunkedWriter, ChunkedReader
    ↓
tlogfs/        remote/
  uses             uses
```

**Pros:**
- Clean separation of concerns
- Reusable across crates
- Single source of truth for chunking logic

**Cons:**
- Adds heavy dependencies to `utilities` (bao-tree, blake3, arrow, parquet, deltalake)
- `utilities` becomes less "utility-like"
- More complex build

**Option B: Duplicate Chunking Logic**

Copy chunking code into `tlogfs`.

**Pros:**
- No dependency changes
- Simple build

**Cons:**
- Code duplication
- Maintenance burden (must keep in sync)
- Violates DRY principle

**Option C: Keep Current, Add Optimization Layer**

Keep raw binary files locally, but add "fast path" for already-remote files.

**Pros:**
- Minimal changes
- No dependency issues
- Backward compatible

**Cons:**
- Doesn't achieve "literal copy" goal
- Still has chunking overhead on first backup

**Option D: Make `tlogfs` Depend on `remote`**

Add `remote` as dependency to `tlogfs`.

**Pros:**
- Can use ChunkedWriter directly
- No code duplication

**Cons:**
- **Circular dependency**: `remote` already depends on `tlogfs`!
- Won't compile

## Recommended Solution: Move to `utilities`

Despite the dependency concerns, this is the cleanest long-term solution.

### Step 1: Update `utilities/Cargo.toml`

```toml
[dependencies]
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
parquet = { workspace = true }
bao-tree = { workspace = true }
blake3 = { workspace = true }
tokio = { workspace = true }
futures = { workspace = true }
log = { workspace = true }
# ... existing dependencies
```

### Step 2: Move Chunking Code

Extract from `remote/src/writer.rs` and `remote/src/reader.rs`:
- `ChunkedWriter` → `utilities/src/chunked_files.rs`
- `ChunkedReader` → `utilities/src/chunked_files.rs`
- Helper functions (combine_chunk_hashes, etc.)

### Step 3: Update Dependencies

```toml
# tlogfs/Cargo.toml
[dependencies]
utilities = { workspace = true }

# remote/Cargo.toml
[dependencies]
utilities = { workspace = true }  # Already has this
```

### Step 4: Update Code

**tlogfs:**
```rust
use utilities::chunked_files::{ChunkedWriter, ChunkedReader};

pub async fn write_large_file_parquet(
    pond_path: &str,
    content: &[u8],
    pond_txn_id: i64,
) -> Result<String> {
    let path = format!("_large_files/file-{}", pond_txn_id);
    let reader = std::io::Cursor::new(content);
    
    let writer = ChunkedWriter::new(pond_txn_id, path, reader);
    let parquet_bytes = writer.write_to_bytes().await?;
    let root_hash = writer.root_hash();
    
    // Write to disk
    let file_path = large_file_parquet_path(pond_path, &root_hash);
    tokio::fs::create_dir_all(file_path.parent().unwrap()).await?;
    tokio::fs::write(&file_path, &parquet_bytes).await?;
    
    Ok(root_hash)
}
```

**remote:**
```rust
use utilities::chunked_files::{ChunkedWriter, ChunkedReader};

// backup: check if file is already parquet, if so copy directly
// restore: write parquet directly
```

## Alternative: Simpler Hybrid Approach

If moving to `utilities` is too invasive, consider:

**Store metadata about parquet location in OpLog, but keep files in a shared cache**

```
_large_files/
  cache/
    blake3=X/chunks.parquet  ← shared parquet files
```

On backup:
1. Check if `blake3=X/chunks.parquet` exists
2. If yes: copy to remote (fast path)
3. If no: read raw, chunk, write parquet to cache, then copy to remote

On restore:
1. Write parquet to cache: `blake3=X/chunks.parquet`
2. OpLog references the BLAKE3 hash

This gives most benefits without major refactoring.

## Decision Matrix

| Criterion | Move to Utilities | Hybrid Approach | Keep Current |
|-----------|-------------------|-----------------|--------------|
| Code Reuse | ✅ Best | ⚠️ Some duplication | ❌ Most duplication |
| Dependencies | ⚠️ Heavier utilities | ✅ No change | ✅ No change |
| Backup Speed | ✅ Fast (direct copy) | ✅ Fast (with cache) | ❌ Slow (always chunk) |
| Restore Speed | ✅ Fast (direct copy) | ✅ Fast (to cache) | ❌ Slow (always reconstruct) |
| Complexity | ⚠️ Medium | ⚠️ Medium | ✅ Low |
| Maintenance | ✅ Single source | ⚠️ Cache management | ❌ Keep in sync |

## Recommendation

**For Production: Move to `utilities`**

This is the "right" solution architecturally. The dependency concerns are manageable - `utilities` is already a core crate, and these dependencies make sense for file handling utilities.

**For Quick Win: Hybrid Approach**

If time is limited or we want to minimize risk, the hybrid approach gets 80% of the benefits with 20% of the complexity.

## Implementation Plan (utilities approach)

1. ✅ Create design document
2. ⏳ Add dependencies to `utilities/Cargo.toml`
3. ⏳ Create `utilities/src/chunked_files.rs` module
4. ⏳ Move `ChunkedWriter` from `remote` to `utilities`
5. ⏳ Move `ChunkedReader` from `remote` to `utilities`
6. ⏳ Update `remote` to use `utilities::chunked_files`
7. ⏳ Update `tlogfs` to write/read parquet files
8. ⏳ Update remote backup to copy parquet files
9. ⏳ Update remote restore to copy parquet files
10. ⏳ Add migration support
11. ⏳ Update tests
12. ⏳ Update documentation

**Estimated Effort:** 2-3 days

**Risk Level:** Medium (architectural change, but well-isolated)

## Questions for User

1. **Is the `utilities` dependency approach acceptable?**
   - This adds `bao-tree`, `blake3`, heavier `parquet` deps to utilities

2. **Do we need backward compatibility with raw binary files?**
   - If yes: need dual-format support during migration
   - If no: can force conversion on first backup

3. **Timing: Quick win or proper solution?**
   - Hybrid approach: 1 day, 80% benefit
   - Full solution: 2-3 days, 100% benefit, cleaner code

4. **Migration strategy:**
   - Automatic on first backup?
   - Manual migration command?
   - Support both formats indefinitely?

---

**Next Steps:** Await user feedback on approach before proceeding with implementation.
