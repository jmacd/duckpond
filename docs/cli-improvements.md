# CLI Improvement Suggestions

Collected from experimentation - usability improvements that would reduce confusion.

## Pending Suggestions

### 1. Rename `--query` to `--sql`

**Command**: `pond cat`

**Current**:
```bash
pond cat /data/file.csv --query "SELECT * FROM source WHERE x > 10"
```

**Suggested**:
```bash
pond cat /data/file.csv --sql "SELECT * FROM source WHERE x > 10"
```

**Rationale**: Users intuitively expect `--sql` when writing SQL. The existing documentation (duckpond-overview.md) even shows `--sql` in examples, causing confusion.

**Status**: Suggested

---

### 2. Better error message for `pond list /`

**Current**:
```
Error: Failed to list files matching '/' from data filesystem: EmptyPath
```

**Suggested**:
```
Error: Invalid pattern '/'. Use '/*' to list root directory or '**/*' for all files.
```

**Rationale**: "EmptyPath" is cryptic. Users need actionable guidance.

**Status**: Suggested

---

### 3. Document `host://` prefix requirement

**Command**: `pond copy`

The deprecation warning is good:
```
Copying without 'host://' prefix is deprecated. Use 'host://' on sources when copying IN.
```

But this needs prominent documentation since it's a breaking change pattern.

**Status**: ✅ Added to cli-reference.md

---

## Investigated Issues

### ✅ `--query` not filtering results? (RESOLVED)

In experiment 020, SQL queries appeared to return all rows regardless of WHERE clause.

**Root Cause**: Files stored as `FilePhysicalVersion` (raw bytes) cannot be queried with SQL.
The `--query` flag is silently ignored for raw data files.

**Resolution**: 
- For CSV files: Use `csv://` URL scheme to enable SQL querying
  ```bash
  pond cat csv:///data/file.csv --query "SELECT * FROM source WHERE x > 10"
  ```
- For Parquet files: Store with `--format=table` to enable querying
  ```bash
  pond copy host:///tmp/file.parquet /data/file.parquet --format=table
  pond cat /data/file.parquet --query "SELECT * FROM source"
  ```

**Status**: ✅ Fixed and documented in cli-reference.md

---

## Completed

- [x] Document glob pattern requirements for `pond list`
- [x] Document `--query` flag (not `--sql`) 
- [x] Document `host://` prefix for `pond copy`
- [x] Fix `--format=table` to validate Parquet input (was corrupting CSV files)
- [x] Fix table name from `series` to `source` in cat.rs
- [x] Document `--format=data` vs `--format=table` distinction
- [x] Document `csv://` URL scheme for querying CSV files
- [x] Resolve `--query` not filtering issue (needed `csv://` prefix)
