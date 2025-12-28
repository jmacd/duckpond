# Large Output Debugging Protocol

## 🛑 STOP: Never use grep/tail/head in terminal commands

**Problem**: Terminal output gets truncated, losing critical context for debugging.

**Solution**: ALWAYS redirect to a file, THEN search the file.

## The Correct Pattern

### Step 1: Run with full output to file
```bash
# Use tee to view output in real-time AND save to file:
RUST_LOG=debug RUST_LOG=datafusion=debug POND=/tmp/pond \
  cargo run --bin pond show 2>&1 | tee OUT

# Or for tests:
cargo test test_name 2>&1 | tee OUT

# Exit status is preserved - you'll know if it succeeded/failed
# You can watch progress while still capturing everything
```

### Step 2: Search the file with grep_search tool
```bash
# Now use grep_search on the OUT file (not terminal commands!)
# This preserves full context and allows repeated searches
```

## Why This Matters

✅ **Full context preserved**: No truncation, all logs available  
✅ **Repeatable analysis**: Search again without re-running  
✅ **No loss of output**: Non-deterministic tests become debuggable  
✅ **Pattern matching**: Use grep_search tool on complete output  
✅ **Real-time viewing**: With tee, monitor progress on long-running commands

## Red Flags (What NOT to Do)

❌ `cargo test 2>&1 | grep ERROR` - Loses context  
❌ `cargo run | tail -n 100` - Truncates important info  
❌ `cargo run | head -n 50` - Misses later output  

✅ `cargo test 2>&1 | tee OUT` then `grep_search` on OUT file

## When to Use This

- Any debugging with RUST_LOG=debug
- Test output analysis
- DataFusion query plan inspection  
- Transaction lifecycle debugging
- Any command producing > 100 lines of output

**Remember**: If the user says "debug", "test", or "check output" → Use this protocol automatically.
