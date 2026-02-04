# DuckPond - Copilot Instructions

## ⚡ THE CORE LOOP (Every Session)

**DuckPond development is test-driven discovery.** Every work session follows this loop:

```
1. UNDERSTAND what we're testing/building
2. RUN test in containerized sandbox (`testsuite/`)
3. CLASSIFY result: ✅ Success | ❌ Bug | ❓ Bad Docs | 🤷 Bad Interface
4. DISCUSS with user → get decision
5. FIX the right layer (code, docs, or interface)
6. SAVE successful test to `testsuite/tests/NNN-description.sh`
7. UPDATE `docs/cli-reference.md` if behavior was clarified
```

**Start every session by asking**: "What are we testing?" or "Show me the backlog" (`testsuite/BACKLOG.md`)

---

## 🧪 TESTSUITE FIRST

The testsuite is your primary tool. **Use it before writing production code.**

```bash
cd testsuite

# Run a test by number (finds NNN-*.sh in tests/)
./run-test.sh 032

# Run interactively (explore in container)
./run-test.sh --interactive

# Run with S3/MinIO for replication tests
docker-compose up -d minio
docker-compose run --rm duckpond ./tests/500-*.sh
```

### Test Script Template
```bash
#!/bin/bash
# EXPERIMENT: [Brief title]
# EXPECTED: [What should happen]
set -e  # Stop on first error

pond init
# ... test steps ...

echo "=== VERIFICATION ==="
pond list /path/to/check  # ALWAYS verify results
```

### After Every Test
1. **Success?** → Save to `testsuite/tests/NNN-name.sh`, update docs if needed
2. **Bug?** → Report to user with exact error, propose proper fix (NO BAND-AIDS)
3. **Docs wrong?** → Fix `docs/cli-reference.md` immediately
4. **Interface confusing?** → Discuss with user before changing

---

## 🚫 HARD RULES (Violations = Wasted Time)

### Rust Code Anti-Patterns

| ❌ NEVER | ✅ INSTEAD | WHY |
|----------|-----------|-----|
| `persistence.begin()` twice | Pass `&mut tx` to helpers | Single transaction rule - **PANICS** |
| `OpLogPersistence::open(path)` in helpers | Use existing `tx.session_context()` | Creates duplicate transaction |
| `.unwrap_or(default)` | `.map_err(\|e\| Error::from(e))?` | Silent fallbacks mask bugs |
| `_v2` / `_with_options` suffixes | Options struct + single function | Duplicate code = tech debt |
| `Err("not implemented")` | `unimplemented!("reason")` | Runtime errors vs dev markers |

### Debugging Anti-Patterns

| ❌ NEVER | ✅ INSTEAD |
|----------|-----------|
| `cargo test \| grep ERROR` | `cargo test 2>&1 \| tee OUT` then `grep_search` on OUT |
| `cargo run \| tail -100` | Full output to file, then search |
| Truncated terminal output | Always capture complete logs |

### Development Anti-Patterns

| ❌ NEVER | ✅ INSTEAD |
|----------|-----------|
| "Simple fix" / "Quick patch" | Understand root cause, design proper solution |
| Guess at CLI syntax | Check `docs/cli-reference.md` first |
| Skip verification | Always `pond list` / `pond cat` to verify |
| Test multiple things at once | One concept per test |

---

## 📚 REFERENCE DOCS (Read When Needed)

| Doc | When to Read |
|-----|--------------|
| `docs/cli-reference.md` | **ALWAYS** - CLI syntax, factory configs |
| `docs/duckpond-overview.md` | Architecture understanding |
| `docs/duckpond-system-patterns.md` | Transaction patterns, NodeID/PartID |
| `docs/large-file-storage-implementation.md` | Files >64KB, hybrid writer |

---

## 🏷️ CLASSIFICATION GUIDE

When a test completes, classify and act:

| Result | Meaning | Action |
|--------|---------|--------|
| ✅ **Success** | Works as documented | Save test, verify docs are accurate |
| ❌ **BUG** | Code doesn't work as intended | Report to user, propose proper fix |
| ❓ **DOCS** | Documentation is wrong/missing | Fix `docs/cli-reference.md` immediately |
| 🤷 **UX** | Interface is confusing but works | Discuss with user before changing |
| 💀 **DESIGN** | Fundamental approach is wrong | Full discussion needed |

---

## 🔧 KEY CONSTANTS

- **LARGE_FILE_THRESHOLD = 64KB**: Files >64KB stored externally in `_large_files/`
- **Transaction Guard**: Only ONE active transaction per operation (enforced by panic)
- **Table name in SQL**: Always `source` (e.g., `SELECT * FROM source WHERE ...`)
