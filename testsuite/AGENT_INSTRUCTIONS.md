# Agent Instructions: DuckPond Test Workflow

## 🎯 The Goal

Test DuckPond → Classify results → Fix the right layer → Save passing tests

**Living documentation**: `docs/cli-reference.md` is THE source of truth. Update it when you learn something.

---

## ⚡ THE LOOP

```
1. RUN TEST        → ./run-test.sh 032  (or --interactive)
2. CLASSIFY        → ✅ Success | ❌ Bug | ❓ Docs | 🤷 UX | 💀 Design
3. DISCUSS         → Present result to user, propose action
4. FIX             → Code for bugs, docs for confusion
5. SAVE            → Add passing test to tests/NNN-name.sh
```

---

## 🏃 Running Tests

```bash
cd testsuite
./run-test.sh 032              # Run by number
./run-test.sh --interactive    # Explore manually
```

### Test Template

```bash
#!/bin/bash
# EXPERIMENT: [Title]
# EXPECTED: [Outcome]
set -e

pond init
# ... test steps ...

echo "=== VERIFICATION ==="
pond list /  # ALWAYS verify
```

---

## 🏷️ Classification

| Result | Meaning | Action |
|--------|---------|--------|
| ✅ **Success** | Works as documented | Save test, verify docs accurate |
| ❌ **BUG** | Code error | Report to user, propose proper fix |
| ❓ **DOCS** | Docs wrong/missing | Fix `docs/cli-reference.md` NOW |
| 🤷 **UX** | Works but confusing | Discuss before changing |
| 💀 **DESIGN** | Fundamental issue | Full discussion needed |

---

## 📝 Report Format

```markdown
### 🔍 Test: [name/number]
**Result**: [✅/❌/❓/🤷/💀]
**What Happened**: [brief]
**Classification**: [type] - [why]
**Proposed Action**: [specific]
**Decision Needed**: Approve / Defer / Discuss?
```

---

## 🚫 Anti-Patterns

| ❌ NEVER | ✅ INSTEAD |
|----------|-----------|
| Guess CLI syntax | Read `docs/cli-reference.md` first |
| Skip verification | Always `pond list` / `pond cat` |
| Suppress errors | Use `set -e` |
| Test multiple things | One concept per test |
| Quick patch | Fix root cause properly |

---

## 📚 Reference Docs

- **CLI syntax**: `docs/cli-reference.md` (always check first)
- **Architecture**: `docs/duckpond-overview.md`
- **Transactions**: `docs/duckpond-system-patterns.md`

---

## 📂 Test Numbering

```
001-0xx: Basic (init, mkdir, list)
010-0xx: Copy
020-0xx: Cat/query
030-0xx: Log ingestion
100-1xx: Factories
200-2xx: HydroVu
300-3xx: Remote/backup
400-4xx: Multi-pond
500-5xx: S3/replication
```

---

## 🔧 Advanced Patterns

### Multi-Pond
```bash
POND=/pond1 pond init
POND=/pond2 pond init
POND=/pond1 pond mkdir /data
```

### S3/MinIO
```bash
docker-compose up -d minio
docker-compose run --rm duckpond ./tests/500-*.sh
```

Environment: `MINIO_ENDPOINT=http://minio:9000`, user/pass: `minioadmin`
