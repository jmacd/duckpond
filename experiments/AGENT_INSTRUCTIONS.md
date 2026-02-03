# Agent Instructions: DuckPond Experiment Workflow

## 🎯 Purpose

You are helping test and document DuckPond through controlled experiments in a containerized sandbox. Your goals:

1. **Validate behavior**: Does the CLI do what the docs say?
2. **Surface bugs**: Find and document technical failures
3. **Improve docs**: Identify and fix documentation gaps

## 📋 Workflow Checklist

For each experiment cycle:

### Step 1: Understand the Test Request

- [ ] Read the user's test description carefully
- [ ] Identify which pond commands are involved
- [ ] Note what behavior is expected

### Step 2: Research DuckPond Usage

**MANDATORY** - Before writing ANY experiment script:

- [ ] Read `docs/duckpond-overview.md` for command reference
- [ ] Check if any factory types are involved → read factory section
- [ ] If SQL queries involved → understand DataFusion patterns

### Step 3: Write the Experiment Script

Create `experiments/active/experiment.sh`:

```bash
#!/bin/bash
# EXPERIMENT: [Brief title]
# DESCRIPTION: [What we're testing]
# EXPECTED: [What should happen]
set -e  # Stop on first error

# Setup
pond init

# Test steps
...

# Verification
echo "=== VERIFICATION ==="
pond list /path/to/check
```

### Step 4: Run the Experiment

```bash
cd experiments
./run-experiment.sh active/experiment.sh
```

### Step 5: Analyze Output

The output will be one of:

| Outcome | Meaning | Action |
|---------|---------|--------|
| ✅ Success | Worked as expected | Save to `library/`, report to user |
| ❌ Failure | Technical error/bug | Create failure report |
| ❓ Confusion | Docs unclear/wrong | Create confusion report, fix docs |

### Step 6: Document and Iterate

**For Failures** - Create `results/TIMESTAMP-failure.md`:
```markdown
# Failure Report

## Experiment
[Brief description]

## Command That Failed
```
[exact command]
```

## Error Output
```
[exact error message]
```

## Analysis
[What went wrong - code bug, missing feature, etc.]

## Suggested Fix
[If apparent, what needs to change in the code]
```

**For Confusion** - Create `results/TIMESTAMP-confusion.md`:
```markdown
# Documentation Confusion Report

## What I Tried
[The command or pattern I attempted]

## What I Expected (Based on Docs)
[Quote from docs that led to this expectation]

## What Actually Happened
[Actual behavior]

## Documentation Fix Needed
[Specific doc change to clarify]
```

Then:
- For failures → report to user for development
- For confusion → update the relevant doc file immediately

## 🚫 Anti-Patterns to Avoid

### Don't: Guess at command syntax
```bash
# ❌ WRONG - guessing
pond create-table /path  # Does this command exist?

# ✅ RIGHT - verify first
# Check docs/duckpond-overview.md for actual command
```

### Don't: Skip verification
```bash
# ❌ WRONG - no verification
pond copy file.csv /dest.csv

# ✅ RIGHT - verify the result
pond copy file.csv /dest.csv
pond list /dest.csv  # Did it work?
pond cat /dest.csv   # Is content correct?
```

### Don't: Ignore errors silently
```bash
# ❌ WRONG - suppressing errors
pond something 2>/dev/null || true

# ✅ RIGHT - let errors surface
set -e
pond something  # Will stop if it fails
```

### Don't: Create overly complex experiments
```bash
# ❌ WRONG - testing too many things at once
pond init && pond mkdir /a && pond mkdir /b && pond copy ... && pond mknod ...

# ✅ RIGHT - one concept per experiment
# Experiment 1: just test mkdir
# Experiment 2: just test copy
# Experiment 3: test mkdir + copy together
```

## 📚 Reference: Key Documentation

| Doc | When to Read |
|-----|--------------|
| `docs/duckpond-overview.md` | ALWAYS - command reference |
| `docs/duckpond-system-patterns.md` | Transaction/state issues |
| `docs/logfile-ingestion-factory-design.md` | Log ingestion scenarios |
| `docs/remote-backup-chunked-parquet-design.md` | S3/replication scenarios |
| `docs/fallback-antipattern-philosophy.md` | Error handling confusion |
| `docs/large-output-debugging.md` | When output is truncated |

## 🔄 Iteration Mindset

Each experiment is cheap. Don't aim for perfection:

1. **Try something** → See what happens
2. **Observe failure** → Understand why
3. **Fix or document** → Make progress
4. **Repeat** → Knowledge accumulates

The container resets completely between runs. There's no state to corrupt, no cleanup needed. Be bold!

## 🏗️ Complex Experiment Patterns

### Multi-Pond Experiments

For experiments with multiple pond instances:

```bash
#!/bin/bash
set -e

# Initialize both ponds
POND=/pond1 pond init
POND=/pond2 pond init

# Or use helpers
pond-init 1
pond-init 2

# Run commands on specific ponds
POND=/pond1 pond mkdir /data
POND=/pond2 pond mkdir /replica
```

### S3/MinIO Experiments

For replication testing, start with docker-compose:

```bash
# Terminal 1: Start MinIO
cd experiments
docker-compose up -d minio

# Terminal 2: Run experiment with S3 access
docker-compose run --rm duckpond ./library/500-s3-replication-minio.sh
```

Environment variables available in container:
- `MINIO_ENDPOINT=http://minio:9000`
- `MINIO_ROOT_USER=minioadmin`
- `MINIO_ROOT_PASSWORD=minioadmin`

### Cron/Scheduled Experiments

For testing periodic operations:

```bash
#!/bin/bash
# Setup cron job in container
echo "* * * * * root POND=/pond1 pond run /etc/system.d/20-logs ingest" > /etc/cron.d/experiment
chmod 0644 /etc/cron.d/experiment
cron

# Run for a few minutes and observe
sleep 180
POND=/pond1 pond control recent --limit 10
```

## 🏷️ Experiment Naming

When saving successful experiments to `library/`:

```
library/
├── 001-basic-init.sh           # Numbered for ordering
├── 002-mkdir-nested.sh
├── 010-copy-csv.sh
├── 011-copy-parquet.sh
├── 020-cat-with-sql.sh
├── 100-factory-sql-derived.sh
├── 200-hydrovu-config.sh
└── 300-remote-backup.sh
```

Categories:
- `001-0xx`: Basic operations (init, mkdir, list)
- `010-0xx`: Copy operations
- `020-0xx`: Cat/query operations
- `100-1xx`: Factory nodes
- `200-2xx`: HydroVu integration
- `300-3xx`: Remote/backup operations
