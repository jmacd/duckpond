# DuckPond Test Framework

A containerized test harness for safe testation with the `pond` CLI.

## Purpose

This framework enables iterative testation with duckpond:
- **Safe sandbox**: Mistakes are harmless - everything runs in disposable containers
- **Fast feedback**: Failures produce actionable output for development or documentation
- **Agent-friendly**: Structured workflow for AI-assisted testing

## Directory Structure

```
testsuite/
├── README.md                   # This file
├── AGENT_INSTRUCTIONS.md       # Instructions for AI agents running tests
├── run-test.sh           # Main test runner
├── Dockerfile.test       # Container image for tests
├── templates/                  # Test script templates
│   └── basic.sh.tmpl
├── tests/                     # Currently running test
│   └── test.sh           # Generated test script
├── results/                    # Test outcomes
│   ├── YYYYMMDD-HHMMSS-success.md
│   └── YYYYMMDD-HHMMSS-failure.md
└── tests/                    # Saved tests that work
    └── *.sh
```

## Quick Start

### 1. Build the test container

```bash
cd tests
./build-image.sh
```

### 2. Run an test

```bash
# Interactive mode - enter commands manually
./run-test.sh --interactive

# Run a script
./run-test.sh ./tests/basic-init-copy.sh

# Run from inline script
./run-test.sh --inline '
pond init
pond mkdir /data
pond list /
'
```

### 3. Review results

Results are saved in `results/` with timestamps and categorized as:
- `*-success.md` - Test worked as expected
- `*-failure.md` - Technical failure (code bug, unexpected error)
- `*-confusion.md` - Usage confusion (documentation gap)

## Workflow for Agents

See [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md) for the full agent workflow.

### TL;DR Agent Loop

1. User describes test → Read the description
2. Review `docs/duckpond-overview.md` for relevant commands
3. Write test script in `tests/test.sh`
4. Run: `./run-test.sh tests/test.sh`
5. Analyze output → Categorize as success/failure/confusion
6. If failure: Create issue description for development
7. If confusion: Update documentation
8. Repeat

## Container Environment

The test container provides:
- The compiled `pond` binary at `/usr/local/bin/pond`
- A fresh `/pond` directory for each run (initialized or not, as needed)
- Standard Unix tools for inspection
- `POND=/pond` environment variable preset

## Example Tests

### Basic: Initialize and list

```bash
#!/bin/bash
set -e
pond init
pond list /
# Expected: empty root directory listing
```

### Copy and query

```bash
#!/bin/bash
set -e
pond init

# Create test data
cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,22.5,45
2024-01-01T01:00:00Z,23.1,44
EOF

pond mkdir /sensors
pond copy /tmp/data.csv /sensors/readings.csv
pond cat /sensors/readings.csv
pond cat /sensors/readings.csv --sql "SELECT * FROM source WHERE temperature > 23"
```

### Factory node creation

```bash
#!/bin/bash
set -e
pond init

# Create source data
pond mkdir /raw
cat > /tmp/measurements.csv << 'EOF'
timestamp,value
2024-01-01T00:00:00Z,100
2024-01-01T01:00:00Z,200
EOF
pond copy /tmp/measurements.csv /raw/data.csv

# Create derived view
cat > /tmp/filter.yaml << 'EOF'
factory: "sql-derived-table"
config:
  patterns:
    source: "table:///raw/data.csv"
  query: "SELECT timestamp, value * 2 as doubled FROM source"
EOF

pond mknod --config /tmp/filter.yaml /derived/doubled.series
pond cat /derived/doubled.series
```
