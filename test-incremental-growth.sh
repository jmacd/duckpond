#!/bin/bash
# Test script for incremental logfile growth with rotation
#
# Creates 10000 rows, 100 at a time, rotating at 1000 lines per file.
# Result: 10 archived files + 1 empty active file
#
# Usage: ./test-incremental-growth.sh [output_dir]

set -e

OUTPUT_DIR=/tmp/synth-incremental
POND=/tmp/pond-incremental
TOTAL_ROWS=10000
BATCH_SIZE=100
LINES_PER_FILE=1000
MAX_FILES=20  # Keep plenty of archived files

# Clean start
rm -rf "$OUTPUT_DIR"
rm -rf "$POND"
mkdir -p "$OUTPUT_DIR"

export POND

cargo run --bin pond init
cargo run --bin pond mkdir /ingest
cargo run --bin pond mknod --config-path test-ingest.yaml logfile-ingest /config

echo "=== Incremental Log Growth Test ==="
echo "Output: $OUTPUT_DIR"
echo "Total rows: $TOTAL_ROWS"
echo "Batch size: $BATCH_SIZE"
echo "Lines per file: $LINES_PER_FILE"
echo "Expected files: $((TOTAL_ROWS / LINES_PER_FILE))"
echo ""

# Calculate number of batches
NUM_BATCHES=$((TOTAL_ROWS / BATCH_SIZE))

for i in $(seq 1 $NUM_BATCHES); do
    START_ROW=$(( (i - 1) * BATCH_SIZE + 1 ))
    
    # Run synth-logs with --start-row to continue numbering
    cargo run --bin synth-logs --quiet -- \
        --output-dir "$OUTPUT_DIR" \
        --rows $BATCH_SIZE \
        --lines $LINES_PER_FILE \
        --max-files $MAX_FILES \
        --start-row $START_ROW

    # Run the logfile ingest execute command
    cargo run --bin pond run /config
    
    # # Progress indicator
    # CURRENT_TOTAL=$((i * BATCH_SIZE))
    # if (( i % 10 == 0 )); then
    #     echo "Progress: $CURRENT_TOTAL / $TOTAL_ROWS rows"
    # fi

    
done

echo ""
echo "=== Generation Complete ==="
echo ""

exit 0

# Show results
echo "=== Files Created ==="
ls -la "$OUTPUT_DIR"/*.log* 2>/dev/null | while read line; do
    echo "  $line"
done

echo ""
echo "=== File Count ==="
FILE_COUNT=$(ls -1 "$OUTPUT_DIR"/*.log* 2>/dev/null | wc -l | tr -d ' ')
echo "Total files: $FILE_COUNT"

echo ""
echo "=== Verification ==="
# Verify by checking first and last lines
OLDEST_FILE=$(ls -1 "$OUTPUT_DIR"/*.log.* 2>/dev/null | sort | head -1)
ACTIVE_FILE="$OUTPUT_DIR/app.log"

if [ -f "$OLDEST_FILE" ]; then
    echo "Oldest archived file: $OLDEST_FILE"
    echo "  First line: $(head -1 "$OLDEST_FILE")"
    echo "  Last line:  $(tail -1 "$OLDEST_FILE")"
fi

NEWEST_ARCHIVED=$(ls -1 "$OUTPUT_DIR"/*.log.* 2>/dev/null | sort | tail -1)
if [ -f "$NEWEST_ARCHIVED" ]; then
    echo "Newest archived file: $NEWEST_ARCHIVED"
    echo "  First line: $(head -1 "$NEWEST_ARCHIVED")"
    echo "  Last line:  $(tail -1 "$NEWEST_ARCHIVED")"
fi

if [ -s "$ACTIVE_FILE" ]; then
    echo "Active file: $ACTIVE_FILE"
    echo "  First line: $(head -1 "$ACTIVE_FILE")"
    echo "  Last line:  $(tail -1 "$ACTIVE_FILE")"
else
    echo "Active file: $ACTIVE_FILE (empty - all rows rotated)"
fi

# Count total lines across all files
TOTAL_LINES=$(cat "$OUTPUT_DIR"/*.log* 2>/dev/null | wc -l | tr -d ' ')
echo ""
echo "Total lines across all files: $TOTAL_LINES"

if [ "$TOTAL_LINES" -eq "$TOTAL_ROWS" ]; then
    echo "✓ Line count matches expected ($TOTAL_ROWS)"
else
    echo "✗ Line count mismatch! Expected $TOTAL_ROWS, got $TOTAL_LINES"
    exit 1
fi
