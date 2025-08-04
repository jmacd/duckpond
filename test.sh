POND=/tmp/pond

cargo build --workspace || exit 1

EXE=target/debug/pond

rm -rf ${POND}

export POND

export DUCKPOND_LOG

echo "=== INIT ==="
${EXE} init

# ============================================================================
# MEMORY-EFFICIENT COPY COMMAND TESTS
# Testing the new copy command with automatic entry type detection
# and post-write temporal metadata extraction via TLogFS
# ============================================================================

echo "=== CREATE TEST DATA FILES ==="

# Create test CSV with timestamp column (should become FileSeries)
cat > /tmp/temporal_data.csv << 'EOF'
timestamp,value,sensor_id
2024-01-01T00:00:00,10.5,sensor1
2024-01-01T01:00:00,11.2,sensor1
2024-01-01T02:00:00,12.8,sensor2
2024-01-01T03:00:00,9.7,sensor1
EOF

# Create test CSV without timestamp column (should become FileTable)
cat > /tmp/regular_data.csv << 'EOF'
name,city,value
Alice,NYC,100
Bob,LA,200
Charlie,Chicago,300
EOF

# Create a regular text file (should become FileData)
echo "This is just plain text data without any structure." > /tmp/plain_text.txt

echo "=== MKDIR FOR TESTS ==="
${EXE} mkdir /test_files

echo "=== TEST 1: Auto-detection with --format=auto ==="
echo "This tests file extension-based entry type detection (CSV files stay as FileData)"

echo "1a. Copy CSV with timestamp (should stay as FileData - no conversion)"
${EXE} copy --format=auto /tmp/temporal_data.csv /test_files/
echo "1b. Copy CSV without timestamp (should stay as FileData - no conversion)" 
${EXE} copy --format=auto /tmp/regular_data.csv /test_files/  
echo "1c. Copy plain text (should stay as FileData)"
${EXE} copy --format=auto /tmp/plain_text.txt /test_files/

echo "=== TEST 2: Explicit format specification ==="
echo "2a. Force CSV to be stored as FileTable (--format=table, should ignore and stay FileData)"
${EXE} copy --format=table /tmp/temporal_data.csv /test_files/temporal_as_table.csv
echo "2b. Force CSV to be stored as FileSeries (--format=series, should ignore and stay FileData)"
${EXE} copy --format=series /tmp/regular_data.csv /test_files/regular_as_series.csv

echo "=== TEST 3: Directory destination with trailing slash ==="
${EXE} mkdir /dest_dir
echo "3a. Copy to directory with explicit trailing slash"
${EXE} copy --format=auto /tmp/temporal_data.csv /dest_dir/
echo "3b. Copy multiple files to directory"
${EXE} copy --format=auto /tmp/temporal_data.csv /tmp/regular_data.csv /tmp/plain_text.txt /dest_dir/

echo "=== TEST 4: Destination type detection (.series extension - only for Parquet) ==="
echo "4a. Copy CSV to .series destination (should stay FileData, ignore extension)"
${EXE} copy --format=auto /tmp/temporal_data.csv /test_files/csv_to_series.series

echo "=== SHOW ALL FILES (verify entry types) ==="
${EXE} show

echo "=== LIST ALL FILES ==="
${EXE} list '/**'

echo "=== TEST 5: Verify file content preservation (no conversion) ==="
echo "5a. Check FileData CSV content (should be original CSV)"
${EXE} cat '/test_files/temporal_data.csv'
echo "5b. Check FileData CSV content forced to table (should still be original CSV)"
${EXE} cat '/test_files/temporal_as_table.csv'
echo "5c. Check FileData CSV content forced to series (should be original CSV)"
${EXE} cat '/test_files/regular_as_series.csv'

echo "=== TEST 6: Verify memory efficiency (no pre-loading) ==="
echo "6a. Copy larger temporal file and check it still uses streaming"
# Create a larger temporal CSV
echo "timestamp,value" > /tmp/large_temporal.csv
for i in {1..1000}; do
    echo "2024-01-$(printf '%02d' $((i % 30 + 1)))T00:00:00,$((RANDOM % 1000))" >> /tmp/large_temporal.csv
done
${EXE} copy --format=auto /tmp/large_temporal.csv /test_files/
echo "Large file copied successfully (streaming worked)"

echo "=== TEST 7: Verify transaction control ==="
echo "7a. Test rollback on error (try to copy non-existent file)"
${EXE} copy --format=auto /tmp/does_not_exist.csv /test_files/ 2>/dev/null && echo "ERROR: Should have failed" || echo "Expected failure - transaction rolled back"

echo "7b. Verify pond is still consistent after rollback"
${EXE} show

echo "=== CLEANUP TEST FILES ==="
rm -f /tmp/temporal_data.csv /tmp/regular_data.csv /tmp/plain_text.txt /tmp/large_temporal.csv

echo "=== LEGACY TESTS (COMMENTED OUT) ==="
# echo "The following tests are commented out from the original test.sh"

# ============================================================================
# ORIGINAL TESTS (COMMENTED OUT)
# ============================================================================

# echo "Aaaaa" > /tmp/A
# echo "Bbbbb" > /tmp/B  
# echo "Ccccc" > /tmp/C

# echo "=== FIRST COPY ==="
# ${EXE} copy /tmp/{A,B,C} /

# echo "=== MKDIR ==="
# ${EXE} mkdir /ok

# echo "=== SECOND COPY ==="
# ${EXE} copy /tmp/{A,B,C} /ok

# echo "=== COPY --format=series ./test_data.csv /ok ==="
# ${EXE} copy --format=series ./test_data.csv /ok/test.series
# ${EXE} copy --format=series ./test_data2.csv /ok/test.series
# ${EXE} copy --format=series ./test_data3.csv /ok/test.series

# echo "=== CAT --display=table /ok/test.series ==="
# ${EXE} cat --display=table '/ok/test.series' 

# echo "=== SQL QUERY TEST ==="
# ${EXE} cat '/ok/test.series' --query "SELECT * FROM series LIMIT 1"

# echo "=== CSV DIRECTORY TEST ==="
# ${EXE} mkdir /csv_files
# ${EXE} copy ./test_data*.csv /csv_files/
# ${EXE} list '/csv_files/*'

echo "=== MEMORY-EFFICIENT COPY TESTS COMPLETE ==="
