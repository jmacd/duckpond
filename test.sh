POND=/tmp/pond

cargo build --workspace || exit 1

EXE=`pwd`/target/debug/pond

rm -rf ${POND}

export POND

export DUCKPOND_LOG

echo "=== INIT ==="
${EXE} init

# echo "=== AFTER INIT - CONTROL FILESYSTEM ==="
# ${EXE} list '/**' --filesystem control

# echo "Aaaaa" > /tmp/A
# echo "Bbbbb" > /tmp/B
# echo "Ccccc" > /tmp/C

# echo "=== FIRST COPY ==="
# ${EXE} copy /tmp/{A,B,C} /

# echo "=== AFTER FIRST COPY - CONTROL FILESYSTEM ==="
# ${EXE} list '/**' --filesystem control

echo "=== MKDIR ==="
${EXE} mkdir /ok

# echo "=== AFTER MKDIR - CONTROL FILESYSTEM ==="
# ${EXE} list '/**' --filesystem control

# echo "=== SECOND COPY (this should trigger the bug) ==="
# ${EXE} copy /tmp/{A,B,C} /ok

# echo "=== AFTER SECOND COPY - CONTROL FILESYSTEM (bug should be visible) ==="
# ${EXE} list '/**' --filesystem control

# echo "=== SHOW DATA FILESYSTEM ==="
# ${EXE} show

# echo "=== LIST DATA FILESYSTEM ==="
# ${EXE} list '/**'
# ${EXE} list '/**/A'

# echo "=== MKDIR ==="
# ${EXE} mkdir /empty

# echo "=== SHOW DATA FILESYSTEM ==="
# ${EXE} show

# echo "=== CAT TXN 5 ==="
# ${EXE} cat '/txn/5' --filesystem control

# echo "=== CAT  /ok/A  ==="
# ${EXE} cat '/ok/A' | cmp - /tmp/A

# echo "========================="

echo "=== COPY --format=series ./test_data.csv /ok ==="
${EXE} copy --format=series ./test_data.csv /ok/test.series
${EXE} copy --format=series ./test_data2.csv /ok/test.series
${EXE} copy --format=series ./test_data3.csv /ok/test.series

echo "=== CAT --display=table /ok/test.series ==="
${EXE} cat --display=table '/ok/test.series' 

echo "=== SQL QUERY TEST ==="
${EXE} cat '/ok/test.series' --query "SELECT * FROM series LIMIT 1"
echo "=== DESCRIBE ==="
${EXE} describe  '/ok/test.series' 

# HERE - Testing SQL-derived functionality
echo "=== TESTING SQL-DERIVED MKNOD ==="

# Create SQL-derived config that renames columns from test.series
echo "=== TESTING SQL-DERIVED MKNOD ==="

# Create SQL-derived config that renames columns from test.series
echo "source: "/ok/test.series"" > alternate.yaml
echo "sql: "SELECT name as Apple, city as Berry, timestamp FROM series"" >> alternate.yaml

echo "=== MKNOD SQL-DERIVED ==="
${EXE} mknod sql-derived /ok/alternate.series alternate.yaml

echo "=== SHOW (should now include alternate.series) ==="
${EXE} show

echo "=== CAT SQL-DERIVED FILE --display=table ==="
${EXE} cat --display=table '/ok/alternate.series' 2>&1 | head -20

echo "=== DESCRIBE SQL-DERIVED FILE ==="
${EXE} describe '/ok/alternate.series' 2>&1 | head -10

echo "=== CLEANUP SQL-DERIVED TEST ==="
rm -f alternate.yaml

echo "=== MKNOD SQL-DERIVED ==="
${EXE} mknod sql-derived /ok/alternate.series alternate.yaml

echo "=== SHOW (should now include alternate.series) ==="
${EXE} show

echo "=== CAT SQL-DERIVED FILE --display=table ==="
${EXE} cat --display=table '/ok/alternate.series'

echo "=== DESCRIBE SQL-DERIVED FILE ==="
${EXE} describe '/ok/alternate.series'

echo "=== SQL QUERY ON SQL-DERIVED FILE ==="
${EXE} cat '/ok/alternate.series' --query "SELECT Apple, Berry, timestamp FROM series LIMIT 2"

# echo "=== Testing FileTable: CSV-to-Parquet conversion ==="
# echo "=== COPY --format=parquet ./test_data.csv /ok/test.table ==="
# ${EXE} copy --format=parquet ./test_data.csv /ok/test.table

# echo "=== CAT FileTable as table ==="
# ${EXE} cat --display=table '/ok/test.table' 

# echo "=== SQL QUERY TEST on FileTable ==="
# ${EXE} cat '/ok/test.table' --query "SELECT * FROM series WHERE timestamp > 1672531200000"

# echo "=== DESCRIBE FileTable ==="
# ${EXE} describe '/ok/test.table' 

# echo "=== CAT FileSeries as table ==="
# ${EXE} cat --display=table '/ok/test.series' 

# echo "=== SQL QUERY TEST on FileSeries ==="
# ${EXE} cat '/ok/test.series' --query "SELECT * FROM series WHERE timestamp > 1672531200000"

# echo "=== DESCRIBE FileSeries ==="
# ${EXE} describe '/ok/test.series'

# echo "=== SHOW (should see both FileTable and FileSeries entries) ==="
# ${EXE} show

# echo "=== SETUP TEST HOSTMOUNT DIRECTORY ==="
# TEST_HOST_DIR="/tmp/duckpond_hostmount_test"
# rm -rf ${TEST_HOST_DIR}
# mkdir -p ${TEST_HOST_DIR}

# # Create some test files and directories
# echo "Hello from file1.txt" > ${TEST_HOST_DIR}/file1.txt
# echo "Hello from file2.txt" > ${TEST_HOST_DIR}/file2.txt
# mkdir -p ${TEST_HOST_DIR}/subdir
# echo "Hello from nested file" > ${TEST_HOST_DIR}/subdir/nested.txt
# echo "More nested content" > ${TEST_HOST_DIR}/subdir/another.txt

# # Create hostmount config pointing to our test directory
# echo "directory: ${TEST_HOST_DIR}" > hostmount_test.yaml

# echo "=== MKNOD ==="
# ${EXE} mknod hostmount /mnt hostmount_test.yaml

# echo "=== SHOW ==="
# ${EXE} show

# echo "=== LIST (should show hostmount directory and test files) ==="
# ${EXE} list '/**'

# echo "=== TEST FILE ACCESS ==="
# echo "Reading /mnt/file1.txt:"
# ${EXE} cat /mnt/file1.txt

# echo "Reading /mnt/file2.txt:"
# ${EXE} cat /mnt/file2.txt

# echo "=== TEST SUBDIRECTORY TRAVERSAL ==="
# echo "Listing /mnt/subdir:"
# ${EXE} list '/mnt/subdir/*'

# echo "Reading /mnt/subdir/nested.txt:"
# ${EXE} cat /mnt/subdir/nested.txt

# echo "Reading /mnt/subdir/another.txt:"
# ${EXE} cat /mnt/subdir/another.txt

# echo "=== CLEANUP ==="
# rm -f hostmount_test.yaml
# rm -rf ${TEST_HOST_DIR}
