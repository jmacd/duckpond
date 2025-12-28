#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
POND=/tmp/pond
POND2=/tmp/pond-replica
SAMPLE=${ROOT}/sample-oteljson
REMOTE_CONFIG=/tmp/oteljson-remote.yaml
BACKUP_TO=/tmp/pond-backups

EXE="cargo run --bin pond"

export POND

rm -rf ${POND}
rm -rf ${POND2}
rm -rf ${BACKUP_TO}

# Create remote backup configuration
cat > ${REMOTE_CONFIG} <<EOF
compression_level: 3
url: "file:///tmp/pond-backups"
EOF

${EXE} init

${EXE} mkdir -p /etc/system.d

${EXE} mknod remote /etc/system.d/1-backup --config-path ${REMOTE_CONFIG}

${EXE} mkdir -p /otel

# Copy OTel JSON files into pond
${EXE} copy "host://${SAMPLE}/casparwater-2025-07-02T07-31-58.222.json" /otel/caspar-jul02.json
${EXE} copy "host://${SAMPLE}/casparwater-2025-07-13T08-54-27.820.json" /otel/caspar-jul13.json
${EXE} copy "host://${SAMPLE}/casparwater-2025-07-24T09-29-56.192.json" /otel/caspar-jul24.json

# Describe the OTel JSON files to see their schemas
echo "=== Describing OTel JSON file schema ==="
${EXE} describe oteljson:///otel/caspar-jul02.json

echo "=== Original pond SHA256 values ==="
shasum -a 256 ${POND}/data/_large_files/*

echo ""
echo "=== Generating replication command ==="
INIT_CMD=$(${EXE} run /etc/system.d/1-backup replicate)
echo "Init command: ${INIT_CMD}"

echo ""
echo "=== Creating replica pond from remote backup ==="
export POND=${POND2}
# Extract just the arguments after "pond init"
INIT_ARGS=$(echo "${INIT_CMD}" | sed 's/^pond init //')
${EXE} init ${INIT_ARGS}

echo "=== Replica pond SHA256 values ==="
shasum -a 256 ${POND2}/data/_large_files/*

echo ""
echo "=== Comparing SHA256 values ==="
echo "If the backup/restore worked correctly, these should match:"
diff <(shasum -a 256 /tmp/pond/data/_large_files/* | awk '{print $1}' | sort) \
     <(shasum -a 256 /tmp/pond-replica/data/_large_files/* | awk '{print $1}' | sort) \
  && echo "✅ SUCCESS: All SHA256 values match!" \
  || echo "❌ FAILURE: SHA256 values don't match"

# Cat the data from first file
#echo "=== Displaying data from first file ==="
#${EXE} cat --format=table oteljson:///otel/caspar-jul02.json | head -40

# # Cat with a glob pattern to combine all files
# echo "=== Displaying combined data from all OTel files ==="
# ${EXE} cat 'oteljson:///otel/*.json'

# # Cat with SQL query to filter specific metrics
# echo "=== Showing only system_pressure_value and chlorine_level_value ==="
# ${EXE} cat --query "SELECT timestamp, well_depth_value FROM series where well_depth_value is not null order by timestamp" 'oteljson:///otel/*.json'

#./catparquet.sh OUT.parquet

# # Show timestamp range
# echo "=== Showing timestamp range ==="
# ${EXE} cat --sql "SELECT MIN(timestamp) as first_ts, MAX(timestamp) as last_ts, COUNT(*) as row_count FROM 'oteljson:///otel/*.json'" 'oteljson:///otel/*.json'
