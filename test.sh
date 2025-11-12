#!/bin/bash
set -x -e  # Exit on first error

POND=/tmp/pond
BACKUPS=/tmp/pond-backups

cargo build --workspace || exit 1

EXE=target/debug/pond

# Clean up previous test runs
rm -rf ${POND}
rm -rf ${BACKUPS}
mkdir ${BACKUPS}

export POND

export RUST_LOG=info
#,tlogfs=debug

echo "=== INIT ==="
${EXE} init

echo "=== MKDIR ==="
${EXE} mkdir /etc
${EXE} mkdir /etc/system.d

echo "=== CREATE HYDROVU CONFIG ==="
${EXE} mknod hydrovu /etc/hydrovu --config-path hydrovu-config.yaml

echo "=== CREATE POST-COMMIT REMOTE CONFIG ==="
${EXE} mknod remote /etc/system.d/10-remote --config-path remote-config-local-push.yaml

#echo "=== CAT HYDROVU ==="
#${EXE} cat /etc/hydrovu

#echo "=== CAT REMOTE ==="
#${EXE} cat /etc/system.d/10-remote

echo "=== RUN HYDROVU ==="
echo "Running with RUST_LOG=debug,tlogfs::remote_factory=trace"
${EXE} run /etc/hydrovu collect

echo "=== Check for created bundles ==="
echo "Contents of ${BACKUPS}:"
ls -lah ${BACKUPS}/ || echo "No backups directory or empty"

echo "=== Generate replication command ==="
POND=/tmp/pond
echo "Generating replication command..."
REPL_ARGS=$(${EXE} run /etc/system.d/10-remote replicate | grep "pond init" | cut -d ' ' -f 2-)
echo "Generated args: ${REPL_ARGS}"

echo "=== Init replica using base64 config ==="
export POND=/tmp/pond-replica
rm -rf ${POND}

# Execute with the EXE prefix and debug logging
echo "Running: ${EXE} ${REPL_ARGS}"
${EXE} ${REPL_ARGS}

echo "=== Verify replica pond identity ==="
echo "Source pond:"
POND=/tmp/pond ${EXE} show --mode=detailed
echo ""
echo "Replica pond:"
POND=/tmp/pond-replica ${EXE} show --mode=detailed

echo "=== Run again ==="

POND=/tmp/pond
${EXE} run /etc/hydrovu collect

echo "=== Sync replica ==="

POND=/tmp/pond-replica
${EXE} control --mode=sync

echo "=== Run again ==="

POND=/tmp/pond
${EXE} run /etc/hydrovu collect

echo "=== Sync replica ==="

POND=/tmp/pond-replica
${EXE} control --mode=sync

echo "=== Run again ==="

POND=/tmp/pond
${EXE} run /etc/hydrovu collect

echo "=== Sync replica ==="

POND=/tmp/pond-replica
${EXE} control --mode=sync

echo "=== Verify replica pond identity ==="
echo "Source pond:"
POND=/tmp/pond ${EXE} show --mode=detailed
echo ""
echo "Replica pond:"
POND=/tmp/pond-replica ${EXE} show --mode=detailed

