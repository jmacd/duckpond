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

echo "=== CREATE POST-COMMIT REMOTE CONFIG (LOCAL PUSH MODE) ==="
${EXE} mknod remote /etc/system.d/10-remote --config-path remote-config-local-push.yaml

echo "=== CAT HYDROVU ==="
${EXE} cat /etc/hydrovu

echo "=== CAT REMOTE ==="
${EXE} cat /etc/system.d/10-remote

echo "=== RUN HYDROVU ==="
${EXE} run /etc/hydrovu

echo "=== Init replica ==="

export POND=/tmp/pond-replica
rm -rf ${POND}

${EXE} init --from-backup remote-config-local-init.yaml

echo "=== Run again ==="

POND=/tmp/pond
${EXE} run /etc/hydrovu

echo "=== Sync replica ==="

POND=/tmp/pond-replica
${EXE} control --mode=sync

echo "=== Run again ==="

POND=/tmp/pond
${EXE} run /etc/hydrovu

echo "=== Sync replica ==="

POND=/tmp/pond-replica
${EXE} control --mode=sync

echo "=== Run again ==="

POND=/tmp/pond
${EXE} run /etc/hydrovu

echo "=== Sync replica ==="

POND=/tmp/pond-replica
${EXE} control --mode=sync
