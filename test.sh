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

echo "=== RUN HYDROVU (triggers post-commit) ==="
${EXE} run /etc/hydrovu

echo ""
echo "=== VERIFY BACKUP CREATED ==="
echo "Checking for backup files in ${BACKUPS}:"
if [ -d "${BACKUPS}" ]; then
    find ${BACKUPS} -type f -exec ls -lh {} \; | head -20
    echo ""
    if [ -f "${BACKUPS}/backups/version-000001/metadata.json" ]; then
        echo "=== BACKUP METADATA ==="
        cat ${BACKUPS}/backups/version-000001/metadata.json | jq .
    fi
else
    echo "❌ No backup directory created at ${BACKUPS}"
fi

echo ""
echo "=== TEST COMPLETE ==="
echo "Post-commit remote factory should have executed after hydrovu run"
