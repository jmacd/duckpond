POND=/tmp/pond

cargo build --workspace || exit 1

EXE=target/debug/pond

rm -rf ${POND}

export POND

export RUST_LOG

echo "=== INIT ==="
${EXE} init

echo "=== MKDIR ==="
${EXE} mkdir /etc
${EXE} mkdir /etc/system.d

echo "=== CREATE HYDROVU CONFIG ==="
${EXE} mknod hydrovu /etc/hydrovu --config-path hydrovu-config.yaml

echo "=== CREATE POST-COMMIT REMOTE CONFIG ==="
${EXE} mknod remote /etc/system.d/10-remote --config-path remote-config.yaml

echo "=== CAT HYDROVU ==="
${EXE} cat /etc/hydrovu

echo "=== CAT REMOTE ==="
${EXE} cat /etc/system.d/10-remote

echo "=== RUN HYDROVU (triggers post-commit) ==="
${EXE} run /etc/hydrovu

echo "=== TEST COMPLETE ==="
echo "Post-commit remote factory should have executed after hydrovu run"
