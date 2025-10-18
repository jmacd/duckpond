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

echo "=== CREATE ==="
${EXE} mknod hydrovu /etc/hydrovu --config-path hydrovu-config.yaml

echo "=== CAT ==="
${EXE} cat /etc/hydrovu

echo "=== RUN ==="
${EXE} run /etc/hydrovu




