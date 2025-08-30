POND=/tmp/pond

cargo build --workspace || exit 1

EXE=target/debug/pond

rm -rf ${POND}

export POND

export DUCKPOND_LOG

echo "=== INIT ==="
${EXE} init

echo "=== CREATE ==="
${EXE} hydrovu create hydrovu-config.yaml

echo "=== RUN ==="
${EXE} hydrovu run hydrovu-config.yaml




