#!/bin/bash
# Build the experiment container image
#
# This script:
# 1. Cross-compiles pond for linux (matching host arch)
# 2. Builds the experiment Docker image
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== DuckPond Experiment Image Builder ==="
echo "Repository: ${REPO_ROOT}"

# Determine target architecture
HOST_OS=$(uname -s)
HOST_ARCH=$(uname -m)

echo "Host: ${HOST_OS} ${HOST_ARCH}"

# Build pond binary for the container target
# Match the host architecture for native container execution
if [[ "${HOST_ARCH}" == "arm64" ]] || [[ "${HOST_ARCH}" == "aarch64" ]]; then
    CONTAINER_TARGET="aarch64-unknown-linux-gnu"
    DOCKER_PLATFORM="linux/arm64"
else
    CONTAINER_TARGET="x86_64-unknown-linux-gnu"
    DOCKER_PLATFORM="linux/amd64"
fi

BINARY_PATH="${REPO_ROOT}/target/${CONTAINER_TARGET}/release/pond"
echo "Target: ${CONTAINER_TARGET} (Docker: ${DOCKER_PLATFORM})"

if [[ "${HOST_OS}" == "Darwin" ]]; then
    echo ""
    echo "=== Cross-compiling for Linux (${CONTAINER_TARGET}) ==="
    
    # Ensure target is installed
    rustup target add "${CONTAINER_TARGET}" 2>/dev/null || true
    
    # Try zigbuild if available (works best for cross-compilation)
    if command -v cargo-zigbuild &> /dev/null; then
        echo "Using cargo-zigbuild..."
        cd "${REPO_ROOT}"
        cargo zigbuild --release --bin pond --target ${CONTAINER_TARGET}
    else
        echo "ERROR: cargo-zigbuild not found."
        echo "Install with: cargo install cargo-zigbuild"
        exit 1
    fi
else
    echo ""
    echo "=== Building for native Linux ==="
    cd "${REPO_ROOT}"
    cargo build --release --bin pond
    BINARY_PATH="${REPO_ROOT}/target/release/pond"
fi

# Verify binary exists
if [[ ! -f "${BINARY_PATH}" ]]; then
    echo "ERROR: Binary not found at ${BINARY_PATH}"
    exit 1
fi

echo ""
echo "=== Binary info ==="
file "${BINARY_PATH}"
ls -lh "${BINARY_PATH}"

# Copy binary to experiments directory for Docker build
cp "${BINARY_PATH}" "${SCRIPT_DIR}/pond"

echo ""
echo "=== Building Docker image ==="
cd "${SCRIPT_DIR}"

# Ensure helpers are executable
chmod +x "${SCRIPT_DIR}/helpers/"* 2>/dev/null || true

# Create pond2 symlink for helper
ln -sf pond1 "${SCRIPT_DIR}/helpers/pond2" 2>/dev/null || true

docker build \
    -f Dockerfile.experiment \
    -t duckpond-experiment:latest \
    .

# Clean up copied binary
rm -f "${SCRIPT_DIR}/pond"

echo ""
echo "=== Build complete ==="
echo "Image: duckpond-experiment:latest"
echo ""
echo "Test with:"
echo "  docker run --rm -it duckpond-experiment:latest"
echo "  pond --help"
echo ""
echo "For S3 experiments:"
echo "  docker-compose up -d minio"
echo "  docker-compose run --rm duckpond"
