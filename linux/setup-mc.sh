#!/bin/bash
# Install MinIO client (mc) to ~/.local/bin.
#
# Usage:
#   ./linux/setup-mc.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env.sh"

MC="$HOME/.local/bin/mc"
mkdir -p "$HOME/.local/bin"

if [ -x "$MC" ]; then
    echo "mc already installed at $MC"
else
    ARCH=$(dpkg --print-architecture)
    case "$ARCH" in
        arm64|aarch64) ARCH=arm64 ;;
        amd64|x86_64)  ARCH=amd64 ;;
        *)             echo "Unsupported architecture: $ARCH"; exit 1 ;;
    esac
    echo "Downloading mc for linux-$ARCH..."
    curl -sL "https://dl.min.io/client/mc/release/linux-$ARCH/mc" -o "$MC"
    chmod +x "$MC"
    echo "Installed mc at $MC"
fi

echo "Configuring local MinIO alias..."
"$MC" alias set local "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
echo ""
"$MC" alias list local
