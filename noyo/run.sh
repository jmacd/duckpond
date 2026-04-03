#!/bin/sh
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
export POND=${SCRIPTS}/pond
export RUST_LOG=info

# Load credentials (HydroVu keys)
if [ -f "${SCRIPTS}/deploy.env" ]; then
  . "${SCRIPTS}/deploy.env"
fi
if [ -f ~/.zshrc.private ]; then
  . ~/.zshrc.private
fi

CARGO="cargo run --release -p cmd --"

${CARGO} run /system/etc/20-hydrovu collect
