#!/bin/sh
#
# setup.sh -- Initialize the cross-pond example.
#
# Imports three source ponds (noyo, water, septic) from their S3 backups
# and installs a combined sitegen factory that exports metrics from all three.
#
# Each source pond backs up to its own dedicated S3 bucket.
# The bucket name is the stable identity -- no UUID discovery needed.
#
# Prerequisites:
#   - deploy.env configured (cp deploy.env.example deploy.env)
#   - All three source ponds (noyo, water, septic) have been backed up to S3
#   - cargo build works in the workspace root
#
# The pond is stored in ./pond/.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
POND_DIR=${SCRIPTS}/pond

export POND=${POND_DIR}

# Load deployment config
. "${SCRIPTS}/deploy.env"

# Cargo run helper
CARGO="cargo run --release -p cmd --"

# Wipe and initialize
rm -rf "${POND_DIR}"
${CARGO} init

# Create directory structure
${CARGO} mkdir -p /system/etc
${CARGO} mkdir -p /sources

# Copy site templates into the pond
${CARGO} copy host:///${SCRIPTS}/site /system/site

# Generate import configs.
# Each source pond has its own bucket (set in deploy.env).
# URLs are used directly -- no pond-{uuid} prefix needed.
# Credentials stay as ${env:} references for runtime expansion.
make_import_config() {
    local SOURCE_URL="$1"
    local LOCAL_PATH="$2"
    local OUTFILE="$3"
    cat > "${OUTFILE}" <<ENDCFG
region: "us-east-1"
url: "${SOURCE_URL}"
endpoint: "\${env:S3_ENDPOINT}"
access_key_id: "\${env:S3_ACCESS_KEY}"
secret_access_key: "\${env:S3_SECRET_KEY}"
allow_http: \${env:S3_ALLOW_HTTP}
import:
  source_path: "/**"
  local_path: "${LOCAL_PATH}"
ENDCFG
}

NOYO_CFG=$(mktemp)
SEPTIC_CFG=$(mktemp)
WATER_CFG=$(mktemp)

make_import_config "${NOYO_S3_URL}"   "/sources/noyo"   "${NOYO_CFG}"
make_import_config "${SEPTIC_S3_URL}" "/sources/septic" "${SEPTIC_CFG}"
make_import_config "${WATER_S3_URL}"  "/sources/water"  "${WATER_CFG}"

# Install import factories
${CARGO} mknod remote /system/etc/10-noyo   --config-path "${NOYO_CFG}"
${CARGO} mknod remote /system/etc/11-septic --config-path "${SEPTIC_CFG}"
${CARGO} mknod remote /system/etc/12-water  --config-path "${WATER_CFG}"

# Install combined sitegen
${CARGO} mknod sitegen /system/etc/90-sitegen --config-path ${SCRIPTS}/site.yaml

rm -f "${NOYO_CFG}" "${SEPTIC_CFG}" "${WATER_CFG}"

echo
echo "=== Setup complete ==="
echo "Next: ./import.sh     # pull data from all three source ponds"
echo "Then: ./generate.sh   # build the combined site"
