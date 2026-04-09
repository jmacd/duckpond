#!/bin/sh
#
# setup.sh -- Initialize the cross-pond example.
#
# Imports three source ponds (noyo, water, septic) from their S3 backups
# and installs a combined sitegen factory that exports metrics from all three.
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

# Discover pond_ids from the local source ponds.
# In production these would be configured in deploy.env or discovered
# via 'pond run host+remote:///config.yaml list-ponds'.
echo "=== Discovering source pond IDs ==="

NOYO_POND_ID=$(POND=${SCRIPTS}/../noyo/pond ${CARGO} config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Noyo pond_id: ${NOYO_POND_ID}"

SEPTIC_POND_ID=$(POND=${SCRIPTS}/../septic/pond ${CARGO} config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Septic pond_id: ${SEPTIC_POND_ID}"

WATER_POND_ID=$(POND=${SCRIPTS}/../water/pond ${CARGO} config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Water pond_id: ${WATER_POND_ID}"

# Wipe and initialize
rm -rf "${POND_DIR}"
${CARGO} init

# Create directory structure
${CARGO} mkdir -p /system/etc
${CARGO} mkdir -p /sources

# Copy site templates into the pond
${CARGO} copy host:///${SCRIPTS}/site /system/site

# Generate import configs with discovered pond_ids.
# URLs are baked (dynamic config, not secrets); credentials stay as
# ${env:} references for runtime expansion.
make_import_config() {
    local SOURCE_URL="$1"
    local POND_ID="$2"
    local LOCAL_PATH="$3"
    local OUTFILE="$4"
    cat > "${OUTFILE}" <<ENDCFG
region: "us-east-1"
url: "${SOURCE_URL}/pond-${POND_ID}"
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

make_import_config "${NOYO_S3_URL}"   "${NOYO_POND_ID}"   "/sources/noyo"   "${NOYO_CFG}"
make_import_config "${SEPTIC_S3_URL}" "${SEPTIC_POND_ID}" "/sources/septic" "${SEPTIC_CFG}"
make_import_config "${WATER_S3_URL}"  "${WATER_POND_ID}"  "/sources/water"  "${WATER_CFG}"

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
