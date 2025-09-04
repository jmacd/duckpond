#!/bin/bash -e

POND=/tmp/dynpond
export POND

rm -rf ${POND}
cp -r /tmp/hydrotestpond ${POND}

CONFIG_FILE="test-hydrovu-dynamic-config.yaml"

echo "📂 Creating dynamic directory /test-locations..."

cargo run --bin pond mknod dynamic-dir /test-locations $CONFIG_FILE

echo ""
echo "✅ Dynamic directory created!"
echo ""

echo "📋 Listing virtual FileSeries in /test-locations..."
POND=$POND_PATH cargo run --bin pond list '/test-locations/**'
