#!/bin/bash -e

POND=/tmp/dynpond
export POND

rm -rf ${POND}
cp -r /tmp/pond ${POND}

CONFIG_FILE="test-hydrovu-dynamic-config.yaml"

echo "📂 Creating dynamic directory /test-locations..."

cargo run --bin pond mknod dynamic-dir /test-locations $CONFIG_FILE

echo "✅ Dynamic directory created!"

#echo "📋 Listing virtual FileSeries in /test-locations..."
cargo run --bin pond list '/test-locations/**'

#cargo run --bin pond cat '/test-locations/BDock'
#cargo run --bin pond cat '/test-locations/Silver' --query "select count(*) from series"
#cargo run --bin pond cat '/test-locations/Silver' --query "select count(*) from series"
#cargo run --bin pond cat '/test-locations/Princess' 

cargo run --bin pond query --show
