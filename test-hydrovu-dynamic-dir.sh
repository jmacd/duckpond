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

#cargo run --bin pond query --show

cargo run --bin pond detect-overlaps "/hydrovu/devices/**/SilverVulink*.series"

cargo run --bin pond set-temporal-bounds /hydrovu/devices/6582334615060480/SilverVulink1.series \
  --min-time "2024-01-01 00:00:00" \
  --max-time "2024-05-30 23:59:59"

cargo run --bin pond detect-overlaps "/hydrovu/devices/**/SilverVulink*.series"
