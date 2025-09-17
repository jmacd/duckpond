#!/bin/bash -e

POND=/tmp/dynpond
export POND

rm -rf ${POND}
cp -r /tmp/pond ${POND}

CONFIG_FILE="test-hydrovu-dynamic-config.yaml"

echo "📂 Creating dynamic directory /test-locations..."

cargo run --bin pond mknod dynamic-dir /test-locations $CONFIG_FILE

echo "✅ Dynamic directory created!"

cargo run --bin pond list '/test-locations/**'

cargo run --bin pond list '/hydrovu/devices/**'

echo "✅ First detect-overlaps"

cargo run --bin pond detect-overlaps "/hydrovu/devices/**/SilverVulink*.series"

echo "✅ First set temporal override"

cargo run --bin pond set-temporal-bounds /hydrovu/devices/6582334615060480/SilverVulink1.series \
  --min-time "2024-01-01 00:00:00" \
  --max-time "2024-05-30 23:59:59"

echo "✅ Next detect-overlap should see the effect"

cargo run --bin pond detect-overlaps "/hydrovu/devices/**/SilverVulink*.series"

#echo "✅ Should not print out-of-range rows"
#cargo run --bin pond cat /hydrovu/devices/6582334615060480/SilverVulink1.series

#echo "✅ Should print around 11,000 rows"
#cargo run --bin pond cat '/test-locations/Silver' --query "select count(*) from series"

echo "✅ Sample 1-hour aggregated data from BDock"
cargo run --bin pond cat '/test-locations/BDockDownsampled/res=1d.series'
#--query "select * from series limit 10"
