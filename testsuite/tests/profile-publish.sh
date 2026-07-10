#!/bin/bash
# Local samply profile of a publish (sitegen build) step. Scaled-up synthetic
# pond to drive temporal-reduce + export hard, mirroring the watershop site
# build. NOT a checked test -- run manually:  bash profile-publish.sh
set -e
B=$(cd "$(dirname "$0")/../.." && pwd)/target/release/pond
POND=/tmp/profpond
OUT=/tmp/profsite
rm -rf "$POND" "$OUT"
export POND DUCKPOND_VENDOR=$(cd "$(dirname "$0")/../.." && pwd)/crates/sitegen/vendor/dist

START=$(date -u -v-1095d +%Y-%m-%dT00:00:00Z 2>/dev/null || date -u -d "1095 days ago" +%Y-%m-%dT00:00:00Z)
END=$(date -u +%Y-%m-%dT00:00:00Z)

$B init --birthplace prof >/dev/null

# 8 sensors, 5-min interval, 3 years -> ~315k pts each
{ echo "entries:"; for i in $(seq 1 8); do cat <<E
  - name: "s${i}_v"
    factory: "synthetic-timeseries"
    config: { start: "$START", end: "$END", interval: "5m", points: [ { name: "p${i}.C", components: [ { type: sine, amplitude: 4.0, period: "24h", offset: 14.0 } ] } ] }
E
done; } > /tmp/p-sensors.yaml

{ echo "entries:"; for i in $(seq 1 8); do cat <<E
  - name: "Dock${i}"
    factory: "timeseries-pivot"
    config: { pattern: "/sensors/s${i}_v", columns: [ "p${i}.C" ] }
E
done; } > /tmp/p-single.yaml

cat > /tmp/p-reduce.yaml <<'Y'
entries:
  - name: "single_param"
    factory: "temporal-reduce"
    config:
      in_pattern: "/singled/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1m","5m","15m","1h","4h","24h"]
      aggregations: [ {type: avg, columns: ["*"]}, {type: min, columns: ["*"]}, {type: max, columns: ["*"]} ]
Y

$B mknod dynamic-dir /sensors --config-path /tmp/p-sensors.yaml >/dev/null
$B mknod dynamic-dir /singled --config-path /tmp/p-single.yaml >/dev/null
$B mknod dynamic-dir /reduced --config-path /tmp/p-reduce.yaml >/dev/null
$B mkdir -p /site >/dev/null
printf -- '---\ntitle: x\nlayout: default\n---\n# x\n' > /tmp/p-idx.md
$B copy host:///tmp/p-idx.md /site/index.md >/dev/null

cat > /tmp/p-site.yaml <<'Y'
site: { title: prof, base_url: "/" }
exports:
  - { name: reduced, pattern: "/reduced/single_param/*/*.series", target_points: 1500 }
routes:
  - { name: home, type: static, slug: "", page: "/site/index.md" }
Y
$B mknod sitegen /site.yaml --config-path /tmp/p-site.yaml >/dev/null

echo "=== profiling build (samply) ==="
$B run /site.yaml build "$OUT"
echo "profile: /tmp/profile.json.gz  -> samply load /tmp/profile.json.gz"
