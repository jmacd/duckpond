#!/bin/bash
# EXPERIMENT: Weblog format provider end-to-end
# DESCRIPTION: Test weblog:// parsing of Combined Log Format through the full
#   pipeline: host file queries, logfile-ingest mirroring, in-pond queries via
#   weblog:// scheme, sql-derived-series aggregation, and gzip compression.
# EXPECTED: CLF lines parsed into typed columns (timestamp, status, etc.)
#   and sql-derived-series produces correct aggregated output.
#
set -e
source check.sh

echo "=== Experiment: Weblog Format Provider ==="

export POND=/pond
pond init

#############################
# STEP 1: Create sample CLF access logs
#############################

echo ""
echo "=== Step 1: Create sample access logs ==="

mkdir -p /var/log/nginx

cat > /var/log/nginx/access.log << 'EOF'
93.184.216.34 - - [15/Mar/2025:10:00:00 +0000] "GET / HTTP/1.1" 200 5120 "https://search.example.com/results" "Mozilla/5.0 (X11; Linux x86_64)"
93.184.216.34 - - [15/Mar/2025:10:00:01 +0000] "GET /about HTTP/1.1" 200 3200 "https://duckpond.example.com/" "Mozilla/5.0 (X11; Linux x86_64)"
10.0.0.1 - admin [15/Mar/2025:10:00:02 +0000] "POST /api/data HTTP/2.0" 201 64 "-" "curl/8.0"
172.16.0.50 - - [15/Mar/2025:10:05:00 +0000] "GET /style.css HTTP/1.1" 200 1024 "https://duckpond.example.com/" "Mozilla/5.0 (Windows NT 10.0)"
172.16.0.50 - - [15/Mar/2025:10:05:01 +0000] "GET /logo.png HTTP/1.1" 200 8192 "https://duckpond.example.com/" "Mozilla/5.0 (Windows NT 10.0)"
192.168.1.100 - - [15/Mar/2025:11:00:00 +0000] "GET / HTTP/1.1" 200 5120 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
192.168.1.100 - - [15/Mar/2025:11:00:05 +0000] "GET /about HTTP/1.1" 200 3200 "https://duckpond.example.com/" "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
192.168.1.100 - - [15/Mar/2025:11:00:10 +0000] "GET /docs/guide HTTP/1.1" 200 7500 "https://duckpond.example.com/about" "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
10.0.0.99 - - [15/Mar/2025:11:30:00 +0000] "GET /missing HTTP/1.1" 404 128 "-" "Mozilla/5.0 (compatible; bot/1.0)"
10.0.0.99 - - [15/Mar/2025:11:30:01 +0000] "GET / HTTP/1.1" 500 256 "-" "Mozilla/5.0 (compatible; bot/1.0)"
EOF

echo "Created access.log with 10 entries"

# Create a gzip-compressed archived log
cat > /tmp/access.log.1 << 'EOF'
203.0.113.5 - - [14/Mar/2025:23:00:00 +0000] "GET / HTTP/1.1" 200 5120 "-" "Mozilla/5.0"
203.0.113.5 - - [14/Mar/2025:23:30:00 +0000] "GET /about HTTP/1.1" 200 3200 "-" "Mozilla/5.0"
203.0.113.5 - - [14/Mar/2025:23:59:59 +0000] "GET /docs/guide HTTP/1.1" 301 0 "-" "Mozilla/5.0"
EOF

gzip -c /tmp/access.log.1 > /var/log/nginx/access.log.1.gz
echo "Created access.log.1.gz with 3 archived entries"

#############################
# STEP 2: Test host+weblog:// direct query
#############################

echo ""
echo "=== Step 2: Query host file directly via host+weblog:// ==="

# Basic query: all rows
pond cat "host+weblog:///var/log/nginx/access.log" --format=table \
  --sql "SELECT COUNT(*) as total FROM source" > /tmp/host-count.txt 2>&1
cat /tmp/host-count.txt
check 'grep -q "10" /tmp/host-count.txt' "host+weblog counts 10 lines"

# Typed column query: status as integer, no CAST needed
pond cat "host+weblog:///var/log/nginx/access.log" --format=table \
  --sql "SELECT method, path, status FROM source WHERE status >= 400 ORDER BY status" \
  > /tmp/host-errors.txt 2>&1
cat /tmp/host-errors.txt
check 'grep -q "404" /tmp/host-errors.txt' "host+weblog parses status as integer (404)"
check 'grep -q "500" /tmp/host-errors.txt' "host+weblog parses status as integer (500)"

# Typed column query: body_bytes_sent as integer, SUM works
pond cat "host+weblog:///var/log/nginx/access.log" --format=table \
  --sql "SELECT SUM(body_bytes_sent) as total_bytes FROM source" \
  > /tmp/host-bytes.txt 2>&1
cat /tmp/host-bytes.txt
check 'grep -q "33804" /tmp/host-bytes.txt' "host+weblog body_bytes_sent SUM = 33804"

# Timestamp query: date_trunc works on native timestamp
pond cat "host+weblog:///var/log/nginx/access.log" --format=table \
  --sql "SELECT date_trunc('hour', timestamp) as hour, COUNT(*) as cnt FROM source GROUP BY hour ORDER BY hour" \
  > /tmp/host-hourly.txt 2>&1
cat /tmp/host-hourly.txt
check 'grep -c "2025-03-15" /tmp/host-hourly.txt | grep -q "2"' "host+weblog groups into 2 hourly buckets"

# Gzip compressed
pond cat "host+weblog+gzip:///var/log/nginx/access.log.1.gz" --format=table \
  --sql "SELECT COUNT(*) as total FROM source" > /tmp/host-gz-count.txt 2>&1
cat /tmp/host-gz-count.txt
check 'grep -q "3" /tmp/host-gz-count.txt' "host+weblog+gzip counts 3 archived lines"

#############################
# STEP 3: Ingest via logfile-ingest
#############################

echo ""
echo "=== Step 3: Ingest logs via logfile-ingest ==="

pond mkdir -p /system/run
pond mkdir -p /logs/web/test

cat > /tmp/web-ingest.yaml << 'EOF'
archived_pattern: /var/log/nginx/access.log.*
active_pattern: /var/log/nginx/access.log
pond_path: /logs/web/test
EOF

pond mknod logfile-ingest /system/run/10-weblogs --config-path /tmp/web-ingest.yaml
echo "Created logfile-ingest factory node"

pond run /system/run/10-weblogs 2>&1 | tee /tmp/ingest-run.log

echo ""
echo "--- Pond contents ---"
pond list '/logs/web/test/*'

# Verify active log was ingested
POND_SIZE=$(pond cat /logs/web/test/access.log 2>/dev/null | wc -c | tr -d ' ')
HOST_SIZE=$(wc -c < /var/log/nginx/access.log | tr -d ' ')
check '[ "${POND_SIZE}" = "${HOST_SIZE}" ]' "active log ingested (${POND_SIZE} bytes)"

# Verify archived log was ingested
check 'pond list "/logs/web/test/*" 2>/dev/null | grep -q "access.log.1.gz"' "archived gzip log ingested"

#############################
# STEP 4: Query in-pond via weblog://
#############################

echo ""
echo "=== Step 4: Query in-pond files via weblog:// ==="

pond cat "weblog:///logs/web/test/access.log" --format=table \
  --sql "SELECT method, path, status FROM source WHERE method = 'GET' AND status = 200 ORDER BY path" \
  > /tmp/pond-get200.txt 2>&1
cat /tmp/pond-get200.txt

check 'grep -q "/about" /tmp/pond-get200.txt' "pond weblog:// returns /about"
check 'grep -q "/docs/guide" /tmp/pond-get200.txt' "pond weblog:// returns /docs/guide"

# Verify the compressed archived file too
pond cat "weblog+gzip:///logs/web/test/access.log.1.gz" --format=table \
  --sql "SELECT path, status FROM source ORDER BY path" \
  > /tmp/pond-gz.txt 2>&1
cat /tmp/pond-gz.txt
check 'grep -q "/about" /tmp/pond-gz.txt' "pond weblog+gzip:// reads archived log"

#############################
# STEP 5: sql-derived-series with weblog:// patterns
#############################

echo ""
echo "=== Step 5: sql-derived-series aggregation ==="

pond mkdir -p /derived/web/test

# Pageview aggregation: hourly counts by page, exclude static assets
cat > /tmp/pageviews.yaml << 'EOF'
patterns:
  logs: "weblog:///logs/web/test/access.log"
query: >-
  SELECT
    date_trunc('hour', timestamp) as timestamp,
    path,
    COUNT(*) as requests,
    COUNT(DISTINCT remote_addr) as unique_visitors
  FROM logs
  WHERE status >= 200 AND status < 400
    AND method = 'GET'
    AND path NOT LIKE '%.css'
    AND path NOT LIKE '%.png'
  GROUP BY date_trunc('hour', timestamp), path
  ORDER BY timestamp, requests DESC
EOF

pond mknod sql-derived-series /derived/web/test/pageviews.series \
  --config-path /tmp/pageviews.yaml
echo "Created sql-derived-series node for pageviews"

# Query the derived data
pond cat /derived/web/test/pageviews.series --format=table \
  --sql "SELECT * FROM source" > /tmp/derived-pageviews.txt 2>&1
cat /tmp/derived-pageviews.txt

# Verify aggregation results
# Hour 10: GET / (200), GET /about (200) => 2 page requests (excluding .css and .png)
# Hour 11: GET / (200), GET /about (200), GET /docs/guide (200) => 3 page requests
check 'grep -q "/" /tmp/derived-pageviews.txt' "derived pageviews contains root path"
check 'grep -q "/about" /tmp/derived-pageviews.txt' "derived pageviews contains /about"
check 'grep -q "/docs/guide" /tmp/derived-pageviews.txt' "derived pageviews contains /docs/guide"
# Static assets should be excluded
check '! grep -q "/style.css" /tmp/derived-pageviews.txt' "derived pageviews excludes .css"
check '! grep -q "/logo.png" /tmp/derived-pageviews.txt' "derived pageviews excludes .png"

# Status code distribution
cat > /tmp/statuscodes.yaml << 'EOF'
patterns:
  logs: "weblog:///logs/web/test/access.log"
query: >-
  SELECT
    date_trunc('hour', timestamp) as timestamp,
    CASE
      WHEN status >= 200 AND status < 300 THEN '2xx'
      WHEN status >= 300 AND status < 400 THEN '3xx'
      WHEN status >= 400 AND status < 500 THEN '4xx'
      WHEN status >= 500 THEN '5xx'
    END as status_class,
    COUNT(*) as count
  FROM logs
  GROUP BY date_trunc('hour', timestamp), status_class
  ORDER BY timestamp
EOF

pond mknod sql-derived-series /derived/web/test/status_codes.series \
  --config-path /tmp/statuscodes.yaml

pond cat /derived/web/test/status_codes.series --format=table \
  --sql "SELECT * FROM source" > /tmp/derived-status.txt 2>&1
cat /tmp/derived-status.txt

check 'grep -q "2xx" /tmp/derived-status.txt' "status distribution includes 2xx"
check 'grep -q "4xx" /tmp/derived-status.txt' "status distribution includes 4xx"
check 'grep -q "5xx" /tmp/derived-status.txt' "status distribution includes 5xx"

#############################
# STEP 6: Verify typed columns in derived queries
#############################

echo ""
echo "=== Step 6: Verify typed column behavior ==="

# Remote user: admin should appear, dashes should be null
pond cat "host+weblog:///var/log/nginx/access.log" --format=table \
  --sql "SELECT remote_addr, remote_user, method FROM source WHERE remote_user IS NOT NULL" \
  > /tmp/typed-user.txt 2>&1
cat /tmp/typed-user.txt
check 'grep -q "admin" /tmp/typed-user.txt' "remote_user = admin (non-null)"
# Only 1 row should have non-null remote_user
ROW_COUNT=$(grep -c "admin" /tmp/typed-user.txt || true)
check '[ "${ROW_COUNT}" = "1" ]' "exactly 1 row with non-null remote_user"

# Referer: dash becomes null
pond cat "host+weblog:///var/log/nginx/access.log" --format=table \
  --sql "SELECT path, http_referer FROM source WHERE http_referer IS NOT NULL ORDER BY path" \
  > /tmp/typed-referer.txt 2>&1
cat /tmp/typed-referer.txt
check 'grep -q "search.example.com" /tmp/typed-referer.txt' "referer parsed from Combined format"

check_finish
