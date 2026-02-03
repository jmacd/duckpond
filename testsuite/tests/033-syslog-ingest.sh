#!/bin/bash
# EXPERIMENT: Ingest Real Linux System Logs with logfile-ingest
# DESCRIPTION: Configure rsyslog with nocompress rotation, generate log entries
#              via logger command, ingest them into pond, verify counts increase
#
# This tests the realistic scenario of ingesting /var/log/syslog on a Linux host
#
set -e

echo "=== Experiment: Syslog Ingest with logfile-ingest ==="
echo ""

export POND=/pond
pond init

#############################
# STEP 1: Configure rsyslog and logrotate
#############################

echo "=== Step 1: Configure rsyslog and logrotate ==="

# Create logrotate config with NOCOMPRESS for our testing
cat > /etc/logrotate.d/syslog-nocompress << 'EOF'
/var/log/syslog
/var/log/messages
{
    rotate 4
    weekly
    missingok
    notifempty
    nocompress
    create 644 root adm
    postrotate
        /usr/lib/rsyslog/rsyslog-rotate 2>/dev/null || true
    endscript
}
EOF

echo "✓ Created logrotate config with nocompress"

# Clear existing rsyslog configs to avoid duplicate logging
rm -f /etc/rsyslog.d/*.conf
# Comment out the default syslog rule in rsyslog.conf to avoid duplicates
sed -i 's|^\\*.*;.*syslog|#&|' /etc/rsyslog.conf
# Configure rsyslog to write ONLY to /var/log/syslog (no duplicates)
cat > /etc/rsyslog.d/50-experiment.conf << 'EOF'
# Log everything to syslog for testing (single destination)
*.*     /var/log/syslog
EOF

echo "✓ Configured rsyslog to write to /var/log/syslog"

# Disable kernel logging (not available in containers)
sed -i 's/module(load="imklog")/#module(load="imklog")/' /etc/rsyslog.conf

# Start rsyslog
rsyslogd
sleep 1
echo "✓ Started rsyslog"

# Verify rsyslog is running
if pidof rsyslogd > /dev/null 2>&1 || pgrep rsyslogd > /dev/null 2>&1; then
    echo "✓ rsyslogd is running"
else
    echo "❌ rsyslogd failed to start"
    exit 1
fi

#############################
# STEP 2: Generate initial log entries
#############################

echo ""
echo "=== Step 2: Generate initial log entries ==="

# Generate some initial log entries via logger
for i in $(seq 1 10); do
    logger -t duckpond-test "Initial log entry ${i}: Starting experiment at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
done

# Give rsyslog time to flush
sleep 1

# Count initial lines
INITIAL_HOST_LINES=$(wc -l < /var/log/syslog 2>/dev/null || echo 0)
echo "Host syslog has ${INITIAL_HOST_LINES} lines"

if [ "${INITIAL_HOST_LINES}" -eq 0 ]; then
    echo "❌ No log entries in /var/log/syslog - rsyslog may not be working"
    cat /var/log/syslog 2>/dev/null || echo "(file doesn't exist)"
    exit 1
fi

# Show sample of logs
echo ""
echo "--- Sample log entries ---"
head -5 /var/log/syslog
echo "..."

#############################
# STEP 3: Configure logfile-ingest factory
#############################

echo ""
echo "=== Step 3: Configure logfile-ingest factory ==="

cat > /tmp/syslog-ingest.yaml << 'EOF'
archived_pattern: /var/log/syslog.*
active_pattern: /var/log/syslog
pond_path: /logs/system
EOF

pond mkdir -p /etc/system.d
pond mkdir -p /logs/system

pond mknod logfile-ingest /etc/system.d/10-syslog --config-path /tmp/syslog-ingest.yaml
echo "✓ Created logfile-ingest factory node"

#############################
# STEP 4: First ingest run
#############################

echo ""
echo "=== Step 4: First ingest run ==="

pond run /etc/system.d/10-syslog 2>&1 | tee /tmp/run1.log

# Count lines in pond
POND_LINES_1=$(pond cat /logs/system/syslog 2>/dev/null | wc -l)
echo ""
echo "After first run: ${POND_LINES_1} lines in pond (${INITIAL_HOST_LINES} on host)"

if [ "${POND_LINES_1}" -ne "${INITIAL_HOST_LINES}" ]; then
    echo "⚠ Line count mismatch: pond=${POND_LINES_1}, host=${INITIAL_HOST_LINES}"
fi

#############################
# STEP 5: Generate more log entries
#############################

echo ""
echo "=== Step 5: Generate more log entries ==="

# Generate additional entries
for i in $(seq 1 15); do
    logger -t duckpond-test "Additional log entry ${i}: Testing append at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    sleep 0.1
done

sleep 1

NEW_HOST_LINES=$(wc -l < /var/log/syslog)
EXPECTED_NEW=$((NEW_HOST_LINES - INITIAL_HOST_LINES))
echo "Host syslog now has ${NEW_HOST_LINES} lines (+${EXPECTED_NEW} new)"

#############################
# STEP 6: Second ingest run (should detect appends)
#############################

echo ""
echo "=== Step 6: Second ingest run (detect appends) ==="

RUST_LOG=provider::factory::logfile_ingest=debug,info pond run /etc/system.d/10-syslog 2>&1 | tee /tmp/run2.log

POND_LINES_2=$(pond cat /logs/system/syslog 2>/dev/null | wc -l)
POND_ADDED=$((POND_LINES_2 - POND_LINES_1))

echo ""
echo "After second run: ${POND_LINES_2} lines in pond (+${POND_ADDED} added)"
echo "Expected: ${NEW_HOST_LINES} lines (host has ${NEW_HOST_LINES})"

#############################
# VERIFICATION
#############################

echo ""
echo "=== VERIFICATION ==="

# Check run2 log for append detection
if grep -q "Appending to" /tmp/run2.log; then
    echo "✓ Second run detected append correctly"
    grep "Appending to" /tmp/run2.log
elif grep -q "no changes" /tmp/run2.log; then
    echo "❌ Second run reported 'no changes' but host file grew!"
    echo "This might indicate the bao_outboard cumulative_size bug"
    exit 1
else
    echo "⚠ Unexpected output from second run"
    cat /tmp/run2.log
fi

# Verify final line counts match
if [ "${POND_LINES_2}" -eq "${NEW_HOST_LINES}" ]; then
    echo "✓ Pond line count matches host: ${POND_LINES_2} lines"
else
    echo "⚠ Line count mismatch: pond=${POND_LINES_2}, host=${NEW_HOST_LINES}"
    echo "  Difference: $((NEW_HOST_LINES - POND_LINES_2)) lines"
fi

# Show last few lines from pond to verify content
echo ""
echo "--- Last 5 lines from pond ---"
pond cat /logs/system/syslog 2>/dev/null | tail -5

echo ""
echo "=== Experiment Complete ==="
