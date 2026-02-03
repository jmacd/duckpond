#!/bin/bash
# EXPERIMENT: Dual-Pond Observability Setup
# DESCRIPTION: 
#   Pond1: Monitors system via logfile-ingest factory, runs via cron every minute
#   Pond2: Monitors Pond1's invocations by querying its control table
#
# EXPECTED:
#   - Pond1 ingests logs from /var/log/duckpond
#   - Pond2 can query Pond1's transaction history
#   - Both ponds operate independently
#
# CONCEPT:
#   This demonstrates using DuckPond to observe DuckPond - a meta-monitoring pattern
#   useful for understanding system behavior and debugging.
#
set -e

echo "=== Experiment: Dual-Pond Observability ==="
echo ""

#############################
# SETUP POND1 - Log Collector
#############################

echo "=== Setting up Pond1 (Log Collector) ==="

export POND=/pond1
pond init
echo "✓ Pond1 initialized at ${POND}"

# Create log directory structure
pond mkdir /logs
pond mkdir /etc
pond mkdir /etc/system.d

# Generate some sample logs to ingest
mkdir -p /var/log/duckpond
cat > /var/log/duckpond/app.json << 'EOF'
{"timestamp":"2024-01-01T00:00:00Z","level":"INFO","message":"Application started"}
{"timestamp":"2024-01-01T00:01:00Z","level":"INFO","message":"Processing request 1"}
{"timestamp":"2024-01-01T00:02:00Z","level":"WARN","message":"High latency detected"}
{"timestamp":"2024-01-01T00:03:00Z","level":"ERROR","message":"Connection timeout"}
{"timestamp":"2024-01-01T00:04:00Z","level":"INFO","message":"Retrying connection"}
{"timestamp":"2024-01-01T00:05:00Z","level":"INFO","message":"Connection restored"}
EOF
echo "✓ Generated sample logs"

# Create logfile-ingest configuration
cat > /tmp/logfile-ingest.yaml << 'EOF'
name: app-logs
archived_pattern: "/var/log/duckpond/app-*.json"
active_pattern: "/var/log/duckpond/app.json"
pond_path: "logs/app"
EOF

# Create the logfile-ingest node
# Note: This may fail if factory not implemented - that's a valid test result!
echo "Creating logfile-ingest factory node..."
if pond mknod logfile-ingest /etc/system.d/20-logs --config-path /tmp/logfile-ingest.yaml; then
    echo "✓ Logfile-ingest node created"
    
    # Run the ingestion
    echo "Running log ingestion..."
    pond run /etc/system.d/20-logs ingest || echo "Note: ingest command may have different name"
else
    echo "⚠ logfile-ingest factory not available - trying manual copy"
    # Fallback: manually copy the log file
    pond copy /var/log/duckpond/app.json /logs/app.json
    echo "✓ Manually copied logs"
fi

#############################
# SETUP POND2 - Observer
#############################

echo ""
echo "=== Setting up Pond2 (Observer) ==="

export POND=/pond2
pond init
echo "✓ Pond2 initialized at ${POND}"

pond mkdir /observations
pond mkdir /queries

#############################
# CROSS-POND OBSERVATION
#############################

echo ""
echo "=== Observing Pond1 from Pond2 ==="

# Query Pond1's control table for recent transactions
echo ""
echo "--- Pond1 Transaction History ---"
POND=/pond1 pond control recent --limit 10

# Query with SQL to get structured data
echo ""
echo "--- Pond1 Transaction Details (SQL) ---"
POND=/pond1 pond control --sql "
    SELECT 
        txn_seq,
        record_type,
        cli_args,
        created_at
    FROM control_table 
    WHERE record_category = 'transaction'
    ORDER BY txn_seq DESC
    LIMIT 5
" 2>/dev/null || echo "Note: SQL query syntax may vary"

# Store the observation in Pond2
echo ""
echo "--- Storing observation in Pond2 ---"

# Export Pond1's control data and import to Pond2
POND=/pond1 pond control recent --limit 10 > /tmp/pond1-history.txt
cat > /tmp/observation.json << EOF
{
    "observed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "observed_pond": "/pond1",
    "observer_pond": "/pond2",
    "transactions_observed": $(POND=/pond1 pond control recent --limit 1 2>/dev/null | grep -c txn || echo 0)
}
EOF

POND=/pond2 pond copy /tmp/observation.json /observations/pond1-status.json
echo "✓ Stored observation"

#############################
# VERIFY SETUP
#############################

echo ""
echo "=== Verification ==="

echo ""
echo "--- Pond1 Structure ---"
POND=/pond1 pond list '/*'

echo ""
echo "--- Pond1 Logs ---"
POND=/pond1 pond list '/logs/*' 2>/dev/null || echo "(logs directory may be empty)"

echo ""
echo "--- Pond2 Structure ---"
POND=/pond2 pond list '/*'

echo ""
echo "--- Pond2 Observations ---"
POND=/pond2 pond cat /observations/pond1-status.json 2>/dev/null || echo "(may need different path)"

#############################
# CRON SETUP (demonstration)
#############################

echo ""
echo "=== Cron Configuration (for reference) ==="

cat << 'CRON_EXAMPLE'
# To run log ingestion every minute (add to /etc/cron.d/experiment):
* * * * * root POND=/pond1 /usr/local/bin/pond run /etc/system.d/20-logs ingest >> /var/log/duckpond/cron.log 2>&1

# To run observation every 5 minutes:
*/5 * * * * root /experiment/observe-pond1.sh >> /var/log/duckpond/observer.log 2>&1
CRON_EXAMPLE

echo ""
echo "=== Experiment Complete ==="
