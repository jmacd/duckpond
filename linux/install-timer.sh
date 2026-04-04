#!/bin/bash
# Install a systemd user timer for periodic journal collection.
#
# Usage:
#   ./linux/install-timer.sh [--interval INTERVAL]
#
# Options:
#   --interval INTERVAL  Timer interval (default: 1min).
#                         Accepts systemd time spans, e.g. 30s, 1min, 10min.
set -e

INTERVAL="1min"
while [ $# -gt 0 ]; do
    case "$1" in
        --interval) INTERVAL="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

cat > "$UNIT_DIR/pond-journal.service" << EOF
[Unit]
Description=DuckPond journal collection and backup

[Service]
Type=oneshot
MemoryAccounting=yes
ExecStart=$SCRIPT_DIR/run.sh
EOF

cat > "$UNIT_DIR/pond-journal.timer" << EOF
[Unit]
Description=DuckPond journal collection every $INTERVAL

[Timer]
OnBootSec=$INTERVAL
OnUnitActiveSec=$INTERVAL

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now pond-journal.timer

echo "Timer installed (interval: $INTERVAL). Status:"
systemctl --user status pond-journal.timer --no-pager
