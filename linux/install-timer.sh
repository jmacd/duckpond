#!/bin/bash
# Install a systemd user timer for 10-minute journal collection.
#
# Usage:
#   ./linux/install-timer.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

cat > "$UNIT_DIR/pond-journal.service" << EOF
[Unit]
Description=DuckPond journal collection and backup

[Service]
Type=oneshot
ExecStart=$SCRIPT_DIR/run.sh
EOF

cat > "$UNIT_DIR/pond-journal.timer" << EOF
[Unit]
Description=DuckPond journal collection every 10 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=10min

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now pond-journal.timer

echo "Timer installed. Status:"
systemctl --user status pond-journal.timer --no-pager
