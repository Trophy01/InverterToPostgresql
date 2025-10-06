#!/usr/bin/env bash
set -euo pipefail

# Usage: sudo ./setup_tunnel.sh sa@154.119.80.42 5432 127.0.0.1 5432
TUNNEL_HOST=${1:-sa@154.119.80.42}
LOCAL_PORT=${2:-5432}
REMOTE_HOST=${3:-127.0.0.1}
REMOTE_PORT=${4:-5432}

SERVICE=/etc/systemd/system/ssh-db-tunnel.service

cp ./ssh-db-tunnel.service /tmp/ssh-db-tunnel.service
sed -i "s|sa@154.119.80.42|$TUNNEL_HOST|g" /tmp/ssh-db-tunnel.service
sed -i "s|Environment=LOCAL_PORT=5432|Environment=LOCAL_PORT=$LOCAL_PORT|g" /tmp/ssh-db-tunnel.service
sed -i "s|Environment=REMOTE_HOST=127.0.0.1|Environment=REMOTE_HOST=$REMOTE_HOST|g" /tmp/ssh-db-tunnel.service
sed -i "s|Environment=REMOTE_PORT=5432|Environment=REMOTE_PORT=$REMOTE_PORT|g" /tmp/ssh-db-tunnel.service

install -m 0644 /tmp/ssh-db-tunnel.service "$SERVICE"

systemctl daemon-reload
systemctl enable ssh-db-tunnel
systemctl restart ssh-db-tunnel

systemctl status --no-pager ssh-db-tunnel || true

echo "Tunnel active on 127.0.0.1:$LOCAL_PORT → $REMOTE_HOST:$REMOTE_PORT via $TUNNEL_HOST"
