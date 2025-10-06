#!/usr/bin/env bash
set -euo pipefail

# Usage: sudo ./easy_setup.sh batteries.telco.co.zw sa@154.119.80.42 5432
#        DOMAIN                    TUNNEL_HOST        LOCAL_PORT(optional, default 5432)

DOMAIN=${1:?"Domain required, e.g. batteries.telco.co.zw"}
TUNNEL_HOST=${2:?"Tunnel SSH host required, e.g. sa@154.119.80.42"}
LOCAL_PORT=${3:-5432}
EMAIL=${EMAIL:-}
if [[ -z "${EMAIL}" ]]; then
  read -p "Enter email for Let's Encrypt (for expiry notices): " EMAIL
fi

APP_DIR="/home/sa/InverterToPostgresql/web"
APP_SERVICE="/etc/systemd/system/battery-monitor.service"
TUNNEL_SERVICE="/etc/systemd/system/ssh-db-tunnel.service"
SITE_AVAIL="/etc/nginx/sites-available/battery-monitor"
SITE_ENAB="/etc/nginx/sites-enabled/battery-monitor"

# Ensure deps
apt-get update -y
apt-get install -y nginx certbot python3-certbot-nginx netcat-openbsd

# 1) Configure persistent SSH tunnel
cp "$APP_DIR/ssh-db-tunnel.service" /tmp/ssh-db-tunnel.service
sed -i "s|User=www-data|User=sa|; s|Group=www-data|Group=sa|" /tmp/ssh-db-tunnel.service
sed -i "s|sa@154.119.80.42|$TUNNEL_HOST|" /tmp/ssh-db-tunnel.service
sed -i "s|Environment=LOCAL_PORT=5432|Environment=LOCAL_PORT=$LOCAL_PORT|" /tmp/ssh-db-tunnel.service
install -m 0644 /tmp/ssh-db-tunnel.service "$TUNNEL_SERVICE"
systemctl daemon-reload
systemctl enable ssh-db-tunnel
systemctl restart ssh-db-tunnel
systemctl status --no-pager ssh-db-tunnel || true

# Check tunnel
sleep 1
nc -zv 127.0.0.1 "$LOCAL_PORT"

# 2) Configure app service
cp "$APP_DIR/battery-monitor.service" /tmp/battery-monitor.service
sed -i "s|/home/trophy/BatteryMonitoring2/web|$APP_DIR|g" /tmp/battery-monitor.service
sed -i "s|/home/trophy/BatteryMonitoring2/web/venv|$APP_DIR/venv|g" /tmp/battery-monitor.service
sed -i "s|Environment=DB_PORT=5432|Environment=DB_PORT=$LOCAL_PORT|" /tmp/battery-monitor.service
sed -i "s|Environment=SECRET_KEY=.*|Environment=SECRET_KEY=$(openssl rand -hex 32)|" /tmp/battery-monitor.service
install -m 0644 /tmp/battery-monitor.service "$APP_SERVICE"
systemctl daemon-reload
systemctl enable battery-monitor
systemctl restart battery-monitor
systemctl status --no-pager battery-monitor || true

# 3) Temporary HTTP site for ACME
tee "$SITE_AVAIL" >/dev/null <<EOF
server {
    listen 80;
    server_name $DOMAIN;

    location ^~ /.well-known/acme-challenge/ {
        default_type "text/plain";
        root /var/www/html;
    }

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF
mkdir -p /var/www/html
ln -sf "$SITE_AVAIL" "$SITE_ENAB"
nginx -t
systemctl restart nginx

# 4) Issue certificate and let certbot convert to HTTPS
certbot --nginx -d "$DOMAIN" --email "$EMAIL" --agree-tos --non-interactive

# 5) Final checks
nginx -t
systemctl restart nginx

echo "Setup complete. Visit: https://$DOMAIN"
