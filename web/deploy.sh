#!/bin/bash

# Battery Monitor Deployment Script
# This script sets up the web application with HTTPS and domain support

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DOMAIN=""
EMAIL=""
WEB_ROOT="/home/trophy/BatteryMonitoring2/web"
NGINX_SITES_AVAILABLE="/etc/nginx/sites-available"
NGINX_SITES_ENABLED="/etc/nginx/sites-enabled"
SERVICE_FILE="/etc/systemd/system/battery-monitor.service"

echo -e "${GREEN}Battery Monitor Deployment Script${NC}"
echo "=================================="

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo -e "${RED}This script should not be run as root. Please run as a regular user with sudo privileges.${NC}"
   exit 1
fi

# Get domain and email from user
if [ -z "$DOMAIN" ]; then
    read -p "Enter your domain name (e.g., battery-monitor.example.com): " DOMAIN
fi

if [ -z "$EMAIL" ]; then
    read -p "Enter your email address for Let's Encrypt: " EMAIL
fi

echo -e "${YELLOW}Setting up Battery Monitor for domain: $DOMAIN${NC}"

# Update system packages
echo -e "${YELLOW}Updating system packages...${NC}"
sudo apt update && sudo apt upgrade -y

# Install required packages
echo -e "${YELLOW}Installing required packages...${NC}"
sudo apt install -y nginx certbot python3-certbot-nginx postgresql-client python3-pip python3-venv

# Create virtual environment and install dependencies
echo -e "${YELLOW}Setting up Python virtual environment...${NC}"
cd "$WEB_ROOT"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create users table in database
echo -e "${YELLOW}Creating users table in database...${NC}"
psql -h 127.0.0.1 -p 5432 -U troy -d batteries -f ../db/create_users_table.sql

# Update nginx configuration with actual domain
echo -e "${YELLOW}Configuring nginx...${NC}"
sudo cp nginx.conf "$NGINX_SITES_AVAILABLE/battery-monitor"
sudo sed -i "s/your-domain.com/$DOMAIN/g" "$NGINX_SITES_AVAILABLE/battery-monitor"

# Remove default nginx site if it exists
if [ -f "$NGINX_SITES_ENABLED/default" ]; then
    sudo rm "$NGINX_SITES_ENABLED/default"
fi

# Enable the site
sudo ln -sf "$NGINX_SITES_AVAILABLE/battery-monitor" "$NGINX_SITES_ENABLED/"

# Test nginx configuration
sudo nginx -t

# Start nginx
sudo systemctl enable nginx
sudo systemctl start nginx

# Get SSL certificate
echo -e "${YELLOW}Obtaining SSL certificate from Let's Encrypt...${NC}"
sudo certbot --nginx -d "$DOMAIN" --email "$EMAIL" --agree-tos --non-interactive

# Update systemd service file
echo -e "${YELLOW}Setting up systemd service...${NC}"
sudo cp battery-monitor.service "$SERVICE_FILE"
sudo systemctl daemon-reload
sudo systemctl enable battery-monitor
sudo systemctl start battery-monitor

# Set proper permissions
echo -e "${YELLOW}Setting file permissions...${NC}"
sudo chown -R www-data:www-data "$WEB_ROOT"
sudo chmod -R 755 "$WEB_ROOT"

# Create log directories
sudo mkdir -p /var/log/nginx
sudo chown www-data:www-data /var/log/nginx

echo -e "${GREEN}Deployment completed successfully!${NC}"
echo ""
echo -e "${GREEN}Your Battery Monitor is now available at: https://$DOMAIN${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Visit https://$DOMAIN to access the login page"
echo "2. Use your battery ID as the username"
echo "3. Set a password on first login"
echo "4. Monitor your battery data!"
echo ""
echo -e "${YELLOW}Useful commands:${NC}"
echo "- Check service status: sudo systemctl status battery-monitor"
echo "- View logs: sudo journalctl -u battery-monitor -f"
echo "- Restart service: sudo systemctl restart battery-monitor"
echo "- Check nginx status: sudo systemctl status nginx"
echo "- Renew SSL certificate: sudo certbot renew"
