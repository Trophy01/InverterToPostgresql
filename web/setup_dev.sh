#!/bin/bash

# Battery Monitor Development Setup Script
# This script sets up the web application for local development

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

WEB_ROOT="/home/trophy/BatteryMonitoring2/web"

echo -e "${GREEN}Battery Monitor Development Setup${NC}"
echo "=================================="

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

echo -e "${GREEN}Development setup completed successfully!${NC}"
echo ""
echo -e "${YELLOW}To start the development server:${NC}"
echo "1. cd $WEB_ROOT"
echo "2. source venv/bin/activate"
echo "3. python app.py"
echo ""
echo -e "${GREEN}The application will be available at: http://localhost:5000${NC}"
echo ""
echo -e "${YELLOW}For production deployment, use: ./deploy.sh${NC}"
