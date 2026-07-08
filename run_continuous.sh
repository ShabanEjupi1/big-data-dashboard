#!/bin/bash

###############################################################################
# RUN CONTINUOUS - Start 8-day continuous data collection
# Runs in screen session for persistence
###############################################################################

echo "================================================================================"
echo "🚀 CONTINUOUS DATA COLLECTION - 8 DAYS"
echo "================================================================================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check if cluster is running
MASTER_VM="10.0.0.8"
echo "Checking if cluster is running on $MASTER_VM..."

curl -s "http://$MASTER_VM:8080/json/" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Cluster is not running!${NC}"
    echo ""
    echo "Start cluster first:"
    echo "  ./run_cluster.sh"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Cluster is running${NC}"
echo ""

# Check if screen is installed
if ! command -v screen &> /dev/null; then
    echo -e "${YELLOW}⚠️  'screen' not installed${NC}"
    echo "Installing screen..."
    sudo apt-get update && sudo apt-get install -y screen
fi

# Create logs directory
mkdir -p logs
mkdir -p data/stocks data/crypto data/forex
mkdir -p checkpoints

echo "================================================================================"
echo "  COLLECTION PARAMETERS"
echo "================================================================================"
echo ""
echo "  Duration:        8 days (192 hours)"
echo "  Interval:        5 seconds"
echo "  Expected rows:   ~700,000,000"
echo "  Expected size:   ~100 GB compressed"
echo ""
echo "  Data sources:"
echo "    - Yahoo Finance (stocks + forex)"
echo "    - CoinGecko (crypto)"
echo "    - Binance (crypto)"
echo ""
echo "  Output:"
echo "    - Parquet: data/{stocks,crypto,forex}/"
echo "    - Logs:    logs/collector.log"
echo ""
echo "================================================================================"
echo ""

# Confirm start
read -p "Start 8-day collection? This will run in background. (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo "Starting collection in screen session 'bigdata'..."
echo ""

# Kill existing sessions if any
screen -S bigdata -X quit 2>/dev/null
screen -S dashboard -X quit 2>/dev/null

sleep 1

# Start data collection in screen session
echo "Starting data collection in screen session 'bigdata'..."
screen -dmS bigdata bash -c '
    cd /home/krenuser/big-data-dashboard
    echo "[$(date)] Starting data collector..." >> logs/screen.log
    python3 data_collector.py 2>&1 | tee -a logs/collector.log
    echo "[$(date)] Collector exited with code $?" >> logs/screen.log
    exec bash
'

sleep 2

# Start dashboard in separate screen session
echo "Starting dashboard in screen session 'dashboard'..."
screen -dmS dashboard bash -c '
    cd /home/krenuser/big-data-dashboard
    echo "[$(date)] Starting dashboard..." >> logs/screen.log
    python3 dashboard.py 2>&1 | tee -a logs/dashboard.log
    echo "[$(date)] Dashboard exited with code $?" >> logs/screen.log
    exec bash
'

sleep 2

# Check if sessions are running
if screen -list | grep -q "bigdata" && screen -list | grep -q "dashboard"; then
    echo -e "${GREEN}✅ Collection and Dashboard started successfully!${NC}"
    echo ""
    echo "================================================================================"
    echo "  🌐 WEB DASHBOARD"
    echo "================================================================================"
    echo ""
    echo "  Dashboard URL:  http://localhost:5000"
    echo "  Dashboard URL:  http://10.0.0.8:5000"
    echo ""
    echo "  Access from your browser to see real-time data!"
    echo ""
    echo "================================================================================"
    echo "  MANAGEMENT COMMANDS"
    echo "================================================================================"
    echo ""
    echo "  View collector logs:   tail -f logs/collector.log"
    echo "  View dashboard logs:   tail -f logs/dashboard.log"
    echo "  View all logs:         tail -f logs/*.log"
    echo ""
    echo "  Attach to collector:   screen -r bigdata"
    echo "  Attach to dashboard:   screen -r dashboard"
    echo "  Detach from screen:    Ctrl+A, then D"
    echo ""
    echo "  Stop collector:        screen -S bigdata -X quit"
    echo "  Stop dashboard:        screen -S dashboard -X quit"
    echo "  Stop all:              screen -S bigdata -X quit && screen -S dashboard -X quit"
    echo ""
    echo "  List screen sessions:  screen -list"
    echo "  Monitor progress:      ./monitor.sh"
    echo ""
    echo "================================================================================"
    echo ""
    echo "Collection will run for 8 days. Dashboard updates automatically!"
    echo ""
else
    echo -e "${RED}✗ Failed to start collection or dashboard${NC}"
    echo ""
    echo "Debug information:"
    echo "  Screen sessions:"
    screen -list
    echo ""
    echo "  Recent logs:"
    tail -20 logs/screen.log 2>/dev/null || echo "  No logs found"
    echo ""
    exit 1
fi

exit 0
