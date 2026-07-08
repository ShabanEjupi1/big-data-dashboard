#!/bin/bash
# Start the enhanced crypto-focused data collector

echo "=================================================="
echo " STARTING ENHANCED DATA COLLECTOR"
echo " Focus: 350+ Cryptocurrencies from CoinGecko + Binance"
echo " Secondary: Stock data when available (rate limit aware)"
echo "=================================================="
echo ""

cd /home/krenuser/big-data-dashboard

# Stop any existing collectors
echo "Stopping any existing collectors..."
pkill -f "data_collector" 2>/dev/null
sleep 2

# Clean old screen sessions
screen -S bigdata -X quit 2>/dev/null
screen -S collector -X quit 2>/dev/null

# Start the enhanced collector in a screen session
echo "Starting enhanced collector in background..."
screen -dmS collector bash -c "
    cd /home/krenuser/big-data-dashboard
    echo \"[$(date)] Starting enhanced collector...\" >> logs/collector.log
    python3 data_collector_v2.py 2>&1 | tee -a logs/collector_v2.log
    echo \"[$(date)] Collector exited with code \$?\" >> logs/collector.log
"

sleep 3

# Check if it's running
if ps aux | grep -q "[p]ython3 data_collector_v2.py"; then
    echo "✓ Collector started successfully!"
    echo ""
    echo "Monitor with: screen -r collector"
    echo "View logs: tail -f logs/collector_v2.log"
    echo "Stop with: screen -S collector -X quit"
else
    echo "✗ Failed to start collector"
    echo "Check logs: tail -30 logs/collector_v2.log"
    exit 1
fi

echo ""
echo "The collector will:"
echo "  - Collect 250 coins from CoinGecko every 5 seconds"
echo "  - Collect 100 pairs from Binance every 5 seconds"  
echo "  - Try 3 stocks from Yahoo Finance every 100 seconds (if not rate limited)"
echo ""
echo "Dashboard will be generated automatically."
echo "Run: python3 generate_dashboard.py (to create dashboard JSON)"
echo "Run: python3 dashboard.py (to start web dashboard)"
