#!/bin/bash

# Enhanced Dashboard - Quick Start Script
# This script helps you start the dashboard with optional API configuration

echo "════════════════════════════════════════════════════════════════"
echo " 🚀 BIG DATA DASHBOARD - Enhanced Version 2.0"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if API keys are set
echo "🔍 Checking API Configuration..."
echo ""

API_COUNT=0

if [ ! -z "$ALPHA_VANTAGE_KEY" ] && [ "$ALPHA_VANTAGE_KEY" != "demo" ]; then
    echo -e "${GREEN}✓${NC} Alpha Vantage API: Configured"
    API_COUNT=$((API_COUNT+1))
else
    echo -e "${YELLOW}○${NC} Alpha Vantage API: Using demo (limited access)"
fi

if [ ! -z "$FINNHUB_KEY" ] && [ "$FINNHUB_KEY" != "demo" ]; then
    echo -e "${GREEN}✓${NC} Finnhub API: Configured"
    API_COUNT=$((API_COUNT+1))
else
    echo -e "${YELLOW}○${NC} Finnhub API: Using demo (limited access)"
fi

if [ ! -z "$TWELVE_DATA_KEY" ] && [ "$TWELVE_DATA_KEY" != "demo" ]; then
    echo -e "${GREEN}✓${NC} Twelve Data API: Configured"
    API_COUNT=$((API_COUNT+1))
else
    echo -e "${YELLOW}○${NC} Twelve Data API: Using demo (limited access)"
fi

if [ ! -z "$IEX_CLOUD_KEY" ] && [ "$IEX_CLOUD_KEY" != "demo" ]; then
    echo -e "${GREEN}✓${NC} IEX Cloud API: Configured"
    API_COUNT=$((API_COUNT+1))
else
    echo -e "${YELLOW}○${NC} IEX Cloud API: Using demo (limited access)"
fi

echo ""
echo "📊 Stock APIs configured: $API_COUNT/4"
echo -e "${GREEN}✓${NC} Crypto APIs: Always available (no keys needed)"
echo ""

if [ $API_COUNT -eq 0 ]; then
    echo -e "${YELLOW}⚠️  No stock API keys configured!${NC}"
    echo "   Stock data will be limited with demo keys."
    echo "   See API_KEYS_SETUP.md for configuration instructions."
    echo ""
fi

# Ask user what they want to do
echo "════════════════════════════════════════════════════════════════"
echo " What would you like to do?"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo " 1) Start Dashboard Only (view existing data)"
echo " 2) Start Data Collector (collect new data)"
echo " 3) Start Both (collector in background + dashboard)"
echo " 4) Configure API Keys"
echo " 5) View System Status"
echo " 6) Exit"
echo ""
read -p "Enter your choice (1-6): " choice

case $choice in
    1)
        echo ""
        echo "🚀 Starting Dashboard Server..."
        echo ""
        python3 dashboard.py
        ;;
    2)
        echo ""
        echo "📊 Starting Data Collector..."
        echo "   (This will run for 8 days collecting crypto and stock data)"
        echo ""
        python3 data_collector_v2.py
        ;;
    3)
        echo ""
        echo "🚀 Starting Both Services..."
        echo ""
        
        # Start collector in background
        nohup python3 data_collector_v2.py > logs/collector_output.log 2>&1 &
        COLLECTOR_PID=$!
        echo "✓ Data Collector started (PID: $COLLECTOR_PID)"
        echo "  Logs: logs/collector_output.log"
        
        # Wait a moment
        sleep 2
        
        # Start dashboard
        echo "✓ Starting Dashboard Server..."
        python3 dashboard.py
        ;;
    4)
        echo ""
        echo "════════════════════════════════════════════════════════════════"
        echo " 🔑 API Keys Configuration"
        echo "════════════════════════════════════════════════════════════════"
        echo ""
        echo "To configure API keys, add these lines to your ~/.bashrc:"
        echo ""
        echo "  export ALPHA_VANTAGE_KEY=\"your_key_here\""
        echo "  export FINNHUB_KEY=\"your_key_here\""
        echo "  export TWELVE_DATA_KEY=\"your_key_here\""
        echo "  export IEX_CLOUD_KEY=\"your_key_here\""
        echo ""
        echo "Get free API keys from:"
        echo "  • Alpha Vantage: https://www.alphavantage.co/support/#api-key"
        echo "  • Finnhub: https://finnhub.io/register"
        echo "  • Twelve Data: https://twelvedata.com/pricing"
        echo "  • IEX Cloud: https://iexcloud.io/pricing"
        echo ""
        echo "See API_KEYS_SETUP.md for detailed instructions."
        echo ""
        read -p "Press Enter to continue..."
        ;;
    5)
        echo ""
        echo "════════════════════════════════════════════════════════════════"
        echo " 📊 System Status"
        echo "════════════════════════════════════════════════════════════════"
        echo ""
        
        # Check if collector is running
        if pgrep -f "data_collector_v2.py" > /dev/null; then
            echo -e "${GREEN}✓${NC} Data Collector: Running"
            echo "  PID: $(pgrep -f 'data_collector_v2.py')"
        else
            echo -e "${RED}✗${NC} Data Collector: Not running"
        fi
        
        # Check if dashboard is running
        if pgrep -f "dashboard.py" > /dev/null; then
            echo -e "${GREEN}✓${NC} Dashboard Server: Running"
            echo "  PID: $(pgrep -f 'dashboard.py')"
            echo "  URL: http://localhost:5000"
        else
            echo -e "${RED}✗${NC} Dashboard Server: Not running"
        fi
        
        echo ""
        
        # Check data files
        DASH_FILES=$(ls dashboard_*.json 2>/dev/null | wc -l)
        if [ $DASH_FILES -gt 0 ]; then
            LATEST=$(ls -t dashboard_*.json 2>/dev/null | head -1)
            echo -e "${GREEN}✓${NC} Dashboard Data: $DASH_FILES files found"
            echo "  Latest: $LATEST"
        else
            echo -e "${RED}✗${NC} Dashboard Data: No files found"
            echo "  Run data collector first!"
        fi
        
        echo ""
        
        # Check data directories
        CRYPTO_DATA=$(find data/crypto -name "*.parquet" 2>/dev/null | wc -l)
        STOCK_DATA=$(find data/stocks -name "*.parquet" 2>/dev/null | wc -l)
        
        echo "📁 Data Collection:"
        echo "  • Crypto parquet files: $CRYPTO_DATA"
        echo "  • Stock parquet files: $STOCK_DATA"
        
        echo ""
        
        # Recent logs
        if [ -f "logs/collector.log" ]; then
            echo "📝 Recent Collector Logs (last 5 lines):"
            tail -5 logs/collector.log | sed 's/^/  /'
        fi
        
        echo ""
        read -p "Press Enter to continue..."
        ;;
    6)
        echo ""
        echo "👋 Goodbye!"
        echo ""
        exit 0
        ;;
    *)
        echo ""
        echo -e "${RED}✗${NC} Invalid choice. Please run the script again."
        echo ""
        exit 1
        ;;
esac

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " 🎉 Thank you for using Big Data Dashboard!"
echo "════════════════════════════════════════════════════════════════"
echo ""
