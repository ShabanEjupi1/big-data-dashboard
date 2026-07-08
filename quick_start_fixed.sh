#!/bin/bash

echo "=============================================================================="
echo " 🚀 BIG DATA DASHBOARD - QUICK START GUIDE (FIXED VERSION)"
echo "=============================================================================="
echo ""
echo "Authors: Shaban Ejupi & Majlinda Bajraktari"
echo "University of Prishtina - FSHMN"
echo "Date: January 8, 2026"
echo ""
echo "=============================================================================="
echo ""

# Check if running on master VM
if [[ $(hostname) == *"VM5"* ]] || [[ $(hostname -I | grep -q "10.0.0.8") ]]; then
    echo "✅ Running on Master VM (VM5)"
else
    echo "⚠️  Warning: Not running on VM5. Some features may not work."
fi

echo ""
echo "AVAILABLE DASHBOARDS:"
echo "====================="
echo ""
echo "1️⃣  V1 Dashboard (Flask) - Production Stable"
echo "   Port: 5003"
echo "   URL: http://10.0.0.8:5003 or http://localhost:5003"
echo "   Features:"
echo "   - Real-time crypto & stock data from 10 VM cluster"
echo "   - ML predictions with Linear Regression"
echo "   - Investment recommendations"
echo "   - Portfolio optimization"
echo "   - Data export (CSV, JSON)"
echo "   ✅ FIXED: Broken pipe errors"
echo "   ✅ FIXED: Better error handling for ML features"
echo ""
echo "2️⃣  V2 Dashboard (FastAPI) - Next Generation (2030)"
echo "   Port: 8000"
echo "   URL: http://10.0.0.8:8000 or http://localhost:8000"
echo "   Features:"
echo "   - Modern React-based UI"
echo "   - WebSocket real-time updates"
echo "   - Advanced ML models"
echo "   - Interactive API docs at /api/docs"
echo "   ✅ FIXED: Now shows HTML dashboard (not just JSON)"
echo "   ✅ FIXED: WebSocket connection works on any host"
echo ""
echo "=============================================================================="
echo ""

# Function to check if port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

echo "CURRENT STATUS:"
echo "==============="
echo ""

# Check V1
if check_port 5003; then
    echo "✅ V1 Dashboard (Flask) is RUNNING on port 5003"
    echo "   Access: http://$(hostname -I | awk '{print $1}'):5003"
else
    echo "❌ V1 Dashboard is NOT running"
    echo "   Start with: ./start_dashboard.sh"
fi

echo ""

# Check V2
if check_port 8000; then
    echo "✅ V2 Dashboard (FastAPI) is RUNNING on port 8000"
    echo "   Access: http://$(hostname -I | awk '{print $1}'):8000"
    echo "   API Docs: http://$(hostname -I | awk '{print $1}'):8000/api/docs"
else
    echo "❌ V2 Dashboard is NOT running"
    echo "   Start with: ./start_dashboard_v2.sh"
fi

echo ""
echo "=============================================================================="
echo ""
echo "QUICK ACTIONS:"
echo "=============="
echo ""
echo "Start V1 Dashboard:"
echo "  ./start_dashboard.sh"
echo ""
echo "Start V2 Dashboard:"
echo "  ./start_dashboard_v2.sh"
echo ""
echo "Test Both Dashboards:"
echo "  ./test_dashboards.sh"
echo ""
echo "Start Data Collector (required for fresh data):"
echo "  ./start_collector.sh"
echo ""
echo "Check Spark Cluster:"
echo "  Open browser: http://10.0.0.8:8080"
echo ""
echo "View Logs:"
echo "  tail -f logs/*.log"
echo ""
echo "Stop All Services:"
echo "  pkill -f dashboard"
echo "  pkill -f uvicorn"
echo ""
echo "=============================================================================="
echo ""
echo "TROUBLESHOOTING:"
echo "================"
echo ""
echo "If ML features show 'insufficient data':"
echo "  1. Start data collector: ./start_collector.sh"
echo "  2. Wait at least 2-4 hours for data collection"
echo "  3. Verify data: ls -lh data/crypto/date=*/hour=*/*.parquet"
echo ""
echo "If V2 shows only JSON:"
echo "  ✅ FIXED! Just restart: ./start_dashboard_v2.sh"
echo ""
echo "If V1 stocks page shows broken pipe:"
echo "  ✅ FIXED! Just restart: ./start_dashboard.sh"
echo ""
echo "If ports are in use:"
echo "  pkill -f 'dashboard|uvicorn'"
echo "  Wait 5 seconds, then restart"
echo ""
echo "=============================================================================="
echo ""
echo "DATA REQUIREMENTS FOR ML FEATURES:"
echo "==================================="
echo ""
echo "Feature                    | Min Data Needed | Recommended"
echo "---------------------------|-----------------|------------------"
echo "ML Predictions             | 10 data points  | 24 hours"
echo "Investment Recommendations | 24 hours        | 48 hours"
echo "Portfolio Optimization     | 24 hours        | 48-72 hours"
echo ""
echo "Current data age:"
LATEST_CRYPTO=$(find data/crypto -name "*.parquet" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | awk '{print $2}')
if [ -n "$LATEST_CRYPTO" ]; then
    LATEST_TIME=$(stat -c %y "$LATEST_CRYPTO" 2>/dev/null | cut -d' ' -f1-2)
    echo "  Latest crypto data: $LATEST_TIME"
else
    echo "  ⚠️  No crypto data found - start data collector!"
fi

LATEST_STOCK=$(find data/stocks -name "*.parquet" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | awk '{print $2}')
if [ -n "$LATEST_STOCK" ]; then
    LATEST_TIME=$(stat -c %y "$LATEST_STOCK" 2>/dev/null | cut -d' ' -f1-2)
    echo "  Latest stock data:  $LATEST_TIME"
else
    echo "  ⚠️  No stock data found - start data collector!"
fi

echo ""
echo "=============================================================================="
echo ""
echo "DOCUMENTATION:"
echo "=============="
echo ""
echo "Full Fix Summary: cat DASHBOARD_FIXES_SUMMARY.md"
echo "Project Docs:     cat project_documentation.json"
echo "Access Guide:     cat DASHBOARD_ACCESS_GUIDE.md"
echo "README:           cat README.md"
echo ""
echo "=============================================================================="
echo ""
echo "Ready to start? Choose your dashboard:"
echo ""
echo "  For stable production → ./start_dashboard.sh      (V1 - Flask)"
echo "  For modern features  → ./start_dashboard_v2.sh    (V2 - FastAPI)"
echo "  To test both         → ./test_dashboards.sh"
echo ""
echo "=============================================================================="
