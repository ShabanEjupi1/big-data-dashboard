#!/bin/bash
# Dashboard Diagnostic and Launch Script

echo "================================================================================"
echo " 🔍 BIG DATA DASHBOARD - DIAGNOSTIC & LAUNCH TOOL"
echo "================================================================================"
echo ""

# Function to check if port is in use
check_port() {
    local port=$1
    if netstat -tuln 2>/dev/null | grep -q ":$port " || ss -tuln 2>/dev/null | grep -q ":$port "; then
        echo "✓ Port $port is IN USE"
        return 0
    else
        echo "✗ Port $port is FREE"
        return 1
    fi
}

# Function to test HTTP endpoint
test_endpoint() {
    local url=$1
    if curl -s --connect-timeout 3 "$url" >/dev/null 2>&1; then
        echo "✓ $url is ACCESSIBLE"
        return 0
    else
        echo "✗ $url is NOT ACCESSIBLE"
        return 1
    fi
}

echo "1️⃣  Checking Python and dependencies..."
echo "----------------------------------------"
python3 --version || echo "❌ Python3 not found!"
echo ""

echo "Required packages for V1:"
python3 -c "import flask; print('  ✓ Flask:', flask.__version__)" 2>/dev/null || echo "  ✗ Flask not installed"
python3 -c "import pandas; print('  ✓ Pandas:', pandas.__version__)" 2>/dev/null || echo "  ✗ Pandas not installed"
python3 -c "import pyarrow; print('  ✓ PyArrow:', pyarrow.__version__)" 2>/dev/null || echo "  ✗ PyArrow not installed"
echo ""

echo "Required packages for V2:"
python3 -c "import fastapi; print('  ✓ FastAPI:', fastapi.__version__)" 2>/dev/null || echo "  ✗ FastAPI not installed (optional for V2)"
python3 -c "import polars; print('  ✓ Polars:', polars.__version__)" 2>/dev/null || echo "  ✗ Polars not installed (optional for V2)"
python3 -c "import redis; print('  ✓ Redis:', redis.__version__)" 2>/dev/null || echo "  ✗ Redis not installed (optional for V2)"
echo ""

echo "2️⃣  Checking data directories..."
echo "----------------------------------------"
if [ -d "/home/krenuser/big-data-dashboard/data/crypto" ]; then
    crypto_files=$(find /home/krenuser/big-data-dashboard/data/crypto -name "*.parquet" 2>/dev/null | wc -l)
    echo "  ✓ Crypto data directory exists ($crypto_files parquet files)"
else
    echo "  ✗ Crypto data directory missing"
fi

if [ -d "/home/krenuser/big-data-dashboard/data/stocks" ]; then
    stock_files=$(find /home/krenuser/big-data-dashboard/data/stocks -name "*.parquet" 2>/dev/null | wc -l)
    echo "  ✓ Stock data directory exists ($stock_files parquet files)"
else
    echo "  ✗ Stock data directory missing"
fi
echo ""

echo "3️⃣  Checking network and ports..."
echo "----------------------------------------"
check_port 5003
check_port 8000
echo ""

echo "4️⃣  Checking for running dashboards..."
echo "----------------------------------------"
if ps aux | grep -E "python.*dashboard\.py" | grep -v grep >/dev/null; then
    echo "  ⚠️  Dashboard V1 process is running:"
    ps aux | grep -E "python.*dashboard\.py" | grep -v grep | awk '{print "     PID: "$2" - "$11" "$12" "$13}'
else
    echo "  ✗ No Dashboard V1 process running"
fi

if ps aux | grep -E "python.*main\.py|uvicorn" | grep -v grep >/dev/null; then
    echo "  ⚠️  Dashboard V2 process is running:"
    ps aux | grep -E "python.*main\.py|uvicorn" | grep -v grep | awk '{print "     PID: "$2" - "$11" "$12" "$13}'
else
    echo "  ✗ No Dashboard V2 process running"
fi
echo ""

echo "5️⃣  Testing dashboard endpoints..."
echo "----------------------------------------"
test_endpoint "http://localhost:5003"
test_endpoint "http://10.0.0.8:5003"
test_endpoint "http://localhost:8000"
test_endpoint "http://10.0.0.8:8000"
echo ""

echo "================================================================================"
echo " 🚀 LAUNCH OPTIONS"
echo "================================================================================"
echo ""
echo "Choose an option:"
echo "  1) Start Dashboard V1 (Flask - Original)"
echo "  2) Start Dashboard V2 (FastAPI - Advanced)"
echo "  3) Stop all dashboards"
echo "  4) Install missing dependencies"
echo "  5) Exit"
echo ""
read -p "Enter choice [1-5]: " choice

case $choice in
    1)
        echo ""
        echo "Starting Dashboard V1..."
        ./start_dashboard_v1.sh
        ;;
    2)
        echo ""
        echo "Starting Dashboard V2..."
        ./start_dashboard_v2.sh
        ;;
    3)
        echo ""
        echo "Stopping all dashboards..."
        pkill -f "python.*dashboard\.py"
        pkill -f "python.*main\.py"
        pkill -f "uvicorn"
        echo "✓ All dashboard processes stopped"
        ;;
    4)
        echo ""
        echo "Installing dependencies..."
        echo "For V1 (required):"
        pip install flask pandas pyarrow pyspark
        echo ""
        echo "For V2 (optional):"
        read -p "Install V2 dependencies? (y/n): " install_v2
        if [ "$install_v2" = "y" ]; then
            pip install -r v2_advanced/requirements_v2.txt
        fi
        ;;
    5)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac
