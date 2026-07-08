#!/bin/bash

# ============================================================================
# DIAGNOSTIC AND FIX SCRIPT
# ============================================================================
# This script diagnoses and fixes common issues with the dashboard

echo "==========================================================================="
echo " 🔧 BIG DATA DASHBOARD - DIAGNOSTIC & FIX TOOL"
echo "==========================================================================="
echo ""

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if a Python module is installed
python_module_exists() {
    python3 -c "import $1" 2>/dev/null
}

echo "📊 SYSTEM DIAGNOSTICS"
echo "==========================================================================="

# Check Python
echo -n "1. Python 3: "
if command_exists python3; then
    PYTHON_VERSION=$(python3 --version)
    echo "✅ $PYTHON_VERSION"
else
    echo "❌ Not found"
    exit 1
fi

# Check Redis
echo -n "2. Redis Server: "
if command_exists redis-server; then
    REDIS_VERSION=$(redis-server --version | head -n1)
    echo "✅ $REDIS_VERSION"
    
    # Check if Redis is running
    echo -n "   Redis Status: "
    if pgrep -x "redis-server" > /dev/null; then
        echo "✅ Running"
    else
        echo "⚠️  Not running - Starting..."
        sudo systemctl start redis-server
        sleep 2
        if pgrep -x "redis-server" > /dev/null; then
            echo "   ✅ Started successfully"
        else
            echo "   ❌ Failed to start"
        fi
    fi
else
    echo "❌ Not installed"
    echo "   Installing Redis..."
    sudo apt-get update && sudo apt-get install -y redis-server
fi

# Check Spark
echo -n "3. Apache Spark: "
if python_module_exists pyspark; then
    echo "✅ Installed"
else
    echo "⚠️  PySpark not found (required for V1)"
fi

# Check Flask
echo -n "4. Flask: "
if python_module_exists flask; then
    echo "✅ Installed"
else
    echo "⚠️  Not installed"
    pip3 install flask
fi

# Check FastAPI
echo -n "5. FastAPI: "
if python_module_exists fastapi; then
    echo "✅ Installed"
else
    echo "⚠️  Not installed (required for V2)"
fi

echo ""
echo "📦 CHECKING V1 DEPENDENCIES"
echo "==========================================================================="

V1_DEPS=("flask" "pandas" "pyarrow" "numpy")
V1_MISSING=()

for dep in "${V1_DEPS[@]}"; do
    echo -n "   $dep: "
    if python_module_exists "$dep"; then
        echo "✅"
    else
        echo "❌"
        V1_MISSING+=("$dep")
    fi
done

if [ ${#V1_MISSING[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  Missing V1 dependencies: ${V1_MISSING[*]}"
    echo "   Installing..."
    pip3 install "${V1_MISSING[@]}"
fi

echo ""
echo "📦 CHECKING V2 DEPENDENCIES"
echo "==========================================================================="

V2_DEPS=("fastapi" "uvicorn" "polars" "redis")
V2_MISSING=()

for dep in "${V2_DEPS[@]}"; do
    echo -n "   $dep: "
    if python_module_exists "$dep"; then
        echo "✅"
    else
        echo "❌"
        V2_MISSING+=("$dep")
    fi
done

if [ ${#V2_MISSING[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  Missing V2 dependencies"
    read -p "Install V2 dependencies? (y/n): " install_v2
    if [ "$install_v2" = "y" ]; then
        echo "Installing V2 requirements (this may take several minutes)..."
        pip3 install -r v2_advanced/requirements_v2.txt
    fi
fi

echo ""
echo "🔍 CHECKING DATA FILES"
echo "==========================================================================="

# Check for parquet data
CRYPTO_DATA=$(find data/crypto -name "*.parquet" 2>/dev/null | wc -l)
STOCK_DATA=$(find data/stocks -name "*.parquet" 2>/dev/null | wc -l)

echo "   Crypto parquet files: $CRYPTO_DATA"
echo "   Stock parquet files: $STOCK_DATA"

if [ "$CRYPTO_DATA" -eq 0 ] && [ "$STOCK_DATA" -eq 0 ]; then
    echo "   ⚠️  No data files found!"
    echo "   Run data collector first: python3 data_collector_v2.py"
fi

echo ""
echo "🚀 PROCESS STATUS"
echo "==========================================================================="

# Check for running processes
V1_RUNNING=$(ps aux | grep "python3 dashboard.py" | grep -v grep | wc -l)
V2_RUNNING=$(ps aux | grep "python3 main.py" | grep -v grep | wc -l)

echo -n "   V1 (Flask): "
if [ "$V1_RUNNING" -gt 0 ]; then
    echo "✅ Running"
    ps aux | grep "python3 dashboard.py" | grep -v grep | awk '{print "      PID: " $2}'
else
    echo "⚠️  Not running"
fi

echo -n "   V2 (FastAPI): "
if [ "$V2_RUNNING" -gt 0 ]; then
    echo "✅ Running"
    ps aux | grep "python3 main.py" | grep -v grep | awk '{print "      PID: " $2}'
else
    echo "⚠️  Not running"
fi

echo ""
echo "📝 LOG FILES"
echo "==========================================================================="

if [ -f logs/v1.log ]; then
    echo "   V1 Log (last 5 lines):"
    tail -5 logs/v1.log | sed 's/^/      /'
else
    echo "   V1 Log: Not found"
fi

echo ""

if [ -f logs/v2.log ]; then
    echo "   V2 Log (last 5 lines):"
    tail -5 logs/v2.log | sed 's/^/      /'
else
    echo "   V2 Log: Not found"
fi

echo ""
echo "🎯 RECOMMENDATIONS"
echo "==========================================================================="

if [ "$CRYPTO_DATA" -eq 0 ] && [ "$STOCK_DATA" -eq 0 ]; then
    echo "   ⚠️  1. Collect data first: ./start_collector.sh"
fi

if ! pgrep -x "redis-server" > /dev/null; then
    echo "   ⚠️  2. Start Redis: sudo systemctl start redis-server"
fi

if [ ${#V1_MISSING[@]} -gt 0 ]; then
    echo "   ⚠️  3. Install V1 dependencies: pip3 install ${V1_MISSING[*]}"
fi

if [ ${#V2_MISSING[@]} -gt 0 ]; then
    echo "   ⚠️  4. Install V2 dependencies: pip3 install -r v2_advanced/requirements_v2.txt"
fi

if [ "$V1_RUNNING" -eq 0 ] && [ "$V2_RUNNING" -eq 0 ]; then
    echo "   ℹ️  5. Start dashboard: ./run_dashboard.sh"
fi

echo ""
echo "==========================================================================="
echo " ✅ DIAGNOSTIC COMPLETE"
echo "==========================================================================="
