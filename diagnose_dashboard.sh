#!/bin/bash

# ============================================================================
# DASHBOARD DIAGNOSTICS & TROUBLESHOOTING
# ============================================================================

echo "============================================================================="
echo " 🔍 BIG DATA DASHBOARD - SYSTEM DIAGNOSTICS"
echo "============================================================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check command
check_command() {
    if command -v "$1" &> /dev/null; then
        echo -e "${GREEN}✓${NC} $1 is installed: $(command -v $1)"
        if [ "$1" == "python3" ]; then
            python3 --version
        elif [ "$1" == "redis-server" ]; then
            redis-server --version 2>/dev/null | head -n1
        fi
        return 0
    else
        echo -e "${RED}✗${NC} $1 is NOT installed"
        return 1
    fi
}

# Function to check service
check_service() {
    if pgrep -x "$1" > /dev/null; then
        echo -e "${GREEN}✓${NC} $1 is running"
        pgrep -x "$1" | head -n1 | xargs ps -p | tail -n1
        return 0
    else
        echo -e "${RED}✗${NC} $1 is NOT running"
        return 1
    fi
}

# Function to check port
check_port() {
    if lsof -Pi :$1 -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠${NC}  Port $1 is in use:"
        lsof -Pi :$1 -sTCP:LISTEN 2>/dev/null | head -n2
        return 1
    else
        echo -e "${GREEN}✓${NC} Port $1 is available"
        return 0
    fi
}

echo "1️⃣  CHECKING REQUIRED COMMANDS..."
echo "-------------------------------------------"
check_command python3
check_command pip3
check_command redis-server
check_command redis-cli
check_command uvicorn
echo ""

echo "2️⃣  CHECKING PYTHON PACKAGES..."
echo "-------------------------------------------"
python3 << 'EOF'
import sys
packages = [
    'flask', 'pandas', 'pyarrow', 'pyspark', 
    'fastapi', 'uvicorn', 'redis', 'polars'
]

for pkg in packages:
    try:
        mod = __import__(pkg)
        version = getattr(mod, '__version__', 'unknown')
        print(f"\033[0;32m✓\033[0m {pkg:15} {version}")
    except ImportError:
        print(f"\033[0;31m✗\033[0m {pkg:15} NOT INSTALLED")
EOF
echo ""

echo "3️⃣  CHECKING SERVICES..."
echo "-------------------------------------------"
check_service redis-server
echo ""

echo "4️⃣  CHECKING PORTS..."
echo "-------------------------------------------"
check_port 5003
check_port 8000
check_port 6379
echo ""

echo "5️⃣  CHECKING DATA DIRECTORIES..."
echo "-------------------------------------------"
DATA_DIRS=(
    "/home/krenuser/big-data-dashboard/databaza.csv"
    "/home/krenuser/big-data-dashboard/databaza-stocks.csv"
    "/home/krenuser/big-data-dashboard/data"
    "/home/krenuser/big-data-dashboard/logs"
)

for dir in "${DATA_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        file_count=$(find "$dir" -name "*.parquet" 2>/dev/null | wc -l)
        echo -e "${GREEN}✓${NC} $dir exists (${file_count} parquet files)"
    else
        echo -e "${RED}✗${NC} $dir NOT FOUND"
    fi
done
echo ""

echo "6️⃣  CHECKING RUNNING DASHBOARDS..."
echo "-------------------------------------------"
if pgrep -f "dashboard.py" > /dev/null; then
    echo -e "${GREEN}✓${NC} V1 Dashboard is running:"
    pgrep -f "dashboard.py" | xargs ps -p | tail -n+2
else
    echo -e "${YELLOW}⚠${NC}  V1 Dashboard is not running"
fi

if pgrep -f "main.py.*fastapi\|uvicorn.*main:app" > /dev/null; then
    echo -e "${GREEN}✓${NC} V2 Dashboard is running:"
    pgrep -f "main.py.*fastapi\|uvicorn.*main:app" | xargs ps -p 2>/dev/null | tail -n+2
else
    echo -e "${YELLOW}⚠${NC}  V2 Dashboard is not running"
fi
echo ""

echo "7️⃣  CHECKING RECENT LOGS..."
echo "-------------------------------------------"
if [ -f "logs/v1.log" ]; then
    echo "V1 Last 5 lines:"
    tail -n5 logs/v1.log 2>/dev/null | sed 's/^/  /'
else
    echo "No V1 logs found"
fi
echo ""

if [ -f "logs/v2.log" ]; then
    echo "V2 Last 5 lines:"
    tail -n5 logs/v2.log 2>/dev/null | sed 's/^/  /'
else
    echo "No V2 logs found"
fi
echo ""

echo "8️⃣  TESTING REDIS CONNECTION..."
echo "-------------------------------------------"
if command -v redis-cli &> /dev/null; then
    if redis-cli ping 2>/dev/null | grep -q PONG; then
        echo -e "${GREEN}✓${NC} Redis is responsive"
        echo "  Server info:"
        redis-cli INFO server 2>/dev/null | grep -E "redis_version|os|uptime_in_seconds" | sed 's/^/  /'
    else
        echo -e "${RED}✗${NC} Redis is not responding"
    fi
else
    echo -e "${RED}✗${NC} redis-cli not available"
fi
echo ""

echo "9️⃣  MEMORY & CPU USAGE..."
echo "-------------------------------------------"
echo "Memory usage:"
free -h | sed 's/^/  /'
echo ""
echo "CPU Load:"
uptime | sed 's/^/  /'
echo ""

echo "🔟 JAVA VERSION (for Spark)..."
echo "-------------------------------------------"
if command -v java &> /dev/null; then
    java -version 2>&1 | head -n1 | sed 's/^/  /'
else
    echo -e "${RED}✗${NC} Java not installed (required for Spark)"
fi
echo ""

echo "============================================================================="
echo " 📋 DIAGNOSTIC SUMMARY"
echo "============================================================================="
echo ""
echo "Common Issues & Solutions:"
echo ""
echo "1. Port already in use:"
echo "   → Kill existing process: sudo lsof -ti:5003 | xargs kill -9"
echo "   → Or use different port in dashboard.py"
echo ""
echo "2. Redis not running:"
echo "   → Start Redis: sudo systemctl start redis-server"
echo "   → Or: redis-server --daemonize yes"
echo ""
echo "3. V2 shuts down immediately:"
echo "   → Use: ./start_dashboard_stable.sh v2"
echo "   → Check logs: tail -f logs/v2_stable.log"
echo ""
echo "4. V1 Spark ThreadPool errors:"
echo "   → Increase Java heap: export _JAVA_OPTIONS='-Xmx4g'"
echo "   → Use stable script: ./start_dashboard_stable.sh v1"
echo ""
echo "5. Missing Python packages:"
echo "   → Install: pip3 install -r requirements.txt"
echo "   → Or V2: pip3 install -r v2_advanced/requirements_v2_minimal.txt"
echo ""
echo "============================================================================="
echo ""
