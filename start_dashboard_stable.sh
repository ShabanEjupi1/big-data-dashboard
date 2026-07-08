#!/bin/bash

# ============================================================================
# STABLE START SCRIPT - Using uvicorn CLI for better stability
# ============================================================================
# This script uses uvicorn CLI directly which is more stable than running
# Python scripts with workers. Better for production use.

set -e  # Exit on error

echo "==========================================================================="
echo " 🚀 STARTING BIG DATA DASHBOARD - STABLE MODE"
echo "==========================================================================="
echo ""

# Get the version choice
if [ "$1" == "v1" ] || [ "$1" == "1" ]; then
    VERSION="v1"
elif [ "$1" == "v2" ] || [ "$1" == "2" ]; then
    VERSION="v2"
else
    echo "Usage: $0 [v1|v2]"
    echo "  v1   - Start V1 (Flask) on port 5003"
    echo "  v2   - Start V2 (FastAPI) on port 8000"
    echo ""
    read -p "Enter choice [v1/v2]: " VERSION
fi

# Kill any existing dashboard processes
echo "🔄 Checking for existing processes..."
pkill -f "python3.*dashboard.py" 2>/dev/null && echo "   Stopped existing V1" || true
pkill -f "python3.*main.py" 2>/dev/null && echo "   Stopped existing V2" || true
pkill -f "uvicorn.*main:app" 2>/dev/null && echo "   Stopped existing V2 (uvicorn)" || true

# Start Redis if needed
if ! pgrep -x "redis-server" > /dev/null; then
    echo "🔴 Starting Redis..."
    sudo systemctl start redis-server 2>/dev/null || redis-server --daemonize yes
    sleep 1
fi

cd /home/krenuser/big-data-dashboard

# Create logs directory
mkdir -p logs

case "$VERSION" in
    v1|V1)
        echo ""
        echo "🔵 STARTING V1 (Flask Dashboard)"
        echo "==========================================================================="
        echo "   Port: 5003"
        echo "   URL:  http://localhost:5003"
        echo "   Logs: logs/v1_stable.log"
        echo ""
        echo "⏳ Initializing (this may take 30-60 seconds)..."
        echo ""
        
        # Set environment variables
        export PYTHONUNBUFFERED=1
        export FLASK_APP=dashboard.py
        export SPARK_NO_DAEMONIZE=1
        
        # Trap SIGINT for graceful shutdown
        trap "echo ''; echo '🛑 Stopping V1...'; exit" INT
        
        # Run Flask app
        python3 -u dashboard.py 2>&1 | tee logs/v1_stable.log
        ;;
        
    v2|V2)
        echo ""
        echo "🟢 STARTING V2 (FastAPI Dashboard)"
        echo "==========================================================================="
        echo "   Port: 8000"
        echo "   URL:  http://localhost:8000"
        echo "   Logs: logs/v2_stable.log"
        echo "   Docs: http://localhost:8000/docs"
        echo ""
        echo "⏳ Starting FastAPI server..."
        echo ""
        
        # Set environment variables
        export PYTHONUNBUFFERED=1
        
        # Trap SIGINT for graceful shutdown
        trap "echo ''; echo '🛑 Stopping V2...'; exit" INT
        
        cd v2_advanced/backend
        
        # Check if uvicorn is installed
        if command -v uvicorn &> /dev/null; then
            # Use uvicorn CLI directly (most stable)
            echo "Using uvicorn CLI (stable mode)..."
            uvicorn main:app \
                --host 0.0.0.0 \
                --port 8000 \
                --log-level info \
                --no-access-log 2>&1 | tee ../../logs/v2_stable.log
        else
            # Fallback to Python execution
            echo "Using Python execution (fallback mode)..."
            python3 -u main.py 2>&1 | tee ../../logs/v2_stable.log
        fi
        ;;
        
    *)
        echo "❌ Invalid choice: $VERSION"
        exit 1
        ;;
esac

echo ""
echo "✅ Dashboard stopped"
echo ""
