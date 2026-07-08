#!/bin/bash

# ============================================================================
# QUICK START SCRIPT - Optimized Version
# ============================================================================
# This script provides faster startup with better diagnostics

set -e  # Exit on error

echo "==========================================================================="
echo " 🚀 STARTING BIG DATA DASHBOARD - QUICK MODE"
echo "==========================================================================="
echo ""

# Get the version choice
if [ "$1" == "v1" ] || [ "$1" == "1" ]; then
    VERSION="v1"
elif [ "$1" == "v2" ] || [ "$1" == "2" ]; then
    VERSION="v2"
elif [ "$1" == "both" ] || [ "$1" == "3" ]; then
    VERSION="both"
else
    echo "Usage: $0 [v1|v2|both]"
    echo "  v1   - Start V1 (Flask) on port 5003"
    echo "  v2   - Start V2 (FastAPI) on port 8000"
    echo "  both - Start both versions"
    echo ""
    read -p "Enter choice [v1/v2/both]: " VERSION
fi

# Kill any existing dashboard processes
echo "🔄 Checking for existing processes..."
pkill -f "python3 dashboard.py" 2>/dev/null && echo "   Stopped existing V1" || true
pkill -f "python3 main.py" 2>/dev/null && echo "   Stopped existing V2" || true

# Start Redis if needed
if ! pgrep -x "redis-server" > /dev/null; then
    echo "🔴 Starting Redis..."
    sudo systemctl start redis-server || redis-server --daemonize yes
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
        echo "   Logs: logs/v1.log"
        echo ""
        echo "⏳ Initializing Spark... (this may take 30-60 seconds)"
        echo ""
        
        # Set Python unbuffered mode for immediate output
        export PYTHONUNBUFFERED=1
        
        # Trap SIGINT to allow graceful shutdown
        trap "echo ''; echo '🛑 Stopping V1...'; exit" INT
        
        # Run in foreground with startup indicator
        python3 -u dashboard.py 2>&1 | tee logs/v1.log
        ;;
        
    v2|V2)
        echo ""
        echo "🟢 STARTING V2 (FastAPI Dashboard)"
        echo "==========================================================================="
        echo "   Port: 8000"
        echo "   URL:  http://localhost:8000"
        echo "   Logs: logs/v2.log"
        echo "   Docs: http://localhost:8000/docs"
        echo ""
        echo "⏳ Starting FastAPI server..."
        echo ""
        
        # Set Python unbuffered mode for immediate output
        export PYTHONUNBUFFERED=1
        
        # Trap SIGINT to allow graceful shutdown
        trap "echo ''; echo '🛑 Stopping V2...'; exit" INT
        
        cd v2_advanced/backend
        python3 -u main.py 2>&1 | tee ../../logs/v2.log
        ;;
        
    both|BOTH)
        echo ""
        echo "🔄 STARTING BOTH VERSIONS"
        echo "==========================================================================="
        
        # Start V1
        echo "🔵 Starting V1 in background (port 5003)..."
        python3 dashboard.py > logs/v1.log 2>&1 &
        V1_PID=$!
        echo "   PID: $V1_PID"
        
        sleep 2
        
        # Start V2
        echo "🟢 Starting V2 in background (port 8000)..."
        cd v2_advanced/backend
        python3 main.py > ../../logs/v2.log 2>&1 &
        V2_PID=$!
        echo "   PID: $V2_PID"
        
        cd ../..
        
        echo ""
        echo "✅ BOTH VERSIONS STARTED"
        echo "==========================================================================="
        echo "   V1 (Flask):  http://localhost:5003  (PID: $V1_PID)"
        echo "   V2 (FastAPI): http://localhost:8000  (PID: $V2_PID)"
        echo "   V2 Docs:     http://localhost:8000/docs"
        echo ""
        echo "📊 Monitor logs:"
        echo "   tail -f logs/v1.log"
        echo "   tail -f logs/v2.log"
        echo ""
        echo "🛑 To stop:"
        echo "   kill $V1_PID $V2_PID"
        echo "   or press Ctrl+C"
        echo ""
        
        # Wait for interrupt
        trap "echo ''; echo '🛑 Stopping servers...'; kill $V1_PID $V2_PID 2>/dev/null; exit" INT
        
        # Show real-time logs
        tail -f logs/v1.log logs/v2.log
        ;;
        
    *)
        echo "❌ Invalid choice: $VERSION"
        exit 1
        ;;
esac
