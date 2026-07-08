#!/bin/bash

# ============================================================================
# BIG DATA ANALYTICS PLATFORM - LAUNCHER SCRIPT
# ============================================================================
# Authors: Shaban Ejupi & Majlinda Bajraktari
# University of Prishtina - FSHMN
# 
# This script allows you to run either V1 (2025) or V2 (2030) version
# ============================================================================

echo "========================================================================="
echo " 🚀 BIG DATA FINANCIAL ANALYTICS PLATFORM"
echo " 🎓 University of Prishtina - FSHMN"
echo " 👨‍💻 Shaban Ejupi & Majlinda Bajraktari"
echo "========================================================================="
echo ""
echo "Select version to run:"
echo "  1) V1 (2025) - Flask Classic (port 5003)"
echo "  2) V2 (2030) - Advanced AI FastAPI (port 8000)"
echo "  3) Both versions (parallel)"
echo "  4) Exit"
echo ""
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo ""
        echo "🔵 Starting V1 (2025) - Flask Classic Dashboard..."
        echo "========================================================================="
        cd /home/krenuser/big-data-dashboard
        python3 dashboard.py
        ;;
    2)
        echo ""
        echo "🟢 Starting V2 (2030) - Advanced AI Platform..."
        echo "========================================================================="
        
        # Check if Redis is running
        if ! pgrep -x "redis-server" > /dev/null; then
            echo "⚠️  Redis not running. Starting Redis for caching..."
            redis-server --daemonize yes
            sleep 2
        fi
        
        cd /home/krenuser/big-data-dashboard/v2_advanced/backend
        
        # Check if dependencies are installed
        if ! python3 -c "import fastapi" 2>/dev/null; then
            echo "📦 Installing V2 dependencies..."
            pip3 install -r ../requirements_v2.txt
        fi
        
        # Start FastAPI backend
        python3 main.py
        ;;
    3)
        echo ""
        echo "🔄 Starting BOTH versions in parallel..."
        echo "========================================================================="
        
        # Start Redis if needed
        if ! pgrep -x "redis-server" > /dev/null; then
            echo "⚠️  Starting Redis..."
            redis-server --daemonize yes
            sleep 2
        fi
        
        # Start V1 in background
        echo "🔵 Starting V1 on port 5003..."
        cd /home/krenuser/big-data-dashboard
        python3 dashboard.py > logs/v1.log 2>&1 &
        V1_PID=$!
        
        sleep 3
        
        # Start V2 in background
        echo "🟢 Starting V2 on port 8000..."
        cd /home/krenuser/big-data-dashboard/v2_advanced/backend
        python3 main.py > ../../logs/v2.log 2>&1 &
        V2_PID=$!
        
        echo ""
        echo "✅ Both versions started!"
        echo "   V1 (2025): http://localhost:5003 (PID: $V1_PID)"
        echo "   V2 (2030): http://localhost:8000 (PID: $V2_PID)"
        echo "   Frontend:  Open v2_advanced/frontend/index.html in browser"
        echo ""
        echo "📊 Logs:"
        echo "   V1: logs/v1.log"
        echo "   V2: logs/v2.log"
        echo ""
        echo "Press Ctrl+C to stop both servers..."
        
        # Wait for user interrupt
        trap "echo ''; echo 'Stopping servers...'; kill $V1_PID $V2_PID; exit" INT
        wait
        ;;
    4)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice. Exiting..."
        exit 1
        ;;
esac
