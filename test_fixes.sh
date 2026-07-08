#!/bin/bash

# Quick test script for both dashboards

echo "========================================="
echo "  Testing Dashboard Fixes"
echo "========================================="
echo ""

# Test V2 first (faster startup)
echo "1️⃣  Testing V2 (FastAPI)..."
echo "-----------------------------------------"

cd /home/krenuser/big-data-dashboard/v2_advanced/backend

# Start V2 in background
echo "Starting V2..."
python3 main.py > /tmp/v2_test.log 2>&1 &
V2_PID=$!
echo "V2 PID: $V2_PID"

# Wait for startup
echo "Waiting 5 seconds for startup..."
sleep 5

# Check if still running
if kill -0 $V2_PID 2>/dev/null; then
    echo "✅ V2 is still running!"
    
    # Test endpoint
    echo "Testing endpoint..."
    response=$(curl -s http://localhost:8000/api/v2/health 2>/dev/null)
    if [ $? -eq 0 ]; then
        echo "✅ V2 endpoint responding!"
        echo "Response: $response"
    else
        echo "❌ V2 endpoint not responding"
    fi
    
    # Make another request to ensure it doesn't shut down
    echo "Making second request..."
    sleep 2
    curl -s http://localhost:8000/ > /dev/null 2>&1
    sleep 2
    
    if kill -0 $V2_PID 2>/dev/null; then
        echo "✅ V2 still running after multiple requests!"
    else
        echo "❌ V2 shut down after requests"
    fi
    
    # Stop V2
    echo "Stopping V2..."
    kill $V2_PID 2>/dev/null
    sleep 2
else
    echo "❌ V2 shut down immediately"
    echo "Last 10 lines of log:"
    tail -n10 /tmp/v2_test.log
fi

echo ""
echo "========================================="
echo "  Test Complete"
echo "========================================="
echo ""
echo "V2 Test Log saved to: /tmp/v2_test.log"
echo ""
echo "To view full log:"
echo "  cat /tmp/v2_test.log"
echo ""
