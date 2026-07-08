#!/bin/bash
# Quick Start Dashboard V1 in background

echo "Starting Dashboard V1 in background..."

cd /home/krenuser/big-data-dashboard

# Start dashboard in background and redirect output to log file
nohup python3 dashboard.py > dashboard_v1.log 2>&1 &
PID=$!

echo "Dashboard V1 started with PID: $PID"
echo "Log file: dashboard_v1.log"
echo ""
echo "Waiting for dashboard to initialize..."
sleep 5

# Check if process is still running
if ps -p $PID > /dev/null; then
    echo "✓ Dashboard is running!"
    echo ""
    echo "Access the dashboard at:"
    echo "  - http://localhost:5003"
    echo "  - http://10.0.0.8:5003"
    echo ""
    echo "To stop the dashboard: kill $PID"
    echo "To view logs: tail -f dashboard_v1.log"
else
    echo "✗ Dashboard failed to start. Check dashboard_v1.log for errors:"
    tail -20 dashboard_v1.log
    exit 1
fi
