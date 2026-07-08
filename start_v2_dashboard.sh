#!/bin/bash
# Start V2 Advanced Dashboard (2030 Edition)
# Authors: Shaban Ejupi & Majlinda Bajraktari

set -e

echo "============================================================================="
echo " 🚀 STARTING V2 ADVANCED FINANCIAL ANALYTICS PLATFORM (2030)"
echo "============================================================================="
echo ""

# Change to project directory
cd /home/krenuser/big-data-dashboard

# Check if Redis is running (required for V2)
echo "📡 Checking Redis..."
if ! pgrep -x redis-server > /dev/null; then
    echo "⚠️  Redis is not running. Starting Redis..."
    redis-server --daemonize yes --port 6379
    sleep 2
fi

if redis-cli ping > /dev/null 2>&1; then
    echo "✓ Redis is running"
else
    echo "❌ Failed to start Redis. V2 requires Redis for caching."
    echo "   Install Redis: sudo apt-get install redis-server"
    exit 1
fi

# Stop any existing V2 backend
echo ""
echo "🔄 Stopping any existing V2 backend..."
pkill -f "python.*v2_advanced/backend/main.py" 2>/dev/null || true
sleep 2

# Start V2 backend
echo ""
echo "🚀 Starting V2 Backend (FastAPI on port 8000)..."
cd v2_advanced/backend
nohup python3 main.py > /tmp/v2_backend.log 2>&1 &
V2_PID=$!
cd ../..

# Wait for backend to start
echo "⏳ Waiting for backend to initialize..."
sleep 5

# Check if backend is running
if ps -p $V2_PID > /dev/null 2>&1; then
    echo "✓ V2 Backend started successfully (PID: $V2_PID)"
    echo "   - API: http://10.0.0.8:8000"
    echo "   - Docs: http://10.0.0.8:8000/docs"
    echo "   - WebSocket: ws://10.0.0.8:8000/ws/updates"
else
    echo "❌ Failed to start V2 backend. Check logs: tail -f /tmp/v2_backend.log"
    exit 1
fi

# Test API endpoint
echo ""
echo "🧪 Testing API endpoint..."
sleep 2
if curl -s http://localhost:8000/api/v2/health > /dev/null 2>&1; then
    echo "✓ API is responding"
else
    echo "⚠️  API health check failed (this might be ok if endpoint doesn't exist)"
fi

echo ""
echo "============================================================================="
echo " ✅ V2 DASHBOARD READY!"
echo "============================================================================="
echo ""
echo "📊 Access the dashboard:"
echo "   - V2 Frontend: http://10.0.0.8:8000/ (served by FastAPI)"
echo "   - V2 API: http://10.0.0.8:8000/api/v2/*"
echo "   - API Docs: http://10.0.0.8:8000/docs"
echo ""
echo "📝 Logs:"
echo "   - Backend: tail -f /tmp/v2_backend.log"
echo ""
echo "🛑 To stop: pkill -f 'python.*v2_advanced/backend/main.py'"
echo ""
echo "============================================================================="
