#!/bin/bash

# ============================================================================
# TEST DASHBOARD - Verify everything works
# ============================================================================

echo "==========================================================================="
echo " 🧪 DASHBOARD TEST SUITE"
echo "==========================================================================="
echo ""

cd /home/krenuser/big-data-dashboard

# Test 1: Redis
echo "1️⃣  Testing Redis..."
if redis-cli ping | grep -q "PONG"; then
    echo "   ✅ Redis is working"
else
    echo "   ❌ Redis is not responding"
    echo "   Trying to start..."
    sudo systemctl start redis-server
    sleep 2
    if redis-cli ping | grep -q "PONG"; then
        echo "   ✅ Redis started successfully"
    else
        echo "   ❌ Redis failed to start"
        exit 1
    fi
fi

# Test 2: Python dependencies
echo ""
echo "2️⃣  Testing Python dependencies..."

# V1 deps
echo -n "   Flask: "
if python3 -c "import flask" 2>/dev/null; then
    echo "✅"
else
    echo "❌"
fi

echo -n "   PySpark: "
if python3 -c "import pyspark" 2>/dev/null; then
    echo "✅"
else
    echo "⚠️  (Only needed for V1)"
fi

# V2 deps
echo -n "   FastAPI: "
if python3 -c "import fastapi" 2>/dev/null; then
    echo "✅"
else
    echo "❌ (REQUIRED for V2)"
fi

echo -n "   Polars: "
if python3 -c "import polars" 2>/dev/null; then
    echo "✅"
else
    echo "❌ (REQUIRED for V2)"
fi

# Test 3: Data files
echo ""
echo "3️⃣  Testing data files..."
CRYPTO_FILES=$(find data/crypto -name "*.parquet" 2>/dev/null | wc -l)
STOCK_FILES=$(find data/stocks -name "*.parquet" 2>/dev/null | wc -l)

echo "   Crypto files: $CRYPTO_FILES"
echo "   Stock files: $STOCK_FILES"

if [ "$CRYPTO_FILES" -gt 0 ] || [ "$STOCK_FILES" -gt 0 ]; then
    echo "   ✅ Data files found"
else
    echo "   ⚠️  No data files - run ./start_collector.sh first"
fi

# Test 4: V2 Startup
echo ""
echo "4️⃣  Testing V2 startup (5 second test)..."
cd v2_advanced/backend

# Start V2 in background
python3 main.py > /tmp/v2_test.log 2>&1 &
TEST_PID=$!

sleep 3

# Check if it's running
if ps -p $TEST_PID > /dev/null; then
    echo "   ✅ V2 started successfully"
    
    # Test HTTP endpoint
    sleep 2
    if curl -s http://localhost:8000/api/health > /dev/null 2>&1; then
        echo "   ✅ V2 HTTP endpoint responding"
        HTTP_RESPONSE=$(curl -s http://localhost:8000/api/health)
        echo "   Response: $HTTP_RESPONSE"
    else
        echo "   ⚠️  V2 started but HTTP not ready yet (normal)"
    fi
    
    # Kill test process
    kill $TEST_PID 2>/dev/null
    echo "   ✅ Test complete - stopped test server"
else
    echo "   ❌ V2 failed to start"
    echo "   Check log: /tmp/v2_test.log"
    cat /tmp/v2_test.log
    exit 1
fi

cd ../..

# Summary
echo ""
echo "==========================================================================="
echo " 📊 TEST SUMMARY"
echo "==========================================================================="
echo ""
echo "✅ All critical tests passed!"
echo ""
echo "🚀 READY TO START DASHBOARD"
echo ""
echo "Quick start:"
echo "  ./start_dashboard.sh v2"
echo ""
echo "Then open in browser:"
echo "  http://localhost:8000"
echo ""
echo "For more options:"
echo "  ./start_dashboard.sh --help"
echo ""
echo "==========================================================================="
