#!/bin/bash
echo "=========================================="
echo "  BIG DATA DASHBOARD - SYSTEM CHECK"
echo "=========================================="
echo ""

# Check Dashboard
echo "1. Dashboard Status:"
if ps aux | grep -q "[p]ython3 dashboard.py"; then
    echo "   ✅ Dashboard is RUNNING"
    DASH_PID=$(ps aux | grep "[p]ython3 dashboard.py" | awk '{print $2}')
    echo "   PID: $DASH_PID"
else
    echo "   ❌ Dashboard is NOT running"
fi
echo ""

# Check Collector
echo "2. Data Collector Status:"
if ps aux | grep -q "[p]ython3 data_collector_v2.py"; then
    echo "   ✅ Collector is RUNNING"
    COLL_PID=$(ps aux | grep "[p]ython3 data_collector_v2.py" | awk '{print $2}')
    echo "   PID: $COLL_PID"
else
    echo "   ❌ Collector is NOT running"
fi
echo ""

# Check Spark
echo "3. Spark Cluster:"
WORKERS=$(ps aux | grep -c "[W]orker")
if [ $WORKERS -gt 0 ]; then
    echo "   ✅ Spark cluster ACTIVE"
    echo "   Workers detected: $WORKERS"
else
    echo "   ⚠️  No Spark workers detected"
fi
echo ""

# Check Data Files
echo "4. Data Collection:"
CRYPTO_FILES=$(find data/crypto -name "*.parquet" 2>/dev/null | wc -l)
STOCK_FILES=$(find data/stocks -name "*.parquet" 2>/dev/null | wc -l)
echo "   Crypto files: $CRYPTO_FILES"
echo "   Stock files: $STOCK_FILES"
echo ""

# Test API
echo "5. API Test:"
if curl -s http://localhost:5000/api/refresh > /dev/null 2>&1; then
    echo "   ✅ Dashboard API responding"
    CRYPTOS=$(curl -s http://localhost:5000/api/refresh | python3 -c "import sys, json; print(json.load(sys.stdin)['cryptos'])" 2>/dev/null)
    TOTAL=$(curl -s http://localhost:5000/api/refresh | python3 -c "import sys, json; print(json.load(sys.stdin)['total_records'])" 2>/dev/null)
    echo "   Current cryptos: $CRYPTOS"
    echo "   Total records: $TOTAL"
else
    echo "   ❌ Dashboard API not responding"
fi
echo ""

# Check Cluster Status API
echo "6. Cluster Status API:"
if curl -s http://localhost:5000/api/cluster_status > /dev/null 2>&1; then
    echo "   ✅ Cluster status API working"
    CLUSTER_ACTIVE=$(curl -s http://localhost:5000/api/cluster_status | python3 -c "import sys, json; print(json.load(sys.stdin)['cluster_active'])" 2>/dev/null)
    echo "   Cluster active: $CLUSTER_ACTIVE"
else
    echo "   ❌ Cluster status API failed"
fi
echo ""

echo "=========================================="
echo "  ACCESS URLs:"
echo "=========================================="
echo "  Main: http://10.0.0.8:5000"
echo "  Crypto: http://10.0.0.8:5000/crypto"
echo "  API: http://10.0.0.8:5000/api/cluster_status"
echo "=========================================="
