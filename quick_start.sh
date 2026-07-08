#!/bin/bash

###############################################################################
# QUICK START - One-Command Cluster Management
###############################################################################

echo "=========================================="
echo "🚀 SPARK CLUSTER QUICK START"
echo "=========================================="
echo ""
echo "✅ All scripts now work WITHOUT password prompts!"
echo ""
echo "CLUSTER MANAGEMENT:"
echo "  ./run_cluster.sh     - Start Master + 9 Workers"
echo "  ./stop_cluster.sh    - Stop entire cluster"
echo "  ./monitor.sh         - Real-time monitoring"
echo ""
echo "CLUSTER INFO:"
echo "  Master: 10.0.0.8 (VM5)"
echo "  Workers: 9 VMs (VM1-4, VM6-10)"
echo "  Web UI: http://10.0.0.8:8080"
echo "  Master URL: spark://10.0.0.8:7077"
echo ""
echo "DEPLOYMENT:"
echo "  ./deploy.sh          - Full deployment wizard"
echo "  python3 main.py      - Test data collection"
echo "  ./run_continuous.sh  - Start 8-day collection"
echo ""
echo "STATUS CHECK:"
echo "=========================================="

# Quick status check
if curl -s http://10.0.0.8:8080/json/ > /dev/null 2>&1; then
    WORKERS=$(curl -s http://10.0.0.8:8080/json/ | grep -o '"state" : "ALIVE"' | wc -l)
    echo "  Cluster Status: ✅ RUNNING"
    echo "  Workers Online: $WORKERS"
else
    echo "  Cluster Status: ❌ STOPPED"
    echo "  Run: ./run_cluster.sh to start"
fi

echo "=========================================="
echo ""
