#!/bin/bash

echo "=============================================================================="
echo " 🧪 TESTING DASHBOARD FIXES"
echo "=============================================================================="
echo ""

# Test V1 Dashboard
echo "📊 Testing V1 Dashboard (Flask)..."
echo "   Checking if port 5003 is available..."
if lsof -Pi :5003 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "   ⚠️  Port 5003 is in use. Testing endpoints..."
    
    echo ""
    echo "   Testing /api/data endpoint..."
    curl -s http://localhost:5003/api/data | head -c 200
    echo ""
    
    echo ""
    echo "   Testing /api/ml/predictions endpoint..."
    curl -s http://localhost:5003/api/ml/predictions | python3 -m json.tool 2>/dev/null | head -20
    echo ""
    
    echo ""
    echo "   Testing /api/investment/recommendations endpoint..."
    curl -s http://localhost:5003/api/investment/recommendations | python3 -m json.tool 2>/dev/null | head -20
    echo ""
    
    echo ""
    echo "   Testing /api/portfolio/optimize endpoint..."
    curl -s http://localhost:5003/api/portfolio/optimize | python3 -m json.tool 2>/dev/null | head -20
    echo ""
else
    echo "   ℹ️  V1 Dashboard not running on port 5003"
    echo "   Start with: ./start_dashboard.sh"
fi

echo ""
echo "=============================================================================="

# Test V2 Dashboard
echo "📊 Testing V2 Dashboard (FastAPI)..."
echo "   Checking if port 8000 is available..."
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "   ⚠️  Port 8000 is in use. Testing endpoints..."
    
    echo ""
    echo "   Testing / (root - should show HTML)..."
    curl -s http://localhost:8000/ | head -c 200
    echo ""
    
    echo ""
    echo "   Testing /api/info endpoint..."
    curl -s http://localhost:8000/api/info | python3 -m json.tool 2>/dev/null
    echo ""
    
    echo ""
    echo "   Testing /api/health endpoint..."
    curl -s http://localhost:8000/api/health | python3 -m json.tool 2>/dev/null
    echo ""
    
    echo ""
    echo "   Testing /api/v2/crypto/latest endpoint..."
    curl -s http://localhost:8000/api/v2/crypto/latest?limit=5 | python3 -m json.tool 2>/dev/null | head -30
    echo ""
else
    echo "   ℹ️  V2 Dashboard not running on port 8000"
    echo "   Start with: ./start_dashboard_v2.sh"
fi

echo ""
echo "=============================================================================="
echo " ✅ TEST COMPLETE"
echo "=============================================================================="
echo ""
echo "SUMMARY:"
echo "--------"
echo "V1 (Flask) - http://localhost:5003"
echo "  - Fixed: Broken pipe error (limited file reads)"
echo "  - Fixed: ML predictions error handling"
echo "  - Fixed: Investment recommendations error handling"
echo "  - Fixed: Portfolio optimizer error handling"
echo ""
echo "V2 (FastAPI) - http://localhost:8000"
echo "  - Fixed: Now serves HTML dashboard (not just JSON)"
echo "  - Fixed: WebSocket URL uses relative paths"
echo "  - Feature: Modern React-based UI"
echo ""
echo "To start dashboards:"
echo "  V1: ./start_dashboard.sh"
echo "  V2: ./start_dashboard_v2.sh"
echo ""
