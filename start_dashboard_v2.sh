#!/bin/bash
# Start Dashboard V2 (Advanced FastAPI Dashboard)

echo "================================================================================"
echo " 🚀 Starting Big Data Dashboard V2 (FastAPI - Advanced)"
echo "================================================================================"
echo ""
echo "⚠️  NOTE: V2 requires additional dependencies!"
echo "   Run: pip install -r v2_advanced/requirements_v2.txt"
echo ""
echo "📊 This dashboard will be available at:"
echo "   - http://localhost:8000"
echo "   - http://10.0.0.8:8000"
echo ""
echo "📄 API Documentation:"
echo "   /api/docs ........ Swagger UI (Interactive API docs)"
echo "   /api/redoc ....... ReDoc (Alternative API docs)"
echo ""
echo "🔌 Endpoints:"
echo "   GET  /api/health ......... Health check"
echo "   GET  /api/v2/crypto/latest Latest crypto prices"
echo "   GET  /api/v2/crypto/{symbol} Crypto details"
echo "   GET  /api/v2/crypto/{symbol}/history Historical data"
echo "   WS   /ws/updates ........ WebSocket real-time updates"
echo ""
echo "⚠️  Press CTRL+C to stop the server"
echo "================================================================================"
echo ""

cd /home/krenuser/big-data-dashboard/v2_advanced/backend

# Check if required packages are installed
python3 -c "import fastapi, redis, polars" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Required packages not installed!"
    echo ""
    echo "Please install dependencies first:"
    echo "  cd /home/krenuser/big-data-dashboard"
    echo "  pip install -r v2_advanced/requirements_v2.txt"
    echo ""
    exit 1
fi

python3 main.py
