#!/bin/bash
# Start Dashboard V1 (Original Flask Dashboard)

echo "================================================================================"
echo " 🚀 Starting Big Data Dashboard V1 (Flask)"
echo "================================================================================"
echo ""
echo "📊 This dashboard will be available at:"
echo "   - http://localhost:5003"
echo "   - http://10.0.0.8:5003"
echo ""
echo "📄 Available pages:"
echo "   / ................ Homepage (Overview)"
echo "   /stocks .......... Stock Analysis"
echo "   /crypto .......... Cryptocurrency Analysis"
echo "   /predictions ..... ML Price Predictions"
echo "   /recommendations.. Investment Recommendations"
echo "   /portfolio ....... Portfolio Optimization"
echo ""
echo "⚠️  Press CTRL+C to stop the server"
echo "================================================================================"
echo ""

cd /home/krenuser/big-data-dashboard
python3 dashboard.py
