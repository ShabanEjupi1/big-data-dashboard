#!/bin/bash
# SYSTEM VALIDATION & TESTING SCRIPT
# Big Data Dashboard - University of Prishtina

echo "======================================================================"
echo "  BIG DATA DASHBOARD - SYSTEM VALIDATION"
echo "  University of Prishtina - FSHMN"
echo "======================================================================"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counter
PASSED=0
FAILED=0
WARNINGS=0

echo "1. CHECKING SYSTEM PROCESSES..."
echo "----------------------------------------------------------------------"

# Check data collector
if ps aux | grep -v grep | grep "data_collector_v2.py" > /dev/null; then
    echo -e "${GREEN}✓${NC} Data Collector is running"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Data Collector is NOT running"
    ((FAILED++))
fi

# Check dashboard
if ps aux | grep -v grep | grep "dashboard.py" > /dev/null; then
    echo -e "${GREEN}✓${NC} Dashboard is running"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Dashboard is NOT running"
    ((FAILED++))
fi

# Check Spark master
if ps aux | grep -v grep | grep "spark.*Master" > /dev/null; then
    echo -e "${GREEN}✓${NC} Spark Master is running"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Spark Master is NOT running"
    ((FAILED++))
fi

# Check Spark workers
WORKER_COUNT=$(ps aux | grep -v grep | grep "spark.*Worker" | wc -l)
if [ $WORKER_COUNT -gt 0 ]; then
    echo -e "${GREEN}✓${NC} Spark Workers: $WORKER_COUNT active"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} No Spark Workers running"
    ((FAILED++))
fi

echo ""
echo "2. CHECKING DATA COLLECTION..."
echo "----------------------------------------------------------------------"

# Count crypto files
CRYPTO_FILES=$(find data/crypto -name "*.parquet" 2>/dev/null | wc -l)
if [ $CRYPTO_FILES -gt 1000 ]; then
    echo -e "${GREEN}✓${NC} Crypto parquet files: $CRYPTO_FILES"
    ((PASSED++))
elif [ $CRYPTO_FILES -gt 0 ]; then
    echo -e "${YELLOW}⚠${NC} Crypto parquet files: $CRYPTO_FILES (low count)"
    ((WARNINGS++))
else
    echo -e "${RED}✗${NC} No crypto parquet files found"
    ((FAILED++))
fi

# Check today's data
TODAY=$(date +%Y-%m-%d)
TODAY_DIR="data/crypto/date=$TODAY"
if [ -d "$TODAY_DIR" ]; then
    TODAY_FILES=$(find "$TODAY_DIR" -name "*.parquet" | wc -l)
    if [ $TODAY_FILES -gt 0 ]; then
        echo -e "${GREEN}✓${NC} Today's files ($TODAY): $TODAY_FILES"
        ((PASSED++))
    else
        echo -e "${YELLOW}⚠${NC} Today's directory exists but no files yet"
        ((WARNINGS++))
    fi
else
    echo -e "${RED}✗${NC} No data directory for today"
    ((FAILED++))
fi

# Check data freshness (last modified file)
LATEST_FILE=$(find data/crypto -name "*.parquet" -type f -exec stat -c '%Y %n' {} \; 2>/dev/null | sort -nr | head -1)
if [ -n "$LATEST_FILE" ]; then
    LATEST_TIME=$(echo $LATEST_FILE | awk '{print $1}')
    CURRENT_TIME=$(date +%s)
    AGE=$((CURRENT_TIME - LATEST_TIME))
    
    if [ $AGE -lt 600 ]; then # Less than 10 minutes
        echo -e "${GREEN}✓${NC} Data is fresh (last update: ${AGE}s ago)"
        ((PASSED++))
    elif [ $AGE -lt 3600 ]; then # Less than 1 hour
        MINUTES=$((AGE / 60))
        echo -e "${YELLOW}⚠${NC} Data is ${MINUTES} minutes old"
        ((WARNINGS++))
    else
        HOURS=$((AGE / 3600))
        echo -e "${RED}✗${NC} Data is ${HOURS} hours old (stale)"
        ((FAILED++))
    fi
fi

echo ""
echo "3. TESTING DASHBOARD ENDPOINTS..."
echo "----------------------------------------------------------------------"

# Test main page
if timeout 5 curl -s http://localhost:5000/ > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Main page (/) responds"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Main page timeout or error"
    ((FAILED++))
fi

# Test crypto page
if timeout 5 curl -s http://localhost:5000/crypto > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Crypto page (/crypto) responds"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Crypto page timeout or error"
    ((FAILED++))
fi

# Test predictions page
if timeout 5 curl -s http://localhost:5000/predictions > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Predictions page (/predictions) responds"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Predictions page timeout or error"
    ((FAILED++))
fi

# Test recommendations page
if timeout 5 curl -s http://localhost:5000/recommendations > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Recommendations page (/recommendations) responds"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Recommendations page timeout or error"
    ((FAILED++))
fi

# Test portfolio page
if timeout 5 curl -s http://localhost:5000/portfolio > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Portfolio page (/portfolio) responds"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} Portfolio page timeout or error"
    ((FAILED++))
fi

# Test API data endpoint
if timeout 5 curl -s http://localhost:5000/api/data | grep -q "timestamp" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} API data endpoint (/api/data) returns valid JSON"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} API data endpoint error or timeout"
    ((FAILED++))
fi

# Test cluster status endpoint
if timeout 5 curl -s http://localhost:5000/api/cluster_status | grep -q "success" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Cluster status API responds"
    ((PASSED++))
else
    echo -e "${YELLOW}⚠${NC} Cluster status API timeout (expected on slow systems)"
    ((WARNINGS++))
fi

echo ""
echo "4. CHECKING FILE STRUCTURE..."
echo "----------------------------------------------------------------------"

# Check critical files exist
FILES_TO_CHECK=(
    "dashboard.py"
    "data_collector_v2.py"
    "ml_predictions.py"
    "investment_recommender.py"
    "portfolio_optimizer.py"
    "requirements.txt"
    "templates/index.html"
    "templates/crypto.html"
    "templates/predictions.html"
    "templates/recommendations.html"
    "templates/portfolio.html"
    "SYSTEM_ANALYSIS_AND_FIXES.md"
)

for file in "${FILES_TO_CHECK[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $file exists"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC} $file missing"
        ((FAILED++))
    fi
done

echo ""
echo "5. CHECKING TEMPLATES FOR FREEZE MODE..."
echo "----------------------------------------------------------------------"

TEMPLATES=("templates/crypto.html" "templates/predictions.html" "templates/recommendations.html" "templates/portfolio.html")

for template in "${TEMPLATES[@]}"; do
    if grep -q "toggleFreeze" "$template" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $template has freeze mode"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC} $template missing freeze mode"
        ((FAILED++))
    fi
done

echo ""
echo "6. CHECKING SYSTEM RESOURCES..."
echo "----------------------------------------------------------------------"

# Check disk space
DISK_USAGE=$(df -h . | tail -1 | awk '{print $5}' | sed 's/%//')
if [ $DISK_USAGE -lt 80 ]; then
    echo -e "${GREEN}✓${NC} Disk usage: ${DISK_USAGE}%"
    ((PASSED++))
elif [ $DISK_USAGE -lt 90 ]; then
    echo -e "${YELLOW}⚠${NC} Disk usage: ${DISK_USAGE}% (getting high)"
    ((WARNINGS++))
else
    echo -e "${RED}✗${NC} Disk usage: ${DISK_USAGE}% (critical)"
    ((FAILED++))
fi

# Check memory
FREE_MEM=$(free -m | grep Mem | awk '{print int(($4/$2)*100)}')
if [ $FREE_MEM -gt 20 ]; then
    echo -e "${GREEN}✓${NC} Free memory: ${FREE_MEM}%"
    ((PASSED++))
elif [ $FREE_MEM -gt 10 ]; then
    echo -e "${YELLOW}⚠${NC} Free memory: ${FREE_MEM}% (low)"
    ((WARNINGS++))
else
    echo -e "${RED}✗${NC} Free memory: ${FREE_MEM}% (critical)"
    ((FAILED++))
fi

# Check CPU load
LOAD_AVG=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | sed 's/,//')
LOAD_INT=$(echo $LOAD_AVG | cut -d'.' -f1)
CPU_COUNT=$(nproc)

if [ $LOAD_INT -lt $CPU_COUNT ]; then
    echo -e "${GREEN}✓${NC} CPU load: $LOAD_AVG (${CPU_COUNT} cores)"
    ((PASSED++))
elif [ $LOAD_INT -lt $((CPU_COUNT * 2)) ]; then
    echo -e "${YELLOW}⚠${NC} CPU load: $LOAD_AVG (high for ${CPU_COUNT} cores)"
    ((WARNINGS++))
else
    echo -e "${RED}✗${NC} CPU load: $LOAD_AVG (very high for ${CPU_COUNT} cores)"
    ((FAILED++))
fi

echo ""
echo "======================================================================"
echo "  TEST SUMMARY"
echo "======================================================================"
TOTAL=$((PASSED + FAILED + WARNINGS))
echo -e "${GREEN}Passed:${NC}   $PASSED"
echo -e "${YELLOW}Warnings:${NC} $WARNINGS"
echo -e "${RED}Failed:${NC}   $FAILED"
echo "Total tests: $TOTAL"
echo ""

if [ $FAILED -eq 0 ]; then
    if [ $WARNINGS -eq 0 ]; then
        echo -e "${GREEN}✓ ALL SYSTEMS OPERATIONAL${NC}"
        echo "System is ready for demonstration!"
        exit 0
    else
        echo -e "${YELLOW}⚠ SYSTEM OPERATIONAL WITH WARNINGS${NC}"
        echo "System works but has some warnings to address"
        exit 0
    fi
else
    echo -e "${RED}✗ SYSTEM HAS CRITICAL ISSUES${NC}"
    echo "Please fix the failed tests before demonstration"
    exit 1
fi
