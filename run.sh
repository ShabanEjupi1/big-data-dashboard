#!/bin/bash

###############################################################################
# BIG DATA DASHBOARD - Automation Script
# Universiteti i Prishtinës - KREN Server (VM5)
# Ekzekuton Spark job-in dhe nis Flask dashboard-in
###############################################################################

set -e  # Exit on error

# Colors për output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "================================================================================"
echo -e "${BLUE}🚀 BIG DATA DASHBOARD - Automation Script${NC}"
echo -e "${BLUE}📊 Universiteti i Prishtinës - KREN Server${NC}"
echo "================================================================================"
echo ""

# 1. Check Python
echo -e "${YELLOW}[1/5] Duke kontrolluar Python...${NC}"
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓ Python i gjetur: $PYTHON_VERSION${NC}"
else
    echo -e "${RED}✗ Python3 nuk u gjet! Instaloje së pari.${NC}"
    exit 1
fi

# 2. Check Spark
echo -e "${YELLOW}[2/5] Duke kontrolluar Apache Spark...${NC}"
if [ -d "/opt/spark" ]; then
    echo -e "${GREEN}✓ Spark i gjetur në /opt/spark/${NC}"
    SPARK_SUBMIT="/opt/spark/bin/spark-submit"
else
    echo -e "${RED}✗ Spark nuk u gjet në /opt/spark/${NC}"
    exit 1
fi

# 3. Install Dependencies
echo -e "${YELLOW}[3/5] Duke instaluar dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    echo "Duke instaluar paketat nga requirements.txt..."
    pip3 install --user -r requirements.txt
    echo -e "${GREEN}✓ Dependencies u instaluan${NC}"
else
    echo -e "${YELLOW}⚠ requirements.txt nuk u gjet, duke vazhduar...${NC}"
fi

# 4. Run Spark Job
echo ""
echo "================================================================================"
echo -e "${BLUE}[4/5] 🔥 DUKE EKZEKUTUAR SPARK JOB - Data Mining & ML${NC}"
echo "================================================================================"
echo ""

if [ -f "main.py" ]; then
    echo "Duke nisur Spark job me main.py..."
    echo "Kjo mund të zgjasë 2-5 minuta..."
    echo ""
    
    # Ekzekuto Spark job
    $SPARK_SUBMIT --master local[*] \
                  --driver-memory 2g \
                  --executor-memory 2g \
                  main.py
    
    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ Spark job u ekzekutua me sukses!${NC}"
        
        # Check për JSON output
        JSON_FILE=$(ls -t dashboard_*.json 2>/dev/null | head -1)
        if [ -n "$JSON_FILE" ]; then
            echo -e "${GREEN}✓ Dashboard data u krijua: $JSON_FILE${NC}"
            FILE_SIZE=$(du -h "$JSON_FILE" | cut -f1)
            echo -e "  Madhësia: $FILE_SIZE"
        else
            echo -e "${YELLOW}⚠ Asnjë JSON file nuk u gjet${NC}"
        fi
    else
        echo -e "${RED}✗ Spark job dështoi!${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ main.py nuk u gjet!${NC}"
    exit 1
fi

# 5. Start Flask Dashboard
echo ""
echo "================================================================================"
echo -e "${BLUE}[5/5] 🌐 DUKE NISUR FLASK DASHBOARD${NC}"
echo "================================================================================"
echo ""

if [ -f "dashboard.py" ]; then
    echo -e "${GREEN}Dashboard i disponueshëm në:${NC}"
    echo "  - http://localhost:5000"
    echo "  - http://10.0.0.8:5000"
    echo ""
    echo -e "${YELLOW}⚠️  Shtyp CTRL+C për të ndaluar serverin${NC}"
    echo ""
    
    # Start Flask (në foreground, jo background)
    python3 dashboard.py
else
    echo -e "${RED}✗ dashboard.py nuk u gjet!${NC}"
    exit 1
fi
