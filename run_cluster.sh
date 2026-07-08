#!/bin/bash

###############################################################################
# RUN CLUSTER - Start 10 VM Spark Cluster
# Master: VM5 (10.0.0.8)
# Workers: VM1-4, VM6-10 (9 workers)
###############################################################################

echo "================================================================================"
echo "🖥️  STARTING 10 VM SPARK CLUSTER"
echo "================================================================================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# VM Configuration
MASTER_VM="10.0.0.8"
WORKER_VMS=("10.0.0.4" "10.0.0.5" "10.0.0.6" "10.0.0.7" "10.0.0.9" "10.0.0.10" "10.0.0.11" "10.0.0.12" "10.0.0.13")

echo "Master VM: $MASTER_VM (VM5)"
echo "Worker VMs: ${#WORKER_VMS[@]} workers"
echo ""

# Step 1: Start Master
echo "Step 1: Starting Spark Master on $MASTER_VM..."
sshpass -p "M3t01MISTXef3J6Uspck" ssh -o StrictHostKeyChecking=no krenuser@$MASTER_VM "/opt/spark/sbin/start-master.sh" 2>/dev/null

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Master started successfully${NC}"
else
    echo -e "${RED}✗ Failed to start master${NC}"
    exit 1
fi

echo "Waiting 10 seconds for master to initialize..."
sleep 10

# Step 2: Start Workers
echo ""
echo "Step 2: Starting Workers on ${#WORKER_VMS[@]} VMs..."

# VM passwords
declare -A VM_PASSWORDS
VM_PASSWORDS["10.0.0.4"]="jh87qLXHzFGt6gkb9ukV"
VM_PASSWORDS["10.0.0.5"]="ed6673p1GCasuoGn7OHS"
VM_PASSWORDS["10.0.0.6"]="93becjVKKOzJEqserofC"
VM_PASSWORDS["10.0.0.7"]="55t6o9wPd3U7Pqt4sJVV"
VM_PASSWORDS["10.0.0.9"]="lEwxgSAZa7T8lY85Z7UM"
VM_PASSWORDS["10.0.0.10"]="3rcQePQnsV5bBZ9YULSd"
VM_PASSWORDS["10.0.0.11"]="iCeKbi51jKmtL3XmEVhG"
VM_PASSWORDS["10.0.0.12"]="hww1F8lLz21cFKHYBwYL"
VM_PASSWORDS["10.0.0.13"]="j67k4k6QAl1l9TDmEgsN"

for vm in "${WORKER_VMS[@]}"; do
    echo -n "  Starting worker on $vm... "
    sshpass -p "${VM_PASSWORDS[$vm]}" ssh -o StrictHostKeyChecking=no krenuser@$vm "/opt/spark/sbin/start-worker.sh spark://$MASTER_VM:7077" &>/dev/null &
    echo -e "${GREEN}✓${NC}"
    sleep 2
done

# Step 3: Wait for workers to connect
echo ""
echo "Waiting 30 seconds for workers to connect..."
sleep 30

# Step 4: Check cluster status
echo ""
echo "Step 3: Checking cluster status..."
echo ""

# Try to fetch cluster info from Master UI
curl -s "http://$MASTER_VM:8080/json/" > /tmp/cluster_status.json 2>/dev/null

if [ $? -eq 0 ]; then
    echo "================================================================================"
    echo "  CLUSTER STATUS"
    echo "================================================================================"
    echo ""
    echo "  Master URL:  spark://$MASTER_VM:7077"
    echo "  Master UI:   http://$MASTER_VM:8080"
    echo ""
    
    # Parse JSON to count workers (simple grep)
    ALIVE_WORKERS=$(grep -o '"state" : "ALIVE"' /tmp/cluster_status.json | wc -l)
    echo "  Workers Connected: $ALIVE_WORKERS/${#WORKER_VMS[@]}"
    echo ""
    echo "================================================================================"
    echo ""
    
    if [ "$ALIVE_WORKERS" -ge "${#WORKER_VMS[@]}" ]; then
        echo -e "${GREEN}✅ ALL WORKERS CONNECTED! Cluster is ready!${NC}"
    else
        echo -e "${YELLOW}⚠️  Only $ALIVE_WORKERS/${#WORKER_VMS[@]} workers connected${NC}"
        echo "   Check http://$MASTER_VM:8080 for details"
    fi
else
    echo -e "${YELLOW}⚠️  Could not verify cluster status (Master UI not responding)${NC}"
    echo "   But cluster processes should be running"
fi

echo ""
echo "================================================================================"
echo "  NEXT STEPS"
echo "================================================================================"
echo ""
echo "1. Monitor cluster:  http://$MASTER_VM:8080"
echo "2. Test collection:  python3 main.py"
echo "3. Start continuous: ./run_continuous.sh"
echo ""
echo "To stop cluster: ./stop_cluster.sh"
echo ""

exit 0
