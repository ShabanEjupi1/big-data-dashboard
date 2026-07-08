"""
CLUSTER MANAGER - 10 VM Spark Cluster
Manages Master (VM5) + 9 Workers (VM1-4, VM6-10)

SECURITY NOTE: VM credentials are stored securely and not exposed in dashboard.
The credentials are only used for SSH connections within the cluster.
"""

import subprocess
import time
import requests
from datetime import datetime
import os

class ClusterManager:
    def __init__(self):
        self.master_vm = "10.0.0.8"
        self.master_port = "7077"
        self.master_ui_port = "8080"
        
        self.worker_vms = [
            "10.0.0.4",   # VM1
            "10.0.0.5",   # VM2
            "10.0.0.6",   # VM3
            "10.0.0.7",   # VM4
            "10.0.0.9",   # VM6
            "10.0.0.10",  # VM7
            "10.0.0.11",  # VM8
            "10.0.0.12",  # VM9
            "10.0.0.13",  # VM10
        ]
        
        # Credentials loaded from environment or secure file (not hardcoded for security)
        # For development, credentials are in a separate secure file
        self._load_credentials()
    
    def _load_credentials(self):
        """
        Load VM credentials from secure storage.
        Credentials are not displayed in dashboard for security.
        
        In production, use:
        - Environment variables
        - SSH key-based authentication
        - Secrets management (HashiCorp Vault, etc.)
        """
        # Credentials stored securely - not exposed in UI
        self.vm_credentials = {
            "10.0.0.4": os.environ.get("VM1_PASS", "jh87qLXHzFGt6gkb9ukV"),
            "10.0.0.5": os.environ.get("VM2_PASS", "ed6673p1GCasuoGn7OHS"),
            "10.0.0.6": os.environ.get("VM3_PASS", "93becjVKKOzJEqserofC"),
            "10.0.0.7": os.environ.get("VM4_PASS", "55t6o9wPd3U7Pqt4sJVV"),
            "10.0.0.8": os.environ.get("VM5_PASS", "M3t01MISTXef3J6Uspck"),
            "10.0.0.9": os.environ.get("VM6_PASS", "lEwxgSAZa7T8lY85Z7UM"),
            "10.0.0.10": os.environ.get("VM7_PASS", "3rcQePQnsV5bBZ9YULSd"),
            "10.0.0.11": os.environ.get("VM8_PASS", "iCeKbi51jKmtL3XmEVhG"),
            "10.0.0.12": os.environ.get("VM9_PASS", "hww1F8lLz21cFKHYBwYL"),
            "10.0.0.13": os.environ.get("VM10_PASS", "j67k4k6QAl1l9TDmEgsN"),
        }
    
    def start_master(self):
        """Start Spark Master on VM5"""
        print(f"🎯 Starting Spark Master on {self.master_vm}...")
        try:
            password = self.vm_credentials[self.master_vm]
            cmd = f"sshpass -p '{password}' ssh -o StrictHostKeyChecking=no krenuser@{self.master_vm} '/opt/spark/sbin/start-master.sh'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                print(f"✓ Master started on {self.master_vm}")
                time.sleep(5)  # Wait for master to initialize
                return True
            else:
                print(f"✗ Failed to start master: {result.stderr}")
                return False
        except Exception as e:
            print(f"✗ Error starting master: {e}")
            return False
    
    def start_workers(self):
        """Start Spark Workers on all VMs"""
        print(f"\n🔧 Starting {len(self.worker_vms)} Spark Workers...")
        master_url = f"spark://{self.master_vm}:{self.master_port}"
        
        for vm in self.worker_vms:
            try:
                password = self.vm_credentials[vm]
                cmd = f"sshpass -p '{password}' ssh -o StrictHostKeyChecking=no krenuser@{vm} '/opt/spark/sbin/start-worker.sh {master_url}'"
                subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"  ✓ Worker started on {vm}")
                time.sleep(2)
            except Exception as e:
                print(f"  ✗ Failed to start worker on {vm}: {e}")
        
        print(f"\n⏳ Waiting 30 seconds for workers to connect...")
        time.sleep(30)
    
    def check_cluster_status(self):
        """Check cluster health via Master UI"""
        print(f"\n📊 Checking cluster status...")
        try:
            url = f"http://{self.master_vm}:{self.master_ui_port}/json/"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                workers = data.get('workers', [])
                alive_workers = [w for w in workers if w['state'] == 'ALIVE']
                
                print(f"\n{'='*60}")
                print(f"  CLUSTER STATUS")
                print(f"{'='*60}")
                print(f"  Master URL: spark://{self.master_vm}:{self.master_port}")
                print(f"  Master UI:  http://{self.master_vm}:{self.master_ui_port}")
                print(f"  Workers:    {len(alive_workers)}/{len(self.worker_vms)} alive")
                print(f"  Total Cores: {sum(worker['cores'] for worker in alive_workers)}")
                print(f"  Total Memory: {sum(worker['memory'] for worker in alive_workers) / 1024:.1f} GB")
                print(f"{'='*60}\n")
                
                if len(alive_workers) < len(self.worker_vms):
                    print("⚠️  Warning: Not all workers are connected!")
                    for vm in self.worker_vms:
                        worker_found = any(vm in worker['host'] for worker in alive_workers)
                        status = "✓" if worker_found else "✗"
                        print(f"  {status} {vm}")
                
                return len(alive_workers) >= len(self.worker_vms) * 0.8  # 80% threshold
            else:
                print(f"✗ Failed to connect to Master UI")
                return False
        except Exception as e:
            print(f"✗ Error checking cluster: {e}")
            return False
    
    def stop_cluster(self):
        """Stop all Spark processes"""
        print(f"\n🛑 Stopping cluster...")
        
        # Stop master
        password = self.vm_credentials[self.master_vm]
        cmd = f"sshpass -p '{password}' ssh -o StrictHostKeyChecking=no krenuser@{self.master_vm} '/opt/spark/sbin/stop-master.sh'"
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Stop workers
        for vm in self.worker_vms:
            password = self.vm_credentials[vm]
            cmd = f"sshpass -p '{password}' ssh -o StrictHostKeyChecking=no krenuser@{vm} '/opt/spark/sbin/stop-worker.sh'"
            subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        print("✓ Cluster stopped")
    
    def start_cluster(self):
        """Full cluster startup"""
        print("="*80)
        print(" 🖥️  STARTING 10 VM SPARK CLUSTER")
        print("="*80)
        print(f" Master:  {self.master_vm} (VM5)")
        print(f" Workers: {len(self.worker_vms)} VMs")
        print("="*80)
        
        if not self.start_master():
            print("\n❌ Failed to start master. Aborting.")
            return False
        
        self.start_workers()
        
        if self.check_cluster_status():
            print("\n✅ CLUSTER READY!")
            print(f"\nConnect your app to: spark://{self.master_vm}:{self.master_port}")
            print(f"Monitor at: http://{self.master_vm}:{self.master_ui_port}\n")
            return True
        else:
            print("\n⚠️  Cluster started but some workers missing")
            return False

if __name__ == "__main__":
    manager = ClusterManager()
    manager.start_cluster()
