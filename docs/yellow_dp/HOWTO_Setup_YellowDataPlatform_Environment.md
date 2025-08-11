# HOWTO: Setup YellowDataPlatform Kubernetes Environment

## Tip on how to use this document.

I (Billy) am testing this on my Macbook Pro with cursor and docker desktop installed. Docker desktop is running and has kubernetes enabled. Open cursor and select this file in your edit window in cursor. Make a new AI chat, it should automatically add this file in the chat. Select the auto model. Use the following prompt to stand up your YellowDataPlatform environment.

```text
I want to stand up a  yellowdataplatform on my local kubernetes machine. Please follow the instructions in @HOWTO_Setup_YellowDataPlatform_Environment.md exactly. The gut hub PAT to use is:
put_your_git_pat_here
```

The first time you do this, it will download the different container images used (postgres/airflow/kafka/etc). This will take a couple of minutes. The AI usually sees this and automatically adds in waits for the containers to be ready.

The running environment takes just over 4GB of memory in docker. I use 2 assigned CPUs. I have assigned 2 CPUs, 24GB of memory and 180GB of disk space to docker desktop. My machine has 96GB of RAM and 2TB of disk. When configuring kubernetes in docker desktop, I use kind with 1.32.3 and 6 nodes.

I have also tested this on an M2 Macbook Air with 24GB RAM and 2TB SSD. The same docker desktop settings. It's slower and tight on memory (20GB used total) but it does work.

## Overview

This document provides a step-by-step guide to set up a complete YellowDataPlatform environment on Kubernetes. 

It is designed as an AI first document, easy to following repeatedly by an AI assistant to speed installs. Tested in Cursor with a chat session in auto mode.

The setup uses a two-ring approach:

- **Ring 0**: Generate bootstrap artifacts (runs in Docker container)
- **Ring 1**: Deploy to Kubernetes with full infrastructure (requires secrets and K8s cluster)

## Prerequisites

- Docker Desktop with Kubernetes enabled
- kubectl configured for your target cluster
- GitHub repository with your datasurface ecosystem model
- GitHub Personal Access Token with repository access

## Phase 1: Bootstrap Artifact Generation (Ring 0)

### Step 1: Clone the Starter Repository

**Optional: Remove Previous Environment**
If you have an existing yellow_starter deployment, clean it up first:
```bash
# Remove old Kubernetes namespace and all resources
kubectl delete namespace ns-yellow-starter

# Remove local artifacts (if reusing same directory)
rm -rf yellow_starter/generated_output/
```

**Clone Fresh Repository**
```bash
git clone https://github.com/billynewport/yellow_starter.git
cd yellow_starter
```

### Step 2: Configure the Ecosystem Model

```bash
# Review the ecosystem model
cat eco.py

# Review the platform assignments file (should already exist with correct format)
cat dsg_platform_mapping.json

# Expected format for dsg_platform_mapping.json:
# [
#   {
#     "dsgName": "LiveDSG",
#     "workspace": "Consumer1",
#     "assignments": [
#       {
#         "dataPlatform": "YellowLive",
#         "documentation": "Live Yellow DataPlatform",
#         "productionStatus": "PRODUCTION",
#         "deprecationsAllowed": "NEVER",
#         "status": "PROVISIONED"
#       }
#     ]
#   },
#   {
#     "dsgName": "ForensicDSG", 
#     "workspace": "Consumer1",
#     "assignments": [
#       {
#         "dataPlatform": "YellowForensic",
#         "documentation": "Forensic Yellow DataPlatform", 
#         "productionStatus": "PRODUCTION",
#         "deprecationsAllowed": "NEVER",
#         "status": "PROVISIONED"
#       }
#     ]
#   }
# ]
```

**Key configurations to verify:**
- DataPlatform names and credentials
- PostgreSQL hostname and port configuration
- Merge database name specification
- Datastore connection details
- Workspace and DatasetGroup configurations

### Step 3: Generate Bootstrap Artifacts

```bash
# Generate artifacts for both platforms
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  datasurface/datasurface:latest \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Test_DP
```

### Step 4: Verify Generated Artifacts

```bash
# Check the generated files
ls -la generated_output/Test_DP/
```

**Expected artifacts for PSP (Test_DP):**
- `kubernetes-bootstrap.yaml` - Kubernetes deployment configuration
- `test_dp_infrastructure_dag.py` - Platform management DAG
- `test_dp_model_merge_job.yaml` - Model merge job for populating ingestion stream configurations
- `test_dp_ring1_init_job.yaml` - Ring 1 initialization job for creating database schemas
- `test_dp_reconcile_views_job.yaml` - Workspace views reconciliation job

### Step 5: Validate Configuration

## Phase 2: Kubernetes Infrastructure Setup (Ring 1)

### Step 1: Create Kubernetes Namespace and Secrets

```bash
# Create namespace
kubectl create namespace ns-yellow-starter

# Create database credentials secret (consistent format for all components)
kubectl create secret generic postgres \
  --from-literal=postgres_USER=postgres \
  --from-literal=postgres_PASSWORD=datasurface123 \
  -n ns-yellow-starter

# Create GitHub credentials secret  
kubectl create secret generic git \
  --from-literal=token=your-github-personal-access-token \
  -n ns-yellow-starter

```

**Default Credentials:**
- **PostgreSQL Database**: `postgres/datasurface123`
- **Airflow Web UI**: `admin/admin123` (created after deployment)
- **GitHub Token**: Replace `your-github-token` with your actual GitHub Personal Access Token

### Step 2: Deploy PostgreSQL Database

```bash
# Apply the Kubernetes configuration
kubectl apply -f generated_output/Test_DP/kubernetes-bootstrap.yaml
```

### Step 3: Run Ring 1 Initialization

Ring 1 initialization creates the database schemas required for the platform operations.

```bash
# Wait for PostgreSQL to be ready
kubectl wait --for=condition=ready pod -l app=pg-postgres -n ns-yellow-starter --timeout=300s

# Create required databases manually (required before Ring 1 initialization)
kubectl exec -it deployment/pg-postgres -n ns-yellow-starter -- psql -U postgres -c "CREATE DATABASE airflow_db;"
kubectl exec -it deployment/pg-postgres -n ns-yellow-starter -- psql -U postgres -c "CREATE DATABASE customer_db;"
kubectl exec -it deployment/pg-postgres -n ns-yellow-starter -- psql -U postgres -c "CREATE DATABASE datasurface_merge;"

# Create source tables and initial test data using the data simulator
# This creates the customers and addresses tables with initial data and simulates some changes and leaves it running continuously.
kubectl run data-simulator --rm -i --restart=Never \
  --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=postgres" \
  --env="POSTGRES_PASSWORD=datasurface123" \
  -n ns-yellow-starter \
  -- python src/tests/data_change_simulator.py \
  --host pg-data.ns-yellow-starter.svc.cluster.local \
  --port 5432 \
  --database customer_db \
  --user postgres \
  --password datasurface123 \
  --create-tables \
  --max-changes 1000000 \
  --verbose &

# Wait a moment for the data simulator to start creating tables, then continue
echo "Data simulator started in background. Continuing with setup..."
echo "Note: The data simulator will run continuously for days, simulating ongoing data changes."
echo "You can monitor it with: kubectl logs data-simulator -n ns-yellow-starter -f"
sleep 10

# Apply Ring 1 initialization job (creates platform database schemas)
kubectl apply -f generated_output/Test_DP/test_dp_ring1_init_job.yaml

# Wait for Ring 1 initialization to complete
kubectl wait --for=condition=complete job/test-dp-ring1-init -n ns-yellow-starter --timeout=300s
```

### Step 4: Verify Airflow Services

Airflow requires database initialization before the webserver can start properly.

```bash
# Wait for Airflow scheduler to be ready
kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n ns-yellow-starter --timeout=300s

# Initialize Airflow database (required for webserver to start)
kubectl exec -it deployment/airflow-scheduler -n ns-yellow-starter -- airflow db init

# Wait for Airflow webserver to be ready (after database initialization)
kubectl wait --for=condition=ready pod -l app=airflow-webserver -n ns-yellow-starter --timeout=300s

# Verify Airflow database connection
kubectl exec -it deployment/airflow-scheduler -n ns-yellow-starter -- airflow db check
```

### Step 5: Create Airflow Admin User

```bash
# Create Airflow admin user
kubectl exec -it deployment/airflow-scheduler -n ns-yellow-starter -- \
  airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin123
```

### Step 6: Deploy DAG Factory and Model Merge Jobs

```bash
# Get the current scheduler pod name
SCHEDULER_POD=$(kubectl get pods -n ns-yellow-starter -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')

# Copy infrastructure DAG to Airflow
kubectl cp generated_output/Test_DP/test_dp_infrastructure_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter

# Deploy model merge job to populate ingestion stream configurations
kubectl apply -f generated_output/Test_DP/test_dp_model_merge_job.yaml

# Wait for model merge job to complete
kubectl wait --for=condition=complete job/test-dp-model-merge-job -n ns-yellow-starter --timeout=300s

# Deploy reconcile views job to create/update workspace views
kubectl apply -f generated_output/Test_DP/test_dp_reconcile_views_job.yaml

# Wait for reconcile views job to complete
kubectl wait --for=condition=complete job/test-dp-reconcile-views-job -n ns-yellow-starter --timeout=300s

# Restart Airflow scheduler to trigger factory DAGs (creates dynamic ingestion stream DAGs)
kubectl delete pod -n ns-yellow-starter -l app=airflow-scheduler
kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n ns-yellow-starter --timeout=300s
```

### Step 7: Verify Deployment

```bash
# Check all pods are running
kubectl get pods -n ns-yellow-starter

# Verify databases exist
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'SELECT datname FROM pg_database;'"

# Access Airflow web interface
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n ns-yellow-starter
```

The kafka and kafka connect pods are not used and may be ignored.

Open http://localhost:8080 and login with:
- **Username**: `admin`
- **Password**: `admin123`

**Expected DAGs in Airflow UI:**
- `test-dp_infrastructure` - Platform infrastructure management (paused by default)
- `yellowlive_factory_dag` - YellowLive platform factory (created dynamically)
- `yellowlive_datatransformer_factory` - YellowLive datatransformer factory (created dynamically, paused)
- `yellowforensic_factory_dag` - YellowForensic platform factory (created dynamically)
- `yellowforensic_datatransformer_factory` - YellowForensic datatransformer factory (created dynamically, paused)
- `yellowlive__Store1_ingestion` - YellowLive Store1 ingestion stream DAG (created dynamically)
- `yellowforensic__Store1_ingestion` - YellowForensic Store1 ingestion stream DAG (created dynamically)
- `yellowlive__MaskedCustomers_dt_ingestion` - YellowLive DataTransformer output ingestion (created dynamically)
- `yellowforensic__MaskedCustomers_dt_ingestion` - YellowForensic DataTransformer output ingestion (created dynamically)
- `yellowlive__MaskedStoreGenerator_datatransformer` - YellowLive DataTransformer execution DAG (created dynamically)
- `yellowforensic__MaskedStoreGenerator_datatransformer` - YellowForensic DataTransformer execution DAG (created dynamically)

## Ring Level Explanation

**Ring 0**: Generate artifacts only (no external dependencies)
- Creates Kubernetes YAML, DAG files, and job templates
- Requires no external services, runs in Docker container
- Generated artifacts are ready for direct deployment

**Ring 1**: Initialize databases and runtime configuration (requires Kubernetes cluster)
- Would create platform-specific database schemas and configurations

## Troubleshooting

### Container Issues
```bash
# Rebuild container if needed
docker build -f Dockerfile.datasurface -t datasurface/datasurface:latest .
docker push datasurface/datasurface:latest
```

### Model Validation
- Verify `eco.py` syntax is correct
- Ensure platform names match in ecosystem and mapping files
- Check all required imports are available

### Kubernetes Issues
```bash
# Check pod status
kubectl describe pod <pod-name> -n ns-yellow-starter

# View logs
kubectl logs <pod-name> -n ns-yellow-starter

# Check all pods in namespace
kubectl get pods -n ns-yellow-starter

# Check services
kubectl get svc -n ns-yellow-starter
```

### Database Connection
```bash
# Test database connectivity (non-interactive)
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "pg_isready -U postgres -h localhost"

# List databases (non-interactive)
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'SELECT datname FROM pg_database;'"

# Test with expected credentials
# Username: postgres
# Password: datasurface123
```
AI 
**💡 AI Tip: Database Querying Best Practices**
When querying the database through kubectl exec, use non-interactive SQL commands with proper password handling:

```bash
# ✅ CORRECT: Use PGPASSWORD environment variable with bash -c wrapper
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'SELECT datname FROM pg_database;'"

# ✅ CORRECT: Check specific database tables
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -d customer_db -c 'SELECT table_name FROM information_schema.tables WHERE table_schema = '\''public'\'';'"

# ✅ CORRECT: Test database connectivity first
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "pg_isready -U postgres -h localhost"

# ❌ AVOID: Interactive mode with -it flag (hangs in non-interactive environments)
kubectl exec -it deployment/pg-postgres -n ns-yellow-starter -- psql -U postgres -l

# ❌ AVOID: Without PGPASSWORD (hangs waiting for password input)
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -c 'SELECT datname FROM pg_database;'"

# ❌ AVOID: psql meta-commands like \l, \dv (require interactive mode)
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -l"
```

**Key improvements:**
- Use `PGPASSWORD=datasurface123` environment variable to avoid password prompts
- Specify `-h localhost` to be explicit about the host connection
- Use single quotes around SQL queries to avoid escaping issues
- Test connectivity with `pg_isready` before running queries
- Use standard SQL (`SELECT`) instead of psql meta-commands (`\l`, `\dv`, etc.)
- Remove `-it` flag completely for non-interactive environments

### Common Issues and Solutions

**Issue: Ingestion stream DAGs not appearing in Airflow UI**
```bash
# Cause: Factory DAGs haven't run yet or model merge job failed
# Solution: Verify model merge job completed and restart scheduler
kubectl get jobs -n ns-yellow-starter
kubectl logs job/test-dp-model-merge-job -n ns-yellow-starter
kubectl delete pod -n ns-yellow-starter -l app=airflow-scheduler
```

**Issue: Ring 1 initialization job fails**
```bash
# Cause: Secret keys mismatch or database not accessible
# Solution: Verify secret format and database connectivity
kubectl describe job/test-dp-ring1-init -n ns-yellow-starter
kubectl logs job/test-dp-ring1-init -n ns-yellow-starter
```

**Issue: Secret configuration errors**
```bash
# Cause: Inconsistent secret key names
# Verify all secrets use postgres_USER/postgres_PASSWORD format:
kubectl get secret postgres -n ns-yellow-starter -o yaml
```

**Issue: PostgreSQL commands hang or fail in non-interactive environments**
```bash
# Cause: Using interactive psql commands or missing password handling
# Symptoms: Commands hang indefinitely or fail with authentication errors
# Solution: Use PGPASSWORD environment variable and avoid interactive flags

# ✅ Test database connectivity first:
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "pg_isready -U postgres -h localhost"

# ✅ List databases (non-interactive):
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'SELECT datname FROM pg_database;'"

# ✅ Check specific database tables:
kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -d customer_db -c 'SELECT table_name FROM information_schema.tables WHERE table_schema = '\''public'\'';'"

# ❌ AVOID these approaches (they will hang):
# kubectl exec -it deployment/pg-postgres -n ns-yellow-starter -- psql -U postgres -l
# kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "psql -U postgres -c 'SELECT datname FROM pg_database;'"
```

## Next Steps

Once deployment is complete:
1. Access Airflow UI at http://localhost:8080 (admin/admin123) to view and trigger DAGs
2. Test ingestion jobs by manually triggering the generated ingestion stream DAGs
3. Optionally run continuous data simulation for ongoing testing (see below)
4. Create workspace views for data access
5. Monitor pipeline execution and DAG health

## Verify all kubernetes pods and airflow DAGs are running with no errors

Pull the logs for all kubernetes pods and check all airflow DAG latest run logs for any issues.

## Testing with Continuous Data Simulation

To continuously simulate realistic data changes for testing the ingestion pipeline:

```bash
# Run continuous data simulation in a Kubernetes pod (background execution)
kubectl run data-simulator-continuous --rm -i --restart=Never \
  --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=postgres" \
  --env="POSTGRES_PASSWORD=datasurface123" \
  -n ns-yellow-starter \
  -- python src/tests/data_change_simulator.py \
  --host pg-data.ns-yellow-starter.svc.cluster.local \
  --port 5432 \
  --database customer_db \
  --user postgres \
  --password datasurface123 \
  --min-interval 10 \
  --max-interval 30 \
  --verbose &

# The simulator will run continuously in the background
# You can continue with other tasks while it runs

# To stop the simulator from another terminal:
# kubectl delete pod data-simulator-continuous -n ns-yellow-starter

# To check if the simulator is still running:
# kubectl get pods -n ns-yellow-starter | grep data-simulator

# To view simulator logs:
# kubectl logs data-simulator-continuous -n ns-yellow-starter -f
```

**What the data simulator does:**
- Creates new customers with addresses (15% of changes)
- Updates customer information like email and phone (25% of changes)  
- Adds new addresses for existing customers (20% of changes)
- Updates existing addresses (30% of changes)
- Occasionally deletes old addresses (10% of changes)

**Observing the Pipeline:**
- Watch the Airflow DAGs process the simulated changes
- Monitor staging and merge tables for new data
- Verify both live and forensic processing modes work correctly

## Success Criteria

**✅ Infrastructure Deployed:**
- PostgreSQL running with airflow_db, customer_db, and datasurface_merge databases
- Airflow scheduler and webserver operational
- Admin user created (admin/admin123)
- All required Kubernetes secrets configured

**✅ DAGs Deployed:**
- Factory DAGs for dynamic ingestion stream creation
- Infrastructure DAGs for platform management
- All DAGs visible in Airflow UI without parsing errors

**✅ Ready for Data Pipeline:**
- Source database (customer_db) ready for ingestion
- Merge database (datasurface_merge) ready for platform operations
- Clean configuration with no manual fixes required

## Remote Kubernetes Deployment

This section explains how to deploy YellowDataPlatform on a remote Kubernetes machine (e.g., VM, cloud instance, or remote server) instead of local Docker Desktop.

### Prerequisites for Remote Deployment

**Remote Machine Requirements:**
- Ubuntu 24.04 LTS or compatible Linux distribution
- Minimum 32GB RAM (recommended for production workloads)
- 4+ CPU cores
- 100GB+ available disk space
- Network connectivity for Docker image downloads
- SSH access with sudo privileges

**Local Machine Requirements:**
- SSH client configured for remote machine access
- kubectl configured to access remote cluster (optional, for local management)

### Step 1: Set Up Remote Kubernetes Cluster

**Install k3s (Lightweight Kubernetes):**
```bash
# On remote machine
curl -sfL https://get.k3s.io | sh -

# Verify installation
sudo systemctl status k3s
sudo k3s kubectl get nodes
```

**Configure kubectl for remote access:**
```bash
# Copy kubeconfig from remote machine to local machine
sudo cat /etc/rancher/k3s/k3s.yaml > ~/.kube/config-k3s-remote

# Update server IP in kubeconfig (replace with actual remote IP)
sed -i 's|server: https://127.0.0.1:6443|server: https://REMOTE_IP:6443|g' ~/.kube/config-k3s-remote

# Test connection (use --insecure-skip-tls-verify for self-signed certificates)
kubectl --kubeconfig ~/.kube/config-k3s-remote --insecure-skip-tls-verify get nodes
```

### Step 2: Install Required Software on Remote Machine

**Install Docker and Git:**
```bash
# On remote machine
sudo apt update
sudo apt install -y docker.io docker-compose git

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER

# Verify Docker installation
docker --version
docker run --rm hello-world
```

### Step 3: Deploy YellowDataPlatform Remotely

**Clone and Generate Artifacts:**
```bash
# On remote machine
cd ~
git clone https://github.com/billynewport/yellow_starter.git
cd yellow_starter

# Generate bootstrap artifacts
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  datasurface/datasurface:latest \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Test_DP
```

**Deploy to Remote Kubernetes:**
```bash
# Create namespace and secrets (use sudo for k3s)
sudo kubectl create namespace ns-yellow-starter

sudo kubectl create secret generic postgres \
  --from-literal=postgres_USER=postgres \
  --from-literal=postgres_PASSWORD=datasurface123 \
  -n ns-yellow-starter

sudo kubectl create secret generic git \
  --from-literal=token=your-github-personal-access-token \
  -n ns-yellow-starter

# Deploy infrastructure
sudo kubectl apply -f generated_output/Test_DP/kubernetes-bootstrap.yaml
```

**Initialize Databases and Deploy DAGs:**
```bash
# Wait for PostgreSQL to be ready
sudo kubectl wait --for=condition=ready pod -l app=pg-postgres -n ns-yellow-starter --timeout=300s

# Create required databases
sudo kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'CREATE DATABASE airflow_db;'"
sudo kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'CREATE DATABASE customer_db;'"
sudo kubectl exec deployment/pg-postgres -n ns-yellow-starter -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'CREATE DATABASE datasurface_merge;'"

# Run Ring 1 initialization
sudo kubectl apply -f generated_output/Test_DP/test_dp_ring1_init_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-ring1-init -n ns-yellow-starter --timeout=300s

# Initialize Airflow
sudo kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n ns-yellow-starter --timeout=300s
sudo kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- airflow db init
sudo kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin123

# Deploy DAGs and jobs
SCHEDULER_POD=$(sudo kubectl get pods -n ns-yellow-starter -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')
sudo kubectl cp generated_output/Test_DP/test_dp_infrastructure_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter

# Deploy model merge and reconcile jobs
sudo kubectl apply -f generated_output/Test_DP/test_dp_model_merge_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-model-merge-job -n ns-yellow-starter --timeout=300s

sudo kubectl apply -f generated_output/Test_DP/test_dp_reconcile_views_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-reconcile-views-job -n ns-yellow-starter --timeout=300s

# Restart scheduler to trigger factory DAGs
sudo kubectl delete pod -n ns-yellow-starter -l app=airflow-scheduler
sudo kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n ns-yellow-starter --timeout=300s
```

### Step 4: Access Remote YellowDataPlatform

**Set up port forwarding:**
```bash
# On remote machine (background execution)
sudo kubectl port-forward svc/airflow-webserver-service 8080:8080 -n ns-yellow-starter &

# Or from local machine via SSH tunnel
ssh -L 8080:localhost:8080 user@remote-machine-ip
```

**Access Airflow Web Interface:**
- **URL**: http://remote-machine-ip:8080
- **Username**: `admin`
- **Password**: `admin123`

### Step 5: Remote Management Commands

**Check cluster status:**
```bash
# From local machine
kubectl --kubeconfig ~/.kube/config-k3s-remote --insecure-skip-tls-verify get pods -n ns-yellow-starter

# Or via SSH
ssh user@remote-machine-ip "sudo kubectl get pods -n ns-yellow-starter"
```

**View logs:**
```bash
# Via SSH
ssh user@remote-machine-ip "sudo kubectl logs <pod-name> -n ns-yellow-starter"

# Or from local machine
kubectl --kubeconfig ~/.kube/config-k3s-remote --insecure-skip-tls-verify logs <pod-name> -n ns-yellow-starter
```

**Run data simulator:**
```bash
# On remote machine
sudo kubectl run data-simulator --rm -i --restart=Never \
  --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=postgres" \
  --env="POSTGRES_PASSWORD=datasurface123" \
  -n ns-yellow-starter \
  -- python src/tests/data_change_simulator.py \
  --host pg-data.ns-yellow-starter.svc.cluster.local \
  --port 5432 \
  --database customer_db \
  --user postgres \
  --password datasurface123 \
  --create-tables \
  --max-changes 1000000 \
  --verbose
```

### Step 6: Set Up NFS for Git Caching (Required for YellowDataPlatform)

**NFS Server and Client Setup:**
The YellowDataPlatform uses NFS for shared git repository caching between pods. This requires specific kernel modules and packages to be installed on the host system.

**Install NFS Server Modules (Required):**
```bash
# On remote machine - Load NFS kernel modules
sudo modprobe nfs
sudo modprobe nfsd

# Verify modules are loaded
lsmod | grep nfs

# Expected output:
# nfsd                  847872  0
# auth_rpcgss           184320  1 nfsd
# nfs_acl                12288  1 nfsd
# nfs                   569344  0
# lockd                 143360  2 nfsd,nfs
# grace                  12288  2 nfsd,lockd
# sunrpc                802816  5 nfsd,auth_rpcgss,lockd,nfs_acl,nfs
```

**Install NFS Client Utilities (Required):**
```bash
# On remote machine - Install NFS client packages
sudo apt update
sudo apt install -y nfs-common

# Verify installation
which mount.nfs
# Expected: /sbin/mount.nfs
```

**Make NFS Modules Persistent (Recommended):**
```bash
# Ensure NFS modules load on boot
echo 'nfs' | sudo tee -a /etc/modules
echo 'nfsd' | sudo tee -a /etc/modules
```

### Step 7: Handle Multi-Platform Docker Images

**For Apple Silicon Development Machines:**
If developing on Apple Silicon (M1/M2) and deploying to x86/AMD64 remote machines, build multi-platform images:

```bash
# On local development machine
cd /path/to/datasurface

# Build and push multi-platform image
docker buildx build --platform linux/amd64,linux/arm64 \
  -f Dockerfile.datasurface \
  -t datasurface/datasurface:latest \
  --push .
```

**Pull Updated Image on Remote Machine:**

For Kubernetes environments using containerd (K3s, most modern clusters):
```bash
# On remote machine - use crictl for containerd (Kubernetes)
sudo crictl pull datasurface/datasurface:latest
```

For Docker-based environments:
```bash
# On remote machine - use docker for Docker environments  
docker pull datasurface/datasurface:latest
```

**Note:** Most Kubernetes distributions (including K3s) use containerd as the container runtime, not Docker. Always use `crictl` commands for Kubernetes clusters to ensure images are cached in the correct location that Kubernetes can access.

### Remote Deployment Considerations

**Network Configuration:**
- Ensure firewall allows port 8080 for Airflow web interface
- Configure SSH access for remote management
- Consider using Tailscale or VPN for secure remote access

**Resource Management:**
- Monitor memory usage (YellowDataPlatform uses ~4GB RAM)
- Ensure adequate disk space for logs and data
- Consider persistent storage for production deployments

**Security:**
- Use strong passwords for admin accounts
- Configure proper network policies
- Regularly update system packages and container images

**NFS-Specific Requirements:**
- NFS server requires kernel modules: `nfs`, `nfsd`
- NFS client requires: `nfs-common` package
- Host path storage must support NFS exports
- Node selectors may need adjustment for cluster node names

**Troubleshooting Remote Issues:**
```bash
# Check system resources
ssh user@remote-machine-ip "free -h && df -h"

# Verify k3s status
ssh user@remote-machine-ip "sudo systemctl status k3s"

# Check NFS modules
ssh user@remote-machine-ip "lsmod | grep nfs"

# Check NFS client tools
ssh user@remote-machine-ip "which mount.nfs"

# Check pod status and logs
ssh user@remote-machine-ip "sudo kubectl get pods -n ns-yellow-starter && sudo kubectl describe pod <pod-name> -n ns-yellow-starter"
```

## Troubleshooting: NFS and Remote Kubernetes Issues

### Problem: NFS Server Pod Stuck in "Pending" Status

**Symptoms:**
- NFS server pod shows `STATUS: Pending`
- Ring 1 initialization pod stuck in `ContainerCreating`
- Events show "FailedScheduling" with node affinity conflicts

**Root Causes and Solutions:**

#### **Issue 1: Incorrect Node Selector**
**Cause:** Generated YAML uses wrong node name (e.g., `desktop-worker` instead of actual node name)

**Solution:**
```bash
# Check actual node names
kubectl get nodes

# Fix node selector in NFS deployment
kubectl patch deployment test-dp-nfs-server -n ns-yellow-starter --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/nodeSelector/kubernetes.io~1hostname", "value": "actual-node-name"}
]'
```

#### **Issue 2: PV Node Affinity Mismatch**
**Cause:** Persistent Volume created with wrong node affinity

**Solution:**
```bash
# Delete and recreate PV/PVC with correct node affinity
kubectl delete pvc test-dp-nfs-server-pvc -n ns-yellow-starter
kubectl delete pv test-dp-nfs-server-pv

# Create corrected PV with proper node affinity
cat << 'EOF' | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: test-dp-nfs-server-pv
spec:
  capacity:
    storage: 5Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  hostPath:
    path: /opt/Test_DP-nfs-storage
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - actual-node-name
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-dp-nfs-server-pvc
  namespace: ns-yellow-starter
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  volumeName: test-dp-nfs-server-pv
  storageClassName: ""
EOF
```

#### **Issue 3: Missing NFS Kernel Modules**
**Cause:** NFS server container fails with "nfs module is missing"

**Symptoms:**
```
----> ERROR: nfs module is not loaded in the Docker host's kernel (try: modprobe nfs)
```

**Solution:**
```bash
# Load required NFS modules on host
sudo modprobe nfs
sudo modprobe nfsd

# Verify modules are loaded
lsmod | grep nfs

# Restart NFS server pods
kubectl delete pod -l app=test-dp-nfs-server -n ns-yellow-starter
```

#### **Issue 4: Missing NFS Client Utilities**
**Cause:** Kubernetes nodes cannot mount NFS volumes

**Symptoms:**
```
MountVolume.SetUp failed for volume "test-dp-git-model-cache-pv" : mount failed: exit status 32
mount: bad option; for several filesystems (e.g. nfs, cifs) you might need a /sbin/mount.<type> helper program
```

**Solution:**
```bash
# Install NFS client utilities on all nodes
sudo apt update
sudo apt install -y nfs-common

# Restart affected pods
kubectl delete job test-dp-ring1-init -n ns-yellow-starter
# Reapply the job
kubectl apply -f generated_output/Test_DP/test_dp_ring1_init_job.yaml
```

#### **Issue 5: DNS Resolution for NFS Service Names**
**Cause:** Kubelet uses host DNS (not cluster DNS) for NFS mounts, but host cannot resolve Kubernetes service names

**Symptoms:**
```
MountVolume.SetUp failed for volume "test-dp-git-model-cache-pv" : mount failed: exit status 32
Output: mount.nfs: Failed to resolve server test-dp-nfs-service.ns-yellow-starter.svc.cluster.local: Name or service not known
```

**Verification:**
```bash
# Test if host can resolve Kubernetes service names
getent hosts test-dp-nfs-service.ns-yellow-starter.svc.cluster.local

# If this fails but cluster DNS works:
kubectl run dns-test --image=busybox --rm -it --restart=Never -n ns-yellow-starter -- nslookup test-dp-nfs-service.ns-yellow-starter.svc.cluster.local
```

**Solution:**
```bash
# Add cluster DNS to host /etc/resolv.conf
echo 'nameserver 10.43.0.10' | sudo tee -a /etc/resolv.conf

# Verify host can now resolve service names
getent hosts test-dp-nfs-service.ns-yellow-starter.svc.cluster.local

# Expected output:
# 10.43.161.139   test-dp-nfs-service.ns-yellow-starter.svc.cluster.local
```

**Why This Works:**
- Kubernetes pods use cluster DNS (10.43.0.10) for service name resolution
- NFS mount operations are performed by kubelet on the host, using host DNS
- Host DNS typically only resolves external domains, not cluster service names
- Adding cluster DNS to host configuration enables DNS-based NFS mounting

### Problem: Docker Image Architecture Mismatch

**Symptoms:**
- `docker pull` fails with "no matching manifest for linux/amd64"
- Pods fail to start with image pull errors

**Cause:** Image built on Apple Silicon (ARM64) but deployed to x86/AMD64 Linux

**Solution:**
```bash
# Build multi-platform image on development machine
docker buildx build --platform linux/amd64,linux/arm64 \
  -f Dockerfile.datasurface \
  -t datasurface/datasurface:latest \
  --push .

# Pull updated image on remote machine
# For Kubernetes environments (K3s, most modern clusters):
sudo crictl pull datasurface/datasurface:latest

# For Docker-based environments:
# docker pull datasurface/datasurface:latest
```

### Verification Steps for NFS Setup

**Check NFS Server Status:**
```bash
# Verify NFS server is running and ready
kubectl logs -l app=test-dp-nfs-server -n ns-yellow-starter --tail=10

# Expected success output:
# ==================================================================
#       READY AND WAITING FOR NFS CLIENT CONNECTIONS
# ==================================================================
```

**Test NFS Mount:**
```bash
# Test NFS mount with simple pod
kubectl run nfs-test --rm -it --restart=Never --image=busybox -n ns-yellow-starter \
  --overrides='{"spec":{"containers":[{"name":"nfs-test","image":"busybox","command":["ls","-la","/cache"],"volumeMounts":[{"name":"nfs-cache","mountPath":"/cache"}]}],"volumes":[{"name":"nfs-cache","persistentVolumeClaim":{"claimName":"test-dp-git-model-cache-pvc"}}]}}'
```

**Verify Service Endpoints:**
```bash
# Check NFS service has endpoints
kubectl get endpoints test-dp-nfs-service -n ns-yellow-starter

# Expected output shows IP:PORT combinations
# NAME                  ENDPOINTS                                         AGE
# test-dp-nfs-service   10.42.0.98:20048,10.42.0.98:111,10.42.0.98:2049   8m17s
```

## Troubleshooting: Longhorn RWX Performance Issues

### Problem: Longhorn Single-Node Configuration Issues

**Symptoms:**
- **Inconsistent mount times**: Some jobs 6-10 seconds, others 30-40 seconds
- **Volume status "degraded"** instead of "healthy"
- **Share-manager pod restarts** every few minutes
- **Multiple stopped replicas** in `kubectl get replicas -n longhorn-system`

**Root Cause:** Default replica count of 3 in single-node clusters

**Solution:**
```bash
# Fix replica count for single-node setup
kubectl patch setting default-replica-count -n longhorn-system --type='merge' -p='{"value":"1"}'

# Find your RWX volume ID
kubectl get pvc -n ns-yellow-starter | grep RWX
kubectl get volumes -n longhorn-system

# Fix existing RWX volume (replace with your volume ID)
kubectl patch volume pvc-<volume-id> -n longhorn-system --type='merge' -p='{"spec":{"numberOfReplicas":1}}'

# Verify fix - should show "healthy" not "degraded"
kubectl get volumes -n longhorn-system
```

### Problem: Longhorn Share-Manager Restart Issues

**Symptoms:**
- **Share-manager pod restarts frequently** (every 30-60 seconds)
- **Intermittent mount delays**: Jobs vary between 6-15 seconds vs 30-45 seconds
- **Logs show**: `"Stopping share manager since it is no longer required"` despite active volume usage
- **Events show**: Frequent `Killing` and `Started` events for share-manager pods

**Root Cause:** Share-manager controller incorrectly determining volume usage in certain configurations, leading to premature shutdown and restart cycles

**Solutions (in order of effectiveness):**

**Option 1: Configuration Adjustments**
```bash
# Enable storage network for RWX volumes (improves stability)
kubectl patch setting storage-network-for-rwx-volume-enabled -n longhorn-system --type='merge' -p='{"value":"true"}'

# Enable RWX fast failover
kubectl patch setting rwx-volume-fast-failover -n longhorn-system --type='merge' -p='{"value":"true"}'

# Increase aggressive timeouts
kubectl patch setting engine-replica-timeout -n longhorn-system --type='merge' -p='{"value":"30"}'
kubectl patch setting backup-execution-timeout -n longhorn-system --type='merge' -p='{"value":"60"}'
```

**Option 2: Update Longhorn (if issues persist)**
```bash
# Check current version
kubectl get setting current-longhorn-version -n longhorn-system -o jsonpath='{.value}'

# Consider upgrading to latest stable version if configuration workarounds insufficient
# Follow official Longhorn upgrade documentation
```

**Verification:**
```bash
# Monitor share-manager stability (should NOT restart frequently)
kubectl get events -n longhorn-system --sort-by='.lastTimestamp' | grep share-manager | tail -5

# Check for stable mount times (should be consistently 6-15 seconds)
# Test by triggering Airflow jobs and timing container creation
```

### Summary: Inherent NFS Mount Delays

**Key Finding**: After fixing Longhorn configuration, RWX volumes have **6-16 second mount delays**. Unfixed systems show 20-40 second delays due to configuration issues.

**Typical Symptoms After Fix**: Airflow jobs taking 10-20 seconds total (6-16s mount + 3s execution)

### Problem: Slow Container Startup Times Due to Volume Mount Delays

**Symptoms:**
- Pods stuck in `ContainerCreating` state for 30+ seconds
- Volume attach failures with error: `volume pvc-xxxxx is not ready for workloads`
- Events show repeated `FailedAttachVolume` followed by eventual `SuccessfulAttachVolume`
- High-frequency job creation (like Airflow tasks) experiences significant delays
- **Inconsistent timing**: Some jobs 6-10 seconds, others 30-40 seconds
- **Share-manager pod restarts**: Frequent restarts of share-manager pods

**Root Causes:**

**Primary Cause - Longhorn Single-Node Configuration:**
- **Default replica count of 3** in single-node clusters
- **Degraded volume states** due to inability to place multiple replicas
- **Share-manager instability** causing intermittent 30-40 second delays
- **Volume status shows "degraded"** instead of "healthy"

**Secondary Cause - Flannel CNI Issue:**
This is caused by a **Flannel CNI bug** affecting NFS mounts for Longhorn ReadWriteMany (RWX) volumes. The issue occurs due to TX checksum offloading problems in Flannel that cause slow or failing NFS handshakes between the kubelet and Longhorn's share-manager pods.

**Diagnosis Steps:**

**1. Check Longhorn Volume Health (Primary):**
```bash
# Check volume status - look for "degraded" instead of "healthy"
kubectl get volumes -n longhorn-system

# Check replica count settings
kubectl get setting default-replica-count -n longhorn-system -o yaml | grep value

# Check replica distribution
kubectl get replicas -n longhorn-system | grep -E 'stopped|running'

# Check share-manager stability
kubectl get pods -n longhorn-system | grep share-manager
```

**2. Identify RWX Volume Mount Issues:**
```bash
# Check for volume-related events
kubectl get events --sort-by=.metadata.creationTimestamp -n ns-yellow-starter | grep -i volume

# Look for share-manager pod restarts
kubectl get pods -n longhorn-system | grep share-manager

# Check for ReadWriteMany volumes
kubectl get pvc -n ns-yellow-starter -o yaml | grep -A 2 -B 2 ReadWriteMany
```

**3. Verify Flannel Interface:**
```bash
# Check if Flannel interface exists
ip addr show flannel.1
```

**4. Monitor Container Creation Times:**
```bash
# Monitor stuck pods
kubectl get pods -n ns-yellow-starter | grep ContainerCreating

# Check share-manager logs
kubectl logs share-manager-pvc-<volume-id> -n longhorn-system
```

### Primary Solution: Fix Longhorn Single-Node Configuration

**Root Cause:** Longhorn default replica count (3) in single-node clusters causes degraded volume states and share-manager restarts.

**Immediate Fix:**
```bash
# Fix Longhorn replica count for single-node setup
kubectl patch setting default-replica-count -n longhorn-system --type='merge' -p='{"value":"1"}'

# Fix existing RWX volume
kubectl patch volume pvc-<volume-id> -n longhorn-system --type='merge' -p='{"spec":{"numberOfReplicas":1}}'

# Verify fix
kubectl get volumes -n longhorn-system
# RWX volume should show "healthy" instead of "degraded"
```

### Alternative Solution: Apply Flannel TX Checksum Fix

**Note:** This fix has limited effectiveness. Address Longhorn configuration first.

**Immediate Fix:**
```bash
# Disable TX checksum offloading on Flannel interface
sudo ethtool -K flannel.1 tx-checksum-ip-generic off
```

**Make Fix Persistent (Required for Reboots):**
```bash
# Add to system startup
echo '# Flannel NFS fix for Longhorn RWX performance' | sudo tee -a /etc/rc.local
echo 'ethtool -K flannel.1 tx-checksum-ip-generic off' | sudo tee -a /etc/rc.local
chmod +x /etc/rc.local
```

**Alternative: Systemd Service Approach:**
```bash
# Create a systemd service for the fix
sudo tee /etc/systemd/system/flannel-nfs-fix.service << 'EOF'
[Unit]
Description=Fix Flannel NFS performance for Longhorn RWX
After=network.target
Wants=network.target

[Service]
Type=oneshot
ExecStart=/sbin/ethtool -K flannel.1 tx-checksum-ip-generic off
RemainAfterExit=true

[Install]
WantedBy=multi-user.target
EOF

# Enable the service
sudo systemctl enable flannel-nfs-fix.service
sudo systemctl start flannel-nfs-fix.service
```

### Verification

**Test Container Creation Speed:**
```bash
# Before fix: 30+ seconds
# After fix: Should be <10 seconds
start_time=$(date +%s)
kubectl run test-mount-speed --rm -i --restart=Never \
  --image=datasurface/datasurface:latest \
  --overrides='{"spec":{"containers":[{"name":"test","image":"datasurface/datasurface:latest","command":["sleep","5"],"volumeMounts":[{"name":"git-cache","mountPath":"/test"}]}],"volumes":[{"name":"git-cache","persistentVolumeClaim":{"claimName":"test-dp-git-model-cache-pvc"}}]}}' \
  -n ns-yellow-starter
end_time=$(date +%s)
echo "Pod creation time: $((end_time - start_time)) seconds"
```

**Monitor Improvement:**
```bash
# Check for reduced stuck pods
for i in {1..6}; do
  echo "=== Check $i/6 ==="
  kubectl get pods -n ns-yellow-starter | grep ContainerCreating | wc -l | xargs echo 'Stuck pods:'
  sleep 5
done
```

### Expected Results

**Performance Characteristics After Longhorn Fix:**
- **Longhorn RWX (NFS) Mount Time**: 6-16 seconds (improved from 30-40 seconds)
- **Job Execution Time**: 2-5 seconds (fast once mounted)
- **Total Job Time**: 10-20 seconds per Airflow task (improved from 25-45 seconds)
- **Consistency**: No more intermittent 40-second delays

**Success Indicators:**
- **Volume status shows "healthy"** instead of "degraded"
- **Share-manager pod stability** (no frequent restarts)
- **Consistent pod startup times** (6-16 seconds)
- Zero pods stuck in `ContainerCreating` state for extended periods
- No `FailedAttachVolume` events
- Fast Airflow job execution

### Additional Optimizations (Optional)

**Airflow Configuration Improvements:**
```python
# In KubernetesPodOperator tasks
startup_timeout_seconds=30  # Reduced from default 120
is_delete_operator_pod=False  # Reuse pods when possible
```

**Longhorn Storage Class Optimization:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: longhorn-fast-rwx
provisioner: driver.longhorn.io
parameters:
  numberOfReplicas: "1"  # Reduce for faster mounts if HA not critical
  fsType: ext4
mountOptions:
  - vers=4.1
  - norelatime
```

### Performance Acceptance vs. Alternatives

**If 25-45 second job times are acceptable:**
- ✅ **Keep current setup** - system is working as designed
- ✅ **Longhorn RWX provides** shared storage with data persistence
- ✅ **Jobs execute quickly** once mounted (2-5 seconds)

**If faster startup is required:**

1. **Switch to RWO Volumes (Recommended):**
   ```bash
   # Use ReadWriteOnce volumes - typically 5-10 second startup
   # Trade-off: No shared storage between pods
   # Solution: Use external storage (S3) for shared data
   ```

2. **External Shared Storage:**
   ```bash
   # Replace git cache with S3/MinIO/NFS server
   # Startup time: 5-10 seconds
   # Trade-off: Additional infrastructure complexity
   ```

3. **Hardware/Infrastructure Improvements:**
   - Upgrade to faster storage (NVMe SSD)
   - Increase node resources (CPU/Memory)  
   - Consider cloud-managed storage (EFS, Azure Files)

### References

- **Longhorn Issue**: Known performance issue with RWX volumes on Flannel
- **Flannel Bug**: TX checksum offloading causes NFS mount delays
- **K3s/Rancher**: Common in k3s deployments using default Flannel CNI

---

## Troubleshooting: Airflow Webserver Resource Allocation Issues

### Problem: Airflow Webserver Pod Stuck in "Pending" Status

**Symptoms:**
- Airflow webserver pod shows `STATUS: Pending`
- Port forwarding fails with error: `unable to forward port because pod is not running. Current status=Pending`
- Scheduler logs show successful DAG processing, but web interface is inaccessible

**Root Cause:**
This is typically caused by **resource allocation constraints** in your Kubernetes cluster. The webserver pod requires CPU and memory resources that may not be available on any node due to:

1. **Resource Allocation Lottery**: Pod placement varies between deployments
2. **Cluster Resource Exhaustion**: Other components consuming available resources
3. **Node Resource Imbalance**: Uneven distribution of workloads across nodes

### Diagnosis Steps

**Check Current Resource Allocation:**
```bash
# View node resource usage
kubectl describe nodes | grep -A 5 "Allocated resources"

# Check pod placement across nodes
kubectl get pods -n ns-yellow-starter -o wide

# Verify webserver resource requirements
kubectl get pod -l app=airflow-webserver -n ns-yellow-starter -o yaml | grep -A 10 "resources:"
```

**Expected Output Analysis:**
- Each node typically has 2000m CPU (2 cores)
- Webserver requests 200m-500m CPU and 256Mi-1Gi memory
- Look for nodes with sufficient available resources

### Solutions

#### **Solution 1: Add More Kubernetes Nodes (Recommended)**

**For Docker Desktop:**
1. Open Docker Desktop → Settings → Kubernetes
2. Increase number of nodes (e.g., from 4 to 6)
3. Apply & Restart
4. Redeploy the webserver deployment

**For Remote Kubernetes:**
```bash
# Add worker nodes to your cluster
# Example for k3s:
curl -sfL https://get.k3s.io | K3S_URL=https://server-ip:6443 K3S_TOKEN=your-token sh -
```

#### **Solution 2: Reduce Webserver Resource Requirements**

**Temporarily reduce resource requests:**
```bash
# Reduce CPU and memory requirements
kubectl patch deployment airflow-webserver -n ns-yellow-starter --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/requests/cpu", "value": "100m"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/requests/memory", "value": "256Mi"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/cpu", "value": "300m"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/memory", "value": "512Mi"}
]'

# Delete pending pods to trigger recreation
kubectl delete pod -l app=airflow-webserver -n ns-yellow-starter
```

#### **Solution 3: Force Pod Placement on Available Node**

**Target a specific node with available resources:**
```bash
# Check which node has available resources
kubectl describe nodes | grep -A 5 "Allocated resources"

# Force placement on control-plane node (usually has more available resources)
kubectl patch deployment airflow-webserver -n ns-yellow-starter --type='json' -p='[
  {"op": "add", "path": "/spec/template/spec/nodeSelector", "value": {"kubernetes.io/hostname": "desktop-control-plane"}}
]'

# Delete pending pods to trigger recreation
kubectl delete pod -l app=airflow-webserver -n ns-yellow-starter
```

#### **Solution 4: Stop Non-Essential Components**

**Temporarily free up resources:**
```bash
# Stop data simulator if running
kubectl delete pod data-simulator -n ns-yellow-starter

# Stop Kafka components if not needed for testing
kubectl scale deployment kafka -n ns-yellow-starter --replicas=0
kubectl scale deployment kafka-zookeeper -n ns-yellow-starter --replicas=0
kubectl scale deployment kafka-connect -n ns-yellow-starter --replicas=0
```

### Prevention

**Recommended Cluster Configuration:**
- **Minimum 6 nodes** for reliable YellowDataPlatform deployment
- **2 CPU cores per node** (2000m CPU)
- **4GB+ RAM per node**
- **Monitor resource usage** during deployment

**Resource Monitoring:**
```bash
# Regular resource checks
kubectl describe nodes | grep -A 5 "Allocated resources"

# Check for resource pressure
kubectl get events -n ns-yellow-starter --sort-by='.lastTimestamp'

# Monitor pod resource usage (if metrics server available)
kubectl top pods -n ns-yellow-starter
```

### Verification

**After applying fixes:**
```bash
# Verify webserver is running
kubectl get pods -n ns-yellow-starter -l app=airflow-webserver

# Test port forwarding
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n ns-yellow-starter &

# Access web interface
# URL: http://localhost:8080
# Credentials: admin/admin123
```

---

## References

- [MVP Dynamic DAG Factory](MVP_DynamicDAG.md)
- [MVP Kubernetes Infrastructure Setup](MVP_Kubernetes_Infrastructure_Setup.md)
- [July MVP Plan](July_MVP_Plan.md)

---

**Status: Production Ready** - Complete YellowDataPlatform environment deployment

**Infrastructure Components:**
- PostgreSQL database with all required schemas
- Airflow scheduler and webserver operational
- Factory DAGs for dynamic ingestion stream creation
- Model merge jobs for populating ingestion configurations
- Ring 1 initialization for database schema creation
- Admin access configured (admin/admin123)

**Deployment Process:**
- Clean artifact generation (includes all required YAML files)
- Consistent secret configuration ({secret_name}_USER/{secret_name}_PASSWORD format)
- Automated database schema creation via Ring 1 initialization
- Automated ingestion stream DAG creation via factory pattern
- Ready for immediate data pipeline deployment

## Accessing Airflow DAG Logs

This section explains how to access and analyze Airflow DAG logs for monitoring, debugging, and troubleshooting your YellowDataPlatform pipelines.

### Method 1: Airflow Web Interface (Recommended)

**Access the Web Interface:**
```bash
# Set up port forwarding to access Airflow UI
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n ns-yellow-starter
```

**Navigate to DAG Logs:**
1. Open http://localhost:8080 in your browser
2. Login with credentials: `admin` / `admin123`
3. Find your DAG (e.g., `yellowlive__Store1_ingestion`)
4. Click on the DAG name to view the graph
5. Click on any task box to view task details
6. Click "Logs" to view detailed execution logs

**Key DAG Examples:**
- `yellowlive__Store1_ingestion` - Live data ingestion pipeline
- `yellowforensic__Store1_ingestion` - Forensic data ingestion pipeline
- `test-dp_infrastructure` - Platform infrastructure management
- Factory DAGs for dynamic stream creation

### Method 2: Kubernetes Command Line Access

**Access Airflow Scheduler Logs:**
```bash
# View real-time scheduler logs
kubectl logs -f deployment/airflow-scheduler -n ns-yellow-starter

# Search for specific DAG activity
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter | grep "yellowlive__Store1_ingestion"

# View logs for the last hour
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter --since=1h
```

**Access Specific DAG Task Logs:**
```bash
# List available log directories
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- ls -la /opt/airflow/logs/

# View logs for specific DAG
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- ls -la /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/

# Access specific task run logs
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log
```

### Method 3: Remote SSH Access

**For Remote Kubernetes Deployments:**
```bash
# SSH into remote machine and access logs
ssh -i ~/.ssh/id_rsa_batch user@remote-host

# View Airflow scheduler logs on remote machine
sudo kubectl logs -f deployment/airflow-scheduler -n ns-yellow-starter

# Access specific DAG logs
sudo kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log
```

### Understanding Log Structure

**Airflow Log Directory Structure:**
```
/opt/airflow/logs/
├── dag_id=yellowlive__Store1_ingestion/
│   ├── run_id=scheduled__2025-08-11T21:58:00+00:00/
│   │   ├── task_id=snapshot_merge_job/
│   │   │   └── attempt=1.log
│   │   └── task_id=check_result/
│   │       └── attempt=1.log
│   └── run_id=manual__2025-08-11T21:03:02+00:00/
│       └── ...
└── dag_id=test-dp_infrastructure/
    └── ...
```

**Log Types:**
- **Scheduler Logs**: Overall DAG scheduling and execution coordination
- **Task Logs**: Individual task execution details and Python code output
- **Webserver Logs**: Airflow UI access and authentication logs

### Common Log Analysis Commands

**Monitor Active DAG Runs:**
```bash
# Watch for DAG execution in real-time
kubectl logs -f deployment/airflow-scheduler -n ns-yellow-starter | grep -E "(yellowlive|yellowforensic).*ingestion"

# Check for task failures
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter | grep -i "failed\|error" | tail -10

# Monitor specific task execution
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter | grep "snapshot_merge_job"
```

**Search Log History:**
```bash
# Find recent DAG runs
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- find /opt/airflow/logs -name "*.log" -mtime -1 | head -10

# Search for error patterns in task logs
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- grep -r "ERROR\|Exception" /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/ | tail -5

# Check specific date range logs
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- ls -la /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/ | grep "2025-08-11"
```

**Extract Structured Log Data:**
```bash
# View JSON structured logs from YellowDataPlatform
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log | grep '"timestamp"'

# Extract error messages with context
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log | grep -A 3 -B 3 '"level": "ERROR"'
```

### Troubleshooting Common Log Issues

**Issue: No Logs Available**
```bash
# Check if Airflow pods are running
kubectl get pods -n ns-yellow-starter | grep airflow

# Verify DAG has been executed
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- airflow dags list | grep yellowlive

# Check DAG parsing errors
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter | grep -i "parsing\|import"
```

**Issue: Permission Denied Accessing Logs**
```bash
# Ensure proper permissions on log directories
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- ls -la /opt/airflow/logs/

# Check pod exec permissions
kubectl auth can-i exec pods --namespace=ns-yellow-starter
```

**Issue: Logs Not Showing Python Output**
The YellowDataPlatform uses structured JSON logging. Python `print()` statements are not captured in Airflow logs. Use the proper logging framework instead:

```python
# ❌ This will NOT appear in Airflow logs:
print("Debug message")

# ✅ This WILL appear in Airflow logs:
from datasurface.platforms.yellow.logging_utils import get_contextual_logger
logger = get_contextual_logger(__name__)
logger.info("Debug message")
```

### Log Monitoring Best Practices

**Regular Health Checks:**
```bash
# Daily log health check
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- find /opt/airflow/logs -name "*.log" -mtime -1 | wc -l

# Check for recent failures
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter --since=24h | grep -i "failed\|error" | wc -l

# Monitor disk usage for logs
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- du -sh /opt/airflow/logs/
```

**Log Retention Management:**
```bash
# Clean old logs (older than 7 days)
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- find /opt/airflow/logs -name "*.log" -mtime +7 -delete

# Monitor log directory size
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- du -h /opt/airflow/logs/ | tail -1
```

**Performance Monitoring:**
```bash
# Check for slow-running tasks
kubectl exec deployment/airflow-scheduler -n ns-yellow-starter -- grep -r "run_duration" /opt/airflow/logs/ | sort -k4 -n | tail -5

# Monitor task success rates
kubectl logs deployment/airflow-scheduler -n ns-yellow-starter --since=24h | grep -c "Marking task as SUCCESS"
```

### Expected Log Patterns

**Successful DAG Execution:**
```
[2025-08-11T21:59:31.667+0000] {taskinstance.py:2480} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='datasurface' AIRFLOW_CTX_DAG_ID='yellowlive__Store1_ingestion'
[2025-08-11T21:59:49.030+0000] {pod_manager.py:447} INFO - [base] {"timestamp": "2025-08-11T21:59:48.786715+00:00Z", "level": "INFO", "logger": "datasurface.cmd.platform", "message": "Checking remote repository for updates..."}
[2025-08-11T21:59:52.094+0000] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=yellowlive__Store1_ingestion, task_id=snapshot_merge_job
```

**Failed DAG Execution:**
```
[2025-08-11T21:59:52.833+0000] {taskinstance.py:2698} ERROR - Task failed with exception
Exception: SnapshotMergeJob failed with code -1 - manual intervention required
[2025-08-11T21:59:52.841+0000] {taskinstance.py:1138} INFO - Marking task as FAILED. dag_id=yellowlive__Store1_ingestion, task_id=check_result
```

**Structured JSON Log Entries:**
```json
{
  "timestamp": "2025-08-11T21:59:49.034791+00:00Z",
  "level": "INFO",
  "logger": "__main__",
  "message": "Starting snapshot-merge operation",
  "workspace_name": "Store1",
  "platform_name": "YellowLive",
  "operation": "snapshot-merge"
}
```

## References

- [MVP Dynamic DAG Factory](MVP_DynamicDAG.md)
- [MVP Kubernetes Infrastructure Setup](MVP_Kubernetes_Infrastructure_Setup.md)
- [July MVP Plan](July_MVP_Plan.md)
- [HOWTO: Measure DataTransformer Lag](HOWTO_MeasureDataTransformerLag.md) 