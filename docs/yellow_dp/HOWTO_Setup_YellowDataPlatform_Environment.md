# HOWTO: Setup YellowDataPlatform Kubernetes Environment

## Tip on how to use this document.

I (Billy) am testing this on my Macbook Pro with cursor and docker desktop installed. I typically use claude 4 sonnet and I ask Claude in Cursor to setup these environments following this document.Docker desktop is running and has kubernetes enabled. Open cursor and select this file in your edit window in cursor. Make a new AI chat, it should automatically add this file in the chat. Select the auto model. Use the following prompt to stand up your YellowDataPlatform environment.

```text
I have a kubernetes cluster available on kub-test. You can login using ssh using 'ssh -i ~/.ssh/id_rsa_batch billy@kub-test'. billy can sudo if needed.

I want to stand up a  yellowdataplatform on my remote kubernetes machine. Please follow the instructions in @HOWTO_Setup_YellowDataPlatform_Environment.md exactly. The GitHub PAT to use is:
put_your_git_pat_here

The PostgreSQL password to use is:
put_your_postgres_password_here

When creating the postgres secrets, please use the correct case as indicated in the HOWTO exactly.

We first need to rebuild the project docker container using buildx for multiplatform and pull it down on kub-test before starting
put_your_git_pat_here
```

The first time you do this, it will download the different container images used (postgres/airflow/kafka/etc). This will take a couple of minutes. The AI usually sees this and automatically adds in waits for the containers to be ready.

The running environment takes just over 4GB of memory in docker. I use 2 assigned CPUs. I have assigned 2 CPUs, 24GB of memory and 180GB of disk space to docker desktop. My machine has 96GB of RAM and 2TB of disk. When configuring kubernetes in docker desktop, I use kind with 1.32.3 and 6 nodes.

I have also tested this on an M2 Macbook Air with 24GB RAM and 2TB SSD. The same docker desktop settings. It's slower and tight on memory (20GB used total) but it does work.

My dominant test environment is the 128GB Ubuntu 24 Kubernetes machine which is 10th gen Intel with 10 cores, 128GB of RAM and 8TB of SSDs. It runs proxmox and has a container called kub-test (8 vCores, 64GB of RAM, 200GB disk) running Ubuntu 24 with kubernetes installed, this is kub-test in my setup.

## Overview

This document provides a step-by-step guide to set up a complete YellowDataPlatform environment on Kubernetes. 

It is designed as an AI first document, easy to following repeatedly by an AI assistant to speed installs. Tested in Cursor with a chat session in auto mode.

The setup uses a two-ring approach:

- **Ring 0**: Generate bootstrap artifacts (runs in Docker container)
- **Ring 1**: Deploy to Kubernetes with full infrastructure (requires secrets and K8s cluster)

## Prerequisites

This setup requires **Docker** to be installed and running on your system. The environment variable detection and configuration parsing uses the `datasurface/datasurface:latest` Docker image to ensure consistent Python dependencies and module availability.

**Docker Commands Used:**
- The setup automatically detects database configuration from your `eco.py` file using Docker containers
- All Python-based configuration parsing runs inside `datasurface/datasurface:latest` containers
- Ensure Docker is running before starting the setup process
- **Note**: The Docker commands assume you're running from a directory containing both `eco.py` and the `datasurface` source code

## Database Configuration Support

This document supports both **internal** and **external** PostgreSQL database configurations:

### Internal PostgreSQL (YellowSingleDatabaseAssembly)
- **Description**: PostgreSQL runs as a pod within Kubernetes
- **Performance**: Lower performance (runs on Longhorn PVC storage)
- **Use Case**: Development, testing, proof-of-concept
- **Configuration**: Uses hardcoded `datasurface123` password
- **Detection**: Automatically detected when `eco.py` uses `YellowSingleDatabaseAssembly`

### External PostgreSQL (YellowExternalSingleDatabaseAssembly)  
- **Description**: PostgreSQL runs outside Kubernetes (dedicated server, cloud managed database)
- **Performance**: High performance (M.2 NVMe, cloud-optimized instances)
- **Use Case**: Production, performance testing, enterprise deployments
- **Configuration**: Database host/port extracted from `eco.py`, credentials from Kubernetes secrets
- **Detection**: Automatically detected when `eco.py` uses `YellowExternalSingleDatabaseAssembly`
- **Credentials**: Username/password stored in Kubernetes secret referenced by `postgresCredential`

The document automatically detects which configuration is being used and adjusts all commands accordingly.

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
kubectl delete namespace "$NAMESPACE"

# Remove local artifacts (if reusing same directory)
rm -rf yellow_starter/generated_output/
```

**Clone Fresh Repository**
```bash
git clone https://github.com/billynewport/yellow_starter.git
cd yellow_starter
```

### Step 2: Configure the Ecosystem Model and Detect Database Configuration

```bash
# Review the ecosystem model
cat eco.py

# Review the platform assignments file (should already exist with correct format)
cat dsg_platform_mapping.json
```

**Expected format for dsg_platform_mapping.json:**
```json
[
  {
    "dsgName": "LiveDSG",
    "workspace": "Consumer1",
    "assignments": [
      {
        "dataPlatform": "YellowLive",
        "documentation": "Live Yellow DataPlatform",
        "productionStatus": "PRODUCTION",
        "deprecationsAllowed": "NEVER",
        "status": "PROVISIONED"
      }
    ]
  },
  {
    "dsgName": "ForensicDSG", 
    "workspace": "Consumer1",
    "assignments": [
      {
        "dataPlatform": "YellowForensic",
        "documentation": "Forensic Yellow DataPlatform", 
        "productionStatus": "PRODUCTION",
        "deprecationsAllowed": "NEVER",
        "status": "PROVISIONED"
      }
    ]
  }
]
```

**Key configurations to verify:**
- DataPlatform names and credentials
- PostgreSQL hostname and port configuration
- Merge database name specification
- Datastore connection details
- Workspace and DatasetGroup configurations

**Note:** Database configuration detection and environment variable setup is now handled by the utility scripts in Step 3.

### Step 3: Create Environment Setup and Utility Scripts (Recommended)

To avoid shell escaping issues and improve reliability, create these utility scripts in your `yellow_starter` directory:

**Create Environment Setup Script (`setup_env.sh`):**

To avoid shell escaping issues with complex Python code execution, create the script locally and copy it to the remote machine:

```bash
# Create the environment setup script locally
cat > setup_env.sh << 'EOF'
#!/bin/bash

# YellowDataPlatform Environment Setup Script
echo "=== YellowDataPlatform Environment Setup ==="

# Set basic variables
export PG_PASSWORD="password"
export PG_USER="postgres"
export NAMESPACE="ns-yellow-starter"
export MERGE_DB_NAME="datasurface_merge"
export EXTERNAL_DB="true"

# Detect database configuration from eco.py
if grep -q "YellowExternalSingleDatabaseAssembly" eco.py; then
    echo "=== External PostgreSQL Database Configuration Detected ==="
    
    # Extract database host
    PG_HOST=$(docker run --rm -v "$(pwd):/workspace" -w /workspace datasurface/datasurface:latest python3 -c "
import sys
sys.path.append('.')
import eco
ecosystem = eco.createEcosystem()
psp = None
for p in ecosystem.platformServicesProviders:
    if hasattr(p, 'mergeStore'):
        psp = p
        break
if psp and hasattr(psp.mergeStore, 'hostPortPair'):
    print(psp.mergeStore.hostPortPair.hostName)
else:
    print('localhost')
")
    
    # Extract database port
    PG_PORT=$(docker run --rm -v "$(pwd):/workspace" -w /workspace datasurface/datasurface:latest python3 -c "
import sys
sys.path.append('.')
import eco
ecosystem = eco.createEcosystem()
psp = None
for p in ecosystem.platformServicesProviders:
    if hasattr(p, 'mergeStore'):
        psp = p
        break
if psp and hasattr(psp.mergeStore, 'hostPortPair'):
    print(psp.mergeStore.hostPortPair.port)
else:
    print('5432')
")
    
    export PG_HOST PG_PORT
    EXTERNAL_DB=true
    
else
    echo "=== Internal PostgreSQL Database Configuration Detected ==="
    PG_HOST="pg-data.$NAMESPACE.svc.cluster.local"
    PG_PORT="5432"
    export PG_HOST PG_PORT
    EXTERNAL_DB=false
fi

# Export all variables
export PG_HOST PG_PORT PG_USER PG_PASSWORD EXTERNAL_DB NAMESPACE MERGE_DB_NAME

echo "Environment Variables Set:"
echo "  PG_HOST=$PG_HOST"
echo "  PG_PORT=$PG_PORT"
echo "  PG_USER=$PG_USER"
echo "  NAMESPACE=$NAMESPACE"
echo "  MERGE_DB_NAME=$MERGE_DB_NAME"
echo "  EXTERNAL_DB=$EXTERNAL_DB"

# Save to env file for sourcing
cat > .env << ENVEOF
export PG_HOST="$PG_HOST"
export PG_PORT="$PG_PORT"
export PG_USER="$PG_USER"
export PG_PASSWORD="$PG_PASSWORD"
export NAMESPACE="$NAMESPACE"
export MERGE_DB_NAME="$MERGE_DB_NAME"
export EXTERNAL_DB="$EXTERNAL_DB"
ENVEOF

echo "Environment variables saved to .env file"
echo "To use in future sessions: source .env"
EOF

chmod +x setup_env.sh

# For remote deployments, copy the script and execute:
# scp setup_env.sh user@remote-host:~/yellow_starter/
# ssh user@remote-host "cd yellow_starter && chmod +x setup_env.sh && ./setup_env.sh"

# For local deployments, execute directly:
# ./setup_env.sh
```

**Create Utility Script (`utils.sh`):**
```bash
cat > utils.sh << 'EOF'
#!/bin/bash

# YellowDataPlatform Utility Script
source .env 2>/dev/null || { echo "Please run ./setup_env.sh first"; exit 1; }

case "$1" in
    "status")
        echo "=== Pod Status ==="
        sudo kubectl get pods -n "$NAMESPACE"
        echo ""
        echo "=== Recent Events ==="
        sudo kubectl get events -n "$NAMESPACE" --sort-by=.metadata.creationTimestamp | tail -5
        ;;
    "logs")
        echo "=== Airflow Scheduler Logs (last 50 lines) ==="
        sudo kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" --tail=50
        ;;
        
    "dags")
        echo "=== Airflow DAGs ==="
        sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- airflow dags list
        ;;
        
    "db-test")
        echo "=== Testing Database Connection ==="
        if [ "$EXTERNAL_DB" = "true" ]; then
            sudo kubectl run db-test-util --rm -i --restart=Never \
              --image=postgres:16 \
              --env="PGPASSWORD=$PG_PASSWORD" \
              -n "$NAMESPACE" \
              -- psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "SELECT version();"
        else
            sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'SELECT version();'"
        fi
        ;;
        
    "db-list")
        echo "=== Listing Databases ==="
        if [ "$EXTERNAL_DB" = "true" ]; then
            sudo kubectl run db-list-util --rm -i --restart=Never \
              --image=postgres:16 \
              --env="PGPASSWORD=$PG_PASSWORD" \
              -n "$NAMESPACE" \
              -- psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "SELECT datname FROM pg_database;"
        else
            sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'SELECT datname FROM pg_database;'"
        fi
        ;;
        
    "port-forward")
        echo "=== Setting up port forwarding for Airflow UI ==="
        echo "Access Airflow at http://localhost:8080"
        echo "Username: admin"
        echo "Password: admin123"
        sudo kubectl port-forward svc/airflow-webserver-service 8080:8080 -n "$NAMESPACE"
        ;;
        
    *)
        echo "YellowDataPlatform Utility Script"
        echo "Usage: $0 {status|logs|dags|db-test|db-list|port-forward}"
        echo ""
        echo "Commands:"
        echo "  status       - Show pod status and recent events"
        echo "  logs         - Show Airflow scheduler logs"
        echo "  dags         - List Airflow DAGs"
        echo "  db-test      - Test database connection"
        echo "  db-list      - List databases"
        echo "  port-forward - Set up port forwarding for Airflow UI"
        ;;
esac
EOF

chmod +x utils.sh
```

**Run the Environment Setup:**
```bash
# Run the setup script to detect configuration and set environment variables
./setup_env.sh
```

### Step 4: Generate Bootstrap Artifacts

It's important to use the correct docker image, especially when developing. Kubernetes and docker use DIFFERENT container caches. You need to docker pull the container image before running this command.

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

### Step 5: Verify Generated Artifacts

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

### Step 6: Validate Configuration

## Phase 2: Kubernetes Infrastructure Setup (Ring 1)

### Step 1: Create Kubernetes Namespace and Secrets

**Using Environment Variables from Setup Script:**
```bash
# Source environment variables (if not already loaded)
source .env

# Create namespace
sudo kubectl create namespace "$NAMESPACE"

# Create database credentials secret (consistent format for all components)
sudo kubectl create secret generic postgres \
  --from-literal=postgres_USER="$PG_USER" \
  --from-literal=postgres_PASSWORD="$PG_PASSWORD" \
  -n "$NAMESPACE"

# Create GitHub credentials secret  
sudo kubectl create secret generic git \
  --from-literal=token=your-github-personal-access-token \
  -n "$NAMESPACE"

```

**Default Credentials:**
- **PostgreSQL Database**: Uses detected configuration from eco.py (external) or `postgres/datasurface123` (internal)
- **Airflow Web UI**: `admin/admin123` (created after deployment)
- **GitHub Token**: Replace `your-github-token` with your actual GitHub Personal Access Token

### Step 2: Create Required Databases and Run Ring 1 Initialization

**IMPORTANT**: Create databases and run Ring 1 initialization BEFORE deploying Airflow infrastructure to avoid connection conflicts.

Ring 1 initialization creates the database schemas required for the platform operations.

```bash
# Source environment variables
source .env

# Drop and recreate databases (ensures clean state)
if [ "$EXTERNAL_DB" = "true" ]; then
    echo "=== Dropping and Creating Databases on External PostgreSQL ==="
    
    # First, terminate any existing connections and drop databases
    sudo kubectl run db-drop --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- bash -c "
        # Terminate connections to databases
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('airflow_db', 'customer_db', '$MERGE_DB_NAME') AND pid <> pg_backend_pid();\" || echo 'No connections to terminate'
        # Drop databases
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'DROP DATABASE IF EXISTS airflow_db;'
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'DROP DATABASE IF EXISTS customer_db;'  
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'DROP DATABASE IF EXISTS $MERGE_DB_NAME;'
      "
    
    # Create fresh databases
    sudo kubectl run db-create --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- bash -c "
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'CREATE DATABASE airflow_db;'
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'CREATE DATABASE customer_db;'  
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'CREATE DATABASE $MERGE_DB_NAME;'
      "
else
    echo "=== Dropping and Creating Databases on Internal PostgreSQL ==="
    
    # Drop and create databases on internal PostgreSQL
    sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "
        # Terminate connections
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('airflow_db', 'customer_db', '$MERGE_DB_NAME') AND pid <> pg_backend_pid();\" || echo 'No connections to terminate'
        # Drop databases
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'DROP DATABASE IF EXISTS airflow_db;'
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'DROP DATABASE IF EXISTS customer_db;'
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'DROP DATABASE IF EXISTS $MERGE_DB_NAME;'
        # Create fresh databases
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'CREATE DATABASE airflow_db;'
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'CREATE DATABASE customer_db;'
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'CREATE DATABASE $MERGE_DB_NAME;'
    "
fi

# Verify databases were created
./utils.sh db-list

# Create source tables and initial test data using the data simulator
# This creates the customers and addresses tables with initial data and simulates some changes and leaves it running continuously.
sudo kubectl run data-simulator --restart=Never \
  --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=$PG_USER" \
  --env="POSTGRES_PASSWORD=$PG_PASSWORD" \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --host "$PG_HOST" \
  --port "$PG_PORT" \
  --database customer_db \
  --user "$PG_USER" \
  --password "$PG_PASSWORD" \
  --create-tables \
  --max-changes 1000000 \
  --verbose &

# Wait a moment for the data simulator to start creating tables, then continue
echo "Data simulator started in background. Continuing with setup..."
echo "Note: The data simulator will run continuously for days, simulating ongoing data changes."
echo "You can monitor it with: sudo kubectl logs data-simulator -n \$NAMESPACE -f"
sleep 10

# Note: Remove --rm flag to allow background execution and monitoring

# Apply Ring 1 initialization job (creates platform database schemas)
sudo kubectl apply -f generated_output/Test_DP/test_dp_ring1_init_job.yaml

# Wait for Ring 1 initialization to complete
sudo kubectl wait --for=condition=complete job/test-dp-ring1-init -n "$NAMESPACE" --timeout=300s

# Check status
./utils.sh status
```

### Step 3: Deploy Kubernetes Infrastructure

**Now that databases are created and Ring 1 initialization is complete, deploy the Airflow infrastructure:**

```bash
# Apply the Kubernetes configuration
sudo kubectl apply -f generated_output/Test_DP/kubernetes-bootstrap.yaml

# Verify database connectivity using utility script
./utils.sh db-test

# Check deployment status
./utils.sh status
```

### Step 4: Verify Airflow Services

Airflow requires database initialization before the webserver can start properly.

```bash
# Source environment variables
source .env

# Wait for Airflow scheduler to be ready
sudo kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n "$NAMESPACE" --timeout=300s

# Initialize Airflow database (required for webserver to start)
sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- airflow db init

# Wait for Airflow webserver to be ready (after database initialization)
sudo kubectl wait --for=condition=ready pod -l app=airflow-webserver -n "$NAMESPACE" --timeout=300s

# Verify Airflow database connection
sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- airflow db check

# Check status using utility script
./utils.sh status
```

### Step 5: Create Airflow Admin User

```bash
# Create Airflow admin user
sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- \
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
SCHEDULER_POD=$(sudo kubectl get pods -n "$NAMESPACE" -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')

# Copy infrastructure DAG to Airflow
sudo kubectl cp generated_output/Test_DP/test_dp_infrastructure_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n "$NAMESPACE"

# Deploy model merge job to populate ingestion stream configurations
sudo kubectl apply -f generated_output/Test_DP/test_dp_model_merge_job.yaml

# Wait for model merge job to complete
sudo kubectl wait --for=condition=complete job/test-dp-model-merge-job -n "$NAMESPACE" --timeout=300s

# Deploy reconcile views job to create/update workspace views
sudo kubectl apply -f generated_output/Test_DP/test_dp_reconcile_views_job.yaml

# Wait for reconcile views job to complete
sudo kubectl wait --for=condition=complete job/test-dp-reconcile-views-job -n "$NAMESPACE" --timeout=300s

# Restart Airflow scheduler to trigger factory DAGs (creates dynamic ingestion stream DAGs)
sudo kubectl delete pod -n "$NAMESPACE" -l app=airflow-scheduler
sudo kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n "$NAMESPACE" --timeout=300s
```

### Step 7: Verify Deployment

```bash
# Check all pods are running
./utils.sh status

# Verify databases exist
./utils.sh db-list

# List all DAGs
./utils.sh dags

# Access Airflow web interface (runs in foreground)
./utils.sh port-forward
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

### Using Utility Scripts for Troubleshooting

The utility scripts created in Step 3 provide easy access to common troubleshooting commands:

**Quick Status Check:**
```bash
# Check all pod status and recent events
./utils.sh status
```

**View Logs:**
```bash
# View Airflow scheduler logs
./utils.sh logs

# View specific pod logs
sudo kubectl logs <pod-name> -n "$NAMESPACE"
```

**Database Troubleshooting:**
```bash
# Test database connectivity
./utils.sh db-test

# List all databases
./utils.sh db-list
```

**DAG Management:**
```bash
# List all DAGs and their status
./utils.sh dags
```

**Access Airflow UI:**
```bash
# Set up port forwarding (runs in foreground)
./utils.sh port-forward
# Then open http://localhost:8080 (admin/admin123)
```

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
sudo kubectl describe pod <pod-name> -n "$NAMESPACE"

# View logs
sudo kubectl logs <pod-name> -n "$NAMESPACE"

# Check all pods in namespace
sudo kubectl get pods -n "$NAMESPACE"

# Check services
sudo kubectl get svc -n "$NAMESPACE"
```

### Database Connection

**Using Utility Scripts (Recommended):**
```bash
# Test database connectivity
./utils.sh db-test

# List all databases
./utils.sh db-list
```

**Manual Commands (if needed):**
```bash
# Test database connectivity (non-interactive)
if [ "$EXTERNAL_DB" = "true" ]; then
    sudo kubectl run db-test-manual --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER"
else
    sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "pg_isready -U '$PG_USER' -h localhost"
fi
```
AI 
**💡 AI Tip: Database Querying Best Practices**
When querying the database through kubectl exec, use non-interactive SQL commands with proper password handling:

```bash
# ✅ CORRECT: Use PGPASSWORD environment variable with bash -c wrapper (Internal DB)
if [ "$EXTERNAL_DB" = "false" ]; then
    sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'SELECT datname FROM pg_database;'"
fi

# ✅ CORRECT: Use external database connection (External DB)
if [ "$EXTERNAL_DB" = "true" ]; then
    sudo kubectl run db-query --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "SELECT datname FROM pg_database;"
fi

# ✅ CORRECT: Check specific database tables (works for both internal and external)
if [ "$EXTERNAL_DB" = "true" ]; then
    sudo kubectl run db-tables --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d customer_db -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
else
    sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -d customer_db -c 'SELECT table_name FROM information_schema.tables WHERE table_schema = '\''public'\'';'"
fi

# ✅ CORRECT: Test database connectivity first
if [ "$EXTERNAL_DB" = "true" ]; then
    sudo kubectl run db-ready --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER"
else
    sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "pg_isready -U '$PG_USER' -h localhost"
fi

# ❌ AVOID: Interactive mode with -it flag (hangs in non-interactive environments)
sudo kubectl exec -it deployment/pg-postgres -n "$NAMESPACE" -- psql -U postgres -l

# ❌ AVOID: Without PGPASSWORD (hangs waiting for password input)
sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "psql -U postgres -c 'SELECT datname FROM pg_database;'"

# ❌ AVOID: psql meta-commands like \l, \dv (require interactive mode)
sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "psql -U postgres -l"
```

**Key improvements:**
- Use `PGPASSWORD=datasurface123` environment variable to avoid password prompts
- Specify `-h localhost` to be explicit about the host connection
- Use single quotes around SQL queries to avoid escaping issues
- Test connectivity with `pg_isready` before running queries
- Use standard SQL (`SELECT`) instead of psql meta-commands (`\l`, `\dv`, etc.)
- Remove `-it` flag completely for non-interactive environments

### Common Issues and Solutions

**Issue: Database connection conflicts during deployment**
```bash
# Symptoms: 
# - "database is being accessed by other users" when trying to drop databases
# - Airflow pods fail to start after database operations
# - Data simulator fails with "database does not exist" errors

# Cause: Airflow infrastructure deployed before databases are properly initialized
# Solution: Follow correct deployment order:
# 1. Create namespace and secrets
# 2. DROP existing databases and CREATE fresh ones (Step 2 now includes this)
# 3. Run Ring 1 initialization 
# 4. THEN deploy Airflow infrastructure
# 5. Initialize Airflow and create admin user
# 6. Deploy DAGs and jobs

# If you encounter this issue, stop Airflow deployments first:
sudo kubectl delete deployment airflow-scheduler airflow-webserver -n "$NAMESPACE"
# Wait for pods to terminate, then follow the updated Step 2 which drops and recreates databases
```

**Issue: Ingestion stream DAGs not appearing in Airflow UI**
```bash
# Cause: Factory DAGs haven't run yet or model merge job failed
# Solution: Verify model merge job completed and restart scheduler
sudo kubectl get jobs -n "$NAMESPACE"
sudo kubectl logs job/test-dp-model-merge-job -n "$NAMESPACE"
sudo kubectl delete pod -n "$NAMESPACE" -l app=airflow-scheduler
```

**Issue: Data simulator fails to create tables**
```bash
# Symptoms:
# - Data simulator shows "relation 'customers' does not exist" errors
# - Tables not created in customer_db database
# - Pod exits with "Too many consecutive failures"

# Cause: Data simulator may exit before tables are fully created
# Solution: Create a cleanup and restart script to avoid shell escaping issues

# Create cleanup script locally
cat > cleanup_and_restart_simulator.sh << 'EOF'
#!/bin/bash

# Load environment
cd yellow_starter
source .env

# Clean up existing data simulator pod
echo "Cleaning up existing data simulator pod..."
sudo kubectl delete pod data-simulator -n "$NAMESPACE" --ignore-not-found=true

# Wait a moment for cleanup
sleep 5

# Start new data simulator with limited changes first to ensure table creation
echo "Starting data simulator with limited changes to create tables..."
sudo kubectl run data-simulator --restart=Never --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=$PG_USER" \
  --env="POSTGRES_PASSWORD=$PG_PASSWORD" \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --host "$PG_HOST" \
  --port "$PG_PORT" \
  --database customer_db \
  --user "$PG_USER" \
  --password "$PG_PASSWORD" \
  --create-tables \
  --max-changes 100 \
  --verbose

echo "Data simulator started with table creation!"
EOF

chmod +x cleanup_and_restart_simulator.sh

# For remote deployments, copy the script and execute:
# scp cleanup_and_restart_simulator.sh user@remote-host:~/
# ssh user@remote-host "chmod +x cleanup_and_restart_simulator.sh && ./cleanup_and_restart_simulator.sh"

# For local deployments, execute directly:
./cleanup_and_restart_simulator.sh

# Verify tables were created:
sudo kubectl run db-tables --rm -i --restart=Never \
  --image=postgres:16 --env="PGPASSWORD=$PG_PASSWORD" -n "$NAMESPACE" \
  -- psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d customer_db \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
```

**Issue: Ring 1 initialization job fails**
```bash
# Cause: Secret keys mismatch or database not accessible
# Solution: Verify secret format and database connectivity
sudo kubectl describe job/test-dp-ring1-init -n "$NAMESPACE"
sudo kubectl logs job/test-dp-ring1-init -n "$NAMESPACE"
```

**Issue: Secret configuration errors**
```bash
# Cause: Inconsistent secret key names
# Verify all secrets use postgres_USER/postgres_PASSWORD format:
sudo kubectl get secret postgres -n "$NAMESPACE" -o yaml
```

**Issue: PostgreSQL commands hang or fail in non-interactive environments**
```bash
# Cause: Using interactive psql commands or missing password handling
# Symptoms: Commands hang indefinitely or fail with authentication errors
# Solution: Use PGPASSWORD environment variable and avoid interactive flags

# ✅ Test database connectivity first:
sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "pg_isready -U postgres -h localhost"

# ✅ List databases (non-interactive):
sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -c 'SELECT datname FROM pg_database;'"

# ✅ Check specific database tables:
sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "PGPASSWORD=datasurface123 psql -U postgres -h localhost -d customer_db -c 'SELECT table_name FROM information_schema.tables WHERE table_schema = '\''public'\'';'"

# ❌ AVOID these approaches (they will hang):
# kubectl exec -it deployment/pg-postgres -n "$NAMESPACE" -- psql -U postgres -l
# kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "psql -U postgres -c 'SELECT datname FROM pg_database;'"
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
sudo kubectl run data-simulator-continuous --rm -i --restart=Never \
  --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=$PG_USER" \
  --env="POSTGRES_PASSWORD=$PG_PASSWORD" \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --host "$PG_HOST" \
  --port "$PG_PORT" \
  --database customer_db \
  --user "$PG_USER" \
  --password "$PG_PASSWORD" \
  --min-interval 1 \
  --max-interval 1 \
  --verbose &

# The simulator will run continuously in the background
# You can continue with other tasks while it runs

# To stop the simulator from another terminal:
# sudo kubectl delete pod data-simulator-continuous -n "$NAMESPACE"

# To check if the simulator is still running:
# sudo kubectl get pods -n "$NAMESPACE" | grep data-simulator

# To view simulator logs:
# sudo kubectl logs data-simulator-continuous -n "$NAMESPACE" -f
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

## Critical Deployment Order

**⚠️ IMPORTANT**: Follow this exact order to avoid database connection conflicts:

1. **Phase 1: Bootstrap Artifact Generation (Ring 0)**
   - Clone repository and configure environment
   - Generate bootstrap artifacts using Docker

2. **Phase 2: Kubernetes Infrastructure Setup (Ring 1)**
   - Create namespace and secrets
   - **DROP and CREATE fresh databases FIRST** (before deploying Airflow)
   - Run Ring 1 initialization job
   - **THEN deploy Airflow infrastructure**
   - Initialize Airflow database and create admin user
   - Deploy DAGs and run model merge/reconcile jobs

**❌ Common Mistake**: Deploying Airflow infrastructure before databases are ready causes connection conflicts and deployment failures.

## Success Criteria

**✅ Infrastructure Deployed:**
- PostgreSQL running with airflow_db, customer_db, and merge databases (names from eco.py)
- Airflow scheduler and webserver operational
- Admin user created (admin/admin123)
- All required Kubernetes secrets configured

**✅ DAGs Deployed:**
- Factory DAGs for dynamic ingestion stream creation
- Infrastructure DAGs for platform management
- All DAGs visible in Airflow UI without parsing errors

**✅ Ready for Data Pipeline:**
- Source database (customer_db) ready for ingestion
- Merge database (name from eco.py) ready for platform operations
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

# IMPORTANT: Set up the same environment variable detection as in Phase 1, Step 2
# This detects whether external or internal PostgreSQL is being used
# (Copy the entire environment variable detection section from Phase 1, Step 2)

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
sudo kubectl create namespace "$NAMESPACE"

sudo kubectl create secret generic postgres \
  --from-literal=postgres_USER="$PG_USER" \
  --from-literal=postgres_PASSWORD="$PG_PASSWORD" \
  -n "$NAMESPACE"

sudo kubectl create secret generic git \
  --from-literal=token=your-github-personal-access-token \
  -n "$NAMESPACE"

# Deploy infrastructure
sudo kubectl apply -f generated_output/Test_DP/kubernetes-bootstrap.yaml
```

**Initialize Databases and Deploy DAGs:**
```bash
# Wait for PostgreSQL to be ready
sudo kubectl wait --for=condition=ready pod -l app=pg-postgres -n "$NAMESPACE" --timeout=300s

# Create required databases (use same environment variable detection as above)
if [ "$EXTERNAL_DB" = "true" ]; then
    sudo kubectl run db-setup-remote --rm -i --restart=Never \
      --image=postgres:16 \
      --env="PGPASSWORD=$PG_PASSWORD" \
      -n "$NAMESPACE" \
      -- bash -c "
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'CREATE DATABASE IF NOT EXISTS airflow_db;' || echo 'airflow_db may already exist'
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'CREATE DATABASE IF NOT EXISTS customer_db;' || echo 'customer_db may already exist'
        psql -h '$PG_HOST' -p '$PG_PORT' -U '$PG_USER' -c 'CREATE DATABASE IF NOT EXISTS $MERGE_DB_NAME;' || echo '$MERGE_DB_NAME may already exist'
      "
else
    sudo kubectl exec deployment/pg-postgres -n "$NAMESPACE" -- bash -c "
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'CREATE DATABASE IF NOT EXISTS airflow_db;' || echo 'airflow_db may already exist'
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'CREATE DATABASE IF NOT EXISTS customer_db;' || echo 'customer_db may already exist'
        PGPASSWORD='$PG_PASSWORD' psql -U '$PG_USER' -h localhost -c 'CREATE DATABASE IF NOT EXISTS $MERGE_DB_NAME;' || echo '$MERGE_DB_NAME may already exist'
    "
fi

# Run Ring 1 initialization
sudo kubectl apply -f generated_output/Test_DP/test_dp_ring1_init_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-ring1-init -n "$NAMESPACE" --timeout=300s

# Initialize Airflow
sudo kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n "$NAMESPACE" --timeout=300s
sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- airflow db init
sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin123

# Deploy DAGs and jobs
SCHEDULER_POD=$(sudo kubectl get pods -n "$NAMESPACE" -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')
sudo kubectl cp generated_output/Test_DP/test_dp_infrastructure_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n "$NAMESPACE"

# Deploy model merge and reconcile jobs
sudo kubectl apply -f generated_output/Test_DP/test_dp_model_merge_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-model-merge-job -n "$NAMESPACE" --timeout=300s

sudo kubectl apply -f generated_output/Test_DP/test_dp_reconcile_views_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-reconcile-views-job -n "$NAMESPACE" --timeout=300s

# Restart scheduler to trigger factory DAGs
sudo kubectl delete pod -n "$NAMESPACE" -l app=airflow-scheduler
sudo kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n "$NAMESPACE" --timeout=300s
```

### Step 4: Access Remote YellowDataPlatform

**Set up port forwarding:**
```bash
# On remote machine (background execution)
sudo kubectl port-forward svc/airflow-webserver-service 8080:8080 -n "$NAMESPACE" &

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
kubectl --kubeconfig ~/.kube/config-k3s-remote --insecure-skip-tls-verify get pods -n "$NAMESPACE"

# Or via SSH
ssh user@remote-machine-ip "sudo kubectl get pods -n "$NAMESPACE""
```

**View logs:**
```bash
# Via SSH
ssh user@remote-machine-ip "sudo kubectl logs <pod-name> -n "$NAMESPACE""

# Or from local machine
kubectl --kubeconfig ~/.kube/config-k3s-remote --insecure-skip-tls-verify logs <pod-name> -n "$NAMESPACE"
```

**Run data simulator:**
```bash
# On remote machine (use same environment variables as above)
sudo kubectl run data-simulator --rm -i --restart=Never \
  --image=datasurface/datasurface:latest \
  --env="POSTGRES_USER=$PG_USER" \
  --env="POSTGRES_PASSWORD=$PG_PASSWORD" \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --host "$PG_HOST" \
  --port "$PG_PORT" \
  --database customer_db \
  --user "$PG_USER" \
  --password "$PG_PASSWORD" \
  --create-tables \
  --max-changes 1000000 \
  --verbose
```

### Step 6: Set Up NFS for Git Caching (Optional for YellowDataPlatform when using NFS, default for yellow_starter is now longhorn)

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
docker pull datasurface/datasurface:latest
```

We use kubernetes AND docker on the remote machine. Kubernetes containers and docker use different caches. We need to pull the image on BOTH

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
ssh user@remote-machine-ip "sudo kubectl get pods -n "$NAMESPACE" && sudo kubectl describe pod <pod-name> -n "$NAMESPACE""
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
kubectl patch deployment test-dp-nfs-server -n "$NAMESPACE" --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/nodeSelector/kubernetes.io~1hostname", "value": "actual-node-name"}
]'
```

#### **Issue 2: PV Node Affinity Mismatch**
**Cause:** Persistent Volume created with wrong node affinity

**Solution:**
```bash
# Delete and recreate PV/PVC with correct node affinity
kubectl delete pvc test-dp-nfs-server-pvc -n "$NAMESPACE"
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
  namespace: "$NAMESPACE"
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
kubectl delete pod -l app=test-dp-nfs-server -n "$NAMESPACE"
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
kubectl delete job test-dp-ring1-init -n "$NAMESPACE"
# Reapply the job
kubectl apply -f generated_output/Test_DP/test_dp_ring1_init_job.yaml
```

#### **Issue 5: DNS Resolution for NFS Service Names**
**Cause:** Kubelet uses host DNS (not cluster DNS) for NFS mounts, but host cannot resolve Kubernetes service names

**Symptoms:**
```
MountVolume.SetUp failed for volume "test-dp-git-model-cache-pv" : mount failed: exit status 32
Output: mount.nfs: Failed to resolve server test-dp-nfs-service.$NAMESPACE.svc.cluster.local: Name or service not known
```

**Verification:**
```bash
# Test if host can resolve Kubernetes service names
getent hosts test-dp-nfs-service.$NAMESPACE.svc.cluster.local

# If this fails but cluster DNS works:
kubectl run dns-test --image=busybox --rm -it --restart=Never -n "$NAMESPACE" -- nslookup test-dp-nfs-service.$NAMESPACE.svc.cluster.local
```

**Solution:**
```bash
# Add cluster DNS to host /etc/resolv.conf
echo 'nameserver 10.43.0.10' | sudo tee -a /etc/resolv.conf

# Verify host can now resolve service names
getent hosts test-dp-nfs-service.$NAMESPACE.svc.cluster.local

# Expected output:
# 10.43.161.139   test-dp-nfs-service.$NAMESPACE.svc.cluster.local
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
sudo kubectl logs -l app=test-dp-nfs-server -n "$NAMESPACE" --tail=10

# Expected success output:
# ==================================================================
#       READY AND WAITING FOR NFS CLIENT CONNECTIONS
# ==================================================================
```

**Test NFS Mount:**
```bash
# Test NFS mount with simple pod
sudo kubectl run nfs-test --rm -it --restart=Never --image=busybox -n "$NAMESPACE" \
  --overrides='{"spec":{"containers":[{"name":"nfs-test","image":"busybox","command":["ls","-la","/cache"],"volumeMounts":[{"name":"nfs-cache","mountPath":"/cache"}]}],"volumes":[{"name":"nfs-cache","persistentVolumeClaim":{"claimName":"test-dp-git-model-cache-pvc"}}]}}'
```

**Verify Service Endpoints:**
```bash
# Check NFS service has endpoints
sudo kubectl get endpoints test-dp-nfs-service -n "$NAMESPACE"

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
- **Multiple stopped replicas** in `sudo kubectl get replicas -n longhorn-system`

**Root Cause:** Default replica count of 3 in single-node clusters

**Solution:**
```bash
# Fix replica count for single-node setup
sudo kubectl patch setting default-replica-count -n longhorn-system --type='merge' -p='{"value":"1"}'

# Find your RWX volume ID
sudo kubectl get pvc -n "$NAMESPACE" | grep RWX
sudo kubectl get volumes -n longhorn-system

# Fix existing RWX volume (replace with your volume ID)
sudo kubectl patch volume pvc-<volume-id> -n longhorn-system --type='merge' -p='{"spec":{"numberOfReplicas":1}}'

# Verify fix - should show "healthy" not "degraded"
sudo kubectl get volumes -n longhorn-system
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
sudo kubectl patch setting storage-network-for-rwx-volume-enabled -n longhorn-system --type='merge' -p='{"value":"true"}'

# Enable RWX fast failover
sudo kubectl patch setting rwx-volume-fast-failover -n longhorn-system --type='merge' -p='{"value":"true"}'

# Increase aggressive timeouts
sudo kubectl patch setting engine-replica-timeout -n longhorn-system --type='merge' -p='{"value":"30"}'
sudo kubectl patch setting backup-execution-timeout -n longhorn-system --type='merge' -p='{"value":"60"}'
```

**Option 2: Update Longhorn (if issues persist)**
```bash
# Check current version
sudo kubectl get setting current-longhorn-version -n longhorn-system -o jsonpath='{.value}'

# Consider upgrading to latest stable version if configuration workarounds insufficient
# Follow official Longhorn upgrade documentation
```

**Verification:**
```bash
# Monitor share-manager stability (should NOT restart frequently)
sudo kubectl get events -n longhorn-system --sort-by='.lastTimestamp' | grep share-manager | tail -5

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
kubectl get events --sort-by=.metadata.creationTimestamp -n "$NAMESPACE" | grep -i volume

# Look for share-manager pod restarts
kubectl get pods -n longhorn-system | grep share-manager

# Check for ReadWriteMany volumes
kubectl get pvc -n "$NAMESPACE" -o yaml | grep -A 2 -B 2 ReadWriteMany
```

**3. Verify Flannel Interface:**
```bash
# Check if Flannel interface exists
ip addr show flannel.1
```

**4. Monitor Container Creation Times:**
```bash
# Monitor stuck pods
kubectl get pods -n "$NAMESPACE" | grep ContainerCreating

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
  -n "$NAMESPACE"
end_time=$(date +%s)
echo "Pod creation time: $((end_time - start_time)) seconds"
```

**Monitor Improvement:**
```bash
# Check for reduced stuck pods
for i in {1..6}; do
  echo "=== Check $i/6 ==="
  kubectl get pods -n "$NAMESPACE" | grep ContainerCreating | wc -l | xargs echo 'Stuck pods:'
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
kubectl get pods -n "$NAMESPACE" -o wide

# Verify webserver resource requirements
kubectl get pod -l app=airflow-webserver -n "$NAMESPACE" -o yaml | grep -A 10 "resources:"
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
kubectl patch deployment airflow-webserver -n "$NAMESPACE" --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/requests/cpu", "value": "100m"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/requests/memory", "value": "256Mi"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/cpu", "value": "300m"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/memory", "value": "512Mi"}
]'

# Delete pending pods to trigger recreation
kubectl delete pod -l app=airflow-webserver -n "$NAMESPACE"
```

#### **Solution 3: Force Pod Placement on Available Node**

**Target a specific node with available resources:**
```bash
# Check which node has available resources
kubectl describe nodes | grep -A 5 "Allocated resources"

# Force placement on control-plane node (usually has more available resources)
kubectl patch deployment airflow-webserver -n "$NAMESPACE" --type='json' -p='[
  {"op": "add", "path": "/spec/template/spec/nodeSelector", "value": {"kubernetes.io/hostname": "desktop-control-plane"}}
]'

# Delete pending pods to trigger recreation
kubectl delete pod -l app=airflow-webserver -n "$NAMESPACE"
```

#### **Solution 4: Stop Non-Essential Components**

**Temporarily free up resources:**
```bash
# Stop data simulator if running
kubectl delete pod data-simulator -n "$NAMESPACE"

# Stop Kafka components if not needed for testing
kubectl scale deployment kafka -n "$NAMESPACE" --replicas=0
kubectl scale deployment kafka-zookeeper -n "$NAMESPACE" --replicas=0
kubectl scale deployment kafka-connect -n "$NAMESPACE" --replicas=0
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
kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp'

# Monitor pod resource usage (if metrics server available)
kubectl top pods -n "$NAMESPACE"
```

### Verification

**After applying fixes:**
```bash
# Verify webserver is running
kubectl get pods -n "$NAMESPACE" -l app=airflow-webserver

# Test port forwarding
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n "$NAMESPACE" &

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
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n "$NAMESPACE"
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
kubectl logs -f deployment/airflow-scheduler -n "$NAMESPACE"

# Search for specific DAG activity
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" | grep "yellowlive__Store1_ingestion"

# View logs for the last hour
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" --since=1h
```

**Access Specific DAG Task Logs:**
```bash
# List available log directories
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- ls -la /opt/airflow/logs/

# View logs for specific DAG
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- ls -la /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/

# Access specific task run logs
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log
```

### Method 3: Remote SSH Access

**For Remote Kubernetes Deployments:**
```bash
# SSH into remote machine and access logs
ssh -i ~/.ssh/id_rsa_batch user@remote-host

# View Airflow scheduler logs on remote machine
sudo kubectl logs -f deployment/airflow-scheduler -n "$NAMESPACE"

# Access specific DAG logs
sudo kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log
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
kubectl logs -f deployment/airflow-scheduler -n "$NAMESPACE" | grep -E "(yellowlive|yellowforensic).*ingestion"

# Check for task failures
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" | grep -i "failed\|error" | tail -10

# Monitor specific task execution
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" | grep "snapshot_merge_job"
```

**Search Log History:**
```bash
# Find recent DAG runs
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- find /opt/airflow/logs -name "*.log" -mtime -1 | head -10

# Search for error patterns in task logs
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- grep -r "ERROR\|Exception" /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/ | tail -5

# Check specific date range logs
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- ls -la /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/ | grep "2025-08-11"
```

**Extract Structured Log Data:**
```bash
# View JSON structured logs from YellowDataPlatform
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log | grep '"timestamp"'

# Extract error messages with context
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- cat /opt/airflow/logs/dag_id=yellowlive__Store1_ingestion/run_id=scheduled__2025-08-11T21:58:00+00:00/task_id=snapshot_merge_job/attempt=1.log | grep -A 3 -B 3 '"level": "ERROR"'
```

### Troubleshooting Common Log Issues

**Issue: No Logs Available**
```bash
# Check if Airflow pods are running
kubectl get pods -n "$NAMESPACE" | grep airflow

# Verify DAG has been executed
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- airflow dags list | grep yellowlive

# Check DAG parsing errors
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" | grep -i "parsing\|import"
```

**Issue: Permission Denied Accessing Logs**
```bash
# Ensure proper permissions on log directories
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- ls -la /opt/airflow/logs/

# Check pod exec permissions
kubectl auth can-i exec pods --namespace="$NAMESPACE"
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
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- find /opt/airflow/logs -name "*.log" -mtime -1 | wc -l

# Check for recent failures
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" --since=24h | grep -i "failed\|error" | wc -l

# Monitor disk usage for logs
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- du -sh /opt/airflow/logs/
```

**Log Retention Management:**
```bash
# Clean old logs (older than 7 days)
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- find /opt/airflow/logs -name "*.log" -mtime +7 -delete

# Monitor log directory size
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- du -h /opt/airflow/logs/ | tail -1
```

**Performance Monitoring:**
```bash
# Check for slow-running tasks
kubectl exec deployment/airflow-scheduler -n "$NAMESPACE" -- grep -r "run_duration" /opt/airflow/logs/ | sort -k4 -n | tail -5

# Monitor task success rates
kubectl logs deployment/airflow-scheduler -n "$NAMESPACE" --since=24h | grep -c "Marking task as SUCCESS"
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

## DNS: Kubernetes + Tailscale resolution fix

This section documents a reliable DNS configuration for clusters that use Tailscale (MagicDNS) alongside standard internet DNS. It addresses intermittent `SERVFAIL` for public domains and inconsistent `ts.net` resolution inside pods.

### Symptoms

- Intermittent `SERVFAIL` for `github.com` inside pods, while host lookups succeed.
- `ts.net` names (e.g., `postgres.leopard-mizar.ts.net`) not consistently resolving in pods.
- Tailscale status warning on host about reaching configured DNS servers, despite host lookups working.

### Root cause

- CoreDNS forwarded all queries to `192.168.4.1 100.100.100.100`. Using Tailscale resolver (`100.100.100.100`) for general/public domains is unreliable. CoreDNS lacked an explicit stub for `ts.net`.

### Fix (CoreDNS Corefile)

Replace the default forwarders with an explicit `ts.net` stub and set general domains to use a normal resolver plus a public fallback:

```text
forward ts.net 100.100.100.100
forward . 192.168.4.1 1.1.1.1
```

### Apply on K3s

```bash
# Note: On K3s servers you may need sudo; replace kubectl with: sudo k3s kubectl
kubectl -n kube-system get configmap coredns -o yaml > /tmp/coredns.yaml
sed -i 's/forward \. 192\.168\.4\.1 100\.100\.100\.100/forward ts.net 100.100.100.100\n        forward . 192.168.4.1 1.1.1.1/' /tmp/coredns.yaml
kubectl -n kube-system apply -f /tmp/coredns.yaml
kubectl -n kube-system rollout restart deployment coredns
kubectl -n kube-system rollout status deployment coredns --timeout=90s
```

### Verify from a pod

```bash
kubectl run dns-test --image=busybox:1.36 --restart=Never --command -- sleep 300
kubectl wait --for=condition=Ready pod/dns-test --timeout=90s
kubectl exec -i dns-test -- nslookup github.com
kubectl exec -i dns-test -- nslookup postgres.leopard-mizar.ts.net
kubectl exec -i dns-test -- nslookup kubernetes.default.svc.cluster.local
kubectl delete pod dns-test --ignore-not-found
```

### Optional host checks

```bash
getent hosts github.com
getent hosts postgres.leopard-mizar.ts.net
tailscale netcheck
```

Results after applying the fix should show successful resolution of public domains, `ts.net` names via MagicDNS, and Kubernetes service names inside pods.
