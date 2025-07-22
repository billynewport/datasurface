# HOWTO: Setup YellowDataPlatform Kubernetes Environment

## Overview

This document provides a step-by-step guide to set up a complete YellowDataPlatform environment on Kubernetes. 

It is designed as an AI first document, easy to following repeatedly by an AI assistant to speed installs. Tested in Cursor with Claude 4 Sonnet in Max mode.

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
  --platform YellowLive YellowForensic
```

### Step 4: Verify Generated Artifacts

```bash
# Check the generated files
ls -la generated_output/YellowLive/
ls -la generated_output/YellowForensic/
```

**Expected artifacts for each platform:**
- `kubernetes-bootstrap.yaml` - Kubernetes deployment configuration
- `{platform}_infrastructure_dag.py` - Platform management DAG
- `{platform}_factory_dag.py` - Dynamic DAG factory
- `{platform}_datatransformer_factory_dag.py` - Dynamic DataTransformer DAG factory
- `{platform}_model_merge_job.yaml` - Model merge job for populating ingestion stream configurations
- `{platform}_ring1_init_job.yaml` - Ring 1 initialization job for creating database schemas

### Step 5: Validate Configuration

```bash
# Verify platforms have correct differences
diff generated_output/YellowLive/kubernetes-bootstrap.yaml generated_output/YellowForensic/kubernetes-bootstrap.yaml
```

**Expected differences:**
- Platform-specific resource names (`yellowlive-*` vs `yellowforensic-*`)
- Platform environment variables

**Should be identical:**
- Database hostnames and ports
- Connection strings
- Template configurations

---

## Phase 2: Kubernetes Infrastructure Setup (Ring 1)

### Step 1: Create Kubernetes Namespace and Secrets

```bash
# Create namespace
kubectl create namespace ns-yellow-starter

# Create database credentials secret (consistent format for all components)
kubectl create secret generic postgres \
  --from-literal=POSTGRES_USER=postgres \
  --from-literal=POSTGRES_PASSWORD=datasurface123 \
  -n ns-yellow-starter

# Create GitHub credentials secret  
kubectl create secret generic git \
  --from-literal=token=your-github-personal-access-token \
  -n ns-yellow-starter

# Create Slack credentials secret (optional)
kubectl create secret generic slack \
  --from-literal=SLACK_WEBHOOK_URL=your-slack-webhook \
  -n ns-yellow-starter
```

**Default Credentials:**
- **PostgreSQL Database**: `postgres/datasurface123`
- **Airflow Web UI**: `admin/admin123` (created after deployment)
- **GitHub Token**: Replace `your-github-token` with your actual GitHub Personal Access Token
- **Slack Webhook**: Optional - replace with your actual webhook URL or use placeholder

### Step 2: Deploy PostgreSQL Database

```bash
# Apply the Kubernetes configuration
kubectl apply -f generated_output/YellowLive/kubernetes-bootstrap.yaml
```

### Step 3: Run Ring 1 Initialization

Ring 1 initialization creates the database schemas required for the platform operations.

```bash
# Wait for PostgreSQL to be ready
kubectl wait --for=condition=ready pod -l app=yellowlive-postgres -n ns-yellow-starter --timeout=300s

# Create required databases manually (required before Ring 1 initialization)
kubectl exec -it deployment/yellowlive-postgres -n ns-yellow-starter -- psql -U postgres -c "CREATE DATABASE airflow_db;"
kubectl exec -it deployment/yellowlive-postgres -n ns-yellow-starter -- psql -U postgres -c "CREATE DATABASE customer_db;"
kubectl exec -it deployment/yellowlive-postgres -n ns-yellow-starter -- psql -U postgres -c "CREATE DATABASE datasurface_merge;"

# Apply Ring 1 initialization jobs (creates platform database schemas)
kubectl apply -f generated_output/YellowLive/yellowlive_ring1_init_job.yaml
kubectl apply -f generated_output/YellowForensic/yellowforensic_ring1_init_job.yaml

# Wait for Ring 1 initialization to complete
kubectl wait --for=condition=complete job/yellowlive-ring1-init -n ns-yellow-starter --timeout=300s
kubectl wait --for=condition=complete job/yellowforensic-ring1-init -n ns-yellow-starter --timeout=300s
```

### Step 4: Verify Airflow Services

Airflow should initialize automatically with the consistent secret configuration.

```bash
# Wait for Airflow services to be ready
kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n ns-yellow-starter --timeout=300s
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

# Copy factory DAGs to Airflow
kubectl cp generated_output/YellowLive/yellowlive_factory_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter
kubectl cp generated_output/YellowLive/yellowlive_datatransformer_factory_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter
kubectl cp generated_output/YellowLive/yellowlive_infrastructure_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter

kubectl cp generated_output/YellowForensic/yellowforensic_factory_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter
kubectl cp generated_output/YellowForensic/yellowforensic_datatransformer_factory_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter
kubectl cp generated_output/YellowForensic/yellowforensic_infrastructure_dag.py $SCHEDULER_POD:/opt/airflow/dags/ -n ns-yellow-starter

# Deploy model merge jobs to populate ingestion stream configurations
kubectl apply -f generated_output/YellowLive/yellowlive_model_merge_job.yaml
kubectl apply -f generated_output/YellowForensic/yellowforensic_model_merge_job.yaml

# Wait for model merge jobs to complete
kubectl wait --for=condition=complete job/yellowlive-model-merge-job -n ns-yellow-starter --timeout=300s
kubectl wait --for=condition=complete job/yellowforensic-model-merge-job -n ns-yellow-starter --timeout=300s

# Restart Airflow scheduler to trigger factory DAGs (creates dynamic ingestion stream DAGs)
kubectl delete pod -n ns-yellow-starter -l app=airflow-scheduler
kubectl wait --for=condition=ready pod -l app=airflow-scheduler -n ns-yellow-starter --timeout=300s
```

### Step 7: Verify Deployment

```bash
# Check all pods are running
kubectl get pods -n ns-yellow-starter

# Verify databases exist
kubectl exec -it deployment/yellowlive-postgres -n ns-yellow-starter -- psql -U postgres -l

# Access Airflow web interface
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n ns-yellow-starter
```

Open http://localhost:8080 and login with:
- **Username**: `admin`
- **Password**: `admin123`

**Expected DAGs in Airflow UI:**
- `yellowlive_factory_dag` - YellowLive platform factory
- `yellowlive_datatransformer_factory_dag` - YellowLive datatransformer factory
- `yellowlive_infrastructure` - YellowLive infrastructure management
- `yellowlive__CustomerDatabase_ingestion` - YellowLive ingestion stream DAG (created dynamically)
- `yellowforensic_factory_dag` - YellowForensic platform factory  
- `yellowforensic_datatransformer_factory_dag` - YellowForensic datatransformer factory
- `yellowforensic_infrastructure` - YellowForensic infrastructure management
- `yellowforensic__CustomerDatabase_ingestion` - YellowForensic ingestion stream DAG (created dynamically)

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
# Test database connectivity
kubectl exec -it deployment/yellowlive-postgres -n ns-yellow-starter -- psql -U postgres -d postgres

# Test with expected credentials
# Username: postgres
# Password: datasurface123
```

### Common Issues and Solutions

**Issue: Ingestion stream DAGs not appearing in Airflow UI**
```bash
# Cause: Factory DAGs haven't run yet or model merge jobs failed
# Solution: Verify model merge jobs completed and restart scheduler
kubectl get jobs -n ns-yellow-starter
kubectl logs job/yellowlive-model-merge-job -n ns-yellow-starter
kubectl delete pod -n ns-yellow-starter -l app=airflow-scheduler
```

**Issue: Ring 1 initialization job fails**
```bash
# Cause: Secret keys mismatch or database not accessible
# Solution: Verify secret format and database connectivity
kubectl describe job/yellowlive-ring1-init -n ns-yellow-starter
kubectl logs job/yellowlive-ring1-init -n ns-yellow-starter
```

**Issue: Secret configuration errors**
```bash
# Cause: Inconsistent secret key names
# Verify all secrets use POSTGRES_USER/POSTGRES_PASSWORD format:
kubectl get secret postgres -n ns-yellow-starter -o yaml
```

## Next Steps

Once deployment is complete:
1. Set up GitHub Personal Access Token in git secret for actual repository access
2. Configure ingestion streams in Airflow (factory DAGs will create them dynamically)
3. Set up data change simulator for testing the pipeline
4. Create workspace views for data access
5. Monitor pipeline execution and DAG health

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
- Consistent secret configuration (POSTGRES_USER/POSTGRES_PASSWORD format)
- Automated database schema creation via Ring 1 initialization
- Automated ingestion stream DAG creation via factory pattern
- Ready for immediate data pipeline deployment

**Key Improvements:**
- **✅ Automated Ring 1 Initialization**: Database schemas created automatically
- **✅ Consistent Secret Format**: All components use same credential keys
- **✅ Complete Artifact Generation**: Ring 0 generates all required deployment files
- **✅ Dynamic DAG Creation**: Ingestion stream DAGs created automatically from database configurations
- **✅ No Manual Workarounds**: Clean deployment process without sed commands or manual fixes 