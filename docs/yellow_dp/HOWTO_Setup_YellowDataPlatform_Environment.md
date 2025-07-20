# HOWTO: Setup YellowDataPlatform Kubernetes Environment

## Overview

This document provides a step-by-step guide to set up a complete YellowDataPlatform environment on Kubernetes. The setup uses a two-ring approach:

- **Ring 0**: Generate bootstrap artifacts (runs in Docker container)
- **Ring 1**: Deploy to Kubernetes with full infrastructure (requires secrets and K8s cluster)

## Prerequisites

- Docker Desktop with Kubernetes enabled
- kubectl configured for your target cluster
- GitHub repository with your datasurface ecosystem model
- GitHub Personal Access Token with repository access

## Phase 1: Bootstrap Artifact Generation (Ring 0)

### Step 1: Clone the Starter Repository

```bash
git clone https://github.com/billynewport/yellow_starter.git
cd yellow_starter
```

### Step 2: Configure the Ecosystem Model

```bash
# Review the ecosystem model
cat eco.py

# Review the platform assignments
cat dsg_platform_mapping.json
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

# Create database credentials secret (standard PostgreSQL defaults)
kubectl create secret generic postgres \
  --from-literal=POSTGRES_USER=postgres \
  --from-literal=POSTGRES_PASSWORD=datasurface123 \
  -n ns-yellow-starter

# Create GitHub credentials secret  
kubectl create secret generic git \
  --from-literal=GITHUB_TOKEN=your-github-token \
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

### Step 3: Initialize Platform Databases

```bash
# Run Ring 1 initialization inside Kubernetes
kubectl run platform-init --rm -i --tty \
  --image=datasurface/datasurface:latest \
  --restart=Never \
  -n ns-yellow-starter \
  -- python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 1 \
  --model /workspace/model \
  --platform YellowLive YellowForensic
```

### Step 4: Deploy DAG Factory

```bash
# Copy factory DAGs to Airflow
kubectl cp generated_output/YellowLive/yellowlive_factory_dag.py \
  airflow-scheduler-pod:/opt/airflow/dags/

kubectl cp generated_output/YellowForensic/yellowforensic_factory_dag.py \
  airflow-scheduler-pod:/opt/airflow/dags/
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

### Step 6: Verify Deployment

```bash
# Check all pods are running
kubectl get pods -n ns-yellow-starter

# Access Airflow web interface
kubectl port-forward svc/airflow-webserver-service 8080:8080 -n ns-yellow-starter
```

Open http://localhost:8080 and login with:
- **Username**: `admin`
- **Password**: `admin123`

## Ring Level Explanation

**Ring 0**: Generate artifacts only (no external dependencies)
**Ring 1**: Initialize databases and runtime configuration (requires Kubernetes cluster)

## Troubleshooting

### Container Issues
```bash
# Rebuild container if needed
docker build -f Dockerfile.datasurface -t datasurface/datasurface:latest .
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
kubectl exec -it deployment/postgres -n ns-yellow-starter -- psql -U postgres -d postgres

# Test with expected credentials
# Username: postgres
# Password: datasurface123
```

## Next Steps

Once deployment is complete:
1. Configure ingestion streams in Airflow
2. Set up data change simulator for testing
3. Create workspace views for data access
4. Monitor pipeline execution

## References

- [MVP Dynamic DAG Factory](MVP_DynamicDAG.md)
- [MVP Kubernetes Infrastructure Setup](MVP_Kubernetes_Infrastructure_Setup.md)
- [July MVP Plan](July_MVP_Plan.md)

---

**Ready for Production:** Complete YellowDataPlatform environment with dynamic DAG factory 