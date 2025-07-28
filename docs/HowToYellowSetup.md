# YellowDataPlatform Setup Guide

This guide walks you through setting up a complete YellowDataPlatform environment for DataSurface, including all required services and configurations.

This is a work in progress, the guide is not complete and may lag the implementation for now.

## Prerequisites

### Required Software

- **Docker Desktop** with Kubernetes enabled
- **kubectl** command-line tool
- **Python 3.12+** with virtual environment support
- **Git** for version control

### System Requirements

- **Memory**: At least 8GB RAM (16GB recommended)
- **Storage**: At least 20GB free disk space
- **CPU**: Multi-core processor recommended

## Step 1: Environment Setup

### 1.1 Clone and Setup DataSurface Repository

```bash
# Clone the DataSurface repository
git clone <your-datasurface-repo-url>
cd datasurface

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .
```

### 1.2 Verify Docker Desktop Kubernetes

```bash
# Ensure Docker Desktop is running with Kubernetes enabled
kubectl cluster-info

# Verify you can access the cluster
kubectl get nodes
```

## Step 2: Create Your Ecosystem Model

### 2.1 Define Your Ecosystem

Create a directory structure for your ecosystem model:

```bash
mkdir -p src/tests/my-ecosystem
```

Create `src/tests/my-ecosystem/eco.py`:

```python
"""
// Copyright (c) Your Name
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Ecosystem, GitHubRepository, Documentation, PlainTextDocumentation
from datasurface.md import LocationKey, Credential, CredentialType
from datasurface.md import CloudVendor, InfrastructureLocation, InfrastructureVendor
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform

def createEcosystem() -> Ecosystem:
    """Create the ecosystem for the YellowDataPlatform."""
    
    # Define documentation
    doc: Documentation = PlainTextDocumentation("YellowDataPlatform Ecosystem")
    
    # Define locations
    us_location: LocationKey = LocationKey(
        name="US",
        vendor=InfrastructureVendor.PRIVATE,
        cloudVendor=CloudVendor.PRIVATE
    )
    
    # Define credentials
    postgres_credential: Credential = Credential(
        name="postgres",
        credentialType=CredentialType.USER_PASSWORD,
        doc=PlainTextDocumentation("PostgreSQL database credentials")
    )
    
    git_credential: Credential = Credential(
        name="git",
        credentialType=CredentialType.API_TOKEN,
        doc=PlainTextDocumentation("GitHub API token")
    )
    
    slack_credential: Credential = Credential(
        name="slack",
        credentialType=CredentialType.API_TOKEN,
        doc=PlainTextDocumentation("Slack webhook token")
    )
    
    connect_credential: Credential = Credential(
        name="connect",
        credentialType=CredentialType.API_TOKEN,
        doc=PlainTextDocumentation("Kafka Connect API token")
    )
    
    # Define the repository
    repo: GitHubRepository = GitHubRepository(
        repo="your-username/your-repo",
        branchName="main",
        doc=PlainTextDocumentation("Main ecosystem repository")
    )
    
    # Create the ecosystem
    eco: Ecosystem = Ecosystem(
        name="MyKubEcosystem",
        owningRepo=repo,
        doc=doc
    )
    
    # Add locations
    eco.addLocation(us_location)
    
    # Add credentials
    eco.addCredential(postgres_credential)
    eco.addCredential(git_credential)
    eco.addCredential(slack_credential)
    eco.addCredential(connect_credential)
    
    # Create the data platform
    platform: YellowDataPlatform = YellowDataPlatform(
        name="my-kub-platform",
        locs={us_location},
        doc=PlainTextDocumentation("YellowDataPlatform"),
        namespace="my-kub-namespace",
        connectCredentials=connect_credential,
        postgresCredential=postgres_credential,
        gitCredential=git_credential,
        slackCredential=slack_credential,
        airflowName="airflow",
        postgresName="pg-data",
        kafkaConnectName="kafka-connect",
        kafkaClusterName="kafka-cluster",
        slackChannel="datasurface-events"
    )
    
    # Add the platform to the ecosystem
    eco.addDataPlatform(platform)
    
    return eco
```

### 2.2 Validate Your Ecosystem

```bash
# Test your ecosystem model
python -c "
from src.tests.my-ecosystem.eco import createEcosystem
eco = createEcosystem()
print('Ecosystem created successfully:', eco.name)
"
```

## Step 3: Generate Kubernetes Artifacts

### 3.1 Generate Bootstrap Files

```bash
# Generate the Kubernetes YAML and Airflow DAG
python -m datasurface.cmd.platform generatePlatformBootstrap \
    "src/tests/my-ecosystem" \
    "src/tests/my-ecosystem/output" \
    "my-kub-platform"
```

This will create:

- `kubernetes-bootstrap.yaml` - Complete Kubernetes deployment
- `my-kub-platform_infrastructure_dag.py` - Airflow DAG for infrastructure

### 3.2 Review Generated Files

```bash
# Check the generated files
ls -la src/tests/my-ecosystem/output/my-kub-platform/
```

## Step 4: Deploy to Kubernetes

### 4.1 Create Required Secrets

```bash
# Create namespace (if not already created by the YAML)
kubectl create namespace my-kub-namespace

# Create PostgreSQL secret
kubectl create secret generic postgres \
    -n my-kub-namespace \
    --from-literal=POSTGRES_USER=airflow \
    --from-literal=POSTGRES_PASSWORD=your-secure-password

# Create Airflow secret
kubectl create secret generic airflow \
    -n my-kub-namespace \
    --from-literal=AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here \
    --from-literal=AIRFLOW__CORE__SECRET_KEY=your-secret-key-here

# Create Git secret
kubectl create secret generic git \
    -n my-kub-namespace \
    --from-literal=GIT_TOKEN=your-github-token

# Create Slack secret
kubectl create secret generic slack \
    -n my-kub-namespace \
    --from-literal=SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your-webhook

# Create Kafka Connect secret
kubectl create secret generic connect \
    -n my-kub-namespace \
    --from-literal=CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-service.my-kub-namespace.svc.cluster.local
```

### 4.2 Apply Kubernetes Configuration

```bash
# Apply the generated Kubernetes YAML
kubectl apply -f src/tests/my-ecosystem/output/my-kub-platform/kubernetes-bootstrap.yaml
```

### 4.3 Monitor Deployment

```bash
# Watch the pods start up
kubectl get pods -n my-kub-namespace -w

# Check all resources
kubectl get all -n my-kub-namespace
```

## Step 5: Initialize Database

### 5.1 Create Airflow Database

```bash
# Get the PostgreSQL pod name
POSTGRES_POD=$(kubectl get pods -n my-kub-namespace -l app=test-dp-postgres -o jsonpath='{.items[0].metadata.name}')

# Create the airflow_db database
kubectl exec -n my-kub-namespace $POSTGRES_POD -- psql -U airflow -c "CREATE DATABASE airflow_db;"
```

### 5.2 Restart Airflow Pods

```bash
# Restart Airflow pods to pick up the database
kubectl delete pods -n my-kub-namespace -l app=airflow-scheduler
kubectl delete pods -n my-kub-namespace -l app=airflow-webserver
```

## Step 6: Verify Deployment

### 6.1 Check Service Status

```bash
# Verify all pods are running
kubectl get pods -n my-kub-namespace

# Check services
kubectl get svc -n my-kub-namespace

# Verify Airflow webserver is accessible
kubectl get svc airflow-webserver-service -n my-kub-namespace
```

### 6.2 Access Airflow Web Interface

```bash
# Port forward to access Airflow locally
kubectl port-forward -n my-kub-namespace svc/airflow-webserver-service 8080:8080
```

Then open your browser to `http://localhost:8080`

### 6.3 Verify Environment Variables

```bash
# Check Git environment variables in Airflow
kubectl exec -n my-kub-namespace $(kubectl get pods -n my-kub-namespace -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}') -- printenv | grep GIT
```

Expected output:

```text
GIT_REPO_URL=https://github.com/your-username/your-repo
GIT_REPO_BRANCH=main
GIT_REPO_NAME=your-username/your-repo
GIT_TOKEN=your-github-token
```

## Step 7: Deploy Airflow DAG

### 7.1 Copy DAG to Airflow

```bash
# Copy the generated DAG to Airflow's DAGs folder
kubectl cp src/tests/my-ecosystem/output/my-kub-platform/my-kub-platform_infrastructure_dag.py \
    my-kub-namespace/$(kubectl get pods -n my-kub-namespace -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/dags/
```

### 7.2 Verify DAG in Airflow

1. Open Airflow web interface at `http://localhost:8080`
2. Navigate to DAGs
3. Look for your infrastructure DAG
4. Enable and trigger the DAG

## Step 8: Test Your Setup

### 8.1 Test PostgreSQL Connection

```bash
# Test database connectivity
kubectl exec -n my-kub-namespace $POSTGRES_POD -- psql -U airflow -d airflow_db -c "SELECT version();"
```

### 8.2 Test Kafka Connectivity

```bash
# Test Kafka connection
kubectl exec -n my-kub-namespace $(kubectl get pods -n my-kub-namespace -l app=kafka-cluster -o jsonpath='{.items[0].metadata.name}') -- kafka-topics --bootstrap-server localhost:9092 --list
```

### 8.3 Test Git Integration

Create a simple Airflow task to test Git access:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def test_git_access():
    """Test Git repository access."""
    git_url = os.environ.get('GIT_REPO_URL')
    git_token = os.environ.get('GIT_TOKEN')
    print(f"Git URL: {git_url}")
    print(f"Git token available: {'Yes' if git_token else 'No'}")

dag = DAG('test_git_access', start_date=datetime(2024, 1, 1))

test_task = PythonOperator(
    task_id='test_git',
    python_callable=test_git_access,
    dag=dag
)
```

## Troubleshooting

### Common Issues

#### 1. Pods Stuck in Pending

```bash
# Check resource availability
kubectl describe pod <pod-name> -n my-kub-namespace

# Check node resources
kubectl top nodes
```

#### 2. Airflow Database Connection Issues

```bash
# Verify database exists
kubectl exec -n my-kub-namespace $POSTGRES_POD -- psql -U airflow -l

# Check Airflow logs
kubectl logs -n my-kub-namespace <airflow-pod-name>
```

#### 3. Secret Not Found Errors

```bash
# Verify secrets exist
kubectl get secrets -n my-kub-namespace

# Check secret contents (be careful with sensitive data)
kubectl get secret <secret-name> -n my-kub-namespace -o yaml
```

#### 4. Namespace Validation Errors

```bash
# Ensure namespace follows RFC 1123 (lowercase, hyphens only)
# Valid: my-kub-namespace
# Invalid: My_Kub_Namespace
```

### Useful Commands

```bash
# Get detailed pod information
kubectl describe pod <pod-name> -n my-kub-namespace

# View pod logs
kubectl logs <pod-name> -n my-kub-namespace

# Execute commands in pods
kubectl exec -it <pod-name> -n my-kub-namespace -- /bin/bash

# Check resource usage
kubectl top pods -n my-kub-namespace
```

## Cleanup

### Remove the Environment

```bash
# Delete the namespace (removes all resources)
kubectl delete namespace my-kub-namespace

# Or delete individual resources
kubectl delete -f src/tests/my-ecosystem/output/my-kub-platform/kubernetes-bootstrap.yaml
```

## Next Steps

1. **Configure Data Pipelines**: Add your data sources and transformations to the ecosystem
2. **Set Up Monitoring**: Configure logging and monitoring for your data platform
3. **Security Hardening**: Implement proper security practices and secrets management
4. **Scaling**: Configure resource limits and scaling policies
5. **Backup Strategy**: Implement database and configuration backups

## Support

For issues and questions:

- Check the DataSurface documentation
- Review Kubernetes and Airflow logs
- Consult the troubleshooting section above
- Open an issue in the DataSurface repository
