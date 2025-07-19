# HOWTO: Setup DataSurface Dynamic DAG Factory Environment

This guide walks through setting up a complete DataSurface environment with the dynamic DAG factory system on Kubernetes.

## Overview

The DataSurface dynamic DAG factory system automatically generates Airflow DAGs based on database-stored configurations, eliminating the need for static DAG files. This setup includes:

- **Factory DAGs**: Read configurations from database tables and dynamically create ingestion stream DAGs
- **Platform-specific databases**: Store ingestion stream configurations for each data platform
- **Automatic DAG generation**: New ingestion streams are added by updating database configurations

## Prerequisites

### Infrastructure Requirements
- **Kubernetes cluster** (local or cloud)
- **Docker** with access to build images
- **kubectl** configured for your cluster
- **Airflow** deployed in Kubernetes with:
  - Scheduler, Webserver, and Executor pods
  - Persistent storage for DAGs
  - Database for metadata
- **PostgreSQL** for:
  - Airflow metadata
  - DataSurface merge tables
  - DAG configuration storage

### DataSurface Requirements
- **DataSurface source code** with latest fixes
- **Model repository** on GitHub containing your ecosystem configuration
- **Docker registry access** to push/pull DataSurface images

## Step-by-Step Setup

### 1. Prepare DataSurface Docker Image

Build the DataSurface container with the latest code:

```bash
cd /path/to/datasurface
docker build -f Dockerfile.datasurface -t datasurface/datasurface:latest .
```

**Important**: Ensure your image includes the hostname fixes in `yellow_dp.py` where `postgres_hostname` uses `hostPortPair.hostName` directly (not `to_k8s_name()`).

### 2. Configure Kubernetes Secrets

Create secrets for all credentials referenced in your ecosystem model:

```bash
# PostgreSQL credentials (for merge database access)
kubectl create secret generic postgres \
  --from-literal=username=airflow \
  --from-literal=password=airflow \
  --from-literal=postgres_USER=airflow \
  --from-literal=postgres_PASSWORD=airflow \
  -n your-namespace

# Git credentials (for repository access)
kubectl create secret generic git \
  --from-literal=token=your_github_token \
  -n your-namespace

# Slack credentials (for notifications)
kubectl create secret generic slack \
  --from-literal=token=your_slack_token \
  -n your-namespace

# Kafka Connect credentials (if using Kafka ingestion)
kubectl create secret generic connect \
  --from-literal=token=dummy_token \
  -n your-namespace
```

**Note**: Adjust credential values to match your actual database users and tokens.

### 3. Setup Model Repository Access

Create a ConfigMap with your ecosystem model:

```bash
# Clone your model repository locally
git clone https://github.com/your-org/your-model-repo.git -b main

# Create ConfigMap from model files
kubectl create configmap your-model-config \
  --from-file=your-model-repo/ \
  -n your-namespace
```

### 4. Configure Airflow Scheduler

The Airflow scheduler needs access to the merge database and proper environment variables:

```bash
# Add database connection environment variables to scheduler
kubectl patch deployment airflow-scheduler -n your-namespace -p='
{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "airflow-scheduler",
          "env": [
            {"name": "DATASURFACE_POSTGRES_HOST", "value": "pg-data.your-namespace.svc.cluster.local"},
            {"name": "DATASURFACE_POSTGRES_PORT", "value": "5432"},
            {"name": "DATASURFACE_POSTGRES_DATABASE", "value": "datasurface_merge"}
          ]
        }]
      }
    }
  }
}'

# Update scheduler to use correct secrets
kubectl patch deployment airflow-scheduler -n your-namespace -p='
{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "airflow-scheduler",
          "envFrom": [
            {"secretRef": {"name": "postgres"}},
            {"secretRef": {"name": "git"}},
            {"secretRef": {"name": "slack"}}
          ]
        }]
      }
    }
  }
}'
```

### 5. Generate Bootstrap Artifacts

Create a Kubernetes job to generate factory DAGs and infrastructure:

```yaml
# bootstrap-generator.yaml
apiVersion: v1
kind: Pod
metadata:
  name: bootstrap-generator
  namespace: your-namespace
spec:
  serviceAccountName: airflow  # Or appropriate service account
  containers:
  - name: generator
    image: datasurface/datasurface:latest
    imagePullPolicy: Always
    command: ["/bin/bash"]
    args:
    - -c
    - |
      echo "Generating bootstrap artifacts..."
      python -m datasurface.cmd.platform generatePlatformBootstrap \
        --model /model \
        --output /output \
        --platform YellowLive YellowForensic
      
      echo "Bootstrap completed! Generated files:"
      ls -la /output/*/
      
      echo "Sleeping for 10 minutes to allow file extraction..."
      sleep 600
    env:
    - name: postgres_USER
      valueFrom:
        secretKeyRef:
          name: postgres
          key: username
    - name: postgres_PASSWORD
      valueFrom:
        secretKeyRef:
          name: postgres
          key: password
    - name: connect_TOKEN
      valueFrom:
        secretKeyRef:
          name: connect
          key: token
    - name: git_TOKEN
      valueFrom:
        secretKeyRef:
          name: git
          key: token
    - name: slack_TOKEN
      valueFrom:
        secretKeyRef:
          name: slack
          key: token
    volumeMounts:
    - name: model
      mountPath: /model
    - name: output
      mountPath: /output
  volumes:
  - name: model
    configMap:
      name: your-model-config
  - name: output
    emptyDir: {}
  restartPolicy: Never
```

Deploy and run the bootstrap generator:

```bash
kubectl apply -f bootstrap-generator.yaml
kubectl logs -f bootstrap-generator -n your-namespace
```

### 6. Deploy Factory DAGs to Airflow

Extract and deploy the generated factory DAGs:

```bash
# Get Airflow webserver pod name
AIRFLOW_POD=$(kubectl get pods -n your-namespace -l app=airflow-webserver -o jsonpath='{.items[0].metadata.name}')

# Copy factory DAGs from generator to local
kubectl cp bootstrap-generator:/output/YellowLive/yellowlive_factory_dag.py ./yellowlive_factory_dag.py -n your-namespace
kubectl cp bootstrap-generator:/output/YellowForensic/yellowforensic_factory_dag.py ./yellowforensic_factory_dag.py -n your-namespace

# Deploy factory DAGs to Airflow
kubectl cp ./yellowlive_factory_dag.py $AIRFLOW_POD:/opt/airflow/dags/ -n your-namespace
kubectl cp ./yellowforensic_factory_dag.py $AIRFLOW_POD:/opt/airflow/dags/ -n your-namespace

# Clean up local files
rm -f yellowlive_factory_dag.py yellowforensic_factory_dag.py
```

### 7. Populate Database Configurations

Create a job to populate the database with ingestion stream configurations:

```yaml
# populate-configs.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: populate-dag-configs
  namespace: your-namespace
spec:
  template:
    spec:
      serviceAccountName: airflow
      containers:
      - name: populate
        image: datasurface/datasurface:latest
        imagePullPolicy: Always
        command: ["/bin/bash"]
        args:
        - -c
        - |
          echo "Populating DAG configurations..."
          python -m datasurface.cmd.platform handleModelMerge \
            --model /model \
            --output /tmp/output \
            --platform YellowLive YellowForensic
          echo "Configuration population completed!"
        env:
        - name: postgres_USER
          valueFrom:
            secretKeyRef:
              name: postgres
              key: username
        - name: postgres_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres
              key: password
        - name: connect_TOKEN
          valueFrom:
            secretKeyRef:
              name: connect
              key: token
        - name: git_TOKEN
          valueFrom:
            secretKeyRef:
              name: git
              key: token
        - name: slack_TOKEN
          valueFrom:
            secretKeyRef:
              name: slack
              key: token
        volumeMounts:
        - name: model
          mountPath: /model
      volumes:
      - name: model
        configMap:
          name: your-model-config
      restartPolicy: Never
  backoffLimit: 4
```

Deploy and run the configuration population:

```bash
kubectl apply -f populate-configs.yaml
kubectl logs -f job/populate-dag-configs -n your-namespace
```

### 8. Verify Installation

#### Check Factory DAGs in Airflow

1. **Access Airflow UI**: Navigate to your Airflow web interface (typically http://localhost:8080 or via port-forward)
2. **Verify Factory DAGs**: You should now see the factory DAGs as **visible, schedulable DAGs**:
   - `yellowlive_factory_dag` ✅ (visible and schedulable)
   - `yellowforensic_factory_dag` ✅ (visible and schedulable)
3. **Check DAG Status**: Both should be enabled, parsing successfully, and running every 5 minutes
4. **View Factory Logs**: Click on the factory DAGs to see detailed execution logs showing:
   - Which dynamic DAGs were created/updated/removed by name
   - Database configuration loading status  
   - Step-by-step factory execution progress
   - DAG lifecycle management (creation, updates, zombie DAG cleanup)
   - Clear success/failure indicators with change counts

#### Test Factory DAG Execution

Test the factory DAGs to ensure they can read configurations and generate dynamic DAGs:

```bash
# Get scheduler pod name
SCHEDULER_POD=$(kubectl get pods -n your-namespace -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')

# Test factory DAG execution
kubectl exec $SCHEDULER_POD -n your-namespace -- python3 /opt/airflow/dags/yellowlive_factory_dag.py
```

**Expected output**: No errors, possibly a deprecation warning about `is_delete_operator_pod`

#### Verify Database Tables

Check that the configuration tables were created:

```bash
# Connect to PostgreSQL and verify tables exist
kubectl exec -it $POSTGRES_POD -n your-namespace -- psql -U airflow -d datasurface_merge -c "
  SELECT table_name FROM information_schema.tables 
  WHERE table_schema = 'public' AND table_name LIKE '%_airflow_dsg';
"
```

**Expected output**: 
- `yellowlive_airflow_dsg`
- `yellowforensic_airflow_dsg`

#### Check Dynamic DAG Generation

After factory DAGs run, you should see dynamically generated ingestion stream DAGs:

- `yellowlive__Store1_ingestion` (or your store names)
- `yellowforensic__Store1_ingestion`

## Troubleshooting

### Common Issues

#### 1. Factory DAGs Not Appearing in Airflow

**Symptoms**: Factory DAGs don't show up in Airflow UI

**Solutions**:
- Check Airflow scheduler logs for parsing errors
- Verify factory DAG files are in `/opt/airflow/dags/` directory
- Ensure no syntax errors in generated DAGs
- Check that Airflow can access the files (permissions)

**Note**: Factory DAGs should now be **visible in the Airflow UI** as schedulable DAGs running every 5 minutes. If they're not visible, this indicates a configuration or parsing error.

#### 2. Database Connection Errors

**Symptoms**: Factory DAGs fail with connection errors

**Solutions**:
- Verify PostgreSQL credentials in secrets
- Check database hostname resolution in Kubernetes
- Ensure `datasurface_merge` database exists
- Verify scheduler has environment variables set

#### 3. Hostname Resolution Issues

**Symptoms**: `could not translate host name` errors

**Solutions**:
- Verify Kubernetes DNS is working: `nslookup pg-data.your-namespace.svc.cluster.local`
- Check service names and namespaces match your configuration
- Ensure DataSurface image includes hostname fixes

#### 4. Missing Environment Variables

**Symptoms**: `'namespace_name'` or credential errors

**Solutions**:
- Verify all required secrets exist and have correct keys
- Check scheduler pod environment variables
- Restart scheduler after updating secrets/env vars

#### 5. Factory DAG Not Creating Dynamic DAGs

**Symptoms**: Factory DAG is visible but no dynamic ingestion DAGs appear

**Solutions**:
- Check factory DAG execution logs in Airflow UI
- Verify database contains active configurations: `SELECT * FROM yellowlive_airflow_dsg WHERE status='active';`
- Manually trigger the factory DAG to force immediate execution
- Check database connection from factory DAG logs
- Verify all environment variables are set correctly

### Useful Commands

```bash
# Check Airflow scheduler logs
kubectl logs deployment/airflow-scheduler -n your-namespace

# List all DAG files in Airflow
kubectl exec $AIRFLOW_POD -n your-namespace -- ls -la /opt/airflow/dags/

# Test DAG parsing
kubectl exec $SCHEDULER_POD -n your-namespace -- python3 -m py_compile /opt/airflow/dags/yellowlive_factory_dag.py

# Check database tables
kubectl exec -it $POSTGRES_POD -n your-namespace -- psql -U airflow -d datasurface_merge -c "\dt"

# View DAG configurations
kubectl exec -it $POSTGRES_POD -n your-namespace -- psql -U airflow -d datasurface_merge -c "SELECT * FROM yellowlive_airflow_dsg;"

# Check factory DAG execution history (via Airflow UI)
# Navigate to: Airflow UI > DAGs > yellowlive_factory_dag > Graph/Logs

# Manually trigger factory DAG
# Navigate to: Airflow UI > DAGs > yellowlive_factory_dag > Trigger DAG
```

## Updating Configurations

To add new ingestion streams or modify existing ones:

1. **Update your ecosystem model** in the GitHub repository
2. **Update the ConfigMap** with new model files
3. **Re-run the populate job** to update database configurations
4. **Factory DAGs will automatically** pick up changes on next execution

### Automatic Lifecycle Management

The factory DAGs now provide **complete DAG lifecycle management**:

- **➕ Adding configurations** → Factory creates new dynamic DAGs automatically
- **✏️ Modifying configurations** → Factory updates existing DAGs with new settings  
- **❌ Removing configurations** → Factory **automatically removes obsolete DAGs** (zombie DAG cleanup)

**Benefits:**
- ✅ **No zombie DAGs** - removed configurations are automatically cleaned up
- ✅ **Complete visibility** - logs show exactly which DAGs were added/updated/removed
- ✅ **Database synchronization** - DAGs always match current database configuration
- ✅ **Operational clarity** - clear audit trail of all changes

## Next Steps

Once your dynamic DAG factory is operational:

1. **Configure ingestion sources** in your ecosystem model
2. **Set up data platform credentials** for actual data sources
3. **Monitor DAG execution** and ingestion pipeline performance
4. **Add new data sources** by updating database configurations
5. **Scale infrastructure** as needed for production workloads

## Support

For issues or questions:
- Check the [DataSurface documentation](../README.md)
- Review [architectural decisions](ArchitecturalDecisions.md)
- File issues in the DataSurface repository 