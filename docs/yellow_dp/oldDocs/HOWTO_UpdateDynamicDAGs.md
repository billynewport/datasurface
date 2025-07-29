# HOWTO: Update Dynamic DAGs in Kubernetes Cluster

## Overview

This guide explains how to update the dynamic DAG factory system in the Kubernetes cluster. The process involves:

1. **Use Existing CLI**: Use the `generatePlatformBootstrap` command from `platform.py`
2. **Deploy Factory DAGs**: Copy generated DAGs to Airflow
3. **Update Database Configuration**: Populate stream configurations

## Prerequisites

- Kubernetes cluster with Airflow deployed
- Access to `ns-kub-pg-test` namespace
- DataSurface container image built and available
- GitHub repository access (github.com/billynewport/mvpmodel)

## Step 1: Clone Model Repository and Generate Bootstrap Artifacts

### 1.1 Create Bootstrap Job YAML

Create a Kubernetes job that clones the model repository and runs the bootstrap generation:

```yaml
# bootstrap-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: datasurface-bootstrap-job
  namespace: ns-kub-pg-test
spec:
  template:
    spec:
      containers:
      - name: bootstrap-generator
        image: datasurface/datasurface:latest
        command: ["/bin/bash"]
        args:
        - -c
        - |
          echo "🚀 Starting DataSurface Bootstrap Generation"
          
          # Clone the model repository using git token
          echo "📥 Cloning model repository..."
          git clone https://$git_TOKEN@github.com/billynewport/mvpmodel.git /workspace/model
          cd /workspace/model
          
          # Generate bootstrap artifacts for both platforms using CLI
          echo "🔧 Generating YellowLive bootstrap artifacts..."
          python -m datasurface.cmd.platform generatePlatformBootstrap \
            --model /workspace/model \
            --output /workspace/generated_artifacts \
            --platform YellowLive YellowForensic
          
          echo "✅ Bootstrap generation complete!"
          echo "📁 Generated artifacts:"
          ls -la /workspace/generated_artifacts/
          
          # Keep container running to copy files
          echo "⏳ Keeping container alive for file copy..."
          sleep 3600
        env:
        - name: PYTHONPATH
          value: "/app/src"
        - name: git_TOKEN
          valueFrom:
            secretKeyRef:
              name: git
              key: token
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
        volumeMounts:
        - name: git-workspace
          mountPath: /workspace
      volumes:
      - name: git-workspace
        emptyDir: {}
      restartPolicy: Never
  backoffLimit: 3
```

### 1.2 Deploy the Bootstrap Job

```bash
# Apply the job
kubectl apply -f bootstrap-job.yaml -n ns-kub-pg-test

# Check job status
kubectl get jobs -n ns-kub-pg-test datasurface-bootstrap-job

# Monitor job logs
kubectl logs -f job/datasurface-bootstrap-job -n ns-kub-pg-test
```

## Step 2: Copy Generated DAGs to Airflow

### 2.1 Get Pod Names

```bash
# Get the bootstrap pod name
POD_NAME=$(kubectl get pods -n ns-kub-pg-test -l job-name=datasurface-bootstrap-job -o jsonpath='{.items[0].metadata.name}')
echo "Bootstrap pod: $POD_NAME"

# Get Airflow scheduler pod name
AIRFLOW_POD=$(kubectl get pods -n ns-kub-pg-test -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')
echo "Airflow scheduler pod: $AIRFLOW_POD"
```

### 2.2 Copy Factory DAGs to Airflow Scheduler

```bash
# Copy YellowLive factory DAG to local first, then to Airflow
echo "📋 Copying YellowLive factory DAG to local..."
kubectl cp $POD_NAME:/workspace/generated_artifacts/YellowLive/yellowlive_factory_dag.py ./yellowlive_factory_dag.py -n ns-kub-pg-test

echo "📋 Copying YellowForensic factory DAG to local..."
kubectl cp $POD_NAME:/workspace/generated_artifacts/YellowForensic/yellowforensic_factory_dag.py ./yellowforensic_factory_dag.py -n ns-kub-pg-test

# Copy from local to Airflow scheduler
echo "📋 Copying YellowLive factory DAG to Airflow..."
kubectl cp ./yellowlive_factory_dag.py $AIRFLOW_POD:/opt/airflow/dags/yellowlive_factory_dag.py -n ns-kub-pg-test

echo "📋 Copying YellowForensic factory DAG to Airflow..."
kubectl cp ./yellowforensic_factory_dag.py $AIRFLOW_POD:/opt/airflow/dags/yellowforensic_factory_dag.py -n ns-kub-pg-test

# Verify files were copied
echo "✅ Verifying DAG files..."
kubectl exec $AIRFLOW_POD -n ns-kub-pg-test -- ls -la /opt/airflow/dags/ | grep factory
```

## Step 3: Update Database Configuration

### 3.1 Create Model Merge Handler Job

Create a Kubernetes job that clones the model repository and runs the model merge handler:

```yaml
# model-merge-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: datasurface-model-merge-job
  namespace: ns-kub-pg-test
spec:
  template:
    spec:
      containers:
      - name: model-merge-handler
        image: datasurface/datasurface:latest
        command: ["/bin/bash"]
        args:
        - -c
        - |
          echo "🔄 Starting DataSurface Model Merge Handler"
          
          # Clone the model repository using git token
          echo "📥 Cloning model repository..."
          git clone https://$git_TOKEN@github.com/billynewport/mvpmodel.git /workspace/model
          cd /workspace/model
          
          # Run merge handler for both platforms using CLI
          echo "🔧 Running model merge handler..."
          python -m datasurface.cmd.platform handleModelMerge \
            --model /workspace/model \
            --output /workspace/generated_artifacts \
            --platform YellowLive YellowForensic
          
          echo "✅ Model merge handler complete!"
        env:
        - name: PYTHONPATH
          value: "/app/src"
        - name: git_TOKEN
          valueFrom:
            secretKeyRef:
              name: git
              key: token
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
        volumeMounts:
        - name: git-workspace
          mountPath: /workspace
      volumes:
      - name: git-workspace
        emptyDir: {}
      restartPolicy: Never
  backoffLimit: 3
```

### 3.2 Deploy and Monitor the Model Merge Job

```bash
# Apply the job
kubectl apply -f model-merge-job.yaml -n ns-kub-pg-test

# Check job status
kubectl get jobs -n ns-kub-pg-test datasurface-model-merge-job

# Monitor job logs
kubectl logs -f job/datasurface-model-merge-job -n ns-kub-pg-test

# Wait for completion
kubectl wait --for=condition=complete job/datasurface-model-merge-job -n ns-kub-pg-test --timeout=300s
```

### 3.2 Verify Database Configuration

```bash
# Check that configuration tables were populated
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- psql -U airflow -d datasurface_merge -c "
SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%airflow_dsg';
"

# Check YellowLive configuration
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- psql -U airflow -d datasurface_merge -c "
SELECT stream_key, status FROM yellowlive_airflow_dsg;
"

# Check YellowForensic configuration
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- psql -U airflow -d datasurface_merge -c "
SELECT stream_key, status FROM yellowforensic_airflow_dsg;
"
```

### 3.3 Restart Airflow Scheduler (CRITICAL)

**⚠️ IMPORTANT**: After updating the database configuration, the Airflow scheduler must be restarted to pick up the new configuration. Factory DAGs execute code during DAG discovery (when Airflow loads the DAG files), so changes to the database configuration require a scheduler restart.

```bash
# Restart the Airflow scheduler deployment
kubectl rollout restart deployment/airflow-scheduler -n ns-kub-pg-test

# Wait for the new scheduler pod to be ready
kubectl rollout status deployment/airflow-scheduler -n ns-kub-pg-test

# Verify the new scheduler is running
kubectl get pods -n ns-kub-pg-test | grep airflow-scheduler
```

**Why this is necessary**: Factory DAGs contain code that runs during DAG discovery to load configurations from the database and create dynamic DAGs. If the database configuration changes but the scheduler isn't restarted, the factory DAGs will continue using the old cached configuration, resulting in errors like `'schedule_string'` or missing dynamic DAGs.

## Step 4: Verify Dynamic DAG Generation

### 4.1 Check Airflow Scheduler Logs

```bash
# Monitor scheduler logs for DAG generation
kubectl logs -f deployment/airflow-scheduler -n ns-kub-pg-test | grep -E "(factory|yellowlive|yellowforensic)"
```

### 4.2 Verify Dynamic DAGs in Airflow UI

1. Open Airflow UI at http://localhost:8080
2. Look for dynamically generated DAGs:
   - `yellowlive__Store1_ingestion`
   - `yellowforensic__Store1_ingestion`
3. Note: Factory DAGs (`yellowlive_factory_dag`, `yellowforensic_factory_dag`) will NOT appear in UI (this is correct behavior)

### 4.3 Test DAG Execution

```bash
# Trigger a test run of the dynamic DAGs
kubectl exec $AIRFLOW_POD -n ns-kub-pg-test -- airflow dags trigger yellowlive__Store1_ingestion

# Check execution status
kubectl exec $AIRFLOW_POD -n ns-kub-pg-test -- airflow dags list | grep yellowlive
```

## Step 5: Cleanup

### 5.1 Clean Up Bootstrap Job

```bash
# Delete the bootstrap job
kubectl delete job datasurface-bootstrap-job -n ns-kub-pg-test

# Verify cleanup
kubectl get jobs -n ns-kub-pg-test | grep bootstrap
```

## Troubleshooting

### Common Issues

#### Issue 1: Bootstrap Job Fails
```bash
# Check job logs
kubectl logs job/datasurface-bootstrap-job -n ns-kub-pg-test

# Check job status
kubectl describe job datasurface-bootstrap-job -n ns-kub-pg-test
```

#### Issue 2: DAG Files Not Copied
```bash
# Verify source files exist
kubectl exec $POD_NAME -n ns-kub-pg-test -- ls -la /workspace/

# Check destination directory
kubectl exec $AIRFLOW_POD -n ns-kub-pg-test -- ls -la /opt/airflow/dags/
```

#### Issue 3: Database Configuration Missing
```bash
# Check database connectivity
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- psql -U airflow -d datasurface_merge -c "\dt"

# Check configuration tables
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- psql -U airflow -d datasurface_merge -c "SELECT * FROM yellowlive_airflow_dsg;"
```

#### Issue 4: Dynamic DAGs Not Appearing
```bash
# Check scheduler logs for errors
kubectl logs deployment/airflow-scheduler -n ns-kub-pg-test | grep -i error

# Verify factory DAG compilation
kubectl exec $AIRFLOW_POD -n ns-kub-pg-test -- python3 -m py_compile /opt/airflow/dags/yellowlive_factory_dag.py
```

#### Issue 5: Factory DAG Errors After Database Changes
If you see errors like `Error loading platform configurations: 'schedule_string'` after updating the database configuration:

```bash
# This error occurs because the Airflow scheduler needs to be restarted
# to pick up the new database configuration

# Restart the Airflow scheduler
kubectl rollout restart deployment/airflow-scheduler -n ns-kub-pg-test

# Wait for restart to complete
kubectl rollout status deployment/airflow-scheduler -n ns-kub-pg-test

# Check logs for successful factory DAG execution
kubectl logs deployment/airflow-scheduler -n ns-kub-pg-test --tail=50 | grep -E "(FACTORY DAG EXECUTION|Found.*active configurations)"
```

**Root Cause**: Factory DAGs execute code during DAG discovery. When database configuration changes, the scheduler must be restarted to reload the configuration and create dynamic DAGs correctly.

### Debug Commands

```bash
# Check all pods in namespace
kubectl get pods -n ns-kub-pg-test

# Check Airflow scheduler status
kubectl describe deployment airflow-scheduler -n ns-kub-pg-test

# Check database connection
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- psql -U airflow -d datasurface_merge -c "SELECT version();"
```

## Automation Script

For convenience, here's a complete automation script:

```bash
#!/bin/bash
# update-dynamic-dags.sh

set -e

NAMESPACE="ns-kub-pg-test"
MODEL_REPO="https://github.com/billynewport/mvpmodel.git"

echo "🚀 Starting Dynamic DAG Update Process"

# Step 1: Deploy bootstrap job
echo "📦 Deploying bootstrap job..."
kubectl apply -f bootstrap-job.yaml -n $NAMESPACE

# Wait for job to start
echo "⏳ Waiting for bootstrap job to start..."
kubectl wait --for=condition=ready pod -l job-name=datasurface-bootstrap-job -n $NAMESPACE --timeout=300s

# Get pod names
POD_NAME=$(kubectl get pods -n $NAMESPACE -l job-name=datasurface-bootstrap-job -o jsonpath='{.items[0].metadata.name}')
AIRFLOW_POD=$(kubectl get pods -n $NAMESPACE -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')

echo "📋 Bootstrap pod: $POD_NAME"
echo "📋 Airflow pod: $AIRFLOW_POD"

# Wait for bootstrap completion
echo "⏳ Waiting for bootstrap generation..."
kubectl logs -f job/datasurface-bootstrap-job -n $NAMESPACE &

# Copy DAGs to Airflow
echo "📋 Copying factory DAGs to Airflow..."
kubectl cp $POD_NAME:/workspace/yellowlive_factory_dag.py $AIRFLOW_POD:/opt/airflow/dags/yellowlive_factory_dag.py -n $NAMESPACE
kubectl cp $POD_NAME:/workspace/yellowforensic_factory_dag.py $AIRFLOW_POD:/opt/airflow/dags/yellowforensic_factory_dag.py -n $NAMESPACE

# Run model merge handler
echo "🔄 Running model merge handler..."
kubectl run model-merge-handler --image=datasurface/datasurface:latest --rm -it --restart=Never -n $NAMESPACE -- /bin/bash -c "
git clone $MODEL_REPO /workspace/model
cd /workspace/model
python -c \"
import sys
sys.path.append('/workspace/model')
from eco import createEcosystem
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from datasurface.md.credential import CredentialStore

eco = createEcosystem()
cred_store = CredentialStore()

yellowlive = eco.getDataPlatformOrThrow('YellowLive')
yellowlive.renderGraph(eco, cred_store)

yellowforensic = eco.getDataPlatformOrThrow('YellowForensic')
yellowforensic.renderGraph(eco, cred_store)

print('✅ Model merge complete!')
\"
"

# Cleanup
echo "🧹 Cleaning up..."
kubectl delete job datasurface-bootstrap-job -n $NAMESPACE

echo "✅ Dynamic DAG update complete!"
echo "🔍 Check Airflow UI for dynamic DAGs: yellowlive__Store1_ingestion, yellowforensic__Store1_ingestion"
```

## Summary

This process updates the dynamic DAG factory system by:

1. **Running bootstrap generation** in a Kubernetes pod with the latest model
2. **Copying factory DAGs** to the Airflow scheduler
3. **Populating database configuration** for dynamic DAG generation
4. **Verifying operation** of the dynamic DAG system

The factory DAGs will automatically create ingestion DAGs based on the database configuration, providing a dynamic and scalable approach to DAG management.

## Related Documentation

- [MVP Dynamic DAG Implementation](MVP_DynamicDAG.md)
- [Kubernetes Infrastructure Setup](MVP_Kubernetes_Infrastructure_Setup.md)
- [July MVP Plan](July_MVP_Plan.md) 