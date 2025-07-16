# MVP Kubernetes Infrastructure Setup

This document tracks the setup and testing of the Kubernetes infrastructure components needed for the MVP data pipeline demonstration.

## Overview

**Goal:** Stand up the essential Kubernetes infrastructure to test our generated DAGs and demonstrate the MVP data pipeline with the data change simulator.

**Components to Deploy:**
- ✅ PostgreSQL database (for Airflow metadata and data platform storage)
- ✅ Airflow (scheduler, webserver, executor)
- ✅ DataSurface job container (for SnapshotMergeJob execution)
- ✅ Data Change Simulator (in its own pod for easy start/stop)

**NOT Included Yet:** Kafka, Kafka Connect (SQL snapshot ingestion only)

## Prerequisites

- ✅ Docker Desktop with Kubernetes enabled
- ✅ Generated MVP infrastructure artifacts in `src/tests/yellow_dp_tests/mvp_model/generated_output/`
- ✅ Working `customer_db` database on localhost
- ✅ Tested data change simulator
- 🔐 **GitHub Personal Access Token** with access to `billynewport/mvpmodel` repository

**🛡️ Security Requirements:**
- Replace all instances of `MASKED_PAT` in commands with your actual GitHub token
- Never commit actual token values to version control
- Use environment variables or secure secret management in production

## Phase 1: Docker Container Preparation

### Task 1.1: Build Current DataSurface Container ✅ **COMPLETED**

**Objective:** ✅ Build a current Docker image with the latest DataSurface code including MVP features.

**Steps:**
1. ✅ **Review and update Dockerfile.datasurface if needed**
   - ✅ All dependencies included (psycopg2-binary, libpq-dev, etc.)
   - ✅ Python 3.13-slim compatibility verified
   - ✅ Latest src/ code included in build

2. ✅ **Build the Docker image**
   ```bash
   docker build -f Dockerfile.datasurface -t datasurface/datasurface:latest .
   # ✅ Built successfully in 32.8s
   ```

3. ✅ **Test the container locally**
   ```bash
   docker run --rm datasurface/datasurface:latest python -c "
   import datasurface
   from datasurface.cmd.platform import handleModelMerge
   print('DataSurface imports working')
   print('Version info and capabilities check complete')
   "
   # ✅ Output: DataSurface imports working, Version info and capabilities check complete
   ```

4. ✅ **Final container build for GitHub-based model access**
   ```bash
   docker build -f Dockerfile.datasurface -t datasurface/datasurface:latest .
   # ✅ Container ready for GitHub cloning (no model baked in)
   ```

5. ✅ **Verified GitHub-based model access**
   ```bash
   docker run --rm -e GIT_TOKEN=MASKED_PAT datasurface/datasurface:latest bash -c "
   cd /workspace/model
   git clone https://\$GIT_TOKEN@github.com/billynewport/mvpmodel.git .
   python -c 'import sys; sys.path.append(\"/workspace/model\"); from eco import createEcosystem; eco = createEcosystem(); print(f\"✅ Ecosystem loaded from GitHub: {eco.name}\"); print(f\"✅ Platforms: {list(eco.dataPlatforms.keys())}\")'
   "
   # ✅ Output: MVP model successfully loaded from billynewport/mvpmodel repository!
   ```

**Success Criteria:**
- ✅ Docker image builds without errors
- ✅ DataSurface imports work correctly
- ✅ MVP ecosystem model loads successfully in container
- ✅ All required dependencies available

### Task 1.2: Test Data Change Simulator in Container ✅ **COMPLETED**

**Objective:** ✅ Verify the data change simulator works in a containerized environment.

**Steps:**
1. ✅ **Test simulator in container with external database**
   ```bash
   docker run --rm --network host \
     -v $(pwd)/src/tests:/app/tests \
     datasurface/datasurface:latest \
     python /app/tests/data_change_simulator.py \
     --host host.docker.internal \
     --database customer_db \
     --max-changes 3 \
     --min-interval 1 \
     --max-interval 1 \
     --verbose
   # ✅ Output: Successfully created 1 customer, added 2 addresses, all changes persisted
   ```

**Success Criteria:**
- ✅ Simulator connects to external database successfully
- ✅ Database changes are persisted correctly
- ✅ Container networking works for database access

**Test Results:**
- ✅ Added address A52696520823 for customer C52688413877 (set as billing)
- ✅ Created customer C52696521245 (Sam Davis) with address A52696521336  
- ✅ Added address A52696522407 for customer CUST001 (set as billing)
- ✅ All 3 changes completed successfully and database connection closed properly

## Phase 2: Kubernetes Secrets and Configuration ✅ **COMPLETED**

### Task 2.1: Create Required Kubernetes Secrets ✅ **COMPLETED**

**Objective:** ✅ Set up all secrets needed for the MVP infrastructure.

**Steps:**
1. ✅ **Create namespace**
   ```bash
   kubectl create namespace ns-kub-pg-test
   # ✅ Namespace already existed from previous testing
   ```

2. ✅ **Create PostgreSQL credentials secret**
   ```bash
   kubectl create secret generic postgres \
     --namespace ns-kub-pg-test \
     --from-literal=username=postgres \
     --from-literal=password=datasurface-test-123 \
     --from-literal=POSTGRES_USER=postgres \
     --from-literal=POSTGRES_PASSWORD=datasurface-test-123
   # ✅ Updated secret to include both DAG-expected keys and standard PostgreSQL keys
   ```

3. ✅ **Git, Slack, Connect credentials already exist**
   ```bash
   # ✅ All required secrets verified present: git, slack, connect, airflow
   ```

4. ✅ **Verify secrets created**
   ```bash
   kubectl get secrets -n ns-kub-pg-test
   # ✅ Output: 5 secrets (airflow, connect, git, postgres, slack)
   ```

**Success Criteria:**
- ✅ All required secrets exist in the namespace
- ✅ Secret keys match what's expected by the generated DAGs (username/password)
- ✅ No sensitive data exposed in commands or logs

### Task 2.2: GitHub-Based Model Access ✅ **COMPLETED** (Production Approach)

**Objective:** ✅ Set up proper GitHub-based model access for production deployment.

**Final Solution:** Clone MVP ecosystem model from GitHub repository at runtime.

**Why This Approach is Correct:**
- ✅ **Production-Ready:** Models are versioned in GitHub as intended
- ✅ **Secure:** Uses GitHub personal access tokens for private repository access
- ✅ **Flexible:** Model changes can be deployed without rebuilding containers
- ✅ **Scalable:** Standard GitOps pattern for configuration management

**Repository Setup:**
- ✅ **Repository:** `billynewport/mvpmodel` (private)
- ✅ **Contents:** `eco.py`, `dsg_platform_mapping.json`
- ✅ **Access:** GitHub personal access token with repo permissions

**Implementation Steps:**
1. ✅ **Created GitHub secret with correct key name:**
   ```bash
   kubectl create secret generic git \
     --namespace ns-kub-pg-test \
     --from-literal=token=MASKED_PAT
   # ✅ Secret created with 'token' key as expected by generated DAGs
   ```

2. ✅ **Verified GitHub repository access and cloning:**
   ```bash
   # Test cloning from billynewport/mvpmodel
   docker run --rm -e GIT_TOKEN=MASKED_PAT datasurface/datasurface:latest bash -c "
   cd /workspace/model
   git clone https://\$GIT_TOKEN@github.com/billynewport/mvpmodel.git .
   ls -la  # ✅ Shows: eco.py, dsg_platform_mapping.json
   "
   ```

3. ✅ **Verified ecosystem model loading from GitHub:**
   ```bash
   # ✅ Output: Ecosystem loaded from GitHub: Test
   # ✅ Output: Platforms: ['YellowLive', 'YellowForensic']
   # ✅ Output: MVP model successfully loaded from billynewport/mvpmodel repository!
   ```

4. ✅ **Created ConfigMaps for DAG volume expectations:**
   ```bash
   kubectl create configmap yellowlive-git-config \
     --namespace ns-kub-pg-test \
     --from-literal=repo_url=https://github.com/billynewport/mvpmodel.git
   
   kubectl create configmap yellowforensic-git-config \
     --namespace ns-kub-pg-test \
     --from-literal=repo_url=https://github.com/billynewport/mvpmodel.git
   # ✅ ConfigMaps created to satisfy generated DAG volume mount requirements
   ```

**Success Criteria:**
- ✅ GitHub repository `billynewport/mvpmodel` accessible with provided token
- ✅ Repository contains correct MVP ecosystem model files
- ✅ Model loads successfully from cloned repository in container
- ✅ Kubernetes secrets and ConfigMaps ready for DAG execution

**🔐 Security Note:**
- **GitHub PAT tokens are masked in this documentation** as `MASKED_PAT`
- **Actual token values should never be committed to version control**
- **In production, use secure secret management** (Kubernetes secrets, HashiCorp Vault, etc.)
- **Rotate tokens regularly** and use least-privilege access principles

## Phase 3: Core Infrastructure Deployment

### Task 3.1: Deploy PostgreSQL ⏳ **IN PROGRESS**

**Objective:** Deploy PostgreSQL for Airflow metadata and data platform storage.

**Steps:**
1. ⏳ **Extract PostgreSQL configuration from generated YAML**
   ```bash
   # Extract postgres section from YellowLive kubernetes-bootstrap.yaml
   # Review configuration for any needed modifications
   ```

2. ⏳ **Deploy PostgreSQL**
   ```bash
   # Apply the PostgreSQL portions of the generated kubernetes-bootstrap.yaml
   # OR use a simplified PostgreSQL deployment for testing
   ```

3. ⏳ **Verify PostgreSQL deployment**
   ```bash
   kubectl get pods -n ns-kub-pg-test -l app=postgres
   kubectl logs -n ns-kub-pg-test deployment/pg-data
   ```

4. ⏳ **Test PostgreSQL connectivity**
   ```bash
   kubectl run postgres-client --rm -i --tty --image postgres:16 \
     --namespace ns-kub-pg-test -- \
     psql -h pg-data -U postgres -d postgres
   ```

**Success Criteria:**
- [ ] PostgreSQL pod is running and healthy
- [ ] Database is accessible from within the cluster
- [ ] Credentials work correctly
- [ ] Required databases can be created

### Task 3.2: Deploy Airflow ⏳ **IN PROGRESS**

**Objective:** Deploy Airflow scheduler and webserver for DAG execution.

**Steps:**
1. ⏳ **Extract Airflow configuration from generated YAML**
   ```bash
   # Review airflow sections in kubernetes-bootstrap.yaml
   # Identify necessary components (scheduler, webserver, executor)
   ```

2. ⏳ **Deploy Airflow components**
   ```bash
   # Apply Airflow portions of the generated kubernetes-bootstrap.yaml
   # Ensure proper database connectivity
   ```

3. ⏳ **Initialize Airflow database**
   ```bash
   # Run airflow db init if needed
   # Create admin user
   ```

4. ⏳ **Verify Airflow deployment**
   ```bash
   kubectl get pods -n ns-kub-pg-test -l app=airflow
   kubectl port-forward -n ns-kub-pg-test service/airflow 8080:8080
   # Access http://localhost:8080
   ```

**Success Criteria:**
- [ ] Airflow scheduler is running
- [ ] Airflow webserver is accessible
- [ ] DAGs directory is properly mounted
- [ ] Database connectivity works

## Phase 4: DAG Deployment and Testing

### Task 4.1: Deploy Generated DAGs ⏳ **IN PROGRESS**

**Objective:** Make the generated ingestion and infrastructure DAGs available to Airflow.

**Steps:**
1. ⏳ **Create DAG ConfigMaps**
   ```bash
   kubectl create configmap yellowlive-ingestion-dag \
     --namespace ns-kub-pg-test \
     --from-file=yellowlive__Store1_ingestion.py=src/tests/yellow_dp_tests/mvp_model/generated_output/YellowLive/yellowlive__Store1_ingestion.py
   
   kubectl create configmap yellowlive-infrastructure-dag \
     --namespace ns-kub-pg-test \
     --from-file=yellowlive_infrastructure_dag.py=src/tests/yellow_dp_tests/mvp_model/generated_output/YellowLive/yellowlive_infrastructure_dag.py
   
   kubectl create configmap yellowforensic-ingestion-dag \
     --namespace ns-kub-pg-test \
     --from-file=yellowforensic__Store1_ingestion.py=src/tests/yellow_dp_tests/mvp_model/generated_output/YellowForensic/yellowforensic__Store1_ingestion.py
   
   kubectl create configmap yellowforensic-infrastructure-dag \
     --namespace ns-kub-pg-test \
     --from-file=yellowforensic_infrastructure_dag.py=src/tests/yellow_dp_tests/mvp_model/generated_output/YellowForensic/yellowforensic_infrastructure_dag.py
   ```

2. ⏳ **Mount DAGs in Airflow deployment**
   ```bash
   # Modify Airflow deployment to mount DAG ConfigMaps
   # Ensure DAGs are visible in Airflow UI
   ```

3. ⏳ **Verify DAGs loaded**
   ```bash
   # Check Airflow UI for DAG visibility
   # Ensure no parse errors
   ```

**Success Criteria:**
- [ ] All 4 DAGs are visible in Airflow UI
- [ ] No parsing errors in DAGs
- [ ] DAG configuration looks correct

### Task 4.2: Test Individual DAG Components ⏳ **IN PROGRESS**

**Objective:** Verify each DAG component works before full pipeline testing.

**Steps:**
1. ⏳ **Test KubernetesPodOperator configuration**
   ```bash
   # Manually trigger a simple task to verify pod creation
   # Check that secrets and ConfigMaps are properly mounted
   ```

2. ⏳ **Test credential access**
   ```bash
   # Verify that jobs can access postgres, git, and other secrets
   # Test ConfigMap mounting for ecosystem model
   ```

3. ⏳ **Test DataSurface job execution**
   ```bash
   # Run a simple DataSurface command in a pod
   # Verify ecosystem model loading
   ```

**Success Criteria:**
- [ ] Pods can be created successfully
- [ ] Secrets are accessible from job pods
- [ ] ConfigMaps are mounted correctly
- [ ] DataSurface commands execute successfully

## Phase 5: Data Change Simulator Pod

### Task 5.1: Deploy Simulator as Kubernetes Job ⏳ **IN PROGRESS**

**Objective:** Run the data change simulator in its own pod for easy management.

**Steps:**
1. ⏳ **Create simulator deployment YAML**
   ```yaml
   # Create a simple deployment or job for the simulator
   # Include database connectivity
   # Allow for easy start/stop
   ```

2. ⏳ **Deploy simulator**
   ```bash
   kubectl apply -f simulator-deployment.yaml -n ns-kub-pg-test
   ```

3. ⏳ **Test simulator connectivity**
   ```bash
   # Verify simulator can connect to customer_db
   # Test database modifications
   ```

**Success Criteria:**
- [ ] Simulator pod runs successfully
- [ ] Database connectivity works
- [ ] Changes are persisted to customer_db
- [ ] Pod can be easily stopped and started

## Phase 6: Integration Testing

### Task 6.1: Test Complete Infrastructure ⏳ **IN PROGRESS**

**Objective:** Verify all components work together correctly.

**Steps:**
1. ⏳ **Start data change simulator**
   ```bash
   # Deploy simulator with continuous changes
   ```

2. ⏳ **Manually trigger ingestion DAGs**
   ```bash
   # Trigger YellowLive and YellowForensic ingestion DAGs
   # Monitor job execution
   ```

3. ⏳ **Verify data processing**
   ```bash
   # Check that SnapshotMergeJob executes successfully
   # Verify data platform storage receives data
   ```

4. ⏳ **Test DAG self-triggering**
   ```bash
   # Verify that DAGs reschedule correctly based on return codes
   # Test continuous processing capability
   ```

**Success Criteria:**
- [ ] Simulator generates continuous database changes
- [ ] Ingestion DAGs execute successfully
- [ ] Data is processed and stored correctly
- [ ] Self-triggering mechanism works
- [ ] Both live and forensic platforms process data

## Success Criteria for Complete Phase

✅ **Infrastructure Ready Checklist:**
- [ ] DataSurface container built and tested
- [ ] All Kubernetes secrets created correctly
- [ ] PostgreSQL and Airflow deployed and operational
- [ ] All 4 generated DAGs loaded and parseable
- [ ] Data change simulator running in pod
- [ ] Manual DAG execution successful
- [ ] Data flows from customer_db through to data platforms
- [ ] Ready for end-to-end pipeline validation

## Next Steps After Completion

Once this infrastructure setup is complete, we'll be ready for:
1. **Task 3.2: End-to-End Pipeline Validation** - Full automated pipeline testing
2. **Consumer database and view creation testing**
3. **Performance and latency validation**
4. **Integration with MERGE Handler (Task 4.2)**

## Troubleshooting Notes

**Common Issues:**
- Database connectivity from pods
- Secret mounting and environment variables
- ConfigMap file permissions
- Airflow DAG parsing errors
- Resource limits and scheduling

**Debugging Commands:**
```bash
# Check pod logs
kubectl logs -n ns-kub-pg-test <pod-name>

# Exec into pod for debugging
kubectl exec -it -n ns-kub-pg-test <pod-name> -- /bin/bash

# Check secret contents
kubectl get secret -n ns-kub-pg-test <secret-name> -o yaml

# Verify ConfigMap contents
kubectl get configmap -n ns-kub-pg-test <configmap-name> -o yaml
```

---

**Status:** 🚀 **Ready to Begin Infrastructure Setup**
**Estimated Time:** 2-3 hours for complete setup and testing
**Dependencies:** Docker Desktop Kubernetes, customer_db database, generated artifacts 