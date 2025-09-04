# HOWTO: Data Change Simulator

## Overview

The Data Change Simulator is a CLI tool that continuously generates realistic customer lifecycle changes in the producer database to demonstrate real-time ingestion and processing capabilities. It simulates business operations like new customer registration, address updates, and customer information changes.

## Purpose

This simulator supports the MVP data pipeline by:

1. **Real-Time Processing Demonstration**: Continuous changes trigger the ingestion pipeline
2. **Dual Processing Mode Testing**: Changes are visible in both live (1-minute) and forensic (10-minute) processing
3. **Hash Difference Validation**: Substantial changes ensure forensic mode detects differences
4. **System Load Testing**: Realistic change patterns stress-test the pipeline
5. **End-to-End Verification**: Provides observable data flow from producer to consumer

## Kubernetes Deployment

### Basic Long-Running Deployment

For continuous data generation, deploy the simulator as a Kubernetes Job:

```yaml
# data-change-simulator.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: data-change-simulator-longrun
  namespace: ns-kub-pg-test
spec:
  template:
    spec:
      containers:
      - name: data-change-simulator
        image: datasurface/datasurface:latest
        command: ["/bin/bash"]
        args:
        - -c
        - |
          echo "🚀 Starting Data Change Simulator"
          python /app/src/tests/data_change_simulator.py \
            --host pg-data.ns-kub-pg-test.svc.cluster.local \
            --database customer_db \
            --user airflow \
            --password airflow \
            --create-tables \
            --max-changes 1000000 \
            --min-interval 10 \
            --max-interval 25 \
            --verbose
        env:
        - name: PYTHONPATH
          value: "/app/src"
      restartPolicy: Never
  backoffLimit: 3
```

### Deploy to Kubernetes

```bash
# Deploy the simulator
kubectl apply -f data-change-simulator.yaml

# Check status
kubectl get pods -n ns-kub-pg-test | grep simulator

# Monitor logs
kubectl logs -f data-change-simulator-longrun-xxxxx -n ns-kub-pg-test
```

### Quick Testing Deployment

For short-term testing with limited changes:

```yaml
# test-data-simulator.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: data-change-simulator-test
  namespace: ns-kub-pg-test
spec:
  template:
    spec:
      containers:
      - name: data-change-simulator
        image: datasurface/datasurface:latest
        command: ["/bin/bash"]
        args:
        - -c
        - |
          python /app/src/tests/data_change_simulator.py \
            --host pg-data.ns-kub-pg-test.svc.cluster.local \
            --database customer_db \
            --user airflow \
            --password airflow \
            --create-tables \
            --max-changes 50 \
            --min-interval 2 \
            --max-interval 5 \
            --verbose
        env:
        - name: PYTHONPATH
          value: "/app/src"
      restartPolicy: Never
```

## Command Line Parameters

### Database Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--host HOST` | `localhost` | Database hostname or IP address |
| `--port PORT` | `5432` | Database port number |
| `--database DATABASE` | `customer_db` | Database name to connect to |
| `--user USER` | `postgres` | Database username |
| `--password PASSWORD` | `postgres` | Database password |

### Simulation Control Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--min-interval MIN` | `5` | Minimum seconds between changes |
| `--max-interval MAX` | `30` | Maximum seconds between changes |
| `--max-changes MAX` | unlimited | Maximum number of changes before stopping |
| `--create-tables` | disabled | Create tables if they don't exist |
| `--verbose` | disabled | Enable detailed logging |

### Kubernetes-Specific Configuration

For Kubernetes deployments, use these typical settings:

- **Host**: `pg-data.ns-kub-pg-test.svc.cluster.local` (cluster DNS)
- **User/Password**: `airflow/airflow` (matches Kubernetes postgres secret)
- **Database**: `customer_db` (producer database name)
- **Create Tables**: Always use `--create-tables` for self-contained deployment

## Data Operations

The simulator performs these operations with weighted probability:

### 1. CREATE New Customers (15% weight)
- Generates realistic names, emails, phone numbers, dates of birth
- Creates a primary address for each new customer
- Uses the address as both primary and billing address initially

**Sample Output:**
```
✅ CREATED customer C52966497936 (Jack Lee) with address A52966497684
```

### 2. UPDATE Customer Information (25% weight)
- Updates email addresses and/or phone numbers
- Uses realistic email formats and phone number patterns

**Sample Output:**
```
📝 UPDATED customer C52966497936 (email, phone)
```

### 3. ADD New Addresses (20% weight)
- Creates additional addresses for existing customers
- Sometimes sets new address as the billing address (30% chance)

**Sample Output:**
```
🏠 ADDED address A52966502341 for customer C52966497936 (set as billing)
```

### 4. UPDATE Existing Addresses (30% weight)
- Changes street addresses, city/state combinations, or ZIP codes
- Simulates customers moving or correcting address information

**Sample Output:**
```
🏠 UPDATED address A52966502341 (street: 123 New Oak St)
```

### 5. DELETE Addresses (10% weight)
- Removes non-primary, non-billing addresses only
- Only deletes from customers who have multiple addresses
- Preserves referential integrity

**Sample Output:**
```
🗑️  DELETED address A52966502341 for customer C52966497936
```

## Common Use Cases

### Continuous Long-Term Testing

For long-running pipeline validation (days/weeks):

```bash
python data_change_simulator.py \
  --max-changes 1000000 \
  --min-interval 10 \
  --max-interval 25 \
  --verbose
```

**Runtime**: ~203 days with these settings

### Quick Integration Testing

For rapid testing and validation:

```bash
python data_change_simulator.py \
  --max-changes 20 \
  --min-interval 1 \
  --max-interval 3 \
  --verbose
```

**Runtime**: ~1-2 minutes

### Load Testing

For high-frequency change generation:

```bash
python data_change_simulator.py \
  --max-changes 500 \
  --min-interval 1 \
  --max-interval 2 \
  --verbose
```

**Runtime**: ~15-20 minutes with sustained load

### Forensic Mode Validation

For testing 10-minute forensic processing cycles:

```bash
python data_change_simulator.py \
  --min-interval 60 \
  --max-interval 120 \
  --verbose
```

**Purpose**: Ensures changes span multiple forensic processing windows

## Monitoring and Management

### Check Running Status

```bash
# Find simulator pods
kubectl get pods -n ns-kub-pg-test | grep simulator

# Check detailed status
kubectl describe pod data-change-simulator-longrun-xxxxx -n ns-kub-pg-test
```

### Monitor Activity

```bash
# Follow live logs
kubectl logs -f data-change-simulator-longrun-xxxxx -n ns-kub-pg-test

# Check recent activity
kubectl logs data-change-simulator-longrun-xxxxx -n ns-kub-pg-test --tail=20
```

### Stop Running Simulator

```bash
# Delete the job (stops gracefully)
kubectl delete job data-change-simulator-longrun -n ns-kub-pg-test

# Verify stopped
kubectl get pods -n ns-kub-pg-test | grep simulator
```

## Sample Output

### Startup
```
🚀 Starting Data Change Simulator
📊 Configuration: 1,000,000 max changes, 10-25 second intervals
2025-07-19 23:08:17 - INFO - 🚀 Starting Data Change Simulator
2025-07-19 23:08:17 - INFO -    Database: customer_db at pg-data.ns-kub-pg-test.svc.cluster.local:5432
2025-07-19 23:08:17 - INFO -    Change interval: 10-25 seconds
2025-07-19 23:08:17 - INFO -    Max changes: 1000000
2025-07-19 23:08:17 - INFO - Connected to database customer_db
2025-07-19 23:08:17 - INFO - 🗄️ Creating tables if they don't exist...
2025-07-19 23:08:17 - INFO - ✅ Tables are ready!
```

### Active Operation
```
2025-07-19 23:08:17 - INFO - ✅ CREATED customer C52966497936 (Jack Lee) with address A52966497684
2025-07-19 23:08:17 - DEBUG - Waiting 14 seconds until next change...
2025-07-19 23:08:31 - INFO - 📝 UPDATED customer C52966487123 (email, phone)
2025-07-19 23:08:31 - DEBUG - Waiting 22 seconds until next change...
2025-07-19 23:08:53 - INFO - 🏠 ADDED address A52966512847 for customer C52966497936
2025-07-19 23:08:53 - DEBUG - Waiting 18 seconds until next change...
```

### Completion
```
2025-07-19 23:22:27 - INFO - 🎯 Reached maximum changes limit (50). Stopping...
2025-07-19 23:22:27 - INFO - Database connection closed
2025-07-19 23:22:27 - INFO - 🛑 Data Change Simulator stopped
```

## Troubleshooting

### Connection Issues

**Problem**: `connection failed: FATAL: password authentication failed`

**Solution**: Verify database credentials match Kubernetes secrets:
```bash
# Check current credentials
kubectl get secret postgres -n ns-kub-pg-test -o jsonpath='{.data.username}' | base64 -d
kubectl get secret postgres -n ns-kub-pg-test -o jsonpath='{.data.password}' | base64 -d

# Update simulator configuration to match
```

### Table Creation Issues

**Problem**: `relation "customers" does not exist`

**Solution**: Always use `--create-tables` flag in Kubernetes deployments:
```bash
--create-tables
```

### Pod Completion Issues

**Problem**: Pod shows "Completed" status immediately

**Check logs** to identify the issue:
```bash
kubectl logs data-change-simulator-xxxxx -n ns-kub-pg-test
```

Common causes:
- Database connection failure
- Missing `--max-changes` parameter
- Incorrect credentials

### Performance Issues

**Problem**: Changes too fast/slow for testing needs

**Solution**: Adjust interval parameters:
- **Faster**: `--min-interval 1 --max-interval 2`
- **Slower**: `--min-interval 60 --max-interval 120`

## Local Development

For local testing outside Kubernetes:

### Prerequisites

1. **Database Setup**: Create and populate `customer_db`:
   ```bash
   psql -h localhost -p 5432 -U postgres -f src/tests/yellow_dp_tests/mvp_model/setupcustomerdb.sql
   ```

2. **Python Dependencies**:
   ```bash
   pip install psycopg2-binary
   ```

### Basic Local Usage

```bash
cd src/tests
python data_change_simulator.py --verbose
```

### Custom Local Configuration

```bash
python data_change_simulator.py \
    --host localhost \
    --port 5432 \
    --database customer_db \
    --user postgres \
    --password postgres \
    --max-changes 100 \
    --min-interval 5 \
    --max-interval 15 \
    --verbose
```

## Integration with Data Pipeline

The simulator is designed to work seamlessly with the YellowDataPlatform pipeline:

1. **Producer Database**: Simulator writes to `customer_db`
2. **SQL Snapshot Ingestion**: Platform reads changes from `customer_db`
3. **Live Processing**: Changes appear in YellowLive merge tables within 1 minute
4. **Forensic Processing**: Complete change history maintained in YellowForensic tables
5. **Consumer Access**: Changes visible through workspace views

**Validation**: Check that simulator changes trigger pipeline processing:
```bash
# Check live merge tables
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- \
  psql -U airflow -d datasurface_merge -c "SELECT COUNT(*) FROM yellowlive_store1_customers_merge;"

# Check forensic merge tables  
kubectl exec -it test-dp-postgres-bd5c4b886-mr8px -n ns-kub-pg-test -- \
  psql -U airflow -d datasurface_merge -c "SELECT COUNT(*) FROM yellowforensic_store1_customers_merge;"
```

## Summary

The Data Change Simulator is a powerful tool for:
- **Testing data pipelines** with realistic business scenarios
- **Validating processing modes** (live vs forensic)
- **Load testing** ingestion and merge capabilities
- **Demonstrating end-to-end** data flow

With proper configuration, it provides continuous, realistic data changes that thoroughly exercise all aspects of the DataSurface platform. 