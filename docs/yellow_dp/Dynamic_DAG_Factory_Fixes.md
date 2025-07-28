# Dynamic DAG Factory - Key Fixes and Insights

This document summarizes the critical fixes and insights discovered during the implementation and debugging of the DataSurface dynamic DAG factory system.

## Overview

The dynamic DAG factory system was implemented to automatically generate Airflow DAGs based on database-stored configurations, eliminating the need for static DAG files. During implementation, several critical issues were identified and resolved.

## Critical Fixes Applied

### 1. Hostname Mangling Issue

**Problem**: The `to_k8s_name()` method was incorrectly applied to database hostnames, causing connection failures.

**Root Cause**: Kubernetes DNS hostnames like `pg-data.ns-kub-pg-test.svc.cluster.local` were being processed by `to_k8s_name()` which:
- Removes dots (.)
- Converts to lowercase
- Removes special characters
- Results in mangled hostname: `pg-datans-kub-pg-testsvcclusterlocal`

**Files Fixed**:
```python
# src/datasurface/platforms/yellow/yellow_dp.py
# Lines 456, 583, 883 - Changed from:
"postgres_hostname": self.to_k8s_name(self.mergeStore.hostPortPair.hostName)
# To:
"postgres_hostname": self.mergeStore.hostPortPair.hostName
```

**Impact**: Factory DAGs can now connect to PostgreSQL databases correctly.

### 2. Template Configuration Key Mismatch

**Problem**: Factory DAG template used inconsistent key naming causing runtime errors.

**Root Cause**: Template defined `'namespace': '{{ namespace_name }}'` but code accessed `platform_config['namespace_name']`.

**File Fixed**:
```python
# src/datasurface/platforms/yellow/templates/jinja/yellow_platform_factory_dag.py.j2
# Line 303 - Changed from:
'namespace': '{{ namespace_name }}'
# To:
'namespace_name': '{{ namespace_name }}'
```

**Impact**: Factory DAGs no longer fail with `'namespace_name'` key errors.

### 3. Credential Configuration Issues

**Problem**: Airflow scheduler couldn't access DataSurface merge database.

**Root Cause**: 
- Scheduler used Airflow database credentials instead of merge database credentials
- Missing environment variables for database connection
- Incorrect secret key names in environment variables

**Solutions Applied**:
```bash
# Updated PostgreSQL secret with correct credentials
kubectl patch secret postgres -n ns-kub-pg-test -p='{"data":{"username":"YWlyZmxvdw==","password":"YWlyZmxvdw==","postgres_USER":"YWlyZmxvdw==","postgres_PASSWORD":"YWlyZmxvdw=="}}'

# Added database connection environment variables to scheduler
kubectl patch deployment airflow-scheduler -n ns-kub-pg-test -p='{"spec":{"template":{"spec":{"containers":[{"name":"airflow-scheduler","env":[{"name":"DATASURFACE_POSTGRES_HOST","value":"pg-data.ns-kub-pg-test.svc.cluster.local"},{"name":"DATASURFACE_POSTGRES_PORT","value":"5432"},{"name":"DATASURFACE_POSTGRES_DATABASE","value":"datasurface_merge"}]}]}}}}'
```

**Impact**: Factory DAGs can now read configuration tables from the merge database.

## Architectural Insights

### When to Use `to_k8s_name()`

**✅ Correct Usage** (for Kubernetes resource names):
- Pod names
- Service names  
- Secret names
- ConfigMap names
- Any Kubernetes resource identifier

**❌ Incorrect Usage** (for existing identifiers):
- Database hostnames
- DNS names
- URLs
- Database names (if already valid)
- Any pre-existing valid identifier

### Factory DAG Architecture

The dynamic DAG factory follows this pattern:

1. **Platform-specific tables** store ingestion stream configurations
   - `yellowlive_airflow_dsg` 
   - `yellowforensic_airflow_dsg`

2. **Factory DAGs** read from these tables and create dynamic DAGs
   - `yellowlive_factory_dag.py`
   - `yellowforensic_factory_dag.py`

3. **Dynamic DAGs** are generated for each ingestion stream
   - `yellowlive__Store1_ingestion`
   - `yellowforensic__Store1_ingestion`

### Database Configuration Schema

Each platform maintains a table with this structure:
```sql
CREATE TABLE platform_airflow_dsg (
    stream_key VARCHAR(255) PRIMARY KEY,
    config_json TEXT,  -- JSON configuration for the ingestion stream
    status VARCHAR(50), -- 'active', 'disabled', etc.
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

#### **🔧 Fix 4: Factory DAG UI Visibility (ARCHITECTURAL IMPROVEMENT)**

**Initial Discovery:** Factory DAGs initially **did not appear** in Airflow UI as runnable DAGs.

**Problem Identified:** Hidden factory DAGs created operational blindness:
- ❌ No visibility into factory DAG execution
- ❌ No logs to troubleshoot when dynamic DAGs weren't created  
- ❌ No manual triggering when configurations change
- ❌ No monitoring of factory DAG health
- ❌ No way to see what dynamic DAGs were created/updated/removed

**Solution Implemented:** **Hybrid Factory DAG Architecture**

Factory DAGs now have **two operational modes**:
1. **Visible Factory DAG** - Schedulable DAG in Airflow UI for monitoring and control
2. **Dynamic DAG Creation** - Still generates ingestion DAGs based on database configurations

**Code Changes:**
```python
# New: Visible factory DAG that appears in Airflow UI
factory_dag = DAG(
    'yellowlive_factory_dag',
    description='Factory DAG for yellowlive - Creates and manages dynamic ingestion stream DAGs',
    schedule='*/5 * * * *',  # Check for configuration changes every 5 minutes
    is_paused_upon_creation=False,
    tags=['datasurface', 'factory', 'yellowlive', 'dynamic-dag-manager']
)

# Factory task with full logging and error handling
sync_task = PythonOperator(
    task_id='sync_dynamic_dags',
    python_callable=sync_dynamic_dags,
    dag=factory_dag
)
```

**New Operational Benefits:**
- ✅ **Factory DAGs visible** in Airflow UI as schedulable DAGs
- ✅ **Full logging** of factory operations and dynamic DAG creation
- ✅ **Manual triggering** capability when configurations change
- ✅ **Scheduled execution** every 5 minutes for automatic updates
- ✅ **Monitoring and alerting** on factory DAG execution
- ✅ **Troubleshooting visibility** through task logs and execution history
- ✅ **Enhanced logging** with clear indicators of which DAGs were created

**Factory DAG Log Output Example:**
```
================================================================================
🏭 FACTORY DAG EXECUTION - Dynamic DAG Lifecycle Management
📍 Platform: yellowlive
================================================================================
🔍 Scanning for existing factory-managed DAGs...
   Found 3 existing DAGs from previous factory runs
📊 Loading current configurations from database...
   Found 2 active configurations in database

📋 LIFECYCLE ANALYSIS:
   🆕 To Create: 1 DAGs
   🗑️  To Remove: 2 DAGs
   🔄 To Update: 1 DAGs

============================================================
🔄 EXECUTING DAG LIFECYCLE CHANGES
============================================================
🗑️  REMOVING OBSOLETE DAGs:
   🗑️  REMOVED: yellowlive__OldStore_ingestion
      → No longer in database configuration
   🗑️  REMOVED: yellowlive__DeletedStore_ingestion
      → No longer in database configuration

🆕 CREATING NEW DAGs:
   ✅ CREATED: yellowlive__NewStore_ingestion
      → New configuration found in database

🔄 UPDATING EXISTING DAGs:
   🔄 UPDATED: yellowlive__Store1_ingestion
      → Configuration refreshed from database

============================================================
📋 FACTORY DAG LIFECYCLE SUMMARY
============================================================
🗑️  REMOVED DAGS:
   → yellowlive__OldStore_ingestion
   → yellowlive__DeletedStore_ingestion
✅ CREATED DAGS:
   → yellowlive__NewStore_ingestion
🔄 UPDATED DAGS:
   → yellowlive__Store1_ingestion

📊 TOTAL CHANGES: 4
📊 ACTIVE DAGS: 2 (after lifecycle management)

💡 All DAGs are now synchronized with database configuration
💡 Obsolete DAGs have been removed to prevent zombie DAGs
💡 Dynamic DAGs are available in the Airflow UI
================================================================================
```

**Impact:** ✅ **Significant operational improvement** - factory DAGs now provide full visibility while maintaining dynamic DAG generation functionality.

#### **🔧 Fix 5: DAG Lifecycle Management (ZOMBIE DAG ELIMINATION)**

**Problem Identified:** Airflow doesn't track DAG provenance, causing **zombie DAGs** to persist indefinitely.

**Root Cause:** When configurations are removed from the database:
- Factory DAG stops creating those DAGs
- But existing DAGs remain in `globals()` forever  
- **Zombie DAGs** continue to appear in UI and execute
- No automatic cleanup mechanism exists

**Critical Issue:**
```python
# Day 1: Factory creates Store1, Store2, Store3 DAGs
globals()["yellowlive__Store1_ingestion"] = dag1
globals()["yellowlive__Store2_ingestion"] = dag2  
globals()["yellowlive__Store3_ingestion"] = dag3

# Day 2: Store2 config removed from database
# Factory only creates Store1 and Store3...
# Store2 DAG still exists in globals()! ❌ ZOMBIE DAG
```

**Solution Implemented:** **Full DAG Lifecycle Management**

**Code Changes:**
```python
def find_existing_factory_dags():
    """Find all DAGs previously created by this factory"""
    factory_dags = {}
    platform_prefix = "yellowlive__"
    ingestion_suffix = "_ingestion"
    
    for name, obj in globals().items():
        if (name.startswith(platform_prefix) and 
            name.endswith(ingestion_suffix) and 
            hasattr(obj, 'dag_id')):
            factory_dags[name] = obj
    return factory_dags

def sync_dynamic_dags(**context):
    # 1. Find existing factory DAGs
    existing_dags = find_existing_factory_dags()
    
    # 2. Load current database configurations  
    current_dags = load_platform_configurations()
    
    # 3. Calculate lifecycle changes
    to_create = current_dag_ids - existing_dag_ids
    to_remove = existing_dag_ids - current_dag_ids  # 🗑️ Zombie DAGs
    to_update = current_dag_ids & existing_dag_ids
    
    # 4. Execute lifecycle changes
    for dag_id in to_remove:
        del globals()[dag_id]  # ✅ Remove zombie DAG
        log.info(f"🗑️ REMOVED: {dag_id}")
```

**New Operational Benefits:**
- ✅ **Zombie DAG elimination** - obsolete DAGs automatically removed
- ✅ **Full lifecycle tracking** - create, update, delete operations logged
- ✅ **Database synchronization** - DAGs always match current configuration
- ✅ **Operational clarity** - clear visibility into what was changed
- ✅ **Resource cleanup** - prevents accumulation of obsolete DAGs
- ✅ **Audit trail** - complete log of all DAG lifecycle events

**Impact:** ✅ **Critical operational fix** - eliminates zombie DAG accumulation and provides complete DAG lifecycle management with full audit visibility.

## Best Practices Learned

### 1. Credential Management

- Use consistent naming for secret keys across all components
- Include both underscore and non-underscore variants for compatibility
- Test credential access from all components that need them

### 2. Environment Variables

- Always set database connection variables in scheduler
- Use environment variables instead of hardcoded values in templates
- Verify environment variables are available before using them

### 3. Template Generation

- Keep templates generic and parameterized
- Use consistent key naming between Python code and Jinja templates
- Test template generation with actual data

### 4. Kubernetes Integration

- Use proper service account permissions for database access
- Mount model files via ConfigMap for consistency
- Use proper Kubernetes DNS names for service communication
- Test connectivity between pods before deploying

### 5. Debugging Approach

- Test factory DAGs directly using `python3 /opt/airflow/dags/factory_dag.py`
- Check database connectivity separately from DAG parsing
- Verify all environment variables and secrets are available
- Use `py_compile` to catch syntax errors early

## Testing Strategy

### Unit Testing
```bash
# Test factory DAG compilation
kubectl exec $SCHEDULER_POD -- python3 -m py_compile /opt/airflow/dags/yellowlive_factory_dag.py

# Test factory DAG execution  
kubectl exec $SCHEDULER_POD -- python3 /opt/airflow/dags/yellowlive_factory_dag.py
```

### Integration Testing
```bash
# Verify database tables exist
kubectl exec -it $POSTGRES_POD -- psql -U airflow -d datasurface_merge -c "\dt"

# Check configuration data
kubectl exec -it $POSTGRES_POD -- psql -U airflow -d datasurface_merge -c "SELECT * FROM yellowlive_airflow_dsg;"

# Test hostname resolution
kubectl exec $SCHEDULER_POD -- nslookup pg-data.ns-kub-pg-test.svc.cluster.local
```

### End-to-End Testing
1. Deploy factory DAGs to Airflow
2. Verify they appear in Airflow UI without parsing errors
3. Trigger factory DAGs manually
4. Confirm dynamic DAGs are generated
5. Test dynamic DAG execution

## Performance Considerations

### Factory DAG Frequency
- Factory DAGs should run periodically (e.g., every 5 minutes) to pick up configuration changes
- Avoid running too frequently to prevent database load
- Use efficient queries with proper indexing on configuration tables

### Database Optimization
- Index the `stream_key` and `status` columns in configuration tables
- Use connection pooling for database access
- Consider caching mechanisms for frequently accessed configurations

### Kubernetes Resources
- Set appropriate resource limits for factory DAG executions
- Use persistent volumes for any temporary file storage
- Monitor pod resource usage during DAG generation

## Future Enhancements

### Configuration Management
- Add configuration validation before storing in database
- Implement configuration versioning and rollback capabilities
- Add audit logging for configuration changes

### Monitoring and Alerting
- Monitor factory DAG execution success/failure rates
- Alert on missing or invalid configurations
- Track dynamic DAG generation and execution metrics

### Security Improvements
- Implement proper RBAC for configuration table access
- Add encryption for sensitive configuration data
- Audit access to configuration databases

## Conclusion

The dynamic DAG factory system provides a powerful way to manage Airflow DAGs programmatically. The key to successful implementation is:

1. **Proper credential management** with consistent naming
2. **Correct use of utility functions** like `to_k8s_name()`
3. **Thorough testing** at each integration point
4. **Clear separation** between Kubernetes resource names and data identifiers

These fixes and insights ensure a robust, maintainable dynamic DAG factory system. 