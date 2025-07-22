# MVP for DataTransformers in YellowDataPlatform

DataTransformers are now fully implemented in YellowDataPlatform. A DataTransformer is code which executes against a Workspace and produces output which is then ingested into a new Datastore. For example, we can make a DataTransformer which takes a customer table with PII and removes the PII producing a new Datastore containing the customer data without PII through masking.

Common DataTransformer use cases:

* Changing the schema of input data
* Joining data with other data, materializing a view
* Masking data
* Aggregating data
* Data quality validation and enrichment

## Implementation Overview

DataTransformers are triggered when any of their input datasets change. The code runs in a job pod and produces output to staging tables (`dt_` prefix) which are then ingested by a separate output ingestion DAG into the final output datastore.

The DataTransformer code is packaged by its owner in its own GitHub repository with a normal test cycle. The Ecosystem model references a specific version of the code. The output Datastore and associated datasets/schemas are versioned together with the DataTransformer code in the model.

## Architecture Components

### 1. DataTransformer Factory DAG

A dedicated factory DAG (`{platform}_datatransformer_factory_dag.py`) manages all DataTransformer-related DAGs:

- **Reads from**: `{platform}_airflow_datatransformer` database table
- **Creates**: DataTransformer execution DAGs and output ingestion DAGs
- **Schedule**: Every 5 minutes to sync configurations
- **Manages**: DAGs ending with `_datatransformer` and `_dt_ingestion`

### 2. Database Tables

**DataTransformer Configuration Table**: `{platform}_airflow_datatransformer`
```sql
CREATE TABLE platform_airflow_datatransformer (
    workspace_name VARCHAR(255) PRIMARY KEY,
    config_json VARCHAR(4096) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

The `config_json` contains complete DAG configuration including:
- Input DAG IDs for external task sensors
- Output datastore information
- Platform configuration (credentials, git repo, etc.)

### 3. DAG Naming Conventions

To prevent conflicts between factory DAGs, distinct naming patterns are used:

- **Regular ingestion**: `{platform}__{stream_key}_ingestion`
- **DataTransformer execution**: `{platform}__{workspace}_datatransformer`
- **DataTransformer output**: `{platform}__{output_store}_dt_ingestion`

### 4. Dynamic DAG Creation

For each DataTransformer workspace, the factory creates two DAGs:

**A. DataTransformer Execution DAG**
- **External Task Sensors**: Wait for completion of all input ingestion DAGs
- **DataTransformer Job**: Runs `run-datatransformer` operation in Kubernetes pod
- **Trigger Output Ingestion**: Automatically triggers the output ingestion DAG
- **Log-based Result Checking**: Uses Airflow logs to determine next action

**B. DataTransformer Output Ingestion DAG**  
- **Output Ingestion Job**: Runs `ingest-datatransformer-output` operation
- **Triggered Execution**: Only runs when triggered by the execution DAG
- **Data Movement**: From `dt_` staging tables to final output datastore tables

### 5. Smart Sensor Dependencies

The model merge handler automatically generates correct DAG IDs based on store type:

```python
if isinstance(store.cmd, (KafkaIngestion, SQLSnapshotIngestion)):
    # Regular store → wait for regular ingestion DAG
    dag_id = f"{platform}__{stream_key}_ingestion"
    
elif isinstance(store.cmd, DataTransformerOutput):
    # DataTransformer output → wait for DataTransformer output DAG
    dag_id = f"{platform}__{store}_dt_ingestion"
```

This enables DataTransformers to chain together seamlessly - one DataTransformer can use the output of another as input.

## Model Merge Handler Integration

The model merge handler (`YellowGraphHandler.renderGraph()`) now:

1. **Creates DataTransformer table** during bootstrap (ring level 1)
2. **Populates configurations** for all workspaces with DataTransformers
3. **Generates bootstrap artifacts** including the DataTransformer factory DAG
4. **Manages dependencies** by analyzing workspace dataset usage

The handler scans all workspaces assigned to the platform and:
- Identifies workspaces with DataTransformers
- Maps input datasets to their ingestion DAG IDs (regular or DataTransformer output)
- Creates complete configuration context for the factory DAG
- Stores configurations in the database for dynamic DAG generation

## DataTransformer Job Implementation

The `DataTransformerJob` class handles:

1. **Git clone** of the DataTransformer code repository
2. **Table creation** for `dt_` output staging tables
3. **Transaction management** with truncate and rollback capability
4. **Code execution** calling the `executeTransformer` function
5. **Dataset mapping** providing table names to the transformer code

The DataTransformer code receives:
- Database connection for writing output
- Dictionary mapping `store#dataset` names to table names
- Input tables: standard merge table names
- Output tables: `dt_` prefixed staging table names

## Factory DAG Conflict Prevention

Each factory DAG has clear ownership boundaries:

**Regular Factory DAG**:
- Manages: `*_ingestion` (excludes `*_dt_ingestion` and `*_datatransformer`)
- Removes: Only DAGs matching regular ingestion patterns

**DataTransformer Factory DAG**:
- Manages: `*_datatransformer` AND `*_dt_ingestion`
- Removes: Only DAGs matching DataTransformer patterns

This prevents "zombie DAGs" and ensures safe parallel operation of both factory DAGs.

## DataTransformer Versioning and Rollback

The DataTransformer code is versioned in GitHub repositories. The Ecosystem model references specific versions. Output schema changes must be backwards compatible or require creating a new DataTransformer with consumer migration following the standard deprecation process.

Truly breaking changes require:
1. Creating a new DataTransformer with new output datastore
2. Consumer migration to the new output
3. Deprecation of the old DataTransformer
4. Retention of old data for audit/regulatory requirements

## Example Workflow

1. **Model Update**: Workspace with DataTransformer added to model
2. **Model Merge**: `handleModelMerge` triggered, populates DataTransformer table
3. **Factory Execution**: DataTransformer factory DAG reads table, creates execution and output DAGs
4. **Data Ingestion**: Input data changes trigger regular ingestion DAGs
5. **External Sensors**: DataTransformer execution DAG sensors detect ingestion completion  
6. **Transformer Execution**: DataTransformer job runs, writes to `dt_` staging tables
7. **Output Ingestion**: Output ingestion DAG triggered, moves data to final tables
8. **Consumer Access**: Downstream workspaces can now access the transformed data

This implementation provides a complete, production-ready DataTransformer system that integrates seamlessly with the existing YellowDataPlatform infrastructure.

